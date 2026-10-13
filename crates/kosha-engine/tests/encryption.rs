//! Encryption at rest: nothing readable on disk, dedup intact, key checked.

use std::sync::Arc;

use futures::TryStreamExt;
use kosha_engine::*;

fn config(dir: &std::path::Path, key: Option<[u8; 32]>) -> EngineConfig {
    let mut cfg = EngineConfig::new(dir);
    cfg.sync = SyncMode::None;
    cfg.chunking = ChunkerConfig {
        min: 1024,
        avg: 4096,
        max: 16384,
    };
    cfg.encryption = key.map(|k| Arc::new(ChunkKeys::new(&k)));
    cfg
}

async fn put(e: &Engine, b: &str, k: &str, body: &[u8]) -> ObjectMeta {
    let mut w = e.writer();
    w.write(body).await.unwrap();
    e.put_object(
        b,
        k,
        w.finish().await.unwrap(),
        ObjectAttrs::default(),
        WriteCondition::default(),
    )
    .await
    .unwrap()
}

async fn get(e: &Engine, b: &str, k: &str) -> Vec<u8> {
    let m = e.head_object(b, k).await.unwrap();
    let parts: Vec<_> = e.read(&m, 0, m.size).try_collect().await.unwrap();
    parts.concat()
}

/// True if `needle` occurs in any file under `dir`.
fn on_disk(dir: &std::path::Path, needle: &[u8]) -> bool {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if std::fs::read(&p)
                .unwrap()
                .windows(needle.len())
                .any(|w| w == needle)
            {
                return true;
            }
        }
    }
    false
}

/// Compressible text with a marker that must never appear on disk.
fn secret_text(n: usize) -> Vec<u8> {
    let mut v = Vec::new();
    let mut i = 0u64;
    while v.len() < n {
        v.extend_from_slice(format!("line {i}: TOP-SECRET-PAYLOAD quarterly numbers\n").as_bytes());
        i += 1;
    }
    v.truncate(n);
    v
}

#[tokio::test]
async fn data_is_encrypted_and_still_deduplicated() {
    let dir = tempfile::tempdir().unwrap();
    let key = [42u8; 32];
    let big = secret_text(200_000);
    let small = b"TOP-SECRET-PAYLOAD tiny".to_vec();
    {
        let e = Engine::open(config(dir.path(), Some(key))).unwrap();
        assert!(e.encryption_fingerprint().is_some());
        e.create_bucket("vault").await.unwrap();
        put(&e, "vault", "big.txt", &big).await;
        put(&e, "vault", "copy.txt", &big).await;
        put(&e, "vault", "small.txt", &small).await;
        assert_eq!(get(&e, "vault", "big.txt").await, big);
        assert_eq!(get(&e, "vault", "small.txt").await, small);

        // The second copy added no unique bytes.
        let s = e.stats().await.unwrap();
        assert_eq!(s.logical_bytes, 2 * big.len() as u64 + small.len() as u64);
        assert_eq!(s.unique_bytes, big.len() as u64 + small.len() as u64);
        // Compression still applies (it happens before encryption).
        assert!(s.physical_bytes < s.unique_bytes / 2, "{s:?}");
    }
    assert!(!on_disk(dir.path(), b"TOP-SECRET-PAYLOAD"));

    // Compression alone can hide the marker (zstd entropy-codes literals),
    // so check again with compression off: only encryption hides it now.
    let raw_dir = tempfile::tempdir().unwrap();
    {
        let mut cfg = config(raw_dir.path(), Some(key));
        cfg.compression_level = 0;
        let e = Engine::open(cfg).unwrap();
        e.create_bucket("vault").await.unwrap();
        put(&e, "vault", "big.txt", &big).await;
        put(&e, "vault", "small.txt", &small).await;
        assert_eq!(get(&e, "vault", "big.txt").await, big);
    }
    assert!(!on_disk(raw_dir.path(), b"TOP-SECRET-PAYLOAD"));

    // The same key reopens it; no key or another key is refused.
    let e = Engine::open(config(dir.path(), Some(key))).unwrap();
    assert_eq!(get(&e, "vault", "copy.txt").await, big);
    drop(e);
    assert!(Engine::open(config(dir.path(), None)).is_err());
    assert!(Engine::open(config(dir.path(), Some([7u8; 32]))).is_err());
}

#[tokio::test]
async fn control_unencrypted_data_is_visible() {
    // Guards the test above: the marker is found when encryption is off.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), None);
    cfg.compression_level = 0;
    let e = Engine::open(cfg).unwrap();
    e.create_bucket("plain").await.unwrap();
    put(&e, "plain", "a", &secret_text(50_000)).await;
    drop(e);
    assert!(on_disk(dir.path(), b"TOP-SECRET-PAYLOAD"));
    // An unencrypted store cannot be switched to encryption later.
    assert!(Engine::open(config(dir.path(), Some([1u8; 32]))).is_err());
}

fn ring_config(dir: &std::path::Path, primary: [u8; 32], old: Vec<[u8; 32]>) -> EngineConfig {
    let mut cfg = config(dir, None);
    cfg.encryption = Some(Arc::new(ChunkKeys::ring(primary, old, None)));
    cfg
}

/// Key tags of every full chunk file (bytes 1..5 of tagged chunks).
fn chunk_tags(dir: &std::path::Path) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut stack = vec![dir.join("chunks")];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            let name = e.file_name().into_string().unwrap();
            if e.file_type().unwrap().is_dir() {
                if name != "tmp" {
                    stack.push(e.path());
                }
            } else if name.len() == 64 {
                out.push(std::fs::read(e.path()).unwrap()[1..5].to_vec());
            }
        }
    }
    out
}

#[tokio::test]
async fn key_rotation_lets_the_old_key_go() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = ([0xA1u8; 32], [0xB2u8; 32]);
    let body = secret_text(150_000);
    {
        let e = Engine::open(config(dir.path(), Some(a))).unwrap();
        e.create_bucket("vault").await.unwrap();
        put(&e, "vault", "doc", &body).await;
    }
    let tag_a = ChunkKeys::new(&a).primary_tag().to_vec();
    let tag_b = ChunkKeys::new(&b).primary_tag().to_vec();
    assert!(chunk_tags(dir.path()).iter().all(|t| *t == tag_a));

    // Rotate: B is primary, A still decrypts.
    {
        let e = Engine::open(ring_config(dir.path(), b, vec![a])).unwrap();
        let fp = e.encryption_fingerprint().unwrap().to_string();
        assert_eq!(get(&e, "vault", "doc").await, body);
        put(&e, "vault", "new", b"written during rotation").await;
        let s = e.rotate_keys(true).await.unwrap();
        assert!(s.rewritten > 0 && s.done(), "{s:?}");
        assert!(chunk_tags(dir.path()).iter().all(|t| *t == tag_b));
        assert_eq!(
            e.rotate_keys(true).await.unwrap().rewritten,
            s.rewritten,
            "nothing left"
        );
        assert_eq!(get(&e, "vault", "doc").await, body);
        drop(e);
        // B alone now: same identity, everything readable.
        let e = Engine::open(config(dir.path(), Some(b))).unwrap();
        assert_eq!(
            e.encryption_fingerprint().unwrap(),
            fp,
            "identity survives rotation"
        );
        assert_eq!(get(&e, "vault", "doc").await, body);
        assert_eq!(get(&e, "vault", "new").await, b"written during rotation");
    }
    // A alone can no longer open the directory.
    assert!(Engine::open(config(dir.path(), Some(a))).is_err());
}
