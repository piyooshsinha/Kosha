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
