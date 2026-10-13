//! Cold tier: transitions, offload, eviction, transparent reads, dedup.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use kosha_engine::*;
use parking_lot::Mutex;

#[derive(Default)]
struct MemCold {
    objects: Mutex<HashMap<String, Bytes>>,
}

/// Shares the map with the test, which inspects it.
struct Cold(Arc<MemCold>);

impl ColdStore for Cold {
    fn put(&self, key: String, data: Bytes) -> BoxFuture<'static, Result<()>> {
        self.0.objects.lock().insert(key, data);
        Box::pin(async { Ok(()) })
    }
    fn get(&self, key: String) -> BoxFuture<'static, Result<Bytes>> {
        let r = self
            .0
            .objects
            .lock()
            .get(&key)
            .cloned()
            .ok_or_else(|| Error::Corrupt(format!("cold object {key} missing")));
        Box::pin(async move { r })
    }
    fn delete(&self, key: String) -> BoxFuture<'static, Result<()>> {
        self.0.objects.lock().remove(&key);
        Box::pin(async { Ok(()) })
    }
}

fn engine(dir: &std::path::Path, key: Option<[u8; 32]>) -> (Engine, Arc<MemCold>) {
    let mut cfg = EngineConfig::new(dir);
    cfg.sync = SyncMode::None;
    cfg.chunking = ChunkerConfig {
        min: 1024,
        avg: 4096,
        max: 16384,
    };
    cfg.encryption = key.map(|k| Arc::new(ChunkKeys::new(&k)));
    let e = Engine::open(cfg).unwrap();
    let cold = Arc::new(MemCold::default());
    e.set_cold_store(Arc::new(Cold(cold.clone()))).unwrap();
    (e, cold)
}

fn data(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

async fn put(e: &Engine, b: &str, k: &str, body: &[u8]) {
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
    .unwrap();
}

async fn get(e: &Engine, b: &str, k: &str) -> Vec<u8> {
    let m = e.head_object(b, k).await.unwrap();
    let parts: Vec<_> = e.read(&m, 0, m.size).try_collect().await.unwrap();
    parts.concat()
}

fn chunk_files(dir: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path) -> usize {
        std::fs::read_dir(p)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                if e.file_type().unwrap().is_dir() {
                    if e.file_name() == "tmp" {
                        0
                    } else {
                        walk(&e.path())
                    }
                } else {
                    usize::from(e.file_name() != "ENCRYPTION")
                }
            })
            .sum()
    }
    walk(&dir.join("chunks"))
}

fn target(k: &str) -> Vec<DeleteTarget> {
    vec![DeleteTarget {
        key: k.into(),
        version_id: None,
    }]
}

#[tokio::test]
async fn transition_offloads_and_reads_back() {
    let dir = tempfile::tempdir().unwrap();
    let (e, cold) = engine(dir.path(), None);
    e.create_bucket("archive").await.unwrap();
    let body = data(300_000, 1);
    put(&e, "archive", "old.bin", &body).await;
    let local_before = chunk_files(dir.path());
    assert!(local_before > 10);

    assert_eq!(
        e.transition_objects("archive", target("old.bin"), "GLACIER_IR")
            .await
            .unwrap(),
        1
    );
    // Already cold: a second transition is a no-op.
    assert_eq!(
        e.transition_objects("archive", target("old.bin"), "GLACIER_IR")
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        e.head_object("archive", "old.bin")
            .await
            .unwrap()
            .storage_class
            .as_deref(),
        Some("GLACIER_IR")
    );
    // Data is still local until the offloader runs.
    assert_eq!(chunk_files(dir.path()), local_before);
    assert_eq!(e.run_offloader().await.unwrap(), local_before);

    assert_eq!(chunk_files(dir.path()), 0, "local copies evicted");
    assert_eq!(cold.objects.lock().len(), local_before);
    let s = e.stats().await.unwrap();
    assert_eq!((s.cold_chunks, s.physical_bytes), (local_before as u64, 0));
    assert!(s.cold_bytes > 0);
    assert_eq!(
        get(&e, "archive", "old.bin").await,
        body,
        "read through the cold tier"
    );
    // Ranged reads too.
    let m = e.head_object("archive", "old.bin").await.unwrap();
    let parts: Vec<_> = e.read(&m, 100_000, 100_100).try_collect().await.unwrap();
    assert_eq!(parts.concat(), body[100_000..100_100]);

    // Deleting the object releases the remote copies.
    e.delete_object("archive", "old.bin").await.unwrap();
    assert!(e.collect_remote_garbage().await > 0);
    assert!(cold.objects.lock().is_empty());
    let s = e.stats().await.unwrap();
    assert_eq!((s.cold_chunks, s.cold_bytes, s.chunks), (0, 0, 0));

    // Without a cold store, transitions are refused.
    let plain = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(plain.path());
    cfg.sync = SyncMode::None;
    let e2 = Engine::open(cfg).unwrap();
    e2.create_bucket("b1b").await.unwrap();
    put(&e2, "b1b", "k", b"x").await;
    assert!(
        e2.transition_objects("b1b", target("k"), "GLACIER")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn shared_chunks_stay_local_while_a_hot_object_uses_them() {
    let dir = tempfile::tempdir().unwrap();
    let (e, cold) = engine(dir.path(), None);
    e.create_bucket("mixed").await.unwrap();
    let body = data(200_000, 2);
    put(&e, "mixed", "cold.bin", &body).await;
    put(&e, "mixed", "hot.bin", &body).await; // identical: shares every chunk
    let n = chunk_files(dir.path());

    e.transition_objects("mixed", target("cold.bin"), "STANDARD_IA")
        .await
        .unwrap();
    assert_eq!(
        e.run_offloader().await.unwrap(),
        0,
        "hot.bin still uses the chunks"
    );
    assert_eq!(chunk_files(dir.path()), n);

    // Once the hot object is gone, the chunks move to the cold tier.
    e.delete_object("mixed", "hot.bin").await.unwrap();
    assert_eq!(e.run_offloader().await.unwrap(), n);
    assert_eq!(chunk_files(dir.path()), 0);
    assert_eq!(get(&e, "mixed", "cold.bin").await, body);

    // A new upload of the same content is hot again and stored locally,
    // while the cold object keeps reading.
    put(&e, "mixed", "again.bin", &body).await;
    assert_eq!(chunk_files(dir.path()), n);
    assert_eq!(get(&e, "mixed", "again.bin").await, body);
    // A copy of a cold object is STANDARD.
    let copy = e
        .copy_object(
            "mixed",
            "cold.bin",
            "mixed",
            "copy.bin",
            None,
            WriteCondition::default(),
        )
        .await
        .unwrap();
    assert_eq!(copy.storage_class, None);
    assert_eq!(get(&e, "mixed", "copy.bin").await, body);
    assert!(cold.objects.lock().len() == n);
}

#[tokio::test]
async fn cold_copies_are_encrypted_when_the_store_is() {
    let dir = tempfile::tempdir().unwrap();
    let (e, cold) = engine(dir.path(), Some([5u8; 32]));
    e.create_bucket("vault").await.unwrap();
    let body: Vec<u8> = b"CONFIDENTIAL-LEDGER-LINE "
        .iter()
        .cycle()
        .take(100_000)
        .copied()
        .collect();
    put(&e, "vault", "ledger", &body).await;
    e.transition_objects("vault", target("ledger"), "DEEP_ARCHIVE")
        .await
        .unwrap();
    assert!(e.run_offloader().await.unwrap() > 0);
    for v in cold.objects.lock().values() {
        assert!(!v.windows(12).any(|w| w == b"CONFIDENTIAL"));
    }
    assert_eq!(get(&e, "vault", "ledger").await, body);
}

#[tokio::test]
async fn lifecycle_transition_rules() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _cold) = engine(dir.path(), None);
    e.create_bucket("logs").await.unwrap();
    put(&e, "logs", "app/1.log", &data(50_000, 3)).await;
    put(&e, "logs", "keep/1.log", &data(50_000, 4)).await;
    e.set_bucket_config(
        "logs",
        BucketConfigUpdate::Lifecycle(Some(vec![LifecycleRule {
            id: Some("tier".into()),
            enabled: true,
            prefix: "app/".into(),
            tags: Default::default(),
            expiration_days: None,
            expiration_date: None,
            expired_object_delete_marker: false,
            noncurrent_days: None,
            abort_incomplete_days: None,
            transition_days: Some(30),
            transition_date: None,
            transition_class: Some("GLACIER_IR".into()),
        }])),
    )
    .await
    .unwrap();
    let soon = chrono::Utc::now() + chrono::Duration::days(1);
    assert_eq!(e.run_lifecycle(soon).await.unwrap().transitioned, 0);
    let later = chrono::Utc::now() + chrono::Duration::days(31);
    assert_eq!(e.run_lifecycle(later).await.unwrap().transitioned, 1);
    assert_eq!(
        e.run_lifecycle(later).await.unwrap().transitioned,
        0,
        "idempotent"
    );
    let page = e
        .list_objects(
            "logs",
            ListQuery {
                max_keys: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let classes: Vec<_> = page
        .objects
        .iter()
        .map(|o| (o.key.as_str(), o.storage_class.clone()))
        .collect();
    assert_eq!(
        classes,
        [
            ("app/1.log", Some("GLACIER_IR".to_string())),
            ("keep/1.log", None)
        ]
    );
}
