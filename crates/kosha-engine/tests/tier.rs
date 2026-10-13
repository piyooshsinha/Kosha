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
            noncurrent_transition_days: None,
            noncurrent_transition_class: None,
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

#[tokio::test]
async fn noncurrent_versions_move_to_the_cold_tier() {
    let dir = tempfile::tempdir().unwrap();
    let (e, cold) = engine(dir.path(), None);
    e.create_bucket("docs").await.unwrap();
    e.set_bucket_config("docs", BucketConfigUpdate::Versioning(Versioning::Enabled))
        .await
        .unwrap();
    let v1 = data(80_000, 7);
    let v2 = data(80_000, 8);
    put(&e, "docs", "report", &v1).await;
    put(&e, "docs", "report", &v2).await;
    e.set_bucket_config(
        "docs",
        BucketConfigUpdate::Lifecycle(Some(vec![LifecycleRule {
            id: Some("old-versions".into()),
            enabled: true,
            prefix: String::new(),
            tags: Default::default(),
            expiration_days: None,
            expiration_date: None,
            expired_object_delete_marker: false,
            noncurrent_days: None,
            abort_incomplete_days: None,
            transition_days: None,
            transition_date: None,
            transition_class: None,
            noncurrent_transition_days: Some(7),
            noncurrent_transition_class: Some("STANDARD_IA".into()),
        }])),
    )
    .await
    .unwrap();
    let r = e
        .run_lifecycle(chrono::Utc::now() + chrono::Duration::days(3))
        .await
        .unwrap();
    assert_eq!(r.noncurrent_transitioned, 0, "not noncurrent long enough");
    let r = e
        .run_lifecycle(chrono::Utc::now() + chrono::Duration::days(8))
        .await
        .unwrap();
    assert_eq!(r.noncurrent_transitioned, 1);
    let r = e
        .run_lifecycle(chrono::Utc::now() + chrono::Duration::days(8))
        .await
        .unwrap();
    assert_eq!(r.noncurrent_transitioned, 0, "idempotent");

    let versions = e
        .list_versions(
            "docs",
            VersionListQuery {
                max_keys: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .versions;
    let classes: Vec<_> = versions
        .iter()
        .map(|v| (v.is_latest, v.meta.as_ref().unwrap().storage_class.clone()))
        .collect();
    assert_eq!(classes, [(true, None), (false, Some("STANDARD_IA".into()))]);
    // Only the old version's chunks leave; both versions still read.
    assert!(e.run_offloader().await.unwrap() > 0);
    assert!(!cold.objects.lock().is_empty());
    let old = versions[1].version_id.clone();
    let m = e
        .head_version("docs", "report", &old)
        .await
        .unwrap()
        .meta
        .unwrap();
    let parts: Vec<_> = e.read(&m, 0, m.size).try_collect().await.unwrap();
    assert_eq!(parts.concat(), v1);
    assert_eq!(get(&e, "docs", "report").await, v2);
}

#[tokio::test]
async fn access_based_tiering_demotes_unread_objects_and_promotes_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let (e, cold) = engine(dir.path(), None);
    e.create_bucket("media").await.unwrap();
    let unread = data(120_000, 11);
    let popular = data(120_000, 12);
    put(&e, "media", "unread.bin", &unread).await;
    put(&e, "media", "popular.bin", &popular).await;
    e.set_bucket_config(
        "media",
        BucketConfigUpdate::Tiering(Some(vec![TieringRule {
            id: "auto".into(),
            enabled: true,
            prefix: String::new(),
            tags: Default::default(),
            days: 30,
            access_tier: "ARCHIVE_ACCESS".into(),
        }])),
    )
    .await
    .unwrap();

    // popular.bin was read 25 days into the future we simulate.
    let now = chrono::Utc::now();
    e.note_access_at("media", "popular.bin", now + chrono::Duration::days(25));
    assert_eq!(e.flush_access().await.unwrap(), 1);

    let r = e
        .run_lifecycle(now + chrono::Duration::days(40))
        .await
        .unwrap();
    assert_eq!(r.auto_tiered, 1);
    let m = e.head_object("media", "unread.bin").await.unwrap();
    assert_eq!(
        (m.storage_class.as_deref(), m.auto_tiered),
        (Some("INTELLIGENT_TIERING"), true)
    );
    assert_eq!(
        e.head_object("media", "popular.bin")
            .await
            .unwrap()
            .storage_class,
        None
    );

    let local_before = chunk_files(dir.path());
    let offloaded = e.run_offloader().await.unwrap();
    assert!(offloaded > 0);
    assert_eq!(chunk_files(dir.path()), local_before - offloaded);
    let s = e.stats().await.unwrap();
    // Random data is stored raw: one codec byte per chunk on top.
    let stored = s.unique_bytes + s.chunks;
    assert_eq!(s.physical_bytes + s.cold_bytes, stored, "{s:?}");

    // Reading it brings it back: local copies again, STANDARD class.
    assert_eq!(get(&e, "media", "unread.bin").await, unread);
    assert!(e.promote_object("media", "unread.bin", None).await.unwrap());
    let m = e.head_object("media", "unread.bin").await.unwrap();
    assert_eq!((m.storage_class, m.auto_tiered), (None, false));
    assert_eq!(chunk_files(dir.path()), local_before);
    let s = e.stats().await.unwrap();
    assert_eq!(
        s.physical_bytes,
        s.unique_bytes + s.chunks,
        "every chunk is local again: {s:?}"
    );
    // The remote copy is kept, so demoting again needs no upload.
    assert_eq!(s.cold_chunks as usize, offloaded);
    assert!(
        !e.promote_object("media", "unread.bin", None).await.unwrap(),
        "already STANDARD"
    );

    // Explicitly cold objects are not promoted by reads.
    // (Seeds are OR-ed with 1, so 13 would repeat popular.bin's data.)
    put(&e, "media", "archive.bin", &data(50_000, 20)).await;
    e.transition_objects(
        "media",
        vec![DeleteTarget {
            key: "archive.bin".into(),
            version_id: None,
        }],
        "GLACIER",
    )
    .await
    .unwrap();
    assert!(
        !e.promote_object("media", "archive.bin", None)
            .await
            .unwrap()
    );
    assert!(e.run_offloader().await.unwrap() > 0);

    // Demoting the promoted object again evicts it without re-uploading:
    // only popular.bin's chunks are new in the cold store.
    let r = e
        .run_lifecycle(now + chrono::Duration::days(80))
        .await
        .unwrap();
    assert_eq!(r.auto_tiered, 2, "unread.bin and popular.bin");
    let popular_chunks = e
        .head_object("media", "popular.bin")
        .await
        .unwrap()
        .segments
        .iter()
        .filter(|s| matches!(s, Segment::Chunk { .. }))
        .count();
    let uploaded_before = cold.objects.lock().len();
    assert_eq!(e.run_offloader().await.unwrap(), popular_chunks);
    assert_eq!(cold.objects.lock().len(), uploaded_before + popular_chunks);
    assert_eq!(chunk_files(dir.path()), 0, "everything is cold now");
    assert_eq!(get(&e, "media", "unread.bin").await, unread);
    assert_eq!(get(&e, "media", "popular.bin").await, popular);
    let s = e.stats().await.unwrap();
    assert_eq!(
        (s.physical_bytes, s.cold_bytes),
        (0, s.unique_bytes + s.chunks),
        "{s:?}"
    );
}

#[tokio::test]
async fn key_rotation_re_encrypts_cold_copies() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = ([0x0Au8; 32], [0x0Bu8; 32]);
    let cold = Arc::new(MemCold::default());
    let open = |keys: ChunkKeys| {
        let mut cfg = EngineConfig::new(dir.path());
        cfg.sync = SyncMode::None;
        cfg.chunking = ChunkerConfig {
            min: 1024,
            avg: 4096,
            max: 16384,
        };
        cfg.encryption = Some(Arc::new(keys));
        let e = Engine::open(cfg).unwrap();
        e.set_cold_store(Arc::new(Cold(cold.clone()))).unwrap();
        e
    };
    let body = data(100_000, 31);
    {
        let e = open(ChunkKeys::new(&a));
        e.create_bucket("arch").await.unwrap();
        put(&e, "arch", "x", &body).await;
        e.transition_objects("arch", target("x"), "GLACIER")
            .await
            .unwrap();
        assert!(e.run_offloader().await.unwrap() > 0);
    }
    let before: Vec<String> = cold.objects.lock().keys().cloned().collect();
    {
        let e = open(ChunkKeys::ring(b, vec![a], None));
        let s = e.rotate_keys(true).await.unwrap();
        assert_eq!(
            (s.remote_pending, s.rewritten as usize),
            (0, before.len()),
            "{s:?}"
        );
        e.collect_remote_garbage().await;
        let after: Vec<String> = cold.objects.lock().keys().cloned().collect();
        assert_eq!(after.len(), before.len());
        assert!(
            after.iter().all(|k| !before.contains(k)),
            "old copies deleted"
        );
    }
    // B alone reads the cold data.
    let e = open(ChunkKeys::new(&b));
    assert_eq!(get(&e, "arch", "x").await, body);
}
