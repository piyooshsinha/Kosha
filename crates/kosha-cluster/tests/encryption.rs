//! Encrypted clusters: replication with keyed chunk ids, and key checks on join.

use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use kosha_cluster::{Cluster, ClusterConfig};
use kosha_engine::*;

struct Node {
    engine: Engine,
    cluster: Cluster,
    addr: String,
    _dir: tempfile::TempDir,
}

async fn start_node(id: u64, key: Option<[u8; 32]>) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path().join("data"));
    cfg.sync = SyncMode::None;
    cfg.encryption = key.map(|k| Arc::new(ChunkKeys::new(&k)));
    cfg.chunking = ChunkerConfig {
        min: 1024,
        avg: 4096,
        max: 16384,
    };
    let engine = Engine::open(cfg).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut ccfg = ClusterConfig::new(id, port, "test-secret", dir.path().join("raft"));
    ccfg.heartbeat_ms = 50;
    ccfg.election_timeout_ms = (300, 600);
    let cluster = Cluster::start(engine.clone(), ccfg).await.unwrap();
    Node {
        engine,
        cluster,
        addr: port.to_string(),
        _dir: dir,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encrypted_cluster_replicates_and_refuses_other_keys() {
    let key = [9u8; 32];
    let n1 = start_node(1, Some(key)).await;
    let n2 = start_node(2, Some(key)).await;
    n1.cluster.initialize().await.unwrap();
    n1.cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    n2.cluster.join(&n1.addr).await.unwrap();
    n2.cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    n2.engine.create_bucket("enc").await.unwrap();
    let body: Vec<u8> = (0..400_000u32)
        .map(|i| (i % 253) as u8 ^ (i >> 9) as u8)
        .collect();
    let mut w = n2.engine.writer();
    w.write(&body).await.unwrap();
    let meta = n2
        .engine
        .put_object(
            "enc",
            "k",
            w.finish().await.unwrap(),
            ObjectAttrs::default(),
            WriteCondition::default(),
        )
        .await
        .unwrap();
    // Chunk ids are keyed: each is the keyed hash of its bytes, never the
    // plain BLAKE3 that anyone could compute.
    let keys = ChunkKeys::new(&key);
    let mut off = 0usize;
    let mut chunks = 0;
    for seg in &meta.segments {
        if let Segment::Chunk { id, len } = seg {
            let bytes = &body[off..off + *len as usize];
            assert_eq!(*id, keys.chunk_id(bytes));
            assert_ne!(*id, ChunkId::of(bytes));
            off += *len as usize;
            chunks += 1;
        }
    }
    assert!(chunks > 1 && off == body.len());
    let parts: Vec<_> = n1
        .engine
        .read(&meta, 0, meta.size)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(parts.concat(), body);

    for (id, key) in [(3, None), (4, Some([1u8; 32]))] {
        let other = start_node(id, key).await;
        let err = other.cluster.join(&n1.addr).await.unwrap_err();
        assert!(err.to_string().contains("encryption mismatch"), "{err}");
    }
    assert_eq!(n1.cluster.status().nodes.len(), 2);
}
