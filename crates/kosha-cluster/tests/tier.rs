//! The cold tier in a cluster: offload on the leader, eviction everywhere,
//! reads from any node, and repair leaving evicted chunks alone.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use kosha_cluster::{Cluster, ClusterConfig};
use kosha_engine::*;
use parking_lot::Mutex;

#[derive(Clone, Default)]
struct SharedCold(Arc<Mutex<HashMap<String, Bytes>>>);

impl ColdStore for SharedCold {
    fn put(&self, key: String, data: Bytes) -> BoxFuture<'static, Result<()>> {
        self.0.lock().insert(key, data);
        Box::pin(async { Ok(()) })
    }
    fn get(&self, key: String) -> BoxFuture<'static, Result<Bytes>> {
        let r = self.0.lock().get(&key).cloned().ok_or(Error::Corrupt(key));
        Box::pin(async move { r })
    }
    fn delete(&self, key: String) -> BoxFuture<'static, Result<()>> {
        self.0.lock().remove(&key);
        Box::pin(async { Ok(()) })
    }
}

struct Node {
    engine: Engine,
    cluster: Cluster,
    addr: String,
    dir: tempfile::TempDir,
}

async fn start_node(id: u64, cold: &SharedCold) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path().join("data"));
    cfg.sync = SyncMode::None;
    cfg.chunking = ChunkerConfig {
        min: 1024,
        avg: 4096,
        max: 16384,
    };
    let engine = Engine::open(cfg).unwrap();
    engine.set_cold_store(Arc::new(cold.clone())).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut ccfg = ClusterConfig::new(id, port, "test-secret", dir.path().join("raft"));
    ccfg.heartbeat_ms = 50;
    ccfg.election_timeout_ms = (300, 600);
    ccfg.repair_interval = None;
    let cluster = Cluster::start(engine.clone(), ccfg).await.unwrap();
    Node {
        engine,
        cluster,
        addr: port.to_string(),
        dir,
    }
}

fn chunk_files(n: &Node) -> usize {
    fn walk(p: &std::path::Path) -> usize {
        std::fs::read_dir(p)
            .map(|rd| {
                rd.map(|e| {
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
            })
            .unwrap_or(0)
    }
    walk(&n.dir.path().join("data/chunks"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_tier_in_a_cluster() {
    let cold = SharedCold::default();
    let nodes = vec![
        start_node(1, &cold).await,
        start_node(2, &cold).await,
        start_node(3, &cold).await,
    ];
    nodes[0].cluster.initialize().await.unwrap();
    nodes[0]
        .cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    for n in &nodes[1..] {
        n.cluster.join(&nodes[0].addr).await.unwrap();
    }
    for n in &nodes {
        n.cluster
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
    }
    let leader_id = nodes[0].cluster.status().leader.unwrap();
    let leader = nodes
        .iter()
        .find(|n| n.cluster.node_id() == leader_id)
        .unwrap();

    let e = &nodes[1].engine;
    e.create_bucket("archive").await.unwrap();
    let body: Vec<u8> = (0..250_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    let mut w = e.writer();
    w.write(&body).await.unwrap();
    e.put_object(
        "archive",
        "old",
        w.finish().await.unwrap(),
        ObjectAttrs::default(),
        WriteCondition::default(),
    )
    .await
    .unwrap();
    let before: usize = nodes.iter().map(chunk_files).sum();
    assert!(before > 20, "replicated chunks on disk");

    e.transition_objects(
        "archive",
        vec![DeleteTarget {
            key: "old".into(),
            version_id: None,
        }],
        "GLACIER_IR",
    )
    .await
    .unwrap();
    let offloaded = leader.engine.run_offloader().await.unwrap();
    assert!(offloaded > 10);
    assert_eq!(cold.0.lock().len(), offloaded);

    // Every node applies the eviction from the log.
    for _ in 0..50 {
        if nodes.iter().map(chunk_files).sum::<usize>() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(nodes.iter().map(chunk_files).sum::<usize>(), 0);

    // Any node reads it back through the cold tier.
    for n in &nodes {
        let m = n.engine.head_object("archive", "old").await.unwrap();
        assert_eq!(m.storage_class.as_deref(), Some("GLACIER_IR"));
        let parts: Vec<_> = n.engine.read(&m, 0, m.size).try_collect().await.unwrap();
        assert_eq!(parts.concat(), body);
    }

    // Repair does not pull evicted chunks back.
    let report = leader.cluster.repair_now().await;
    assert_eq!(report.repaired, 0, "{report:?}");
    assert_eq!(nodes.iter().map(chunk_files).sum::<usize>(), 0);
}
