//! Replicated while hot, erasure coded once cold.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use futures::TryStreamExt;
use kosha_cluster::{Cluster, ClusterConfig, ErasureLayout};
use kosha_engine::*;

struct Node {
    engine: Engine,
    cluster: Cluster,
    addr: String,
    dir: tempfile::TempDir,
}

async fn start_node(id: u64) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path().join("data"));
    cfg.sync = SyncMode::None;
    cfg.chunking = ChunkerConfig {
        min: 4096,
        avg: 16384,
        max: 65536,
    };
    let engine = Engine::open(cfg).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut ccfg = ClusterConfig::new(id, port, "test-secret", dir.path().join("raft"));
    ccfg.heartbeat_ms = 50;
    ccfg.election_timeout_ms = (300, 600);
    ccfg.repair_interval = None;
    ccfg.replication = 2;
    ccfg.erasure = Some(ErasureLayout { k: 3, m: 2 });
    ccfg.erasure_min_len = 1024;
    ccfg.erasure_cold_only = true;
    let cluster = Cluster::start(engine.clone(), ccfg).await.unwrap();
    Node {
        engine,
        cluster,
        addr: port.to_string(),
        dir,
    }
}

/// (full chunk files, chunk hex -> shard indices held by live nodes)
fn layout(nodes: &[Node], skip: Option<usize>) -> (usize, BTreeMap<String, BTreeSet<u8>>) {
    let mut full = 0;
    let mut shards: BTreeMap<String, BTreeSet<u8>> = BTreeMap::new();
    for (i, n) in nodes.iter().enumerate() {
        if Some(i) == skip {
            continue;
        }
        let mut stack = vec![n.dir.path().join("data/chunks")];
        while let Some(d) = stack.pop() {
            for e in std::fs::read_dir(&d).unwrap() {
                let e = e.unwrap();
                let name = e.file_name().into_string().unwrap();
                if e.file_type().unwrap().is_dir() {
                    if name != "tmp" {
                        stack.push(e.path());
                    }
                } else if let Some((hex, idx)) = name.split_once(".s") {
                    shards
                        .entry(hex.into())
                        .or_default()
                        .insert(idx.parse().unwrap());
                } else if name.len() == 64 {
                    full += 1;
                }
            }
        }
    }
    (full, shards)
}

async fn read(e: &Engine) -> Vec<u8> {
    let m = e.head_object("coldec", "video").await.unwrap();
    let parts: Vec<_> = e.read(&m, 0, m.size).try_collect().await.unwrap();
    parts.concat()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cold_chunks_become_erasure_coded() {
    let mut nodes = Vec::new();
    for id in 1..=6 {
        nodes.push(start_node(id).await);
    }
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
        .position(|n| n.cluster.node_id() == leader_id)
        .unwrap();

    let e = &nodes[1].engine;
    assert!(
        e.can_tier(),
        "erasure coding counts as a place for cold data"
    );
    e.create_bucket("coldec").await.unwrap();
    let mut x = 0x2545F4914F6CDD1Du64;
    let body: Vec<u8> = (0..500_000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    let mut w = e.writer();
    w.write(&body).await.unwrap();
    let meta = e
        .put_object(
            "coldec",
            "video",
            w.finish().await.unwrap(),
            ObjectAttrs::default(),
            WriteCondition::default(),
        )
        .await
        .unwrap();
    let chunks = meta
        .segments
        .iter()
        .filter(|s| matches!(s, Segment::Chunk { .. }))
        .count();

    // Hot: two full replicas, no shards.
    let (full, shards) = layout(&nodes, None);
    assert_eq!((full, shards.len()), (2 * chunks, 0));

    // Cold: converted to 3+2 shards, full copies dropped everywhere.
    e.transition_objects(
        "coldec",
        vec![DeleteTarget {
            key: "video".into(),
            version_id: None,
        }],
        "GLACIER_IR",
    )
    .await
    .unwrap();
    assert_eq!(nodes[leader].engine.run_offloader().await.unwrap(), chunks);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (full, shards) = layout(&nodes, None);
    assert_eq!(full, 0, "full copies dropped");
    assert_eq!(shards.len(), chunks);
    assert!(shards.values().all(|s| s.len() == 5));
    for n in &nodes {
        assert_eq!(read(&n.engine).await, body);
    }

    // Repair keeps the coded layout: nothing to do while healthy.
    for n in &nodes {
        let r = n.cluster.repair_now().await;
        assert_eq!((r.repaired, r.failed), (0, 0), "{r:?}");
    }
    assert_eq!(
        layout(&nodes, None).0,
        0,
        "repair did not bring replicas back"
    );

    // A lost node's shards are rebuilt elsewhere.
    let dead = (leader + 2) % 6;
    nodes[dead].cluster.shutdown().await;
    for (i, n) in nodes.iter().enumerate() {
        if i != dead {
            n.cluster.repair_now().await;
        }
    }
    let (_, shards) = layout(&nodes, Some(dead));
    assert!(shards.values().all(|s| s.len() == 5), "{shards:?}");
    for (i, n) in nodes.iter().enumerate() {
        if i != dead {
            assert_eq!(read(&n.engine).await, body);
        }
    }
}
