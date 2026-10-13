//! Erasure-coded chunks: placement, degraded reads, damaged shards, repair.

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

async fn start_node(id: u64, layout: Option<ErasureLayout>) -> Node {
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
    ccfg.erasure = layout;
    ccfg.erasure_min_len = 1024;
    let cluster = Cluster::start(engine.clone(), ccfg).await.unwrap();
    Node {
        engine,
        cluster,
        addr: port.to_string(),
        dir,
    }
}

/// chunk hex -> (node index, shard index) for every shard on disk, plus the
/// number of full chunk files.
fn inventory(nodes: &[Node]) -> (BTreeMap<String, Vec<(usize, u8)>>, usize) {
    let mut shards: BTreeMap<String, Vec<(usize, u8)>> = BTreeMap::new();
    let mut full = 0;
    for (ni, n) in nodes.iter().enumerate() {
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
                        .entry(hex.to_string())
                        .or_default()
                        .push((ni, idx.parse().unwrap()));
                } else if name.len() == 64 {
                    full += 1;
                }
            }
        }
    }
    (shards, full)
}

async fn read_all(e: &Engine, key: &str) -> Vec<u8> {
    let m = e.head_object("ecdata", key).await.unwrap();
    let parts: Vec<_> = e.read(&m, 0, m.size).try_collect().await.unwrap();
    parts.concat()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn erasure_coded_chunks() {
    let layout = ErasureLayout { k: 3, m: 2 };
    let mut nodes = Vec::new();
    for id in 1..=6 {
        nodes.push(start_node(id, Some(layout)).await);
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

    // A node with a different layout cannot join.
    let other = start_node(7, Some(ErasureLayout { k: 4, m: 2 })).await;
    let err = other.cluster.join(&nodes[0].addr).await.unwrap_err();
    assert!(err.to_string().contains("erasure coding mismatch"), "{err}");

    let e = &nodes[2].engine;
    e.create_bucket("ecdata").await.unwrap();
    // Incompressible, so the stored form is as large as the data.
    let mut x = 0x9E3779B97F4A7C15u64;
    let body: Vec<u8> = (0..600_000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    let mut w = e.writer();
    w.write(&body).await.unwrap();
    e.put_object(
        "ecdata",
        "obj",
        w.finish().await.unwrap(),
        ObjectAttrs::default(),
        WriteCondition::default(),
    )
    .await
    .unwrap();

    // Every chunk is 3+2 shards on 5 distinct nodes; no full copies.
    let (shards, full) = inventory(&nodes);
    assert_eq!(full, 0);
    assert!(shards.len() > 10, "{} chunks", shards.len());
    for (chunk, held) in &shards {
        let idx: BTreeSet<u8> = held.iter().map(|(_, i)| *i).collect();
        let on: BTreeSet<usize> = held.iter().map(|(n, _)| *n).collect();
        assert_eq!(idx, (0..5).collect(), "{chunk}");
        assert_eq!(on.len(), 5, "shards of {chunk} share a node");
    }
    // Storage overhead is (k+m)/k = 1.67x, against 2x for two replicas.
    let disk: u64 = nodes
        .iter()
        .map(|n| fs_size(&n.dir.path().join("data/chunks")))
        .sum();
    let ratio = disk as f64 / body.len() as f64;
    assert!((1.6..1.8).contains(&ratio), "overhead {ratio:.2}");

    for n in &nodes {
        assert_eq!(read_all(&n.engine, "obj").await, body);
    }

    // Damage one shard on disk: it is detected and the chunk still reads.
    // Pick data shard 0, which a healthy read would use.
    let (victim_chunk, held) = shards.iter().next().unwrap();
    let (vn, vi) = *held.iter().find(|(_, i)| *i == 0).unwrap();
    let path = nodes[vn].dir.path().join(format!(
        "data/chunks/{}/{}/{victim_chunk}.s{vi}",
        &victim_chunk[..2],
        &victim_chunk[2..4]
    ));
    let mut raw = std::fs::read(&path).unwrap();
    let last = raw.len() - 1;
    raw[last] ^= 0xff;
    std::fs::write(&path, raw).unwrap();
    assert_eq!(read_all(&nodes[(vn + 1) % 6].engine, "obj").await, body);

    // Lose a node: reads reconstruct from parity, repair rebuilds its shards.
    let dead = 4;
    nodes[dead].cluster.shutdown().await;
    for (i, n) in nodes.iter().enumerate() {
        if i != dead {
            assert_eq!(read_all(&n.engine, "obj").await, body, "read on node {i}");
        }
    }
    let mut repaired = 0;
    for (i, n) in nodes.iter().enumerate() {
        if i != dead {
            repaired += n.cluster.repair_now().await.repaired;
        }
    }
    let (after, _) = inventory(&nodes);
    let lost = shards
        .values()
        .filter(|h| h.iter().any(|(n, _)| *n == dead))
        .count();
    assert!(
        repaired as usize >= lost,
        "repaired {repaired} of {lost} lost shards"
    );
    for (chunk, held) in &after {
        let live: BTreeSet<u8> = held
            .iter()
            .filter(|(n, _)| *n != dead)
            .map(|(_, i)| *i)
            .collect();
        assert_eq!(live, (0..5).collect(), "{chunk} is back to 5 live shards");
    }

    // Deleting the object removes its shards everywhere.
    e.delete_object("ecdata", "obj").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    for n in &nodes {
        n.engine.sweep().await.unwrap();
    }
    let (left, _) = inventory(&nodes);
    let live_left: usize = left
        .values()
        .map(|h| h.iter().filter(|(n, _)| *n != dead).count())
        .sum();
    assert_eq!(live_left, 0);
}

fn fs_size(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            if e.file_type().unwrap().is_dir() {
                fs_size(&e.path())
            } else {
                e.metadata().unwrap().len()
            }
        })
        .sum()
}
