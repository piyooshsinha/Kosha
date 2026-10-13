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

async fn start_node(id: u64) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path().join("data"));
    cfg.sync = SyncMode::None;
    cfg.chunking = ChunkerConfig {
        min: 1024,
        avg: 4096,
        max: 16384,
    };
    cfg.min_part_size = 1;
    cfg.gc_grace = Duration::from_secs(600);
    let engine = Engine::open(cfg).unwrap();

    // Reserve a port, then hand it to the cluster listener.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut ccfg = ClusterConfig::new(id, port, "test-secret", dir.path().join("raft"));
    ccfg.api_addr = format!("127.0.0.1:{}", 9000 + id);
    ccfg.heartbeat_ms = 50;
    ccfg.election_timeout_ms = (300, 600);
    ccfg.logs_to_keep = 0;
    let cluster = Cluster::start(engine.clone(), ccfg).await.unwrap();
    Node {
        engine,
        cluster,
        addr: port.to_string(),
        _dir: dir,
    }
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

fn chunk_ids(m: &ObjectMeta) -> Vec<ChunkId> {
    m.segments
        .iter()
        .filter_map(|s| match s {
            Segment::Chunk { id, .. } => Some(*id),
            _ => None,
        })
        .collect()
}

async fn three_nodes() -> Vec<Node> {
    let nodes = vec![
        start_node(1).await,
        start_node(2).await,
        start_node(3).await,
    ];
    nodes[0].cluster.initialize().await.unwrap();
    nodes[0]
        .cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    nodes[1].cluster.join(&nodes[0].addr).await.unwrap();
    nodes[2].cluster.join(&nodes[0].addr).await.unwrap();
    for n in &nodes {
        n.cluster
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
    }
    nodes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicates_and_survives_leader_failure() {
    let nodes = three_nodes().await;
    let status = nodes[0].cluster.status();
    assert_eq!(status.nodes.iter().filter(|n| n.voter).count(), 3);

    // Write through a follower: the command is forwarded to the leader.
    let leader = nodes[0].cluster.status().leader.unwrap();
    let follower = nodes
        .iter()
        .find(|n| n.cluster.node_id() != leader)
        .unwrap();
    follower.engine.create_bucket("replicated").await.unwrap();
    let body_a = data(300_000, 1);
    let meta_a = put(&follower.engine, "replicated", "a.bin", &body_a).await;
    assert!(chunk_ids(&meta_a).len() > 10);

    // Linearizable reads on every node, including ones that did not write.
    for n in &nodes {
        assert_eq!(
            get(&n.engine, "replicated", "a.bin").await,
            body_a,
            "node {}",
            n.cluster.node_id()
        );
    }

    // Every chunk lives on exactly `replication` (2) nodes.
    for id in chunk_ids(&meta_a) {
        let copies = nodes
            .iter()
            .filter(|n| n.engine.chunk_store().has(&id))
            .count();
        assert_eq!(copies, 2, "chunk {id:?}");
    }

    // Kill the leader.
    let dead = nodes
        .iter()
        .position(|n| n.cluster.node_id() == leader)
        .unwrap();
    nodes[dead].cluster.shutdown().await;
    let alive: Vec<&Node> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != dead)
        .map(|(_, n)| n)
        .collect();
    let new_leader = alive[0]
        .cluster
        .raft()
        .wait(Some(Duration::from_secs(15)))
        .metrics(
            |m| m.current_leader.is_some_and(|l| l != leader),
            "new leader",
        )
        .await
        .unwrap()
        .current_leader
        .unwrap();
    assert_ne!(new_leader, leader);

    // Old data is still readable everywhere (one replica of each chunk survives).
    for n in &alive {
        assert_eq!(get(&n.engine, "replicated", "a.bin").await, body_a);
    }

    // The cluster keeps accepting writes with one node down.
    let body_b = data(200_000, 2);
    put(&alive[1].engine, "replicated", "b.bin", &body_b).await;
    for n in &alive {
        assert_eq!(get(&n.engine, "replicated", "b.bin").await, body_b);
        let page = n
            .engine
            .list_objects(
                "replicated",
                ListQuery {
                    max_keys: 100,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page.objects.len(), 2);
    }
    // New chunks were placed on the two live nodes.
    let meta_b = alive[0]
        .engine
        .head_object("replicated", "b.bin")
        .await
        .unwrap();
    for id in chunk_ids(&meta_b) {
        assert!(alive.iter().all(|n| n.engine.chunk_store().has(&id)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_joiner_catches_up_from_snapshot() {
    let nodes = three_nodes().await;
    let e = &nodes[0].engine;
    e.create_bucket("history").await.unwrap();
    for i in 0..20 {
        put(
            e,
            "history",
            &format!("k{i:02}"),
            format!("value {i}").as_bytes(),
        )
        .await;
    }
    let big = data(100_000, 9);
    put(e, "history", "big", &big).await;

    // Compact the log on every node so a newcomer cannot replay it.
    for n in &nodes {
        n.cluster.raft().trigger().snapshot().await.unwrap();
    }
    for n in &nodes {
        n.cluster
            .raft()
            .wait(Some(Duration::from_secs(10)))
            .metrics(|m| m.purged.is_some(), "log purged")
            .await
            .unwrap();
    }

    let late = start_node(4).await;
    late.cluster.join(&nodes[1].addr).await.unwrap();
    late.cluster
        .raft()
        .wait(Some(Duration::from_secs(10)))
        .metrics(|m| m.snapshot.is_some(), "snapshot installed")
        .await
        .unwrap();

    let page = late
        .engine
        .list_objects(
            "history",
            ListQuery {
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.objects.len(), 21);
    assert_eq!(get(&late.engine, "history", "k07").await, b"value 7");
    assert_eq!(get(&late.engine, "history", "big").await, big);
    assert_eq!(late.engine.stats().await.unwrap().objects, 21);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflicting_conditional_writes_on_different_nodes() {
    let nodes = three_nodes().await;
    nodes[0].engine.create_bucket("locks").await.unwrap();
    // Every node races to create the same key; exactly one wins.
    let mut tasks = Vec::new();
    for n in &nodes {
        let e = n.engine.clone();
        let id = n.cluster.node_id();
        tasks.push(tokio::spawn(async move {
            let mut w = e.writer();
            w.write(format!("owner {id}").as_bytes()).await.unwrap();
            let cond = WriteCondition {
                if_none_match_any: true,
                ..Default::default()
            };
            e.put_object(
                "locks",
                "leader-lock",
                w.finish().await.unwrap(),
                ObjectAttrs::default(),
                cond,
            )
            .await
            .is_ok()
        }));
    }
    let mut wins = 0;
    for t in tasks {
        wins += t.await.unwrap() as usize;
    }
    assert_eq!(wins, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_use_the_leader_lease_and_stay_linearizable() {
    let nodes = three_nodes().await;
    let leader_id = nodes[0].cluster.status().leader.unwrap();
    let leader = nodes
        .iter()
        .find(|n| n.cluster.node_id() == leader_id)
        .unwrap();
    let followers: Vec<&Node> = nodes
        .iter()
        .filter(|n| n.cluster.node_id() != leader_id)
        .collect();
    leader.engine.create_bucket("leased").await.unwrap();

    for i in 0..30 {
        // Write on one follower, read immediately on the other.
        let key = format!("k{i}");
        put(&followers[0].engine, "leased", &key, key.as_bytes()).await;
        assert_eq!(
            get(&followers[1].engine, "leased", &key).await,
            key.as_bytes()
        );
        assert_eq!(get(&leader.engine, "leased", &key).await, key.as_bytes());
    }
    let s = leader.cluster.status();
    assert!(
        s.lease_reads > s.quorum_reads,
        "lease {} vs quorum {}",
        s.lease_reads,
        s.quorum_reads
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_restores_replicas_after_a_node_dies() {
    let nodes = three_nodes().await;
    nodes[0].engine.create_bucket("durable").await.unwrap();
    let body = data(400_000, 21);
    let meta = put(&nodes[0].engine, "durable", "obj", &body).await;
    let ids = chunk_ids(&meta);
    assert!(ids.len() > 10);

    // Node 3 dies for good. Chunks it held now have a single live copy.
    nodes[2].cluster.shutdown().await;
    let alive = [&nodes[0], &nodes[1]];
    let live_copies = |id: &ChunkId| {
        alive
            .iter()
            .filter(|n| n.engine.chunk_store().has(id))
            .count()
    };
    let under: usize = ids.iter().filter(|id| live_copies(id) < 2).count();
    assert!(under > 0, "some chunks should have lost a replica");

    let mut repaired = 0;
    for n in alive {
        let r = n.cluster.repair_now().await;
        assert_eq!(r.live_voters, 2);
        assert_eq!(r.failed, 0);
        repaired += r.repaired;
    }
    assert_eq!(repaired as usize, under);
    for id in &ids {
        assert_eq!(live_copies(id), 2, "chunk {id:?} back to 2 live copies");
    }
    assert_eq!(get(&nodes[1].engine, "durable", "obj").await, body);
    // A second pass finds nothing to do.
    for n in alive {
        let r = n.cluster.repair_now().await;
        assert_eq!((r.repaired, r.surplus_removed, r.failed), (0, 0, 0));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_removes_surplus_replicas() {
    let nodes = three_nodes().await;
    nodes[0].engine.create_bucket("balance").await.unwrap();
    let body = data(200_000, 33);
    let meta = put(&nodes[0].engine, "balance", "obj", &body).await;

    // Plant a copy of every chunk on every node (as fallback writes would).
    let mut planted = 0;
    for seg in &meta.segments {
        let Segment::Chunk { id, .. } = seg else {
            continue;
        };
        let holder = nodes
            .iter()
            .find(|n| n.engine.chunk_store().has(id))
            .unwrap();
        let raw = holder
            .engine
            .chunk_store()
            .get(id, seg.len() as u32)
            .await
            .unwrap();
        for n in &nodes {
            if !n.engine.chunk_store().has(id) {
                n.engine
                    .chunk_store()
                    .put_replica(*id, raw.clone())
                    .await
                    .unwrap();
                planted += 1;
            }
        }
    }
    assert!(planted > 0);

    let mut removed = 0;
    for n in &nodes {
        removed += n.cluster.repair_now().await.surplus_removed;
    }
    assert_eq!(removed as usize, planted);
    for id in chunk_ids(&meta) {
        let copies = nodes
            .iter()
            .filter(|n| n.engine.chunk_store().has(&id))
            .count();
        assert_eq!(copies, 2);
    }
    for n in &nodes {
        assert_eq!(get(&n.engine, "balance", "obj").await, body);
    }
}

/// A batch re-delivered after the leader already committed it (a forwarding
/// reply lost in transit, then a retry) must not be applied twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retried_batches_apply_exactly_once() {
    use kosha_cluster::CommandBatch;
    let nodes = three_nodes().await;
    let leader_id = nodes[0].cluster.status().leader.unwrap();
    let leader = nodes
        .iter()
        .find(|n| n.cluster.node_id() == leader_id)
        .unwrap();
    let id = RequestId::random();
    let batch = CommandBatch(vec![Request {
        id: Some(id),
        cmd: Command::CreateBucket {
            info: BucketInfo {
                name: "exactly-once".into(),
                created: chrono::Utc::now(),
                config: Default::default(),
            },
        },
    }]);
    let first = leader
        .cluster
        .raft()
        .client_write(batch.clone())
        .await
        .unwrap();
    let retry = leader.cluster.raft().client_write(batch).await.unwrap();
    assert!(first.data.0[0].is_ok());
    assert!(
        retry.data.0[0].is_ok(),
        "retry returns the original success, not BucketAlreadyExists"
    );
    for n in &nodes {
        n.engine.bucket("exactly-once").await.unwrap();
        assert_eq!(n.engine.stats().await.unwrap().buckets, 1);
    }
}
