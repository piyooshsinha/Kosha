//! Kosha clustering: Raft-replicated metadata and rendezvous-placed chunks.
//!
//! * **Metadata** goes through a Raft log (openraft). Every node applies
//!   the same command sequence to its own redb state machine.
//! * **Writes** are proposed to the leader. A follower forwards them and then
//!   waits until it has applied the entry itself, so a client always reads its
//!   own writes on whichever node it talks to.
//! * **Reads** are linearizable via ReadIndex: the leader confirms its
//!   leadership with a quorum and returns its commit index, and the reading
//!   node waits until it has applied that index.
//! * **Chunks** are stored on `replication` nodes picked by rendezvous
//!   hashing of the chunk id over the voters. Unreachable nodes are skipped
//!   (the next-ranked node takes the replica), reads try nodes in rank order
//!   and verify content hashes, and a node that should hold a chunk but does
//!   not repairs itself on read.

// openraft's trait signatures fix the (large) `StorageError` type.
#![allow(clippy::result_large_err)]

mod ec;
mod erasure;
mod lease;
mod log_store;
mod network;
mod repair;
mod rpc;
mod server;
mod state_machine;
pub mod tls;
mod types;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use kosha_engine::{ChunkId, ChunkPlacement, Command, CommandResult, Consensus, Engine};
use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
use openraft::{ChangeMembers, ServerState};
use serde::Serialize;

pub use crate::erasure::Layout as ErasureLayout;
pub use crate::repair::RepairReport;
use crate::rpc::{Client, JoinRequest, LeaderReply, Proposed};
use crate::types::*;
pub use crate::types::{CommandBatch, KoshaNode, NodeId};

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub node_id: NodeId,
    /// Bind address for internal traffic (Raft, chunk replication).
    pub listen: SocketAddr,
    /// Address other nodes use to reach `listen` (`host:port`).
    pub advertise: String,
    /// This node's public S3 address, shown in cluster status.
    pub api_addr: String,
    /// Shared secret authenticating internal requests.
    pub secret: String,
    /// Copies of each chunk.
    pub replication: usize,
    /// Directory for the Raft log and snapshots.
    pub data_dir: PathBuf,
    pub heartbeat_ms: u64,
    pub election_timeout_ms: (u64, u64),
    /// Build a snapshot after this many new log entries.
    pub snapshot_every: u64,
    /// Log entries kept behind the latest snapshot (lets slow followers catch
    /// up from the log instead of a full snapshot).
    pub logs_to_keep: u64,
    /// Serve linearizable reads from a leader lease instead of a quorum
    /// round-trip per read (see `lease.rs`).
    pub lease_reads: bool,
    /// Run background re-replication/rebalancing this often (`None` disables).
    pub repair_interval: Option<Duration>,
    /// Mutual TLS for internal traffic. Every node presents `cert` and
    /// accepts only peers whose certificates `ca` signed.
    pub tls: Option<tls::TlsFiles>,
    /// Erasure-code chunks of at least `erasure_min_len` bytes as K+M
    /// shards instead of replicating them (needs K+M voters).
    pub erasure: Option<erasure::Layout>,
    pub erasure_min_len: usize,
    /// Write chunks replicated and erasure code them only once they are
    /// cold (all their objects in a cold storage class).
    pub erasure_cold_only: bool,
}

impl ClusterConfig {
    pub fn new(
        node_id: NodeId,
        listen: SocketAddr,
        secret: impl Into<String>,
        data_dir: impl Into<PathBuf>,
    ) -> Self {
        ClusterConfig {
            node_id,
            listen,
            advertise: listen.to_string(),
            api_addr: String::new(),
            secret: secret.into(),
            replication: 2,
            data_dir: data_dir.into(),
            heartbeat_ms: 250,
            election_timeout_ms: (1000, 2000),
            snapshot_every: 10_000,
            logs_to_keep: 1_000,
            lease_reads: true,
            repair_interval: Some(Duration::from_secs(60)),
            tls: None,
            erasure: None,
            erasure_min_len: 128 * 1024,
            erasure_cold_only: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("{0}")]
    Storage(#[from] StorageError),
    #[error("raft: {0}")]
    Raft(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("engine: {0}")]
    Engine(#[from] kosha_engine::Error),
    #[error("{0}")]
    Join(String),
}

pub(crate) struct Inner {
    pub raft: Raft,
    pub engine: Engine,
    pub cfg: ClusterConfig,
    pub client: Client,
    pub lease: lease::ReadLease,
    pub repair_stats: repair::RepairStats,
}

/// A running cluster member.
#[derive(Clone)]
pub struct Cluster {
    inner: Arc<Inner>,
    server: Arc<tokio::task::JoinHandle<()>>,
    repairer: Option<Arc<tokio::task::JoinHandle<()>>>,
}

const FORWARD_TIMEOUT: Duration = Duration::from_secs(30);
const APPLY_WAIT: Duration = Duration::from_secs(30);
const RETRIES: u32 = 50;
/// How long a write or linearizable read waits for a leader and a quorum
/// before failing with `Unavailable` (HTTP 503), so clients are not left
/// hanging while the cluster has lost its majority.
const QUORUM_DEADLINE: Duration = Duration::from_secs(10);

fn backoff(attempt: u32) -> Duration {
    Duration::from_millis((50 * (attempt as u64 + 1)).min(500))
}

impl Cluster {
    /// Start Raft and the internal endpoint, and route the engine's metadata
    /// and chunks through the cluster.
    pub async fn start(engine: Engine, cfg: ClusterConfig) -> Result<Cluster, ClusterError> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        let log = log_store::LogStore::open(&cfg.data_dir.join("raft-log.redb"))?;
        let sm = state_machine::StateMachine::open(
            engine.clone(),
            cfg.data_dir.join("raft-snapshot.bin"),
        )?;
        if cfg.tls.as_ref().is_some_and(|t| t.ca.is_none()) {
            return Err(ClusterError::Join(
                "cluster TLS needs a CA to verify peers".into(),
            ));
        }
        let client = Client::new(
            cfg.secret.clone(),
            cfg.tls.as_ref().map(|t| t.client_config()).transpose()?,
        );
        let server_tls = cfg.tls.as_ref().map(|t| t.server_config()).transpose()?;

        let raft_cfg = openraft::Config {
            cluster_name: "kosha".into(),
            heartbeat_interval: cfg.heartbeat_ms,
            election_timeout_min: cfg.election_timeout_ms.0,
            election_timeout_max: cfg.election_timeout_ms.1,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(cfg.snapshot_every),
            max_in_snapshot_log_to_keep: cfg.logs_to_keep,
            install_snapshot_timeout: 10_000,
            ..Default::default()
        }
        .validate()
        .map_err(|e| ClusterError::Raft(e.to_string()))?;

        let raft = Raft::new(
            cfg.node_id,
            Arc::new(raft_cfg),
            network::NetworkFactory {
                client: client.clone(),
            },
            log,
            sm,
        )
        .await
        .map_err(|e| ClusterError::Raft(e.to_string()))?;

        let inner = Arc::new(Inner {
            raft,
            engine: engine.clone(),
            cfg: cfg.clone(),
            client,
            lease: lease::ReadLease::new(cfg.lease_reads, cfg.election_timeout_ms.0),
            repair_stats: Default::default(),
        });
        let repairer = cfg
            .repair_interval
            .map(|every| Arc::new(repair::spawn(Arc::downgrade(&inner), every)));
        engine.attach_cluster(
            Arc::new(RaftConsensus {
                inner: inner.clone(),
                proposer: Arc::new(Proposer::start(Arc::downgrade(&inner))),
            }),
            Arc::new(RendezvousPlacement {
                inner: inner.clone(),
            }),
        )?;

        let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
        let app = server::router(inner.clone());
        let server = match server_tls {
            Some(tls) => {
                let listener = tls::TlsListener::new(listener, tls)?;
                tokio::spawn(async move {
                    if let Err(e) = axum::serve(listener, app).await {
                        tracing::error!(error = %e, "cluster endpoint stopped");
                    }
                })
            }
            None => tokio::spawn(async move {
                use axum::serve::ListenerExt;
                // Small chunk transfers would otherwise wait on Nagle.
                let listener = listener.tap_io(|tcp| {
                    let _ = tcp.set_nodelay(true);
                });
                if let Err(e) = axum::serve(listener, app).await {
                    tracing::error!(error = %e, "cluster endpoint stopped");
                }
            }),
        };
        tracing::info!(node = cfg.node_id, listen = %cfg.listen, "cluster endpoint ready");
        Ok(Cluster {
            inner,
            server: Arc::new(server),
            repairer,
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.inner.cfg.node_id
    }

    pub fn raft(&self) -> &Raft {
        &self.inner.raft
    }

    fn self_node(&self) -> KoshaNode {
        KoshaNode {
            addr: self.inner.cfg.advertise.clone(),
            api: self.inner.cfg.api_addr.clone(),
        }
    }

    pub async fn is_initialized(&self) -> bool {
        self.inner.raft.is_initialized().await.unwrap_or(false)
    }

    /// Bootstrap a new single-member cluster (no-op if already initialized).
    pub async fn initialize(&self) -> Result<(), ClusterError> {
        if self.is_initialized().await {
            return Ok(());
        }
        let members = BTreeMap::from([(self.node_id(), self.self_node())]);
        self.inner
            .raft
            .initialize(members)
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))
    }

    /// Ask an existing member (any node; it redirects to the leader) to add
    /// this node as a voter. No-op if this node already has cluster state.
    pub async fn join(&self, seed: &str) -> Result<(), ClusterError> {
        if self.is_initialized().await {
            return Ok(());
        }
        let req = JoinRequest {
            id: self.node_id(),
            node: self.self_node(),
            encryption: self
                .inner
                .engine
                .encryption_fingerprint()
                .map(str::to_string),
            erasure: self.inner.cfg.erasure,
        };
        let mut target = seed.to_string();
        let mut last_err = String::new();
        for attempt in 0..RETRIES {
            match self
                .inner
                .client
                .call::<_, LeaderReply<Result<(), String>>>(
                    &target,
                    "/cluster/join",
                    &req,
                    FORWARD_TIMEOUT,
                )
                .await
            {
                Ok(LeaderReply::Ok(Ok(()))) => {
                    tracing::info!(node = self.node_id(), "joined cluster");
                    return Ok(());
                }
                Ok(LeaderReply::Ok(Err(refused))) => return Err(ClusterError::Join(refused)),
                Ok(LeaderReply::NotLeader(Some((_, leader)))) => target = leader.addr,
                Ok(LeaderReply::NotLeader(None)) => last_err = "no leader elected yet".into(),
                Err(e) => last_err = e.to_string(),
            }
            tokio::time::sleep(backoff(attempt)).await;
        }
        Err(ClusterError::Join(last_err))
    }

    /// Remove a node from the voter set (run on any member).
    /// Remove a voter from the cluster. Works from any member (forwarded to
    /// the leader). Its chunks are re-replicated by background repair.
    pub async fn remove_node(&self, id: NodeId) -> Result<(), ClusterError> {
        let mut last = String::from("no leader");
        for attempt in 0..RETRIES {
            match self.inner.remove_voter(id).await {
                Ok(LeaderReply::Ok(Ok(()))) => return Ok(()),
                Ok(LeaderReply::Ok(Err(msg))) => return Err(ClusterError::Join(msg)),
                Ok(LeaderReply::NotLeader(Some((_, leader)))) => {
                    match self
                        .inner
                        .client
                        .call::<_, LeaderReply<Result<(), String>>>(
                            &leader.addr,
                            "/cluster/remove",
                            &id,
                            FORWARD_TIMEOUT,
                        )
                        .await
                    {
                        Ok(LeaderReply::Ok(Ok(()))) => return Ok(()),
                        Ok(LeaderReply::Ok(Err(msg))) => return Err(ClusterError::Join(msg)),
                        Ok(LeaderReply::NotLeader(_)) => last = "leader changed".into(),
                        Err(e) => last = e.to_string(),
                    }
                }
                Ok(LeaderReply::NotLeader(None)) => last = "no leader elected".into(),
                Err(e) => last = e,
            }
            tokio::time::sleep(backoff(attempt)).await;
        }
        Err(ClusterError::Raft(last))
    }

    /// Run one re-replication/rebalancing pass now.
    pub async fn repair_now(&self) -> RepairReport {
        self.inner.repair_round().await
    }

    pub fn status(&self) -> ClusterStatus {
        status(&self.inner)
    }

    /// Cluster gauges and counters in Prometheus text format.
    pub fn prometheus(&self) -> String {
        use std::fmt::Write;
        let s = self.status();
        let mut o = String::new();
        let mut g = |name: &str, kind: &str, help: &str, v: f64| {
            let _ = writeln!(o, "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {v}");
        };
        g(
            "kosha_raft_term",
            "gauge",
            "Current Raft term.",
            s.term as f64,
        );
        g(
            "kosha_raft_is_leader",
            "gauge",
            "1 if this node is the Raft leader.",
            (s.leader == Some(s.node_id)) as u8 as f64,
        );
        g(
            "kosha_raft_has_leader",
            "gauge",
            "1 if this node knows a leader.",
            s.leader.is_some() as u8 as f64,
        );
        g(
            "kosha_raft_last_log_index",
            "gauge",
            "Last Raft log index on this node.",
            s.last_log_index.unwrap_or(0) as f64,
        );
        g(
            "kosha_raft_last_applied_index",
            "gauge",
            "Last log index applied to the state machine.",
            s.last_applied.unwrap_or(0) as f64,
        );
        g(
            "kosha_raft_snapshot_index",
            "gauge",
            "Log index of the latest snapshot.",
            s.snapshot_index.unwrap_or(0) as f64,
        );
        g(
            "kosha_cluster_voters",
            "gauge",
            "Voting members.",
            s.nodes.iter().filter(|n| n.voter).count() as f64,
        );
        g(
            "kosha_read_barriers_lease_total",
            "counter",
            "Read barriers served from the leader lease.",
            s.lease_reads as f64,
        );
        g(
            "kosha_read_barriers_quorum_total",
            "counter",
            "Read barriers that needed a quorum round-trip.",
            s.quorum_reads as f64,
        );
        g(
            "kosha_repair_rounds_total",
            "counter",
            "Background repair passes.",
            s.repair.rounds as f64,
        );
        g(
            "kosha_repair_replicas_restored_total",
            "counter",
            "Chunk replicas restored by repair.",
            s.repair.repaired as f64,
        );
        g(
            "kosha_repair_surplus_removed_total",
            "counter",
            "Surplus chunk replicas removed.",
            s.repair.surplus_removed as f64,
        );
        g(
            "kosha_repair_failures_total",
            "counter",
            "Chunks repair could not fix.",
            s.repair.failed as f64,
        );
        if let Some(last) = s.last_log_index {
            let _ = writeln!(
                o,
                "# HELP kosha_raft_follower_lag_entries Entries a follower is behind (leader only).\n# TYPE kosha_raft_follower_lag_entries gauge"
            );
            for n in s.nodes.iter().filter(|n| n.matched.is_some()) {
                let _ = writeln!(
                    o,
                    "kosha_raft_follower_lag_entries{{node=\"{}\"}} {}",
                    n.id,
                    last.saturating_sub(n.matched.unwrap_or(0))
                );
            }
        }
        o
    }

    /// Wait until some node is leader (useful at startup and in tests).
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<NodeId, ClusterError> {
        let m = self
            .inner
            .raft
            .wait(Some(timeout))
            .metrics(|m| m.current_leader.is_some(), "leader elected")
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;
        Ok(m.current_leader.expect("checked"))
    }

    /// Stop Raft and the internal endpoint (simulates a node failure in tests).
    pub async fn shutdown(&self) {
        self.server.abort();
        if let Some(r) = &self.repairer {
            r.abort();
        }
        let _ = self.inner.raft.shutdown().await;
    }
}

// ------------------------------------------------------------------ status

#[derive(Clone, Debug, Serialize)]
pub struct NodeStatus {
    pub id: NodeId,
    pub addr: String,
    pub api: String,
    pub voter: bool,
    /// Highest log index known replicated to this node (leader view only).
    pub matched: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterStatus {
    pub node_id: NodeId,
    pub state: String,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub last_log_index: Option<u64>,
    pub last_applied: Option<u64>,
    pub snapshot_index: Option<u64>,
    pub replication_factor: usize,
    pub nodes: Vec<NodeStatus>,
    /// Read barriers served from the leader lease / by a quorum round-trip
    /// (counted on the leader).
    pub lease_reads: u64,
    pub quorum_reads: u64,
    pub repair: RepairStatus,
}

/// Cumulative background repair counters on this node.
#[derive(Clone, Debug, Serialize)]
pub struct RepairStatus {
    pub rounds: u64,
    pub scanned: u64,
    pub repaired: u64,
    pub surplus_removed: u64,
    pub failed: u64,
    pub last_round_ms: u64,
}

fn status(inner: &Inner) -> ClusterStatus {
    let m = inner.raft.metrics().borrow().clone();
    let membership = m.membership_config.membership();
    let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
    let nodes = membership
        .nodes()
        .map(|(id, n)| NodeStatus {
            id: *id,
            addr: n.addr.clone(),
            api: n.api.clone(),
            voter: voters.contains(id),
            matched: m
                .replication
                .as_ref()
                .and_then(|r| r.get(id).cloned().flatten())
                .map(|l| l.index),
        })
        .collect();
    ClusterStatus {
        node_id: m.id,
        state: match m.state {
            ServerState::Leader => "leader",
            ServerState::Follower => "follower",
            ServerState::Candidate => "candidate",
            ServerState::Learner => "learner",
            ServerState::Shutdown => "shutdown",
        }
        .into(),
        term: m.current_term,
        leader: m.current_leader,
        last_log_index: m.last_log_index,
        last_applied: m.last_applied.map(|l| l.index),
        snapshot_index: m.snapshot.map(|l| l.index),
        replication_factor: inner.cfg.replication,
        nodes,
        lease_reads: inner
            .lease
            .lease_reads
            .load(std::sync::atomic::Ordering::Relaxed),
        quorum_reads: inner
            .lease
            .quorum_reads
            .load(std::sync::atomic::Ordering::Relaxed),
        repair: {
            use std::sync::atomic::Ordering::Relaxed;
            let r = &inner.repair_stats;
            RepairStatus {
                rounds: r.rounds.load(Relaxed),
                scanned: r.scanned.load(Relaxed),
                repaired: r.repaired.load(Relaxed),
                surplus_removed: r.surplus_removed.load(Relaxed),
                failed: r.failed.load(Relaxed),
                last_round_ms: r.last_round_ms.load(Relaxed),
            }
        },
    }
}

// --------------------------------------------------------------- consensus

struct RaftConsensus {
    inner: Arc<Inner>,
    proposer: Arc<Proposer>,
}

fn raft_err(e: impl std::fmt::Display) -> kosha_engine::Error {
    kosha_engine::Error::Internal(format!("raft: {e}"))
}

impl Inner {
    /// Leader-side voter removal with guardrails. `Ok(Ok)` on success,
    /// `Ok(Err(msg))` for a refused request, `NotLeader` to redirect.
    pub(crate) async fn remove_voter(
        &self,
        id: NodeId,
    ) -> Result<LeaderReply<Result<(), String>>, String> {
        let voters: BTreeSet<NodeId> = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect();
        if !voters.contains(&id) {
            return Ok(LeaderReply::Ok(Err(format!("node {id} is not a voter"))));
        }
        if voters.len() == 1 {
            return Ok(LeaderReply::Ok(Err("cannot remove the last voter".into())));
        }
        match self
            .raft
            .change_membership(ChangeMembers::RemoveVoters(BTreeSet::from([id])), false)
            .await
        {
            Ok(_) => {
                tracing::info!(node = id, "removed node from the cluster");
                Ok(LeaderReply::Ok(Ok(())))
            }
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                Ok(LeaderReply::NotLeader(f.leader_id.zip(f.leader_node)))
            }
            Err(e) => Err(e.to_string()),
        }
    }

    async fn wait_applied(&self, index: u64) -> kosha_engine::Result<()> {
        self.raft
            .wait(Some(APPLY_WAIT))
            .applied_index_at_least(Some(index), "local apply")
            .await
            .map(drop)
            .map_err(raft_err)
    }

    /// Replicate a batch of commands as one Raft entry. On a follower the
    /// batch is forwarded to the leader in one RPC, then the follower waits
    /// until it has applied the entry itself (read-your-writes).
    async fn propose_batch(&self, cmds: Vec<Command>) -> kosha_engine::Result<Vec<CommandResult>> {
        // Ids are fixed before the first attempt: every retry below re-sends
        // the same ids, so a batch the leader already committed is not
        // applied twice.
        let batch = CommandBatch(
            cmds.into_iter()
                .map(|cmd| kosha_engine::Request {
                    id: Some(kosha_engine::RequestId::random()),
                    cmd,
                })
                .collect(),
        );
        let mut last = String::from("no leader");
        let deadline = tokio::time::Instant::now() + QUORUM_DEADLINE;
        for attempt in 0..RETRIES {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                break;
            }
            let Ok(written) =
                tokio::time::timeout(left, self.raft.client_write(batch.clone())).await
            else {
                last = "timed out waiting for a quorum".into();
                break;
            };
            match written {
                Ok(resp) => return Ok(resp.data.0),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                    if let Some(leader) = f.leader_node {
                        match self
                            .client
                            .call::<_, LeaderReply<Proposed>>(
                                &leader.addr,
                                "/raft/propose",
                                &batch,
                                FORWARD_TIMEOUT.min(left),
                            )
                            .await
                        {
                            Ok(LeaderReply::Ok(p)) => {
                                self.wait_applied(p.index).await?;
                                return Ok(p.results);
                            }
                            Ok(LeaderReply::NotLeader(_)) => last = "leader changed".into(),
                            Err(e) => last = e.to_string(),
                        }
                    }
                }
                Err(e) => last = e.to_string(),
            }
            tokio::time::sleep(backoff(attempt)).await;
        }
        Err(kosha_engine::Error::Unavailable(format!(
            "cluster unavailable for writes: {last}"
        )))
    }

    async fn read_barrier(&self) -> kosha_engine::Result<()> {
        let mut last = String::from("no leader");
        let deadline = tokio::time::Instant::now() + QUORUM_DEADLINE;
        for attempt in 0..RETRIES {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                break;
            }
            let Ok(index) = tokio::time::timeout(left, self.leader_read_index()).await else {
                last = "timed out waiting for a quorum".into();
                break;
            };
            match index {
                Ok(index) => {
                    if let Some(i) = index {
                        self.wait_applied(i).await?;
                    }
                    return Ok(());
                }
                Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => {
                    if let Some(leader) = f.leader_node {
                        match self
                            .client
                            .call::<_, LeaderReply<Option<u64>>>(
                                &leader.addr,
                                "/raft/read-index",
                                &(),
                                FORWARD_TIMEOUT.min(left),
                            )
                            .await
                        {
                            Ok(LeaderReply::Ok(index)) => {
                                if let Some(i) = index {
                                    self.wait_applied(i).await?;
                                }
                                return Ok(());
                            }
                            Ok(LeaderReply::NotLeader(_)) => last = "leader changed".into(),
                            Err(e) => last = e.to_string(),
                        }
                    }
                }
                Err(e) => last = e.to_string(),
            }
            tokio::time::sleep(backoff(attempt)).await;
        }
        Err(kosha_engine::Error::Unavailable(format!(
            "cluster unavailable for reads: {last}"
        )))
    }
}

/// Group commit for the Raft write path: commands queue up while the
/// previous batch is being replicated, and the next Raft entry carries all of
/// them (up to `MAX_BATCH`). A lone command is proposed immediately.
struct Proposer {
    tx: tokio::sync::mpsc::UnboundedSender<Queued>,
}

type Queued = (
    Command,
    tokio::sync::oneshot::Sender<kosha_engine::Result<CommandResult>>,
);

const MAX_BATCH: usize = 256;

impl Proposer {
    fn start(inner: std::sync::Weak<Inner>) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Queued>();
        tokio::spawn(async move {
            while let Some(first) = rx.recv().await {
                let mut queued = vec![first];
                while queued.len() < MAX_BATCH {
                    match rx.try_recv() {
                        Ok(q) => queued.push(q),
                        Err(_) => break,
                    }
                }
                let (cmds, replies): (Vec<_>, Vec<_>) = queued.into_iter().unzip();
                let n = cmds.len();
                let result = match inner.upgrade() {
                    Some(inner) => inner.propose_batch(cmds).await,
                    None => Err(kosha_engine::Error::Internal("cluster stopped".into())),
                };
                match result {
                    Ok(results) if results.len() == n => {
                        for (reply, r) in replies.into_iter().zip(results) {
                            let _ = reply.send(Ok(r));
                        }
                    }
                    Ok(results) => {
                        let msg = format!("expected {n} results, got {}", results.len());
                        for reply in replies {
                            let _ = reply.send(Err(kosha_engine::Error::Internal(msg.clone())));
                        }
                    }
                    Err(e) => {
                        for reply in replies {
                            let _ = reply.send(Err(match &e {
                                kosha_engine::Error::Unavailable(m) => {
                                    kosha_engine::Error::Unavailable(m.clone())
                                }
                                e => kosha_engine::Error::Internal(e.to_string()),
                            }));
                        }
                    }
                }
            }
        });
        Proposer { tx }
    }

    async fn propose(&self, cmd: Command) -> kosha_engine::Result<CommandResult> {
        let (reply, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send((cmd, reply))
            .map_err(|_| kosha_engine::Error::Internal("proposer stopped".into()))?;
        rx.await
            .map_err(|_| kosha_engine::Error::Internal("proposer dropped the request".into()))?
    }
}

impl Consensus for RaftConsensus {
    fn propose(&self, cmd: Command) -> BoxFuture<'static, kosha_engine::Result<CommandResult>> {
        let proposer = self.proposer.clone();
        Box::pin(async move { proposer.propose(cmd).await })
    }

    fn read_barrier(&self) -> BoxFuture<'static, kosha_engine::Result<()>> {
        let inner = self.inner.clone();
        Box::pin(async move { inner.read_barrier().await })
    }
}

// --------------------------------------------------------------- placement

struct RendezvousPlacement {
    inner: Arc<Inner>,
}

/// Voters ordered by rendezvous (highest-random-weight) score for `id`.
/// Every node computes the same order without coordination, and adding or
/// removing a node only moves the chunks that rank it first.
pub(crate) fn rank_voters(inner: &Inner, id: &ChunkId) -> Vec<(NodeId, KoshaNode)> {
    let m = inner.raft.metrics().borrow().clone();
    let membership = m.membership_config.membership();
    let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
    let mut nodes: Vec<(NodeId, KoshaNode, [u8; 32])> = membership
        .nodes()
        .filter(|(n, _)| voters.contains(n))
        .map(|(n, node)| {
            let mut h = blake3::Hasher::new();
            h.update(&id.0);
            h.update(&n.to_le_bytes());
            (*n, node.clone(), *h.finalize().as_bytes())
        })
        .collect();
    nodes.sort_by_key(|n| std::cmp::Reverse(n.2));
    nodes.into_iter().map(|(n, node, _)| (n, node)).collect()
}

impl Inner {
    async fn store_replica(
        &self,
        node: &(NodeId, KoshaNode),
        id: ChunkId,
        data: Bytes,
    ) -> kosha_engine::Result<u64> {
        if node.0 == self.cfg.node_id {
            Ok(self
                .engine
                .chunk_store()
                .put_replica(id, data)
                .await?
                .stored_len)
        } else {
            self.client
                .put_chunk(&node.1.addr, &id.to_hex(), data)
                .await
                .map_err(|e| {
                    kosha_engine::Error::Internal(format!("replica to node {}: {e}", node.0))
                })
        }
    }

    async fn place(self: Arc<Self>, id: ChunkId, data: Bytes) -> kosha_engine::Result<u64> {
        let ranked = rank_voters(&self, &id);
        if let Some(layout) = self.ec_layout(data.len(), ranked.len()) {
            return self.place_shards(id, data, layout).await;
        }
        let want = self.cfg.replication.clamp(1, ranked.len().max(1));
        let mut stored = 0u64;
        let mut ok = 0usize;
        // Write the preferred replicas in parallel, then fall back down the
        // ranking for any that failed.
        let first = futures::future::join_all(
            ranked
                .iter()
                .take(want)
                .map(|n| self.store_replica(n, id, data.clone())),
        )
        .await;
        for r in first {
            match r {
                Ok(s) => {
                    ok += 1;
                    stored = stored.max(s);
                }
                Err(e) => tracing::warn!(error = %e, "replica write failed, trying next node"),
            }
        }
        for n in ranked.iter().skip(want) {
            if ok >= want {
                break;
            }
            match self.store_replica(n, id, data.clone()).await {
                Ok(s) => {
                    ok += 1;
                    stored = stored.max(s);
                }
                Err(e) => tracing::warn!(error = %e, "fallback replica write failed"),
            }
        }
        if ok < want {
            return Err(kosha_engine::Error::Internal(format!(
                "stored {ok} of {want} replicas for chunk {}",
                id.to_hex()
            )));
        }
        Ok(stored)
    }

    async fn fetch(self: Arc<Self>, id: ChunkId, len: u32) -> kosha_engine::Result<Bytes> {
        let store = self.engine.chunk_store();
        if store.has(&id) {
            match store.get(&id, len).await {
                Ok(b) => return Ok(b),
                Err(e) => tracing::warn!(error = %e, "local replica unusable, fetching remotely"),
            }
        }
        // Large chunks are probably erasure coded; try shards first. Either
        // way both layouts are tried, so changing the setting is safe.
        let ec_first = self.cfg.erasure.is_some()
            && if self.cfg.erasure_cold_only {
                // Hot chunks are replicated; metadata says which were coded.
                self.engine.chunk_is_erasure_coded(&id)
            } else {
                len as usize >= self.cfg.erasure_min_len || self.engine.chunk_is_erasure_coded(&id)
            };
        if ec_first && let Ok(b) = self.fetch_shards(id, len).await {
            return Ok(b);
        }
        match self.fetch_replica(id, len).await {
            Ok(b) => Ok(b),
            Err(e) if !ec_first => self.fetch_shards(id, len).await.map_err(|_| e),
            Err(e) => Err(e),
        }
    }

    async fn fetch_replica(&self, id: ChunkId, len: u32) -> kosha_engine::Result<Bytes> {
        let store = self.engine.chunk_store();
        let ranked = rank_voters(self, &id);
        let want = self.cfg.replication.clamp(1, ranked.len().max(1));
        let should_hold = ranked
            .iter()
            .take(want)
            .any(|(n, _)| *n == self.cfg.node_id);
        for (n, node) in ranked.iter().filter(|(n, _)| *n != self.cfg.node_id) {
            match self.client.get_chunk(&node.addr, &id.to_hex(), len).await {
                Ok(data) if data.len() == len as usize && store.id_of(&data) == id => {
                    if should_hold {
                        // Read-repair: this node is a designated replica.
                        let (store, data) = (store.clone(), data.clone());
                        tokio::spawn(async move {
                            if let Err(e) = store.put_replica(id, data).await {
                                tracing::warn!(error = %e, "read-repair failed");
                            }
                        });
                    }
                    return Ok(data);
                }
                Ok(_) => {
                    tracing::warn!(node = n, chunk = %id.to_hex(), "replica failed verification")
                }
                Err(e) => tracing::debug!(node = n, error = %e, "replica fetch failed"),
            }
        }
        Err(kosha_engine::Error::Corrupt(format!(
            "chunk {} is unavailable on every replica",
            id.to_hex()
        )))
    }
}

impl ChunkPlacement for RendezvousPlacement {
    fn put(&self, id: ChunkId, data: Bytes) -> BoxFuture<'static, kosha_engine::Result<u64>> {
        Box::pin(self.inner.clone().place(id, data))
    }

    fn supports_erasure(&self) -> bool {
        self.inner.cfg.erasure.is_some()
    }

    fn rotate_shards(
        &self,
        limit: usize,
    ) -> BoxFuture<'static, kosha_engine::Result<(usize, usize, usize)>> {
        let store = self.inner.engine.chunk_store();
        Box::pin(async move { crate::ec::rotate_shards(&store, limit).await })
    }

    fn erasure_code(&self, id: ChunkId, len: u32) -> BoxFuture<'static, kosha_engine::Result<()>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let layout = inner.cfg.erasure.ok_or_else(|| {
                kosha_engine::Error::Internal("erasure coding is not configured".into())
            })?;
            let voters = rank_voters(&inner, &id).len();
            if voters < layout.total() {
                return Err(kosha_engine::Error::Internal(format!(
                    "erasure coding {layout} needs {} voters, the cluster has {voters}",
                    layout.total()
                )));
            }
            let data = inner.clone().fetch(id, len).await?;
            inner.place_shards(id, data, layout).await.map(drop)
        })
    }

    fn get(&self, id: ChunkId, len: u32) -> BoxFuture<'static, kosha_engine::Result<Bytes>> {
        Box::pin(self.inner.clone().fetch(id, len))
    }
}
