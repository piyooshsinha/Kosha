use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use kosha_cluster::{Cluster, ClusterConfig};
use kosha_engine::{Engine, EngineConfig, IoMode, SyncMode};
use kosha_s3::{ClusterHooks, Credentials, S3Config};
use tracing_subscriber::EnvFilter;

mod ui;

/// Kosha: an S3-compatible object store with content-addressed, deduplicated storage.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Address to listen on for the S3 API and web console.
    #[arg(long, env = "KOSHA_LISTEN", default_value = "127.0.0.1:9000")]
    listen: SocketAddr,

    /// Directory holding metadata, chunks and (in a cluster) the Raft log.
    #[arg(long, env = "KOSHA_DATA_DIR", default_value = "./data")]
    data_dir: PathBuf,

    /// Access key id clients sign requests with.
    #[arg(long, env = "KOSHA_ACCESS_KEY")]
    access_key: Option<String>,

    /// Secret key clients sign requests with.
    #[arg(long, env = "KOSHA_SECRET_KEY", hide_env_values = true)]
    secret_key: Option<String>,

    /// Allow unauthenticated access when no keys are configured (development only).
    #[arg(long, env = "KOSHA_ANONYMOUS")]
    anonymous: bool,

    /// Region reported to clients.
    #[arg(long, env = "KOSHA_REGION", default_value = "us-east-1")]
    region: String,

    /// Base domain for virtual-hosted-style requests (bucket.<domain>).
    #[arg(long, env = "KOSHA_DOMAIN")]
    domain: Option<String>,

    /// zstd level for compressible chunks (0 disables compression).
    #[arg(long, env = "KOSHA_COMPRESSION_LEVEL", default_value_t = 3)]
    compression_level: i32,

    /// How chunk files are made durable: group (shared syncfs, default), file
    /// (fsync each file and its directory) or none. Metadata is always durable.
    #[arg(long, env = "KOSHA_SYNC", default_value = "group")]
    sync: SyncMode,

    /// Shorthand for --sync none. Chunk data written just before a power
    /// failure can be lost.
    #[arg(long, env = "KOSHA_NO_FSYNC")]
    no_fsync: bool,

    /// Serve /_kosha/metrics without authentication (for Prometheus scrapers).
    #[arg(long, env = "KOSHA_METRICS_PUBLIC")]
    metrics_public: bool,

    /// Chunk I/O backend: auto (io_uring if available), uring, threads.
    #[arg(long, env = "KOSHA_IO", default_value = "auto")]
    io: IoMode,

    // ----------------------------------------------------------- cluster
    /// Enable cluster mode with this node id (unique, non-zero).
    #[arg(long, env = "KOSHA_NODE_ID")]
    node_id: Option<u64>,

    /// Bind address for internal cluster traffic (Raft + chunk replication).
    #[arg(long, env = "KOSHA_CLUSTER_LISTEN", default_value = "127.0.0.1:9100")]
    cluster_listen: SocketAddr,

    /// Address other nodes use to reach this node's cluster port (defaults to --cluster-listen).
    #[arg(long, env = "KOSHA_CLUSTER_ADVERTISE")]
    cluster_advertise: Option<String>,

    /// Address of this node's S3 API as clients see it (shown in the console).
    #[arg(long, env = "KOSHA_API_ADVERTISE")]
    api_advertise: Option<String>,

    /// Shared secret for internal cluster traffic (same on every node).
    #[arg(long, env = "KOSHA_CLUSTER_SECRET", hide_env_values = true)]
    cluster_secret: Option<String>,

    /// Bootstrap a new cluster with this node as its first member.
    #[arg(long, env = "KOSHA_INIT")]
    init: bool,

    /// Join an existing cluster through any member's cluster address.
    #[arg(long, env = "KOSHA_JOIN")]
    join: Option<String>,

    /// Copies of every chunk across the cluster.
    #[arg(long, env = "KOSHA_REPLICATION", default_value_t = 2)]
    replication: usize,

    /// Prove leadership with a quorum round-trip on every read instead of using
    /// a leader lease (slower; for environments with unreliable clocks).
    #[arg(long, env = "KOSHA_NO_LEASE_READS")]
    no_lease_reads: bool,

    /// Seconds between background re-replication/rebalancing passes (0 disables).
    #[arg(long, env = "KOSHA_REPAIR_INTERVAL_SECS", default_value_t = 60)]
    repair_interval_secs: u64,

    /// Seconds an unreferenced chunk file is kept before it may be collected
    /// (gives uploads on other nodes time to commit).
    #[arg(long, env = "KOSHA_GC_GRACE_SECS", default_value_t = 900)]
    gc_grace_secs: u64,
}

/// Connects the S3 front end's admin endpoints to the cluster.
struct Hooks(Cluster);

impl ClusterHooks for Hooks {
    fn status(&self) -> serde_json::Value {
        serde_json::to_value(self.0.status()).unwrap_or_default()
    }

    fn remove_node(&self, id: u64) -> futures::future::BoxFuture<'static, Result<(), String>> {
        let c = self.0.clone();
        Box::pin(async move { c.remove_node(id).await.map_err(|e| e.to_string()) })
    }

    fn prometheus(&self) -> String {
        self.0.prometheus()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            "kosha=info,kosha_s3=info,kosha_engine=info,kosha_cluster=info,openraft=warn".into()
        }))
        .init();
    let args = Args::parse();

    let credentials = match (&args.access_key, &args.secret_key) {
        (Some(a), Some(s)) => Credentials::new().with_key(a, s),
        (None, None) if args.anonymous => {
            tracing::warn!(
                "running WITHOUT authentication: anyone who can reach this port has full access"
            );
            Credentials::new()
        }
        (None, None) => anyhow::bail!(
            "set KOSHA_ACCESS_KEY and KOSHA_SECRET_KEY, or pass --anonymous for local development"
        ),
        _ => anyhow::bail!("KOSHA_ACCESS_KEY and KOSHA_SECRET_KEY must be set together"),
    };

    let clustered = args.node_id.is_some();
    let mut cfg = EngineConfig::new(&args.data_dir);
    cfg.compression_level = args.compression_level;
    cfg.sync = if args.no_fsync {
        SyncMode::None
    } else {
        args.sync
    };
    cfg.io_mode = args.io;
    // A single node protects in-flight uploads with pins alone; a cluster also
    // needs the grace period for uploads coordinated by other nodes.
    cfg.gc_grace = if clustered {
        Duration::from_secs(args.gc_grace_secs)
    } else {
        Duration::ZERO
    };
    let engine = Engine::open(cfg)
        .with_context(|| format!("opening data directory {}", args.data_dir.display()))?;
    tracing::info!(io = engine.io_backend(), sync = ?engine.config().sync, "storage engine ready");

    let mut s3 = S3Config {
        region: args.region.clone(),
        credentials,
        domain: args.domain.clone(),
        cluster: None,
        metrics_public: args.metrics_public,
    };

    let cluster = match args.node_id {
        None => None,
        Some(0) => anyhow::bail!("--node-id must be non-zero"),
        Some(id) => {
            let secret = args
                .cluster_secret
                .clone()
                .context("cluster mode requires --cluster-secret / KOSHA_CLUSTER_SECRET")?;
            anyhow::ensure!(
                secret.len() >= 16,
                "the cluster secret must be at least 16 characters"
            );
            let mut ccfg =
                ClusterConfig::new(id, args.cluster_listen, secret, args.data_dir.join("raft"));
            ccfg.advertise = args
                .cluster_advertise
                .clone()
                .unwrap_or_else(|| args.cluster_listen.to_string());
            ccfg.api_addr = args
                .api_advertise
                .clone()
                .unwrap_or_else(|| args.listen.to_string());
            ccfg.replication = args.replication.max(1);
            ccfg.lease_reads = !args.no_lease_reads;
            ccfg.repair_interval = (args.repair_interval_secs > 0)
                .then(|| Duration::from_secs(args.repair_interval_secs));
            let cluster = Cluster::start(engine.clone(), ccfg).await?;
            if args.init {
                cluster.initialize().await?;
            }
            if let Some(seed) = &args.join {
                cluster.join(seed).await?;
            }
            if !cluster.is_initialized().await {
                tracing::warn!(
                    "this node is not part of a cluster yet: start one with --init or join one with --join"
                );
            }
            s3.cluster = Some(Arc::new(Hooks(cluster.clone())));
            Some(cluster)
        }
    };

    engine.spawn_sweeper(Duration::from_secs(600));

    let app = ui::router().merge(kosha_s3::router(engine, s3));
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    tracing::info!(
        listen = %args.listen,
        console = %format!("http://{}/_kosha/ui/", args.listen),
        mode = if clustered { "cluster" } else { "single-node" },
        "Kosha is ready"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    if let Some(c) = cluster {
        c.shutdown().await;
    }
    Ok(())
}
