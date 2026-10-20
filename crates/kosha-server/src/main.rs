use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::serve::ListenerExt;
use clap::Parser;
use kosha_cluster::tls::{TlsFiles, TlsListener};
use kosha_cluster::{Cluster, ClusterConfig};
use kosha_engine::{
    ChunkKeys, DirectConfig, Engine, EngineConfig, IoMode, SyncMode, read_key_file,
};
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

    /// Encrypt object data at rest with this 32-byte key (64 hex characters,
    /// e.g. from `openssl rand -hex 32`). Chosen when a data directory is
    /// created; every cluster node needs the same key. Keep a backup: data
    /// cannot be read without it.
    #[arg(long, env = "KOSHA_ENCRYPTION_KEY_FILE")]
    encryption_key_file: Option<PathBuf>,

    /// Previous encryption key(s), for key rotation (repeatable). Data
    /// under them stays readable and is re-encrypted with
    /// --encryption-key-file in the background; remove them once
    /// /_kosha/stats shows the rotation is done on every node.
    #[arg(
        long = "encryption-old-key-file",
        env = "KOSHA_ENCRYPTION_OLD_KEY_FILES",
        value_delimiter = ',',
        requires = "encryption_key_file"
    )]
    encryption_old_key_files: Vec<PathBuf>,

    /// The key a cluster was first created with. Only needed to create the
    /// data directory of a node joining a cluster whose keys have been
    /// rotated since (it derives the cluster's chunk-id key).
    #[arg(
        long,
        env = "KOSHA_ENCRYPTION_IDENTITY_KEY_FILE",
        requires = "encryption_key_file"
    )]
    encryption_identity_key_file: Option<PathBuf>,

    /// Event notification target, `name=url` (repeatable). Buckets refer to
    /// it as `arn:kosha:sqs::<name>:webhook`. Use the same list on every
    /// cluster node.
    #[arg(
        long = "notify-webhook",
        env = "KOSHA_NOTIFY_WEBHOOKS",
        value_delimiter = ','
    )]
    notify_webhooks: Vec<String>,

    /// S3-compatible endpoint for the cold tier (e.g. https://s3.eu-west-1.amazonaws.com).
    /// Objects in a cold storage class (by lifecycle transition or on
    /// upload) have their chunks moved there; reads stay transparent.
    #[arg(long, env = "KOSHA_COLD_TIER_ENDPOINT", requires_all = ["cold_tier_bucket", "cold_tier_access_key", "cold_tier_secret_key"])]
    cold_tier_endpoint: Option<String>,

    /// Bucket in the cold tier (must exist).
    #[arg(long, env = "KOSHA_COLD_TIER_BUCKET")]
    cold_tier_bucket: Option<String>,

    /// Key prefix for chunk objects in the cold bucket.
    #[arg(long, env = "KOSHA_COLD_TIER_PREFIX", default_value = "kosha-cold/")]
    cold_tier_prefix: String,

    /// Region the cold tier signs requests for.
    #[arg(long, env = "KOSHA_COLD_TIER_REGION", default_value = "us-east-1")]
    cold_tier_region: String,

    #[arg(long, env = "KOSHA_COLD_TIER_ACCESS_KEY")]
    cold_tier_access_key: Option<String>,

    #[arg(long, env = "KOSHA_COLD_TIER_SECRET_KEY", hide_env_values = true)]
    cold_tier_secret_key: Option<String>,

    /// Seconds between passes that copy newly cold chunks to the cold tier.
    #[arg(long, env = "KOSHA_OFFLOAD_INTERVAL_SECS", default_value_t = 60)]
    offload_interval_secs: u64,

    /// PEM certificate (chain) for serving the S3 API and console over HTTPS.
    #[arg(long, env = "KOSHA_TLS_CERT", requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// PEM private key for --tls-cert.
    #[arg(long, env = "KOSHA_TLS_KEY", requires = "tls_cert")]
    tls_key: Option<PathBuf>,

    /// Chunk I/O backend: auto (io_uring if available), uring, threads.
    #[arg(long, env = "KOSHA_IO", default_value = "auto")]
    io: IoMode,

    /// Read and write large chunk files with O_DIRECT through registered,
    /// aligned io_uring buffers, bypassing the page cache.
    #[arg(long, env = "KOSHA_IO_DIRECT")]
    io_direct: bool,

    /// Chunk files at least this many bytes use O_DIRECT (with --io-direct).
    #[arg(long, env = "KOSHA_IO_DIRECT_MIN", default_value_t = 256 * 1024)]
    io_direct_min: usize,

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

    /// PEM certificate this node presents to peers (mutual TLS for all
    /// internal traffic). Needs --cluster-tls-key and --cluster-tls-ca.
    #[arg(long, env = "KOSHA_CLUSTER_TLS_CERT", requires_all = ["cluster_tls_key", "cluster_tls_ca"])]
    cluster_tls_cert: Option<PathBuf>,

    /// PEM private key for --cluster-tls-cert.
    #[arg(long, env = "KOSHA_CLUSTER_TLS_KEY", requires = "cluster_tls_cert")]
    cluster_tls_key: Option<PathBuf>,

    /// PEM CA certificate(s) that sign every node's certificate. Peers
    /// without a certificate from this CA are refused.
    #[arg(long, env = "KOSHA_CLUSTER_TLS_CA", requires = "cluster_tls_cert")]
    cluster_tls_ca: Option<PathBuf>,

    /// Bootstrap a new cluster with this node as its first member.
    #[arg(long, env = "KOSHA_INIT")]
    init: bool,

    /// Join an existing cluster through any member's cluster address.
    #[arg(long, env = "KOSHA_JOIN")]
    join: Option<String>,

    /// Copies of every chunk across the cluster.
    #[arg(long, env = "KOSHA_REPLICATION", default_value_t = 2)]
    replication: usize,

    /// Erasure-code large chunks as K data + M parity shards instead of
    /// keeping --replication full copies (e.g. 4+2: 1.5x storage, survives
    /// any 2 node failures). Needs at least K+M nodes; the same on every node.
    #[arg(long, env = "KOSHA_ERASURE")]
    erasure: Option<String>,

    /// With --erasure: write chunks replicated and erasure code them only
    /// once they are cold (lifecycle transition, cold upload class, or
    /// access-based tiering) and no --cold-tier-endpoint is configured.
    #[arg(long, env = "KOSHA_ERASURE_COLD_ONLY", requires = "erasure")]
    erasure_cold_only: bool,

    /// Chunks smaller than this many bytes stay replicated.
    #[arg(long, env = "KOSHA_ERASURE_MIN_CHUNK", default_value_t = 128 * 1024)]
    erasure_min_chunk: usize,

    /// Prove leadership with a quorum round-trip on every read instead of using
    /// a leader lease (slower; for environments with unreliable clocks).
    #[arg(long, env = "KOSHA_NO_LEASE_READS")]
    no_lease_reads: bool,

    /// Seconds between background re-replication/rebalancing passes (0 disables).
    #[arg(long, env = "KOSHA_REPAIR_INTERVAL_SECS", default_value_t = 60)]
    repair_interval_secs: u64,

    /// Cluster: take a Raft snapshot after this many log entries.
    #[arg(long, env = "KOSHA_SNAPSHOT_EVERY", default_value_t = 10_000)]
    snapshot_every: u64,

    /// Cluster: log entries kept behind a snapshot (for slow followers).
    #[arg(long, env = "KOSHA_RAFT_LOGS_TO_KEEP", default_value_t = 1_000)]
    raft_logs_to_keep: u64,

    /// Seconds between lifecycle passes (expiration, noncurrent versions,
    /// abandoned uploads). 0 disables lifecycle processing.
    #[arg(long, env = "KOSHA_LIFECYCLE_INTERVAL_SECS", default_value_t = 3600)]
    lifecycle_interval_secs: u64,

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

    fn node_id(&self) -> u64 {
        self.0.node_id()
    }

    fn flows(&self, limit: usize) -> futures::future::BoxFuture<'static, serde_json::Value> {
        let c = self.0.clone();
        Box::pin(async move { serde_json::to_value(c.flows(limit).await).unwrap_or_default() })
    }

    fn remove_node(&self, id: u64) -> futures::future::BoxFuture<'static, Result<(), String>> {
        let c = self.0.clone();
        Box::pin(async move { c.remove_node(id).await.map_err(|e| e.to_string()) })
    }

    fn add_node(&self, addr: String) -> futures::future::BoxFuture<'static, Result<u64, String>> {
        let c = self.0.clone();
        Box::pin(async move { c.add_node(&addr).await.map_err(|e| e.to_string()) })
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
    if args.io_direct {
        cfg.io_direct = Some(DirectConfig {
            min_size: args.io_direct_min,
            ..DirectConfig::default()
        });
    }
    if let Some(path) = &args.encryption_key_file {
        let read = |p: &PathBuf| {
            read_key_file(p).with_context(|| format!("loading encryption key {}", p.display()))
        };
        let old = args
            .encryption_old_key_files
            .iter()
            .map(read)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let identity = args
            .encryption_identity_key_file
            .as_ref()
            .map(read)
            .transpose()?;
        cfg.encryption = Some(Arc::new(ChunkKeys::ring(read(path)?, old, identity)));
    }
    // A single node protects in-flight uploads with pins alone; a cluster also
    // needs the grace period for uploads coordinated by other nodes.
    cfg.gc_grace = if clustered {
        Duration::from_secs(args.gc_grace_secs)
    } else {
        Duration::ZERO
    };
    let engine = Engine::open(cfg)
        .with_context(|| format!("opening data directory {}", args.data_dir.display()))?;
    if let Some(endpoint) = &args.cold_tier_endpoint {
        let store = kosha_s3::coldtier::S3ColdStore::new(kosha_s3::coldtier::S3ColdConfig {
            endpoint: endpoint.clone(),
            bucket: args.cold_tier_bucket.clone().expect("required by clap"),
            prefix: args.cold_tier_prefix.clone(),
            region: args.cold_tier_region.clone(),
            access_key: args.cold_tier_access_key.clone().expect("required by clap"),
            secret_key: args.cold_tier_secret_key.clone().expect("required by clap"),
        })?;
        store
            .probe()
            .await
            .context("the cold tier is not writable with the given settings")?;
        engine.set_cold_store(Arc::new(store))?;
        tracing::info!(%endpoint, bucket = args.cold_tier_bucket.as_deref().unwrap_or_default(), "cold tier ready");
    }
    tracing::info!(
        io = engine.io_backend(),
        sync = ?engine.config().sync,
        encryption = engine.encryption_fingerprint().unwrap_or("off"),
        "storage engine ready"
    );

    let mut s3 = S3Config {
        region: args.region.clone(),
        credentials,
        domain: args.domain.clone(),
        cluster: None,
        metrics_public: args.metrics_public,
        secure_transport: args.tls_cert.is_some(),
        webhooks: Default::default(),
    };
    for spec in &args.notify_webhooks {
        let (name, url) = spec
            .split_once('=')
            .with_context(|| format!("--notify-webhook {spec:?}: expected name=url"))?;
        anyhow::ensure!(
            !name.is_empty()
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b)),
            "--notify-webhook: target names use A-Z a-z 0-9 - _"
        );
        anyhow::ensure!(
            url.starts_with("http://") || url.starts_with("https://"),
            "--notify-webhook {name}: the URL must start with http:// or https://"
        );
        s3.webhooks.insert(name.to_string(), url.to_string());
    }

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
            ccfg.erasure = args
                .erasure
                .as_deref()
                .map(kosha_cluster::ErasureLayout::parse)
                .transpose()
                .map_err(anyhow::Error::msg)?;
            ccfg.erasure_min_len = args.erasure_min_chunk;
            ccfg.erasure_cold_only = args.erasure_cold_only;
            ccfg.snapshot_every = args.snapshot_every.max(1);
            ccfg.logs_to_keep = args.raft_logs_to_keep;
            ccfg.tls = args.cluster_tls_cert.clone().map(|cert| TlsFiles {
                cert,
                key: args.cluster_tls_key.clone().expect("required by clap"),
                ca: args.cluster_tls_ca.clone(),
            });
            if ccfg.tls.is_none() {
                tracing::warn!(
                    "internal cluster traffic is not encrypted: use --cluster-tls-* outside a trusted network"
                );
            }
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
            } else {
                let c = cluster.clone();
                tokio::spawn(async move {
                    if let Err(e) = c.refresh_advertised().await {
                        tracing::warn!(error = %e, "could not update this node's advertised API address");
                    }
                });
            }
            s3.cluster = Some(Arc::new(Hooks(cluster.clone())));
            Some(cluster)
        }
    };

    engine.spawn_sweeper(Duration::from_secs(600));
    // Lifecycle passes and event delivery run only on the leader (always
    // true on a single node), so they do not race each other.
    let leader_gate: Arc<dyn Fn() -> bool + Send + Sync> = match &cluster {
        Some(c) => {
            let c = c.clone();
            Arc::new(move || {
                let s = c.status();
                s.leader == Some(s.node_id)
            })
        }
        None => Arc::new(|| true),
    };
    if args.lifecycle_interval_secs > 0 {
        engine.spawn_lifecycle(
            Duration::from_secs(args.lifecycle_interval_secs),
            leader_gate.clone(),
        );
    }
    if engine.encryption_fingerprint().is_some() {
        // Re-encrypts anything under an old key (a no-op otherwise).
        engine.spawn_rotation(Duration::from_secs(60), leader_gate.clone());
    }
    if engine.can_tier() {
        // Reads are recorded in batches for access-based tiering.
        engine.spawn_access_flusher(Duration::from_secs(30));
        engine.spawn_offloader(
            Duration::from_secs(args.offload_interval_secs.max(1)),
            leader_gate.clone(),
        );
    }
    kosha_s3::notify::spawn_dispatcher(
        engine.clone(),
        s3.webhooks.clone(),
        s3.region.clone(),
        leader_gate,
    );

    let app = ui::router()
        .merge(kosha_s3::router(engine, s3))
        .into_make_service_with_connect_info::<SocketAddr>();
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    let scheme = if args.tls_cert.is_some() {
        "https"
    } else {
        "http"
    };
    tracing::info!(
        listen = %args.listen,
        console = %format!("{scheme}://{}/_kosha/ui/", args.listen),
        mode = if clustered { "cluster" } else { "single-node" },
        "Kosha is ready"
    );
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutting down");
    };
    match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => {
            let tls = TlsFiles {
                cert: cert.clone(),
                key: key.clone(),
                ca: None,
            }
            .server_config()?;
            // `tap_io` gives the listener axum's `ConnectInfo` support.
            let listener = TlsListener::new(listener, tls)?.tap_io(|_| {});
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .await?
        }
        _ => {
            // Small responses are written as headers plus a body frame;
            // without TCP_NODELAY, Nagle holds the second write until the
            // client's delayed ACK (~40 ms).
            let listener = listener.tap_io(|tcp| {
                let _ = tcp.set_nodelay(true);
            });
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .await?
        }
    }
    if let Some(c) = cluster {
        c.shutdown().await;
    }
    Ok(())
}
