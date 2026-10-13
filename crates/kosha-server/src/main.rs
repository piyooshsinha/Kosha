use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use kosha_engine::{Engine, EngineConfig};
use kosha_s3::{Credentials, S3Config};
use tracing_subscriber::EnvFilter;

/// Kosha: an S3-compatible object store with content-addressed, deduplicated storage.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "KOSHA_LISTEN", default_value = "127.0.0.1:9000")]
    listen: SocketAddr,

    /// Directory holding metadata and chunk files.
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

    /// Skip fsync on writes. Faster, but recent writes can be lost on power failure.
    #[arg(long, env = "KOSHA_NO_FSYNC")]
    no_fsync: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kosha=info,kosha_s3=info,kosha_engine=info".into()),
        )
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

    let mut cfg = EngineConfig::new(&args.data_dir);
    cfg.compression_level = args.compression_level;
    cfg.fsync = !args.no_fsync;
    let engine = Engine::open(cfg)
        .with_context(|| format!("opening data directory {}", args.data_dir.display()))?;

    let app = kosha_s3::router(
        engine,
        S3Config {
            region: args.region,
            credentials,
            domain: args.domain,
        },
    );

    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    tracing::info!(listen = %args.listen, data_dir = %args.data_dir.display(), "Kosha is ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    Ok(())
}
