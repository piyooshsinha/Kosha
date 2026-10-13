//! S3-compatible HTTP front end for the Kosha storage engine.

mod auth;
mod body;
mod error;
mod handlers;
mod xml;

use std::sync::Arc;

use axum::Router;
use kosha_engine::Engine;

pub use auth::Credentials;
pub use error::{S3Error, S3Result};

#[derive(Clone, Debug)]
pub struct S3Config {
    /// Region reported by GetBucketLocation. Signatures for any region are
    /// accepted, so clients configured for a different region still work.
    pub region: String,
    /// Access keys. Empty means anonymous access (development only).
    pub credentials: Credentials,
    /// Enables virtual-hosted-style requests (`bucket.<domain>`).
    pub domain: Option<String>,
    /// Cluster status and administration (absent on a single node).
    pub cluster: Option<Arc<dyn ClusterHooks>>,
}

/// What the S3 front end needs from a cluster, for `/_kosha/cluster*`.
pub trait ClusterHooks: Send + Sync + 'static {
    /// JSON served at `GET /_kosha/cluster`.
    fn status(&self) -> serde_json::Value;
    /// `DELETE /_kosha/cluster/nodes/{id}`. `Err` carries a user-facing reason.
    fn remove_node(&self, id: u64) -> futures::future::BoxFuture<'static, Result<(), String>>;
}

impl std::fmt::Debug for dyn ClusterHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterHooks")
    }
}

impl Default for S3Config {
    fn default() -> Self {
        S3Config {
            region: "us-east-1".into(),
            credentials: Credentials::new(),
            domain: None,
            cluster: None,
        }
    }
}

pub(crate) struct AppState {
    pub engine: Engine,
    pub cfg: S3Config,
}

/// Build the S3 API router.
pub fn router(engine: Engine, cfg: S3Config) -> Router {
    let state = Arc::new(AppState { engine, cfg });
    Router::new().fallback(handlers::dispatch).with_state(state)
}
