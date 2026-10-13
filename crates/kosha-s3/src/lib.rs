//! S3-compatible HTTP front end for the Kosha storage engine.

mod admin;
mod auth;
mod body;
mod config;
mod error;
mod handlers;
pub mod iam;
mod metrics;
pub mod notify;
mod protection;
mod xml;

use std::sync::Arc;

use axum::Router;
use kosha_engine::Engine;

pub use auth::{Credentials, KeyLookup};
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
    /// Serve `/_kosha/metrics` without authentication (for Prometheus).
    pub metrics_public: bool,
    /// Requests arrive over TLS (`aws:SecureTransport` in policies).
    pub secure_transport: bool,
    /// Notification targets that bucket configurations may reference.
    pub webhooks: notify::Webhooks,
}

/// What the S3 front end needs from a cluster, for `/_kosha/cluster*`.
pub trait ClusterHooks: Send + Sync + 'static {
    /// JSON served at `GET /_kosha/cluster`.
    fn status(&self) -> serde_json::Value;
    /// `DELETE /_kosha/cluster/nodes/{id}`. `Err` carries a user-facing reason.
    fn remove_node(&self, id: u64) -> futures::future::BoxFuture<'static, Result<(), String>>;
    /// Extra Prometheus series appended to `/_kosha/metrics`.
    fn prometheus(&self) -> String {
        String::new()
    }
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
            metrics_public: false,
            secure_transport: false,
            webhooks: Default::default(),
        }
    }
}

pub(crate) struct AppState {
    pub engine: Engine,
    pub cfg: S3Config,
    pub metrics: metrics::Metrics,
}

/// Build the S3 API router.
///
/// Besides the root keys in `cfg.credentials`, users stored in the engine
/// can authenticate; their access is governed by policies.
pub fn router(engine: Engine, mut cfg: S3Config) -> Router {
    if !cfg.credentials.is_empty() {
        let e = engine.clone();
        cfg.credentials = cfg.credentials.with_users(Arc::new(move |ak: &str| {
            e.user_local(ak)
                .ok()
                .flatten()
                .filter(|u| u.enabled)
                .map(|u| u.secret_key)
        }));
    }
    let state = Arc::new(AppState {
        engine,
        cfg,
        metrics: Default::default(),
    });
    Router::new().fallback(handlers::dispatch).with_state(state)
}
