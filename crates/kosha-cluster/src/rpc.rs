//! Internal HTTP transport: msgpack bodies, shared-secret authentication.

use std::time::Duration;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::types::*;

pub const SECRET_HEADER: &str = "x-kosha-cluster-secret";

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("node unreachable: {0}")]
    Unreachable(String),
    #[error("remote returned {0}: {1}")]
    Status(u16, String),
    #[error("bad response: {0}")]
    Decode(String),
}

/// Requests and replies for leader-only operations made from any node.
#[derive(Debug, Serialize, serde::Deserialize)]
pub enum LeaderReply<T> {
    Ok(T),
    NotLeader(Option<(NodeId, KoshaNode)>),
}

#[derive(Debug, Serialize, serde::Deserialize)]
pub struct Proposed {
    pub results: Vec<kosha_engine::CommandResult>,
    pub index: u64,
}

#[derive(Debug, Serialize, serde::Deserialize)]
pub struct JoinRequest {
    pub id: NodeId,
    pub node: KoshaNode,
    /// Fingerprint of the joiner's encryption key (`None`: unencrypted).
    /// Every member must store chunks under the same key.
    #[serde(default)]
    pub encryption: Option<String>,
}

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    secret: String,
    /// `http` or `https` (mutual TLS).
    scheme: &'static str,
}

pub fn encode<T: Serialize>(v: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(v).expect("rpc types serialize")
}

pub fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, RpcError> {
    rmp_serde::from_slice(b).map_err(|e| RpcError::Decode(e.to_string()))
}

impl Client {
    pub fn new(secret: String, tls: Option<rustls::ClientConfig>) -> Self {
        // reqwest needs a process-wide rustls provider even for plain HTTP.
        // Installing fails harmlessly when one is already set.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .pool_idle_timeout(Duration::from_secs(60))
            .tcp_nodelay(true);
        let scheme = match tls {
            Some(cfg) => {
                builder = builder.tls_backend_preconfigured(cfg);
                "https"
            }
            None => "http",
        };
        let http = builder.build().expect("http client");
        Client {
            http,
            secret,
            scheme,
        }
    }

    async fn send(
        &self,
        req: reqwest::RequestBuilder,
        timeout: Duration,
    ) -> Result<Bytes, RpcError> {
        let resp = req
            .header(SECRET_HEADER, &self.secret)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| RpcError::Unreachable(e.to_string()))?;
        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .map_err(|e| RpcError::Unreachable(e.to_string()))?;
        if !status.is_success() {
            return Err(RpcError::Status(
                status.as_u16(),
                String::from_utf8_lossy(&body).into_owned(),
            ));
        }
        Ok(body)
    }

    pub async fn call<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        addr: &str,
        path: &str,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp, RpcError> {
        let body = self
            .send(
                self.http
                    .post(format!("{}://{addr}{path}", self.scheme))
                    .body(encode(req)),
                timeout,
            )
            .await?;
        decode(&body)
    }

    pub async fn put_chunk(&self, addr: &str, id: &str, data: Bytes) -> Result<u64, RpcError> {
        let body = self
            .send(
                self.http
                    .put(format!("{}://{addr}/chunk/{id}", self.scheme))
                    .body(data),
                Duration::from_secs(60),
            )
            .await?;
        decode(&body)
    }

    pub async fn health(&self, addr: &str, timeout: Duration) -> Result<(), RpcError> {
        self.send(
            self.http.get(format!("{}://{addr}/health", self.scheme)),
            timeout,
        )
        .await
        .map(drop)
    }

    /// Whether `addr` holds a copy of the chunk.
    pub async fn has_chunk(&self, addr: &str, id: &str) -> Result<bool, RpcError> {
        match self
            .send(
                self.http
                    .head(format!("{}://{addr}/chunk/{id}", self.scheme)),
                Duration::from_secs(5),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(RpcError::Status(404, _)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub async fn get_chunk(&self, addr: &str, id: &str, len: u32) -> Result<Bytes, RpcError> {
        self.send(
            self.http
                .get(format!("{}://{addr}/chunk/{id}?len={len}", self.scheme)),
            Duration::from_secs(60),
        )
        .await
    }
}
