//! The cold tier over any S3-compatible service (AWS S3, MinIO, another
//! Kosha, ...): a minimal SigV4 client for PUT, GET and DELETE of chunk
//! objects under `bucket/prefix`.

use bytes::Bytes;
use chrono::Utc;
use futures::future::BoxFuture;
use kosha_engine::{ColdStore, Error, Result};
use sha2::{Digest, Sha256};

use crate::auth::{hmac, signing_key, uri_encode};

#[derive(Clone, Debug)]
pub struct S3ColdConfig {
    /// `https://s3.us-east-1.amazonaws.com`, `http://minio:9000`, ...
    pub endpoint: String,
    pub bucket: String,
    /// Key prefix for chunk objects, e.g. `kosha/`.
    pub prefix: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

/// Cheap to clone (the HTTP client is shared).
#[derive(Clone)]
pub struct S3ColdStore {
    cfg: S3ColdConfig,
    host: String,
    http: reqwest::Client,
}

impl S3ColdStore {
    pub fn new(mut cfg: S3ColdConfig) -> Result<Self> {
        cfg.endpoint = cfg.endpoint.trim_end_matches('/').to_string();
        let host = cfg
            .endpoint
            .split_once("://")
            .map(|(_, h)| h)
            .filter(|h| !h.is_empty() && !h.contains('/'))
            .ok_or_else(|| {
                Error::InvalidRequest(format!(
                    "cold tier endpoint {:?} must look like https://host[:port]",
                    cfg.endpoint
                ))
            })?
            .to_string();
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(S3ColdStore { cfg, host, http })
    }

    /// A SigV4-signed request for `key` (path style).
    fn request(&self, method: reqwest::Method, key: &str, body: &[u8]) -> reqwest::RequestBuilder {
        let path = format!(
            "/{}/{}",
            uri_encode(&self.cfg.bucket),
            format!("{}{key}", self.cfg.prefix)
                .split('/')
                .map(uri_encode)
                .collect::<Vec<_>>()
                .join("/")
        );
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = &amz_date[..8];
        let payload = hex::encode(Sha256::digest(body));
        let canonical = format!(
            "{}\n{path}\n\nhost:{}\nx-amz-content-sha256:{payload}\nx-amz-date:{amz_date}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload}",
            method.as_str(),
            self.host,
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.cfg.region);
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical.as_bytes()))
        );
        let key = signing_key(&self.cfg.secret_key, date, &self.cfg.region, "s3");
        let signature = hex::encode(hmac(&key, to_sign.as_bytes()));
        self.http
            .request(method, format!("{}{path}", self.cfg.endpoint))
            .header("x-amz-date", amz_date)
            .header("x-amz-content-sha256", payload)
            .header(
                "authorization",
                format!(
                    "AWS4-HMAC-SHA256 Credential={}/{scope},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={signature}",
                    self.cfg.access_key
                ),
            )
    }

    async fn send(&self, req: reqwest::RequestBuilder, what: &str) -> Result<reqwest::Response> {
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Io(std::io::Error::other(format!("cold tier {what}: {e}"))))?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        let msg = format!(
            "cold tier {what}: HTTP {status}: {}",
            body.chars().take(300).collect::<String>()
        );
        Err(if status == reqwest::StatusCode::NOT_FOUND {
            Error::Corrupt(msg)
        } else {
            Error::Io(std::io::Error::other(msg))
        })
    }

    /// Check that the bucket is reachable with these credentials.
    pub async fn probe(&self) -> Result<()> {
        let key = format!(".kosha-probe-{}", uuid::Uuid::new_v4().simple());
        self.put_object(&key, Bytes::from_static(b"ok")).await?;
        self.delete_object(&key).await
    }

    async fn put_object(&self, key: &str, data: Bytes) -> Result<()> {
        let req = self.request(reqwest::Method::PUT, key, &data).body(data);
        self.send(req, &format!("PUT {key}")).await.map(drop)
    }

    async fn get_object(&self, key: &str) -> Result<Bytes> {
        let req = self.request(reqwest::Method::GET, key, b"");
        let resp = self.send(req, &format!("GET {key}")).await?;
        resp.bytes()
            .await
            .map_err(|e| Error::Io(std::io::Error::other(format!("cold tier GET {key}: {e}"))))
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        let req = self.request(reqwest::Method::DELETE, key, b"");
        match self.send(req, &format!("DELETE {key}")).await {
            Ok(_) | Err(Error::Corrupt(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl ColdStore for S3ColdStore {
    fn put(&self, key: String, data: Bytes) -> BoxFuture<'static, Result<()>> {
        let this = self.clone();
        Box::pin(async move { this.put_object(&key, data).await })
    }
    fn get(&self, key: String) -> BoxFuture<'static, Result<Bytes>> {
        let this = self.clone();
        Box::pin(async move { this.get_object(&key).await })
    }
    fn delete(&self, key: String) -> BoxFuture<'static, Result<()>> {
        let this = self.clone();
        Box::pin(async move { this.delete_object(&key).await })
    }
}
