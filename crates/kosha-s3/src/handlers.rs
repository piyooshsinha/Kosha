//! S3 request routing and operation handlers.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use kosha_engine::{BucketConfigUpdate, ListQuery, ObjectAttrs, ObjectMeta, WriteCondition};
use percent_encoding::percent_decode_str;
use serde::Deserialize;

use crate::AppState;
use crate::auth::{self, AuthRequest, Payload};
use crate::body::{BodySource, check_md5, content_md5};
use crate::error::{S3Error, S3Result};
use crate::iam::{self, Policy, PolicyKind, Principal, RequestFacts};
use crate::xml::{Xml, iso8601};

const MAX_XML_BODY: usize = 8 * 1024 * 1024;
const MAX_KEYS: usize = 1000;
const OWNER_ID: &str = "kosha";

/// Stored standard headers that are echoed back on GET/HEAD.
const STORED_HEADERS: [&str; 5] = [
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "expires",
];

struct Ctx {
    st: Arc<AppState>,
    method: Method,
    headers: HeaderMap,
    query: Vec<(String, String)>,
    bucket: Option<String>,
    key: Option<String>,
    payload: Payload,
    authz: Authz,
}

/// Authorization state of one request.
pub(crate) struct Authz {
    /// Root key, or no credentials configured: everything is allowed.
    open: bool,
    identity: Option<Policy>,
    facts: RequestFacts,
    /// Parsed bucket policies, by bucket, loaded on first use.
    buckets: parking_lot::Mutex<std::collections::HashMap<String, Option<Arc<Policy>>>>,
}

impl Authz {
    fn bucket_policy(&self, engine: &kosha_engine::Engine, bucket: &str) -> Option<Arc<Policy>> {
        self.buckets
            .lock()
            .entry(bucket.to_string())
            .or_insert_with(|| {
                let doc = engine.bucket_local(bucket).ok()?.config.policy?;
                Policy::parse(&doc, PolicyKind::Bucket(bucket))
                    .ok()
                    .map(Arc::new)
            })
            .clone()
    }

    fn allows(
        &self,
        engine: &kosha_engine::Engine,
        bucket: Option<&str>,
        check: &iam::Check,
    ) -> bool {
        if self.open {
            return true;
        }
        let bp = bucket.and_then(|b| self.bucket_policy(engine, b));
        iam::decide(
            self.identity.as_ref(),
            bp.as_deref(),
            check.action,
            &check.resource,
            &self.facts,
        )
    }
}

impl Ctx {
    fn q(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    fn has(&self, name: &str) -> bool {
        self.query.iter().any(|(k, _)| k == name)
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    fn engine(&self) -> &kosha_engine::Engine {
        &self.st.engine
    }

    /// Require `action` on `resource` (in `bucket`, whose policy applies).
    fn authorize(&self, action: &'static str, bucket: &str, resource: String) -> S3Result<()> {
        let check = iam::Check { action, resource };
        if self.authz.allows(&self.st.engine, Some(bucket), &check) {
            Ok(())
        } else {
            Err(S3Error::access_denied("Access Denied"))
        }
    }

    fn may(&self, action: &'static str, bucket: &str, resource: String) -> bool {
        self.authorize(action, bucket, resource).is_ok()
    }

    fn resource(&self) -> String {
        match (&self.bucket, &self.key) {
            (Some(b), Some(k)) => format!("/{b}/{k}"),
            (Some(b), None) => format!("/{b}"),
            _ => "/".into(),
        }
    }
}

pub async fn dispatch(State(st): State<Arc<AppState>>, req: Request) -> Response {
    let request_id = format!("{:016X}", rand_u64());
    let is_head = req.method() == Method::HEAD;
    let started = std::time::Instant::now();
    let (method, path) = (req.method().clone(), req.uri().path().to_string());
    let op = crate::metrics::operation(&method, &path, req.uri().query());
    let bytes_in = content_length(req.headers());
    let metrics_st = st.clone();

    let mut resp = match handle(st, req).await {
        Ok(r) => r,
        Err(mut e) => {
            e.request_id = Some(request_id.clone());
            if e.status.is_server_error() {
                tracing::error!(code = e.code, message = %e.message, "request failed");
            }
            let status = e.status;
            let mut r = e.into_response();
            if is_head {
                // HEAD responses carry only the status.
                *r.body_mut() = Body::empty();
                *r.status_mut() = status;
            }
            r
        }
    };
    let h = resp.headers_mut();
    h.insert(
        "x-amz-request-id",
        HeaderValue::from_str(&request_id).unwrap(),
    );
    h.insert(header::SERVER, HeaderValue::from_static("Kosha"));
    let elapsed = started.elapsed();
    let bytes_out = if is_head {
        0
    } else {
        content_length(resp.headers())
    };
    metrics_st
        .metrics
        .observe(op, resp.status().as_u16(), elapsed, bytes_in, bytes_out);
    tracing::debug!(
        %method, %path, op, status = resp.status().as_u16(),
        us = elapsed.as_micros() as u64, "request"
    );
    resp
}

fn content_length(h: &HeaderMap) -> u64 {
    h.get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Prometheus exposition for `/_kosha/metrics`.
async fn metrics_response(st: &AppState) -> S3Result<Response> {
    use crate::metrics::{counter, gauge};
    let mut out = String::new();
    st.metrics.render(&mut out);
    let s = st.engine.stats().await?;
    gauge(&mut out, "kosha_buckets", "Buckets.", s.buckets);
    gauge(&mut out, "kosha_objects", "Objects.", s.objects);
    gauge(
        &mut out,
        "kosha_logical_bytes",
        "Sum of object sizes as clients see them.",
        s.logical_bytes,
    );
    gauge(
        &mut out,
        "kosha_unique_bytes",
        "Bytes of unique chunks (after dedup).",
        s.unique_bytes,
    );
    gauge(
        &mut out,
        "kosha_physical_bytes",
        "Bytes of chunks on disk (after dedup and compression), per replica.",
        s.physical_bytes,
    );
    gauge(&mut out, "kosha_chunks", "Unique chunks.", s.chunks);
    gauge(
        &mut out,
        "kosha_cold_bytes",
        "Bytes of chunks stored in the cold tier.",
        s.cold_bytes,
    );
    let rot = st.engine.rotation_status();
    gauge(
        &mut out,
        "kosha_rotation_pending",
        "Chunks, shards and cold copies still under an old encryption key.",
        rot.chunks_pending + rot.shards_pending + rot.remote_pending,
    );
    gauge(
        &mut out,
        "kosha_rotation_legacy_shards",
        "Shards in the pre-rotation format (readable only with their original key).",
        rot.legacy_shards,
    );
    gauge(
        &mut out,
        "kosha_cold_chunks",
        "Chunks stored in the cold tier.",
        s.cold_chunks,
    );
    {
        use std::sync::atomic::Ordering::Relaxed;
        let d = &kosha_engine::DIRECT_STATS;
        counter(
            &mut out,
            "kosha_direct_reads_total",
            "Chunk files read with O_DIRECT.",
            d.reads.load(Relaxed),
        );
        counter(
            &mut out,
            "kosha_direct_writes_total",
            "Chunk files written with O_DIRECT.",
            d.writes.load(Relaxed),
        );
        counter(
            &mut out,
            "kosha_direct_fallbacks_total",
            "Large chunk I/O that went through the page cache instead.",
            d.fallbacks.load(Relaxed),
        );
    }
    let w = st.engine.write_stats();
    counter(
        &mut out,
        "kosha_commit_batches_total",
        "Local metadata transactions (single node).",
        w.commit_batches,
    );
    counter(
        &mut out,
        "kosha_commit_commands_total",
        "Commands carried by those transactions.",
        w.commit_commands,
    );
    counter(
        &mut out,
        "kosha_sync_requests_total",
        "Chunk writes that waited for durability (group sync).",
        w.sync_requests,
    );
    counter(
        &mut out,
        "kosha_sync_calls_total",
        "syncfs calls that made them durable.",
        w.sync_calls,
    );
    counter(
        &mut out,
        "kosha_events_delivered_total",
        "Event notifications delivered to webhooks by this node.",
        crate::notify::STATS
            .delivered
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    counter(
        &mut out,
        "kosha_events_failed_total",
        "Webhook delivery attempts that failed (retried).",
        crate::notify::STATS
            .failed
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    counter(
        &mut out,
        "kosha_events_dropped_total",
        "Events discarded because a target's queue was full.",
        s.events_dropped,
    );
    if let Ok(pending) = st.engine.pending_event_counts() {
        crate::metrics::header(
            &mut out,
            "kosha_events_pending",
            "gauge",
            "Undelivered event notifications per target.",
        );
        for (target, n) in pending {
            out.push_str(&format!(
                "kosha_events_pending{{target=\"{target}\"}} {n}\n"
            ));
        }
    }
    crate::metrics::header(&mut out, "kosha_info", "gauge", "Build and configuration.");
    out.push_str(&format!(
        "kosha_info{{version=\"{}\",io_backend=\"{}\",sync=\"{:?}\"}} 1\n",
        env!("CARGO_PKG_VERSION"),
        st.engine.io_backend(),
        st.engine.config().sync
    ));
    if let Some(c) = &st.cfg.cluster {
        out.push_str(&c.prometheus());
    }
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(out))
        .unwrap())
}

fn rand_u64() -> u64 {
    let u = uuid::Uuid::new_v4();
    u.as_u64_pair().0
}

async fn handle(st: Arc<AppState>, req: Request) -> S3Result<Response> {
    let (parts, body) = req.into_parts();
    let raw_path = parts.uri.path().to_string();

    if raw_path == "/_kosha/health" {
        return Ok((StatusCode::OK, "ok\n").into_response());
    }
    if raw_path == "/_kosha/metrics" && st.cfg.metrics_public {
        return metrics_response(&st).await;
    }

    // CORS preflights are never signed.
    if parts.method == Method::OPTIONS {
        let (bucket, _) = resolve_target(&st, &parts.headers, &raw_path);
        let rules = bucket
            .as_deref()
            .and_then(|b| st.engine.bucket_local(b).ok())
            .and_then(|b| b.config.cors);
        return Ok(crate::config::preflight(rules.as_deref(), &parts.headers));
    }

    let query = auth::parse_query(parts.uri.query());
    let authn = auth::authenticate(
        &st.cfg.credentials,
        &AuthRequest {
            method: &parts.method,
            raw_path: &raw_path,
            query: &query,
            headers: &parts.headers,
        },
    )?;
    let payload = authn.payload;
    let open = st.cfg.credentials.is_empty()
        || authn
            .access_key
            .as_deref()
            .is_some_and(|k| st.cfg.credentials.is_root(k));
    let (principal, identity) = match (&authn.access_key, open) {
        (_, true) | (None, false) => (Principal::Anonymous, None),
        (Some(ak), false) => {
            let user = st
                .engine
                .user_local(ak)?
                .ok_or_else(|| S3Error::access_denied("unknown access key"))?;
            let identity = user
                .policy
                .as_deref()
                .and_then(|p| Policy::parse(p, PolicyKind::Identity).ok());
            (
                Principal::User {
                    name: user.name,
                    access_key: user.access_key,
                },
                identity,
            )
        }
    };

    if raw_path.starts_with("/_kosha/") && !open {
        return Err(S3Error::access_denied(if authn.access_key.is_none() {
            "anonymous access is disabled"
        } else {
            "administration requires a root access key"
        }));
    }
    if let Some(rest) = raw_path.strip_prefix("/_kosha/admin/") {
        return crate::admin::handle(&st, &parts.method, rest, body, payload, &parts.headers).await;
    }
    if raw_path == "/_kosha/stats" {
        return stats(&st).await;
    }
    if raw_path == "/_kosha/metrics" {
        return metrics_response(&st).await;
    }
    if raw_path == "/_kosha/cluster" {
        let body = match &st.cfg.cluster {
            Some(c) => c.status(),
            None => serde_json::json!({ "mode": "single-node" }),
        };
        return Ok(axum::Json(body).into_response());
    }
    if let Some(id) = raw_path.strip_prefix("/_kosha/cluster/nodes/") {
        if parts.method != Method::DELETE {
            return Err(S3Error::new(
                StatusCode::METHOD_NOT_ALLOWED,
                "MethodNotAllowed",
                "use DELETE to remove a node",
            ));
        }
        let Some(cluster) = &st.cfg.cluster else {
            return Err(S3Error::invalid_request(
                "this server is not part of a cluster",
            ));
        };
        let id: u64 = id
            .parse()
            .map_err(|_| S3Error::invalid_argument("node id must be a number"))?;
        cluster
            .remove_node(id)
            .await
            .map_err(|e| S3Error::new(StatusCode::CONFLICT, "InvalidRequest", e))?;
        return Ok(StatusCode::NO_CONTENT.into_response());
    }

    let (bucket, key) = resolve_target(&st, &parts.headers, &raw_path);
    let mut facts = RequestFacts::new(principal);
    if !open {
        request_facts(&mut facts, &st, &parts, &query, &authn.access_key);
    }
    let authz = Authz {
        open,
        identity,
        facts,
        buckets: Default::default(),
    };
    for check in iam::required(
        &parts.method,
        bucket.as_deref(),
        key.as_deref(),
        &query,
        &parts.headers,
    ) {
        if !authz.allows(&st.engine, bucket.as_deref(), &check) {
            if authn.access_key.is_none() && bucket.is_none() {
                return Err(S3Error::access_denied("anonymous access is disabled"));
            }
            return Err(S3Error::access_denied("Access Denied"));
        }
    }
    check_sse_request(&st, &parts.headers)?;
    let ctx = Ctx {
        st,
        method: parts.method,
        headers: parts.headers,
        query,
        bucket,
        key,
        payload,
        authz,
    };
    let result = route(&ctx, body).await.map_err(|e| {
        // Like S3: without s3:ListBucket, a missing key reads as AccessDenied
        // so callers cannot probe which keys exist.
        let e = match (&ctx.bucket, e.code) {
            (Some(b), "NoSuchKey" | "NoSuchVersion")
                if !ctx.may("s3:ListBucket", b, iam::bucket_arn(b)) =>
            {
                S3Error::access_denied("Access Denied")
            }
            _ => e,
        };
        if e.resource.is_none() {
            e.with_resource(ctx.resource())
        } else {
            e
        }
    });
    // Object data is encrypted server-wide when a key is configured.
    let result = result.map(|mut r| {
        if ctx.key.is_some()
            && r.status().is_success()
            && ctx.engine().encryption_fingerprint().is_some()
        {
            r.headers_mut().insert(
                "x-amz-server-side-encryption",
                HeaderValue::from_static("AES256"),
            );
        }
        r
    });
    let origin = ctx.header("origin").map(str::to_string);
    match (origin, ctx.bucket.as_deref()) {
        (Some(origin), Some(bucket)) => {
            let rules = ctx
                .st
                .engine
                .bucket_local(bucket)
                .ok()
                .and_then(|b| b.config.cors);
            let mut resp = match result {
                Ok(r) => r,
                Err(e) => e.into_response(),
            };
            if let Some(rules) = rules {
                crate::config::decorate(&mut resp, &rules, &origin, &ctx.method);
            }
            Ok(resp)
        }
        _ => result,
    }
}

/// Condition keys available to policies.
fn request_facts(
    facts: &mut RequestFacts,
    st: &AppState,
    parts: &axum::http::request::Parts,
    query: &[(String, String)],
    access_key: &Option<String>,
) {
    if let Some(ci) = parts
        .extensions
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
    {
        facts.set("aws:SourceIp", ci.0.ip().to_string());
    }
    facts.set("aws:SecureTransport", st.cfg.secure_transport.to_string());
    facts.set("aws:CurrentTime", Utc::now().to_rfc3339());
    facts.set("aws:EpochTime", Utc::now().timestamp().to_string());
    for (h, key) in [
        ("user-agent", "aws:UserAgent"),
        ("referer", "aws:Referer"),
        (
            "x-amz-server-side-encryption",
            "s3:x-amz-server-side-encryption",
        ),
        ("x-amz-acl", "s3:x-amz-acl"),
        ("x-amz-copy-source", "s3:x-amz-copy-source"),
        ("x-amz-metadata-directive", "s3:x-amz-metadata-directive"),
        ("x-amz-storage-class", "s3:x-amz-storage-class"),
        ("x-amz-object-lock-mode", "s3:object-lock-mode"),
    ] {
        if let Some(v) = parts.headers.get(h).and_then(|v| v.to_str().ok()) {
            facts.set(key, v);
        }
    }
    for (q, key) in [
        ("prefix", "s3:prefix"),
        ("delimiter", "s3:delimiter"),
        ("max-keys", "s3:max-keys"),
        ("versionId", "s3:VersionId"),
    ] {
        if let Some((_, v)) = query.iter().find(|(k, _)| k == q) {
            facts.set(key, v.clone());
        }
    }
    if access_key.is_some() {
        let v4 = parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("AWS4"))
            || query.iter().any(|(k, _)| k == "X-Amz-Signature");
        facts.set(
            "s3:signatureversion",
            if v4 { "AWS4-HMAC-SHA256" } else { "AWS" },
        );
        let header = parts.headers.contains_key("authorization");
        facts.set(
            "s3:authType",
            if header {
                "REST-HEADER"
            } else {
                "REST-QUERY-STRING"
            },
        );
    }
}

/// Server-side encryption requested by a client. Kosha encrypts server-wide
/// (SSE-S3 semantics) when started with a key; a request for encryption
/// that cannot be honoured is refused rather than stored in plaintext.
fn check_sse_request(st: &AppState, headers: &HeaderMap) -> S3Result<()> {
    if headers.contains_key("x-amz-server-side-encryption-customer-algorithm")
        || headers.contains_key("x-amz-copy-source-server-side-encryption-customer-algorithm")
    {
        return Err(S3Error::not_implemented("SSE-C (customer-provided keys)"));
    }
    let Some(v) = headers
        .get("x-amz-server-side-encryption")
        .and_then(|v| v.to_str().ok())
    else {
        return Ok(());
    };
    match v {
        "AES256" if st.engine.encryption_fingerprint().is_some() => Ok(()),
        "AES256" => Err(S3Error::invalid_argument(
            "Server-side encryption was requested, but this server stores data unencrypted (start it with --encryption-key-file)",
        )),
        "aws:kms" | "aws:kms:dsse" => Err(S3Error::not_implemented("SSE-KMS")),
        other => Err(S3Error::invalid_argument(format!(
            "unsupported x-amz-server-side-encryption: {other}"
        ))),
    }
}

/// Get/Put/DeleteBucketEncryption. Encryption is a server-wide setting, so
/// buckets report it and accept only a configuration that matches it.
async fn bucket_encryption(ctx: &Ctx, bucket: &str, body: Body) -> S3Result<Response> {
    let engine = ctx.engine();
    let enabled = engine.encryption_fingerprint().is_some();
    match ctx.method {
        Method::GET => {
            engine.bucket(bucket).await?;
            if !enabled {
                return Err(S3Error::new(
                    StatusCode::NOT_FOUND,
                    "ServerSideEncryptionConfigurationNotFoundError",
                    "The server side encryption configuration was not found",
                ));
            }
            let mut x = Xml::new("ServerSideEncryptionConfiguration");
            x.open("Rule")
                .open("ApplyServerSideEncryptionByDefault")
                .elem("SSEAlgorithm", "AES256")
                .close("ApplyServerSideEncryptionByDefault")
                .elem("BucketKeyEnabled", "false")
                .close("Rule");
            Ok(xml_response(x.finish("ServerSideEncryptionConfiguration")))
        }
        Method::PUT => {
            let doc = read_text(ctx, body).await?;
            engine.bucket(bucket).await?;
            if !enabled {
                return Err(S3Error::not_implemented(
                    "per-bucket encryption (start the server with --encryption-key-file to encrypt all data)",
                ));
            }
            if !doc.contains("AES256") || doc.contains("aws:kms") {
                return Err(S3Error::not_implemented("SSE-KMS"));
            }
            Ok(StatusCode::OK.into_response())
        }
        Method::DELETE => {
            // Like S3 since 2023: removing the configuration leaves the
            // default (SSE-S3) in place.
            engine.bucket(bucket).await?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        _ => Err(S3Error::not_implemented("this bucket encryption operation")),
    }
}

/// Get/Put/Delete/ListBucketIntelligentTieringConfiguration(s).
async fn bucket_tiering(ctx: &Ctx, bucket: &str, body: Body) -> S3Result<Response> {
    let engine = ctx.engine();
    let rules = engine
        .bucket(bucket)
        .await?
        .config
        .tiering
        .unwrap_or_default();
    let id = ctx.q("id").map(str::to_string);
    let no_config = || {
        S3Error::new(
            StatusCode::NOT_FOUND,
            "NoSuchConfiguration",
            "The specified configuration does not exist.",
        )
    };
    match (&ctx.method, id) {
        (&Method::GET, None) => Ok(xml_response(crate::tiering::render_list(&rules))),
        (&Method::GET, Some(id)) => rules
            .iter()
            .find(|r| r.id == id)
            .map(|r| xml_response(crate::tiering::render(r)))
            .ok_or_else(no_config),
        (&Method::PUT, Some(id)) => {
            let rule = crate::tiering::parse(&read_text(ctx, body).await?, &id)?;
            if !engine.can_tier() {
                return Err(S3Error::invalid_request(
                    "Intelligent-Tiering needs a cold tier, and none is configured on this server",
                ));
            }
            let mut rules: Vec<_> = rules.into_iter().filter(|r| r.id != id).collect();
            rules.push(rule);
            engine
                .set_bucket_config(bucket, BucketConfigUpdate::Tiering(Some(rules)))
                .await?;
            Ok(StatusCode::OK.into_response())
        }
        (&Method::DELETE, Some(id)) => {
            if !rules.iter().any(|r| r.id == id) {
                return Err(no_config());
            }
            let rules: Vec<_> = rules.into_iter().filter(|r| r.id != id).collect();
            engine
                .set_bucket_config(
                    bucket,
                    BucketConfigUpdate::Tiering((!rules.is_empty()).then_some(rules)),
                )
                .await?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        _ => Err(S3Error::invalid_argument("the id parameter is required")),
    }
}

/// Get/PutBucketNotificationConfiguration.
async fn bucket_notification(ctx: &Ctx, bucket: &str, body: Body) -> S3Result<Response> {
    let engine = ctx.engine();
    match ctx.method {
        Method::GET => {
            let rules = engine.bucket(bucket).await?.config.notifications;
            Ok(xml_response(crate::notify::render(
                rules.as_deref().unwrap_or_default(),
            )))
        }
        Method::PUT => {
            let doc = read_text(ctx, body).await?;
            engine.bucket(bucket).await?;
            let rules = crate::notify::parse(&doc, &ctx.st.cfg.webhooks)?;
            engine
                .set_bucket_config(bucket, BucketConfigUpdate::Notifications(rules))
                .await?;
            Ok(StatusCode::OK.into_response())
        }
        _ => Err(S3Error::not_implemented("this notification operation")),
    }
}

/// Get/Put/DeleteBucketPolicy and GetBucketPolicyStatus.
async fn bucket_policy(ctx: &Ctx, bucket: &str, body: Body) -> S3Result<Response> {
    let engine = ctx.engine();
    if ctx.has("policyStatus") {
        let info = engine.bucket(bucket).await?;
        let public = info
            .config
            .policy
            .as_deref()
            .and_then(|p| Policy::parse(p, PolicyKind::Bucket(bucket)).ok())
            .is_some_and(|p| p.is_public());
        let mut x = Xml::new("PolicyStatus");
        x.elem("IsPublic", if public { "true" } else { "false" });
        return Ok(xml_response(x.finish("PolicyStatus")));
    }
    match ctx.method {
        Method::GET => {
            let info = engine.bucket(bucket).await?;
            let Some(doc) = info.config.policy else {
                return Err(S3Error::new(
                    StatusCode::NOT_FOUND,
                    "NoSuchBucketPolicy",
                    "The bucket policy does not exist",
                ));
            };
            Ok(Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(doc))
                .unwrap())
        }
        Method::PUT => {
            let doc = read_text(ctx, body).await?;
            if doc.len() > 20 * 1024 {
                return Err(S3Error::new(
                    StatusCode::BAD_REQUEST,
                    "PolicyTooLarge",
                    "Policies must be at most 20 KiB",
                ));
            }
            Policy::parse(&doc, PolicyKind::Bucket(bucket))
                .map_err(|e| S3Error::new(StatusCode::BAD_REQUEST, "MalformedPolicy", e))?;
            engine
                .set_bucket_config(bucket, BucketConfigUpdate::Policy(Some(doc)))
                .await?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Method::DELETE => {
            engine
                .set_bucket_config(bucket, BucketConfigUpdate::Policy(None))
                .await?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        _ => Err(S3Error::not_implemented("this bucket policy operation")),
    }
}

async fn read_text(ctx: &Ctx, body: Body) -> S3Result<String> {
    let md5 = content_md5(&ctx.headers)?;
    let raw = BodySource::new(body, ctx.payload.clone(), &ctx.headers)?
        .collect(MAX_XML_BODY)
        .await?;
    if md5.is_some() {
        use md5::Digest;
        check_md5(md5, &md5::Md5::digest(&raw).into())?;
    }
    String::from_utf8(raw.to_vec()).map_err(|_| S3Error::malformed_xml("body is not UTF-8"))
}

/// Bucket configuration subresources (tagging, CORS).
async fn bucket_config(ctx: &Ctx, bucket: &str, body: Body) -> S3Result<Option<Response>> {
    use crate::config::*;
    let engine = ctx.engine();
    let m = &ctx.method;
    if ctx.has("tagging") {
        return Ok(Some(match *m {
            Method::PUT => {
                let tags = parse_tagging(&read_text(ctx, body).await?, true)?;
                engine
                    .set_bucket_config(bucket, BucketConfigUpdate::Tags(tags))
                    .await?;
                StatusCode::NO_CONTENT.into_response()
            }
            Method::DELETE => {
                engine
                    .set_bucket_config(bucket, BucketConfigUpdate::Tags(Default::default()))
                    .await?;
                StatusCode::NO_CONTENT.into_response()
            }
            _ => {
                let info = engine.bucket(bucket).await?;
                if info.config.tags.is_empty() {
                    return Err(S3Error::new(
                        StatusCode::NOT_FOUND,
                        "NoSuchTagSet",
                        "The TagSet does not exist",
                    ));
                }
                xml_response(render_tagging(&info.config.tags))
            }
        }));
    }
    if ctx.has("cors") {
        return Ok(Some(match *m {
            Method::PUT => {
                let update = parse_cors(&read_text(ctx, body).await?)?;
                engine.set_bucket_config(bucket, update).await?;
                StatusCode::OK.into_response()
            }
            Method::DELETE => {
                engine
                    .set_bucket_config(bucket, BucketConfigUpdate::Cors(None))
                    .await?;
                StatusCode::NO_CONTENT.into_response()
            }
            _ => match engine.bucket(bucket).await?.config.cors {
                Some(rules) => xml_response(render_cors(&rules)),
                None => {
                    return Err(S3Error::new(
                        StatusCode::NOT_FOUND,
                        "NoSuchCORSConfiguration",
                        "The CORS configuration does not exist",
                    ));
                }
            },
        }));
    }
    if ctx.has("versioning") {
        return Ok(Some(match *m {
            Method::PUT => {
                let v = crate::protection::parse_versioning(&read_text(ctx, body).await?)?;
                engine
                    .set_bucket_config(bucket, BucketConfigUpdate::Versioning(v))
                    .await?;
                StatusCode::OK.into_response()
            }
            _ => xml_response(crate::protection::render_versioning(
                engine.bucket(bucket).await?.config.versioning,
            )),
        }));
    }
    if ctx.has("object-lock") {
        return Ok(Some(match *m {
            Method::PUT => {
                let c = crate::protection::parse_lock_config(&read_text(ctx, body).await?)?;
                engine
                    .set_bucket_config(bucket, BucketConfigUpdate::ObjectLock(c))
                    .await?;
                StatusCode::OK.into_response()
            }
            _ => match engine.bucket(bucket).await?.config.object_lock {
                Some(c) => xml_response(crate::protection::render_lock_config(&c)),
                None => {
                    return Err(S3Error::new(
                        StatusCode::NOT_FOUND,
                        "ObjectLockConfigurationNotFoundError",
                        "Object Lock configuration does not exist for this bucket",
                    ));
                }
            },
        }));
    }
    if ctx.has("lifecycle") {
        return Ok(Some(match *m {
            Method::PUT => {
                let rules = crate::protection::parse_lifecycle(&read_text(ctx, body).await?)?;
                if rules.iter().any(|r| {
                    r.transition_class.is_some() || r.noncurrent_transition_class.is_some()
                }) && !engine.can_tier()
                {
                    return Err(S3Error::invalid_request(
                        "Transition rules need a cold tier, and none is configured on this server",
                    ));
                }
                engine
                    .set_bucket_config(bucket, BucketConfigUpdate::Lifecycle(Some(rules)))
                    .await?;
                StatusCode::OK.into_response()
            }
            Method::DELETE => {
                engine
                    .set_bucket_config(bucket, BucketConfigUpdate::Lifecycle(None))
                    .await?;
                StatusCode::NO_CONTENT.into_response()
            }
            _ => match engine.bucket(bucket).await?.config.lifecycle {
                Some(rules) => xml_response(crate::protection::render_lifecycle(&rules)),
                None => {
                    return Err(S3Error::new(
                        StatusCode::NOT_FOUND,
                        "NoSuchLifecycleConfiguration",
                        "The lifecycle configuration does not exist",
                    ));
                }
            },
        }));
    }
    Ok(None)
}

/// Object tagging subresource.
async fn object_tagging(ctx: &Ctx, bucket: &str, key: &str, body: Body) -> S3Result<Response> {
    use crate::config::*;
    match ctx.method {
        Method::PUT => {
            let tags = parse_tagging(&read_text(ctx, body).await?, false)?;
            ctx.engine().set_object_tags(bucket, key, tags).await?;
            Ok(StatusCode::OK.into_response())
        }
        Method::DELETE => {
            ctx.engine()
                .set_object_tags(bucket, key, Default::default())
                .await?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        _ => {
            let meta = ctx.engine().head_object(bucket, key).await?;
            Ok(xml_response(render_tagging(&meta.attrs.tags)))
        }
    }
}

fn decode_path(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}

/// Work out bucket and key from a virtual-hosted or path-style request.
fn resolve_target(
    st: &AppState,
    headers: &HeaderMap,
    raw_path: &str,
) -> (Option<String>, Option<String>) {
    let path = raw_path.strip_prefix('/').unwrap_or(raw_path);
    if let Some(domain) = &st.cfg.domain {
        let host = headers
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default();
        let host = host.rsplit_once(':').map(|(h, _)| h).unwrap_or(host);
        if let Some(bucket) = host
            .strip_suffix(domain.as_str())
            .and_then(|b| b.strip_suffix('.'))
        {
            let key = (!path.is_empty()).then(|| decode_path(path));
            return (Some(bucket.to_string()), key);
        }
    }
    if path.is_empty() {
        return (None, None);
    }
    let mut it = path.splitn(2, '/');
    let bucket = it.next().map(decode_path);
    let key = it.next().filter(|k| !k.is_empty()).map(decode_path);
    (bucket, key)
}

async fn route(ctx: &Ctx, body: Body) -> S3Result<Response> {
    let m = &ctx.method;
    match (&ctx.bucket, &ctx.key) {
        (None, _) => match *m {
            Method::GET => list_buckets(ctx).await,
            _ => Err(S3Error::new(
                StatusCode::METHOD_NOT_ALLOWED,
                "MethodNotAllowed",
                "The specified method is not allowed against this resource.",
            )),
        },
        (Some(bucket), None) => {
            let bucket = bucket.as_str();
            if ctx.has("policy") || ctx.has("policyStatus") {
                return bucket_policy(ctx, bucket, body).await;
            }
            if ctx.has("encryption") {
                return bucket_encryption(ctx, bucket, body).await;
            }
            if ctx.has("notification") {
                return bucket_notification(ctx, bucket, body).await;
            }
            if ctx.has("intelligent-tiering") {
                return bucket_tiering(ctx, bucket, body).await;
            }
            if ["tagging", "cors", "versioning", "object-lock", "lifecycle"]
                .iter()
                .any(|s| ctx.has(s))
            {
                if let Some(resp) = bucket_config(ctx, bucket, body).await? {
                    return Ok(resp);
                }
                return Err(S3Error::internal("unhandled bucket subresource"));
            }
            match *m {
                Method::PUT => {
                    if ctx.has("acl") {
                        ctx.engine().bucket(bucket).await?;
                        return put_acl(ctx, body).await;
                    }
                    for sub in ["object-lock", "website", "replication", "logging"] {
                        if ctx.has(sub) {
                            return Err(S3Error::not_implemented(&format!("PutBucket {sub}")));
                        }
                    }
                    let lock = ctx
                        .header("x-amz-bucket-object-lock-enabled")
                        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
                    ctx.engine().create_bucket_with(bucket, lock).await?;
                    Ok(Response::builder()
                        .header(header::LOCATION, format!("/{bucket}"))
                        .body(Body::empty())
                        .unwrap())
                }
                Method::DELETE => {
                    ctx.engine().delete_bucket(bucket).await?;
                    Ok(StatusCode::NO_CONTENT.into_response())
                }
                Method::HEAD => {
                    ctx.engine().bucket(bucket).await?;
                    Ok(Response::builder()
                        .header("x-amz-bucket-region", &ctx.st.cfg.region)
                        .body(Body::empty())
                        .unwrap())
                }
                Method::GET => get_bucket(ctx, bucket).await,
                Method::POST if ctx.has("delete") => delete_objects(ctx, bucket, body).await,
                _ => Err(S3Error::not_implemented(&format!("{m} on a bucket"))),
            }
        }
        (Some(bucket), Some(key)) => {
            let (bucket, key) = (bucket.as_str(), key.as_str());
            match *m {
                Method::PUT if ctx.has("uploadId") => upload_part(ctx, bucket, key, body).await,
                Method::PUT | Method::GET | Method::DELETE if ctx.has("tagging") => {
                    object_tagging(ctx, bucket, key, body).await
                }
                Method::PUT | Method::GET if ctx.has("retention") || ctx.has("legal-hold") => {
                    object_lock_subresource(ctx, bucket, key, body).await
                }
                Method::PUT if ctx.has("acl") => {
                    resolve_object(ctx, bucket, key).await?;
                    put_acl(ctx, body).await
                }
                Method::GET if ctx.has("acl") => {
                    resolve_object(ctx, bucket, key).await?;
                    Ok(owner_acl())
                }
                Method::POST if ctx.has("restore") => restore_object(ctx, bucket, key).await,
                Method::PUT if ctx.headers.contains_key("x-amz-copy-source") => {
                    copy_object(ctx, bucket, key).await
                }
                Method::PUT => put_object(ctx, bucket, key, body).await,
                Method::GET if ctx.has("uploadId") => list_parts(ctx, bucket, key).await,
                Method::GET | Method::HEAD => get_object(ctx, bucket, key).await,
                Method::DELETE if ctx.has("uploadId") => {
                    ctx.engine()
                        .abort_upload(bucket, key, ctx.q("uploadId").unwrap_or_default())
                        .await?;
                    Ok(StatusCode::NO_CONTENT.into_response())
                }
                Method::DELETE => delete_object(ctx, bucket, key).await,
                Method::POST if ctx.has("uploads") => create_upload(ctx, bucket, key).await,
                Method::POST if ctx.has("uploadId") => {
                    complete_upload(ctx, bucket, key, body).await
                }
                _ => Err(S3Error::not_implemented(&format!("{m} on an object"))),
            }
        }
    }
}

fn xml_response(body: String) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(body))
        .unwrap()
}

fn quoted(etag: &str) -> String {
    format!("\"{etag}\"")
}

fn http_date(t: &DateTime<Utc>) -> String {
    t.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn parse_http_date(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(s)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

// ------------------------------------------------------------------ service

async fn list_buckets(ctx: &Ctx) -> S3Result<Response> {
    let buckets = ctx.engine().list_buckets().await?;
    let mut x = Xml::new("ListAllMyBucketsResult");
    x.open("Owner")
        .elem("ID", OWNER_ID)
        .elem("DisplayName", OWNER_ID)
        .close("Owner");
    x.open("Buckets");
    for b in &buckets {
        x.open("Bucket")
            .elem("Name", &b.name)
            .elem("CreationDate", iso8601(&b.created))
            .close("Bucket");
    }
    x.close("Buckets");
    Ok(xml_response(x.finish("ListAllMyBucketsResult")))
}

async fn stats(st: &AppState) -> S3Result<Response> {
    let s = st.engine.stats().await?;
    let ratio = |a: u64, b: u64| if b == 0 { 1.0 } else { a as f64 / b as f64 };
    // Stored bytes live locally or in the cold tier.
    let stored = s.physical_bytes + s.cold_bytes;
    let rot = st.engine.rotation_status();
    let body = format!(
        "{{\"io_backend\":\"{}\",\"buckets\":{},\"objects\":{},\"logical_bytes\":{},\"unique_bytes\":{},\"physical_bytes\":{},\"cold_bytes\":{},\"chunks\":{},\"cold_chunks\":{},\"dedup_ratio\":{:.3},\"compression_ratio\":{:.3},\"overall_ratio\":{:.3},\"rotation\":{}}}\n",
        st.engine.io_backend(),
        s.buckets,
        s.objects,
        s.logical_bytes,
        s.unique_bytes,
        s.physical_bytes,
        s.cold_bytes,
        s.chunks,
        s.cold_chunks,
        ratio(s.logical_bytes, s.unique_bytes),
        ratio(s.unique_bytes, stored),
        ratio(s.logical_bytes, stored),
        serde_json::to_string(&rot).unwrap_or_default(),
    );
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap())
}

// -------------------------------------------------------------------- ACLs

/// Kosha's only ACL: the owner has full control. Access for anyone else is
/// granted with bucket and user policies.
fn owner_acl() -> Response {
    let mut x = Xml::new("AccessControlPolicy");
    x.open("Owner")
        .elem("ID", OWNER_ID)
        .elem("DisplayName", OWNER_ID)
        .close("Owner");
    x.open("AccessControlList")
        .open("Grant")
        .raw("<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">")
        .elem("ID", OWNER_ID)
        .elem("DisplayName", OWNER_ID)
        .close("Grantee")
        .elem("Permission", "FULL_CONTROL")
        .close("Grant")
        .close("AccessControlList");
    xml_response(x.finish("AccessControlPolicy"))
}

/// PutBucketAcl / PutObjectAcl: accept what Kosha already enforces (the
/// `private` canned ACL, or an ACL granting only the owner full control);
/// refuse anything that would grant more, rather than ignore it.
async fn put_acl(ctx: &Ctx, body: Body) -> S3Result<Response> {
    let refuse = || {
        S3Error::not_implemented(
            "ACLs other than private (grant access with bucket or user policies instead)",
        )
    };
    if ctx
        .headers
        .keys()
        .any(|k| k.as_str().starts_with("x-amz-grant-"))
    {
        return Err(refuse());
    }
    if ctx.header("x-amz-acl").is_some_and(|a| a != "private") {
        return Err(refuse());
    }
    let doc = read_text(ctx, body).await?;
    if !doc.trim().is_empty() {
        let grants = doc.matches("<Permission>").count();
        let full_control = doc.matches("<Permission>FULL_CONTROL</Permission>").count();
        let only_owner = doc
            .split("<ID>")
            .skip(1)
            .all(|rest| rest.starts_with(&format!("{OWNER_ID}</ID>")));
        if grants != full_control
            || !only_owner
            || doc.contains("<URI>")
            || doc.contains("<EmailAddress>")
        {
            return Err(refuse());
        }
    }
    Ok(StatusCode::OK.into_response())
}

/// RestoreObject. Cold data is read directly, so a cold object is already
/// available; STANDARD objects are refused like S3 does.
async fn restore_object(ctx: &Ctx, bucket: &str, key: &str) -> S3Result<Response> {
    let meta = resolve_object(ctx, bucket, key).await?;
    if meta.storage_class.is_none() {
        return Err(S3Error::new(
            StatusCode::FORBIDDEN,
            "InvalidObjectState",
            "Restore is not allowed for the object's current storage class",
        ));
    }
    Ok(StatusCode::OK.into_response())
}

// ------------------------------------------------------------------- bucket

async fn get_bucket(ctx: &Ctx, bucket: &str) -> S3Result<Response> {
    if ctx.has("location") {
        ctx.engine().bucket(bucket).await?;
        let region = &ctx.st.cfg.region;
        let mut x = Xml::new("LocationConstraint");
        if region != "us-east-1" {
            x.elem_text(region);
        }
        return Ok(xml_response(x.finish("LocationConstraint")));
    }
    if ctx.has("versions") {
        return list_versions(ctx, bucket).await;
    }
    if ctx.has("uploads") {
        return list_uploads(ctx, bucket).await;
    }
    if ctx.has("acl") {
        ctx.engine().bucket(bucket).await?;
        return Ok(owner_acl());
    }
    // Configuration that Kosha does not store yet: answer the way S3 does
    // for a bucket where it was never set.
    let absent = [
        (
            "website",
            StatusCode::NOT_FOUND,
            "NoSuchWebsiteConfiguration",
            "The specified bucket does not have a website configuration",
        ),
        (
            "replication",
            StatusCode::NOT_FOUND,
            "ReplicationConfigurationNotFoundError",
            "The replication configuration was not found",
        ),
    ];
    for (sub, status, code, msg) in absent {
        if ctx.has(sub) {
            ctx.engine().bucket(bucket).await?;
            return Err(S3Error::new(status, code, msg));
        }
    }
    list_objects(ctx, bucket).await
}

async fn list_objects(ctx: &Ctx, bucket: &str) -> S3Result<Response> {
    let v2 = ctx.q("list-type") == Some("2");
    let url = ctx.q("encoding-type") == Some("url");
    let enc = |s: &str| {
        if url {
            auth::uri_encode(s).replace("%2F", "/")
        } else {
            s.to_string()
        }
    };
    let max_keys = match ctx.q("max-keys") {
        Some(v) => v
            .parse::<usize>()
            .map_err(|_| S3Error::invalid_argument("max-keys must be a non-negative integer"))?
            .min(MAX_KEYS),
        None => MAX_KEYS,
    };
    let prefix = ctx.q("prefix").unwrap_or_default().to_string();
    let delimiter = ctx
        .q("delimiter")
        .filter(|d| !d.is_empty())
        .map(str::to_string);
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;

    let start_after = if v2 {
        match ctx.q("continuation-token") {
            Some(t) => Some(
                String::from_utf8(
                    b64.decode(t)
                        .map_err(|_| S3Error::invalid_argument("invalid continuation token"))?,
                )
                .map_err(|_| S3Error::invalid_argument("invalid continuation token"))?,
            ),
            None => ctx.q("start-after").map(str::to_string),
        }
    } else {
        ctx.q("marker").map(str::to_string)
    };

    let page = ctx
        .engine()
        .list_objects(
            bucket,
            ListQuery {
                prefix: prefix.clone(),
                delimiter: delimiter.clone(),
                start_after,
                max_keys,
            },
        )
        .await?;

    let mut x = Xml::new("ListBucketResult");
    x.elem("Name", bucket).elem("Prefix", enc(&prefix));
    if let Some(d) = &delimiter {
        x.elem("Delimiter", enc(d));
    }
    x.elem("MaxKeys", max_keys.to_string());
    if url {
        x.elem("EncodingType", "url");
    }
    x.elem("IsTruncated", page.is_truncated.to_string());
    if v2 {
        x.elem(
            "KeyCount",
            (page.objects.len() + page.common_prefixes.len()).to_string(),
        );
        x.opt("ContinuationToken", ctx.q("continuation-token"));
        x.opt("StartAfter", ctx.q("start-after").map(enc));
        x.opt(
            "NextContinuationToken",
            page.next_marker.as_ref().map(|m| b64.encode(m)),
        );
    } else {
        x.elem("Marker", enc(ctx.q("marker").unwrap_or_default()));
        x.opt("NextMarker", page.next_marker.as_deref().map(enc));
    }
    let fetch_owner = !v2 || ctx.q("fetch-owner") == Some("true");
    for o in &page.objects {
        x.open("Contents")
            .elem("Key", enc(&o.key))
            .elem("LastModified", iso8601(&o.last_modified))
            .elem("ETag", quoted(&o.etag))
            .elem("Size", o.size.to_string())
            .elem(
                "StorageClass",
                o.storage_class.as_deref().unwrap_or("STANDARD"),
            );
        if fetch_owner {
            x.open("Owner")
                .elem("ID", OWNER_ID)
                .elem("DisplayName", OWNER_ID)
                .close("Owner");
        }
        x.close("Contents");
    }
    for p in &page.common_prefixes {
        x.open("CommonPrefixes")
            .elem("Prefix", enc(p))
            .close("CommonPrefixes");
    }
    Ok(xml_response(x.finish("ListBucketResult")))
}

#[derive(Deserialize)]
struct DeleteRequest {
    #[serde(rename = "Object", default)]
    objects: Vec<DeleteEntry>,
    #[serde(rename = "Quiet", default)]
    quiet: bool,
}

#[derive(Deserialize)]
struct DeleteEntry {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: Option<String>,
}

async fn read_xml<T: for<'de> Deserialize<'de>>(ctx: &Ctx, body: Body) -> S3Result<T> {
    let md5 = content_md5(&ctx.headers)?;
    let raw = BodySource::new(body, ctx.payload.clone(), &ctx.headers)?
        .collect(MAX_XML_BODY)
        .await?;
    if md5.is_some() {
        use md5::Digest;
        check_md5(md5, &md5::Md5::digest(&raw).into())?;
    }
    let text =
        std::str::from_utf8(&raw).map_err(|_| S3Error::malformed_xml("body is not UTF-8"))?;
    quick_xml::de::from_str(text).map_err(|e| S3Error::malformed_xml(e.to_string()))
}

async fn delete_objects(ctx: &Ctx, bucket: &str, body: Body) -> S3Result<Response> {
    let req: DeleteRequest = read_xml(ctx, body).await?;
    if req.objects.len() > 1000 {
        return Err(S3Error::malformed_xml(
            "at most 1000 keys can be deleted per request",
        ));
    }
    let bypass = crate::protection::bypass_governance(&ctx.headers);
    let mut denied = Vec::new();
    let targets = req
        .objects
        .into_iter()
        .map(|o| kosha_engine::DeleteTarget {
            key: o.key,
            version_id: o.version_id,
        })
        .filter(|t| {
            let arn = iam::object_arn(bucket, &t.key);
            let action = if t.version_id.is_some() {
                "s3:DeleteObjectVersion"
            } else {
                "s3:DeleteObject"
            };
            let ok = ctx.may(action, bucket, arn.clone())
                && (!bypass || ctx.may("s3:BypassGovernanceRetention", bucket, arn));
            if !ok {
                denied.push(kosha_engine::DeleteOutcome {
                    key: t.key.clone(),
                    version_id: t.version_id.clone(),
                    delete_marker: false,
                    existed: false,
                    error: Some(kosha_engine::Rejection::AccessDenied(
                        "Access Denied".into(),
                    )),
                });
            }
            ok
        })
        .collect::<Vec<_>>();
    let mut outcomes = if targets.is_empty() {
        Vec::new()
    } else {
        ctx.engine().delete_targets(bucket, targets, bypass).await?
    };
    outcomes.extend(denied);
    let mut x = Xml::new("DeleteResult");
    for o in &outcomes {
        match &o.error {
            Some(rej) => {
                let e = S3Error::from(kosha_engine::Error::from(rej.clone()));
                x.open("Error")
                    .elem("Key", &o.key)
                    .opt("VersionId", o.version_id.as_deref())
                    .elem("Code", e.code)
                    .elem("Message", &e.message)
                    .close("Error");
            }
            None if !req.quiet => {
                x.open("Deleted").elem("Key", &o.key);
                if o.delete_marker {
                    x.elem("DeleteMarker", "true");
                    x.opt("DeleteMarkerVersionId", o.version_id.as_deref());
                } else {
                    x.opt("VersionId", o.version_id.as_deref());
                }
                x.close("Deleted");
            }
            None => {}
        }
    }
    Ok(xml_response(x.finish("DeleteResult")))
}

/// The page of uploads after (`key_marker`, `id_marker`), in (key, upload id)
/// order, plus whether more remain.
fn page_uploads<'a>(
    uploads: &'a [kosha_engine::UploadInfo],
    key_marker: &str,
    id_marker: &str,
    max: usize,
) -> (Vec<&'a kosha_engine::UploadInfo>, bool) {
    let after = |u: &&kosha_engine::UploadInfo| {
        if key_marker.is_empty() {
            true
        } else if id_marker.is_empty() {
            u.key.as_str() > key_marker
        } else {
            u.key.as_str() > key_marker || (u.key == key_marker && u.upload_id.as_str() > id_marker)
        }
    };
    let mut rest: Vec<_> = uploads.iter().filter(after).collect();
    rest.sort_by(|a, b| (&a.key, &a.upload_id).cmp(&(&b.key, &b.upload_id)));
    let truncated = rest.len() > max;
    rest.truncate(max);
    (rest, truncated)
}

async fn list_uploads(ctx: &Ctx, bucket: &str) -> S3Result<Response> {
    let prefix = ctx.q("prefix").unwrap_or_default();
    let key_marker = ctx.q("key-marker").unwrap_or_default();
    let id_marker = ctx.q("upload-id-marker").unwrap_or_default();
    let max = match ctx.q("max-uploads") {
        Some(v) => v
            .parse::<usize>()
            .map_err(|_| S3Error::invalid_argument("max-uploads must be a non-negative integer"))?
            .min(1000),
        None => 1000,
    };
    let uploads = ctx.engine().list_uploads(bucket, prefix).await?;
    let (page, truncated) = page_uploads(&uploads, key_marker, id_marker, max);

    let mut x = Xml::new("ListMultipartUploadsResult");
    x.elem("Bucket", bucket)
        .elem("KeyMarker", key_marker)
        .elem("UploadIdMarker", id_marker)
        .elem("Prefix", prefix)
        .elem("MaxUploads", max.to_string())
        .elem("IsTruncated", truncated.to_string());
    if truncated && let Some(last) = page.last() {
        x.elem("NextKeyMarker", &last.key)
            .elem("NextUploadIdMarker", &last.upload_id);
    }
    for u in &page {
        x.open("Upload")
            .elem("Key", &u.key)
            .elem("UploadId", &u.upload_id)
            .open("Initiator")
            .elem("ID", OWNER_ID)
            .elem("DisplayName", OWNER_ID)
            .close("Initiator")
            .open("Owner")
            .elem("ID", OWNER_ID)
            .elem("DisplayName", OWNER_ID)
            .close("Owner")
            .elem("StorageClass", "STANDARD")
            .elem("Initiated", iso8601(&u.initiated))
            .close("Upload");
    }
    Ok(xml_response(x.finish("ListMultipartUploadsResult")))
}

// ------------------------------------------------------------------- object

/// Attributes from request headers, including `x-amz-tagging`.
fn attrs_with_tags(headers: &HeaderMap) -> S3Result<ObjectAttrs> {
    let mut attrs = attrs_from_headers(headers);
    if let Some(tags) = crate::config::parse_tag_header(headers)? {
        attrs.tags = tags;
    }
    Ok(attrs)
}

fn attrs_from_headers(headers: &HeaderMap) -> ObjectAttrs {
    let mut attrs = ObjectAttrs {
        content_type: headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        ..Default::default()
    };
    for name in STORED_HEADERS {
        if let Some(v) = headers.get(name).and_then(|v| v.to_str().ok()) {
            let v = if name == "content-encoding" {
                // aws-chunked describes the transfer, not the object.
                v.split(',')
                    .map(str::trim)
                    .filter(|e| *e != "aws-chunked" && !e.is_empty())
                    .collect::<Vec<_>>()
                    .join(", ")
            } else {
                v.to_string()
            };
            if !v.is_empty() {
                attrs.headers.insert(name.to_string(), v);
            }
        }
    }
    for (name, value) in headers {
        if let Some(k) = name.as_str().strip_prefix("x-amz-meta-")
            && let Ok(v) = value.to_str()
        {
            attrs.user_meta.insert(k.to_string(), v.to_string());
        }
    }
    attrs
}

fn write_condition(ctx: &Ctx) -> WriteCondition {
    WriteCondition {
        if_none_match_any: ctx.header("if-none-match").is_some_and(|v| v.trim() == "*"),
        if_match: ctx.header("if-match").map(str::to_string),
    }
}

/// Feed a request body through the engine writer.
async fn ingest(ctx: &Ctx, body: Body) -> S3Result<kosha_engine::WrittenData> {
    let md5 = content_md5(&ctx.headers)?;
    let mut src = BodySource::new(body, ctx.payload.clone(), &ctx.headers)?;
    let mut w = ctx.engine().writer();
    while let Some(d) = src.next().await? {
        w.write(&d).await?;
    }
    let data = w.finish().await?;
    if let Some(declared) = ctx
        .header("x-amz-decoded-content-length")
        .and_then(|v| v.parse::<u64>().ok())
        && declared != data.size
    {
        return Err(S3Error::new(
            StatusCode::BAD_REQUEST,
            "IncompleteBody",
            "You did not provide the number of bytes specified by the Content-Length HTTP header.",
        ));
    }
    check_md5(md5, &data.md5)?;
    Ok(data)
}

/// `x-amz-storage-class`: `None` for STANDARD, or a cold class (which
/// needs a configured cold tier).
fn storage_class(ctx: &Ctx) -> S3Result<Option<String>> {
    match ctx.header("x-amz-storage-class") {
        None | Some("STANDARD") | Some("REDUCED_REDUNDANCY") => Ok(None),
        Some(c) if kosha_engine::is_cold_class(c) => {
            if ctx.engine().can_tier() {
                Ok(Some(c.to_string()))
            } else {
                Err(S3Error::new(
                    StatusCode::BAD_REQUEST,
                    "InvalidStorageClass",
                    format!("{c} needs a cold tier, and none is configured on this server"),
                ))
            }
        }
        Some(c) => Err(S3Error::new(
            StatusCode::BAD_REQUEST,
            "InvalidStorageClass",
            format!("The storage class you specified is not valid: {c}"),
        )),
    }
}

async fn put_object(ctx: &Ctx, bucket: &str, key: &str, body: Body) -> S3Result<Response> {
    // Fail fast before reading a potentially large body.
    ctx.engine().bucket(bucket).await?;
    let lock = crate::protection::lock_from_headers(&ctx.headers)?;
    let attrs = attrs_with_tags(&ctx.headers)?;
    let class = storage_class(ctx)?;
    let data = ingest(ctx, body).await?;
    let meta = ctx
        .engine()
        .put_object_as(bucket, key, data, attrs, write_condition(ctx), lock, class)
        .await?;
    let resp = Response::builder()
        .header(header::ETAG, quoted(&meta.etag))
        .body(Body::empty())
        .unwrap();
    Ok(with_version(resp, meta.version_id.as_deref()))
}

/// The `versionId` query parameter, if any.
fn version_param(ctx: &Ctx) -> Option<String> {
    ctx.q("versionId")
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// Add `x-amz-version-id` when the object lives in a versioned bucket.
fn with_version(mut resp: Response, version_id: Option<&str>) -> Response {
    if let Some(v) = version_id.and_then(|v| HeaderValue::from_str(v).ok()) {
        resp.headers_mut().insert("x-amz-version-id", v);
    }
    resp
}

/// The object a GET/HEAD-style request addresses: a specific version, or
/// the latest. A delete marker answers like S3: 405 for an explicit version,
/// 404 with `x-amz-delete-marker` for the latest.
async fn resolve_object(ctx: &Ctx, bucket: &str, key: &str) -> S3Result<ObjectMeta> {
    match version_param(ctx) {
        Some(v) => {
            let info = ctx.engine().head_version(bucket, key, &v).await?;
            match info.meta {
                Some(m) => Ok(m),
                None => {
                    let mut e = S3Error::new(
                        StatusCode::METHOD_NOT_ALLOWED,
                        "MethodNotAllowed",
                        "The specified method is not allowed against this resource.",
                    );
                    e.headers.push(("x-amz-delete-marker", "true".into()));
                    e.headers.push(("x-amz-version-id", info.version_id));
                    Err(e)
                }
            }
        }
        None => match ctx.engine().head_object(bucket, key).await {
            Ok(m) => Ok(m),
            Err(kosha_engine::Error::NoSuchKey) => {
                let mut e = S3Error::from(kosha_engine::Error::NoSuchKey);
                if let Ok(Some(marker)) = ctx.engine().latest_delete_marker(bucket, key).await {
                    e.headers.push(("x-amz-delete-marker", "true".into()));
                    e.headers.push(("x-amz-version-id", marker));
                }
                Err(e)
            }
            Err(e) => Err(e.into()),
        },
    }
}

/// DELETE on an object: with `versionId`, permanently remove that version;
/// otherwise delete (or add a delete marker in a versioned bucket).
async fn delete_object(ctx: &Ctx, bucket: &str, key: &str) -> S3Result<Response> {
    let target = kosha_engine::DeleteTarget {
        key: key.into(),
        version_id: version_param(ctx),
    };
    let mut outcomes = ctx
        .engine()
        .delete_targets(
            bucket,
            vec![target],
            crate::protection::bypass_governance(&ctx.headers),
        )
        .await?;
    let o = outcomes.pop().unwrap_or_default();
    if let Some(rej) = o.error {
        return Err(kosha_engine::Error::from(rej).into());
    }
    let mut resp = with_version(
        StatusCode::NO_CONTENT.into_response(),
        o.version_id.as_deref(),
    );
    if o.delete_marker {
        resp.headers_mut()
            .insert("x-amz-delete-marker", HeaderValue::from_static("true"));
    }
    Ok(resp)
}

/// GET/PUT ?retention and ?legal-hold.
async fn object_lock_subresource(
    ctx: &Ctx,
    bucket: &str,
    key: &str,
    body: Body,
) -> S3Result<Response> {
    use crate::protection::*;
    let engine = ctx.engine();
    let retention = ctx.has("retention");
    if ctx.method == Method::GET {
        let meta = resolve_object(ctx, bucket, key).await?;
        if retention {
            return match &meta.lock.retention {
                Some(r) => Ok(xml_response(render_retention(r))),
                None => Err(S3Error::new(
                    StatusCode::NOT_FOUND,
                    "NoSuchObjectLockConfiguration",
                    "The specified object does not have a ObjectLock configuration",
                )),
            };
        }
        return Ok(xml_response(render_legal_hold(meta.lock.legal_hold)));
    }
    let text = read_text(ctx, body).await?;
    let m = if retention {
        engine
            .set_object_lock(
                bucket,
                key,
                version_param(ctx),
                Some(parse_retention(&text)?),
                None,
                bypass_governance(&ctx.headers),
            )
            .await?
    } else {
        engine
            .set_object_lock(
                bucket,
                key,
                version_param(ctx),
                None,
                Some(parse_legal_hold(&text)?),
                false,
            )
            .await?
    };
    Ok(with_version(
        StatusCode::OK.into_response(),
        m.version_id.as_deref(),
    ))
}

async fn list_versions(ctx: &Ctx, bucket: &str) -> S3Result<Response> {
    let max = match ctx.q("max-keys") {
        Some(v) => v
            .parse::<usize>()
            .map_err(|_| S3Error::invalid_argument("max-keys must be a non-negative integer"))?
            .min(MAX_KEYS),
        None => MAX_KEYS,
    };
    let prefix = ctx.q("prefix").unwrap_or_default();
    let delimiter = ctx.q("delimiter").filter(|d| !d.is_empty());
    let key_marker = ctx.q("key-marker").unwrap_or_default();
    let version_marker = ctx.q("version-id-marker").unwrap_or_default();
    let page = ctx
        .engine()
        .list_versions(
            bucket,
            kosha_engine::VersionListQuery {
                prefix: prefix.into(),
                delimiter: delimiter.map(str::to_string),
                key_marker: (!key_marker.is_empty()).then(|| key_marker.to_string()),
                version_marker: (!version_marker.is_empty()).then(|| version_marker.to_string()),
                max_keys: max,
            },
        )
        .await?;
    Ok(xml_response(crate::protection::render_versions(
        bucket,
        prefix,
        delimiter,
        key_marker,
        version_marker,
        max,
        &page,
        OWNER_ID,
    )))
}

/// `bucket/key[?versionId=...]`, URL-encoded, optional leading slash.
fn parse_copy_source_version(v: &str) -> S3Result<(String, String, Option<String>)> {
    let (v, version) = match v.split_once("?versionId=") {
        Some((s, ver)) => (s, Some(decode_path(ver)).filter(|x| !x.is_empty())),
        None => (v, None),
    };
    let decoded = decode_path(v);
    let decoded = decoded.strip_prefix('/').unwrap_or(&decoded);
    match decoded.split_once('/') {
        Some((b, k)) if !b.is_empty() && !k.is_empty() => {
            Ok((b.to_string(), k.to_string(), version))
        }
        _ => Err(S3Error::invalid_argument(
            "x-amz-copy-source must be bucket/key",
        )),
    }
}

/// Copying needs read access to the source as well.
fn authorize_copy_source(ctx: &Ctx, sb: &str, sk: &str, sv: &Option<String>) -> S3Result<()> {
    let action = if sv.is_some() {
        "s3:GetObjectVersion"
    } else {
        "s3:GetObject"
    };
    ctx.authorize(action, sb, iam::object_arn(sb, sk))
}

async fn copy_object(ctx: &Ctx, bucket: &str, key: &str) -> S3Result<Response> {
    let (sb, sk, sv) =
        parse_copy_source_version(ctx.header("x-amz-copy-source").unwrap_or_default())?;
    authorize_copy_source(ctx, &sb, &sk, &sv)?;
    let replace = ctx
        .header("x-amz-metadata-directive")
        .is_some_and(|d| d.eq_ignore_ascii_case("REPLACE"));
    if sb == bucket && sk == key && sv.is_none() && !replace {
        return Err(S3Error::invalid_request(
            "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata.",
        ));
    }
    let src = match &sv {
        Some(v) => ctx
            .engine()
            .head_version(&sb, &sk, v)
            .await?
            .meta
            .ok_or_else(|| S3Error::invalid_request("The source of a copy request may not specifically refer to a delete marker by version id."))?,
        None => ctx.engine().head_object(&sb, &sk).await?,
    };
    check_copy_conditions(ctx, &src)?;
    let lock = crate::protection::lock_from_headers(&ctx.headers)?;
    let replace_tags = ctx
        .header("x-amz-tagging-directive")
        .is_some_and(|d| d.eq_ignore_ascii_case("REPLACE"));
    let mut attrs = if replace {
        attrs_from_headers(&ctx.headers)
    } else {
        src.attrs.clone()
    };
    attrs.tags = if replace_tags {
        crate::config::parse_tag_header(&ctx.headers)?.unwrap_or_default()
    } else {
        src.attrs.tags.clone()
    };
    let meta = ctx
        .engine()
        .copy_object_with(
            kosha_engine::CopySource {
                bucket: sb,
                key: sk,
                version_id: sv,
            },
            bucket,
            key,
            Some(attrs),
            write_condition(ctx),
            lock,
            storage_class(ctx)?,
        )
        .await?;
    let mut x = Xml::new("CopyObjectResult");
    x.elem("LastModified", iso8601(&meta.last_modified))
        .elem("ETag", quoted(&meta.etag));
    let mut resp = with_version(
        xml_response(x.finish("CopyObjectResult")),
        meta.version_id.as_deref(),
    );
    if let Some(v) = src
        .version_id
        .as_deref()
        .and_then(|v| HeaderValue::from_str(v).ok())
    {
        resp.headers_mut().insert("x-amz-copy-source-version-id", v);
    }
    Ok(resp)
}

fn check_copy_conditions(ctx: &Ctx, src: &ObjectMeta) -> S3Result<()> {
    let failed = || Err(S3Error::from(kosha_engine::Error::PreconditionFailed));
    if let Some(m) = ctx.header("x-amz-copy-source-if-match")
        && !etag_matches(m, &src.etag)
    {
        return failed();
    }
    if let Some(m) = ctx.header("x-amz-copy-source-if-none-match")
        && etag_matches(m, &src.etag)
    {
        return failed();
    }
    if let Some(t) = ctx
        .header("x-amz-copy-source-if-unmodified-since")
        .and_then(parse_http_date)
        && src.last_modified.timestamp() > t.timestamp()
    {
        return failed();
    }
    if let Some(t) = ctx
        .header("x-amz-copy-source-if-modified-since")
        .and_then(parse_http_date)
        && src.last_modified.timestamp() <= t.timestamp()
    {
        return failed();
    }
    Ok(())
}

fn etag_matches(header: &str, etag: &str) -> bool {
    header
        .split(',')
        .map(|t| t.trim().trim_start_matches("W/").trim_matches('"'))
        .any(|t| t == "*" || t == etag)
}

/// Parse a single `bytes=` range against an object of `size` bytes into a
/// half-open `[start, end)`. `Ok(None)` means "ignore the header".
fn parse_range(v: &str, size: u64) -> S3Result<Option<(u64, u64)>> {
    let Some(spec) = v.trim().strip_prefix("bytes=") else {
        return Ok(None);
    };
    if spec.contains(',') {
        return Ok(None); // multi-range: S3 serves the whole object
    }
    let Some((a, b)) = spec.split_once('-') else {
        return Ok(None);
    };
    let unsatisfiable = || {
        Err(S3Error::new(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "InvalidRange",
            "The requested range is not satisfiable",
        ))
    };
    let (start, end) = match (a.trim(), b.trim()) {
        ("", "") => return Ok(None),
        ("", n) => {
            let Ok(n) = n.parse::<u64>() else {
                return Ok(None);
            };
            if n == 0 {
                return unsatisfiable();
            }
            (size.saturating_sub(n), size)
        }
        (s, e) => {
            let Ok(s) = s.parse::<u64>() else {
                return Ok(None);
            };
            let e = if e.is_empty() {
                size
            } else {
                match e.parse::<u64>() {
                    Ok(e) if e >= s => (e + 1).min(size),
                    _ => return Ok(None),
                }
            };
            if s >= size {
                return unsatisfiable();
            }
            (s, e)
        }
    };
    Ok(Some((start, end)))
}

async fn get_object(ctx: &Ctx, bucket: &str, key: &str) -> S3Result<Response> {
    let meta = resolve_object(ctx, bucket, key).await?;

    // Conditional requests (RFC 7232 precedence).
    if let Some(m) = ctx.header("if-match") {
        if !etag_matches(m, &meta.etag) {
            return Err(kosha_engine::Error::PreconditionFailed.into());
        }
    } else if let Some(t) = ctx.header("if-unmodified-since").and_then(parse_http_date)
        && meta.last_modified.timestamp() > t.timestamp()
    {
        return Err(kosha_engine::Error::PreconditionFailed.into());
    }
    let not_modified = if let Some(m) = ctx.header("if-none-match") {
        etag_matches(m, &meta.etag)
    } else if let Some(t) = ctx.header("if-modified-since").and_then(parse_http_date) {
        meta.last_modified.timestamp() <= t.timestamp()
    } else {
        false
    };

    if ctx.method == Method::GET && !not_modified {
        // Access-based tiering: record the read, and bring an auto-tiered
        // object back to STANDARD (in the background; this read is served
        // from the cold tier).
        ctx.engine().note_access(bucket, key);
        if meta.auto_tiered && meta.storage_class.is_some() {
            let (engine, bucket, key, version) = (
                ctx.engine().clone(),
                bucket.to_string(),
                key.to_string(),
                meta.version_id
                    .clone()
                    .filter(|_| version_param(ctx).is_some()),
            );
            tokio::spawn(async move {
                if let Err(e) = engine.promote_object(&bucket, &key, version).await {
                    tracing::warn!(%bucket, %key, error = %e, "promoting a read object failed");
                }
            });
        }
    }

    let mut b = Response::builder()
        .header(header::ETAG, quoted(&meta.etag))
        .header(header::LAST_MODIFIED, http_date(&meta.last_modified))
        .header(header::ACCEPT_RANGES, "bytes");
    if let Some(v) = &meta.version_id {
        b = b.header("x-amz-version-id", v);
    }
    for (k, v) in crate::protection::lock_headers(&meta.lock) {
        b = b.header(k, v);
    }
    if not_modified {
        return Ok(b
            .status(StatusCode::NOT_MODIFIED)
            .body(Body::empty())
            .unwrap());
    }

    let content_type = ctx
        .q("response-content-type")
        .map(str::to_string)
        .or_else(|| meta.attrs.content_type.clone())
        .unwrap_or_else(|| "binary/octet-stream".into());
    b = b.header(header::CONTENT_TYPE, content_type);
    for name in STORED_HEADERS {
        let override_param = format!("response-{name}");
        if let Some(v) = ctx
            .q(&override_param)
            .or(meta.attrs.headers.get(name).map(String::as_str))
        {
            b = b.header(name, v);
        }
    }
    for (k, v) in &meta.attrs.user_meta {
        b = b.header(format!("x-amz-meta-{k}"), v);
    }
    if !meta.attrs.tags.is_empty() {
        b = b.header("x-amz-tagging-count", meta.attrs.tags.len().to_string());
    }
    if let Some(class) = &meta.storage_class {
        b = b.header("x-amz-storage-class", class);
    }

    let range = match ctx.header("range") {
        Some(r) => parse_range(r, meta.size)?,
        None => None,
    };
    let (start, end) = range.unwrap_or((0, meta.size));
    b = b.header(header::CONTENT_LENGTH, end - start);
    if range.is_some() {
        b = b.status(StatusCode::PARTIAL_CONTENT).header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end - 1, meta.size),
        );
    }

    if ctx.method == Method::HEAD {
        return Ok(b.body(Body::empty()).unwrap());
    }
    // The status line is already sent when a chunk fails mid-stream, so
    // the client only sees a cut-off body: log the cause here.
    let (bucket, key) = (bucket.to_string(), key.to_string());
    let stream = ctx.engine().read(&meta, start, end).map_err(move |e| {
        tracing::warn!(%bucket, %key, error = %e, "object read failed mid-stream");
        std::io::Error::other(e.to_string())
    });
    Ok(b.body(Body::from_stream(stream)).unwrap())
}

// ---------------------------------------------------------------- multipart

async fn create_upload(ctx: &Ctx, bucket: &str, key: &str) -> S3Result<Response> {
    let info = ctx
        .engine()
        .create_upload_with(
            bucket,
            key,
            attrs_with_tags(&ctx.headers)?,
            {
                let lock = crate::protection::lock_from_headers(&ctx.headers)?;
                (lock != kosha_engine::ObjectLockState::default()).then_some(lock)
            },
            storage_class(ctx)?,
        )
        .await?;
    let mut x = Xml::new("InitiateMultipartUploadResult");
    x.elem("Bucket", bucket)
        .elem("Key", key)
        .elem("UploadId", &info.upload_id);
    Ok(xml_response(x.finish("InitiateMultipartUploadResult")))
}

fn part_number(ctx: &Ctx) -> S3Result<u32> {
    ctx.q("partNumber")
        .and_then(|n| n.parse().ok())
        .ok_or_else(|| {
            S3Error::invalid_argument("partNumber must be an integer between 1 and 10000")
        })
}

async fn upload_part(ctx: &Ctx, bucket: &str, key: &str, body: Body) -> S3Result<Response> {
    let upload_id = ctx.q("uploadId").unwrap_or_default();
    let n = part_number(ctx)?;
    // Reject unknown uploads before reading the body.
    ctx.engine().list_parts(bucket, key, upload_id).await?;

    if let Some(src) = ctx.header("x-amz-copy-source") {
        return upload_part_copy(ctx, bucket, key, upload_id, n, src).await;
    }
    let data = ingest(ctx, body).await?;
    let part = ctx
        .engine()
        .put_part(bucket, key, upload_id, n, data)
        .await?;
    Ok(Response::builder()
        .header(header::ETAG, quoted(&part.etag))
        .body(Body::empty())
        .unwrap())
}

/// UploadPartCopy: stream the source range back through the writer. Content
/// addressing means unchanged chunks are recognised and not stored twice.
async fn upload_part_copy(
    ctx: &Ctx,
    bucket: &str,
    key: &str,
    upload_id: &str,
    n: u32,
    src: &str,
) -> S3Result<Response> {
    let (sb, sk, sv) = parse_copy_source_version(src)?;
    authorize_copy_source(ctx, &sb, &sk, &sv)?;
    let meta = match &sv {
        Some(v) => ctx
            .engine()
            .head_version(&sb, &sk, v)
            .await?
            .meta
            .ok_or_else(|| S3Error::invalid_request("The source of a copy request may not specifically refer to a delete marker by version id."))?,
        None => ctx.engine().head_object(&sb, &sk).await?,
    };
    check_copy_conditions(ctx, &meta)?;
    let (start, end) = match ctx.header("x-amz-copy-source-range") {
        Some(r) => parse_range(r, meta.size)?
            .ok_or_else(|| S3Error::invalid_argument("invalid x-amz-copy-source-range"))?,
        None => (0, meta.size),
    };
    let mut stream = ctx.engine().read(&meta, start, end);
    let mut w = ctx.engine().writer();
    while let Some(piece) = stream.next().await {
        w.write(&piece?).await?;
    }
    let part = ctx
        .engine()
        .put_part(bucket, key, upload_id, n, w.finish().await?)
        .await?;
    let mut x = Xml::new("CopyPartResult");
    x.elem("LastModified", iso8601(&part.last_modified))
        .elem("ETag", quoted(&part.etag));
    Ok(xml_response(x.finish("CopyPartResult")))
}

#[derive(Deserialize)]
struct CompleteRequest {
    #[serde(rename = "Part", default)]
    parts: Vec<CompletePart>,
}

#[derive(Deserialize)]
struct CompletePart {
    #[serde(rename = "PartNumber")]
    part_number: u32,
    #[serde(rename = "ETag")]
    etag: String,
}

async fn complete_upload(ctx: &Ctx, bucket: &str, key: &str, body: Body) -> S3Result<Response> {
    let upload_id = ctx.q("uploadId").unwrap_or_default();
    let req: CompleteRequest = read_xml(ctx, body).await?;
    let parts = req
        .parts
        .into_iter()
        .map(|p| (p.part_number, p.etag))
        .collect();
    let meta = ctx
        .engine()
        .complete_upload(bucket, key, upload_id, parts, write_condition(ctx))
        .await?;
    let mut x = Xml::new("CompleteMultipartUploadResult");
    x.elem("Location", format!("/{bucket}/{key}"))
        .elem("Bucket", bucket)
        .elem("Key", key)
        .elem("ETag", quoted(&meta.etag));
    Ok(with_version(
        xml_response(x.finish("CompleteMultipartUploadResult")),
        meta.version_id.as_deref(),
    ))
}

async fn list_parts(ctx: &Ctx, bucket: &str, key: &str) -> S3Result<Response> {
    let upload_id = ctx.q("uploadId").unwrap_or_default();
    let (_, parts) = ctx.engine().list_parts(bucket, key, upload_id).await?;
    let marker: u32 = ctx
        .q("part-number-marker")
        .and_then(|m| m.parse().ok())
        .unwrap_or(0);
    let max_parts: usize = ctx
        .q("max-parts")
        .and_then(|m| m.parse().ok())
        .unwrap_or(1000)
        .min(1000);
    let remaining: Vec<_> = parts.iter().filter(|p| p.part_number > marker).collect();
    let page = &remaining[..remaining.len().min(max_parts)];
    let truncated = remaining.len() > page.len();

    let mut x = Xml::new("ListPartsResult");
    x.elem("Bucket", bucket)
        .elem("Key", key)
        .elem("UploadId", upload_id)
        .open("Initiator")
        .elem("ID", OWNER_ID)
        .elem("DisplayName", OWNER_ID)
        .close("Initiator")
        .open("Owner")
        .elem("ID", OWNER_ID)
        .elem("DisplayName", OWNER_ID)
        .close("Owner")
        .elem("StorageClass", "STANDARD")
        .elem("PartNumberMarker", marker.to_string())
        .opt(
            "NextPartNumberMarker",
            page.last().map(|p| p.part_number.to_string()),
        )
        .elem("MaxParts", max_parts.to_string())
        .elem("IsTruncated", truncated.to_string());
    for p in page {
        x.open("Part")
            .elem("PartNumber", p.part_number.to_string())
            .elem("LastModified", iso8601(&p.last_modified))
            .elem("ETag", quoted(&p.etag))
            .elem("Size", p.size.to_string())
            .close("Part");
    }
    Ok(xml_response(x.finish("ListPartsResult")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(parse_range("bytes=0-9", 100).unwrap(), Some((0, 10)));
        assert_eq!(parse_range("bytes=90-", 100).unwrap(), Some((90, 100)));
        assert_eq!(parse_range("bytes=-10", 100).unwrap(), Some((90, 100)));
        assert_eq!(parse_range("bytes=50-1000", 100).unwrap(), Some((50, 100)));
        assert_eq!(parse_range("bytes=-1000", 100).unwrap(), Some((0, 100)));
        assert!(parse_range("bytes=100-", 100).is_err());
        assert_eq!(parse_range("bytes=5-1", 100).unwrap(), None);
        assert_eq!(parse_range("items=1-2", 100).unwrap(), None);
    }

    #[test]
    fn copy_source() {
        assert_eq!(
            parse_copy_source_version("/bkt/a%20b/c.txt?versionId=x").unwrap(),
            ("bkt".into(), "a b/c.txt".into(), Some("x".into()))
        );
        assert_eq!(
            parse_copy_source_version("bkt/k").unwrap(),
            ("bkt".into(), "k".into(), None)
        );
        assert!(parse_copy_source_version("bkt").is_err());
    }
}
