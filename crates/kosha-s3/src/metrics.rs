//! Prometheus metrics for `/_kosha/metrics` (text exposition format 0.0.4).

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axum::http::Method;
use parking_lot::Mutex;

/// Latency histogram bounds in seconds.
const BUCKETS: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Default)]
struct OpStats {
    /// Requests by status code.
    by_status: BTreeMap<u16, u64>,
    /// Cumulative-from-zero bucket counts (rendered as cumulative).
    buckets: [u64; BUCKETS.len()],
    count: u64,
    sum_secs: f64,
}

#[derive(Default)]
pub struct Metrics {
    ops: Mutex<BTreeMap<&'static str, OpStats>>,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
}

impl Metrics {
    pub fn observe(
        &self,
        op: &'static str,
        status: u16,
        elapsed: Duration,
        bytes_in: u64,
        bytes_out: u64,
    ) {
        self.bytes_in.fetch_add(bytes_in, Relaxed);
        self.bytes_out.fetch_add(bytes_out, Relaxed);
        let secs = elapsed.as_secs_f64();
        let mut ops = self.ops.lock();
        let s = ops.entry(op).or_default();
        *s.by_status.entry(status).or_default() += 1;
        if let Some(i) = BUCKETS.iter().position(|b| secs <= *b) {
            s.buckets[i] += 1;
        }
        s.count += 1;
        s.sum_secs += secs;
    }

    pub fn render(&self, out: &mut String) {
        let ops = self.ops.lock();
        header(
            out,
            "kosha_requests_total",
            "counter",
            "S3 requests by operation and HTTP status.",
        );
        for (op, s) in ops.iter() {
            for (status, n) in &s.by_status {
                let _ = writeln!(
                    out,
                    "kosha_requests_total{{op=\"{op}\",status=\"{status}\"}} {n}"
                );
            }
        }
        header(
            out,
            "kosha_request_duration_seconds",
            "histogram",
            "S3 request latency by operation (time to response headers).",
        );
        for (op, s) in ops.iter() {
            let mut cumulative = 0;
            for (i, b) in BUCKETS.iter().enumerate() {
                cumulative += s.buckets[i];
                let _ = writeln!(
                    out,
                    "kosha_request_duration_seconds_bucket{{op=\"{op}\",le=\"{b}\"}} {cumulative}"
                );
            }
            let _ = writeln!(
                out,
                "kosha_request_duration_seconds_bucket{{op=\"{op}\",le=\"+Inf\"}} {}",
                s.count
            );
            let _ = writeln!(
                out,
                "kosha_request_duration_seconds_sum{{op=\"{op}\"}} {}",
                s.sum_secs
            );
            let _ = writeln!(
                out,
                "kosha_request_duration_seconds_count{{op=\"{op}\"}} {}",
                s.count
            );
        }
        drop(ops);
        counter(
            out,
            "kosha_request_bytes_total",
            "Request body bytes received (Content-Length).",
            self.bytes_in.load(Relaxed),
        );
        counter(
            out,
            "kosha_response_bytes_total",
            "Response body bytes sent (Content-Length).",
            self.bytes_out.load(Relaxed),
        );
    }
}

pub fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}

pub fn gauge(out: &mut String, name: &str, help: &str, v: impl std::fmt::Display) {
    header(out, name, "gauge", help);
    let _ = writeln!(out, "{name} {v}");
}

pub fn counter(out: &mut String, name: &str, help: &str, v: impl std::fmt::Display) {
    header(out, name, "counter", help);
    let _ = writeln!(out, "{name} {v}");
}

/// Name the S3 operation a request maps to, for metric labels.
pub fn operation(method: &Method, path: &str, query: Option<&str>) -> &'static str {
    if path.starts_with("/_kosha/") {
        return match path {
            "/_kosha/health" => "Health",
            "/_kosha/stats" => "Stats",
            "/_kosha/metrics" => "Metrics",
            p if p.starts_with("/_kosha/ui") => "Console",
            _ => "Admin",
        };
    }
    let trimmed = path.trim_start_matches('/');
    let (has_bucket, has_key) = match trimmed.split_once('/') {
        Some((_, k)) => (true, !k.is_empty()),
        None => (!trimmed.is_empty(), false),
    };
    let q = query.unwrap_or("");
    let has = |name: &str| {
        q.split('&')
            .any(|p| p == name || p.starts_with(&format!("{name}=")))
    };
    match (method.as_str(), has_bucket, has_key) {
        ("GET", false, _) => "ListBuckets",
        ("PUT", true, false) if has("tagging") => "PutBucketTagging",
        ("PUT", true, false) if has("cors") => "PutBucketCors",
        ("PUT", true, false) if has("versioning") => "PutBucketVersioning",
        ("PUT", true, false) if has("lifecycle") => "PutBucketLifecycle",
        ("PUT", true, false) if has("policy") => "PutBucketPolicy",
        ("PUT", true, false) if has("object-lock") => "PutObjectLockConfiguration",
        ("PUT", true, false) if has("notification") => "PutBucketNotification",
        ("PUT", true, false) => "CreateBucket",
        ("DELETE", true, false) if q.is_empty() => "DeleteBucket",
        ("DELETE", true, false) => "DeleteBucketConfig",
        ("HEAD", true, false) => "HeadBucket",
        ("GET", true, false) if has("uploads") => "ListMultipartUploads",
        ("GET", true, false) if has("versions") => "ListObjectVersions",
        ("GET", true, false) if has("list-type") => "ListObjectsV2",
        ("GET", true, false)
            if q.is_empty() || has("prefix") || has("marker") || has("delimiter") =>
        {
            "ListObjects"
        }
        ("GET", true, false) => "GetBucketConfig",
        ("POST", true, false) if has("delete") => "DeleteObjects",
        ("PUT", true, true) if has("uploadId") => "UploadPart",
        ("PUT", true, true) if has("tagging") => "PutObjectTagging",
        ("PUT", true, true) if has("retention") => "PutObjectRetention",
        ("PUT", true, true) if has("legal-hold") => "PutObjectLegalHold",
        ("PUT", true, true) => "PutObject",
        ("GET", true, true) if has("uploadId") => "ListParts",
        ("GET", true, true) if has("tagging") => "GetObjectTagging",
        ("GET", true, true) if has("retention") => "GetObjectRetention",
        ("GET", true, true) if has("legal-hold") => "GetObjectLegalHold",
        ("GET", true, true) => "GetObject",
        ("HEAD", true, true) => "HeadObject",
        ("DELETE", true, true) if has("uploadId") => "AbortMultipartUpload",
        ("DELETE", true, true) if has("tagging") => "DeleteObjectTagging",
        ("DELETE", true, true) => "DeleteObject",
        ("POST", true, true) if has("uploads") => "CreateMultipartUpload",
        ("POST", true, true) if has("uploadId") => "CompleteMultipartUpload",
        ("OPTIONS", _, _) => "Preflight",
        _ => "Other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_operations() {
        let g = Method::GET;
        assert_eq!(operation(&g, "/", None), "ListBuckets");
        assert_eq!(
            operation(&g, "/b", Some("list-type=2&prefix=a")),
            "ListObjectsV2"
        );
        assert_eq!(operation(&g, "/b/k/x.txt", None), "GetObject");
        assert_eq!(
            operation(&Method::PUT, "/b/k", Some("partNumber=1&uploadId=x")),
            "UploadPart"
        );
        assert_eq!(
            operation(&Method::POST, "/b", Some("delete")),
            "DeleteObjects"
        );
        assert_eq!(operation(&Method::PUT, "/b", None), "CreateBucket");
    }

    #[test]
    fn renders_cumulative_histograms() {
        let m = Metrics::default();
        m.observe("GetObject", 200, Duration::from_micros(300), 0, 10);
        m.observe("GetObject", 200, Duration::from_millis(3), 0, 10);
        m.observe("GetObject", 404, Duration::from_millis(30), 0, 0);
        let mut out = String::new();
        m.render(&mut out);
        assert!(out.contains("kosha_requests_total{op=\"GetObject\",status=\"200\"} 2"));
        assert!(out.contains("kosha_requests_total{op=\"GetObject\",status=\"404\"} 1"));
        assert!(
            out.contains("kosha_request_duration_seconds_bucket{op=\"GetObject\",le=\"0.0005\"} 1")
        );
        assert!(
            out.contains("kosha_request_duration_seconds_bucket{op=\"GetObject\",le=\"0.005\"} 2")
        );
        assert!(
            out.contains("kosha_request_duration_seconds_bucket{op=\"GetObject\",le=\"+Inf\"} 3")
        );
        assert!(out.contains("kosha_response_bytes_total 20"));
    }
}
