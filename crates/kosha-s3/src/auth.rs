//! AWS Signature Version 4: header auth, presigned URLs and streaming
//! (aws-chunked) payload signatures.

use std::collections::HashMap;

use axum::http::{HeaderMap, Method};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use hmac::{Hmac, KeyInit, Mac};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use sha2::{Digest, Sha256};

use crate::error::{S3Error, S3Result};

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const MAX_SKEW_SECS: i64 = 15 * 60;
const MAX_PRESIGN_SECS: i64 = 7 * 24 * 3600;
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Characters SigV4 leaves unescaped: A-Z a-z 0-9 - _ . ~
const AWS_URI: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');
const AWS_PATH: &AsciiSet = &AWS_URI.remove(b'/');

#[derive(Clone, Debug, Default)]
pub struct Credentials {
    keys: HashMap<String, String>,
}

impl Credentials {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_key(mut self, access_key: impl Into<String>, secret: impl Into<String>) -> Self {
        self.keys.insert(access_key.into(), secret.into());
        self
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// How the request body is protected, as declared by `x-amz-content-sha256`.
#[derive(Clone, Debug)]
pub enum Payload {
    Unsigned,
    /// The whole body must hash to this value.
    Sha256(String),
    /// aws-chunked body, each chunk signed in a chain seeded by the request
    /// signature.
    StreamingSigned(ChunkSigner),
    /// aws-chunked body without chunk signatures (checksum in a trailer).
    StreamingUnsigned,
}

impl Payload {
    pub fn is_chunked(&self) -> bool {
        matches!(
            self,
            Payload::StreamingSigned(_) | Payload::StreamingUnsigned
        )
    }
}

#[derive(Clone, Debug)]
pub struct ChunkSigner {
    signing_key: [u8; 32],
    amz_date: String,
    scope: String,
    prev_signature: String,
}

impl ChunkSigner {
    #[cfg(test)]
    pub(crate) fn for_test(secret: &str, amz_date: &str, region: &str, seed: &str) -> Self {
        let date = &amz_date[..8];
        ChunkSigner {
            signing_key: signing_key(secret, date, region, "s3"),
            amz_date: amz_date.to_string(),
            scope: format!("{date}/{region}/s3/aws4_request"),
            prev_signature: seed.to_string(),
        }
    }

    /// Verify the next chunk's signature and advance the chain.
    pub fn verify(&mut self, chunk: &[u8], signature: &str) -> S3Result<()> {
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            self.amz_date,
            self.scope,
            self.prev_signature,
            EMPTY_SHA256,
            hex::encode(Sha256::digest(chunk)),
        );
        let expected = hex::encode(hmac(&self.signing_key, sts.as_bytes()));
        if !ct_eq(expected.as_bytes(), signature.as_bytes()) {
            return Err(S3Error::signature_mismatch());
        }
        self.prev_signature = expected;
        Ok(())
    }
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut m = Hmac::<Sha256>::new_from_slice(key).expect("hmac accepts any key length");
    m.update(data);
    m.finalize().into_bytes().into()
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k = hmac(&k, region.as_bytes());
    let k = hmac(&k, service.as_bytes());
    hmac(&k, b"aws4_request")
}

pub fn uri_encode(s: &str) -> String {
    utf8_percent_encode(s, AWS_URI).to_string()
}

fn decode(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}

/// Decode a raw query string into pairs (`+` means space, as S3 treats it).
pub fn parse_query(raw: Option<&str>) -> Vec<(String, String)> {
    raw.unwrap_or_default()
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (decode(&k.replace('+', " ")), decode(&v.replace('+', " ")))
        })
        .collect()
}

fn canonical_query(pairs: &[(String, String)], skip_signature: bool) -> String {
    let mut enc: Vec<(String, String)> = pairs
        .iter()
        .filter(|(k, _)| !(skip_signature && k == "X-Amz-Signature"))
        .map(|(k, v)| (uri_encode(k), uri_encode(v)))
        .collect();
    enc.sort();
    enc.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(headers: &HeaderMap, signed: &[&str]) -> S3Result<String> {
    let mut out = String::new();
    for name in signed {
        let values: Vec<String> = headers
            .get_all(*name)
            .iter()
            .map(|v| {
                let s = String::from_utf8_lossy(v.as_bytes());
                s.split_whitespace().collect::<Vec<_>>().join(" ")
            })
            .collect();
        if values.is_empty() {
            return Err(S3Error::access_denied(format!(
                "signed header {name} is missing"
            )));
        }
        out.push_str(name);
        out.push(':');
        out.push_str(&values.join(","));
        out.push('\n');
    }
    Ok(out)
}

fn parse_amz_date(s: &str) -> S3Result<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(s, "%Y%m%dT%H%M%SZ")
        .map(|t| Utc.from_utc_datetime(&t))
        .map_err(|_| S3Error::access_denied("invalid X-Amz-Date"))
}

/// The parts of a SigV4 signature, from either the header or the query.
struct SigParts {
    access_key: String,
    date: String,
    region: String,
    service: String,
    signed_headers: Vec<String>,
    signature: String,
    amz_date: String,
}

fn parse_credential(cred: &str) -> S3Result<(String, String, String, String)> {
    let parts: Vec<&str> = cred.split('/').collect();
    match parts.as_slice() {
        [ak, date, region, service, "aws4_request"] => Ok((
            ak.to_string(),
            date.to_string(),
            region.to_string(),
            service.to_string(),
        )),
        _ => Err(S3Error::access_denied("malformed credential scope")),
    }
}

fn payload_from_header(headers: &HeaderMap) -> S3Result<PayloadDecl> {
    let v = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD");
    Ok(match v {
        "UNSIGNED-PAYLOAD" => PayloadDecl::Unsigned,
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => PayloadDecl::StreamingUnsigned,
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" | "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => {
            PayloadDecl::StreamingSigned
        }
        h if h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()) => {
            PayloadDecl::Sha256(h.to_ascii_lowercase())
        }
        other => {
            return Err(S3Error::not_implemented(&format!(
                "x-amz-content-sha256 value {other}"
            )));
        }
    })
}

enum PayloadDecl {
    Unsigned,
    Sha256(String),
    StreamingSigned,
    StreamingUnsigned,
}

impl PayloadDecl {
    fn header_value<'a>(&self, headers: &'a HeaderMap) -> &'a str {
        headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("UNSIGNED-PAYLOAD")
    }
}

pub struct AuthRequest<'a> {
    pub method: &'a Method,
    pub raw_path: &'a str,
    pub query: &'a [(String, String)],
    pub headers: &'a HeaderMap,
}

/// Authenticate a request. With empty credentials every request is allowed
/// (anonymous mode), but the payload declaration is still honoured so
/// aws-chunked bodies decode correctly.
pub fn authenticate(creds: &Credentials, req: &AuthRequest<'_>) -> S3Result<Payload> {
    let decl = payload_from_header(req.headers)?;
    let presigned = req.query.iter().any(|(k, _)| k == "X-Amz-Signature");
    let has_header = req.headers.contains_key("authorization");

    let presigned_v2 = req.query.iter().any(|(k, _)| k == "AWSAccessKeyId");
    if creds.is_empty() || (!presigned && !presigned_v2 && !has_header) {
        if !creds.is_empty() {
            return Err(S3Error::access_denied("anonymous access is disabled"));
        }
        return Ok(match decl {
            PayloadDecl::Unsigned => Payload::Unsigned,
            PayloadDecl::Sha256(h) => Payload::Sha256(h),
            PayloadDecl::StreamingSigned | PayloadDecl::StreamingUnsigned => {
                Payload::StreamingUnsigned
            }
        });
    }

    if req.query.iter().any(|(k, _)| k == "Signature")
        && req.query.iter().any(|(k, _)| k == "AWSAccessKeyId")
    {
        verify_v2_presigned(creds, req)?;
        return Ok(Payload::Unsigned);
    }

    let now = Utc::now();
    let (parts, payload_hash) = if presigned {
        let q = |k: &str| {
            req.query
                .iter()
                .find(|(n, _)| n == k)
                .map(|(_, v)| v.clone())
                .ok_or_else(|| S3Error::access_denied(format!("missing {k}")))
        };
        if q("X-Amz-Algorithm")? != ALGORITHM {
            return Err(S3Error::access_denied("unsupported signing algorithm"));
        }
        let (access_key, date, region, service) = parse_credential(&q("X-Amz-Credential")?)?;
        let amz_date = q("X-Amz-Date")?;
        let expires: i64 = q("X-Amz-Expires")?
            .parse()
            .map_err(|_| S3Error::access_denied("invalid X-Amz-Expires"))?;
        if !(1..=MAX_PRESIGN_SECS).contains(&expires) {
            return Err(S3Error::access_denied("X-Amz-Expires out of range"));
        }
        let t = parse_amz_date(&amz_date)?;
        if now > t + chrono::Duration::seconds(expires) {
            return Err(S3Error::access_denied("Request has expired"));
        }
        if t > now + chrono::Duration::seconds(MAX_SKEW_SECS) {
            return Err(S3Error::access_denied("request is dated in the future"));
        }
        let parts = SigParts {
            access_key,
            date,
            region,
            service,
            signed_headers: q("X-Amz-SignedHeaders")?
                .split(';')
                .map(str::to_string)
                .collect(),
            signature: q("X-Amz-Signature")?,
            amz_date,
        };
        (parts, "UNSIGNED-PAYLOAD".to_string())
    } else {
        let auth = req.headers["authorization"]
            .to_str()
            .map_err(|_| S3Error::access_denied("invalid Authorization header"))?;
        let rest = auth
            .strip_prefix(ALGORITHM)
            .ok_or_else(|| S3Error::access_denied("only AWS4-HMAC-SHA256 is supported"))?;
        let mut cred = None;
        let mut signed = None;
        let mut sig = None;
        for field in rest.split(',') {
            let (k, v) = field.trim().split_once('=').unwrap_or(("", ""));
            match k {
                "Credential" => cred = Some(v),
                "SignedHeaders" => signed = Some(v),
                "Signature" => sig = Some(v),
                _ => {}
            }
        }
        let (Some(cred), Some(signed), Some(sig)) = (cred, signed, sig) else {
            return Err(S3Error::access_denied("malformed Authorization header"));
        };
        let (access_key, date, region, service) = parse_credential(cred)?;
        let amz_date = req
            .headers
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| S3Error::access_denied("missing x-amz-date"))?;
        let t = parse_amz_date(&amz_date)?;
        if (now - t).num_seconds().abs() > MAX_SKEW_SECS {
            return Err(S3Error::new(
                axum::http::StatusCode::FORBIDDEN,
                "RequestTimeTooSkewed",
                "The difference between the request time and the server's time is too large.",
            ));
        }
        let parts = SigParts {
            access_key,
            date,
            region,
            service,
            signed_headers: signed.split(';').map(str::to_string).collect(),
            signature: sig.to_string(),
            amz_date,
        };
        (parts, decl.header_value(req.headers).to_string())
    };

    if !parts.amz_date.starts_with(&parts.date) {
        return Err(S3Error::access_denied(
            "credential date does not match X-Amz-Date",
        ));
    }
    let secret = creds.keys.get(&parts.access_key).ok_or_else(|| {
        S3Error::new(
            axum::http::StatusCode::FORBIDDEN,
            "InvalidAccessKeyId",
            "The AWS Access Key Id you provided does not exist in our records.",
        )
    })?;

    let signed: Vec<&str> = parts.signed_headers.iter().map(String::as_str).collect();
    if !signed.contains(&"host") {
        return Err(S3Error::access_denied("host must be a signed header"));
    }
    let canonical_uri = utf8_percent_encode(&decode(req.raw_path), AWS_PATH).to_string();
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        req.method.as_str(),
        canonical_uri,
        canonical_query(req.query, presigned),
        canonical_headers(req.headers, &signed)?,
        parts.signed_headers.join(";"),
        payload_hash,
    );
    let scope = format!(
        "{}/{}/{}/aws4_request",
        parts.date, parts.region, parts.service
    );
    let string_to_sign = format!(
        "{ALGORITHM}\n{}\n{}\n{}",
        parts.amz_date,
        scope,
        hex::encode(Sha256::digest(canonical_request.as_bytes())),
    );
    let key = signing_key(secret, &parts.date, &parts.region, &parts.service);
    let expected = hex::encode(hmac(&key, string_to_sign.as_bytes()));
    if !ct_eq(expected.as_bytes(), parts.signature.as_bytes()) {
        tracing::debug!(%canonical_request, "signature mismatch");
        return Err(S3Error::signature_mismatch());
    }

    Ok(match decl {
        _ if presigned => Payload::Unsigned,
        PayloadDecl::Unsigned => Payload::Unsigned,
        PayloadDecl::Sha256(h) => Payload::Sha256(h),
        PayloadDecl::StreamingUnsigned => Payload::StreamingUnsigned,
        PayloadDecl::StreamingSigned => Payload::StreamingSigned(ChunkSigner {
            signing_key: key,
            amz_date: parts.amz_date,
            scope,
            prev_signature: expected,
        }),
    })
}

/// Sub-resources that are part of the SigV2 canonical resource.
const V2_SUBRESOURCES: &[&str] = &[
    "acl",
    "cors",
    "delete",
    "lifecycle",
    "location",
    "logging",
    "notification",
    "partNumber",
    "policy",
    "requestPayment",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
    "restore",
    "tagging",
    "torrent",
    "uploadId",
    "uploads",
    "versionId",
    "versioning",
    "versions",
    "website",
];

/// Legacy SigV2 query-string authentication. boto3 still produces these
/// presigned URLs by default, so they are accepted for GET/PUT/HEAD/DELETE.
fn verify_v2_presigned(creds: &Credentials, req: &AuthRequest<'_>) -> S3Result<()> {
    let q = |k: &str| {
        req.query
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.as_str())
            .ok_or_else(|| S3Error::access_denied(format!("missing {k}")))
    };
    let expires = q("Expires")?;
    let expires_at: i64 = expires
        .parse()
        .map_err(|_| S3Error::access_denied("invalid Expires"))?;
    if Utc::now().timestamp() > expires_at {
        return Err(S3Error::access_denied("Request has expired"));
    }
    let secret = creds.keys.get(q("AWSAccessKeyId")?).ok_or_else(|| {
        S3Error::new(
            axum::http::StatusCode::FORBIDDEN,
            "InvalidAccessKeyId",
            "The AWS Access Key Id you provided does not exist in our records.",
        )
    })?;

    let header = |n: &str| {
        req.headers
            .get(n)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .trim()
            .to_string()
    };
    let mut amz: Vec<(String, String)> = req
        .headers
        .iter()
        .filter(|(k, _)| k.as_str().starts_with("x-amz-"))
        .map(|(k, v)| {
            (
                k.as_str().to_string(),
                String::from_utf8_lossy(v.as_bytes()).trim().to_string(),
            )
        })
        .collect();
    amz.sort();
    let mut sub: Vec<&(String, String)> = req
        .query
        .iter()
        .filter(|(k, _)| V2_SUBRESOURCES.contains(&k.as_str()))
        .collect();
    sub.sort();
    let mut resource = req.raw_path.to_string();
    if !sub.is_empty() {
        resource.push('?');
        resource.push_str(
            &sub.iter()
                .map(|(k, v)| {
                    if v.is_empty() {
                        k.clone()
                    } else {
                        format!("{k}={v}")
                    }
                })
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    let string_to_sign = format!(
        "{}\n{}\n{}\n{}\n{}{}",
        req.method.as_str(),
        header("content-md5"),
        header("content-type"),
        expires,
        amz.iter()
            .map(|(k, v)| format!("{k}:{v}\n"))
            .collect::<String>(),
        resource,
    );
    let mut m = Hmac::<sha1::Sha1>::new_from_slice(secret.as_bytes()).expect("any key length");
    m.update(string_to_sign.as_bytes());
    use base64::Engine as _;
    let expected = base64::engine::general_purpose::STANDARD.encode(m.finalize().into_bytes());
    if !ct_eq(expected.as_bytes(), q("Signature")?.as_bytes()) {
        return Err(S3Error::signature_mismatch());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Example from the AWS SigV4 documentation ("GET Object" with a range).
    #[test]
    fn aws_documented_example() {
        let creds = Credentials::new().with_key(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        let mut headers = HeaderMap::new();
        headers.insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());
        headers.insert("range", "bytes=0-9".parse().unwrap());
        headers.insert("x-amz-content-sha256", EMPTY_SHA256.parse().unwrap());
        headers.insert("x-amz-date", "20130524T000000Z".parse().unwrap());
        headers.insert(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
                .parse()
                .unwrap(),
        );
        let req = AuthRequest {
            method: &Method::GET,
            raw_path: "/test.txt",
            query: &[],
            headers: &headers,
        };
        // The example is dated 2013, so only the clock-skew check may fail.
        match authenticate(&creds, &req) {
            Err(e) => assert_eq!(e.code, "RequestTimeTooSkewed"),
            Ok(_) => panic!("expected skew failure"),
        }
        // Verify the signature math directly.
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
            "s3",
        );
        let creq = format!(
            "GET\n/test.txt\n\n{}\nhost;range;x-amz-content-sha256;x-amz-date\n{EMPTY_SHA256}",
            canonical_headers(
                &headers,
                &["host", "range", "x-amz-content-sha256", "x-amz-date"]
            )
            .unwrap()
        );
        let sts = format!(
            "{ALGORITHM}\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n{}",
            hex::encode(Sha256::digest(creq.as_bytes()))
        );
        assert_eq!(
            hex::encode(hmac(&key, sts.as_bytes())),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// Example from the S3 SigV2 documentation (query string authentication).
    #[test]
    fn v2_presigned_documented_example() {
        let creds = Credentials::new().with_key(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );
        let query = parse_query(Some(
            "AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&Expires=4141891200&Signature=placeholder",
        ));
        let req = AuthRequest {
            method: &Method::GET,
            raw_path: "/awsexamplebucket1/photos/puppy.jpg",
            query: &query,
            headers: &HeaderMap::new(),
        };
        // Recompute independently: HMAC-SHA1 over the documented string-to-sign.
        let sts = "GET\n\n\n4141891200\n/awsexamplebucket1/photos/puppy.jpg";
        let mut m = Hmac::<sha1::Sha1>::new_from_slice(b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
            .unwrap();
        m.update(sts.as_bytes());
        use base64::Engine as _;
        let sig = base64::engine::general_purpose::STANDARD.encode(m.finalize().into_bytes());
        assert!(authenticate(&creds, &req).is_err());
        let query = parse_query(Some(&format!(
            "AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&Expires=4141891200&Signature={}",
            uri_encode(&sig)
        )));
        let req = AuthRequest {
            query: &query,
            ..req
        };
        assert!(authenticate(&creds, &req).is_ok());
    }

    #[test]
    fn query_canonicalization() {
        let q = parse_query(Some("prefix=a%2Fb&list-type=2&delimiter=%2F&x"));
        assert_eq!(
            canonical_query(&q, false),
            "delimiter=%2F&list-type=2&prefix=a%2Fb&x="
        );
    }
}
