//! Bucket and object configuration APIs: tagging and CORS.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use kosha_engine::{BucketConfigUpdate, CorsRule};
use percent_encoding::percent_decode_str;
use serde::Deserialize;

use crate::error::{S3Error, S3Result};
use crate::xml::Xml;

const MAX_OBJECT_TAGS: usize = 10;
const MAX_BUCKET_TAGS: usize = 50;
const MAX_CORS_RULES: usize = 100;
const CORS_METHODS: [&str; 5] = ["GET", "PUT", "HEAD", "POST", "DELETE"];

fn invalid_tag(msg: impl Into<String>) -> S3Error {
    S3Error::new(StatusCode::BAD_REQUEST, "InvalidTag", msg)
}

pub fn validate_tags(tags: &BTreeMap<String, String>, max: usize) -> S3Result<()> {
    if tags.len() > max {
        return Err(invalid_tag(format!("at most {max} tags are allowed")));
    }
    for (k, v) in tags {
        if k.is_empty() || k.chars().count() > 128 {
            return Err(invalid_tag("tag keys must be 1-128 characters"));
        }
        if v.chars().count() > 256 {
            return Err(invalid_tag("tag values must be at most 256 characters"));
        }
        if k.starts_with("aws:") {
            return Err(invalid_tag("tag keys may not start with aws:"));
        }
    }
    Ok(())
}

/// Parse the `x-amz-tagging` header (URL query encoding: `k1=v1&k2=v2`).
pub fn parse_tag_header(headers: &HeaderMap) -> S3Result<Option<BTreeMap<String, String>>> {
    let Some(raw) = headers.get("x-amz-tagging").and_then(|v| v.to_str().ok()) else {
        return Ok(None);
    };
    let decode = |s: &str| {
        percent_decode_str(&s.replace('+', " "))
            .decode_utf8_lossy()
            .into_owned()
    };
    let mut tags = BTreeMap::new();
    for pair in raw.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if tags.insert(decode(k), decode(v)).is_some() {
            return Err(invalid_tag("duplicate tag key"));
        }
    }
    validate_tags(&tags, MAX_OBJECT_TAGS)?;
    Ok(Some(tags))
}

#[derive(Deserialize)]
struct TaggingXml {
    #[serde(rename = "TagSet", default)]
    tag_set: TagSetXml,
}

#[derive(Deserialize, Default)]
struct TagSetXml {
    #[serde(rename = "Tag", default)]
    tags: Vec<TagXml>,
}

#[derive(Deserialize)]
struct TagXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value", default)]
    value: String,
}

pub fn parse_tagging(xml: &str, bucket: bool) -> S3Result<BTreeMap<String, String>> {
    let parsed: TaggingXml =
        quick_xml::de::from_str(xml).map_err(|e| S3Error::malformed_xml(e.to_string()))?;
    let mut tags = BTreeMap::new();
    for t in parsed.tag_set.tags {
        if tags.insert(t.key, t.value).is_some() {
            return Err(invalid_tag("duplicate tag key"));
        }
    }
    validate_tags(
        &tags,
        if bucket {
            MAX_BUCKET_TAGS
        } else {
            MAX_OBJECT_TAGS
        },
    )?;
    Ok(tags)
}

pub fn render_tagging(tags: &BTreeMap<String, String>) -> String {
    let mut x = Xml::new("Tagging");
    x.open("TagSet");
    for (k, v) in tags {
        x.open("Tag").elem("Key", k).elem("Value", v).close("Tag");
    }
    x.close("TagSet");
    x.finish("Tagging")
}

// ------------------------------------------------------------------- CORS

#[derive(Deserialize)]
struct CorsXml {
    #[serde(rename = "CORSRule", default)]
    rules: Vec<CorsRuleXml>,
}

#[derive(Deserialize)]
struct CorsRuleXml {
    #[serde(rename = "ID")]
    id: Option<String>,
    #[serde(rename = "AllowedOrigin", default)]
    origins: Vec<String>,
    #[serde(rename = "AllowedMethod", default)]
    methods: Vec<String>,
    #[serde(rename = "AllowedHeader", default)]
    headers: Vec<String>,
    #[serde(rename = "ExposeHeader", default)]
    expose: Vec<String>,
    #[serde(rename = "MaxAgeSeconds")]
    max_age: Option<u32>,
}

pub fn parse_cors(xml: &str) -> S3Result<BucketConfigUpdate> {
    let parsed: CorsXml =
        quick_xml::de::from_str(xml).map_err(|e| S3Error::malformed_xml(e.to_string()))?;
    if parsed.rules.is_empty() || parsed.rules.len() > MAX_CORS_RULES {
        return Err(S3Error::malformed_xml(format!(
            "a CORS configuration needs 1-{MAX_CORS_RULES} rules"
        )));
    }
    let mut rules = Vec::new();
    for r in parsed.rules {
        if r.origins.is_empty() || r.methods.is_empty() {
            return Err(S3Error::malformed_xml(
                "each CORSRule needs AllowedOrigin and AllowedMethod",
            ));
        }
        for m in &r.methods {
            if !CORS_METHODS.contains(&m.as_str()) {
                return Err(S3Error::invalid_request(format!(
                    "Found unsupported HTTP method in CORS config. Unsupported method is {m}"
                )));
            }
        }
        for o in &r.origins {
            if o.matches('*').count() > 1 {
                return Err(S3Error::invalid_request(
                    "AllowedOrigin can have at most one wildcard",
                ));
            }
        }
        rules.push(CorsRule {
            id: r.id,
            allowed_origins: r.origins,
            allowed_methods: r.methods,
            allowed_headers: r.headers,
            expose_headers: r.expose,
            max_age_seconds: r.max_age,
        });
    }
    Ok(BucketConfigUpdate::Cors(Some(rules)))
}

pub fn render_cors(rules: &[CorsRule]) -> String {
    let mut x = Xml::new("CORSConfiguration");
    for r in rules {
        x.open("CORSRule");
        x.opt("ID", r.id.as_deref());
        for o in &r.allowed_origins {
            x.elem("AllowedOrigin", o);
        }
        for m in &r.allowed_methods {
            x.elem("AllowedMethod", m);
        }
        for h in &r.allowed_headers {
            x.elem("AllowedHeader", h);
        }
        for h in &r.expose_headers {
            x.elem("ExposeHeader", h);
        }
        x.opt("MaxAgeSeconds", r.max_age_seconds.map(|s| s.to_string()));
        x.close("CORSRule");
    }
    x.finish("CORSConfiguration")
}

/// `pattern` with at most one `*` wildcard, case-insensitive.
fn wildcard(pattern: &str, value: &str) -> bool {
    let (p, v) = (pattern.to_ascii_lowercase(), value.to_ascii_lowercase());
    match p.split_once('*') {
        None => p == v,
        Some((pre, suf)) => {
            v.len() >= pre.len() + suf.len() && v.starts_with(pre) && v.ends_with(suf)
        }
    }
}

/// The first rule allowing `origin` to use `method` with `request_headers`.
pub fn matching_rule<'a>(
    rules: &'a [CorsRule],
    origin: &str,
    method: &str,
    request_headers: &[String],
) -> Option<&'a CorsRule> {
    rules.iter().find(|r| {
        r.allowed_origins.iter().any(|o| wildcard(o, origin))
            && r.allowed_methods.iter().any(|m| m == method)
            && request_headers
                .iter()
                .all(|h| r.allowed_headers.iter().any(|a| wildcard(a, h)))
    })
}

fn cors_forbidden(msg: &str) -> Response {
    let mut x = Xml::new("Error");
    x.elem("Code", "AccessForbidden").elem("Message", msg);
    let mut r = (StatusCode::FORBIDDEN, x.finish("Error")).into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    r
}

/// Answer a CORS preflight (`OPTIONS`) for a bucket. Preflights are never
/// signed, so this runs before authentication.
pub fn preflight(rules: Option<&[CorsRule]>, headers: &HeaderMap) -> Response {
    let h = |n: &str| headers.get(n).and_then(|v| v.to_str().ok());
    let (Some(origin), Some(method)) = (h("origin"), h("access-control-request-method")) else {
        return cors_forbidden("Insufficient information. Origin request header needed.");
    };
    let requested: Vec<String> = h("access-control-request-headers")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    let Some(rules) = rules else {
        return cors_forbidden("CORSResponse: CORS is not enabled for this bucket.");
    };
    let Some(rule) = matching_rule(rules, origin, method, &requested) else {
        return cors_forbidden("CORSResponse: This CORS request is not allowed.");
    };
    let mut b = Response::builder()
        .status(StatusCode::OK)
        .header(
            "access-control-allow-methods",
            rule.allowed_methods.join(", "),
        )
        .header(
            header::VARY,
            "Origin, Access-Control-Request-Headers, Access-Control-Request-Method",
        );
    b = allow_origin(b, rule, origin);
    if !requested.is_empty() {
        b = b.header("access-control-allow-headers", requested.join(", "));
    }
    if let Some(age) = rule.max_age_seconds {
        b = b.header("access-control-max-age", age.to_string());
    }
    b.body(Body::empty()).unwrap()
}

fn allow_origin(
    b: axum::http::response::Builder,
    rule: &CorsRule,
    origin: &str,
) -> axum::http::response::Builder {
    if rule.allowed_origins.iter().any(|o| o == "*") {
        b.header("access-control-allow-origin", "*")
    } else {
        b.header("access-control-allow-origin", origin)
            .header("access-control-allow-credentials", "true")
    }
}

/// Add CORS headers to an actual (non-preflight) response.
pub fn decorate(resp: &mut Response, rules: &[CorsRule], origin: &str, method: &Method) {
    let Some(rule) = matching_rule(rules, origin, method.as_str(), &[]) else {
        return;
    };
    let h = resp.headers_mut();
    if rule.allowed_origins.iter().any(|o| o == "*") {
        h.insert("access-control-allow-origin", HeaderValue::from_static("*"));
    } else if let Ok(v) = HeaderValue::from_str(origin) {
        h.insert("access-control-allow-origin", v);
        h.insert(
            "access-control-allow-credentials",
            HeaderValue::from_static("true"),
        );
    }
    h.insert(
        "access-control-allow-methods",
        HeaderValue::from_str(&rule.allowed_methods.join(", ")).unwrap(),
    );
    if !rule.expose_headers.is_empty()
        && let Ok(v) = HeaderValue::from_str(&rule.expose_headers.join(", "))
    {
        h.insert("access-control-expose-headers", v);
    }
    h.append(header::VARY, HeaderValue::from_static("Origin"));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(origins: &[&str], methods: &[&str], headers: &[&str]) -> CorsRule {
        CorsRule {
            allowed_origins: origins.iter().map(|s| s.to_string()).collect(),
            allowed_methods: methods.iter().map(|s| s.to_string()).collect(),
            allowed_headers: headers.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn cors_matching() {
        let rules = vec![
            rule(&["https://*.example.com"], &["GET", "PUT"], &["*"]),
            rule(&["*"], &["GET"], &[]),
        ];
        assert!(
            matching_rule(
                &rules,
                "https://app.example.com",
                "PUT",
                &["content-type".into()]
            )
            .is_some()
        );
        assert!(matching_rule(&rules, "https://evil.com", "PUT", &[]).is_none());
        assert_eq!(
            matching_rule(&rules, "https://evil.com", "GET", &[])
                .unwrap()
                .allowed_origins,
            ["*"]
        );
        // The wildcard rule allows no headers.
        assert!(matching_rule(&rules, "https://evil.com", "GET", &["x-custom".into()]).is_none());
    }

    #[test]
    fn tag_header_and_xml() {
        let mut h = HeaderMap::new();
        h.insert(
            "x-amz-tagging",
            "project=kosha&team=storage%20core".parse().unwrap(),
        );
        let tags = parse_tag_header(&h).unwrap().unwrap();
        assert_eq!(tags["team"], "storage core");
        let xml = render_tagging(&tags);
        assert_eq!(parse_tagging(&xml, false).unwrap(), tags);
        h.insert("x-amz-tagging", "aws:reserved=1".parse().unwrap());
        assert!(parse_tag_header(&h).is_err());
    }
}
