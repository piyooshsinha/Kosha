//! XML codecs and header handling for versioning, Object Lock and lifecycle.

use std::collections::BTreeMap;

use axum::http::{HeaderMap, StatusCode};
use chrono::{DateTime, Utc};
use kosha_engine::{
    DefaultRetention, LifecycleRule, ObjectLockConfig, ObjectLockState, Retention, RetentionMode,
    VersionPage, Versioning,
};
use serde::Deserialize;

use crate::error::{S3Error, S3Result};
use crate::xml::{Xml, iso8601};

fn parse<T: for<'de> Deserialize<'de>>(xml: &str) -> S3Result<T> {
    quick_xml::de::from_str(xml).map_err(|e| S3Error::malformed_xml(e.to_string()))
}

fn mode_str(m: RetentionMode) -> &'static str {
    match m {
        RetentionMode::Governance => "GOVERNANCE",
        RetentionMode::Compliance => "COMPLIANCE",
    }
}

fn parse_mode(s: &str) -> S3Result<RetentionMode> {
    match s {
        "GOVERNANCE" => Ok(RetentionMode::Governance),
        "COMPLIANCE" => Ok(RetentionMode::Compliance),
        other => Err(S3Error::malformed_xml(format!(
            "unknown retention mode {other:?}"
        ))),
    }
}

fn parse_date(s: &str) -> S3Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s.trim())
        .map(|d| d.with_timezone(&Utc))
        .map_err(|_| S3Error::invalid_argument(format!("invalid date {s:?}")))
}

// ------------------------------------------------------------- versioning

#[derive(Deserialize)]
struct VersioningXml {
    #[serde(rename = "Status")]
    status: Option<String>,
}

pub fn parse_versioning(xml: &str) -> S3Result<Versioning> {
    let v: VersioningXml = parse(xml)?;
    match v.status.as_deref() {
        Some("Enabled") => Ok(Versioning::Enabled),
        Some("Suspended") => Ok(Versioning::Suspended),
        _ => Err(S3Error::malformed_xml(
            "Status must be Enabled or Suspended",
        )),
    }
}

pub fn render_versioning(v: Versioning) -> String {
    let mut x = Xml::new("VersioningConfiguration");
    match v {
        Versioning::Enabled => {
            x.elem("Status", "Enabled");
        }
        Versioning::Suspended => {
            x.elem("Status", "Suspended");
        }
        Versioning::Unversioned => {}
    }
    x.finish("VersioningConfiguration")
}

#[allow(clippy::too_many_arguments)]
pub fn render_versions(
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    key_marker: &str,
    version_marker: &str,
    max: usize,
    page: &VersionPage,
    owner: &str,
) -> String {
    let mut x = Xml::new("ListVersionsResult");
    x.elem("Name", bucket)
        .elem("Prefix", prefix)
        .elem("KeyMarker", key_marker)
        .elem("VersionIdMarker", version_marker)
        .elem("MaxKeys", max.to_string())
        .elem("IsTruncated", page.is_truncated.to_string());
    if let Some(d) = delimiter {
        x.elem("Delimiter", d);
    }
    x.opt("NextKeyMarker", page.next_key_marker.as_deref());
    x.opt("NextVersionIdMarker", page.next_version_marker.as_deref());
    for v in &page.versions {
        match &v.meta {
            Some(m) => {
                x.open("Version")
                    .elem("Key", &v.key)
                    .elem("VersionId", &v.version_id)
                    .elem("IsLatest", v.is_latest.to_string())
                    .elem("LastModified", iso8601(&v.last_modified))
                    .elem("ETag", format!("\"{}\"", m.etag))
                    .elem("Size", m.size.to_string())
                    .elem("StorageClass", "STANDARD")
                    .open("Owner")
                    .elem("ID", owner)
                    .elem("DisplayName", owner)
                    .close("Owner")
                    .close("Version");
            }
            None => {
                x.open("DeleteMarker")
                    .elem("Key", &v.key)
                    .elem("VersionId", &v.version_id)
                    .elem("IsLatest", v.is_latest.to_string())
                    .elem("LastModified", iso8601(&v.last_modified))
                    .open("Owner")
                    .elem("ID", owner)
                    .elem("DisplayName", owner)
                    .close("Owner")
                    .close("DeleteMarker");
            }
        }
    }
    for p in &page.common_prefixes {
        x.open("CommonPrefixes")
            .elem("Prefix", p)
            .close("CommonPrefixes");
    }
    x.finish("ListVersionsResult")
}

// ------------------------------------------------------------ object lock

#[derive(Deserialize)]
struct LockConfigXml {
    #[serde(rename = "ObjectLockEnabled")]
    enabled: Option<String>,
    #[serde(rename = "Rule")]
    rule: Option<LockRuleXml>,
}

#[derive(Deserialize)]
struct LockRuleXml {
    #[serde(rename = "DefaultRetention")]
    default_retention: DefaultRetentionXml,
}

#[derive(Deserialize)]
struct DefaultRetentionXml {
    #[serde(rename = "Mode")]
    mode: String,
    #[serde(rename = "Days")]
    days: Option<u32>,
    #[serde(rename = "Years")]
    years: Option<u32>,
}

pub fn parse_lock_config(xml: &str) -> S3Result<ObjectLockConfig> {
    let c: LockConfigXml = parse(xml)?;
    if c.enabled.as_deref() != Some("Enabled") {
        return Err(S3Error::malformed_xml("ObjectLockEnabled must be Enabled"));
    }
    let default_retention = match c.rule {
        None => None,
        Some(r) => {
            let d = r.default_retention;
            let days = match (d.days, d.years) {
                (Some(days), None) if days > 0 => days,
                (None, Some(years)) if years > 0 => years * 365,
                _ => {
                    return Err(S3Error::malformed_xml(
                        "DefaultRetention needs exactly one positive Days or Years",
                    ));
                }
            };
            Some(DefaultRetention {
                mode: parse_mode(&d.mode)?,
                days,
            })
        }
    };
    Ok(ObjectLockConfig { default_retention })
}

pub fn render_lock_config(c: &ObjectLockConfig) -> String {
    let mut x = Xml::new("ObjectLockConfiguration");
    x.elem("ObjectLockEnabled", "Enabled");
    if let Some(d) = &c.default_retention {
        x.open("Rule")
            .open("DefaultRetention")
            .elem("Mode", mode_str(d.mode))
            .elem("Days", d.days.to_string())
            .close("DefaultRetention")
            .close("Rule");
    }
    x.finish("ObjectLockConfiguration")
}

#[derive(Deserialize)]
struct RetentionXml {
    #[serde(rename = "Mode")]
    mode: Option<String>,
    #[serde(rename = "RetainUntilDate")]
    until: Option<String>,
}

/// `Ok(None)` removes retention (an empty Retention element).
pub fn parse_retention(xml: &str) -> S3Result<Option<Retention>> {
    let r: RetentionXml = parse(xml)?;
    match (r.mode, r.until) {
        (None, None) => Ok(None),
        (Some(m), Some(u)) => Ok(Some(Retention {
            mode: parse_mode(&m)?,
            until: parse_date(&u)?,
        })),
        _ => Err(S3Error::malformed_xml(
            "Retention needs both Mode and RetainUntilDate",
        )),
    }
}

pub fn render_retention(r: &Retention) -> String {
    let mut x = Xml::new("Retention");
    x.elem("Mode", mode_str(r.mode))
        .elem("RetainUntilDate", iso8601(&r.until));
    x.finish("Retention")
}

#[derive(Deserialize)]
struct LegalHoldXml {
    #[serde(rename = "Status")]
    status: String,
}

pub fn parse_legal_hold(xml: &str) -> S3Result<bool> {
    match parse::<LegalHoldXml>(xml)?.status.as_str() {
        "ON" => Ok(true),
        "OFF" => Ok(false),
        other => Err(S3Error::malformed_xml(format!(
            "unknown legal hold status {other:?}"
        ))),
    }
}

pub fn render_legal_hold(on: bool) -> String {
    let mut x = Xml::new("LegalHold");
    x.elem("Status", if on { "ON" } else { "OFF" });
    x.finish("LegalHold")
}

/// Object Lock settings requested by PutObject / Copy / CreateMultipartUpload headers.
pub fn lock_from_headers(headers: &HeaderMap) -> S3Result<ObjectLockState> {
    let h = |n: &str| headers.get(n).and_then(|v| v.to_str().ok());
    let retention = match (
        h("x-amz-object-lock-mode"),
        h("x-amz-object-lock-retain-until-date"),
    ) {
        (None, None) => None,
        (Some(m), Some(u)) => Some(Retention {
            mode: parse_mode(m).map_err(|_| {
                S3Error::invalid_argument("x-amz-object-lock-mode must be GOVERNANCE or COMPLIANCE")
            })?,
            until: parse_date(u)?,
        }),
        _ => {
            return Err(S3Error::invalid_argument(
                "x-amz-object-lock-mode and x-amz-object-lock-retain-until-date must be used together",
            ));
        }
    };
    let legal_hold = match h("x-amz-object-lock-legal-hold") {
        None | Some("OFF") => false,
        Some("ON") => true,
        Some(_) => {
            return Err(S3Error::invalid_argument(
                "x-amz-object-lock-legal-hold must be ON or OFF",
            ));
        }
    };
    Ok(ObjectLockState {
        retention,
        legal_hold,
    })
}

/// Response headers describing a version's lock.
pub fn lock_headers(lock: &ObjectLockState) -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    if let Some(r) = &lock.retention {
        out.push(("x-amz-object-lock-mode", mode_str(r.mode).to_string()));
        out.push(("x-amz-object-lock-retain-until-date", iso8601(&r.until)));
    }
    if lock.legal_hold {
        out.push(("x-amz-object-lock-legal-hold", "ON".to_string()));
    }
    out
}

pub fn bypass_governance(headers: &HeaderMap) -> bool {
    headers
        .get("x-amz-bypass-governance-retention")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

// -------------------------------------------------------------- lifecycle

#[derive(Deserialize)]
struct LifecycleXml {
    #[serde(rename = "Rule", default)]
    rules: Vec<RuleXml>,
}

#[derive(Deserialize)]
struct RuleXml {
    #[serde(rename = "ID")]
    id: Option<String>,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "Prefix")]
    prefix: Option<String>,
    #[serde(rename = "Filter")]
    filter: Option<FilterXml>,
    #[serde(rename = "Expiration")]
    expiration: Option<ExpirationXml>,
    #[serde(rename = "NoncurrentVersionExpiration")]
    noncurrent: Option<NoncurrentXml>,
    #[serde(rename = "AbortIncompleteMultipartUpload")]
    abort: Option<AbortXml>,
    #[serde(rename = "Transition", default)]
    transitions: Vec<TransitionXml>,
    #[serde(rename = "NoncurrentVersionTransition", default)]
    noncurrent_transitions: Vec<serde::de::IgnoredAny>,
}

#[derive(Deserialize)]
struct TransitionXml {
    #[serde(rename = "Days")]
    days: Option<u32>,
    #[serde(rename = "Date")]
    date: Option<String>,
    #[serde(rename = "StorageClass")]
    class: String,
}

#[derive(Deserialize, Default)]
struct FilterXml {
    #[serde(rename = "Prefix")]
    prefix: Option<String>,
    #[serde(rename = "Tag")]
    tag: Option<TagXml>,
    #[serde(rename = "And")]
    and: Option<AndXml>,
}

#[derive(Deserialize)]
struct AndXml {
    #[serde(rename = "Prefix")]
    prefix: Option<String>,
    #[serde(rename = "Tag", default)]
    tags: Vec<TagXml>,
}

#[derive(Deserialize)]
struct TagXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value")]
    value: String,
}

#[derive(Deserialize)]
struct ExpirationXml {
    #[serde(rename = "Days")]
    days: Option<u32>,
    #[serde(rename = "Date")]
    date: Option<String>,
    #[serde(rename = "ExpiredObjectDeleteMarker")]
    expired_marker: Option<bool>,
}

#[derive(Deserialize)]
struct NoncurrentXml {
    #[serde(rename = "NoncurrentDays")]
    days: u32,
}

#[derive(Deserialize)]
struct AbortXml {
    #[serde(rename = "DaysAfterInitiation")]
    days: u32,
}

pub fn parse_lifecycle(xml: &str) -> S3Result<Vec<LifecycleRule>> {
    let c: LifecycleXml = parse(xml)?;
    if c.rules.is_empty() || c.rules.len() > 1000 {
        return Err(S3Error::malformed_xml(
            "a lifecycle configuration needs 1-1000 rules",
        ));
    }
    let mut out = Vec::new();
    for r in c.rules {
        if !r.noncurrent_transitions.is_empty() || r.transitions.len() > 1 {
            return Err(S3Error::new(
                StatusCode::NOT_IMPLEMENTED,
                "NotImplemented",
                "only one Transition (of current versions) per rule is supported",
            ));
        }
        let (mut transition_days, mut transition_date, mut transition_class) = (None, None, None);
        if let Some(t) = r.transitions.into_iter().next() {
            if !kosha_engine::is_cold_class(&t.class) {
                return Err(S3Error::new(
                    StatusCode::BAD_REQUEST,
                    "InvalidStorageClass",
                    format!("cannot transition to {}", t.class),
                ));
            }
            if t.days.is_none() == t.date.is_none() {
                return Err(S3Error::malformed_xml("a Transition needs Days or Date"));
            }
            transition_days = t.days;
            transition_date = t.date.as_deref().map(parse_date).transpose()?;
            transition_class = Some(t.class);
        }
        let mut prefix = r.prefix.unwrap_or_default();
        let mut tags = BTreeMap::new();
        if let Some(f) = r.filter {
            if let Some(p) = f.prefix {
                prefix = p;
            }
            if let Some(t) = f.tag {
                tags.insert(t.key, t.value);
            }
            if let Some(a) = f.and {
                if let Some(p) = a.prefix {
                    prefix = p;
                }
                for t in a.tags {
                    tags.insert(t.key, t.value);
                }
            }
        }
        let (mut expiration_days, mut expiration_date, mut expired_marker) = (None, None, false);
        if let Some(e) = r.expiration {
            expiration_days = e.days;
            expiration_date = e.date.as_deref().map(parse_date).transpose()?;
            expired_marker = e.expired_marker.unwrap_or(false);
        }
        let rule = LifecycleRule {
            id: r.id,
            enabled: match r.status.as_str() {
                "Enabled" => true,
                "Disabled" => false,
                other => {
                    return Err(S3Error::malformed_xml(format!(
                        "unknown rule status {other:?}"
                    )));
                }
            },
            prefix,
            tags,
            expiration_days,
            expiration_date,
            expired_object_delete_marker: expired_marker,
            noncurrent_days: r.noncurrent.map(|n| n.days),
            abort_incomplete_days: r.abort.map(|a| a.days),
            transition_days,
            transition_date,
            transition_class,
        };
        if rule.expiration_days.is_none()
            && rule.transition_class.is_none()
            && rule.expiration_date.is_none()
            && !rule.expired_object_delete_marker
            && rule.noncurrent_days.is_none()
            && rule.abort_incomplete_days.is_none()
        {
            return Err(S3Error::malformed_xml(
                "each rule needs at least one action",
            ));
        }
        out.push(rule);
    }
    Ok(out)
}

pub fn render_lifecycle(rules: &[LifecycleRule]) -> String {
    let mut x = Xml::new("LifecycleConfiguration");
    for r in rules {
        x.open("Rule");
        x.opt("ID", r.id.as_deref());
        x.open("Filter");
        if r.tags.is_empty() {
            x.elem("Prefix", &r.prefix);
        } else {
            x.open("And").elem("Prefix", &r.prefix);
            for (k, v) in &r.tags {
                x.open("Tag").elem("Key", k).elem("Value", v).close("Tag");
            }
            x.close("And");
        }
        x.close("Filter");
        x.elem("Status", if r.enabled { "Enabled" } else { "Disabled" });
        if r.expiration_days.is_some()
            || r.expiration_date.is_some()
            || r.expired_object_delete_marker
        {
            x.open("Expiration");
            x.opt("Days", r.expiration_days.map(|d| d.to_string()));
            x.opt("Date", r.expiration_date.as_ref().map(iso8601));
            if r.expired_object_delete_marker {
                x.elem("ExpiredObjectDeleteMarker", "true");
            }
            x.close("Expiration");
        }
        if let Some(class) = &r.transition_class {
            x.open("Transition");
            x.opt("Days", r.transition_days.map(|d| d.to_string()));
            x.opt("Date", r.transition_date.as_ref().map(iso8601));
            x.elem("StorageClass", class).close("Transition");
        }
        if let Some(d) = r.noncurrent_days {
            x.open("NoncurrentVersionExpiration")
                .elem("NoncurrentDays", d.to_string())
                .close("NoncurrentVersionExpiration");
        }
        if let Some(d) = r.abort_incomplete_days {
            x.open("AbortIncompleteMultipartUpload")
                .elem("DaysAfterInitiation", d.to_string())
                .close("AbortIncompleteMultipartUpload");
        }
        x.close("Rule");
    }
    x.finish("LifecycleConfiguration")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_roundtrip_and_filters() {
        let xml = r#"<LifecycleConfiguration><Rule><ID>tmp</ID><Filter><And><Prefix>logs/</Prefix><Tag><Key>tier</Key><Value>tmp</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Days>7</Days></Expiration><NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays></NoncurrentVersionExpiration><AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>"#;
        let rules = parse_lifecycle(xml).unwrap();
        assert_eq!(rules[0].prefix, "logs/");
        assert_eq!(rules[0].tags["tier"], "tmp");
        assert_eq!(
            (
                rules[0].expiration_days,
                rules[0].noncurrent_days,
                rules[0].abort_incomplete_days
            ),
            (Some(7), Some(30), Some(2))
        );
        assert_eq!(parse_lifecycle(&render_lifecycle(&rules)).unwrap(), rules);
        let transition = r#"<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>"#;
        let rules = parse_lifecycle(transition).unwrap();
        assert_eq!(
            (
                rules[0].transition_days,
                rules[0].transition_class.as_deref()
            ),
            (Some(30), Some("GLACIER"))
        );
        assert_eq!(parse_lifecycle(&render_lifecycle(&rules)).unwrap(), rules);
        let bad = transition.replace("GLACIER", "PLATINUM");
        assert_eq!(
            parse_lifecycle(&bad).unwrap_err().code,
            "InvalidStorageClass"
        );
        let noncurrent = transition
            .replace("<Transition>", "<NoncurrentVersionTransition>")
            .replace("</Transition>", "</NoncurrentVersionTransition>");
        assert_eq!(
            parse_lifecycle(&noncurrent).unwrap_err().code,
            "NotImplemented"
        );
    }

    #[test]
    fn lock_config_and_retention() {
        let c = parse_lock_config("<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Years>1</Years></DefaultRetention></Rule></ObjectLockConfiguration>").unwrap();
        assert_eq!(c.default_retention.as_ref().unwrap().days, 365);
        assert_eq!(parse_lock_config(&render_lock_config(&c)).unwrap(), c);
        let r = parse_retention("<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>").unwrap().unwrap();
        assert_eq!(r.mode, RetentionMode::Governance);
        assert!(parse_legal_hold("<LegalHold><Status>ON</Status></LegalHold>").unwrap());
    }
}
