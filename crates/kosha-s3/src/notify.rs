//! Bucket notifications: the S3 configuration XML, the event JSON, and the
//! webhook dispatcher.
//!
//! Targets are configured on the servers (`--notify-webhook name=url`) and
//! referenced from bucket configurations by ARN, `arn:kosha:sqs::<name>:webhook`
//! (Queue, Topic and CloudFunction configurations are all accepted). Events
//! come from the engine's replicated queues; the dispatcher on the leader
//! posts them in order, one request per event, and acknowledges delivered
//! ones. Delivery is at least once: receivers can deduplicate on
//! `s3.object.sequencer`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use kosha_engine::{Engine, Event, NotificationRule};
use serde::Deserialize;
use serde_json::json;

use crate::error::{S3Error, S3Result};
use crate::xml::{Xml, iso8601};

/// Webhook targets by name.
pub type Webhooks = BTreeMap<String, String>;

pub fn target_arn(name: &str) -> String {
    format!("arn:kosha:sqs::{name}:webhook")
}

fn target_of_arn(arn: &str) -> &str {
    arn.strip_prefix("arn:kosha:sqs::")
        .and_then(|r| r.strip_suffix(":webhook"))
        .unwrap_or(arn)
}

// -------------------------------------------------------------------- XML

#[derive(Deserialize, Default)]
struct FilterRuleXml {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Value")]
    value: String,
}

#[derive(Deserialize, Default)]
struct S3KeyXml {
    #[serde(rename = "FilterRule", default)]
    rules: Vec<FilterRuleXml>,
}

#[derive(Deserialize, Default)]
struct FilterXml {
    #[serde(rename = "S3Key", default)]
    key: S3KeyXml,
}

#[derive(Deserialize)]
struct TargetConfigXml {
    #[serde(rename = "Id")]
    id: Option<String>,
    #[serde(rename = "Queue", alias = "Topic", alias = "CloudFunction")]
    arn: String,
    #[serde(rename = "Event", default)]
    events: Vec<String>,
    #[serde(rename = "Filter", default)]
    filter: FilterXml,
}

#[derive(Deserialize)]
struct NotificationXml {
    #[serde(rename = "QueueConfiguration", default)]
    queues: Vec<TargetConfigXml>,
    #[serde(rename = "TopicConfiguration", default)]
    topics: Vec<TargetConfigXml>,
    #[serde(rename = "CloudFunctionConfiguration", default)]
    functions: Vec<TargetConfigXml>,
}

const EVENT_TYPES: &[&str] = &[
    "ObjectCreated:*",
    "ObjectCreated:Put",
    "ObjectCreated:Post",
    "ObjectCreated:Copy",
    "ObjectCreated:CompleteMultipartUpload",
    "ObjectRemoved:*",
    "ObjectRemoved:Delete",
    "ObjectRemoved:DeleteMarkerCreated",
    "LifecycleExpiration:*",
    "LifecycleExpiration:Delete",
    "LifecycleExpiration:DeleteMarkerCreated",
    "ObjectTagging:*",
    "ObjectTagging:Put",
    "ObjectTagging:Delete",
];

/// Parse a NotificationConfiguration; every target must be configured.
pub fn parse(xml: &str, webhooks: &Webhooks) -> S3Result<Option<Vec<NotificationRule>>> {
    let doc: NotificationXml =
        quick_xml::de::from_str(xml).map_err(|e| S3Error::malformed_xml(e.to_string()))?;
    let mut rules = Vec::new();
    for (i, c) in doc
        .queues
        .into_iter()
        .chain(doc.topics)
        .chain(doc.functions)
        .enumerate()
    {
        let target = target_of_arn(&c.arn).to_string();
        if !webhooks.contains_key(&target) {
            return Err(S3Error::invalid_argument(format!(
                "Unable to validate the destination {}: configure it on the servers with --notify-webhook {target}=<url>",
                c.arn
            )));
        }
        if c.events.is_empty() {
            return Err(S3Error::invalid_argument(
                "each configuration needs an Event",
            ));
        }
        for e in &c.events {
            let bare = e.strip_prefix("s3:").unwrap_or(e);
            if !EVENT_TYPES.contains(&bare) {
                return Err(S3Error::invalid_argument(format!(
                    "unsupported event type {e}"
                )));
            }
        }
        let (mut prefix, mut suffix) = (String::new(), String::new());
        for r in c.filter.key.rules {
            match r.name.to_ascii_lowercase().as_str() {
                "prefix" => prefix = r.value,
                "suffix" => suffix = r.value,
                other => {
                    return Err(S3Error::invalid_argument(format!(
                        "unsupported filter rule {other}"
                    )));
                }
            }
        }
        rules.push(NotificationRule {
            id: c.id.unwrap_or_else(|| format!("rule-{}", i + 1)),
            target,
            events: c.events,
            prefix,
            suffix,
        });
    }
    Ok((!rules.is_empty()).then_some(rules))
}

pub fn render(rules: &[NotificationRule]) -> String {
    let mut x = Xml::new("NotificationConfiguration");
    for r in rules {
        x.open("QueueConfiguration")
            .elem("Id", &r.id)
            .elem("Queue", target_arn(&r.target));
        for e in &r.events {
            x.elem("Event", e);
        }
        if !r.prefix.is_empty() || !r.suffix.is_empty() {
            x.open("Filter").open("S3Key");
            for (name, v) in [("prefix", &r.prefix), ("suffix", &r.suffix)] {
                if !v.is_empty() {
                    x.open("FilterRule")
                        .elem("Name", name)
                        .elem("Value", v)
                        .close("FilterRule");
                }
            }
            x.close("S3Key").close("Filter");
        }
        x.close("QueueConfiguration");
    }
    x.finish("NotificationConfiguration")
}

// ------------------------------------------------------------------ JSON

/// Object keys in event messages are URL-encoded the way S3 does it
/// (spaces become `+`).
fn encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The S3 event message (structure version 2.1) for one event.
pub fn event_json(e: &Event, region: &str) -> serde_json::Value {
    let mut object = json!({
        "key": encode_key(&e.key),
        "sequencer": format!("{:016X}", e.seq),
    });
    if let Some(size) = e.size {
        object["size"] = json!(size);
    }
    if let Some(etag) = &e.etag {
        object["eTag"] = json!(etag);
    }
    if let Some(v) = &e.version_id {
        object["versionId"] = json!(v);
    }
    json!({
        "Records": [{
            "eventVersion": "2.1",
            "eventSource": "kosha:s3",
            "awsRegion": region,
            "eventTime": iso8601(&e.time),
            "eventName": e.name,
            "userIdentity": { "principalId": "kosha" },
            "requestParameters": { "sourceIPAddress": "" },
            "responseElements": {},
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": e.rule_id,
                "bucket": {
                    "name": e.bucket,
                    "ownerIdentity": { "principalId": "kosha" },
                    "arn": format!("arn:aws:s3:::{}", e.bucket),
                },
                "object": object,
            },
        }]
    })
}

// ------------------------------------------------------------ dispatcher

/// Delivery counters, exported as Prometheus metrics.
#[derive(Default, Debug)]
pub struct DispatchStats {
    pub delivered: AtomicU64,
    pub failed: AtomicU64,
}

pub static STATS: DispatchStats = DispatchStats {
    delivered: AtomicU64::new(0),
    failed: AtomicU64::new(0),
};

/// Deliver each target's events while `gate()` holds (in a cluster: while
/// this node leads). One task per target, so a dead target delays only its
/// own queue.
pub fn spawn_dispatcher(
    engine: Engine,
    webhooks: Webhooks,
    region: String,
    gate: Arc<dyn Fn() -> bool + Send + Sync>,
) {
    if webhooks.is_empty() {
        return;
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .expect("http client");
    for (name, url) in webhooks {
        let (engine, http, region, gate) =
            (engine.clone(), http.clone(), region.clone(), gate.clone());
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                if !gate() {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                let batch = match engine.pending_events(&name, 64) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(target = %name, error = %e, "reading events failed");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                if batch.is_empty() {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                let mut delivered = None;
                for e in &batch {
                    let sent = http
                        .post(&url)
                        .header("content-type", "application/json")
                        .header("x-kosha-event-sequencer", format!("{:016X}", e.seq))
                        .body(event_json(e, &region).to_string())
                        .send()
                        .await;
                    match sent {
                        Ok(r) if r.status().is_success() => {
                            delivered = Some(e.seq);
                            STATS.delivered.fetch_add(1, Relaxed);
                        }
                        other => {
                            STATS.failed.fetch_add(1, Relaxed);
                            let why = match other {
                                Ok(r) => format!("HTTP {}", r.status()),
                                Err(e) => e.to_string(),
                            };
                            tracing::warn!(target = %name, seq = e.seq, error = %why, "webhook delivery failed; will retry");
                            break;
                        }
                    }
                }
                if let Some(upto) = delivered
                    && let Err(e) = engine.ack_events(&name, upto).await
                {
                    tracing::warn!(target = %name, error = %e, "acknowledging events failed");
                }
                if delivered.map(|s| s == batch.last().unwrap().seq) == Some(true) {
                    backoff = Duration::from_millis(500);
                } else {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_renders_configuration() {
        let hooks = Webhooks::from([("audit".to_string(), "http://x".to_string())]);
        let xml = r#"<NotificationConfiguration>
          <QueueConfiguration><Id>imgs</Id><Queue>arn:kosha:sqs::audit:webhook</Queue>
            <Event>s3:ObjectCreated:*</Event><Event>s3:ObjectRemoved:Delete</Event>
            <Filter><S3Key><FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule>
            <FilterRule><Name>Suffix</Name><Value>.jpg</Value></FilterRule></S3Key></Filter>
          </QueueConfiguration>
          <TopicConfiguration><Topic>audit</Topic><Event>s3:ObjectTagging:*</Event></TopicConfiguration>
        </NotificationConfiguration>"#;
        let rules = parse(xml, &hooks).unwrap().unwrap();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].prefix, "images/");
        assert_eq!(rules[0].suffix, ".jpg");
        assert!(rules[0].matches("ObjectCreated:Copy", "images/a.jpg"));
        assert!(!rules[0].matches("ObjectCreated:Copy", "images/a.png"));
        assert!(!rules[0].matches("ObjectRemoved:DeleteMarkerCreated", "images/a.jpg"));
        assert_eq!(rules[1].id, "rule-2");
        let again = parse(&render(&rules), &hooks).unwrap().unwrap();
        assert_eq!(again, rules);

        assert!(
            parse("<NotificationConfiguration/>", &hooks)
                .unwrap()
                .is_none()
        );
        let unknown = xml.replace("audit:webhook", "nope:webhook");
        assert!(parse(&unknown, &hooks).is_err());
        let bad_event = xml.replace("s3:ObjectTagging:*", "s3:ObjectTeleported");
        assert!(parse(&bad_event, &hooks).is_err());
    }

    #[test]
    fn keys_are_url_encoded() {
        assert_eq!(encode_key("photos/my cat+1.jpg"), "photos/my+cat%2B1.jpg");
    }
}
