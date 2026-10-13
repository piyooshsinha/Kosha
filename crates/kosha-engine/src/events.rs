//! Event notifications from the ordered metadata log.
//!
//! When a command succeeds on a bucket with notification rules, the state
//! machine appends one event per matching rule to that rule's target queue,
//! in the same transaction as the change itself. Every replica therefore
//! holds the same queue, in log order, and no change can commit without its
//! event. A dispatcher (outside the engine) delivers events and proposes
//! [`Command::AckEvents`] to remove them: at-least-once delivery that
//! survives crashes and leader changes.

use chrono::{DateTime, Utc};
use redb::{ReadableDatabase, ReadableTable};

use crate::engine::Engine;
use crate::error::Result;
use crate::meta::{Command, CommandOutput, EVENTS, META, MetaStore, Step, Tx, dec, enc};
use crate::types::*;

/// Undelivered events kept per target; the oldest are dropped beyond this.
pub const MAX_PENDING: u64 = 100_000;
const EVENT_SEQ_KEY: &str = "event_seq";

/// What a command needs to describe its events, captured before it runs.
pub(crate) enum Source {
    None,
    Put {
        bucket: String,
        key: String,
        name: &'static str,
    },
    Delete {
        bucket: String,
        lifecycle: bool,
        now: DateTime<Utc>,
    },
    Tags {
        bucket: String,
        key: String,
        removed: bool,
        now: Option<DateTime<Utc>>,
    },
}

impl Source {
    pub(crate) fn of(cmd: &Command) -> Source {
        match cmd {
            Command::PutObject { bucket, key, .. } => Source::Put {
                bucket: bucket.clone(),
                key: key.clone(),
                name: "ObjectCreated:Put",
            },
            Command::CopyObject {
                dst_bucket,
                dst_key,
                ..
            } => Source::Put {
                bucket: dst_bucket.clone(),
                key: dst_key.clone(),
                name: "ObjectCreated:Copy",
            },
            Command::CompleteUpload { bucket, key, .. } => Source::Put {
                bucket: bucket.clone(),
                key: key.clone(),
                name: "ObjectCreated:CompleteMultipartUpload",
            },
            Command::DeleteObjects {
                bucket,
                lifecycle,
                now,
                ..
            } => Source::Delete {
                bucket: bucket.clone(),
                lifecycle: *lifecycle,
                now: *now,
            },
            Command::SetObjectTags {
                bucket,
                key,
                tags,
                now,
                ..
            } => Source::Tags {
                bucket: bucket.clone(),
                key: key.clone(),
                removed: tags.is_empty(),
                now: *now,
            },
            _ => Source::None,
        }
    }

    fn bucket(&self) -> Option<&str> {
        match self {
            Source::None => None,
            Source::Put { bucket, .. }
            | Source::Delete { bucket, .. }
            | Source::Tags { bucket, .. } => Some(bucket),
        }
    }
}

fn queue_key(target: &str, seq: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(target.len() + 9);
    k.extend_from_slice(target.as_bytes());
    k.push(0);
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

fn queue_range(target: &str) -> (Vec<u8>, Vec<u8>) {
    (queue_key(target, 0), queue_key(target, u64::MAX))
}

struct Occurrence {
    key: String,
    name: &'static str,
    size: Option<u64>,
    etag: Option<String>,
    version_id: Option<String>,
    time: DateTime<Utc>,
}

impl Tx<'_> {
    pub(crate) fn emit(&mut self, source: Source, out: &CommandOutput) -> Step<()> {
        let Some(bucket) = source.bucket() else {
            return Ok(());
        };
        let rules = match self.bucket_info(bucket) {
            Ok(info) => info.config.notifications.unwrap_or_default(),
            Err(_) => return Ok(()),
        };
        if rules.is_empty() {
            return Ok(());
        }
        let created = |key: &str, m: &ObjectMeta, name| Occurrence {
            key: key.to_string(),
            name,
            size: Some(m.size),
            etag: Some(m.etag.clone()),
            version_id: m.version_id.clone(),
            time: m.last_modified,
        };
        let occurrences: Vec<Occurrence> = match (&source, out) {
            (Source::Put { key, name, .. }, CommandOutput::Object(m)) => {
                vec![created(key, m, name)]
            }
            (
                Source::Tags {
                    key, removed, now, ..
                },
                CommandOutput::Object(m),
            ) => {
                let name = if *removed {
                    "ObjectTagging:Delete"
                } else {
                    "ObjectTagging:Put"
                };
                vec![Occurrence {
                    time: now.unwrap_or(m.last_modified),
                    ..created(key, m, name)
                }]
            }
            (Source::Delete { lifecycle, now, .. }, CommandOutput::Deleted(outcomes)) => outcomes
                .iter()
                .filter(|o| o.error.is_none() && (o.existed || o.delete_marker))
                .map(|o| Occurrence {
                    key: o.key.clone(),
                    name: match (lifecycle, o.delete_marker) {
                        (false, false) => "ObjectRemoved:Delete",
                        (false, true) => "ObjectRemoved:DeleteMarkerCreated",
                        (true, false) => "LifecycleExpiration:Delete",
                        (true, true) => "LifecycleExpiration:DeleteMarkerCreated",
                    },
                    size: None,
                    etag: None,
                    version_id: o.version_id.clone(),
                    time: *now,
                })
                .collect(),
            _ => Vec::new(),
        };
        for occ in occurrences {
            for rule in rules.iter().filter(|r| r.matches(occ.name, &occ.key)) {
                let seq = self.next_event_seq()?;
                let event = Event {
                    seq,
                    target: rule.target.clone(),
                    rule_id: rule.id.clone(),
                    bucket: bucket.to_string(),
                    key: occ.key.clone(),
                    name: occ.name.to_string(),
                    size: occ.size,
                    etag: occ.etag.clone(),
                    version_id: occ.version_id.clone(),
                    time: occ.time,
                };
                let mut q = self.txn.open_table(EVENTS)?;
                q.insert(
                    queue_key(&rule.target, seq).as_slice(),
                    enc(&event)?.as_slice(),
                )?;
                // Bound the queue of a target that is down for a long time.
                let (lo, hi) = queue_range(&rule.target);
                let oldest = q
                    .range(lo.as_slice()..=hi.as_slice())?
                    .next()
                    .transpose()?
                    .map(|(k, _)| k.value().to_vec());
                if let Some(oldest) = oldest {
                    let oldest_seq =
                        u64::from_be_bytes(oldest[oldest.len() - 8..].try_into().unwrap());
                    if seq - oldest_seq >= MAX_PENDING {
                        q.remove(oldest.as_slice())?;
                        self.stats.events_dropped += 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn next_event_seq(&mut self) -> Step<u64> {
        let mut meta = self.txn.open_table(META)?;
        let seq = match meta.get(EVENT_SEQ_KEY)? {
            Some(v) => dec::<u64>(v.value())? + 1,
            None => 1,
        };
        meta.insert(EVENT_SEQ_KEY, enc(&seq)?.as_slice())?;
        Ok(seq)
    }

    pub(crate) fn ack_events(&mut self, target: &str, upto: u64) -> Step<()> {
        let mut q = self.txn.open_table(EVENTS)?;
        let (lo, _) = queue_range(target);
        let hi = queue_key(target, upto);
        let keys: Vec<Vec<u8>> = q
            .range(lo.as_slice()..=hi.as_slice())?
            .map(|r| r.map(|(k, _)| k.value().to_vec()))
            .collect::<std::result::Result<_, _>>()?;
        for k in keys {
            q.remove(k.as_slice())?;
        }
        Ok(())
    }
}

impl MetaStore {
    /// The oldest undelivered events of `target`, in order.
    pub fn pending_events(&self, target: &str, limit: usize) -> Result<Vec<Event>> {
        let r = self.db.begin_read()?;
        let q = r.open_table(EVENTS)?;
        let (lo, hi) = queue_range(target);
        let mut out = Vec::new();
        for item in q.range(lo.as_slice()..=hi.as_slice())?.take(limit) {
            out.push(dec(item?.1.value())?);
        }
        Ok(out)
    }

    /// Number of undelivered events per target.
    pub fn pending_counts(&self) -> Result<Vec<(String, u64)>> {
        let r = self.db.begin_read()?;
        let q = r.open_table(EVENTS)?;
        let mut out: Vec<(String, u64)> = Vec::new();
        for item in q.iter()? {
            let (k, _) = item?;
            let k = k.value();
            let target = String::from_utf8_lossy(&k[..k.len().saturating_sub(9)]).into_owned();
            match out.last_mut() {
                Some((t, n)) if *t == target => *n += 1,
                _ => out.push((target, 1)),
            }
        }
        Ok(out)
    }
}

impl Engine {
    /// Undelivered events of `target` from local state (the dispatcher runs
    /// on the leader, whose state is current).
    pub fn pending_events(&self, target: &str, limit: usize) -> Result<Vec<Event>> {
        self.meta().pending_events(target, limit)
    }

    pub fn pending_event_counts(&self) -> Result<Vec<(String, u64)>> {
        self.meta().pending_counts()
    }

    /// Remove delivered events of `target` up to `upto`.
    pub async fn ack_events(&self, target: &str, upto: u64) -> Result<()> {
        self.propose_cmd(Command::AckEvents {
            target: target.into(),
            upto,
        })
        .await
        .map(drop)
    }
}
