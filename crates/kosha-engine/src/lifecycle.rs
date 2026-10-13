//! Lifecycle rules: expiration of current objects and noncurrent versions,
//! removal of expired delete markers, and aborting abandoned multipart
//! uploads.
//!
//! A pass only *proposes* ordinary commands (DeleteObjects, AbortUpload), so
//! every action is replicated, ordered and checked like a client request.
//! In particular, Object Lock still applies: locked versions are counted as
//! `refused` and left alone. In a cluster only the leader runs passes.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};

use crate::engine::Engine;
use crate::error::Result;
use crate::types::*;

const BATCH: usize = 1000;
/// The storage class auto-tiered objects carry while they are cold.
pub const AUTO_TIER_CLASS: &str = "INTELLIGENT_TIERING";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LifecycleReport {
    /// Current objects expired (deleted, or hidden behind a delete marker).
    pub expired: u64,
    /// Noncurrent versions permanently removed.
    pub noncurrent_removed: u64,
    /// Delete markers removed because no other version remained.
    pub markers_removed: u64,
    pub uploads_aborted: u64,
    /// Current objects moved to the cold tier.
    pub transitioned: u64,
    /// Noncurrent versions moved to the cold tier.
    pub noncurrent_transitioned: u64,
    /// Objects moved to the cold tier because nobody read them.
    pub auto_tiered: u64,
    /// Actions refused by Object Lock.
    pub refused: u64,
}

fn tags_match(rule: &LifecycleRule, tags: &BTreeMap<String, String>) -> bool {
    rule.tags.iter().all(|(k, v)| tags.get(k) == Some(v))
}

fn older_than(t: DateTime<Utc>, days: u32, now: DateTime<Utc>) -> bool {
    now - t >= Duration::days(days as i64)
}

impl Engine {
    async fn delete_batch(
        &self,
        bucket: &str,
        targets: &mut Vec<DeleteTarget>,
        report: &mut LifecycleReport,
        kind: fn(&mut LifecycleReport) -> &mut u64,
    ) -> Result<()> {
        if targets.is_empty() {
            return Ok(());
        }
        let outcomes = self
            .delete_targets_as(bucket, std::mem::take(targets), false, true)
            .await?;
        for o in outcomes {
            if o.error.is_some() {
                report.refused += 1;
            } else {
                *kind(report) += 1;
            }
        }
        Ok(())
    }

    /// Evaluate every bucket's lifecycle rules as of `now`.
    pub async fn run_lifecycle(&self, now: DateTime<Utc>) -> Result<LifecycleReport> {
        let mut report = LifecycleReport::default();
        for info in self.list_buckets().await? {
            if let Some(rules) = info.config.lifecycle.clone() {
                for rule in rules.iter().filter(|r| r.enabled) {
                    self.apply_rule(&info, rule, now, &mut report).await?;
                }
            }
            if let Some(rules) = info.config.tiering.clone()
                && self.can_tier()
            {
                for rule in rules.iter().filter(|r| r.enabled) {
                    self.apply_tiering(&info, rule, now, &mut report).await?;
                }
            }
        }
        Ok(report)
    }

    /// Access-based tiering: STANDARD objects under the rule that nobody
    /// has read (or written) for `days` move to the cold tier, marked so a
    /// later read brings them back.
    async fn apply_tiering(
        &self,
        info: &BucketInfo,
        rule: &TieringRule,
        now: DateTime<Utc>,
        report: &mut LifecycleReport,
    ) -> Result<()> {
        let bucket = info.name.as_str();
        let mut targets = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .list_objects(
                    bucket,
                    ListQuery {
                        prefix: rule.prefix.clone(),
                        delimiter: None,
                        start_after: after.clone(),
                        max_keys: BATCH,
                    },
                )
                .await?;
            for o in &page.objects {
                if o.storage_class.is_some() {
                    continue;
                }
                let last_read = self.meta().last_access(bucket, &o.key)?;
                let last_use = last_read.map_or(o.last_modified, |r| r.max(o.last_modified));
                if !older_than(last_use, rule.days, now) {
                    continue;
                }
                if !rule.tags.is_empty() {
                    match self.head_object(bucket, &o.key).await {
                        Ok(m)
                            if rule
                                .tags
                                .iter()
                                .all(|(k, v)| m.attrs.tags.get(k) == Some(v)) => {}
                        _ => continue,
                    }
                }
                targets.push(DeleteTarget {
                    key: o.key.clone(),
                    version_id: None,
                });
                if targets.len() >= BATCH {
                    report.auto_tiered += self
                        .transition_objects_as(
                            bucket,
                            std::mem::take(&mut targets),
                            AUTO_TIER_CLASS,
                            true,
                        )
                        .await?;
                }
            }
            if !page.is_truncated {
                break;
            }
            after = page.next_marker;
        }
        if !targets.is_empty() {
            report.auto_tiered += self
                .transition_objects_as(bucket, targets, AUTO_TIER_CLASS, true)
                .await?;
        }
        Ok(())
    }

    async fn apply_rule(
        &self,
        info: &BucketInfo,
        rule: &LifecycleRule,
        now: DateTime<Utc>,
        report: &mut LifecycleReport,
    ) -> Result<()> {
        let bucket = info.name.as_str();

        // Current-version expiration.
        let expires = |t: DateTime<Utc>| {
            rule.expiration_days.is_some_and(|d| older_than(t, d, now))
                || rule.expiration_date.is_some_and(|d| now >= d)
        };
        if rule.expiration_days.is_some() || rule.expiration_date.is_some() {
            let mut targets = Vec::new();
            let mut after = None;
            loop {
                let page = self
                    .list_objects(
                        bucket,
                        ListQuery {
                            prefix: rule.prefix.clone(),
                            delimiter: None,
                            start_after: after.clone(),
                            max_keys: BATCH,
                        },
                    )
                    .await?;
                for o in &page.objects {
                    if !expires(o.last_modified) {
                        continue;
                    }
                    if !rule.tags.is_empty() {
                        match self.head_object(bucket, &o.key).await {
                            Ok(m) if tags_match(rule, &m.attrs.tags) => {}
                            _ => continue,
                        }
                    }
                    targets.push(DeleteTarget {
                        key: o.key.clone(),
                        version_id: None,
                    });
                    if targets.len() >= BATCH {
                        self.delete_batch(bucket, &mut targets, report, |r| &mut r.expired)
                            .await?;
                    }
                }
                if !page.is_truncated {
                    break;
                }
                after = page.next_marker;
            }
            self.delete_batch(bucket, &mut targets, report, |r| &mut r.expired)
                .await?;
        }

        // Transition current objects to the cold tier.
        let transition_due = |t: DateTime<Utc>| {
            rule.transition_days.is_some_and(|d| older_than(t, d, now))
                || rule.transition_date.is_some_and(|d| now >= d)
        };
        if let Some(class) = &rule.transition_class
            && self.can_tier()
        {
            let mut targets = Vec::new();
            let mut after = None;
            loop {
                let page = self
                    .list_objects(
                        bucket,
                        ListQuery {
                            prefix: rule.prefix.clone(),
                            delimiter: None,
                            start_after: after.clone(),
                            max_keys: BATCH,
                        },
                    )
                    .await?;
                for o in &page.objects {
                    if o.storage_class.is_some()
                        || !transition_due(o.last_modified)
                        || expires(o.last_modified)
                    {
                        continue;
                    }
                    if !rule.tags.is_empty() {
                        match self.head_object(bucket, &o.key).await {
                            Ok(m) if tags_match(rule, &m.attrs.tags) => {}
                            _ => continue,
                        }
                    }
                    targets.push(DeleteTarget {
                        key: o.key.clone(),
                        version_id: None,
                    });
                    if targets.len() >= BATCH {
                        report.transitioned += self
                            .transition_objects(bucket, std::mem::take(&mut targets), class)
                            .await?;
                    }
                }
                if !page.is_truncated {
                    break;
                }
                after = page.next_marker;
            }
            if !targets.is_empty() {
                report.transitioned += self.transition_objects(bucket, targets, class).await?;
            }
        }

        // Noncurrent versions and lone delete markers (versioned buckets).
        let noncurrent_tier = rule
            .noncurrent_transition_days
            .zip(rule.noncurrent_transition_class.as_ref())
            .filter(|_| self.can_tier());
        if info.config.versioning != Versioning::Unversioned
            && (rule.noncurrent_days.is_some()
                || rule.expired_object_delete_marker
                || noncurrent_tier.is_some())
        {
            let mut noncurrent = Vec::new();
            let mut markers = Vec::new();
            let mut to_cold = Vec::new();
            // Versions of the key being scanned, newest first, each with
            // whether the rule's tag filter selects it.
            let mut group: Vec<(VersionInfo, bool)> = Vec::new();
            let flush = |group: &mut Vec<(VersionInfo, bool)>,
                         noncurrent: &mut Vec<DeleteTarget>,
                         markers: &mut Vec<DeleteTarget>,
                         to_cold: &mut Vec<DeleteTarget>| {
                if group.is_empty() {
                    return;
                }
                let mut removed = 0;
                let mut expiring = std::collections::HashSet::new();
                if let Some(days) = rule.noncurrent_days {
                    // A version became noncurrent when its successor was written.
                    for i in 1..group.len() {
                        let (newer, (v, eligible)) = (&group[i - 1].0, &group[i]);
                        if *eligible && older_than(newer.last_modified, days, now) {
                            noncurrent.push(DeleteTarget {
                                key: v.key.clone(),
                                version_id: Some(v.version_id.clone()),
                            });
                            expiring.insert(i);
                            removed += 1;
                        }
                    }
                }
                // Noncurrent versions that stay, but move to the cold tier.
                if let Some((days, _)) = noncurrent_tier {
                    for i in 1..group.len() {
                        let (newer, (v, eligible)) = (&group[i - 1].0, &group[i]);
                        let hot = v.meta.as_ref().is_some_and(|m| m.storage_class.is_none());
                        if *eligible
                            && hot
                            && !expiring.contains(&i)
                            && older_than(newer.last_modified, days, now)
                        {
                            to_cold.push(DeleteTarget {
                                key: v.key.clone(),
                                version_id: Some(v.version_id.clone()),
                            });
                        }
                    }
                }
                // A delete marker that is the only remaining version is expired.
                let (latest, _) = &group[0];
                if rule.expired_object_delete_marker
                    && latest.meta.is_none()
                    && group.len() - removed == 1
                {
                    markers.push(DeleteTarget {
                        key: latest.key.clone(),
                        version_id: Some(latest.version_id.clone()),
                    });
                }
                group.clear();
            };
            let (mut km, mut vm) = (None, None);
            loop {
                let page = self
                    .list_versions(
                        bucket,
                        VersionListQuery {
                            prefix: rule.prefix.clone(),
                            delimiter: None,
                            key_marker: km.clone(),
                            version_marker: vm.clone(),
                            max_keys: BATCH,
                        },
                    )
                    .await?;
                for v in page.versions {
                    if group.first().is_some_and(|(g, _)| g.key != v.key) {
                        flush(&mut group, &mut noncurrent, &mut markers, &mut to_cold);
                    }
                    let eligible = rule.tags.is_empty()
                        || v.meta
                            .as_ref()
                            .is_some_and(|m| tags_match(rule, &m.attrs.tags));
                    group.push((v, eligible));
                }
                if !page.is_truncated {
                    break;
                }
                km = page.next_key_marker;
                vm = page.next_version_marker;
            }
            flush(&mut group, &mut noncurrent, &mut markers, &mut to_cold);
            for chunk in noncurrent.chunks(BATCH) {
                self.delete_batch(bucket, &mut chunk.to_vec(), report, |r| {
                    &mut r.noncurrent_removed
                })
                .await?;
            }
            for chunk in markers.chunks(BATCH) {
                self.delete_batch(bucket, &mut chunk.to_vec(), report, |r| {
                    &mut r.markers_removed
                })
                .await?;
            }
            if let Some((_, class)) = noncurrent_tier {
                for chunk in to_cold.chunks(BATCH) {
                    report.noncurrent_transitioned += self
                        .transition_objects(bucket, chunk.to_vec(), class)
                        .await?;
                }
            }
        }

        // Abandoned multipart uploads.
        if let Some(days) = rule.abort_incomplete_days {
            for u in self.list_uploads(bucket, &rule.prefix).await? {
                if older_than(u.initiated, days, now)
                    && self
                        .abort_upload(bucket, &u.key, &u.upload_id)
                        .await
                        .is_ok()
                {
                    report.uploads_aborted += 1;
                }
            }
        }
        Ok(())
    }

    /// Run lifecycle passes every `every` while `gate()` is true (in a
    /// cluster: while this node is the leader).
    pub fn spawn_lifecycle(
        &self,
        every: StdDuration,
        gate: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                tick.tick().await;
                if !gate() {
                    continue;
                }
                match this.run_lifecycle(Utc::now()).await {
                    Ok(r) if r != LifecycleReport::default() => {
                        tracing::info!(?r, "lifecycle pass")
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "lifecycle pass failed"),
                }
            }
        })
    }
}
