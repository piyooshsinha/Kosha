//! Object versioning and Object Lock, as state-machine operations.
//!
//! Storage model:
//!
//! * `versions` holds every version of every key in a *versioned* bucket,
//!   keyed `bucket \0 key \0 (u64::MAX - seq)`, so a key's versions sort
//!   newest first and keys sort in S3 listing order. In a versioned bucket
//!   the version records own the chunk references.
//! * `objects` stays a mirror of each key's latest *non-delete-marker*
//!   version, so listings and plain GETs cost the same as before.
//! * Unversioned buckets never get version records. Enabling versioning
//!   migrates the bucket's objects into `null` versions in one command.
//!
//! Version ids are the 16-hex-digit sequence number of the version (a global
//! counter in the state machine), or `"null"`.

use chrono::{DateTime, Utc};
use redb::ReadableTable;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::keys;
use crate::meta::{MetaStore, OBJECTS, Rejection, Step, Tx, VERSIONS, dec, enc, reject};
use crate::types::*;
use redb::ReadableDatabase;

const VERSION_SEQ_KEY: &str = "version_seq";
pub const NULL_VERSION: &str = "null";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StoredVersion {
    pub version_id: String,
    pub last_modified: DateTime<Utc>,
    /// `None` for a delete marker.
    pub meta: Option<ObjectMeta>,
}

pub(crate) fn version_prefix(bucket: &str, key: &str) -> Vec<u8> {
    let mut k = keys::object_key(bucket, key);
    k.push(0);
    k
}

pub(crate) fn version_key(bucket: &str, key: &str, seq: u64) -> Vec<u8> {
    let mut k = version_prefix(bucket, key);
    k.extend_from_slice(&(u64::MAX - seq).to_be_bytes());
    k
}

/// Split a `versions` row key into (object key, seq).
pub(crate) fn parse_version_key(bucket: &str, raw: &[u8]) -> Option<(String, u64)> {
    let base = bucket.len() + 1;
    if raw.len() < base + 9 {
        return None;
    }
    let (key, tail) = raw[base..].split_at(raw.len() - base - 9);
    let inv = u64::from_be_bytes(tail[1..].try_into().ok()?);
    Some((String::from_utf8(key.to_vec()).ok()?, u64::MAX - inv))
}

pub(crate) fn id_of_seq(seq: u64) -> String {
    format!("{seq:016x}")
}

pub(crate) fn seq_of_id(id: &str) -> Option<u64> {
    (id.len() == 16)
        .then(|| u64::from_str_radix(id, 16).ok())
        .flatten()
}

/// Refuse deleting a version whose lock forbids it at `now`.
pub(crate) fn check_deletable(
    lock: &ObjectLockState,
    now: DateTime<Utc>,
    bypass: bool,
) -> Step<()> {
    if lock.legal_hold {
        return reject(Rejection::AccessDenied(
            "the object version is under a legal hold".into(),
        ));
    }
    if let Some(r) = &lock.retention
        && r.until > now
    {
        match r.mode {
            RetentionMode::Compliance => {
                return reject(Rejection::AccessDenied(format!(
                    "the object version is under COMPLIANCE retention until {}",
                    r.until.to_rfc3339()
                )));
            }
            RetentionMode::Governance if !bypass => {
                return reject(Rejection::AccessDenied(format!(
                    "the object version is under GOVERNANCE retention until {} (use x-amz-bypass-governance-retention)",
                    r.until.to_rfc3339()
                )));
            }
            RetentionMode::Governance => {}
        }
    }
    Ok(())
}

/// A retention change is allowed if it does not weaken an active lock,
/// except that GOVERNANCE retention may be weakened with bypass.
fn check_retention_change(
    old: &Option<Retention>,
    new: &Option<Retention>,
    now: DateTime<Utc>,
    bypass: bool,
) -> Step<()> {
    let Some(old) = old.as_ref().filter(|r| r.until > now) else {
        return Ok(());
    };
    let weakens = match new {
        None => true,
        Some(n) => {
            n.until < old.until
                || (old.mode == RetentionMode::Compliance && n.mode != RetentionMode::Compliance)
        }
    };
    if !weakens {
        return Ok(());
    }
    match old.mode {
        RetentionMode::Compliance => reject(Rejection::AccessDenied(
            "COMPLIANCE retention cannot be shortened or removed".into(),
        )),
        RetentionMode::Governance if bypass => Ok(()),
        RetentionMode::Governance => reject(Rejection::AccessDenied(
            "GOVERNANCE retention can only be weakened with x-amz-bypass-governance-retention"
                .into(),
        )),
    }
}

impl Tx<'_> {
    pub(crate) fn bucket_info(&self, bucket: &str) -> Step<BucketInfo> {
        match self.txn.open_table(crate::meta::BUCKETS)?.get(bucket)? {
            Some(v) => Ok(dec(v.value())?),
            None => reject(Rejection::NoSuchBucket),
        }
    }

    fn next_version_seq(&mut self) -> Result<u64> {
        let mut meta = self.txn.open_table(crate::meta::META)?;
        let seq: u64 = match meta.get(VERSION_SEQ_KEY)? {
            Some(v) => dec(v.value())?,
            None => 1,
        };
        meta.insert(VERSION_SEQ_KEY, enc(&(seq + 1))?.as_slice())?;
        Ok(seq)
    }

    /// Every version of `key`, newest first.
    pub(crate) fn versions_of(&self, bucket: &str, key: &str) -> Result<Vec<(u64, StoredVersion)>> {
        let t = self.txn.open_table(VERSIONS)?;
        let start = version_prefix(bucket, key);
        let end = keys::prefix_successor(&start).expect("prefix ends with 0");
        let mut out = Vec::new();
        for item in t.range(start.as_slice()..end.as_slice())? {
            let (k, v) = item?;
            let (_, seq) = parse_version_key(bucket, k.value())
                .ok_or_else(|| Error::Internal("malformed version key".into()))?;
            out.push((seq, dec(v.value())?));
        }
        Ok(out)
    }

    pub(crate) fn find_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<(u64, StoredVersion)>> {
        if version_id == NULL_VERSION {
            return Ok(self
                .versions_of(bucket, key)?
                .into_iter()
                .find(|(_, v)| v.version_id == NULL_VERSION));
        }
        let Some(seq) = seq_of_id(version_id) else {
            return Ok(None);
        };
        let t = self.txn.open_table(VERSIONS)?;
        let v = t.get(version_key(bucket, key, seq).as_slice())?;
        v.map(|v| dec::<StoredVersion>(v.value()).map(|sv| (seq, sv)))
            .transpose()
    }

    fn insert_version(
        &mut self,
        bucket: &str,
        key: &str,
        seq: u64,
        v: &StoredVersion,
    ) -> Result<()> {
        self.txn
            .open_table(VERSIONS)?
            .insert(version_key(bucket, key, seq).as_slice(), enc(v)?.as_slice())?;
        Ok(())
    }

    /// Point the `objects` mirror at `latest` (or remove it). No refcount
    /// changes: in versioned buckets the version records own the chunks.
    fn set_latest(&mut self, bucket: &str, key: &str, latest: Option<&ObjectMeta>) -> Result<()> {
        let mut t = self.txn.open_table(OBJECTS)?;
        let k = keys::object_key(bucket, key);
        let had = match latest {
            Some(m) => t.insert(k.as_slice(), enc(m)?.as_slice())?.is_some(),
            None => {
                self.txn
                    .open_table(crate::meta::ACCESS)?
                    .remove(k.as_slice())?;
                t.remove(k.as_slice())?.is_some()
            }
        };
        match (had, latest.is_some()) {
            (false, true) => self.stats.objects += 1,
            (true, false) => self.stats.objects -= 1,
            _ => {}
        }
        Ok(())
    }

    fn refresh_latest(&mut self, bucket: &str, key: &str) -> Result<()> {
        let newest = self.versions_of(bucket, key)?.into_iter().next();
        let latest = newest.and_then(|(_, v)| v.meta);
        self.set_latest(bucket, key, latest.as_ref())
    }

    /// Remove a version record and release what it held.
    fn drop_version(&mut self, bucket: &str, key: &str, seq: u64, v: &StoredVersion) -> Result<()> {
        self.txn
            .open_table(VERSIONS)?
            .remove(version_key(bucket, key, seq).as_slice())?;
        if let Some(meta) = &v.meta {
            self.stats.logical_bytes -= meta.size;
            self.drop_object_refs(meta)?;
        }
        Ok(())
    }

    /// Apply the bucket's default retention to a new version that has none.
    fn apply_default_lock(info: &BucketInfo, meta: &mut ObjectMeta) {
        if meta.lock.retention.is_some() {
            return;
        }
        if let Some(d) = info
            .config
            .object_lock
            .as_ref()
            .and_then(|c| c.default_retention.as_ref())
        {
            meta.lock.retention = Some(Retention {
                mode: d.mode,
                until: meta.last_modified + chrono::Duration::days(d.days as i64),
            });
        }
    }

    /// Store a new object (its chunk references already added) according to
    /// the bucket's versioning state. Returns the stored metadata.
    pub(crate) fn write_object(
        &mut self,
        info: &BucketInfo,
        key: &str,
        mut meta: ObjectMeta,
    ) -> Step<ObjectMeta> {
        let bucket = info.name.as_str();
        if info.config.object_lock.is_none() && meta.lock != ObjectLockState::default() {
            return reject(Rejection::InvalidRequest(
                "Object Lock headers require a bucket with Object Lock enabled".into(),
            ));
        }
        match info.config.versioning {
            Versioning::Unversioned => {
                meta.version_id = None;
                self.put_object(bucket, key, &meta)?;
            }
            mode => {
                if mode == Versioning::Suspended
                    && let Some((seq, old)) = self.find_version(bucket, key, NULL_VERSION)?
                {
                    self.drop_version(bucket, key, seq, &old)?;
                }
                Self::apply_default_lock(info, &mut meta);
                let seq = self.next_version_seq()?;
                let id = if mode == Versioning::Enabled {
                    id_of_seq(seq)
                } else {
                    NULL_VERSION.to_string()
                };
                meta.version_id = Some(id.clone());
                self.insert_version(
                    bucket,
                    key,
                    seq,
                    &StoredVersion {
                        version_id: id,
                        last_modified: meta.last_modified,
                        meta: Some(meta.clone()),
                    },
                )?;
                self.stats.logical_bytes += meta.size;
                self.set_latest(bucket, key, Some(&meta))?;
            }
        }
        Ok(meta)
    }

    /// DELETE without a version id: remove the object (unversioned) or add a
    /// delete marker (versioned).
    pub(crate) fn delete_current(
        &mut self,
        info: &BucketInfo,
        key: &str,
        now: DateTime<Utc>,
    ) -> Result<DeleteOutcome> {
        let bucket = info.name.as_str();
        match info.config.versioning {
            Versioning::Unversioned => {
                let existed = self.delete_object(bucket, key)?;
                Ok(DeleteOutcome {
                    key: key.into(),
                    existed,
                    ..Default::default()
                })
            }
            mode => {
                if mode == Versioning::Suspended
                    && let Some((seq, old)) = self.find_version(bucket, key, NULL_VERSION)?
                {
                    // A locked null version cannot be replaced by a marker.
                    if let Some(m) = &old.meta
                        && check_deletable(&m.lock, now, false).is_err()
                    {
                        return Ok(DeleteOutcome {
                            key: key.into(),
                            error: Some(Rejection::AccessDenied(
                                "the null version is locked".into(),
                            )),
                            ..Default::default()
                        });
                    }
                    self.drop_version(bucket, key, seq, &old)?;
                }
                let existed = self.get_object(bucket, key)?.is_some();
                let seq = self.next_version_seq()?;
                let id = if mode == Versioning::Enabled {
                    id_of_seq(seq)
                } else {
                    NULL_VERSION.to_string()
                };
                self.insert_version(
                    bucket,
                    key,
                    seq,
                    &StoredVersion {
                        version_id: id.clone(),
                        last_modified: now,
                        meta: None,
                    },
                )?;
                self.set_latest(bucket, key, None)?;
                Ok(DeleteOutcome {
                    key: key.into(),
                    version_id: Some(id),
                    delete_marker: true,
                    existed,
                    error: None,
                })
            }
        }
    }

    /// DELETE ?versionId: permanently remove one version.
    pub(crate) fn delete_version(
        &mut self,
        info: &BucketInfo,
        key: &str,
        version_id: &str,
        bypass: bool,
        now: DateTime<Utc>,
    ) -> Step<DeleteOutcome> {
        let bucket = info.name.as_str();
        if info.config.versioning == Versioning::Unversioned {
            if version_id != NULL_VERSION {
                return reject(Rejection::NoSuchVersion);
            }
            let existed = self.delete_object(bucket, key)?;
            return Ok(DeleteOutcome {
                key: key.into(),
                version_id: Some(NULL_VERSION.into()),
                existed,
                ..Default::default()
            });
        }
        let Some((seq, v)) = self.find_version(bucket, key, version_id)? else {
            return reject(Rejection::NoSuchVersion);
        };
        if let Some(m) = &v.meta {
            check_deletable(&m.lock, now, bypass)?;
        }
        self.drop_version(bucket, key, seq, &v)?;
        self.refresh_latest(bucket, key)?;
        Ok(DeleteOutcome {
            key: key.into(),
            version_id: Some(v.version_id),
            delete_marker: v.meta.is_none(),
            existed: true,
            error: None,
        })
    }

    /// Turn every object of a bucket into a `null` version (first enable).
    pub(crate) fn migrate_to_versioned(&mut self, bucket: &str) -> Result<()> {
        let (s, e) = keys::bucket_range(bucket);
        let mut rows = Vec::new();
        for item in self
            .txn
            .open_table(OBJECTS)?
            .range(s.as_slice()..e.as_slice())?
        {
            let (k, v) = item?;
            rows.push((k.value().to_vec(), dec::<ObjectMeta>(v.value())?));
        }
        let base = bucket.len() + 1;
        for (raw, mut meta) in rows {
            let key = String::from_utf8(raw[base..].to_vec())
                .map_err(|_| Error::Internal("non-utf8 key".into()))?;
            meta.version_id = Some(NULL_VERSION.into());
            let seq = self.next_version_seq()?;
            self.insert_version(
                bucket,
                &key,
                seq,
                &StoredVersion {
                    version_id: NULL_VERSION.into(),
                    last_modified: meta.last_modified,
                    meta: Some(meta.clone()),
                },
            )?;
            self.txn
                .open_table(OBJECTS)?
                .insert(raw.as_slice(), enc(&meta)?.as_slice())?;
        }
        Ok(())
    }

    /// Update one version's metadata in place (tags, lock), keeping the
    /// `objects` mirror in sync. `None` version means the latest.
    pub(crate) fn update_version(
        &mut self,
        info: &BucketInfo,
        key: &str,
        version_id: Option<&str>,
        f: impl FnOnce(&mut ObjectMeta) -> Step<()>,
    ) -> Step<ObjectMeta> {
        let bucket = info.name.as_str();
        if info.config.versioning == Versioning::Unversioned {
            if version_id.is_some_and(|v| v != NULL_VERSION) {
                return reject(Rejection::NoSuchVersion);
            }
            let Some(mut meta) = self.get_object(bucket, key)? else {
                return reject(Rejection::NoSuchKey);
            };
            f(&mut meta)?;
            self.txn.open_table(OBJECTS)?.insert(
                keys::object_key(bucket, key).as_slice(),
                enc(&meta)?.as_slice(),
            )?;
            return Ok(meta);
        }
        let found = match version_id {
            Some(id) => self.find_version(bucket, key, id)?,
            None => self.versions_of(bucket, key)?.into_iter().next(),
        };
        let Some((seq, mut v)) = found else {
            return reject(if version_id.is_some() {
                Rejection::NoSuchVersion
            } else {
                Rejection::NoSuchKey
            });
        };
        let Some(meta) = v.meta.as_mut() else {
            return reject(Rejection::MethodNotAllowed(
                "the version is a delete marker".into(),
            ));
        };
        f(meta)?;
        let meta = meta.clone();
        self.insert_version(bucket, key, seq, &v)?;
        let is_latest = self.versions_of(bucket, key)?.first().map(|(s, _)| *s) == Some(seq);
        if is_latest {
            self.set_latest(bucket, key, Some(&meta))?;
        }
        Ok(meta)
    }

    /// PutObjectRetention / PutObjectLegalHold.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn set_lock(
        &mut self,
        info: &BucketInfo,
        key: &str,
        version_id: Option<&str>,
        retention: Option<Option<Retention>>,
        legal_hold: Option<bool>,
        bypass: bool,
        now: DateTime<Utc>,
    ) -> Step<ObjectMeta> {
        if info.config.object_lock.is_none() {
            return reject(Rejection::InvalidRequest(
                "Bucket is missing Object Lock Configuration".into(),
            ));
        }
        self.update_version(info, key, version_id, |meta| {
            if let Some(new) = retention {
                check_retention_change(&meta.lock.retention, &new, now, bypass)?;
                meta.lock.retention = new;
            }
            if let Some(hold) = legal_hold {
                meta.lock.legal_hold = hold;
            }
            Ok(())
        })
    }
}

// ---------------------------------------------------------------- reads

impl MetaStore {
    fn bucket_versioning(&self, r: &redb::ReadTransaction, bucket: &str) -> Result<Versioning> {
        match r.open_table(crate::meta::BUCKETS)?.get(bucket)? {
            Some(v) => Ok(dec::<BucketInfo>(v.value())?.config.versioning),
            None => Err(Error::NoSuchBucket),
        }
    }

    /// One specific version. A delete marker comes back with `meta: None`.
    pub fn head_version(&self, bucket: &str, key: &str, version_id: &str) -> Result<VersionInfo> {
        let r = self.db.begin_read()?;
        if self.bucket_versioning(&r, bucket)? == Versioning::Unversioned {
            if version_id != NULL_VERSION {
                return Err(Error::NoSuchVersion);
            }
            let t = r.open_table(OBJECTS)?;
            let meta: ObjectMeta = match t.get(keys::object_key(bucket, key).as_slice())? {
                Some(v) => dec(v.value())?,
                None => return Err(Error::NoSuchVersion),
            };
            return Ok(VersionInfo {
                key: key.into(),
                version_id: NULL_VERSION.into(),
                is_latest: true,
                last_modified: meta.last_modified,
                meta: Some(meta),
            });
        }
        let t = r.open_table(VERSIONS)?;
        let start = version_prefix(bucket, key);
        let end = keys::prefix_successor(&start).expect("prefix ends with 0");
        let mut first = true;
        for item in t.range(start.as_slice()..end.as_slice())? {
            let (_, v) = item?;
            let sv: StoredVersion = dec(v.value())?;
            if sv.version_id == version_id {
                return Ok(VersionInfo {
                    key: key.into(),
                    version_id: sv.version_id,
                    is_latest: first,
                    last_modified: sv.last_modified,
                    meta: sv.meta,
                });
            }
            first = false;
        }
        Err(Error::NoSuchVersion)
    }

    /// The version id of `key`'s latest version if it is a delete marker.
    pub fn latest_delete_marker(&self, bucket: &str, key: &str) -> Result<Option<String>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(VERSIONS)?;
        let start = version_prefix(bucket, key);
        let end = keys::prefix_successor(&start).expect("prefix ends with 0");
        let first = t.range(start.as_slice()..end.as_slice())?.next();
        match first {
            Some(item) => {
                let sv: StoredVersion = dec(item?.1.value())?;
                Ok(sv.meta.is_none().then_some(sv.version_id))
            }
            None => Ok(None),
        }
    }

    /// ListObjectVersions.
    pub fn list_versions(&self, bucket: &str, q: &VersionListQuery) -> Result<VersionPage> {
        let r = self.db.begin_read()?;
        if self.bucket_versioning(&r, bucket)? == Versioning::Unversioned {
            // Every object is its own null version.
            let page = self.list_objects(
                bucket,
                &ListQuery {
                    prefix: q.prefix.clone(),
                    delimiter: q.delimiter.clone(),
                    start_after: q.key_marker.clone(),
                    max_keys: q.max_keys,
                },
            )?;
            let t = r.open_table(OBJECTS)?;
            let mut versions = Vec::new();
            for o in &page.objects {
                if let Some(v) = t.get(keys::object_key(bucket, &o.key).as_slice())? {
                    let meta: ObjectMeta = dec(v.value())?;
                    versions.push(VersionInfo {
                        key: o.key.clone(),
                        version_id: NULL_VERSION.into(),
                        is_latest: true,
                        last_modified: meta.last_modified,
                        meta: Some(meta),
                    });
                }
            }
            return Ok(VersionPage {
                next_key_marker: page.next_marker,
                next_version_marker: page.is_truncated.then(|| NULL_VERSION.into()),
                versions,
                common_prefixes: page.common_prefixes,
                is_truncated: page.is_truncated,
            });
        }

        let mut page = VersionPage::default();
        if q.max_keys == 0 {
            return Ok(page);
        }
        let t = r.open_table(VERSIONS)?;
        let delim = q.delimiter.as_deref().filter(|d| !d.is_empty());
        let rollup = |key: &str| -> Option<String> {
            let d = delim?;
            let rest = key.strip_prefix(q.prefix.as_str())?;
            rest.find(d)
                .map(|i| key[..q.prefix.len() + i + d.len()].to_string())
        };
        let prefix_key = keys::object_key(bucket, &q.prefix);
        let end = keys::prefix_successor(&prefix_key).expect("bucket name is not all 0xff");

        use std::ops::Bound;
        let mut lower: Bound<Vec<u8>> = Bound::Included(prefix_key.clone());
        // Resume after (key_marker, version_marker).
        if let Some(km) = q.key_marker.as_deref().filter(|k| !k.is_empty()) {
            let bound = match q.version_marker.as_deref().filter(|v| !v.is_empty()) {
                None => match rollup(km) {
                    Some(cp) => {
                        keys::prefix_successor(&keys::object_key(bucket, &cp)).map(Bound::Included)
                    }
                    None => {
                        keys::prefix_successor(&version_prefix(bucket, km)).map(Bound::Included)
                    }
                },
                Some(vm) => {
                    let seq = if vm == NULL_VERSION {
                        let mut found = None;
                        let s0 = version_prefix(bucket, km);
                        let e0 = keys::prefix_successor(&s0).expect("prefix ends with 0");
                        for item in t.range(s0.as_slice()..e0.as_slice())? {
                            let (k, v) = item?;
                            if dec::<StoredVersion>(v.value())?.version_id == NULL_VERSION {
                                found = parse_version_key(bucket, k.value()).map(|(_, s)| s);
                                break;
                            }
                        }
                        found
                    } else {
                        seq_of_id(vm)
                    };
                    seq.map(|s| Bound::Excluded(version_key(bucket, km, s)))
                }
            };
            match bound {
                Some(b) => {
                    let k = match &b {
                        Bound::Included(k) | Bound::Excluded(k) => k.clone(),
                        Bound::Unbounded => unreachable!(),
                    };
                    if k >= prefix_key {
                        lower = b;
                    }
                }
                None => return Ok(page),
            }
        }

        let mut last_key: Option<String> = None;
        'scan: loop {
            let range = t.range::<&[u8]>((
                match &lower {
                    Bound::Included(k) => Bound::Included(k.as_slice()),
                    Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
                    Bound::Unbounded => Bound::Unbounded,
                },
                Bound::Excluded(end.as_slice()),
            ))?;
            for item in range {
                let (k, v) = item?;
                let (key, _seq) = parse_version_key(bucket, k.value())
                    .ok_or_else(|| Error::Internal("malformed version key".into()))?;
                if page.versions.len() + page.common_prefixes.len() >= q.max_keys {
                    page.is_truncated = true;
                    break 'scan;
                }
                if let Some(cp) = rollup(&key) {
                    let skip_to = keys::prefix_successor(&keys::object_key(bucket, &cp));
                    page.next_key_marker = Some(cp.clone());
                    page.next_version_marker = None;
                    page.common_prefixes.push(cp);
                    last_key = None;
                    match skip_to {
                        Some(s) if s < end => {
                            lower = Bound::Included(s);
                            continue 'scan;
                        }
                        _ => break 'scan,
                    }
                }
                // The first row of a key is its latest version, unless we
                // resumed in the middle of that key's versions.
                let is_latest = if last_key.as_deref() == Some(key.as_str()) {
                    false
                } else {
                    let s0 = version_prefix(bucket, &key);
                    t.range(s0.as_slice()..)?
                        .next()
                        .transpose()?
                        .map(|(fk, _)| fk.value().to_vec())
                        == Some(k.value().to_vec())
                };
                let sv: StoredVersion = dec(v.value())?;
                page.next_key_marker = Some(key.clone());
                page.next_version_marker = Some(sv.version_id.clone());
                page.versions.push(VersionInfo {
                    key: key.clone(),
                    version_id: sv.version_id,
                    is_latest,
                    last_modified: sv.last_modified,
                    meta: sv.meta,
                });
                last_key = Some(key);
            }
            break;
        }
        if !page.is_truncated {
            page.next_key_marker = None;
            page.next_version_marker = None;
        }
        Ok(page)
    }
}
