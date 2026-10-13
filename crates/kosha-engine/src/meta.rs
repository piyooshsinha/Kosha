//! The metadata state machine.
//!
//! Every mutation is a serializable [`Command`] applied deterministically by
//! [`MetaStore::apply`]. On a single node commands are applied directly; in a
//! cluster they are entries in the Raft log and every replica applies the same
//! sequence, so every replica ends up with identical metadata.
//!
//! Determinism rules: commands carry everything that is not a pure function
//! of prior state (timestamps, upload ids, chunk sizes), and each command
//! checks all of its preconditions before mutating anything. A command that
//! fails a precondition therefore leaves no trace, and the batch it belongs to
//! still commits.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::path::Path;

use chrono::{DateTime, Utc};
use md5::{Digest, Md5};
use redb::{
    Database, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition, TableHandle,
    WriteTransaction,
};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::keys;
use crate::types::*;

pub(crate) const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");
pub(crate) const OBJECTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("objects");
const CHUNKS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("chunks");
const UPLOADS: TableDefinition<&str, &[u8]> = TableDefinition::new("uploads");
const UPLOAD_INDEX: TableDefinition<&[u8], &str> = TableDefinition::new("upload_index");
const PARTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("parts");
pub(crate) const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
/// Request id -> remembered result (exactly-once application of retries).
const REQUESTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("requests");
/// Sequence number (BE u64) -> request id, for evicting the oldest ids.
const REQUEST_SEQ: TableDefinition<&[u8], &[u8]> = TableDefinition::new("request_seq");

/// Every version of every key in versioned buckets (see `versions.rs`).
pub(crate) const VERSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("versions");

/// Byte-keyed tables included in snapshots generically. New tables only need
/// to be listed here.
const BYTE_TABLES: &[TableDefinition<&[u8], &[u8]>] = &[REQUESTS, REQUEST_SEQ, VERSIONS];

/// How many request ids are remembered for deduplication.
const REMEMBERED_REQUESTS: u64 = 100_000;
const REQUEST_SEQ_KEY: &str = "request_seq";

const STATS_KEY: &str = "stats";
/// Opaque state owned by the consensus layer (applied log id, membership),
/// committed atomically with the commands it describes.
const SM_STATE_KEY: &str = "sm_state";

/// A metadata mutation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    CreateBucket {
        info: BucketInfo,
    },
    DeleteBucket {
        name: String,
    },
    PutObject {
        bucket: String,
        key: String,
        meta: ObjectMeta,
        /// On-disk size of each new chunk, for accounting.
        stored: Vec<(ChunkId, u64)>,
        cond: WriteCondition,
    },
    DeleteObjects {
        bucket: String,
        targets: Vec<DeleteTarget>,
        bypass_governance: bool,
        now: DateTime<Utc>,
    },
    CopyObject {
        src_bucket: String,
        src_key: String,
        #[serde(default)]
        src_version: Option<String>,
        #[serde(default)]
        lock: ObjectLockState,
        dst_bucket: String,
        dst_key: String,
        attrs: Option<ObjectAttrs>,
        now: DateTime<Utc>,
        cond: WriteCondition,
    },
    CreateUpload {
        info: UploadInfo,
    },
    PutPart {
        bucket: String,
        key: String,
        upload_id: String,
        part: PartInfo,
        stored: Vec<(ChunkId, u64)>,
    },
    CompleteUpload {
        bucket: String,
        key: String,
        upload_id: String,
        parts: Vec<(u32, String)>,
        now: DateTime<Utc>,
        cond: WriteCondition,
        min_part_size: u64,
    },
    AbortUpload {
        bucket: String,
        key: String,
        upload_id: String,
    },
    SetBucketConfig {
        bucket: String,
        update: BucketConfigUpdate,
    },
    /// Replace an object's tags (empty map deletes them).
    SetObjectTags {
        bucket: String,
        key: String,
        #[serde(default)]
        version_id: Option<String>,
        tags: BTreeMap<String, String>,
    },
    /// PutObjectRetention / PutObjectLegalHold.
    SetObjectLock {
        bucket: String,
        key: String,
        version_id: Option<String>,
        /// `Some(None)` removes retention.
        retention: Option<Option<Retention>>,
        legal_hold: Option<bool>,
        bypass_governance: bool,
        now: DateTime<Utc>,
    },
}

/// Identifies one logical write across retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(pub u128);

impl RequestId {
    pub fn random() -> Self {
        RequestId(uuid::Uuid::new_v4().as_u128())
    }
}

/// A command plus an optional request id. Commands with an id are applied at
/// most once: re-applying the same id returns the remembered result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub id: Option<RequestId>,
    pub cmd: Command,
}

impl From<Command> for Request {
    fn from(cmd: Command) -> Self {
        Request { id: None, cmd }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CommandOutput {
    Done,
    Object(ObjectMeta),
    Deleted(Vec<DeleteOutcome>),
}

/// A command rejected by a precondition. Deterministic, so it is part of
/// the replicated result rather than a failure of the state machine.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Rejection {
    NoSuchBucket,
    BucketAlreadyExists,
    BucketNotEmpty,
    NoSuchKey,
    NoSuchUpload,
    InvalidPart(String),
    InvalidPartOrder,
    EntityTooSmall,
    PreconditionFailed,
    NoSuchVersion,
    InvalidRequest(String),
    InvalidBucketState(String),
    AccessDenied(String),
    MethodNotAllowed(String),
}

impl From<Rejection> for Error {
    fn from(r: Rejection) -> Self {
        match r {
            Rejection::NoSuchBucket => Error::NoSuchBucket,
            Rejection::BucketAlreadyExists => Error::BucketAlreadyExists,
            Rejection::BucketNotEmpty => Error::BucketNotEmpty,
            Rejection::NoSuchKey => Error::NoSuchKey,
            Rejection::NoSuchUpload => Error::NoSuchUpload,
            Rejection::InvalidPart(m) => Error::InvalidPart(m),
            Rejection::InvalidPartOrder => Error::InvalidPartOrder,
            Rejection::EntityTooSmall => Error::EntityTooSmall,
            Rejection::PreconditionFailed => Error::PreconditionFailed,
            Rejection::NoSuchVersion => Error::NoSuchVersion,
            Rejection::InvalidRequest(m) => Error::InvalidRequest(m),
            Rejection::InvalidBucketState(m) => Error::InvalidBucketState(m),
            Rejection::AccessDenied(m) => Error::AccessDenied(m),
            Rejection::MethodNotAllowed(m) => Error::MethodNotAllowed(m),
        }
    }
}

pub type CommandResult = std::result::Result<CommandOutput, Rejection>;

/// Internal: either a precondition rejection or a real storage failure.
pub(crate) enum Fail {
    Reject(Rejection),
    Fatal(Error),
}

impl<E: Into<Error>> From<E> for Fail {
    fn from(e: E) -> Self {
        Fail::Fatal(e.into())
    }
}

pub(crate) type Step<T> = std::result::Result<T, Fail>;

pub(crate) fn reject<T>(r: Rejection) -> Step<T> {
    Err(Fail::Reject(r))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChunkInfo {
    refs: u64,
    len: u32,
    stored: u64,
}

pub(crate) fn enc<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec_named(v)?)
}

pub(crate) fn dec<T: for<'de> Deserialize<'de>>(b: &[u8]) -> Result<T> {
    Ok(rmp_serde::from_slice(b)?)
}

fn check_condition(current: Option<&ObjectMeta>, cond: &WriteCondition) -> Step<()> {
    if cond.if_none_match_any && current.is_some() {
        return reject(Rejection::PreconditionFailed);
    }
    if let Some(want) = &cond.if_match {
        match current {
            None => return reject(Rejection::NoSuchKey),
            Some(m) if want.trim_matches('"') != m.etag && want != "*" => {
                return reject(Rejection::PreconditionFailed);
            }
            _ => {}
        }
    }
    Ok(())
}

/// Mutation context for one metadata transaction.
pub(crate) struct Tx<'a> {
    pub(crate) txn: &'a WriteTransaction,
    pub(crate) stats: Stats,
    pub(crate) freed: Vec<ChunkId>,
}

impl Tx<'_> {
    fn remembered(&self, id: RequestId) -> Result<Option<CommandResult>> {
        let t = self.txn.open_table(REQUESTS)?;
        let v = t.get(id.0.to_be_bytes().as_slice())?;
        v.map(|v| dec(v.value())).transpose()
    }

    /// Remember a result, evicting the oldest id once the window is full.
    /// Object manifests are dropped from remembered results to keep them
    /// small; a deduplicated reply carries the object's attributes only.
    fn remember(&mut self, id: RequestId, result: &CommandResult) -> Result<()> {
        let slim = match result {
            Ok(CommandOutput::Object(m)) => Ok(CommandOutput::Object(ObjectMeta {
                segments: Vec::new(),
                ..m.clone()
            })),
            other => other.clone(),
        };
        let mut meta = self.txn.open_table(META)?;
        let seq: u64 = match meta.get(REQUEST_SEQ_KEY)? {
            Some(v) => dec(v.value())?,
            None => 0,
        };
        meta.insert(REQUEST_SEQ_KEY, enc(&(seq + 1))?.as_slice())?;
        drop(meta);
        let key = id.0.to_be_bytes();
        self.txn
            .open_table(REQUESTS)?
            .insert(key.as_slice(), enc(&slim)?.as_slice())?;
        let mut order = self.txn.open_table(REQUEST_SEQ)?;
        order.insert(seq.to_be_bytes().as_slice(), key.as_slice())?;
        if seq >= REMEMBERED_REQUESTS {
            let old = (seq - REMEMBERED_REQUESTS).to_be_bytes();
            let evicted = order.remove(old.as_slice())?.map(|v| v.value().to_vec());
            drop(order);
            if let Some(old_id) = evicted {
                self.txn.open_table(REQUESTS)?.remove(old_id.as_slice())?;
            }
        }
        Ok(())
    }

    fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        Ok(self.txn.open_table(BUCKETS)?.get(bucket)?.is_some())
    }

    fn require_bucket(&self, bucket: &str) -> Step<()> {
        if self.bucket_exists(bucket)? {
            Ok(())
        } else {
            reject(Rejection::NoSuchBucket)
        }
    }

    pub(crate) fn add_refs(
        &mut self,
        segments: &[Segment],
        stored: &HashMap<ChunkId, u64>,
    ) -> Result<()> {
        let mut chunks = self.txn.open_table(CHUNKS)?;
        for seg in segments {
            let Segment::Chunk { id, len } = seg else {
                continue;
            };
            let existing = chunks
                .get(id.0.as_slice())?
                .map(|v| dec::<ChunkInfo>(v.value()));
            let info = match existing {
                Some(info) => {
                    let mut info = info?;
                    info.refs += 1;
                    info
                }
                None => {
                    let stored = *stored.get(id).ok_or_else(|| {
                        Error::Internal(format!("unreferenced chunk {} has no size", id.to_hex()))
                    })?;
                    self.stats.chunks += 1;
                    self.stats.unique_bytes += *len as u64;
                    self.stats.physical_bytes += stored;
                    ChunkInfo {
                        refs: 1,
                        len: *len,
                        stored,
                    }
                }
            };
            chunks.insert(id.0.as_slice(), enc(&info)?.as_slice())?;
        }
        Ok(())
    }

    pub(crate) fn drop_refs(&mut self, segments: &[Segment]) -> Result<()> {
        let mut chunks = self.txn.open_table(CHUNKS)?;
        for seg in segments {
            let Segment::Chunk { id, .. } = seg else {
                continue;
            };
            let Some(raw) = chunks.get(id.0.as_slice())?.map(|v| v.value().to_vec()) else {
                tracing::warn!(chunk = %id.to_hex(), "dropping reference to unknown chunk");
                continue;
            };
            let mut info: ChunkInfo = dec(&raw)?;
            info.refs -= 1;
            if info.refs == 0 {
                chunks.remove(id.0.as_slice())?;
                self.stats.chunks -= 1;
                self.stats.unique_bytes -= info.len as u64;
                self.stats.physical_bytes -= info.stored;
                self.freed.push(*id);
            } else {
                chunks.insert(id.0.as_slice(), enc(&info)?.as_slice())?;
            }
        }
        Ok(())
    }

    pub(crate) fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let t = self.txn.open_table(OBJECTS)?;
        let v = t.get(keys::object_key(bucket, key).as_slice())?;
        v.map(|v| dec(v.value())).transpose()
    }

    /// Insert or replace an object whose chunk references have already been added.
    pub(crate) fn put_object(&mut self, bucket: &str, key: &str, meta: &ObjectMeta) -> Result<()> {
        let mut t = self.txn.open_table(OBJECTS)?;
        let k = keys::object_key(bucket, key);
        let old = t
            .insert(k.as_slice(), enc(meta)?.as_slice())?
            .map(|v| dec::<ObjectMeta>(v.value()))
            .transpose()?;
        drop(t);
        if let Some(old) = old {
            self.stats.logical_bytes -= old.size;
            self.drop_refs(&old.segments)?;
        } else {
            self.stats.objects += 1;
        }
        self.stats.logical_bytes += meta.size;
        Ok(())
    }

    pub(crate) fn delete_object(&mut self, bucket: &str, key: &str) -> Result<bool> {
        let mut t = self.txn.open_table(OBJECTS)?;
        let old = t
            .remove(keys::object_key(bucket, key).as_slice())?
            .map(|v| dec::<ObjectMeta>(v.value()))
            .transpose()?;
        drop(t);
        let Some(old) = old else { return Ok(false) };
        self.stats.objects -= 1;
        self.stats.logical_bytes -= old.size;
        self.drop_refs(&old.segments)?;
        Ok(true)
    }

    fn get_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Step<UploadInfo> {
        let t = self.txn.open_table(UPLOADS)?;
        let info: UploadInfo = match t.get(upload_id)? {
            Some(v) => dec(v.value())?,
            None => return reject(Rejection::NoSuchUpload),
        };
        if info.bucket != bucket || info.key != key {
            return reject(Rejection::NoSuchUpload);
        }
        Ok(info)
    }

    fn upload_parts(&self, upload_id: &str) -> Result<Vec<PartInfo>> {
        let t = self.txn.open_table(PARTS)?;
        let (s, e) = keys::upload_parts_range(upload_id);
        let mut out = Vec::new();
        for item in t.range(s.as_slice()..e.as_slice())? {
            let (_, v) = item?;
            out.push(dec(v.value())?);
        }
        Ok(out)
    }

    /// Remove an upload and release every part it holds.
    fn remove_upload(&mut self, info: &UploadInfo) -> Result<()> {
        let parts = self.upload_parts(&info.upload_id)?;
        {
            let mut t = self.txn.open_table(PARTS)?;
            for p in &parts {
                t.remove(keys::part_key(&info.upload_id, p.part_number).as_slice())?;
            }
        }
        for p in &parts {
            self.drop_refs(&p.segments)?;
        }
        self.txn
            .open_table(UPLOADS)?
            .remove(info.upload_id.as_str())?;
        self.txn
            .open_table(UPLOAD_INDEX)?
            .remove(keys::upload_index_key(&info.bucket, &info.key, &info.upload_id).as_slice())?;
        Ok(())
    }

    fn apply(&mut self, cmd: Command) -> Step<CommandOutput> {
        match cmd {
            Command::CreateBucket { info } => {
                if self.bucket_exists(&info.name)? {
                    return reject(Rejection::BucketAlreadyExists);
                }
                self.txn
                    .open_table(BUCKETS)?
                    .insert(info.name.as_str(), enc(&info)?.as_slice())?;
                self.stats.buckets += 1;
                Ok(CommandOutput::Done)
            }
            Command::DeleteBucket { name } => {
                self.require_bucket(&name)?;
                let (s, e) = keys::bucket_range(&name);
                let has_objects = self
                    .txn
                    .open_table(OBJECTS)?
                    .range(s.as_slice()..e.as_slice())?
                    .next()
                    .is_some();
                let has_versions = self
                    .txn
                    .open_table(VERSIONS)?
                    .range(s.as_slice()..e.as_slice())?
                    .next()
                    .is_some();
                if has_objects || has_versions {
                    return reject(Rejection::BucketNotEmpty);
                }
                // Abandon in-progress multipart uploads along with the bucket.
                let mut ids = Vec::new();
                for item in self
                    .txn
                    .open_table(UPLOAD_INDEX)?
                    .range(s.as_slice()..e.as_slice())?
                {
                    ids.push(item?.1.value().to_string());
                }
                for id in ids {
                    let raw = self
                        .txn
                        .open_table(UPLOADS)?
                        .get(id.as_str())?
                        .map(|v| v.value().to_vec())
                        .ok_or_else(|| Error::Internal("dangling upload index".into()))?;
                    self.remove_upload(&dec(&raw)?)?;
                }
                self.txn.open_table(BUCKETS)?.remove(name.as_str())?;
                self.stats.buckets -= 1;
                Ok(CommandOutput::Done)
            }
            Command::PutObject {
                bucket,
                key,
                meta,
                stored,
                cond,
            } => {
                let info = self.bucket_info(&bucket)?;
                check_condition(self.get_object(&bucket, &key)?.as_ref(), &cond)?;
                if info.config.object_lock.is_none() && meta.lock != ObjectLockState::default() {
                    return reject(Rejection::InvalidRequest(
                        "Object Lock headers require a bucket with Object Lock enabled".into(),
                    ));
                }
                self.add_refs(&meta.segments, &stored.into_iter().collect())?;
                let meta = self.write_object(&info, &key, meta)?;
                Ok(CommandOutput::Object(meta))
            }
            Command::DeleteObjects {
                bucket,
                targets,
                bypass_governance,
                now,
            } => {
                let info = self.bucket_info(&bucket)?;
                let mut outcomes = Vec::with_capacity(targets.len());
                for t in targets {
                    let outcome = match &t.version_id {
                        None => self.delete_current(&info, &t.key, now)?,
                        Some(v) => {
                            match self.delete_version(&info, &t.key, v, bypass_governance, now) {
                                Ok(o) => o,
                                Err(Fail::Reject(r)) => DeleteOutcome {
                                    key: t.key.clone(),
                                    version_id: Some(v.clone()),
                                    error: Some(r),
                                    ..Default::default()
                                },
                                Err(Fail::Fatal(e)) => return Err(Fail::Fatal(e)),
                            }
                        }
                    };
                    outcomes.push(outcome);
                }
                Ok(CommandOutput::Deleted(outcomes))
            }
            Command::CopyObject {
                src_bucket,
                src_key,
                src_version,
                lock,
                dst_bucket,
                dst_key,
                attrs,
                now,
                cond,
            } => {
                let src_info = self.bucket_info(&src_bucket)?;
                let dst_info = self.bucket_info(&dst_bucket)?;
                let src = match &src_version {
                    None => self.get_object(&src_bucket, &src_key)?,
                    Some(v) if src_info.config.versioning == Versioning::Unversioned => {
                        if v != crate::versions::NULL_VERSION {
                            return reject(Rejection::NoSuchVersion);
                        }
                        self.get_object(&src_bucket, &src_key)?
                    }
                    Some(v) => match self.find_version(&src_bucket, &src_key, v)? {
                        Some((_, sv)) => match sv.meta {
                            Some(m) => Some(m),
                            None => {
                                return reject(Rejection::InvalidRequest(
                                    "the source version is a delete marker".into(),
                                ));
                            }
                        },
                        None => return reject(Rejection::NoSuchVersion),
                    },
                };
                let Some(src) = src else {
                    return reject(Rejection::NoSuchKey);
                };
                check_condition(self.get_object(&dst_bucket, &dst_key)?.as_ref(), &cond)?;
                if dst_info.config.object_lock.is_none() && lock != ObjectLockState::default() {
                    return reject(Rejection::InvalidRequest(
                        "Object Lock headers require a bucket with Object Lock enabled".into(),
                    ));
                }
                let meta = ObjectMeta {
                    last_modified: now,
                    attrs: attrs.unwrap_or_else(|| src.attrs.clone()),
                    version_id: None,
                    lock,
                    ..src
                };
                self.add_refs(&meta.segments, &HashMap::new())?;
                let meta = self.write_object(&dst_info, &dst_key, meta)?;
                Ok(CommandOutput::Object(meta))
            }
            Command::CreateUpload { info } => {
                self.require_bucket(&info.bucket)?;
                self.txn
                    .open_table(UPLOADS)?
                    .insert(info.upload_id.as_str(), enc(&info)?.as_slice())?;
                self.txn.open_table(UPLOAD_INDEX)?.insert(
                    keys::upload_index_key(&info.bucket, &info.key, &info.upload_id).as_slice(),
                    info.upload_id.as_str(),
                )?;
                Ok(CommandOutput::Done)
            }
            Command::PutPart {
                bucket,
                key,
                upload_id,
                part,
                stored,
            } => {
                self.get_upload(&bucket, &key, &upload_id)?;
                self.add_refs(&part.segments, &stored.into_iter().collect())?;
                let old = self
                    .txn
                    .open_table(PARTS)?
                    .insert(
                        keys::part_key(&upload_id, part.part_number).as_slice(),
                        enc(&part)?.as_slice(),
                    )?
                    .map(|v| dec::<PartInfo>(v.value()))
                    .transpose()?;
                if let Some(old) = old {
                    self.drop_refs(&old.segments)?;
                }
                Ok(CommandOutput::Done)
            }
            Command::CompleteUpload {
                bucket,
                key,
                upload_id,
                parts: requested,
                now,
                cond,
                min_part_size,
            } => {
                let info = self.get_upload(&bucket, &key, &upload_id)?;
                if requested.is_empty() {
                    return reject(Rejection::InvalidPart(
                        "at least one part is required".into(),
                    ));
                }
                if requested.windows(2).any(|w| w[0].0 >= w[1].0) {
                    return reject(Rejection::InvalidPartOrder);
                }
                let uploaded: HashMap<u32, PartInfo> = self
                    .upload_parts(&upload_id)?
                    .into_iter()
                    .map(|p| (p.part_number, p))
                    .collect();

                let mut segments = Vec::new();
                let mut size = 0u64;
                let mut md5s = Md5::new();
                for (i, (n, etag)) in requested.iter().enumerate() {
                    let Some(part) = uploaded.get(n) else {
                        return reject(Rejection::InvalidPart(format!(
                            "part {n} was not uploaded"
                        )));
                    };
                    if etag.trim_matches('"') != part.etag {
                        return reject(Rejection::InvalidPart(format!(
                            "part {n} ETag does not match"
                        )));
                    }
                    if i + 1 < requested.len() && part.size < min_part_size {
                        return reject(Rejection::EntityTooSmall);
                    }
                    md5s.update(
                        hex::decode(&part.etag).map_err(|e| Error::Internal(e.to_string()))?,
                    );
                    size += part.size;
                    segments.extend(part.segments.iter().cloned());
                }
                check_condition(self.get_object(&bucket, &key)?.as_ref(), &cond)?;

                let bucket_info = self.bucket_info(&bucket)?;
                let meta = ObjectMeta {
                    size,
                    etag: format!("{}-{}", hex::encode(md5s.finalize()), requested.len()),
                    last_modified: now,
                    attrs: info.attrs.clone(),
                    segments,
                    version_id: None,
                    lock: info.lock.clone().unwrap_or_default(),
                };
                // Reference the chunks from the object before the parts release them.
                self.add_refs(&meta.segments, &HashMap::new())?;
                let meta = self.write_object(&bucket_info, &key, meta)?;
                self.remove_upload(&info)?;
                Ok(CommandOutput::Object(meta))
            }
            Command::AbortUpload {
                bucket,
                key,
                upload_id,
            } => {
                let info = self.get_upload(&bucket, &key, &upload_id)?;
                self.remove_upload(&info)?;
                Ok(CommandOutput::Done)
            }
            Command::SetBucketConfig { bucket, update } => {
                let t = self.txn.open_table(BUCKETS)?;
                let mut info: BucketInfo = match t.get(bucket.as_str())? {
                    Some(v) => dec(v.value())?,
                    None => return reject(Rejection::NoSuchBucket),
                };
                drop(t);
                match update {
                    BucketConfigUpdate::Tags(tags) => info.config.tags = tags,
                    BucketConfigUpdate::Cors(cors) => info.config.cors = cors,
                    BucketConfigUpdate::Lifecycle(rules) => info.config.lifecycle = rules,
                    BucketConfigUpdate::Versioning(v) => {
                        if v == Versioning::Unversioned {
                            return reject(Rejection::InvalidRequest(
                                "versioning can only be Enabled or Suspended".into(),
                            ));
                        }
                        if v == Versioning::Suspended && info.config.object_lock.is_some() {
                            return reject(Rejection::InvalidBucketState(
                                "An Object Lock configuration is present on this bucket, so the versioning state cannot be changed.".into(),
                            ));
                        }
                        if info.config.versioning == Versioning::Unversioned {
                            self.migrate_to_versioned(&bucket)?;
                        }
                        info.config.versioning = v;
                    }
                    BucketConfigUpdate::ObjectLock(cfg) => {
                        if info.config.versioning != Versioning::Enabled {
                            return reject(Rejection::InvalidBucketState(
                                "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration".into(),
                            ));
                        }
                        info.config.object_lock = Some(cfg);
                    }
                }
                self.txn
                    .open_table(BUCKETS)?
                    .insert(bucket.as_str(), enc(&info)?.as_slice())?;
                Ok(CommandOutput::Done)
            }
            Command::SetObjectTags {
                bucket,
                key,
                version_id,
                tags,
            } => {
                let info = self.bucket_info(&bucket)?;
                // Same manifest, so chunk references are unchanged.
                let meta = self.update_version(&info, &key, version_id.as_deref(), |m| {
                    m.attrs.tags = tags;
                    Ok(())
                })?;
                Ok(CommandOutput::Object(meta))
            }
            Command::SetObjectLock {
                bucket,
                key,
                version_id,
                retention,
                legal_hold,
                bypass_governance,
                now,
            } => {
                let info = self.bucket_info(&bucket)?;
                let meta = self.set_lock(
                    &info,
                    &key,
                    version_id.as_deref(),
                    retention,
                    legal_hold,
                    bypass_governance,
                    now,
                )?;
                Ok(CommandOutput::Object(meta))
            }
        }
    }
}

/// Raw key/value rows of one table.
type TableRows = Vec<(Vec<u8>, Vec<u8>)>;

/// Every table, for snapshots.
#[derive(Default, Serialize, Deserialize)]
struct SnapshotData {
    buckets: Vec<(String, Vec<u8>)>,
    objects: Vec<(Vec<u8>, Vec<u8>)>,
    chunks: Vec<(Vec<u8>, Vec<u8>)>,
    uploads: Vec<(String, Vec<u8>)>,
    upload_index: Vec<(Vec<u8>, String)>,
    parts: Vec<(Vec<u8>, Vec<u8>)>,
    meta: Vec<(String, Vec<u8>)>,
    /// Every table in `BYTE_TABLES`, by name.
    #[serde(default)]
    tables: Vec<(String, TableRows)>,
}

pub struct MetaStore {
    pub(crate) db: Database,
}

/// What one applied batch produced.
pub struct Applied {
    pub results: Vec<CommandResult>,
    /// Chunks whose last reference was dropped.
    pub freed: Vec<ChunkId>,
}

impl MetaStore {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path)?;
        let txn = db.begin_write()?;
        create_tables(&txn)?;
        txn.commit()?;
        Ok(MetaStore { db })
    }

    /// Apply commands in order, in one atomic commit together with the
    /// consensus layer's `sm_state` (if given).
    pub fn apply(&self, cmds: Vec<Command>, sm_state: Option<Vec<u8>>) -> Result<Applied> {
        self.apply_requests(cmds.into_iter().map(Request::from).collect(), sm_state)
    }

    /// Apply requests in order, in one atomic commit together with the
    /// consensus layer's `sm_state` (if given). Requests whose id was already
    /// applied return the remembered result and change nothing.
    pub fn apply_requests(&self, reqs: Vec<Request>, sm_state: Option<Vec<u8>>) -> Result<Applied> {
        let txn = self.db.begin_write()?;
        let stats = match txn.open_table(META)?.get(STATS_KEY)? {
            Some(v) => dec(v.value())?,
            None => Stats::default(),
        };
        let mut tx = Tx {
            txn: &txn,
            stats,
            freed: Vec::new(),
        };
        let mut results = Vec::with_capacity(reqs.len());
        for Request { id, cmd } in reqs {
            if let Some(id) = id
                && let Some(prev) = tx.remembered(id)?
            {
                results.push(prev);
                continue;
            }
            let result = match tx.apply(cmd) {
                Ok(out) => Ok(out),
                Err(Fail::Reject(r)) => Err(r),
                Err(Fail::Fatal(e)) => return Err(e),
            };
            if let Some(id) = id {
                tx.remember(id, &result)?;
            }
            results.push(result);
        }
        let Tx { stats, freed, .. } = tx;
        {
            let mut meta = txn.open_table(META)?;
            meta.insert(STATS_KEY, enc(&stats)?.as_slice())?;
            if let Some(s) = sm_state {
                meta.insert(SM_STATE_KEY, s.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(Applied { results, freed })
    }

    pub fn sm_state(&self) -> Result<Option<Vec<u8>>> {
        let r = self.db.begin_read()?;
        Ok(r.open_table(META)?
            .get(SM_STATE_KEY)?
            .map(|v| v.value().to_vec()))
    }

    /// Persist consensus state without applying commands (e.g. membership
    /// entries).
    pub fn set_sm_state(&self, sm_state: Vec<u8>) -> Result<()> {
        self.apply(Vec::new(), Some(sm_state)).map(drop)
    }

    /// Referenced chunks in id order, starting after `after`, with their
    /// lengths. Used by cluster repair to walk every chunk in pages.
    pub fn chunk_page(&self, after: Option<ChunkId>, limit: usize) -> Result<Vec<(ChunkId, u32)>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(CHUNKS)?;
        let start = after.map(|a| a.0);
        let range = match &start {
            Some(a) => t.range::<&[u8]>((Bound::Excluded(a.as_slice()), Bound::Unbounded))?,
            None => t.range::<&[u8]>(..)?,
        };
        let mut out = Vec::with_capacity(limit.min(4096));
        for item in range.take(limit) {
            let (k, v) = item?;
            let id: [u8; 32] = k
                .value()
                .try_into()
                .map_err(|_| Error::Internal("malformed chunk id".into()))?;
            let info: ChunkInfo = dec(v.value())?;
            out.push((ChunkId(id), info.len));
        }
        Ok(out)
    }

    pub fn is_referenced(&self, id: &ChunkId) -> Result<bool> {
        let r = self.db.begin_read()?;
        Ok(r.open_table(CHUNKS)?.get(id.0.as_slice())?.is_some())
    }

    /// Serialize the whole state machine. Also returns the `sm_state` read
    /// in the same transaction, which describes exactly this data.
    pub fn export_snapshot(&self) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
        fn dump<K, V, KO, VO>(
            r: &ReadTransaction,
            def: TableDefinition<K, V>,
            kf: impl Fn(K::SelfType<'_>) -> KO,
            vf: impl Fn(V::SelfType<'_>) -> VO,
        ) -> Result<Vec<(KO, VO)>>
        where
            K: redb::Key + 'static,
            V: redb::Value + 'static,
        {
            let t = r.open_table(def)?;
            let mut out = Vec::new();
            for item in t.iter()? {
                let (k, v) = item?;
                out.push((kf(k.value()), vf(v.value())));
            }
            Ok(out)
        }
        let r = self.db.begin_read()?;
        let s = |x: &str| x.to_string();
        let b = |x: &[u8]| x.to_vec();
        let data = SnapshotData {
            buckets: dump(&r, BUCKETS, s, b)?,
            objects: dump(&r, OBJECTS, b, b)?,
            chunks: dump(&r, CHUNKS, b, b)?,
            uploads: dump(&r, UPLOADS, s, b)?,
            upload_index: dump(&r, UPLOAD_INDEX, b, s)?,
            parts: dump(&r, PARTS, b, b)?,
            meta: dump(&r, META, s, b)?,
            tables: BYTE_TABLES
                .iter()
                .map(|def| Ok((def.name().to_string(), dump(&r, *def, b, b)?)))
                .collect::<Result<_>>()?,
        };
        let sm_state = data
            .meta
            .iter()
            .find(|(k, _)| k == SM_STATE_KEY)
            .map(|(_, v)| v.clone());
        Ok((enc(&data)?, sm_state))
    }

    /// Replace the whole state machine with a snapshot.
    pub fn import_snapshot(&self, bytes: &[u8]) -> Result<()> {
        let data: SnapshotData = dec(bytes)?;
        let txn = self.db.begin_write()?;
        txn.delete_table(BUCKETS)?;
        txn.delete_table(OBJECTS)?;
        txn.delete_table(CHUNKS)?;
        txn.delete_table(UPLOADS)?;
        txn.delete_table(UPLOAD_INDEX)?;
        txn.delete_table(PARTS)?;
        txn.delete_table(META)?;
        for def in BYTE_TABLES {
            txn.delete_table(*def)?;
        }
        create_tables(&txn)?;
        {
            let mut t = txn.open_table(BUCKETS)?;
            for (k, v) in &data.buckets {
                t.insert(k.as_str(), v.as_slice())?;
            }
            let mut t = txn.open_table(OBJECTS)?;
            for (k, v) in &data.objects {
                t.insert(k.as_slice(), v.as_slice())?;
            }
            let mut t = txn.open_table(CHUNKS)?;
            for (k, v) in &data.chunks {
                t.insert(k.as_slice(), v.as_slice())?;
            }
            let mut t = txn.open_table(UPLOADS)?;
            for (k, v) in &data.uploads {
                t.insert(k.as_str(), v.as_slice())?;
            }
            let mut t = txn.open_table(UPLOAD_INDEX)?;
            for (k, v) in &data.upload_index {
                t.insert(k.as_slice(), v.as_str())?;
            }
            let mut t = txn.open_table(PARTS)?;
            for (k, v) in &data.parts {
                t.insert(k.as_slice(), v.as_slice())?;
            }
            let mut t = txn.open_table(META)?;
            for (k, v) in &data.meta {
                t.insert(k.as_str(), v.as_slice())?;
            }
            for (name, rows) in &data.tables {
                let Some(def) = BYTE_TABLES.iter().find(|d| d.name() == name) else {
                    return Err(Error::Internal(format!(
                        "snapshot has unknown table {name}"
                    )));
                };
                let mut t = txn.open_table(*def)?;
                for (k, v) in rows {
                    t.insert(k.as_slice(), v.as_slice())?;
                }
            }
        }
        txn.commit()?;
        Ok(())
    }

    // ------------------------------------------------------------- queries

    fn read(&self) -> Result<ReadTransaction> {
        Ok(self.db.begin_read()?)
    }

    pub fn bucket(&self, name: &str) -> Result<BucketInfo> {
        match self.read()?.open_table(BUCKETS)?.get(name)? {
            Some(v) => dec(v.value()),
            None => Err(Error::NoSuchBucket),
        }
    }

    pub fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let r = self.read()?;
        let mut out = Vec::new();
        for item in r.open_table(BUCKETS)?.iter()? {
            out.push(dec(item?.1.value())?);
        }
        Ok(out)
    }

    pub fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMeta> {
        let r = self.read()?;
        if r.open_table(BUCKETS)?.get(bucket)?.is_none() {
            return Err(Error::NoSuchBucket);
        }
        match r
            .open_table(OBJECTS)?
            .get(keys::object_key(bucket, key).as_slice())?
        {
            Some(v) => dec(v.value()),
            None => Err(Error::NoSuchKey),
        }
    }

    pub fn list_objects(&self, bucket: &str, q: &ListQuery) -> Result<ListPage> {
        let r = self.read()?;
        if r.open_table(BUCKETS)?.get(bucket)?.is_none() {
            return Err(Error::NoSuchBucket);
        }
        list_page(&r.open_table(OBJECTS)?, bucket, q)
    }

    pub fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(UploadInfo, Vec<PartInfo>)> {
        let r = self.read()?;
        let info: UploadInfo = match r.open_table(UPLOADS)?.get(upload_id)? {
            Some(v) => dec(v.value())?,
            None => return Err(Error::NoSuchUpload),
        };
        if info.bucket != bucket || info.key != key {
            return Err(Error::NoSuchUpload);
        }
        let t = r.open_table(PARTS)?;
        let (s, e) = keys::upload_parts_range(upload_id);
        let mut parts = Vec::new();
        for item in t.range(s.as_slice()..e.as_slice())? {
            parts.push(dec(item?.1.value())?);
        }
        Ok((info, parts))
    }

    pub fn list_uploads(&self, bucket: &str, prefix: &str) -> Result<Vec<UploadInfo>> {
        let r = self.read()?;
        if r.open_table(BUCKETS)?.get(bucket)?.is_none() {
            return Err(Error::NoSuchBucket);
        }
        let index = r.open_table(UPLOAD_INDEX)?;
        let uploads = r.open_table(UPLOADS)?;
        let start = keys::object_key(bucket, prefix);
        let end = keys::prefix_successor(&start).expect("bucket name is not all 0xff");
        let mut out = Vec::new();
        for item in index.range(start.as_slice()..end.as_slice())? {
            let id = item?.1.value().to_string();
            if let Some(v) = uploads.get(id.as_str())? {
                out.push(dec(v.value())?);
            }
        }
        Ok(out)
    }

    pub fn stats(&self) -> Result<Stats> {
        match self.read()?.open_table(META)?.get(STATS_KEY)? {
            Some(v) => dec(v.value()),
            None => Ok(Stats::default()),
        }
    }
}

fn create_tables(txn: &WriteTransaction) -> Result<()> {
    txn.open_table(BUCKETS)?;
    txn.open_table(OBJECTS)?;
    txn.open_table(CHUNKS)?;
    txn.open_table(UPLOADS)?;
    txn.open_table(UPLOAD_INDEX)?;
    txn.open_table(PARTS)?;
    txn.open_table(META)?;
    for def in BYTE_TABLES {
        txn.open_table(*def)?;
    }
    Ok(())
}

fn list_page(
    objects: &impl ReadableTable<&'static [u8], &'static [u8]>,
    bucket: &str,
    q: &ListQuery,
) -> Result<ListPage> {
    let mut page = ListPage::default();
    if q.max_keys == 0 {
        return Ok(page);
    }
    let base = bucket.len() + 1;
    let delim = q.delimiter.as_deref().filter(|d| !d.is_empty());
    let prefix_key = keys::object_key(bucket, &q.prefix);
    let end = keys::prefix_successor(&prefix_key).expect("bucket name is not all 0xff");

    // The common prefix that `key` rolls up into, if any.
    let rollup = |key: &str| -> Option<String> {
        let d = delim?;
        let rest = key.strip_prefix(q.prefix.as_str())?;
        rest.find(d)
            .map(|i| key[..q.prefix.len() + i + d.len()].to_string())
    };

    let mut lower: Bound<Vec<u8>> = Bound::Included(prefix_key.clone());
    if let Some(sa) = q.start_after.as_deref().filter(|s| !s.is_empty()) {
        let sa_bound = match rollup(sa) {
            // Resuming after a common prefix skips everything beneath it.
            Some(cp) => keys::prefix_successor(&keys::object_key(bucket, &cp))
                .map(Bound::Included)
                .unwrap_or(Bound::Excluded(end.clone())),
            None => Bound::Excluded(keys::object_key(bucket, sa)),
        };
        let sa_key = match &sa_bound {
            Bound::Included(k) | Bound::Excluded(k) => k.clone(),
            Bound::Unbounded => unreachable!(),
        };
        if sa_key >= prefix_key {
            lower = sa_bound;
        }
    }

    'scan: loop {
        let range = objects.range::<&[u8]>((
            match &lower {
                Bound::Included(k) => Bound::Included(k.as_slice()),
                Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
                Bound::Unbounded => Bound::Unbounded,
            },
            Bound::Excluded(end.as_slice()),
        ))?;
        for item in range {
            let (k, v) = item?;
            let key = std::str::from_utf8(&k.value()[base..])
                .map_err(|_| Error::Internal("non-utf8 key in metadata".into()))?
                .to_string();
            if page.objects.len() + page.common_prefixes.len() >= q.max_keys {
                page.is_truncated = true;
                break 'scan;
            }
            if let Some(cp) = rollup(&key) {
                let skip_to = keys::prefix_successor(&keys::object_key(bucket, &cp));
                page.next_marker = Some(cp.clone());
                page.common_prefixes.push(cp);
                match skip_to {
                    Some(s) if s < end => {
                        lower = Bound::Included(s);
                        continue 'scan;
                    }
                    _ => break 'scan,
                }
            }
            let meta: ObjectMeta = dec(v.value())?;
            page.objects.push(ObjectSummary {
                key: key.clone(),
                size: meta.size,
                etag: meta.etag,
                last_modified: meta.last_modified,
            });
            page.next_marker = Some(key);
        }
        break;
    }
    if !page.is_truncated {
        page.next_marker = None;
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(name: &str) -> Command {
        Command::CreateBucket {
            info: BucketInfo {
                name: name.into(),
                created: Utc::now(),
                config: Default::default(),
            },
        }
    }

    #[test]
    fn rejected_commands_do_not_abort_the_batch() {
        let dir = tempfile::tempdir().unwrap();
        let m = MetaStore::open(&dir.path().join("m.redb")).unwrap();
        let applied = m
            .apply(
                vec![bucket("one"), bucket("one"), bucket("two")],
                Some(b"applied=3".to_vec()),
            )
            .unwrap();
        assert!(applied.results[0].is_ok());
        assert_eq!(
            applied.results[1].as_ref().unwrap_err(),
            &Rejection::BucketAlreadyExists
        );
        assert!(applied.results[2].is_ok());
        assert_eq!(m.list_buckets().unwrap().len(), 2);
        assert_eq!(m.sm_state().unwrap().as_deref(), Some(&b"applied=3"[..]));
    }

    #[test]
    fn requests_apply_at_most_once() {
        let dir = tempfile::tempdir().unwrap();
        let m = MetaStore::open(&dir.path().join("m.redb")).unwrap();
        let id = RequestId::random();
        let req = || Request {
            id: Some(id),
            cmd: bucket("once"),
        };
        assert!(m.apply_requests(vec![req()], None).unwrap().results[0].is_ok());
        // A retry of the same request returns the original success instead of
        // BucketAlreadyExists, and changes nothing.
        assert!(m.apply_requests(vec![req()], None).unwrap().results[0].is_ok());
        assert_eq!(m.stats().unwrap().buckets, 1);
        // A different request for the same bucket is a genuine conflict.
        let other = Request {
            id: Some(RequestId::random()),
            cmd: bucket("once"),
        };
        assert_eq!(
            m.apply_requests(vec![other], None).unwrap().results[0]
                .as_ref()
                .unwrap_err(),
            &Rejection::BucketAlreadyExists
        );
        // The memory survives a snapshot round trip.
        let (snap, _) = m.export_snapshot().unwrap();
        let n = MetaStore::open(&dir.path().join("n.redb")).unwrap();
        n.import_snapshot(&snap).unwrap();
        assert!(n.apply_requests(vec![req()], None).unwrap().results[0].is_ok());
        assert_eq!(n.stats().unwrap().buckets, 1);
    }

    #[test]
    fn snapshot_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let a = MetaStore::open(&dir.path().join("a.redb")).unwrap();
        a.apply(vec![bucket("alpha"), bucket("beta")], Some(b"s".to_vec()))
            .unwrap();
        let (snap, state) = a.export_snapshot().unwrap();
        assert_eq!(state.as_deref(), Some(&b"s"[..]));

        let b = MetaStore::open(&dir.path().join("b.redb")).unwrap();
        b.apply(vec![bucket("stale")], None).unwrap();
        b.import_snapshot(&snap).unwrap();
        let names: Vec<_> = b
            .list_buckets()
            .unwrap()
            .into_iter()
            .map(|b| b.name)
            .collect();
        assert_eq!(names, ["alpha", "beta"]);
        assert_eq!(b.stats().unwrap().buckets, 2);
        assert_eq!(b.sm_state().unwrap().as_deref(), Some(&b"s"[..]));
    }
}
