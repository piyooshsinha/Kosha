use std::collections::HashMap;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use futures::stream::{self, BoxStream, StreamExt};
use md5::{Digest, Md5};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};

use crate::chunker::ChunkerConfig;
use crate::chunkstore::ChunkStore;
use crate::error::{Error, Result};
use crate::keys;
use crate::types::*;
use crate::writer::{ObjectWriter, WrittenData};

const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");
const OBJECTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("objects");
const CHUNKS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("chunks");
const UPLOADS: TableDefinition<&str, &[u8]> = TableDefinition::new("uploads");
const UPLOAD_INDEX: TableDefinition<&[u8], &str> = TableDefinition::new("upload_index");
const PARTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("parts");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

const STATS_KEY: &str = "stats";
const MAX_KEY_LEN: usize = 1024;
const MAX_PART_NUMBER: u32 = 10_000;

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    /// fsync chunk files and metadata commits before acknowledging writes.
    pub fsync: bool,
    /// zstd level for compressible chunks; 0 disables compression.
    pub compression_level: i32,
    /// Objects up to this size are stored inside their manifest.
    pub inline_max: usize,
    pub chunking: ChunkerConfig,
    /// Chunks hashed/compressed/written concurrently per upload.
    pub write_parallelism: usize,
    /// Chunks fetched ahead of the client during reads.
    pub read_ahead: usize,
    /// Minimum size of every multipart part except the last.
    pub min_part_size: u64,
}

impl EngineConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        EngineConfig {
            data_dir: data_dir.into(),
            fsync: true,
            compression_level: 3,
            inline_max: 8 * 1024,
            chunking: ChunkerConfig::default(),
            write_parallelism: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .min(8),
            read_ahead: 4,
            min_part_size: 5 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChunkInfo {
    refs: u64,
    len: u32,
    stored: u64,
}

struct Inner {
    db: Database,
    store: Arc<ChunkStore>,
    cfg: EngineConfig,
}

/// The storage engine. Cheap to clone; all clones share state.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

fn enc<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec_named(v)?)
}

fn dec<T: for<'de> Deserialize<'de>>(b: &[u8]) -> Result<T> {
    Ok(rmp_serde::from_slice(b)?)
}

pub fn validate_bucket_name(name: &str) -> Result<()> {
    let bad = |why: &str| Err(Error::InvalidBucketName(format!("{name:?}: {why}")));
    if !(3..=63).contains(&name.len()) {
        return bad("must be 3-63 characters");
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
    {
        return bad("only lowercase letters, digits, '.' and '-' are allowed");
    }
    let first = name.as_bytes()[0];
    let last = name.as_bytes()[name.len() - 1];
    if !first.is_ascii_alphanumeric() || !last.is_ascii_alphanumeric() {
        return bad("must start and end with a letter or digit");
    }
    if name.contains("..") || name.parse::<std::net::Ipv4Addr>().is_ok() {
        return bad("must not contain '..' or look like an IP address");
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidKey("key must not be empty".into()));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(Error::InvalidKey("key is longer than 1024 bytes".into()));
    }
    Ok(())
}

fn check_condition(current: Option<&ObjectMeta>, cond: &WriteCondition) -> Result<()> {
    if cond.if_none_match_any && current.is_some() {
        return Err(Error::PreconditionFailed);
    }
    if let Some(want) = &cond.if_match {
        match current {
            None => return Err(Error::NoSuchKey),
            Some(m) if want.trim_matches('"') != m.etag && want != "*" => {
                return Err(Error::PreconditionFailed);
            }
            _ => {}
        }
    }
    Ok(())
}

/// Mutation context for one metadata transaction.
struct Tx<'a> {
    txn: &'a WriteTransaction,
    stats: Stats,
    freed: Vec<ChunkId>,
}

impl Tx<'_> {
    fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        Ok(self.txn.open_table(BUCKETS)?.get(bucket)?.is_some())
    }

    fn require_bucket(&self, bucket: &str) -> Result<()> {
        if self.bucket_exists(bucket)? {
            Ok(())
        } else {
            Err(Error::NoSuchBucket)
        }
    }

    fn add_refs(&mut self, segments: &[Segment], stored: &HashMap<ChunkId, u64>) -> Result<()> {
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

    fn drop_refs(&mut self, segments: &[Segment]) -> Result<()> {
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

    fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let t = self.txn.open_table(OBJECTS)?;
        let v = t.get(keys::object_key(bucket, key).as_slice())?;
        v.map(|v| dec(v.value())).transpose()
    }

    /// Insert or replace an object whose chunk references have already been added.
    fn put_object(&mut self, bucket: &str, key: &str, meta: &ObjectMeta) -> Result<()> {
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

    fn delete_object(&mut self, bucket: &str, key: &str) -> Result<bool> {
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

    fn get_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<UploadInfo> {
        let t = self.txn.open_table(UPLOADS)?;
        let info: UploadInfo = match t.get(upload_id)? {
            Some(v) => dec(v.value())?,
            None => return Err(Error::NoSuchUpload),
        };
        if info.bucket != bucket || info.key != key {
            return Err(Error::NoSuchUpload);
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
}

impl Engine {
    pub fn open(cfg: EngineConfig) -> Result<Engine> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        let store = ChunkStore::open(
            &cfg.data_dir.join("chunks"),
            cfg.fsync,
            cfg.compression_level,
        )?;
        let db = Database::create(cfg.data_dir.join("meta.redb"))?;
        {
            // Create every table up front so read transactions never miss one.
            let txn = db.begin_write()?;
            txn.open_table(BUCKETS)?;
            txn.open_table(OBJECTS)?;
            txn.open_table(CHUNKS)?;
            txn.open_table(UPLOADS)?;
            txn.open_table(UPLOAD_INDEX)?;
            txn.open_table(PARTS)?;
            txn.open_table(META)?;
            txn.commit()?;
        }
        Ok(Engine {
            inner: Arc::new(Inner {
                db,
                store: Arc::new(store),
                cfg,
            }),
        })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.cfg
    }

    /// Start ingesting a body. Feed it with [`ObjectWriter::write`], then pass
    /// the result of [`ObjectWriter::finish`] to a commit method.
    pub fn writer(&self) -> ObjectWriter {
        let cfg = &self.inner.cfg;
        ObjectWriter::new(
            self.inner.store.clone(),
            cfg.chunking,
            cfg.inline_max,
            cfg.write_parallelism,
        )
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Inner) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || f(&inner)).await?
    }

    /// Run `f` in a write transaction, commit, then collect any chunks the
    /// transaction released. The return value is dropped only after the
    /// commit, which is how uploads keep their chunk pins alive until their
    /// references are durable: releasing a pin before the commit would let a
    /// concurrent collector delete a chunk that is about to be referenced.
    async fn write_txn<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Tx<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.blocking(move |inner| {
            let txn = inner.db.begin_write()?;
            let stats = match txn.open_table(META)?.get(STATS_KEY)? {
                Some(v) => dec(v.value())?,
                None => Stats::default(),
            };
            let mut tx = Tx {
                txn: &txn,
                stats,
                freed: Vec::new(),
            };
            let out = f(&mut tx)?;
            let Tx { stats, freed, .. } = tx;
            txn.open_table(META)?
                .insert(STATS_KEY, enc(&stats)?.as_slice())?;
            txn.commit()?;

            if !freed.is_empty() {
                let removed = inner.store.collect(&freed, |id| {
                    let r = inner.db.begin_read()?;
                    Ok(r.open_table(CHUNKS)?.get(id.0.as_slice())?.is_none())
                })?;
                tracing::debug!(candidates = freed.len(), removed, "collected chunks");
            }
            Ok(out)
        })
        .await
    }

    async fn read_txn<T: Send + 'static>(
        &self,
        f: impl FnOnce(&redb::ReadTransaction) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.blocking(move |inner| f(&inner.db.begin_read()?)).await
    }

    // ---------------------------------------------------------------- buckets

    pub async fn create_bucket(&self, name: &str) -> Result<BucketInfo> {
        validate_bucket_name(name)?;
        let info = BucketInfo {
            name: name.to_string(),
            created: Utc::now(),
        };
        let out = info.clone();
        self.write_txn(move |tx| {
            if tx.bucket_exists(&info.name)? {
                return Err(Error::BucketAlreadyExists);
            }
            tx.txn
                .open_table(BUCKETS)?
                .insert(info.name.as_str(), enc(&info)?.as_slice())?;
            tx.stats.buckets += 1;
            Ok(())
        })
        .await?;
        Ok(out)
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<()> {
        let name = name.to_string();
        self.write_txn(move |tx| {
            tx.require_bucket(&name)?;
            let (s, e) = keys::bucket_range(&name);
            if tx
                .txn
                .open_table(OBJECTS)?
                .range(s.as_slice()..e.as_slice())?
                .next()
                .is_some()
            {
                return Err(Error::BucketNotEmpty);
            }
            // Abandon in-progress multipart uploads along with the bucket.
            let mut ids = Vec::new();
            for item in tx
                .txn
                .open_table(UPLOAD_INDEX)?
                .range(s.as_slice()..e.as_slice())?
            {
                ids.push(item?.1.value().to_string());
            }
            for id in ids {
                let info: UploadInfo = dec(tx
                    .txn
                    .open_table(UPLOADS)?
                    .get(id.as_str())?
                    .ok_or_else(|| Error::Internal("dangling upload index".into()))?
                    .value())?;
                tx.remove_upload(&info)?;
            }
            tx.txn.open_table(BUCKETS)?.remove(name.as_str())?;
            tx.stats.buckets -= 1;
            Ok(())
        })
        .await
    }

    pub async fn bucket(&self, name: &str) -> Result<BucketInfo> {
        let name = name.to_string();
        self.read_txn(move |r| match r.open_table(BUCKETS)?.get(name.as_str())? {
            Some(v) => dec(v.value()),
            None => Err(Error::NoSuchBucket),
        })
        .await
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        self.read_txn(|r| {
            let mut out = Vec::new();
            for item in r.open_table(BUCKETS)?.iter()? {
                out.push(dec(item?.1.value())?);
            }
            Ok(out)
        })
        .await
    }

    // ---------------------------------------------------------------- objects

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: WrittenData,
        attrs: ObjectAttrs,
        cond: WriteCondition,
    ) -> Result<ObjectMeta> {
        validate_key(key)?;
        let meta = ObjectMeta {
            size: data.size,
            etag: data.md5_hex(),
            last_modified: Utc::now(),
            attrs,
            segments: data.segments.clone(),
        };
        let (bucket, key, out) = (bucket.to_string(), key.to_string(), meta.clone());
        self.write_txn(move |tx| {
            tx.require_bucket(&bucket)?;
            check_condition(tx.get_object(&bucket, &key)?.as_ref(), &cond)?;
            tx.add_refs(&meta.segments, &data.stored)?;
            tx.put_object(&bucket, &key, &meta)?;
            // Hand the pins back so they outlive the commit; see `write_txn`.
            Ok(data)
        })
        .await?;
        Ok(out)
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMeta> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.read_txn(move |r| {
            if r.open_table(BUCKETS)?.get(bucket.as_str())?.is_none() {
                return Err(Error::NoSuchBucket);
            }
            match r
                .open_table(OBJECTS)?
                .get(keys::object_key(&bucket, &key).as_slice())?
            {
                Some(v) => dec(v.value()),
                None => Err(Error::NoSuchKey),
            }
        })
        .await
    }

    /// Stream bytes `[start, end)` of an object, touching only the chunks
    /// that overlap the range and prefetching a few chunks ahead.
    pub fn read(
        &self,
        meta: &ObjectMeta,
        start: u64,
        end: u64,
    ) -> BoxStream<'static, Result<Bytes>> {
        let mut pieces = Vec::new();
        let mut offset = 0u64;
        for seg in &meta.segments {
            let seg_end = offset + seg.len();
            if seg_end > start && offset < end {
                let from = start.saturating_sub(offset) as usize;
                let to = (end.min(seg_end) - offset) as usize;
                pieces.push((seg.clone(), from, to));
            }
            if seg_end >= end {
                break;
            }
            offset = seg_end;
        }
        let store = self.inner.store.clone();
        stream::iter(pieces)
            .map(move |(seg, from, to)| {
                let store = store.clone();
                async move {
                    match seg {
                        Segment::Inline(d) => Ok(Bytes::from(d).slice(from..to)),
                        Segment::Chunk { id, len } => {
                            let b =
                                tokio::task::spawn_blocking(move || store.get(&id, len)).await??;
                            Ok(b.slice(from..to))
                        }
                    }
                }
            })
            .buffered(self.inner.cfg.read_ahead.max(1))
            .boxed()
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.write_txn(move |tx| {
            tx.require_bucket(&bucket)?;
            tx.delete_object(&bucket, &key)
        })
        .await
    }

    /// Delete many keys in one atomic commit. Returns which keys existed.
    pub async fn delete_objects(&self, bucket: &str, keys: Vec<String>) -> Result<Vec<bool>> {
        let bucket = bucket.to_string();
        self.write_txn(move |tx| {
            tx.require_bucket(&bucket)?;
            keys.iter().map(|k| tx.delete_object(&bucket, k)).collect()
        })
        .await
    }

    /// Server-side copy. Because chunks are content addressed this is a pure
    /// metadata operation: no object bytes are read or written.
    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        new_attrs: Option<ObjectAttrs>,
        cond: WriteCondition,
    ) -> Result<ObjectMeta> {
        validate_key(dst_key)?;
        let (sb, sk, db, dk) = (
            src_bucket.to_string(),
            src_key.to_string(),
            dst_bucket.to_string(),
            dst_key.to_string(),
        );
        self.write_txn(move |tx| {
            tx.require_bucket(&sb)?;
            tx.require_bucket(&db)?;
            let src = tx.get_object(&sb, &sk)?.ok_or(Error::NoSuchKey)?;
            check_condition(tx.get_object(&db, &dk)?.as_ref(), &cond)?;
            let meta = ObjectMeta {
                last_modified: Utc::now(),
                attrs: new_attrs.unwrap_or_else(|| src.attrs.clone()),
                ..src
            };
            tx.add_refs(&meta.segments, &HashMap::new())?;
            tx.put_object(&db, &dk, &meta)?;
            Ok(meta)
        })
        .await
    }

    pub async fn list_objects(&self, bucket: &str, q: ListQuery) -> Result<ListPage> {
        let bucket = bucket.to_string();
        self.read_txn(move |r| {
            if r.open_table(BUCKETS)?.get(bucket.as_str())?.is_none() {
                return Err(Error::NoSuchBucket);
            }
            let objects = r.open_table(OBJECTS)?;
            list_page(&objects, &bucket, &q)
        })
        .await
    }

    // -------------------------------------------------------------- multipart

    pub async fn create_upload(
        &self,
        bucket: &str,
        key: &str,
        attrs: ObjectAttrs,
    ) -> Result<UploadInfo> {
        validate_key(key)?;
        let info = UploadInfo {
            upload_id: uuid::Uuid::new_v4().simple().to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            attrs,
            initiated: Utc::now(),
        };
        let out = info.clone();
        self.write_txn(move |tx| {
            tx.require_bucket(&info.bucket)?;
            tx.txn
                .open_table(UPLOADS)?
                .insert(info.upload_id.as_str(), enc(&info)?.as_slice())?;
            tx.txn.open_table(UPLOAD_INDEX)?.insert(
                keys::upload_index_key(&info.bucket, &info.key, &info.upload_id).as_slice(),
                info.upload_id.as_str(),
            )?;
            Ok(())
        })
        .await?;
        Ok(out)
    }

    pub async fn put_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: WrittenData,
    ) -> Result<PartInfo> {
        if !(1..=MAX_PART_NUMBER).contains(&part_number) {
            return Err(Error::InvalidPart(format!(
                "part number must be between 1 and {MAX_PART_NUMBER}"
            )));
        }
        let part = PartInfo {
            part_number,
            etag: data.md5_hex(),
            size: data.size,
            last_modified: Utc::now(),
            segments: data.segments.clone(),
        };
        let (bucket, key, upload_id, out) = (
            bucket.to_string(),
            key.to_string(),
            upload_id.to_string(),
            part.clone(),
        );
        self.write_txn(move |tx| {
            tx.get_upload(&bucket, &key, &upload_id)?;
            tx.add_refs(&part.segments, &data.stored)?;
            let old = tx
                .txn
                .open_table(PARTS)?
                .insert(
                    keys::part_key(&upload_id, part_number).as_slice(),
                    enc(&part)?.as_slice(),
                )?
                .map(|v| dec::<PartInfo>(v.value()))
                .transpose()?;
            if let Some(old) = old {
                tx.drop_refs(&old.segments)?;
            }
            Ok(data)
        })
        .await?;
        Ok(out)
    }

    /// Stitch parts into an object. Only manifests are concatenated; part
    /// data is never re-read or re-written.
    pub async fn complete_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        requested: Vec<(u32, String)>,
        cond: WriteCondition,
    ) -> Result<ObjectMeta> {
        let min_part = self.inner.cfg.min_part_size;
        let (bucket, key, upload_id) = (bucket.to_string(), key.to_string(), upload_id.to_string());
        self.write_txn(move |tx| {
            let info = tx.get_upload(&bucket, &key, &upload_id)?;
            if requested.is_empty() {
                return Err(Error::InvalidPart("at least one part is required".into()));
            }
            if requested.windows(2).any(|w| w[0].0 >= w[1].0) {
                return Err(Error::InvalidPartOrder);
            }
            let stored: HashMap<u32, PartInfo> = tx
                .upload_parts(&upload_id)?
                .into_iter()
                .map(|p| (p.part_number, p))
                .collect();

            let mut segments = Vec::new();
            let mut size = 0u64;
            let mut md5s = Md5::new();
            for (i, (n, etag)) in requested.iter().enumerate() {
                let part = stored
                    .get(n)
                    .ok_or_else(|| Error::InvalidPart(format!("part {n} was not uploaded")))?;
                if etag.trim_matches('"') != part.etag {
                    return Err(Error::InvalidPart(format!("part {n} ETag does not match")));
                }
                if i + 1 < requested.len() && part.size < min_part {
                    return Err(Error::EntityTooSmall);
                }
                md5s.update(hex::decode(&part.etag).map_err(|e| Error::Internal(e.to_string()))?);
                size += part.size;
                segments.extend(part.segments.iter().cloned());
            }
            check_condition(tx.get_object(&bucket, &key)?.as_ref(), &cond)?;

            let meta = ObjectMeta {
                size,
                etag: format!("{}-{}", hex::encode(md5s.finalize()), requested.len()),
                last_modified: Utc::now(),
                attrs: info.attrs.clone(),
                segments,
            };
            // Reference the chunks from the object before the parts release them.
            tx.add_refs(&meta.segments, &HashMap::new())?;
            tx.put_object(&bucket, &key, &meta)?;
            tx.remove_upload(&info)?;
            Ok(meta)
        })
        .await
    }

    pub async fn abort_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
        let (bucket, key, upload_id) = (bucket.to_string(), key.to_string(), upload_id.to_string());
        self.write_txn(move |tx| {
            let info = tx.get_upload(&bucket, &key, &upload_id)?;
            tx.remove_upload(&info)
        })
        .await
    }

    pub async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(UploadInfo, Vec<PartInfo>)> {
        let (bucket, key, upload_id) = (bucket.to_string(), key.to_string(), upload_id.to_string());
        self.read_txn(move |r| {
            let info: UploadInfo = match r.open_table(UPLOADS)?.get(upload_id.as_str())? {
                Some(v) => dec(v.value())?,
                None => return Err(Error::NoSuchUpload),
            };
            if info.bucket != bucket || info.key != key {
                return Err(Error::NoSuchUpload);
            }
            let t = r.open_table(PARTS)?;
            let (s, e) = keys::upload_parts_range(&upload_id);
            let mut parts = Vec::new();
            for item in t.range(s.as_slice()..e.as_slice())? {
                parts.push(dec(item?.1.value())?);
            }
            Ok((info, parts))
        })
        .await
    }

    pub async fn list_uploads(&self, bucket: &str, prefix: &str) -> Result<Vec<UploadInfo>> {
        let (bucket, prefix) = (bucket.to_string(), prefix.to_string());
        self.read_txn(move |r| {
            if r.open_table(BUCKETS)?.get(bucket.as_str())?.is_none() {
                return Err(Error::NoSuchBucket);
            }
            let index = r.open_table(UPLOAD_INDEX)?;
            let uploads = r.open_table(UPLOADS)?;
            let start = keys::object_key(&bucket, &prefix);
            let end = keys::prefix_successor(&start).expect("bucket name is not all 0xff");
            let mut out = Vec::new();
            for item in index.range(start.as_slice()..end.as_slice())? {
                let id = item?.1.value().to_string();
                if let Some(v) = uploads.get(id.as_str())? {
                    out.push(dec(v.value())?);
                }
            }
            Ok(out)
        })
        .await
    }

    // ------------------------------------------------------------------ admin

    pub async fn stats(&self) -> Result<Stats> {
        self.read_txn(|r| match r.open_table(META)?.get(STATS_KEY)? {
            Some(v) => dec(v.value()),
            None => Ok(Stats::default()),
        })
        .await
    }
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
