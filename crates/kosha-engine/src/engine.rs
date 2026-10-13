use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};

use crate::chunker::ChunkerConfig;
use crate::chunkstore::{ChunkStore, StoreOptions};
use crate::commit::{GroupCommitter, apply_and_collect};
use crate::durability::SyncMode;
use crate::error::{Error, Result};
use crate::io::{IoBackend, IoMode};
use crate::meta::{Command, CommandOutput, CommandResult, MetaStore, Request};
use crate::types::*;
use crate::writer::{ChunkSink, ObjectWriter, WrittenData};

const MAX_KEY_LEN: usize = 1024;
const MAX_PART_NUMBER: u32 = 10_000;

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    /// How chunk files are made durable before a write is acknowledged.
    /// Metadata commits are always durable.
    pub sync: SyncMode,
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
    /// Chunk file I/O backend.
    pub io_mode: IoMode,
    /// Number of io_uring rings (each driven by its own thread).
    pub io_rings: usize,
    /// Unreferenced chunk files younger than this are never deleted. Zero is
    /// safe on a single node; clusters need time for remote uploads to commit.
    pub gc_grace: Duration,
    /// Encrypt object data at rest. Small objects are then stored as
    /// (encrypted) chunks instead of inline in their manifests.
    pub encryption: Option<Arc<crate::crypt::ChunkKeys>>,
}

impl EngineConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        EngineConfig {
            data_dir: data_dir.into(),
            sync: SyncMode::Group,
            compression_level: 3,
            inline_max: 8 * 1024,
            chunking: ChunkerConfig::default(),
            write_parallelism: cpus.min(8),
            read_ahead: 4,
            min_part_size: 5 * 1024 * 1024,
            io_mode: IoMode::Auto,
            io_rings: cpus.min(8),
            gc_grace: Duration::ZERO,
            encryption: None,
        }
    }
}

/// Where a copy reads from.
#[derive(Clone, Debug)]
pub struct CopySource {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

/// Counters describing how well writes are being batched.
#[derive(Clone, Copy, Debug, Default)]
pub struct WriteStats {
    /// Local metadata transactions and the commands they carried (single node).
    pub commit_batches: u64,
    pub commit_commands: u64,
    /// Chunk writes that waited for durability, and syncfs calls that served them.
    pub sync_requests: u64,
    pub sync_calls: u64,
}

/// Orders metadata commands. The default (no consensus attached) applies
/// them directly to the local state machine; a cluster replicates them
/// through Raft first.
pub trait Consensus: Send + Sync + 'static {
    /// Replicate `cmd` and return its result once it is applied locally.
    fn propose(&self, cmd: Command) -> BoxFuture<'static, Result<CommandResult>>;
    /// Wait until the local state machine reflects every write that completed
    /// before this call (linearizable reads).
    fn read_barrier(&self) -> BoxFuture<'static, Result<()>>;
}

/// Decides where chunk replicas live. Without one, chunks are stored locally.
pub trait ChunkPlacement: Send + Sync + 'static {
    /// Store the chunk durably on its replica set; returns its on-disk size.
    fn put(&self, id: ChunkId, data: Bytes) -> BoxFuture<'static, Result<u64>>;
    /// Fetch a chunk from wherever a replica lives.
    fn get(&self, id: ChunkId, len: u32) -> BoxFuture<'static, Result<Bytes>>;
    /// Re-encrypt up to `limit` of this node's shards under the primary key.
    /// Returns (rewritten, pending, legacy shards that cannot be).
    fn rotate_shards(&self, _limit: usize) -> BoxFuture<'static, Result<(usize, usize, usize)>> {
        Box::pin(async { Ok((0, 0, 0)) })
    }
    /// Whether cold chunks can be converted to erasure-coded shards.
    fn supports_erasure(&self) -> bool {
        false
    }
    /// Store a replicated chunk as erasure-coded shards as well. Once this
    /// returns, the full copies may be dropped.
    fn erasure_code(&self, id: ChunkId, _len: u32) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            Err(Error::Internal(format!(
                "chunk {}: erasure coding is not available",
                id.to_hex()
            )))
        })
    }
}

struct Inner {
    meta: Arc<MetaStore>,
    committer: GroupCommitter,
    store: Arc<ChunkStore>,
    cfg: EngineConfig,
    consensus: OnceLock<Arc<dyn Consensus>>,
    placement: OnceLock<Arc<dyn ChunkPlacement>>,
    cold: OnceLock<Arc<dyn crate::tier::ColdStore>>,
    rotation: crate::rotation::RotationCell,
    /// Reads not yet recorded, for access-based tiering.
    access_log:
        parking_lot::Mutex<std::collections::HashMap<(String, String), chrono::DateTime<Utc>>>,
}

/// The storage engine. Cheap to clone; all clones share state.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
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
    // NUL separates key from version sequence in the versions table.
    if key.contains('\0') {
        return Err(Error::InvalidKey("key must not contain NUL".into()));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(Error::InvalidKey("key is longer than 1024 bytes".into()));
    }
    Ok(())
}

fn expect_object(out: CommandOutput) -> Result<ObjectMeta> {
    match out {
        CommandOutput::Object(m) => Ok(m),
        other => Err(Error::Internal(format!(
            "unexpected command output {other:?}"
        ))),
    }
}

impl Engine {
    pub fn open(mut cfg: EngineConfig) -> Result<Engine> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        if cfg.encryption.is_some() {
            // Inline data would sit unencrypted in the metadata.
            cfg.inline_max = 0;
        }
        let io = IoBackend::new(cfg.io_mode, cfg.io_rings)?;
        let store = ChunkStore::open(
            &cfg.data_dir.join("chunks"),
            StoreOptions {
                sync: cfg.sync,
                compression_level: cfg.compression_level,
                io,
                gc_grace: cfg.gc_grace,
                keys: cfg.encryption.clone(),
            },
        )?;
        let meta = Arc::new(MetaStore::open(&cfg.data_dir.join("meta.redb"))?);
        let store = Arc::new(store);
        let committer = GroupCommitter::start(meta.clone(), store.clone())?;
        Ok(Engine {
            inner: Arc::new(Inner {
                meta,
                committer,
                store,
                cfg,
                consensus: OnceLock::new(),
                placement: OnceLock::new(),
                cold: OnceLock::new(),
                access_log: Default::default(),
                rotation: Default::default(),
            }),
        })
    }

    /// The content address of a chunk (keyed BLAKE3 when encrypting).
    pub fn chunk_id(&self, data: &[u8]) -> ChunkId {
        self.inner.store.id_of(data)
    }

    /// Fingerprint of the encryption key, if data is encrypted at rest.
    pub fn encryption_fingerprint(&self) -> Option<&str> {
        self.inner.store.keys().map(|k| k.fingerprint())
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.cfg
    }

    /// Name of the active chunk I/O backend ("io_uring" or "threads").
    pub fn io_backend(&self) -> &'static str {
        self.inner.store.io_name()
    }

    /// Group-commit and group-sync counters since startup.
    pub fn write_stats(&self) -> WriteStats {
        use std::sync::atomic::Ordering::Relaxed;
        let c = &self.inner.committer.stats;
        let (sync_requests, sync_calls) = self.inner.store.sync_stats();
        WriteStats {
            commit_batches: c.0.load(Relaxed),
            commit_commands: c.1.load(Relaxed),
            sync_requests,
            sync_calls,
        }
    }

    /// The local chunk store (used by cluster replication).
    pub fn chunk_store(&self) -> Arc<ChunkStore> {
        self.inner.store.clone()
    }

    /// Route metadata through `consensus` and chunks through `placement`.
    /// Must be called once, before serving requests.
    pub fn attach_cluster(
        &self,
        consensus: Arc<dyn Consensus>,
        placement: Arc<dyn ChunkPlacement>,
    ) -> Result<()> {
        self.inner
            .consensus
            .set(consensus)
            .map_err(|_| Error::Internal("consensus already attached".into()))?;
        self.inner
            .placement
            .set(placement)
            .map_err(|_| Error::Internal("placement already attached".into()))?;
        Ok(())
    }

    // ------------------------------------------------- state machine access

    /// Apply committed commands to the local state machine and collect any
    /// chunks they released. Blocking; called by the consensus layer.
    pub fn apply_committed(
        &self,
        reqs: Vec<Request>,
        sm_state: Option<Vec<u8>>,
    ) -> Result<Vec<CommandResult>> {
        apply_and_collect(&self.inner.meta, &self.inner.store, reqs, sm_state)
    }

    pub fn sm_state(&self) -> Result<Option<Vec<u8>>> {
        self.inner.meta.sm_state()
    }

    pub fn set_sm_state(&self, state: Vec<u8>) -> Result<()> {
        self.inner.meta.set_sm_state(state)
    }

    /// Export metadata plus the consensus state describing it, consistently.
    pub fn export_snapshot(&self) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
        self.inner.meta.export_snapshot()
    }

    pub fn import_snapshot(&self, bytes: &[u8]) -> Result<()> {
        self.inner.meta.import_snapshot(bytes)
    }

    /// A page of referenced chunks (id, length, erasure coded) after
    /// `after`, from local state (no read barrier: callers use it for
    /// background maintenance).
    pub fn referenced_chunks(
        &self,
        after: Option<ChunkId>,
        limit: usize,
    ) -> Result<Vec<(ChunkId, u32, bool)>> {
        self.inner.meta.chunk_page(after, limit)
    }

    pub fn is_chunk_referenced(&self, id: &ChunkId) -> Result<bool> {
        self.inner.meta.is_referenced(id)
    }

    /// Periodically delete unreferenced chunk files (orphans of aborted
    /// uploads and chunks whose collection was deferred by the grace period).
    pub fn spawn_sweeper(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.tick().await;
            loop {
                tick.tick().await;
                let e = this.clone();
                let r = tokio::task::spawn_blocking(move || {
                    let meta = &e.inner.meta;
                    e.inner.store.sweep(|id| Ok(!meta.is_referenced(id)?))
                })
                .await;
                match r {
                    Ok(Ok(n)) if n > 0 => tracing::info!(removed = n, "chunk sweep"),
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => tracing::warn!(error = %e, "chunk sweep failed"),
                    Err(e) => tracing::warn!(error = %e, "chunk sweep panicked"),
                }
            }
        })
    }

    /// Run one sweep now. Returns the number of chunk files removed.
    pub async fn sweep(&self) -> Result<usize> {
        let e = self.clone();
        tokio::task::spawn_blocking(move || {
            let meta = &e.inner.meta;
            e.inner.store.sweep(|id| Ok(!meta.is_referenced(id)?))
        })
        .await?
    }

    async fn propose(&self, cmd: Command) -> Result<CommandOutput> {
        let result = match self.inner.consensus.get() {
            Some(c) => c.propose(cmd).await?,
            None => self.inner.committer.submit(cmd).await?,
        };
        result.map_err(Error::from)
    }

    pub(crate) async fn propose_cmd(&self, cmd: Command) -> Result<CommandOutput> {
        self.propose(cmd).await
    }

    pub(crate) fn meta(&self) -> &MetaStore {
        &self.inner.meta
    }

    pub(crate) fn meta_arc(&self) -> Arc<MetaStore> {
        self.inner.meta.clone()
    }

    pub(crate) fn cold_cell(&self) -> &OnceLock<Arc<dyn crate::tier::ColdStore>> {
        &self.inner.cold
    }

    pub(crate) fn rotation_cell(&self) -> &crate::rotation::RotationCell {
        &self.inner.rotation
    }

    pub(crate) fn access_log(
        &self,
    ) -> &parking_lot::Mutex<std::collections::HashMap<(String, String), chrono::DateTime<Utc>>>
    {
        &self.inner.access_log
    }

    pub(crate) fn placement_cell(&self) -> &OnceLock<Arc<dyn ChunkPlacement>> {
        &self.inner.placement
    }

    pub(crate) async fn query_meta<T: Send + 'static>(
        &self,
        f: impl FnOnce(&MetaStore) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.query(f).await
    }

    /// Read from the local state machine after a linearizability barrier.
    async fn query<T: Send + 'static>(
        &self,
        f: impl FnOnce(&MetaStore) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        if let Some(c) = self.inner.consensus.get() {
            c.read_barrier().await?;
        }
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || f(&inner.meta)).await?
    }

    /// Start ingesting a body. Feed it with [`ObjectWriter::write`], then pass
    /// the result of [`ObjectWriter::finish`] to a commit method.
    pub fn writer(&self) -> ObjectWriter {
        let cfg = &self.inner.cfg;
        let sink = match self.inner.placement.get() {
            Some(p) => ChunkSink::Placed(p.clone()),
            None => ChunkSink::Local(self.inner.store.clone()),
        };
        ObjectWriter::new(
            sink,
            self.inner.store.clone(),
            cfg.chunking,
            cfg.inline_max,
            cfg.write_parallelism,
        )
    }

    // ---------------------------------------------------------------- buckets

    pub async fn create_bucket(&self, name: &str) -> Result<BucketInfo> {
        self.create_bucket_with(name, false).await
    }

    /// Create a bucket, optionally with Object Lock (which implies versioning).
    pub async fn create_bucket_with(&self, name: &str, object_lock: bool) -> Result<BucketInfo> {
        validate_bucket_name(name)?;
        let mut config = BucketConfig::default();
        if object_lock {
            config.versioning = Versioning::Enabled;
            config.object_lock = Some(ObjectLockConfig::default());
        }
        let info = BucketInfo {
            name: name.to_string(),
            created: Utc::now(),
            config,
        };
        self.propose(Command::CreateBucket { info: info.clone() })
            .await?;
        Ok(info)
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<()> {
        self.propose(Command::DeleteBucket { name: name.into() })
            .await
            .map(drop)
    }

    pub async fn set_bucket_config(&self, bucket: &str, update: BucketConfigUpdate) -> Result<()> {
        self.propose(Command::SetBucketConfig {
            bucket: bucket.into(),
            update,
        })
        .await
        .map(drop)
    }

    /// Replace an object's tags; an empty map removes them.
    pub async fn set_object_tags(
        &self,
        bucket: &str,
        key: &str,
        tags: std::collections::BTreeMap<String, String>,
    ) -> Result<ObjectMeta> {
        self.set_version_tags(bucket, key, None, tags).await
    }

    /// Replace the tags of one version (`None` = latest).
    pub async fn set_version_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
        tags: std::collections::BTreeMap<String, String>,
    ) -> Result<ObjectMeta> {
        expect_object(
            self.propose(Command::SetObjectTags {
                bucket: bucket.into(),
                key: key.into(),
                version_id,
                tags,
                now: Some(Utc::now()),
            })
            .await?,
        )
    }

    /// PutObjectRetention / PutObjectLegalHold. `retention: Some(None)`
    /// removes retention (only allowed when that does not weaken a lock).
    pub async fn set_object_lock(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
        retention: Option<Option<Retention>>,
        legal_hold: Option<bool>,
        bypass_governance: bool,
    ) -> Result<ObjectMeta> {
        expect_object(
            self.propose(Command::SetObjectLock {
                bucket: bucket.into(),
                key: key.into(),
                version_id,
                retention,
                legal_hold,
                bypass_governance,
                now: Utc::now(),
            })
            .await?,
        )
    }

    /// Bucket record from local state, without a read barrier. For
    /// configuration lookups (e.g. CORS) where brief staleness is acceptable.
    pub fn bucket_local(&self, name: &str) -> Result<BucketInfo> {
        self.inner.meta.bucket(name)
    }

    pub async fn bucket(&self, name: &str) -> Result<BucketInfo> {
        let name = name.to_string();
        self.query(move |m| m.bucket(&name)).await
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        self.query(|m| m.list_buckets()).await
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
        self.put_object_locked(bucket, key, data, attrs, cond, ObjectLockState::default())
            .await
    }

    /// PutObject with explicit Object Lock settings for the new version.
    pub async fn put_object_locked(
        &self,
        bucket: &str,
        key: &str,
        data: WrittenData,
        attrs: ObjectAttrs,
        cond: WriteCondition,
        lock: ObjectLockState,
    ) -> Result<ObjectMeta> {
        self.put_object_as(bucket, key, data, attrs, cond, lock, None)
            .await
    }

    /// PutObject with Object Lock settings and a storage class. A cold class
    /// moves the new object to the cold tier in the same commit.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_object_as(
        &self,
        bucket: &str,
        key: &str,
        data: WrittenData,
        attrs: ObjectAttrs,
        cond: WriteCondition,
        lock: ObjectLockState,
        storage_class: Option<String>,
    ) -> Result<ObjectMeta> {
        validate_key(key)?;
        self.check_class(storage_class.as_deref())?;
        let meta = ObjectMeta {
            size: data.size,
            etag: data.md5_hex(),
            last_modified: Utc::now(),
            attrs,
            segments: data.segments.clone(),
            version_id: None,
            lock,
            storage_class,
            auto_tiered: false,
        };
        let out = self
            .propose(Command::PutObject {
                bucket: bucket.into(),
                key: key.into(),
                meta,
                stored: data.stored.iter().map(|(k, v)| (*k, *v)).collect(),
                cond,
            })
            .await;
        // Pins (held by `data`) are released only now, after the commit.
        drop(data);
        expect_object(out?)
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMeta> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.query(move |m| m.head_object(&bucket, &key)).await
    }

    /// One specific version (a delete marker has `meta: None`).
    pub async fn head_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<VersionInfo> {
        let (bucket, key, v) = (bucket.to_string(), key.to_string(), version_id.to_string());
        self.query(move |m| m.head_version(&bucket, &key, &v)).await
    }

    /// The id of `key`'s latest version when it is a delete marker.
    pub async fn latest_delete_marker(&self, bucket: &str, key: &str) -> Result<Option<String>> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.query(move |m| m.latest_delete_marker(&bucket, &key))
            .await
    }

    pub async fn list_versions(&self, bucket: &str, q: VersionListQuery) -> Result<VersionPage> {
        let bucket = bucket.to_string();
        self.query(move |m| m.list_versions(&bucket, &q)).await
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
        let engine = self.clone();
        stream::iter(pieces)
            .map(move |(seg, from, to)| {
                let engine = engine.clone();
                async move {
                    match seg {
                        Segment::Inline(d) => Ok(Bytes::from(d).slice(from..to)),
                        Segment::Chunk { id, len } => {
                            Ok(engine.fetch_chunk(id, len).await?.slice(from..to))
                        }
                    }
                }
            })
            .buffered(self.inner.cfg.read_ahead.max(1))
            .boxed()
    }

    /// Delete the current object (or add a delete marker in a versioned
    /// bucket). Returns whether there was an object.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool> {
        Ok(self
            .delete_objects(bucket, vec![key.to_string()])
            .await?
            .pop()
            .unwrap_or(false))
    }

    /// Delete many keys in one atomic commit. Returns which keys existed.
    pub async fn delete_objects(&self, bucket: &str, keys: Vec<String>) -> Result<Vec<bool>> {
        let targets = keys
            .into_iter()
            .map(|key| DeleteTarget {
                key,
                version_id: None,
            })
            .collect();
        Ok(self
            .delete_targets(bucket, targets, false)
            .await?
            .into_iter()
            .map(|o| o.existed)
            .collect())
    }

    /// Delete keys and/or specific versions in one atomic commit. Targets
    /// refused by Object Lock come back with `error` set; the rest proceed.
    pub async fn delete_targets(
        &self,
        bucket: &str,
        targets: Vec<DeleteTarget>,
        bypass_governance: bool,
    ) -> Result<Vec<DeleteOutcome>> {
        self.delete_targets_as(bucket, targets, bypass_governance, false)
            .await
    }

    /// `lifecycle`: the deletion comes from a lifecycle rule.
    pub(crate) async fn delete_targets_as(
        &self,
        bucket: &str,
        targets: Vec<DeleteTarget>,
        bypass_governance: bool,
        lifecycle: bool,
    ) -> Result<Vec<DeleteOutcome>> {
        match self
            .propose(Command::DeleteObjects {
                bucket: bucket.into(),
                targets,
                bypass_governance,
                now: Utc::now(),
                lifecycle,
            })
            .await?
        {
            CommandOutput::Deleted(d) => Ok(d),
            other => Err(Error::Internal(format!(
                "unexpected command output {other:?}"
            ))),
        }
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
        self.copy_object_with(
            CopySource {
                bucket: src_bucket.into(),
                key: src_key.into(),
                version_id: None,
            },
            dst_bucket,
            dst_key,
            new_attrs,
            cond,
            ObjectLockState::default(),
            None,
        )
        .await
    }

    /// Copy a specific source version, with Object Lock settings for the copy.
    #[allow(clippy::too_many_arguments)]
    pub async fn copy_object_with(
        &self,
        src: CopySource,
        dst_bucket: &str,
        dst_key: &str,
        new_attrs: Option<ObjectAttrs>,
        cond: WriteCondition,
        lock: ObjectLockState,
        storage_class: Option<String>,
    ) -> Result<ObjectMeta> {
        validate_key(dst_key)?;
        self.check_class(storage_class.as_deref())?;
        expect_object(
            self.propose(Command::CopyObject {
                src_bucket: src.bucket,
                src_key: src.key,
                src_version: src.version_id,
                lock,
                dst_bucket: dst_bucket.into(),
                dst_key: dst_key.into(),
                attrs: new_attrs,
                now: Utc::now(),
                cond,
                storage_class,
            })
            .await?,
        )
    }

    /// A requested storage class must be a known cold class, with a cold
    /// tier to put it in.
    fn check_class(&self, class: Option<&str>) -> Result<()> {
        match class {
            None => Ok(()),
            Some(c) if !crate::tier::is_cold_class(c) => Err(Error::InvalidRequest(format!(
                "{c} is not a supported storage class"
            ))),
            Some(_) if !self.can_tier() => Err(Error::InvalidRequest(
                "no cold tier is configured on this server".into(),
            )),
            Some(_) => Ok(()),
        }
    }

    pub async fn list_objects(&self, bucket: &str, q: ListQuery) -> Result<ListPage> {
        let bucket = bucket.to_string();
        self.query(move |m| m.list_objects(&bucket, &q)).await
    }

    // -------------------------------------------------------------- multipart

    pub async fn create_upload(
        &self,
        bucket: &str,
        key: &str,
        attrs: ObjectAttrs,
    ) -> Result<UploadInfo> {
        self.create_upload_with(bucket, key, attrs, None, None)
            .await
    }

    /// CreateMultipartUpload with Object Lock settings for the final object.
    pub async fn create_upload_with(
        &self,
        bucket: &str,
        key: &str,
        attrs: ObjectAttrs,
        lock: Option<ObjectLockState>,
        storage_class: Option<String>,
    ) -> Result<UploadInfo> {
        validate_key(key)?;
        self.check_class(storage_class.as_deref())?;
        let info = UploadInfo {
            upload_id: uuid::Uuid::new_v4().simple().to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            attrs,
            initiated: Utc::now(),
            lock,
            storage_class,
        };
        self.propose(Command::CreateUpload { info: info.clone() })
            .await?;
        Ok(info)
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
        let out = self
            .propose(Command::PutPart {
                bucket: bucket.into(),
                key: key.into(),
                upload_id: upload_id.into(),
                part: part.clone(),
                stored: data.stored.iter().map(|(k, v)| (*k, *v)).collect(),
            })
            .await;
        drop(data);
        out?;
        Ok(part)
    }

    /// Stitch parts into an object. Only manifests are concatenated; part
    /// data is never re-read or re-written.
    pub async fn complete_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<(u32, String)>,
        cond: WriteCondition,
    ) -> Result<ObjectMeta> {
        expect_object(
            self.propose(Command::CompleteUpload {
                bucket: bucket.into(),
                key: key.into(),
                upload_id: upload_id.into(),
                parts,
                now: Utc::now(),
                cond,
                min_part_size: self.inner.cfg.min_part_size,
            })
            .await?,
        )
    }

    pub async fn abort_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
        self.propose(Command::AbortUpload {
            bucket: bucket.into(),
            key: key.into(),
            upload_id: upload_id.into(),
        })
        .await
        .map(drop)
    }

    pub async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(UploadInfo, Vec<PartInfo>)> {
        let (bucket, key, upload_id) = (bucket.to_string(), key.to_string(), upload_id.to_string());
        self.query(move |m| m.list_parts(&bucket, &key, &upload_id))
            .await
    }

    pub async fn list_uploads(&self, bucket: &str, prefix: &str) -> Result<Vec<UploadInfo>> {
        let (bucket, prefix) = (bucket.to_string(), prefix.to_string());
        self.query(move |m| m.list_uploads(&bucket, &prefix)).await
    }

    // ------------------------------------------------------------------ admin

    pub async fn stats(&self) -> Result<Stats> {
        self.query(|m| m.stats()).await
    }
}
