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
use crate::error::{Error, Result};
use crate::io::{IoBackend, IoMode};
use crate::meta::{Command, CommandOutput, CommandResult, MetaStore};
use crate::types::*;
use crate::writer::{ChunkSink, ObjectWriter, WrittenData};

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
    /// Chunk file I/O backend.
    pub io_mode: IoMode,
    /// Number of io_uring rings (each driven by its own thread).
    pub io_rings: usize,
    /// Unreferenced chunk files younger than this are never deleted. Zero is
    /// safe on a single node; clusters need time for remote uploads to commit.
    pub gc_grace: Duration,
}

impl EngineConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        EngineConfig {
            data_dir: data_dir.into(),
            fsync: true,
            compression_level: 3,
            inline_max: 8 * 1024,
            chunking: ChunkerConfig::default(),
            write_parallelism: cpus.min(8),
            read_ahead: 4,
            min_part_size: 5 * 1024 * 1024,
            io_mode: IoMode::Auto,
            io_rings: cpus.min(8),
            gc_grace: Duration::ZERO,
        }
    }
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
}

struct Inner {
    meta: Arc<MetaStore>,
    committer: GroupCommitter,
    store: Arc<ChunkStore>,
    cfg: EngineConfig,
    consensus: OnceLock<Arc<dyn Consensus>>,
    placement: OnceLock<Arc<dyn ChunkPlacement>>,
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
    pub fn open(cfg: EngineConfig) -> Result<Engine> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        let io = IoBackend::new(cfg.io_mode, cfg.io_rings)?;
        let store = ChunkStore::open(
            &cfg.data_dir.join("chunks"),
            StoreOptions {
                fsync: cfg.fsync,
                compression_level: cfg.compression_level,
                io,
                gc_grace: cfg.gc_grace,
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
            }),
        })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.cfg
    }

    /// Name of the active chunk I/O backend ("io_uring" or "threads").
    pub fn io_backend(&self) -> &'static str {
        self.inner.store.io_name()
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
        cmds: Vec<Command>,
        sm_state: Option<Vec<u8>>,
    ) -> Result<Vec<CommandResult>> {
        apply_and_collect(&self.inner.meta, &self.inner.store, cmds, sm_state)
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

    /// A page of referenced chunks (id, length) after `after`, from local
    /// state (no read barrier: callers use it for background maintenance).
    pub fn referenced_chunks(
        &self,
        after: Option<ChunkId>,
        limit: usize,
    ) -> Result<Vec<(ChunkId, u32)>> {
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
        validate_bucket_name(name)?;
        let info = BucketInfo {
            name: name.to_string(),
            created: Utc::now(),
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
        validate_key(key)?;
        let meta = ObjectMeta {
            size: data.size,
            etag: data.md5_hex(),
            last_modified: Utc::now(),
            attrs,
            segments: data.segments.clone(),
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
        let placement = self.inner.placement.get().cloned();
        stream::iter(pieces)
            .map(move |(seg, from, to)| {
                let store = store.clone();
                let placement = placement.clone();
                async move {
                    match seg {
                        Segment::Inline(d) => Ok(Bytes::from(d).slice(from..to)),
                        Segment::Chunk { id, len } => {
                            let b = match placement {
                                Some(p) => p.get(id, len).await?,
                                None => store.get(&id, len).await?,
                            };
                            Ok(b.slice(from..to))
                        }
                    }
                }
            })
            .buffered(self.inner.cfg.read_ahead.max(1))
            .boxed()
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool> {
        Ok(self
            .delete_objects(bucket, vec![key.to_string()])
            .await?
            .pop()
            .unwrap_or(false))
    }

    /// Delete many keys in one atomic commit. Returns which keys existed.
    pub async fn delete_objects(&self, bucket: &str, keys: Vec<String>) -> Result<Vec<bool>> {
        match self
            .propose(Command::DeleteObjects {
                bucket: bucket.into(),
                keys,
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
        validate_key(dst_key)?;
        expect_object(
            self.propose(Command::CopyObject {
                src_bucket: src_bucket.into(),
                src_key: src_key.into(),
                dst_bucket: dst_bucket.into(),
                dst_key: dst_key.into(),
                attrs: new_attrs,
                now: Utc::now(),
                cond,
            })
            .await?,
        )
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
        validate_key(key)?;
        let info = UploadInfo {
            upload_id: uuid::Uuid::new_v4().simple().to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            attrs,
            initiated: Utc::now(),
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
