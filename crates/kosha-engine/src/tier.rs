//! Cold tier: chunk data offloaded to an external object store.
//!
//! Tiering works on chunks, so dedup is respected. Each chunk counts the
//! references it has from objects in a cold storage class (`cold_refs`).
//! A lifecycle transition (or an upload with a cold storage class) marks an
//! object cold and bumps the count on each of its chunks. A chunk whose
//! references are all cold is queued; the leader copies its stored bytes
//! (already compressed, and encrypted if enabled) to the cold store under a
//! fresh key and commits [`Command::ChunkOffloaded`]. From then on the chunk
//! is *evicted*: every node drops its local copy and reads fetch it from the
//! cold store, verified against its content address like any read.
//!
//! A chunk shared with a hot object keeps its local copies. Remote copies
//! are deleted only by their exact key once nothing references them, so a
//! late delete can never remove a newer upload of the same content.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use redb::{ReadableDatabase, ReadableTable};

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::meta::{
    CHUNKS, ChunkInfo, Command, CommandOutput, Fail, MetaStore, Rejection, Step, TIER_QUEUE, Tx,
    dec, enc,
};
use crate::types::*;

/// An external object store holding cold chunks (implemented over S3 by
/// the server).
pub trait ColdStore: Send + Sync + 'static {
    fn put(&self, key: String, data: Bytes) -> BoxFuture<'static, Result<()>>;
    fn get(&self, key: String) -> BoxFuture<'static, Result<Bytes>>;
    fn delete(&self, key: String) -> BoxFuture<'static, Result<()>>;
}

/// Storage classes that mean "cold tier". Data in any of them is read back
/// transparently (no restore step), like S3's instant-retrieval classes.
pub const COLD_CLASSES: &[&str] = &[
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "GLACIER_IR",
    "GLACIER",
    "DEEP_ARCHIVE",
];

pub fn is_cold_class(class: &str) -> bool {
    COLD_CLASSES.contains(&class)
}

impl Tx<'_> {
    /// Mark each target cold. Missing keys, delete markers and objects that
    /// are already cold are skipped. Returns how many moved.
    pub(crate) fn transition(
        &mut self,
        bucket: &str,
        targets: Vec<DeleteTarget>,
        class: &str,
    ) -> Step<u64> {
        if !is_cold_class(class) {
            return Err(Fail::Reject(Rejection::InvalidRequest(format!(
                "{class} is not a cold storage class"
            ))));
        }
        let info = self.bucket_info(bucket)?;
        let mut moved = 0;
        for t in targets {
            let r = self.update_version(&info, &t.key, t.version_id.as_deref(), |m| {
                if m.storage_class.is_some() {
                    return Err(Fail::Reject(Rejection::InvalidRequest(
                        "already cold".into(),
                    )));
                }
                m.storage_class = Some(class.to_string());
                Ok(())
            });
            let meta = match r {
                Ok(m) => m,
                Err(Fail::Reject(_)) => continue,
                Err(e) => return Err(e),
            };
            moved += 1;
            let mut chunks = self.txn.open_table(CHUNKS)?;
            for seg in &meta.segments {
                let Segment::Chunk { id, len } = seg else {
                    continue;
                };
                let Some(raw) = chunks.get(id.0.as_slice())?.map(|v| v.value().to_vec()) else {
                    continue;
                };
                let mut c: ChunkInfo = dec(&raw)?;
                c.cold_refs = (c.cold_refs + 1).min(c.refs);
                if c.cold_refs == c.refs {
                    if c.remote.is_some() {
                        self.evicted.push(*id);
                    } else {
                        self.txn
                            .open_table(TIER_QUEUE)?
                            .insert(id.0.as_slice(), len.to_be_bytes().as_slice())?;
                    }
                }
                chunks.insert(id.0.as_slice(), enc(&c)?.as_slice())?;
            }
        }
        Ok(moved)
    }

    /// Apply a storage class requested together with a write (PUT, COPY,
    /// CompleteMultipartUpload) to the version just written.
    pub(crate) fn settle_class(
        &mut self,
        bucket: &str,
        key: &str,
        mut meta: ObjectMeta,
        class: Option<String>,
    ) -> Step<ObjectMeta> {
        let Some(class) = class else {
            return Ok(meta);
        };
        let target = DeleteTarget {
            key: key.to_string(),
            version_id: meta.version_id.clone(),
        };
        if self.transition(bucket, vec![target], &class)? == 1 {
            meta.storage_class = Some(class);
        }
        Ok(meta)
    }

    pub(crate) fn chunk_offloaded(&mut self, id: ChunkId, key: String) -> Step<()> {
        self.txn.open_table(TIER_QUEUE)?.remove(id.0.as_slice())?;
        let mut chunks = self.txn.open_table(CHUNKS)?;
        let Some(raw) = chunks.get(id.0.as_slice())?.map(|v| v.value().to_vec()) else {
            // Freed while the upload ran: the remote copy is garbage.
            self.remote_garbage.push(key);
            return Ok(());
        };
        let mut c: ChunkInfo = dec(&raw)?;
        if c.remote.is_some() {
            // Uploaded twice (e.g. across a leader change): keep the first.
            self.remote_garbage.push(key);
            return Ok(());
        }
        c.remote = Some(key);
        self.stats.physical_bytes = self.stats.physical_bytes.saturating_sub(c.stored);
        self.stats.cold_chunks += 1;
        self.stats.cold_bytes += c.stored;
        if c.evicted() {
            self.evicted.push(id);
        }
        chunks.insert(id.0.as_slice(), enc(&c)?.as_slice())?;
        Ok(())
    }
}

impl MetaStore {
    fn chunk_info(&self, id: &ChunkId) -> Result<Option<ChunkInfo>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(CHUNKS)?;
        t.get(id.0.as_slice())?.map(|v| dec(v.value())).transpose()
    }

    /// Whether every node may drop its local copy of `id`.
    pub fn is_evicted(&self, id: &ChunkId) -> Result<bool> {
        Ok(self.chunk_info(id)?.is_some_and(|c| c.evicted()))
    }

    /// The cold-tier key of a chunk, if it was offloaded.
    pub fn remote_key(&self, id: &ChunkId) -> Result<Option<String>> {
        Ok(self.chunk_info(id)?.and_then(|c| c.remote))
    }

    /// Chunks waiting to be offloaded.
    pub fn tier_queue(&self, limit: usize) -> Result<Vec<(ChunkId, u32)>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(TIER_QUEUE)?;
        let mut out = Vec::new();
        for item in t.iter()?.take(limit) {
            let (k, v) = item?;
            let id: [u8; 32] = k
                .value()
                .try_into()
                .map_err(|_| Error::Internal("malformed chunk id".into()))?;
            let len = u32::from_be_bytes(
                v.value()
                    .try_into()
                    .map_err(|_| Error::Internal("malformed tier queue entry".into()))?,
            );
            out.push((ChunkId(id), len));
        }
        Ok(out)
    }
}

impl Engine {
    /// Attach the cold store. Without one, transitions are refused.
    pub fn set_cold_store(&self, store: Arc<dyn ColdStore>) -> Result<()> {
        self.cold_cell()
            .set(store)
            .map_err(|_| Error::Internal("cold store already attached".into()))
    }

    pub fn has_cold_store(&self) -> bool {
        self.cold_cell().get().is_some()
    }

    /// Move objects (or specific versions) to a cold storage class.
    pub async fn transition_objects(
        &self,
        bucket: &str,
        targets: Vec<DeleteTarget>,
        class: &str,
    ) -> Result<u64> {
        if !self.has_cold_store() {
            return Err(Error::InvalidRequest(
                "no cold tier is configured on this server".into(),
            ));
        }
        match self
            .propose_cmd(Command::TransitionObjects {
                bucket: bucket.into(),
                targets,
                class: class.into(),
            })
            .await?
        {
            CommandOutput::Transitioned(n) => Ok(n),
            other => Err(Error::Internal(format!(
                "unexpected command output {other:?}"
            ))),
        }
    }

    /// Read a chunk from this node, a replica, or the cold tier.
    pub(crate) async fn fetch_chunk(&self, id: ChunkId, len: u32) -> Result<Bytes> {
        let local = match self.placement_cell().get() {
            Some(p) => p.get(id, len).await,
            None => self.chunk_store().get(&id, len).await,
        };
        let err = match local {
            Ok(b) => return Ok(b),
            Err(e) => e,
        };
        let Some(key) = self.meta().remote_key(&id)? else {
            return Err(err);
        };
        let cold = self.cold_cell().get().ok_or_else(|| {
            Error::Internal("chunk is in the cold tier, but none is configured".into())
        })?;
        let raw = cold.get(key).await?;
        let store = self.chunk_store().clone();
        tokio::task::spawn_blocking(move || store.decode_external(id, len, raw.to_vec())).await?
    }

    /// Copy queued chunks to the cold tier and delete remote garbage. Returns
    /// how many chunks were offloaded.
    pub async fn run_offloader(&self) -> Result<usize> {
        let Some(cold) = self.cold_cell().get().cloned() else {
            return Ok(0);
        };
        let mut done = 0;
        loop {
            let batch = {
                let meta = self.meta_arc();
                tokio::task::spawn_blocking(move || meta.tier_queue(32)).await??
            };
            if batch.is_empty() {
                return Ok(done);
            }
            for (id, len) in batch {
                let data = self.fetch_chunk(id, len).await?;
                let store = self.chunk_store().clone();
                let encoded =
                    tokio::task::spawn_blocking(move || store.encode_external(&id, &data))
                        .await??;
                let key = format!(
                    "{}.{}",
                    id.to_hex(),
                    &uuid::Uuid::new_v4().simple().to_string()[..8]
                );
                cold.put(key.clone(), Bytes::from(encoded)).await?;
                self.propose_cmd(Command::ChunkOffloaded { id, key })
                    .await?;
                done += 1;
            }
        }
    }

    /// Delete cold-tier copies released by committed changes.
    pub async fn collect_remote_garbage(&self) -> usize {
        let keys = self.chunk_store().take_remote_garbage();
        let Some(cold) = self.cold_cell().get().cloned() else {
            if !keys.is_empty() {
                tracing::warn!(
                    n = keys.len(),
                    "no cold tier configured; cannot delete remote chunks"
                );
            }
            return 0;
        };
        let mut n = 0;
        for key in keys {
            match cold.delete(key.clone()).await {
                Ok(()) => n += 1,
                Err(e) => {
                    tracing::warn!(%key, error = %e, "deleting remote chunk failed; will retry");
                    self.chunk_store().defer_remote_delete(vec![key]);
                }
            }
        }
        n
    }

    /// Offload on the leader (`gate`) and collect remote garbage on every
    /// node, every `every`.
    pub fn spawn_offloader(
        &self,
        every: Duration,
        gate: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                this.collect_remote_garbage().await;
                if !gate() {
                    continue;
                }
                match this.run_offloader().await {
                    Ok(0) => {}
                    Ok(n) => tracing::info!(chunks = n, "offloaded to the cold tier"),
                    Err(e) => tracing::warn!(error = %e, "offloading failed; will retry"),
                }
            }
        })
    }
}
