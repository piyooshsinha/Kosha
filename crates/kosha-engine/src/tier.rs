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
    ACCESS, CHUNKS, ChunkInfo, Command, CommandOutput, Fail, MetaStore, Rejection, Step,
    TIER_QUEUE, Tx, dec, enc,
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

/// A chunk's id, length and cold-tier key.
pub type RemoteCopy = (ChunkId, u32, String);

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
        auto: bool,
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
                m.auto_tiered = auto;
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
                let was_evicted = c.evicted();
                c.cold_refs = (c.cold_refs + 1).min(c.refs);
                if c.cold_refs == c.refs {
                    if c.remote.is_some() {
                        if !was_evicted {
                            self.stats.physical_bytes -= c.stored;
                        }
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
        if self.transition(bucket, vec![target], &class, false)? == 1 {
            meta.storage_class = Some(class);
        }
        Ok(meta)
    }

    /// Return auto-tiered objects to STANDARD. Their chunks count as hot
    /// again (the caller copied them back to local storage first).
    pub(crate) fn promote(&mut self, bucket: &str, targets: Vec<DeleteTarget>) -> Step<u64> {
        let info = self.bucket_info(bucket)?;
        let mut moved = 0;
        for t in targets {
            let r = self.update_version(&info, &t.key, t.version_id.as_deref(), |m| {
                if m.storage_class.is_none() || !m.auto_tiered {
                    return Err(Fail::Reject(Rejection::InvalidRequest(
                        "not auto-tiered".into(),
                    )));
                }
                m.storage_class = None;
                m.auto_tiered = false;
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
                let Segment::Chunk { id, .. } = seg else {
                    continue;
                };
                let Some(raw) = chunks.get(id.0.as_slice())?.map(|v| v.value().to_vec()) else {
                    continue;
                };
                let mut c: ChunkInfo = dec(&raw)?;
                let was_evicted = c.evicted();
                c.cold_refs = c.cold_refs.saturating_sub(1);
                if was_evicted && !c.evicted() {
                    self.stats.physical_bytes += c.stored;
                }
                chunks.insert(id.0.as_slice(), enc(&c)?.as_slice())?;
                drop(chunks);
                // No longer cold-only: nothing to offload.
                self.txn.open_table(TIER_QUEUE)?.remove(id.0.as_slice())?;
                chunks = self.txn.open_table(CHUNKS)?;
            }
        }
        Ok(moved)
    }

    pub(crate) fn record_access(
        &mut self,
        entries: Vec<(String, String, chrono::DateTime<chrono::Utc>)>,
    ) -> Step<()> {
        let mut t = self.txn.open_table(ACCESS)?;
        let objects = self.txn.open_table(crate::meta::OBJECTS)?;
        for (bucket, key, at) in entries {
            let k = crate::keys::object_key(&bucket, &key);
            // Only for objects that still exist, and never backwards.
            if objects.get(k.as_slice())?.is_none() {
                continue;
            }
            let newer = match t.get(k.as_slice())? {
                Some(v) => dec::<chrono::DateTime<chrono::Utc>>(v.value())? < at,
                None => true,
            };
            if newer {
                t.insert(k.as_slice(), enc(&at)?.as_slice())?;
            }
        }
        Ok(())
    }

    pub(crate) fn chunk_erasure_coded(&mut self, id: ChunkId) -> Step<()> {
        self.txn.open_table(TIER_QUEUE)?.remove(id.0.as_slice())?;
        let mut chunks = self.txn.open_table(CHUNKS)?;
        let Some(raw) = chunks.get(id.0.as_slice())?.map(|v| v.value().to_vec()) else {
            // Freed meanwhile: garbage collection removes the shards.
            return Ok(());
        };
        let mut c: ChunkInfo = dec(&raw)?;
        if !c.erasure {
            c.erasure = true;
            chunks.insert(id.0.as_slice(), enc(&c)?.as_slice())?;
            self.coded.push(id);
        }
        Ok(())
    }

    pub(crate) fn chunk_remote_rekeyed(
        &mut self,
        id: ChunkId,
        old: String,
        new: String,
        tag: Option<[u8; 4]>,
    ) -> Step<()> {
        let mut chunks = self.txn.open_table(CHUNKS)?;
        let current = chunks
            .get(id.0.as_slice())?
            .map(|v| dec::<ChunkInfo>(v.value()))
            .transpose()?;
        match current {
            Some(mut c) if c.remote.as_deref() == Some(old.as_str()) => {
                c.remote = Some(new);
                c.remote_tag = tag;
                chunks.insert(id.0.as_slice(), enc(&c)?.as_slice())?;
                self.remote_garbage.push(old);
            }
            // Freed or changed meanwhile: the new upload is not needed.
            _ => self.remote_garbage.push(new),
        }
        Ok(())
    }

    pub(crate) fn chunk_offloaded(
        &mut self,
        id: ChunkId,
        key: String,
        tag: Option<[u8; 4]>,
    ) -> Step<()> {
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
        c.remote_tag = tag;
        self.stats.cold_chunks += 1;
        self.stats.cold_bytes += c.stored;
        if c.evicted() {
            self.stats.physical_bytes -= c.stored;
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

    pub fn is_erasure_coded(&self, id: &ChunkId) -> Result<bool> {
        Ok(self.chunk_info(id)?.is_some_and(|c| c.erasure))
    }

    /// Whether every node may drop its local copy of `id`.
    pub fn is_evicted(&self, id: &ChunkId) -> Result<bool> {
        Ok(self.chunk_info(id)?.is_some_and(|c| c.evicted()))
    }

    /// The cold-tier key of a chunk, if it was offloaded.
    pub fn remote_key(&self, id: &ChunkId) -> Result<Option<String>> {
        Ok(self.chunk_info(id)?.and_then(|c| c.remote))
    }

    /// When `bucket/key` was last read, as recorded by access tracking.
    pub fn last_access(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(ACCESS)?;
        t.get(crate::keys::object_key(bucket, key).as_slice())?
            .map(|v| dec(v.value()))
            .transpose()
    }

    /// Offloaded chunks whose remote copy is not under `primary`.
    pub fn remote_needing_rotation(
        &self,
        primary: [u8; 4],
        limit: usize,
    ) -> Result<(Vec<RemoteCopy>, usize)> {
        let r = self.db.begin_read()?;
        let t = r.open_table(CHUNKS)?;
        let (mut out, mut total) = (Vec::new(), 0);
        for item in t.iter()? {
            let (k, v) = item?;
            let c: ChunkInfo = dec(v.value())?;
            let Some(key) = c.remote else { continue };
            if c.remote_tag == Some(primary) {
                continue;
            }
            total += 1;
            if out.len() < limit {
                let id: [u8; 32] = k
                    .value()
                    .try_into()
                    .map_err(|_| Error::Internal("malformed chunk id".into()))?;
                out.push((ChunkId(id), c.len, key));
            }
        }
        Ok((out, total))
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

    /// Whether the chunk was converted to erasure-coded shards.
    pub fn chunk_is_erasure_coded(&self, id: &ChunkId) -> bool {
        self.meta().is_erasure_coded(id).unwrap_or(false)
    }

    /// Whether cold data has somewhere cheaper to go: an external cold
    /// store, or erasure-coded shards across the cluster.
    pub fn can_tier(&self) -> bool {
        self.has_cold_store()
            || self
                .placement_cell()
                .get()
                .is_some_and(|p| p.supports_erasure())
    }

    /// Move objects (or specific versions) to a cold storage class.
    pub async fn transition_objects(
        &self,
        bucket: &str,
        targets: Vec<DeleteTarget>,
        class: &str,
    ) -> Result<u64> {
        self.transition_objects_as(bucket, targets, class, false)
            .await
    }

    pub(crate) async fn transition_objects_as(
        &self,
        bucket: &str,
        targets: Vec<DeleteTarget>,
        class: &str,
        auto: bool,
    ) -> Result<u64> {
        if !self.can_tier() {
            return Err(Error::InvalidRequest(
                "no cold tier is configured on this server".into(),
            ));
        }
        match self
            .propose_cmd(Command::TransitionObjects {
                bucket: bucket.into(),
                targets,
                class: class.into(),
                auto,
            })
            .await?
        {
            CommandOutput::Transitioned(n) => Ok(n),
            other => Err(Error::Internal(format!(
                "unexpected command output {other:?}"
            ))),
        }
    }

    /// Note a read of `bucket/key` for access-based tiering. Cheap: reads
    /// are collected in memory and recorded in batches.
    pub fn note_access(&self, bucket: &str, key: &str) {
        self.note_access_at(bucket, key, chrono::Utc::now());
    }

    /// [`note_access`](Self::note_access) with an explicit time.
    pub fn note_access_at(&self, bucket: &str, key: &str, at: chrono::DateTime<chrono::Utc>) {
        let tracked = self
            .bucket_local(bucket)
            .ok()
            .is_some_and(|b| b.config.tiering.as_ref().is_some_and(|r| !r.is_empty()));
        if tracked {
            self.access_log()
                .lock()
                .insert((bucket.to_string(), key.to_string()), at);
        }
    }

    /// Record the reads collected since the last flush (one replicated
    /// command for all of them).
    pub async fn flush_access(&self) -> Result<usize> {
        let entries: Vec<_> = std::mem::take(&mut *self.access_log().lock())
            .into_iter()
            .map(|((b, k), t)| (b, k, t))
            .collect();
        let n = entries.len();
        for batch in entries.chunks(5_000) {
            self.propose_cmd(Command::RecordAccess {
                entries: batch.to_vec(),
            })
            .await?;
        }
        Ok(n)
    }

    pub fn spawn_access_flusher(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(e) = this.flush_access().await {
                    tracing::warn!(error = %e, "recording object reads failed");
                }
            }
        })
    }

    /// Bring an auto-tiered object back to STANDARD: copy its chunks back
    /// to local storage, then commit the change. Returns whether it moved.
    pub async fn promote_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> Result<bool> {
        let meta = match &version_id {
            Some(v) => self.head_version(bucket, key, v).await?.meta,
            None => Some(self.head_object(bucket, key).await?),
        };
        let Some(meta) = meta.filter(|m| m.auto_tiered && m.storage_class.is_some()) else {
            return Ok(false);
        };
        for seg in &meta.segments {
            let Segment::Chunk { id, len } = seg else {
                continue;
            };
            let data = self.fetch_chunk(*id, *len).await?;
            match self.placement_cell().get() {
                Some(p) => {
                    p.put(*id, data).await?;
                }
                None => {
                    self.chunk_store().put_replica(*id, data).await?;
                }
            }
        }
        let n = match self
            .propose_cmd(Command::PromoteObjects {
                bucket: bucket.into(),
                targets: vec![DeleteTarget {
                    key: key.into(),
                    version_id,
                }],
            })
            .await?
        {
            CommandOutput::Transitioned(n) => n,
            other => {
                return Err(Error::Internal(format!(
                    "unexpected command output {other:?}"
                )));
            }
        };
        Ok(n == 1)
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
            return self.run_erasure_coder().await;
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
                let tag = self.chunk_store().keys().map(|k| k.primary_tag());
                self.propose_cmd(Command::ChunkOffloaded { id, key, tag })
                    .await?;
                done += 1;
            }
        }
    }

    /// Without an external cold store, cold chunks become erasure-coded
    /// shards across the cluster instead (when the placement supports it).
    async fn run_erasure_coder(&self) -> Result<usize> {
        let Some(placement) = self
            .placement_cell()
            .get()
            .filter(|p| p.supports_erasure())
            .cloned()
        else {
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
                if !self.meta().is_erasure_coded(&id)? {
                    placement.erasure_code(id, len).await?;
                    done += 1;
                }
                self.propose_cmd(Command::ChunkErasureCoded { id }).await?;
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
