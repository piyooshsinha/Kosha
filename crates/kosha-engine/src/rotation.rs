//! Background key rotation: re-encrypt everything under the primary key.
//!
//! Each node rewrites its own chunk files (and, through the placement, its
//! erasure-coded shards) and re-seals `ID_KEY`; the leader re-uploads
//! cold-tier copies. Once every count reaches zero, old keys can be
//! removed from the configuration.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use serde::Serialize;

use crate::engine::Engine;
use crate::error::Result;
use crate::meta::Command;

/// Work per pass, so rotation does not crowd out client traffic.
const PER_PASS: usize = 2_000;

/// Progress of key rotation on this node.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RotationStatus {
    /// Local chunk files and shards re-encrypted so far.
    pub rewritten: u64,
    /// Local chunk files still under an old key (after the last pass).
    pub chunks_pending: u64,
    /// Local shards still under an old key.
    pub shards_pending: u64,
    /// Shards in the pre-rotation format: readable only with the key that
    /// wrote them, and not re-encrypted (rewrite the objects to move them).
    pub legacy_shards: u64,
    /// Cold-tier copies under an old key (counted on the leader).
    pub remote_pending: u64,
}

impl RotationStatus {
    pub fn done(&self) -> bool {
        self.chunks_pending + self.shards_pending + self.remote_pending == 0
    }
}

impl Engine {
    /// One rotation pass. `leader`: also re-encrypt cold-tier copies.
    pub async fn rotate_keys(&self, leader: bool) -> Result<RotationStatus> {
        let store = self.chunk_store();
        let Some(keys) = store.keys().cloned() else {
            return Ok(RotationStatus::default());
        };
        let mut status = self.rotation_status();
        let s = store.clone();
        let (done, pending) =
            tokio::task::spawn_blocking(move || s.rotate_local(PER_PASS)).await??;
        status.rewritten += done as u64;
        status.chunks_pending = pending as u64;
        if let Some(p) = self.placement_cell().get() {
            let (done, pending, legacy) = p.rotate_shards(PER_PASS).await?;
            status.rewritten += done as u64;
            status.shards_pending = pending as u64;
            status.legacy_shards = legacy as u64;
        }
        if leader && let Some(cold) = self.cold_cell().get().cloned() {
            let meta = self.meta_arc();
            let primary = keys.primary_tag();
            let (batch, total) = tokio::task::spawn_blocking(move || {
                meta.remote_needing_rotation(primary, PER_PASS)
            })
            .await??;
            let mut moved = 0;
            for (id, len, old) in batch {
                let raw = cold.get(old.clone()).await?;
                let st = store.clone();
                let fresh = tokio::task::spawn_blocking(move || {
                    let data = st.decode_external(id, len, raw.to_vec())?;
                    st.encode_external(&id, &data)
                })
                .await??;
                let new = format!(
                    "{}.{}",
                    id.to_hex(),
                    &uuid::Uuid::new_v4().simple().to_string()[..8]
                );
                cold.put(new.clone(), Bytes::from(fresh)).await?;
                self.propose_cmd(Command::ChunkRemoteRekeyed {
                    id,
                    old,
                    new,
                    tag: Some(primary),
                })
                .await?;
                moved += 1;
            }
            status.rewritten += moved as u64;
            status.remote_pending = (total - moved) as u64;
        }
        *self.rotation_cell().lock() = status.clone();
        Ok(status)
    }

    /// Progress as of the last pass.
    pub fn rotation_status(&self) -> RotationStatus {
        self.rotation_cell().lock().clone()
    }

    /// Run rotation passes every `every` while anything is left (they are
    /// cheap no-ops otherwise). `leader` gates cold-tier re-uploads.
    pub fn spawn_rotation(
        &self,
        every: Duration,
        leader: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                match this.rotate_keys(leader()).await {
                    Ok(s) if s.rewritten > 0 && s.done() => {
                        tracing::info!(?s, "key rotation complete on this node")
                    }
                    Ok(s) if !s.done() => tracing::info!(?s, "key rotation in progress"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "key rotation pass failed; will retry"),
                }
            }
        })
    }
}

pub(crate) type RotationCell = Mutex<RotationStatus>;
