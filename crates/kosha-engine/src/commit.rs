//! Group commit for the single-node write path.
//!
//! Every metadata mutation is a durable redb transaction, and durable commits
//! are serialized, so one commit per request caps small writes at the
//! storage's fsync rate. The committer owns the write path instead: while one
//! batch is being made durable, new commands queue up, and the next
//! transaction takes all of them, up to [`MAX_BATCH`]. Batches form from load
//! alone. A lone request commits immediately, with no artificial delay.
//!
//! Batching is safe because a rejected command leaves no trace and does not
//! abort the rest of its batch (see `meta.rs`). Callers keep their chunk pins
//! until their reply arrives, which is after the batch commits.

use std::sync::Arc;
use std::sync::mpsc;

use tokio::sync::oneshot;

use crate::chunkstore::ChunkStore;
use crate::error::{Error, Result};
use crate::meta::{Command, CommandResult, MetaStore};

/// Upper bound on commands per transaction, keeping latency bounded.
const MAX_BATCH: usize = 512;

struct Pending {
    cmd: Command,
    reply: oneshot::Sender<Result<CommandResult>>,
}

pub(crate) struct GroupCommitter {
    tx: Option<mpsc::Sender<Pending>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// Apply a batch and collect chunks it released.
pub(crate) fn apply_and_collect(
    meta: &MetaStore,
    store: &ChunkStore,
    cmds: Vec<Command>,
    sm_state: Option<Vec<u8>>,
) -> Result<Vec<CommandResult>> {
    let applied = meta.apply(cmds, sm_state)?;
    if !applied.freed.is_empty() {
        let removed = store.collect(&applied.freed, |id| Ok(!meta.is_referenced(id)?))?;
        tracing::debug!(
            candidates = applied.freed.len(),
            removed,
            "collected chunks"
        );
    }
    Ok(applied.results)
}

impl GroupCommitter {
    pub(crate) fn start(meta: Arc<MetaStore>, store: Arc<ChunkStore>) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<Pending>();
        let thread = std::thread::Builder::new()
            .name("kosha-commit".into())
            .spawn(move || {
                // Ends when every sender (i.e. the engine) is dropped.
                while let Ok(first) = rx.recv() {
                    let mut batch = vec![first];
                    while batch.len() < MAX_BATCH {
                        match rx.try_recv() {
                            Ok(p) => batch.push(p),
                            Err(_) => break,
                        }
                    }
                    let (cmds, replies): (Vec<_>, Vec<_>) =
                        batch.into_iter().map(|p| (p.cmd, p.reply)).unzip();
                    let n = cmds.len();
                    match apply_and_collect(&meta, &store, cmds, None) {
                        Ok(results) => {
                            for (reply, r) in replies.into_iter().zip(results) {
                                let _ = reply.send(Ok(r));
                            }
                        }
                        Err(e) => {
                            tracing::error!(error = %e, batch = n, "group commit failed");
                            let msg = e.to_string();
                            for reply in replies {
                                let _ = reply.send(Err(Error::Meta(msg.clone())));
                            }
                        }
                    }
                }
            })?;
        Ok(GroupCommitter {
            tx: Some(tx),
            thread: Some(thread),
        })
    }

    pub(crate) async fn submit(&self, cmd: Command) -> Result<CommandResult> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .as_ref()
            .expect("sender lives until drop")
            .send(Pending { cmd, reply })
            .map_err(|_| Error::Internal("commit thread stopped".into()))?;
        rx.await
            .map_err(|_| Error::Internal("commit thread dropped the request".into()))?
    }
}

impl Drop for GroupCommitter {
    /// Close the queue and wait for the thread, so the metadata database is
    /// released (and can be reopened) as soon as the engine is dropped.
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
