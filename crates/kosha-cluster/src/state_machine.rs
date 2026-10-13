//! The Raft state machine is the Kosha metadata store. Applied log id and
//! membership are committed in the same redb transaction as the commands, so
//! the state machine can never be ahead of or behind what it claims.

use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use kosha_engine::{Command, Engine};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{EntryPayload, RaftSnapshotBuilder};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::types::*;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct SmState {
    last_applied: Option<LogId>,
    membership: StoredMembership,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta,
    data: Vec<u8>,
}

#[derive(Clone)]
pub struct StateMachine {
    engine: Engine,
    state: Arc<Mutex<SmState>>,
    snapshot_path: PathBuf,
    snapshot: Arc<Mutex<Option<StoredSnapshot>>>,
}

impl StateMachine {
    pub fn open(engine: Engine, snapshot_path: PathBuf) -> Result<Self, StorageError> {
        let state = match engine.sm_state().map_err(storage_err)? {
            Some(b) => rmp_serde::from_slice(&b).map_err(storage_err)?,
            None => SmState::default(),
        };
        let snapshot = match std::fs::read(&snapshot_path) {
            Ok(b) => Some(rmp_serde::from_slice(&b).map_err(storage_err)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(storage_err(e)),
        };
        Ok(StateMachine {
            engine,
            state: Arc::new(Mutex::new(state)),
            snapshot_path,
            snapshot: Arc::new(Mutex::new(snapshot)),
        })
    }

    fn save_snapshot(&self, snap: &StoredSnapshot) -> Result<(), StorageError> {
        let tmp = self.snapshot_path.with_extension("tmp");
        std::fs::write(&tmp, rmp_serde::to_vec(snap).map_err(storage_err)?).map_err(storage_err)?;
        std::fs::rename(&tmp, &self.snapshot_path).map_err(storage_err)?;
        *self.snapshot.lock() = Some(snap.clone());
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            // openraft may build snapshots concurrently with apply(), so the
            // applied log id is read from the same transaction as the data.
            let (data, raw_state) = this.engine.export_snapshot().map_err(storage_err)?;
            let state: SmState = match raw_state {
                Some(b) => rmp_serde::from_slice(&b).map_err(storage_err)?,
                None => SmState::default(),
            };
            let id = match &state.last_applied {
                Some(l) => format!("{}-{}-{}", l.leader_id.term, l.index, data.len()),
                None => format!("empty-{}", data.len()),
            };
            let snap = StoredSnapshot {
                meta: SnapshotMeta {
                    last_log_id: state.last_applied,
                    last_membership: state.membership.clone(),
                    snapshot_id: id,
                },
                data,
            };
            this.save_snapshot(&snap)?;
            tracing::info!(id = %snap.meta.snapshot_id, bytes = snap.data.len(), "built snapshot");
            Ok(Snapshot {
                meta: snap.meta,
                snapshot: Box::new(Cursor::new(snap.data)),
            })
        })
        .await
        .map_err(storage_err)?
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let s = self.state.lock();
        Ok((s.last_applied, s.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut state = self.state.lock().clone();
        let mut cmds: Vec<Command> = Vec::new();
        // For each entry, the range of `cmds` it contributed.
        let mut slots: Vec<std::ops::Range<usize>> = Vec::new();
        for entry in entries {
            state.last_applied = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => slots.push(cmds.len()..cmds.len()),
                EntryPayload::Normal(CommandBatch(batch)) => {
                    let start = cmds.len();
                    cmds.extend(batch);
                    slots.push(start..cmds.len());
                }
                EntryPayload::Membership(m) => {
                    state.membership = StoredMembership::new(Some(entry.log_id), m);
                    slots.push(cmds.len()..cmds.len());
                }
            }
        }
        if slots.is_empty() {
            return Ok(Vec::new());
        }
        let encoded = rmp_serde::to_vec(&state).map_err(storage_err)?;
        let engine = self.engine.clone();
        let results =
            tokio::task::spawn_blocking(move || engine.apply_committed(cmds, Some(encoded)))
                .await
                .map_err(storage_err)?
                .map_err(storage_err)?;
        *self.state.lock() = state;
        let mut results = results.into_iter();
        Ok(slots
            .into_iter()
            .map(|r| Response(results.by_ref().take(r.len()).collect()))
            .collect())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let data = snapshot.into_inner();
        let state = SmState {
            last_applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
        };
        let engine = self.engine.clone();
        let (d, s) = (
            data.clone(),
            rmp_serde::to_vec(&state).map_err(storage_err)?,
        );
        tokio::task::spawn_blocking(move || {
            engine.import_snapshot(&d)?;
            engine.set_sm_state(s)
        })
        .await
        .map_err(storage_err)?
        .map_err(storage_err)?;
        *self.state.lock() = state;
        self.save_snapshot(&StoredSnapshot {
            meta: meta.clone(),
            data,
        })?;
        tracing::info!(id = %meta.snapshot_id, "installed snapshot");
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError> {
        Ok(self.snapshot.lock().clone().map(|s| Snapshot {
            meta: s.meta,
            snapshot: Box::new(Cursor::new(s.data)),
        }))
    }
}
