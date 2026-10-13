//! Durable Raft log in redb. Every append is one fsynced transaction, and
//! the flush callback fires only after the commit.

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::types::*;

const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("logs");
const STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("state");

const VOTE: &str = "vote";
const COMMITTED: &str = "committed";
const PURGED: &str = "last_purged";

#[derive(Clone)]
pub struct LogStore {
    db: Arc<Database>,
}

fn enc<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, StorageError> {
    rmp_serde::to_vec(v).map_err(storage_err)
}

fn dec<T: for<'de> serde::Deserialize<'de>>(b: &[u8]) -> Result<T, StorageError> {
    rmp_serde::from_slice(b).map_err(storage_err)
}

impl LogStore {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let db = Database::create(path).map_err(storage_err)?;
        let txn = db.begin_write().map_err(storage_err)?;
        txn.open_table(LOGS).map_err(storage_err)?;
        txn.open_table(STATE).map_err(storage_err)?;
        txn.commit().map_err(storage_err)?;
        Ok(LogStore { db: Arc::new(db) })
    }

    fn get_state<T: for<'de> serde::Deserialize<'de>>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StorageError> {
        let r = self.db.begin_read().map_err(storage_err)?;
        let t = r.open_table(STATE).map_err(storage_err)?;
        match t.get(key).map_err(storage_err)? {
            Some(v) => Ok(Some(dec(v.value())?)),
            None => Ok(None),
        }
    }

    fn put_state<T: serde::Serialize>(&self, key: &str, v: &T) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(storage_err)?;
        txn.open_table(STATE)
            .map_err(storage_err)?
            .insert(key, enc(v)?.as_slice())
            .map_err(storage_err)?;
        txn.commit().map_err(storage_err)
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        let db = self.db.clone();
        let (start, end) = (range.start_bound().cloned(), range.end_bound().cloned());
        tokio::task::spawn_blocking(move || {
            let r = db.begin_read().map_err(storage_err)?;
            let t = r.open_table(LOGS).map_err(storage_err)?;
            let mut out = Vec::new();
            for item in t.range::<u64>((start, end)).map_err(storage_err)? {
                let (_, v) = item.map_err(storage_err)?;
                out.push(dec(v.value())?);
            }
            Ok(out)
        })
        .await
        .map_err(storage_err)?
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let last_purged: Option<LogId> = self.get_state(PURGED)?;
        let r = self.db.begin_read().map_err(storage_err)?;
        let t = r.open_table(LOGS).map_err(storage_err)?;
        let last = match t.last().map_err(storage_err)? {
            Some((_, v)) => Some(dec::<Entry>(v.value())?.log_id),
            None => last_purged,
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError> {
        self.put_state(VOTE, vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        self.get_state(VOTE)
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        self.put_state(COMMITTED, &committed)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        Ok(self.get_state::<Option<LogId>>(COMMITTED)?.flatten())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<Entry> = entries.into_iter().collect();
        let db = self.db.clone();
        let r = tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            let txn = db.begin_write().map_err(storage_err)?;
            {
                let mut t = txn.open_table(LOGS).map_err(storage_err)?;
                for e in &entries {
                    t.insert(e.log_id.index, enc(e)?.as_slice())
                        .map_err(storage_err)?;
                }
            }
            txn.commit().map_err(storage_err)
        })
        .await
        .map_err(storage_err)?;
        callback.log_io_completed(r.clone().map_err(std::io::Error::other));
        r
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        // Delete [log_id.index, ∞): conflicting entries from a stale leader.
        let txn = self.db.begin_write().map_err(storage_err)?;
        {
            let mut t = txn.open_table(LOGS).map_err(storage_err)?;
            t.retain_in(log_id.index.., |_, _| false)
                .map_err(storage_err)?;
        }
        txn.commit().map_err(storage_err)
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        // Delete (-∞, log_id.index]: already captured by a snapshot.
        let txn = self.db.begin_write().map_err(storage_err)?;
        {
            txn.open_table(STATE)
                .map_err(storage_err)?
                .insert(PURGED, enc(&log_id)?.as_slice())
                .map_err(storage_err)?;
            let mut t = txn.open_table(LOGS).map_err(storage_err)?;
            t.retain_in((Bound::Unbounded, Bound::Included(log_id.index)), |_, _| {
                false
            })
            .map_err(storage_err)?;
        }
        txn.commit().map_err(storage_err)
    }
}
