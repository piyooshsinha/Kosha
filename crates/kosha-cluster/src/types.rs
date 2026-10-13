use std::fmt;
use std::io::Cursor;

use kosha_engine::{Command, CommandResult};
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

/// A cluster member: where to reach its internal (Raft + chunk) endpoint and
/// its public S3 endpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KoshaNode {
    /// Internal cluster address, `host:port`.
    pub addr: String,
    /// Public S3 address, `host:port` (informational).
    pub api: String,
}

impl fmt::Display for KoshaNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (api {})", self.addr, self.api)
    }
}

/// One Raft entry: a group of commands committed together (group commit).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CommandBatch(pub Vec<Command>);

/// Result of applying one log entry: one result per command in its batch
/// (empty for blank and membership entries).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Response(pub Vec<CommandResult>);

openraft::declare_raft_types!(
    pub TypeConfig:
        D = CommandBatch,
        R = Response,
        NodeId = NodeId,
        Node = KoshaNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type LogId = openraft::LogId<NodeId>;
pub type Entry = openraft::Entry<TypeConfig>;
pub type Vote = openraft::Vote<NodeId>;
pub type StoredMembership = openraft::StoredMembership<NodeId, KoshaNode>;
pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, KoshaNode>;
pub type StorageError = openraft::StorageError<NodeId>;

pub fn storage_err(e: impl std::error::Error + 'static) -> StorageError {
    openraft::StorageIOError::<NodeId>::write(&e).into()
}
