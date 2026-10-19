//! Recent write flows, for the console's data-flow view.
//!
//! The node that receives a write records where each of its chunks was
//! stored and how its metadata command reached the Raft log (directly as
//! leader, or forwarded), with timings. Nothing here affects the write
//! path's behaviour: it is a bounded in-memory log per node, and the
//! console merges the logs of all members.

use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use kosha_engine::{ChunkId, Command, CommandOutput, CommandResult, Segment};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Flows kept per node.
const MAX_FLOWS: usize = 200;
/// Chunk placements kept while waiting for their command.
const MAX_CHUNKS: usize = 8192;
/// Chunks listed per flow (a 1 GiB object has ~1000).
const MAX_CHUNKS_PER_FLOW: usize = 64;

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Where one chunk's bytes went.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkTrace {
    pub id: String,
    pub len: u64,
    /// On-disk size after compression (and encryption).
    pub stored: u64,
    /// Nodes holding a full replica.
    pub replicas: Vec<NodeId>,
    /// Preferred replicas that failed (the next-ranked node took over).
    pub failed: Vec<NodeId>,
    /// Erasure coding: `(shard index, node)` and the layout, e.g. "4+2".
    pub shards: Vec<(u8, NodeId)>,
    pub layout: Option<String>,
    pub start_ms: u64,
    pub end_ms: u64,
}

/// One write, as seen by the node that received it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Flow {
    pub node: NodeId,
    pub seq: u64,
    pub op: String,
    pub bucket: String,
    pub key: String,
    pub size: u64,
    /// Bytes stored in the manifest itself (objects of 8 KiB or less).
    pub inline_bytes: u64,
    pub chunk_count: usize,
    /// Chunks referenced by the object (the first `MAX_CHUNKS_PER_FLOW`).
    pub chunks: Vec<ChunkTrace>,
    /// Chunks referenced but not placed by this write (an existing object's
    /// data, as in CopyObject or CompleteUpload, which reuse stored parts).
    pub reused_chunks: usize,
    /// Leader that committed the entry, and whether this node forwarded it.
    pub leader: Option<NodeId>,
    pub forwarded: bool,
    pub log_index: Option<u64>,
    /// Commands in the same Raft entry (group commit).
    pub batch: usize,
    /// When this node began storing the object's first chunk (or proposed
    /// the command, for writes without new chunks).
    pub started_ms: u64,
    pub proposed_ms: u64,
    pub committed_ms: u64,
    pub error: Option<String>,
}

/// What the console shows for one node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeFlows {
    pub node: NodeId,
    pub last_applied: Option<u64>,
    pub flows: Vec<Flow>,
}

/// A command worth showing, before it is proposed.
pub(crate) struct Pending {
    op: &'static str,
    bucket: String,
    key: String,
    size: u64,
    inline_bytes: u64,
    chunks: Vec<ChunkId>,
}

#[derive(Default)]
pub(crate) struct FlowLog {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    seq: u64,
    flows: VecDeque<Flow>,
    chunks: HashMap<ChunkId, ChunkTrace>,
    chunk_order: VecDeque<ChunkId>,
}

fn segments(segs: &[Segment]) -> (Vec<ChunkId>, u64) {
    let mut ids = Vec::new();
    let mut inline = 0;
    for s in segs {
        match s {
            Segment::Chunk { id, .. } => ids.push(*id),
            Segment::Inline(d) => inline += d.len() as u64,
        }
    }
    (ids, inline)
}

impl FlowLog {
    pub fn record_chunk(&self, id: ChunkId, trace: ChunkTrace) {
        let mut s = self.state.lock();
        if s.chunks.insert(id, trace).is_none() {
            s.chunk_order.push_back(id);
        }
        while s.chunk_order.len() > MAX_CHUNKS {
            if let Some(old) = s.chunk_order.pop_front() {
                s.chunks.remove(&old);
            }
        }
    }

    /// Summaries of the user-visible writes in a batch (`None` for internal
    /// bookkeeping commands such as access times or event acks).
    pub fn describe(cmds: &[Command]) -> Vec<Option<Pending>> {
        cmds.iter()
            .map(|cmd| {
                let p = |op, bucket: &str, key: &str, size, inline_bytes, chunks| Pending {
                    op,
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    size,
                    inline_bytes,
                    chunks,
                };
                Some(match cmd {
                    Command::CreateBucket { info } => {
                        p("CreateBucket", &info.name, "", 0, 0, vec![])
                    }
                    Command::DeleteBucket { name } => p("DeleteBucket", name, "", 0, 0, vec![]),
                    Command::PutObject {
                        bucket, key, meta, ..
                    } => {
                        let (ids, inline) = segments(&meta.segments);
                        p("PutObject", bucket, key, meta.size, inline, ids)
                    }
                    Command::PutPart {
                        bucket, key, part, ..
                    } => {
                        let (ids, inline) = segments(&part.segments);
                        let key = format!("{key} (part {})", part.part_number);
                        p("UploadPart", bucket, &key, part.size, inline, ids)
                    }
                    Command::CompleteUpload { bucket, key, .. } => {
                        p("CompleteMultipartUpload", bucket, key, 0, 0, vec![])
                    }
                    Command::CopyObject {
                        src_bucket,
                        src_key,
                        dst_bucket,
                        dst_key,
                        ..
                    } => {
                        let key = format!("{dst_key} (from {src_bucket}/{src_key})");
                        p("CopyObject", dst_bucket, &key, 0, 0, vec![])
                    }
                    Command::DeleteObjects {
                        bucket,
                        targets,
                        lifecycle,
                        ..
                    } => {
                        let key = match targets.as_slice() {
                            [one] => one.key.clone(),
                            many => format!("{} objects", many.len()),
                        };
                        let op = if *lifecycle {
                            "LifecycleExpiration"
                        } else {
                            "DeleteObjects"
                        };
                        p(op, bucket, &key, 0, 0, vec![])
                    }
                    _ => return None,
                })
            })
            .collect()
    }

    /// Record the outcome of a proposed batch.
    #[allow(clippy::too_many_arguments)]
    pub fn record_commit(
        &self,
        node: NodeId,
        pending: Vec<Option<Pending>>,
        results: Option<&[CommandResult]>,
        leader: Option<NodeId>,
        forwarded: bool,
        log_index: Option<u64>,
        proposed_ms: u64,
        error: Option<String>,
    ) {
        let committed_ms = now_ms();
        let batch = pending.len();
        let mut s = self.state.lock();
        for (i, p) in pending.into_iter().enumerate() {
            let Some(p) = p else { continue };
            let result = results.and_then(|r| r.get(i));
            // CompleteUpload and CopyObject learn the object's layout from
            // the state machine; take it from the result.
            let (mut size, mut ids, mut inline) = (p.size, p.chunks, p.inline_bytes);
            if let Some(Ok(CommandOutput::Object(meta))) = result
                && ids.is_empty()
                && size == 0
            {
                size = meta.size;
                (ids, inline) = segments(&meta.segments);
            }
            let error = error.clone().or_else(|| match result {
                Some(Err(rejection)) => Some(format!("{rejection:?}")),
                _ => None,
            });
            let chunk_count = ids.len();
            let mut chunks = Vec::new();
            let mut reused = 0;
            for id in &ids {
                match s.chunks.get(id) {
                    Some(t) if chunks.len() < MAX_CHUNKS_PER_FLOW => chunks.push(t.clone()),
                    Some(_) => {}
                    None => reused += 1,
                }
            }
            let started_ms = chunks
                .iter()
                .map(|c| c.start_ms)
                .min()
                .unwrap_or(proposed_ms)
                .min(proposed_ms);
            s.seq += 1;
            let flow = Flow {
                node,
                seq: s.seq,
                op: p.op.to_string(),
                bucket: p.bucket,
                key: p.key,
                size,
                inline_bytes: inline,
                chunk_count,
                chunks,
                reused_chunks: reused,
                leader,
                forwarded,
                log_index,
                batch,
                started_ms,
                proposed_ms,
                committed_ms,
                error,
            };
            s.flows.push_back(flow);
            if s.flows.len() > MAX_FLOWS {
                s.flows.pop_front();
            }
        }
    }

    /// The newest `limit` flows, oldest first.
    pub fn latest(&self, limit: usize) -> Vec<Flow> {
        let s = self.state.lock();
        s.flows
            .iter()
            .skip(s.flows.len().saturating_sub(limit))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_engine::{ObjectMeta, WriteCondition};

    fn put(key: &str, ids: &[ChunkId]) -> Command {
        Command::PutObject {
            bucket: "b".into(),
            key: key.into(),
            meta: ObjectMeta {
                size: ids.len() as u64 * 10,
                etag: String::new(),
                last_modified: chrono::Utc::now(),
                attrs: Default::default(),
                segments: ids
                    .iter()
                    .map(|id| Segment::Chunk { id: *id, len: 10 })
                    .collect(),
                version_id: None,
                lock: Default::default(),
                storage_class: None,
                auto_tiered: false,
            },
            stored: vec![],
            cond: WriteCondition::default(),
        }
    }

    fn trace(node: NodeId) -> ChunkTrace {
        ChunkTrace {
            id: "x".into(),
            len: 10,
            stored: 10,
            replicas: vec![node],
            failed: vec![],
            shards: vec![],
            layout: None,
            start_ms: 1,
            end_ms: 2,
        }
    }

    #[test]
    fn joins_chunks_to_their_object() {
        let log = FlowLog::default();
        let (a, b) = (ChunkId([1; 32]), ChunkId([2; 32]));
        log.record_chunk(a, trace(1));
        let cmds = vec![put("k", &[a, b]), Command::RecordAccess { entries: vec![] }];
        let pending = FlowLog::describe(&cmds);
        assert!(pending[1].is_none(), "bookkeeping commands are not shown");
        log.record_commit(1, pending, None, Some(2), true, Some(7), 0, None);
        let flows = log.latest(usize::MAX);
        assert_eq!(flows.len(), 1);
        let f = &flows[0];
        assert_eq!((f.op.as_str(), f.key.as_str()), ("PutObject", "k"));
        assert_eq!((f.chunk_count, f.chunks.len(), f.reused_chunks), (2, 1, 1));
        assert_eq!(
            (f.leader, f.forwarded, f.log_index, f.batch),
            (Some(2), true, Some(7), 2)
        );
    }

    #[test]
    fn bounded() {
        let log = FlowLog::default();
        for i in 0..(MAX_CHUNKS + 10) as u32 {
            let mut id = [0u8; 32];
            id[..4].copy_from_slice(&i.to_le_bytes());
            log.record_chunk(ChunkId(id), trace(1));
        }
        for _ in 0..MAX_FLOWS + 10 {
            log.record_commit(
                1,
                FlowLog::describe(&[put("k", &[])]),
                None,
                None,
                false,
                None,
                0,
                None,
            );
        }
        let s = log.state.lock();
        assert_eq!(s.chunks.len(), MAX_CHUNKS);
        assert_eq!(s.flows.len(), MAX_FLOWS);
    }
}
