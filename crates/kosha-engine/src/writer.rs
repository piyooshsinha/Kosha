//! Streaming object ingest: chunk, hash, compress and persist while the body
//! is still arriving, with bounded parallelism and bounded memory.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use md5::{Digest, Md5};
use tokio::task::JoinHandle;

use crate::chunker::{Chunker, ChunkerConfig};
use crate::chunkstore::{ChunkStore, PinSet, StoredChunk};
use crate::engine::ChunkPlacement;
use crate::error::Result;
use crate::types::{ChunkId, Segment};

/// Where an upload's chunks go.
pub(crate) enum ChunkSink {
    /// This node's chunk store (pinned until the metadata commit).
    Local(Arc<ChunkStore>),
    /// A cluster replica set (protected by the GC grace period).
    Placed(Arc<dyn ChunkPlacement>),
}

pub struct ObjectWriter {
    sink: ChunkSink,
    chunker: Chunker,
    inline_max: usize,
    parallelism: usize,
    md5: Md5,
    size: u64,
    emitted_any: bool,
    in_flight: VecDeque<JoinHandle<Result<StoredChunk>>>,
    segments: Vec<Segment>,
    stored: HashMap<ChunkId, u64>,
    pins: Arc<PinSet>,
}

/// Everything needed to commit an ingested body to the metadata store.
pub struct WrittenData {
    pub size: u64,
    pub md5: [u8; 16],
    pub segments: Vec<Segment>,
    /// On-disk size of every chunk referenced by `segments`.
    pub stored: HashMap<ChunkId, u64>,
    /// Keeps the chunks alive until the metadata commit references them.
    pub(crate) _pins: Arc<PinSet>,
}

impl WrittenData {
    pub fn md5_hex(&self) -> String {
        hex::encode(self.md5)
    }
}

impl ObjectWriter {
    pub(crate) fn new(
        sink: ChunkSink,
        store: Arc<ChunkStore>,
        cfg: ChunkerConfig,
        inline_max: usize,
        parallelism: usize,
    ) -> Self {
        let pins = Arc::new(PinSet::new(store.clone()));
        ObjectWriter {
            sink,
            chunker: Chunker::new(cfg),
            inline_max,
            parallelism: parallelism.max(1),
            md5: Md5::new(),
            size: 0,
            emitted_any: false,
            in_flight: VecDeque::new(),
            segments: Vec::new(),
            stored: HashMap::new(),
            pins,
        }
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.md5.update(data);
        self.size += data.len() as u64;
        for chunk in self.chunker.push(data) {
            self.submit(chunk).await?;
        }
        Ok(())
    }

    pub async fn finish(mut self) -> Result<WrittenData> {
        let chunker = std::mem::replace(&mut self.chunker, Chunker::new(ChunkerConfig::default()));
        if !self.emitted_any && chunker.buffered() <= self.inline_max {
            let rest = chunker.take_remainder();
            if !rest.is_empty() {
                self.segments.push(Segment::Inline(rest.to_vec()));
            }
        } else {
            for chunk in chunker.finish() {
                self.submit(chunk).await?;
            }
        }
        while let Some(h) = self.in_flight.pop_front() {
            self.complete(h.await??);
        }
        Ok(WrittenData {
            size: self.size,
            md5: self.md5.finalize().into(),
            segments: self.segments,
            stored: self.stored,
            _pins: self.pins,
        })
    }

    async fn submit(&mut self, chunk: Bytes) -> Result<()> {
        self.emitted_any = true;
        if self.in_flight.len() >= self.parallelism {
            let h = self.in_flight.pop_front().expect("non-empty");
            self.complete(h.await??);
        }
        let pins = self.pins.clone();
        let task = match &self.sink {
            ChunkSink::Local(store) => {
                let store = store.clone();
                tokio::spawn(async move {
                    let c = store.put(chunk).await?;
                    // Hand the pin to the PinSet immediately so it is released
                    // even if the writer is dropped before this task is awaited.
                    pins.adopt(c.id);
                    Ok(c)
                })
            }
            ChunkSink::Placed(placement) => {
                let placement = placement.clone();
                tokio::spawn(async move {
                    let (id, chunk) =
                        tokio::task::spawn_blocking(move || (ChunkId::of(&chunk), chunk)).await?;
                    let len = chunk.len() as u32;
                    let stored_len = placement.put(id, chunk).await?;
                    Ok(StoredChunk {
                        id,
                        len,
                        stored_len,
                    })
                })
            }
        };
        self.in_flight.push_back(task);
        Ok(())
    }

    fn complete(&mut self, c: StoredChunk) {
        self.stored.insert(c.id, c.stored_len);
        self.segments.push(Segment::Chunk {
            id: c.id,
            len: c.len,
        });
    }
}
