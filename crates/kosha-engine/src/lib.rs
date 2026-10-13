//! Kosha storage engine.
//!
//! The engine is protocol-agnostic: it knows nothing about HTTP or S3. It
//! stores objects as manifests of content-addressed chunks:
//!
//! * Incoming bytes are split with content-defined chunking (FastCDC), so an
//!   edit in the middle of a large object only produces a few new chunks.
//! * Each chunk is addressed by its BLAKE3 hash and stored once, no matter
//!   how many objects reference it (global dedup).
//! * Chunks are compressed with zstd only when it actually pays off.
//! * Metadata (buckets, manifests, refcounts, multipart state) lives in an
//!   ACID embedded database, so every mutation is a single atomic commit.
//! * Tiny objects are stored inline in their manifest and never touch the
//!   chunk store.

mod chunker;
mod chunkstore;
mod commit;
mod engine;
mod error;
pub mod io;
mod keys;
mod meta;
mod types;
mod writer;

pub use chunker::ChunkerConfig;
pub use chunkstore::{ChunkStore, StoredChunk};
pub use engine::{ChunkPlacement, Consensus, Engine, EngineConfig, validate_bucket_name};
pub use error::{Error, Result};
pub use io::{IoBackend, IoMode};
pub use meta::{Command, CommandOutput, CommandResult, Rejection};
pub use types::*;
pub use writer::{ObjectWriter, WrittenData};
