use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// BLAKE3 hash of a chunk's uncompressed content.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChunkId(pub [u8; 32]);

impl ChunkId {
    pub fn of(data: &[u8]) -> Self {
        ChunkId(*blake3::hash(data).as_bytes())
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkId({})", &self.to_hex()[..16])
    }
}

impl Serialize for ChunkId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for ChunkId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let bytes: serde_bytes::ByteBuf = Deserialize::deserialize(d)?;
        let arr: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| serde::de::Error::custom("chunk id must be 32 bytes"))?;
        Ok(ChunkId(arr))
    }
}

/// One piece of an object's body, in order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Segment {
    /// A reference into the content-addressed chunk store.
    Chunk { id: ChunkId, len: u32 },
    /// Small data stored directly in the manifest.
    Inline(#[serde(with = "serde_bytes")] Vec<u8>),
}

impl Segment {
    pub fn len(&self) -> u64 {
        match self {
            Segment::Chunk { len, .. } => *len as u64,
            Segment::Inline(d) => d.len() as u64,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// User-controlled object attributes that are echoed back on GET/HEAD.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ObjectAttrs {
    pub content_type: Option<String>,
    /// Standard HTTP headers stored with the object (Cache-Control,
    /// Content-Disposition, Content-Encoding, Content-Language, Expires).
    pub headers: BTreeMap<String, String>,
    /// `x-amz-meta-*` user metadata, keys without the prefix, lowercase.
    pub user_meta: BTreeMap<String, String>,
}

/// The manifest of a stored object.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub size: u64,
    /// Hex ETag without quotes. MD5 of the body for single-part uploads,
    /// `md5(part md5s)-N` for multipart uploads, matching S3.
    pub etag: String,
    pub last_modified: DateTime<Utc>,
    pub attrs: ObjectAttrs,
    pub segments: Vec<Segment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub created: DateTime<Utc>,
}

/// Summary of one object returned by listings.
#[derive(Clone, Debug)]
pub struct ObjectSummary {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
pub struct ListQuery {
    pub prefix: String,
    pub delimiter: Option<String>,
    /// Exclusive lower bound on returned keys / prefixes.
    pub start_after: Option<String>,
    pub max_keys: usize,
}

#[derive(Clone, Debug, Default)]
pub struct ListPage {
    pub objects: Vec<ObjectSummary>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    /// The last key or common prefix returned; resume listing after it.
    pub next_marker: Option<String>,
}

/// Preconditions evaluated atomically with a write.
#[derive(Clone, Debug, Default)]
pub struct WriteCondition {
    /// `If-None-Match: *` - only write if the key does not exist.
    pub if_none_match_any: bool,
    /// `If-Match: <etag>` - only write if the current ETag matches.
    pub if_match: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadInfo {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub attrs: ObjectAttrs,
    pub initiated: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartInfo {
    pub part_number: u32,
    pub etag: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
    pub segments: Vec<Segment>,
}

/// Storage efficiency counters, maintained transactionally.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Stats {
    pub buckets: u64,
    pub objects: u64,
    /// Sum of object sizes as clients see them.
    pub logical_bytes: u64,
    /// Bytes of unique chunks referenced by objects and in-progress parts
    /// (after dedup, before compression).
    pub unique_bytes: u64,
    /// Bytes actually stored on disk for chunks (after dedup and compression).
    pub physical_bytes: u64,
    pub chunks: u64,
}
