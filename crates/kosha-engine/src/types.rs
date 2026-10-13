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
    /// Object tags (`x-amz-tagging`, PutObjectTagging).
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
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
    /// Version id in versioned buckets (`"null"` for the null version).
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub lock: ObjectLockState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub created: DateTime<Utc>,
    #[serde(default)]
    pub config: BucketConfig,
}

/// Per-bucket configuration, replicated with the bucket record.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BucketConfig {
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub cors: Option<Vec<CorsRule>>,
    #[serde(default)]
    pub versioning: Versioning,
    /// `Some` when Object Lock is enabled for the bucket.
    #[serde(default)]
    pub object_lock: Option<ObjectLockConfig>,
    #[serde(default)]
    pub lifecycle: Option<Vec<LifecycleRule>>,
    /// Bucket policy (IAM JSON). The engine stores it; the S3 layer
    /// validates and evaluates it.
    #[serde(default)]
    pub policy: Option<String>,
    #[serde(default)]
    pub notifications: Option<Vec<NotificationRule>>,
}

/// Send matching events of a bucket to a named target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationRule {
    pub id: String,
    /// Target name (configured on the servers).
    pub target: String,
    /// S3 event types, e.g. `s3:ObjectCreated:*`, `s3:ObjectRemoved:Delete`.
    pub events: Vec<String>,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub suffix: String,
}

impl NotificationRule {
    /// Whether this rule selects event `name` (without the `s3:` prefix)
    /// on `key`.
    pub fn matches(&self, name: &str, key: &str) -> bool {
        key.starts_with(&self.prefix)
            && key.ends_with(&self.suffix)
            && self.events.iter().any(|e| {
                let e = e.strip_prefix("s3:").unwrap_or(e);
                match e.strip_suffix('*') {
                    Some(stem) => name.starts_with(stem),
                    None => e == name,
                }
            })
    }
}

/// One queued notification, in the order the metadata log produced it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Position in the cluster-wide event sequence (unique, increasing).
    pub seq: u64,
    pub target: String,
    pub rule_id: String,
    pub bucket: String,
    pub key: String,
    /// Event type without the `s3:` prefix, e.g. `ObjectCreated:Put`.
    pub name: String,
    pub size: Option<u64>,
    pub etag: Option<String>,
    pub version_id: Option<String>,
    pub time: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Versioning {
    /// Never enabled: one version per key, no version records.
    #[default]
    Unversioned,
    Enabled,
    Suspended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionMode {
    /// Removable or shortenable with `x-amz-bypass-governance-retention`.
    Governance,
    /// Nobody can delete the version or shorten retention until it expires.
    Compliance,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLockConfig {
    pub default_retention: Option<DefaultRetention>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultRetention {
    pub mode: RetentionMode,
    pub days: u32,
}

/// Object Lock state of one object version.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLockState {
    pub retention: Option<Retention>,
    pub legal_hold: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retention {
    pub mode: RetentionMode,
    pub until: DateTime<Utc>,
}

/// One lifecycle rule (a subset of S3's LifecycleConfiguration).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleRule {
    pub id: Option<String>,
    pub enabled: bool,
    pub prefix: String,
    /// All of these tags must be present on an object for the rule to apply.
    pub tags: BTreeMap<String, String>,
    pub expiration_days: Option<u32>,
    pub expiration_date: Option<DateTime<Utc>>,
    pub expired_object_delete_marker: bool,
    pub noncurrent_days: Option<u32>,
    pub abort_incomplete_days: Option<u32>,
}

/// One version of an object, as returned by version listings and lookups.
#[derive(Clone, Debug)]
pub struct VersionInfo {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: DateTime<Utc>,
    /// `None` for a delete marker.
    pub meta: Option<ObjectMeta>,
}

#[derive(Clone, Debug, Default)]
pub struct VersionListQuery {
    pub prefix: String,
    pub delimiter: Option<String>,
    pub key_marker: Option<String>,
    pub version_marker: Option<String>,
    pub max_keys: usize,
}

#[derive(Clone, Debug, Default)]
pub struct VersionPage {
    pub versions: Vec<VersionInfo>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_marker: Option<String>,
}

/// One key (optionally one version) to delete.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteTarget {
    pub key: String,
    pub version_id: Option<String>,
}

/// What happened to one delete target.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DeleteOutcome {
    pub key: String,
    /// Version removed, or delete marker created.
    pub version_id: Option<String>,
    pub delete_marker: bool,
    /// Whether something was there to delete.
    pub existed: bool,
    /// Set when this target was refused (others in the request proceed).
    pub error: Option<crate::meta::Rejection>,
}

/// One cross-origin rule, as in S3's CORSConfiguration.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorsRule {
    pub id: Option<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<u32>,
}

/// A change to one part of a bucket's configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BucketConfigUpdate {
    Tags(BTreeMap<String, String>),
    Cors(Option<Vec<CorsRule>>),
    Versioning(Versioning),
    ObjectLock(ObjectLockConfig),
    Lifecycle(Option<Vec<LifecycleRule>>),
    Policy(Option<String>),
    Notifications(Option<Vec<NotificationRule>>),
}

/// An access key that is not a root key. Its permissions come from its
/// identity policy and from bucket policies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    pub access_key: String,
    pub secret_key: String,
    /// Unique, human-readable; bucket policies refer to users by name.
    pub name: String,
    /// Identity policy (IAM JSON).
    #[serde(default)]
    pub policy: Option<String>,
    #[serde(default = "enabled_default")]
    pub enabled: bool,
    pub created: DateTime<Utc>,
}

fn enabled_default() -> bool {
    true
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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
    /// Explicit Object Lock settings requested at upload creation.
    #[serde(default)]
    pub lock: Option<ObjectLockState>,
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
    /// Notifications discarded because a target's queue was full.
    #[serde(default)]
    pub events_dropped: u64,
}
