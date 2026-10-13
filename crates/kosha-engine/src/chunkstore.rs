//! Content-addressed chunk files on local disk.
//!
//! Layout: `<root>/ab/cd/<64 hex chars>`. Each file is one codec byte
//! followed by the (possibly compressed, possibly encrypted) payload. Writes go to a temp file
//! and are renamed into place, so a chunk file is either absent or complete.
//! File I/O runs on the configured [`IoBackend`] (io_uring or threads);
//! hashing and compression run on the blocking pool.
//!
//! Garbage collection safety has two layers:
//!
//! * **Pins** (local uploads): a writer pins a chunk id before checking
//!   whether its file exists, and keeps it pinned until its metadata commit
//!   has recorded a reference.
//! * **Grace period** (remote uploads in a cluster): storing a chunk, or
//!   hitting an existing copy, refreshes the file's mtime. The collector never
//!   deletes a file younger than the grace period, which gives an upload on
//!   another node that long to commit its reference.
//!
//! The collector deletes a file only while holding the pin lock, after
//! confirming the chunk is unpinned, unreferenced and older than the grace
//! period. A periodic sweep applies the same rule to every file, which also
//! reclaims chunks left behind by aborted uploads.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::crypt::{ChunkKeys, ENCRYPTED, check_unencrypted};
use crate::durability::{GroupSync, SyncMode, sync_dir};
use crate::error::{Error, Result};
use crate::io::IoBackend;
use crate::types::ChunkId;

const CODEC_RAW: u8 = 0;
const CODEC_ZSTD: u8 = 1;

/// Probe size used to decide whether a chunk is worth compressing.
const PROBE: usize = 64 * 1024;

#[derive(Clone)]
pub struct StoreOptions {
    pub sync: SyncMode,
    pub compression_level: i32,
    pub io: IoBackend,
    pub gc_grace: Duration,
    /// Encrypt chunks at rest (and address them with a keyed hash).
    pub keys: Option<Arc<ChunkKeys>>,
}

pub struct ChunkStore {
    root: PathBuf,
    tmp: PathBuf,
    opts: StoreOptions,
    group_sync: Option<GroupSync>,
    pins: Mutex<HashMap<ChunkId, usize>>,
    /// Cold-tier keys to delete (released by committed changes).
    remote_garbage: Mutex<Vec<String>>,
}

/// Result of storing one chunk.
#[derive(Clone, Copy, Debug)]
pub struct StoredChunk {
    pub id: ChunkId,
    pub len: u32,
    /// Bytes this chunk occupies on disk.
    pub stored_len: u64,
}

enum Prepared {
    Exists(StoredChunk),
    Write {
        chunk: StoredChunk,
        tmp: PathBuf,
        path: PathBuf,
        buf: Vec<u8>,
    },
}

impl ChunkStore {
    pub fn open(root: &Path, opts: StoreOptions) -> Result<Self> {
        let tmp = root.join("tmp");
        fs::create_dir_all(&tmp)?;
        // Bind the keyring to this directory (its id key and marker).
        let opts = match &opts.keys {
            Some(k) => StoreOptions {
                keys: Some(Arc::new(k.resolve(root)?)),
                ..opts
            },
            None => {
                check_unencrypted(root)?;
                opts
            }
        };
        // Leftover temp files are from writes that never completed.
        for entry in fs::read_dir(&tmp)? {
            let _ = fs::remove_file(entry?.path());
        }
        let group_sync = match opts.sync {
            SyncMode::Group if cfg!(target_os = "linux") => Some(GroupSync::start(root)?),
            _ => None,
        };
        let opts = match opts.sync {
            // Group sync is unavailable off Linux: fall back to per-file syncs.
            SyncMode::Group if group_sync.is_none() => StoreOptions {
                sync: SyncMode::File,
                ..opts
            },
            _ => opts,
        };
        Ok(ChunkStore {
            root: root.to_path_buf(),
            tmp,
            opts,
            group_sync,
            pins: Mutex::new(HashMap::new()),
            remote_garbage: Mutex::new(Vec::new()),
        })
    }

    /// (durability requests, syncfs calls) for group sync; zeros otherwise.
    pub fn sync_stats(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        match &self.group_sync {
            Some(g) => (g.stats.requests.load(Relaxed), g.stats.syncs.load(Relaxed)),
            None => (0, 0),
        }
    }

    /// The content address of `data`: BLAKE3, keyed when encrypting.
    pub fn id_of(&self, data: &[u8]) -> ChunkId {
        match &self.opts.keys {
            Some(k) => k.chunk_id(data),
            None => ChunkId::of(data),
        }
    }

    pub fn io_name(&self) -> &'static str {
        self.opts.io.name()
    }

    fn path_of(&self, id: &ChunkId) -> PathBuf {
        let h = id.to_hex();
        self.root.join(&h[0..2]).join(&h[2..4]).join(h)
    }

    pub fn pin(&self, id: ChunkId) {
        *self.pins.lock().entry(id).or_default() += 1;
    }

    pub fn unpin(&self, id: &ChunkId) {
        let mut pins = self.pins.lock();
        if let Some(n) = pins.get_mut(id) {
            *n -= 1;
            if *n == 0 {
                pins.remove(id);
            }
        }
    }

    pub fn has(&self, id: &ChunkId) -> bool {
        self.path_of(id).exists()
    }

    fn shard_path(&self, id: &ChunkId, index: u8) -> PathBuf {
        let mut p = self.path_of(id);
        p.set_extension(format!("s{index}"));
        p
    }

    /// Erasure-coded shards of `id` held by this node.
    pub fn shard_indices(&self, id: &ChunkId) -> Vec<u8> {
        let main = self.path_of(id);
        let prefix = format!("{}.s", id.to_hex());
        let Ok(dir) = fs::read_dir(main.parent().expect("chunk path has parent")) else {
            return Vec::new();
        };
        let mut out: Vec<u8> = dir
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_str()?.strip_prefix(&prefix)?.parse().ok())
            .collect();
        out.sort_unstable();
        out
    }

    /// Store one shard (opaque bytes, checked by the caller) durably.
    /// Like a replica, it is protected by the GC grace period until the
    /// upload that wrote it commits.
    pub async fn put_shard(&self, id: ChunkId, index: u8, data: Bytes) -> Result<()> {
        let path = self.shard_path(&id, index);
        if fs::metadata(&path).is_ok() {
            touch(&path);
            return Ok(());
        }
        fs::create_dir_all(path.parent().expect("chunk path has parent"))?;
        let tmp = self.tmp.join(format!(
            "{}.s{index}.{}",
            id.to_hex(),
            uuid::Uuid::new_v4().simple()
        ));
        let prepared = Prepared::Write {
            chunk: StoredChunk {
                id,
                len: 0,
                stored_len: data.len() as u64,
            },
            tmp,
            path,
            buf: data.to_vec(),
        };
        // commit_prepared releases a pin on failure; hold one for symmetry.
        self.pin(id);
        let r = self.commit_prepared(prepared).await;
        if r.is_ok() {
            self.unpin(&id);
        }
        r.map(drop)
    }

    pub async fn get_shard(&self, id: &ChunkId, index: u8) -> Result<Option<Bytes>> {
        match self
            .opts
            .io
            .read_file(self.shard_path(id, index), 64 * 1024)
            .await
        {
            Ok(b) => Ok(Some(Bytes::from(b))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete one shard unless an upload has the chunk pinned.
    pub fn remove_shard(&self, id: &ChunkId, index: u8) -> Result<bool> {
        let pins = self.pins.lock();
        if pins.contains_key(id) {
            return Ok(false);
        }
        let r = match fs::remove_file(self.shard_path(id, index)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        };
        drop(pins);
        r
    }

    /// Remove a chunk's full copy and every shard. Returns whether anything
    /// was removed. The caller holds the pin lock.
    fn remove_all(&self, id: &ChunkId) -> Result<bool> {
        let mut removed = false;
        for path in std::iter::once(self.path_of(id)).chain(
            self.shard_indices(id)
                .into_iter()
                .map(|i| self.shard_path(id, i)),
        ) {
            match fs::remove_file(&path) {
                Ok(()) => removed = true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(removed)
    }

    /// Hash, pin and persist a chunk. The caller owns the pin and must
    /// release it with [`unpin`](Self::unpin) once the reference is committed
    /// (or abandoned).
    pub async fn put(self: &Arc<Self>, data: Bytes) -> Result<StoredChunk> {
        let this = self.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            let id = this.id_of(&data);
            this.pin(id);
            this.prepare(id, &data).inspect_err(|_| this.unpin(&id))
        })
        .await??;
        self.commit_prepared(prepared).await
    }

    /// Store a chunk received from a peer. The content is verified against
    /// `id`; no pin is kept (the grace period protects it until the remote
    /// upload commits).
    pub async fn put_replica(self: &Arc<Self>, id: ChunkId, data: Bytes) -> Result<StoredChunk> {
        let this = self.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            if this.id_of(&data) != id {
                return Err(Error::Corrupt(format!(
                    "replica for {} does not match its address",
                    id.to_hex()
                )));
            }
            this.pin(id);
            this.prepare(id, &data).inspect_err(|_| this.unpin(&id))
        })
        .await??;
        let r = self.commit_prepared(prepared).await;
        self.unpin(&id);
        r
    }

    async fn commit_prepared(&self, prepared: Prepared) -> Result<StoredChunk> {
        match prepared {
            Prepared::Exists(c) => Ok(c),
            Prepared::Write {
                chunk,
                tmp,
                path,
                buf,
            } => {
                let io = &self.opts.io;
                let mode = self.opts.sync;
                let r = async {
                    io.write_file(tmp.clone(), buf, mode == SyncMode::File)
                        .await?;
                    let dir = path.parent().expect("chunk path has parent").to_path_buf();
                    io.rename(tmp.clone(), path).await?;
                    match mode {
                        SyncMode::None => Ok(()),
                        SyncMode::File => tokio::task::spawn_blocking(move || sync_dir(dir))
                            .await
                            .map_err(std::io::Error::other)?,
                        SyncMode::Group => match &self.group_sync {
                            Some(g) => g.sync().await,
                            None => Ok(()),
                        },
                    }
                }
                .await;
                if let Err(e) = r {
                    let _ = fs::remove_file(&tmp);
                    self.unpin(&chunk.id);
                    return Err(e.into());
                }
                Ok(chunk)
            }
        }
    }

    /// CPU-side work: dedup check, compression, directory creation.
    fn prepare(&self, id: ChunkId, data: &[u8]) -> Result<Prepared> {
        let path = self.path_of(&id);
        let len = data.len() as u32;
        if let Ok(md) = fs::metadata(&path) {
            // Dedup hit. Refresh the mtime so the grace period restarts.
            touch(&path);
            return Ok(Prepared::Exists(StoredChunk {
                id,
                len,
                stored_len: md.len(),
            }));
        }
        let buf = self.encode(&id, data)?;
        fs::create_dir_all(path.parent().expect("chunk path has parent"))?;
        Ok(Prepared::Write {
            chunk: StoredChunk {
                id,
                len,
                stored_len: buf.len() as u64,
            },
            tmp: self
                .tmp
                .join(format!("{}.{}", id.to_hex(), uuid::Uuid::new_v4().simple())),
            path,
            buf,
        })
    }

    /// Codec byte + payload. Compress only when it pays off: probe a prefix
    /// with a fast level and skip compression entirely for incompressible
    /// data (media, archives, encrypted blobs), which is most of what object
    /// stores hold.
    fn encode(&self, id: &ChunkId, data: &[u8]) -> Result<Vec<u8>> {
        let plain = self.compress(data)?;
        let Some(keys) = &self.opts.keys else {
            return Ok(plain);
        };
        Ok(keys.seal_chunk(id, plain[0], &plain[1..]))
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let raw = || {
            let mut v = Vec::with_capacity(data.len() + 1);
            v.push(CODEC_RAW);
            v.extend_from_slice(data);
            v
        };
        let level = self.opts.compression_level;
        if level == 0 || data.len() < 512 {
            return Ok(raw());
        }
        let probe = &data[..data.len().min(PROBE)];
        let probed = zstd::bulk::compress(probe, 1)?;
        if probed.len() * 100 > probe.len() * 95 {
            return Ok(raw());
        }
        let full = zstd::bulk::compress(data, level)?;
        if full.len() * 100 > data.len() * 95 {
            return Ok(raw());
        }
        let mut v = Vec::with_capacity(full.len() + 1);
        v.push(CODEC_ZSTD);
        v.extend_from_slice(&full);
        Ok(v)
    }

    /// The keyring, once bound to this store (`None` when unencrypted).
    pub fn keys(&self) -> Option<&Arc<ChunkKeys>> {
        self.opts.keys.as_ref()
    }

    /// The plain stored form (codec byte + payload, compressed but not
    /// encrypted), for erasure coding with per-shard encryption.
    pub fn compress_external(&self, data: &[u8]) -> Result<Vec<u8>> {
        self.compress(data)
    }

    /// Re-encrypt up to `limit` local chunk files that are not under the
    /// primary key. Returns (rewritten, still pending afterwards).
    pub fn rotate_local(&self, limit: usize) -> Result<(usize, usize)> {
        let Some(keys) = self.opts.keys.clone() else {
            return Ok((0, 0));
        };
        if keys.id_key_needs_rotation(&self.root) {
            keys.rotate_id_key(&self.root)?;
        }
        let (mut done, mut pending) = (0, 0);
        for (id, path) in self.chunk_files()? {
            let raw = match fs::read(&path) {
                Ok(r) => r,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            if !keys.needs_rotation(&raw) {
                continue;
            }
            if done >= limit {
                pending += 1;
                continue;
            }
            let fresh = keys.reseal_chunk(&id, &raw)?;
            let tmp = self.tmp.join(format!(
                "{}.rot.{}",
                id.to_hex(),
                uuid::Uuid::new_v4().simple()
            ));
            fs::write(&tmp, &fresh)?;
            fs::File::open(&tmp)?.sync_all()?;
            // Garbage collection may have removed the chunk meanwhile;
            // don't bring it back.
            let pins = self.pins.lock();
            if path.exists() {
                fs::rename(&tmp, &path)?;
            } else {
                let _ = fs::remove_file(&tmp);
            }
            drop(pins);
            done += 1;
        }
        if done > 0 {
            sync_dir(self.root.clone())?;
        }
        Ok((done, pending))
    }

    /// Every full chunk file (not shards) with its id.
    fn chunk_files(&self) -> Result<Vec<(ChunkId, PathBuf)>> {
        let mut out = Vec::new();
        for l1 in fs::read_dir(&self.root)? {
            let l1 = l1?;
            if l1.file_name() == "tmp" || !l1.file_type()?.is_dir() {
                continue;
            }
            for l2 in fs::read_dir(l1.path())? {
                let l2 = l2?;
                if !l2.file_type()?.is_dir() {
                    continue;
                }
                for f in fs::read_dir(l2.path())? {
                    let f = f?;
                    let name = f.file_name();
                    let Some(hex) = name.to_str() else { continue };
                    if let Ok(bytes) = hex::decode(hex)
                        && let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice())
                    {
                        out.push((ChunkId(arr), f.path()));
                    }
                }
            }
        }
        Ok(out)
    }

    /// Every shard file with its chunk id and index.
    pub fn shard_files(&self) -> Result<Vec<(ChunkId, u8)>> {
        let mut out = Vec::new();
        for l1 in fs::read_dir(&self.root)? {
            let l1 = l1?;
            if l1.file_name() == "tmp" || !l1.file_type()?.is_dir() {
                continue;
            }
            for l2 in fs::read_dir(l1.path())? {
                let l2 = l2?;
                if !l2.file_type()?.is_dir() {
                    continue;
                }
                for f in fs::read_dir(l2.path())? {
                    let name = f?.file_name();
                    let Some((hex, idx)) = name.to_str().and_then(|n| n.split_once(".s")) else {
                        continue;
                    };
                    if let (Ok(bytes), Ok(idx)) = (hex::decode(hex), idx.parse::<u8>())
                        && let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice())
                    {
                        out.push((ChunkId(arr), idx));
                    }
                }
            }
        }
        Ok(out)
    }

    /// Replace an existing shard file atomically (rotation, conversion).
    pub async fn replace_shard(&self, id: ChunkId, index: u8, data: Bytes) -> Result<()> {
        let path = self.shard_path(&id, index);
        let tmp = self.tmp.join(format!(
            "{}.s{index}.rep.{}",
            id.to_hex(),
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(path.parent().expect("chunk path has parent"))?;
        fs::write(&tmp, &data)?;
        fs::File::open(&tmp)?.sync_all()?;
        fs::rename(&tmp, &path)?;
        sync_dir(path.parent().expect("chunk path has parent").to_path_buf())?;
        Ok(())
    }

    /// The stored form of a chunk (compressed, and encrypted if enabled), for
    /// copying to the cold tier.
    pub fn encode_external(&self, id: &ChunkId, data: &[u8]) -> Result<Vec<u8>> {
        self.encode(id, data)
    }

    /// Decode and verify a chunk in its stored form (from the cold tier).
    pub fn decode_external(&self, id: ChunkId, len: u32, raw: Vec<u8>) -> Result<Bytes> {
        decode(id, len, raw, self.opts.keys.as_deref())
    }

    pub fn defer_remote_delete(&self, keys: Vec<String>) {
        self.remote_garbage.lock().extend(keys);
    }

    pub fn take_remote_garbage(&self) -> Vec<String> {
        std::mem::take(&mut *self.remote_garbage.lock())
    }

    /// Drop local copies of chunks now served from the cold tier, unless an
    /// upload has them pinned or `still_evicted` says otherwise.
    pub fn evict(
        &self,
        ids: &[ChunkId],
        still_evicted: impl Fn(&ChunkId) -> Result<bool>,
    ) -> Result<usize> {
        let mut removed = 0;
        for id in ids {
            let pins = self.pins.lock();
            if pins.contains_key(id) || !still_evicted(id)? {
                continue;
            }
            if self.remove_all(id)? {
                removed += 1;
            }
            drop(pins);
        }
        Ok(removed)
    }

    /// Read a chunk and verify it against its content address.
    pub async fn get(&self, id: &ChunkId, len: u32) -> Result<Bytes> {
        let raw = self
            .opts
            .io
            .read_file(self.path_of(id), len as usize + 1 + TAG_LEN)
            .await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Error::Corrupt(format!("chunk {} is missing", id.to_hex()))
                } else {
                    Error::Io(e)
                }
            })?;
        let id = *id;
        let keys = self.opts.keys.clone();
        tokio::task::spawn_blocking(move || decode(id, len, raw, keys.as_deref())).await?
    }

    fn old_enough(&self, path: &Path) -> bool {
        if self.opts.gc_grace.is_zero() {
            return true;
        }
        match fs::metadata(path).and_then(|m| m.modified()) {
            Ok(t) => SystemTime::now()
                .duration_since(t)
                .is_ok_and(|age| age >= self.opts.gc_grace),
            Err(_) => true,
        }
    }

    /// Delete chunk files that `is_unreferenced` confirms are garbage,
    /// skipping pinned chunks and chunks inside the grace period.
    pub fn collect(
        &self,
        candidates: &[ChunkId],
        is_unreferenced: impl Fn(&ChunkId) -> Result<bool>,
    ) -> Result<usize> {
        let mut removed = 0;
        for id in candidates {
            let path = self.path_of(id);
            let pins = self.pins.lock();
            let newest = std::iter::once(path.clone())
                .chain(
                    self.shard_indices(id)
                        .into_iter()
                        .map(|i| self.shard_path(id, i)),
                )
                .all(|p| self.old_enough(&p));
            if pins.contains_key(id) || !newest || !is_unreferenced(id)? {
                continue;
            }
            if self.remove_all(id)? {
                removed += 1;
            }
            drop(pins);
        }
        Ok(removed)
    }

    /// Delete this node's copy of a chunk unless an upload has it pinned.
    /// Used by cluster rebalancing once enough other replicas are confirmed.
    pub fn remove_unpinned(&self, id: &ChunkId) -> Result<bool> {
        let pins = self.pins.lock();
        if pins.contains_key(id) {
            return Ok(false);
        }
        let r = match fs::remove_file(self.path_of(id)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        };
        drop(pins);
        r
    }

    /// Walk every chunk file and collect the unreferenced ones (orphans from
    /// aborted uploads, or chunks whose collection was deferred by the grace
    /// period). Returns the number of files removed.
    pub fn sweep(&self, is_unreferenced: impl Fn(&ChunkId) -> Result<bool>) -> Result<usize> {
        let mut candidates = Vec::new();
        for l1 in fs::read_dir(&self.root)? {
            let l1 = l1?;
            if l1.file_name() == "tmp" || !l1.file_type()?.is_dir() {
                continue;
            }
            for l2 in fs::read_dir(l1.path())? {
                let l2 = l2?;
                if !l2.file_type()?.is_dir() {
                    continue;
                }
                for f in fs::read_dir(l2.path())? {
                    let name = f?.file_name();
                    let Some(name) = name.to_str() else { continue };
                    // `<hex>` (full copy) or `<hex>.s<i>` (shard).
                    let hex = name.split('.').next().unwrap_or(name);
                    if let Ok(bytes) = hex::decode(hex)
                        && let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice())
                    {
                        candidates.push(ChunkId(arr));
                    }
                }
            }
        }
        candidates.sort();
        candidates.dedup();
        self.collect(&candidates, is_unreferenced)
    }
}

fn touch(path: &Path) {
    let _ = fs::File::options()
        .write(true)
        .open(path)
        .and_then(|f| f.set_modified(SystemTime::now()));
}

/// Bytes encryption adds: a key tag and the authentication tag.
const TAG_LEN: usize = 4 + 16;

fn decode(id: ChunkId, len: u32, raw: Vec<u8>, keys: Option<&ChunkKeys>) -> Result<Bytes> {
    let (&codec, _) = raw
        .split_first()
        .ok_or_else(|| Error::Corrupt(format!("chunk {} is empty", id.to_hex())))?;
    let (codec, raw) = if codec & ENCRYPTED != 0 {
        let keys = keys.ok_or_else(|| {
            Error::Corrupt(format!(
                "chunk {} is encrypted and no key is configured",
                id.to_hex()
            ))
        })?;
        let (codec, mut plain) = keys.open_chunk(&id, &raw)?;
        plain.insert(0, codec);
        (codec, plain)
    } else {
        (codec, raw)
    };
    let payload = &raw[1..];
    let data = match codec {
        CODEC_RAW => {
            let mut raw = raw;
            raw.remove(0);
            raw
        }
        CODEC_ZSTD => zstd::bulk::decompress(payload, len as usize)?,
        c => {
            return Err(Error::Corrupt(format!(
                "chunk {} has codec {c}",
                id.to_hex()
            )));
        }
    };
    let actual = match keys {
        Some(k) => k.chunk_id(&data),
        None => ChunkId::of(&data),
    };
    if data.len() != len as usize || actual != id {
        return Err(Error::Corrupt(format!(
            "chunk {} failed integrity check",
            id.to_hex()
        )));
    }
    Ok(Bytes::from(data))
}

/// Releases a set of pins when dropped.
pub struct PinSet {
    store: Arc<ChunkStore>,
    ids: Mutex<Vec<ChunkId>>,
}

impl PinSet {
    pub fn new(store: Arc<ChunkStore>) -> Self {
        PinSet {
            store,
            ids: Mutex::new(Vec::new()),
        }
    }

    /// Record a pin that was already taken by [`ChunkStore::put`].
    pub fn adopt(&self, id: ChunkId) {
        self.ids.lock().push(id);
    }
}

impl Drop for PinSet {
    fn drop(&mut self) {
        for id in self.ids.get_mut().drain(..) {
            self.store.unpin(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::IoMode;

    fn store(dir: &Path, level: i32, grace: Duration) -> Arc<ChunkStore> {
        Arc::new(
            ChunkStore::open(
                dir,
                StoreOptions {
                    sync: SyncMode::None,
                    compression_level: level,
                    io: IoBackend::new(IoMode::Auto, 1).unwrap(),
                    gc_grace: grace,
                    keys: None,
                },
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn roundtrip_dedup_and_compression() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), 3, Duration::ZERO);
        let text = Bytes::from(b"kosha ".repeat(20_000));
        let a = store.put(text.clone()).await.unwrap();
        let b = store.put(text.clone()).await.unwrap();
        assert_eq!(a.id, b.id);
        assert!(
            a.stored_len < text.len() as u64 / 10,
            "compressible data shrinks"
        );
        assert_eq!(store.get(&a.id, a.len).await.unwrap(), text);
    }

    #[tokio::test]
    async fn every_sync_mode_roundtrips() {
        for mode in [SyncMode::None, SyncMode::File, SyncMode::Group] {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(
                ChunkStore::open(
                    dir.path(),
                    StoreOptions {
                        sync: mode,
                        compression_level: 0,
                        io: IoBackend::new(IoMode::Auto, 1).unwrap(),
                        gc_grace: Duration::ZERO,
                        keys: None,
                    },
                )
                .unwrap(),
            );
            let data = Bytes::from(vec![9u8; 100_000]);
            let c = store.put(data.clone()).await.unwrap();
            assert_eq!(store.get(&c.id, c.len).await.unwrap(), data, "{mode:?}");
        }
    }

    #[tokio::test]
    async fn detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), 0, Duration::ZERO);
        let c = store
            .put(Bytes::from_static(b"some bytes worth keeping"))
            .await
            .unwrap();
        let path = store.path_of(&c.id);
        let mut raw = fs::read(&path).unwrap();
        raw[3] ^= 0xff;
        fs::write(&path, raw).unwrap();
        assert!(matches!(
            store.get(&c.id, c.len).await,
            Err(Error::Corrupt(_))
        ));
    }

    #[tokio::test]
    async fn pinned_chunks_survive_collection() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), 0, Duration::ZERO);
        let c = store.put(Bytes::from_static(b"pinned")).await.unwrap();
        assert_eq!(store.collect(&[c.id], |_| Ok(true)).unwrap(), 0);
        store.unpin(&c.id);
        assert_eq!(store.collect(&[c.id], |_| Ok(true)).unwrap(), 1);
    }

    #[tokio::test]
    async fn grace_period_and_sweep() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), 0, Duration::from_secs(3600));
        let id = ChunkId::of(b"replica");
        store
            .put_replica(id, Bytes::from_static(b"replica"))
            .await
            .unwrap();
        // Fresh files are protected by the grace period.
        assert_eq!(store.sweep(|_| Ok(true)).unwrap(), 0);
        // Age the file past the grace period.
        let old = SystemTime::now() - Duration::from_secs(7200);
        fs::File::options()
            .write(true)
            .open(store.path_of(&id))
            .unwrap()
            .set_modified(old)
            .unwrap();
        assert_eq!(
            store.sweep(|_| Ok(false)).unwrap(),
            0,
            "referenced chunks stay"
        );
        assert_eq!(store.sweep(|_| Ok(true)).unwrap(), 1);
        assert!(!store.has(&id));
    }

    #[tokio::test]
    async fn rejects_mismatched_replica() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), 0, Duration::ZERO);
        let r = store
            .put_replica(ChunkId::of(b"a"), Bytes::from_static(b"b"))
            .await;
        assert!(matches!(r, Err(Error::Corrupt(_))));
    }
}
