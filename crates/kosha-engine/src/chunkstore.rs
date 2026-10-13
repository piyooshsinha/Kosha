//! Content-addressed chunk files on local disk.
//!
//! Layout: `<root>/ab/cd/<64 hex chars>`. Each file is one codec byte
//! followed by the (possibly compressed) payload. Writes go to a temp file
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

use crate::error::{Error, Result};
use crate::io::IoBackend;
use crate::types::ChunkId;

const CODEC_RAW: u8 = 0;
const CODEC_ZSTD: u8 = 1;

/// Probe size used to decide whether a chunk is worth compressing.
const PROBE: usize = 64 * 1024;

#[derive(Clone)]
pub struct StoreOptions {
    pub fsync: bool,
    pub compression_level: i32,
    pub io: IoBackend,
    pub gc_grace: Duration,
}

pub struct ChunkStore {
    root: PathBuf,
    tmp: PathBuf,
    opts: StoreOptions,
    pins: Mutex<HashMap<ChunkId, usize>>,
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
        // Leftover temp files are from writes that never completed.
        for entry in fs::read_dir(&tmp)? {
            let _ = fs::remove_file(entry?.path());
        }
        Ok(ChunkStore {
            root: root.to_path_buf(),
            tmp,
            opts,
            pins: Mutex::new(HashMap::new()),
        })
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

    /// Hash, pin and persist a chunk. The caller owns the pin and must
    /// release it with [`unpin`](Self::unpin) once the reference is committed
    /// (or abandoned).
    pub async fn put(self: &Arc<Self>, data: Bytes) -> Result<StoredChunk> {
        let this = self.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            let id = ChunkId::of(&data);
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
            if ChunkId::of(&data) != id {
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
                let r = async {
                    io.write_file(tmp.clone(), buf, self.opts.fsync).await?;
                    io.rename(tmp.clone(), path).await
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
        let buf = self.encode(data)?;
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
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>> {
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

    /// Read a chunk and verify it against its content address.
    pub async fn get(&self, id: &ChunkId, len: u32) -> Result<Bytes> {
        let raw = self
            .opts
            .io
            .read_file(self.path_of(id), len as usize + 1)
            .await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Error::Corrupt(format!("chunk {} is missing", id.to_hex()))
                } else {
                    Error::Io(e)
                }
            })?;
        let id = *id;
        tokio::task::spawn_blocking(move || decode(id, len, raw)).await?
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
            if pins.contains_key(id) || !self.old_enough(&path) || !is_unreferenced(id)? {
                continue;
            }
            match fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
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
                    let Some(hex) = name.to_str() else { continue };
                    if let Ok(bytes) = hex::decode(hex)
                        && let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice())
                    {
                        candidates.push(ChunkId(arr));
                    }
                }
            }
        }
        self.collect(&candidates, is_unreferenced)
    }
}

fn touch(path: &Path) {
    let _ = fs::File::options()
        .write(true)
        .open(path)
        .and_then(|f| f.set_modified(SystemTime::now()));
}

fn decode(id: ChunkId, len: u32, raw: Vec<u8>) -> Result<Bytes> {
    let (&codec, payload) = raw
        .split_first()
        .ok_or_else(|| Error::Corrupt(format!("chunk {} is empty", id.to_hex())))?;
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
    if data.len() != len as usize || ChunkId::of(&data) != id {
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
                    fsync: false,
                    compression_level: level,
                    io: IoBackend::new(IoMode::Auto, 1).unwrap(),
                    gc_grace: grace,
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
