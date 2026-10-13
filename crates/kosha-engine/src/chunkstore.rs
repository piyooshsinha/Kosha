//! Content-addressed chunk files on local disk.
//!
//! Layout: `<root>/ab/cd/<64 hex chars>`. Each file is one codec byte
//! followed by the (possibly compressed) payload. Writes go to a temp file
//! and are renamed into place, so a chunk file is either absent or complete.
//!
//! Garbage collection safety: a writer *pins* a chunk id before checking
//! whether its file exists, and keeps it pinned until its metadata commit
//! has recorded a reference. The collector only deletes a file while holding
//! the pin lock and after confirming the chunk is both unpinned and
//! unreferenced, so it can never delete a chunk that a concurrent upload is
//! about to reference.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::error::{Error, Result};
use crate::types::ChunkId;

const CODEC_RAW: u8 = 0;
const CODEC_ZSTD: u8 = 1;

/// Probe size used to decide whether a chunk is worth compressing.
const PROBE: usize = 64 * 1024;

pub struct ChunkStore {
    root: PathBuf,
    tmp: PathBuf,
    fsync: bool,
    compression_level: i32,
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

impl ChunkStore {
    pub fn open(root: &Path, fsync: bool, compression_level: i32) -> Result<Self> {
        let tmp = root.join("tmp");
        fs::create_dir_all(&tmp)?;
        // Leftover temp files are from writes that never completed.
        for entry in fs::read_dir(&tmp)? {
            let _ = fs::remove_file(entry?.path());
        }
        Ok(ChunkStore {
            root: root.to_path_buf(),
            tmp,
            fsync,
            compression_level,
            pins: Mutex::new(HashMap::new()),
        })
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

    /// Hash, pin and persist a chunk. The caller owns the pin and must
    /// release it with [`unpin`](Self::unpin) once the reference is committed
    /// (or abandoned).
    pub fn put(&self, data: &[u8]) -> Result<StoredChunk> {
        let id = ChunkId::of(data);
        self.pin(id);
        match self.put_pinned(id, data) {
            Ok(stored_len) => Ok(StoredChunk {
                id,
                len: data.len() as u32,
                stored_len,
            }),
            Err(e) => {
                self.unpin(&id);
                Err(e)
            }
        }
    }

    fn put_pinned(&self, id: ChunkId, data: &[u8]) -> Result<u64> {
        let path = self.path_of(&id);
        if let Ok(md) = fs::metadata(&path) {
            return Ok(md.len()); // dedup hit
        }

        let (codec, payload) = self.encode(data)?;
        let tmp = self
            .tmp
            .join(format!("{}.{}", id.to_hex(), uuid::Uuid::new_v4()));
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&[codec])?;
            f.write_all(&payload)?;
            if self.fsync {
                f.sync_all()?;
            }
        }
        fs::create_dir_all(path.parent().expect("chunk path has parent"))?;
        fs::rename(&tmp, &path)?;
        Ok(1 + payload.len() as u64)
    }

    /// Compress only when it pays off: probe a prefix with a fast level and
    /// skip compression entirely for incompressible data (media, archives,
    /// encrypted blobs), which is most of what object stores hold.
    fn encode<'a>(&self, data: &'a [u8]) -> Result<(u8, std::borrow::Cow<'a, [u8]>)> {
        use std::borrow::Cow;
        if self.compression_level == 0 || data.len() < 512 {
            return Ok((CODEC_RAW, Cow::Borrowed(data)));
        }
        let probe = &data[..data.len().min(PROBE)];
        let probed = zstd::bulk::compress(probe, 1)?;
        if probed.len() * 100 > probe.len() * 95 {
            return Ok((CODEC_RAW, Cow::Borrowed(data)));
        }
        let full = zstd::bulk::compress(data, self.compression_level)?;
        if full.len() * 100 > data.len() * 95 {
            return Ok((CODEC_RAW, Cow::Borrowed(data)));
        }
        Ok((CODEC_ZSTD, Cow::Owned(full)))
    }

    /// Read a chunk and verify it against its content address.
    pub fn get(&self, id: &ChunkId, len: u32) -> Result<Bytes> {
        let raw = fs::read(self.path_of(id)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::Corrupt(format!("chunk {} is missing", id.to_hex()))
            } else {
                Error::Io(e)
            }
        })?;
        let (&codec, payload) = raw
            .split_first()
            .ok_or_else(|| Error::Corrupt(format!("chunk {} is empty", id.to_hex())))?;
        let data = match codec {
            CODEC_RAW => payload.to_vec(),
            CODEC_ZSTD => zstd::bulk::decompress(payload, len as usize)?,
            c => {
                return Err(Error::Corrupt(format!(
                    "chunk {} has codec {c}",
                    id.to_hex()
                )));
            }
        };
        if data.len() != len as usize || ChunkId::of(&data) != *id {
            return Err(Error::Corrupt(format!(
                "chunk {} failed integrity check",
                id.to_hex()
            )));
        }
        Ok(Bytes::from(data))
    }

    /// Delete chunk files that `is_unreferenced` confirms are garbage,
    /// skipping any that an in-flight upload has pinned.
    pub fn collect(
        &self,
        candidates: &[ChunkId],
        is_unreferenced: impl Fn(&ChunkId) -> Result<bool>,
    ) -> Result<usize> {
        let mut removed = 0;
        for id in candidates {
            let pins = self.pins.lock();
            if pins.contains_key(id) || !is_unreferenced(id)? {
                continue;
            }
            match fs::remove_file(self.path_of(id)) {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            drop(pins);
        }
        Ok(removed)
    }
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

    #[test]
    fn roundtrip_dedup_and_compression() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::open(dir.path(), false, 3).unwrap();
        let text = b"kosha ".repeat(20_000);
        let a = store.put(&text).unwrap();
        let b = store.put(&text).unwrap();
        assert_eq!(a.id, b.id);
        assert!(
            a.stored_len < text.len() as u64 / 10,
            "compressible data shrinks"
        );
        assert_eq!(store.get(&a.id, a.len).unwrap().as_ref(), &text[..]);
    }

    #[test]
    fn detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::open(dir.path(), false, 0).unwrap();
        let c = store.put(b"some bytes worth keeping").unwrap();
        let path = store.path_of(&c.id);
        let mut raw = fs::read(&path).unwrap();
        raw[3] ^= 0xff;
        fs::write(&path, raw).unwrap();
        assert!(matches!(store.get(&c.id, c.len), Err(Error::Corrupt(_))));
    }

    #[test]
    fn pinned_chunks_survive_collection() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::open(dir.path(), false, 0).unwrap();
        let c = store.put(b"pinned").unwrap();
        assert_eq!(store.collect(&[c.id], |_| Ok(true)).unwrap(), 0);
        store.unpin(&c.id);
        assert_eq!(store.collect(&[c.id], |_| Ok(true)).unwrap(), 1);
    }
}
