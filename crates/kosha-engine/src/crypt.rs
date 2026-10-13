//! Encryption at rest that keeps global dedup.
//!
//! Two keys are derived from one 32-byte master key:
//!
//! * a BLAKE3 key for chunk ids, so `id = keyed_hash(content)`. Identical
//!   content still gets one id (dedup works), but nobody without the key
//!   can test whether a guessed file is stored by hashing it.
//! * an AES-256-GCM-SIV key for chunk contents. The nonce is taken from the
//!   chunk id, which makes encryption deterministic: replicas of a chunk
//!   are byte-identical, and GCM-SIV stays secure when equal plaintexts
//!   meet equal nonces (they produce equal ciphertexts, which the id
//!   already reveals). Contents are compressed before they are encrypted.
//!
//! The data directory records a fingerprint of the key, so a node never
//! mixes chunks written under different keys (or with and without one).

use std::path::Path;

use aes_gcm_siv::aead::{Aead, KeyInit, Payload};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};

use crate::error::{Error, Result};
use crate::types::ChunkId;

const MARKER: &str = "ENCRYPTION";

fn nonce(id: &ChunkId) -> Nonce {
    let n: [u8; 12] = id.0[..12].try_into().expect("ids are 32 bytes");
    Nonce::from(n)
}

pub struct ChunkKeys {
    id_key: [u8; 32],
    cipher: Aes256GcmSiv,
    fingerprint: String,
}

impl std::fmt::Debug for ChunkKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChunkKeys({})", self.fingerprint)
    }
}

impl ChunkKeys {
    pub fn new(master: &[u8; 32]) -> Self {
        let id_key = blake3::derive_key("kosha 2026 chunk id", master);
        let enc_key = blake3::derive_key("kosha 2026 chunk contents", master);
        let check = blake3::derive_key("kosha 2026 key fingerprint", master);
        ChunkKeys {
            id_key,
            cipher: Aes256GcmSiv::new(&enc_key.into()),
            fingerprint: hex::encode(&check[..8]),
        }
    }

    /// Parse a key file: 64 hex characters, or exactly 32 raw bytes.
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read(path)?;
        let text = String::from_utf8_lossy(&raw);
        let hex_str = text.trim();
        let key: [u8; 32] = if hex_str.len() == 64 {
            hex::decode(hex_str)
                .ok()
                .and_then(|v| v.try_into().ok())
                .ok_or_else(|| Error::InvalidRequest("key file is not valid hex".into()))?
        } else if raw.len() == 32 {
            raw.try_into().expect("length checked")
        } else {
            return Err(Error::InvalidRequest(format!(
                "{}: expected 64 hex characters or 32 raw bytes",
                path.display()
            )));
        };
        Ok(ChunkKeys::new(&key))
    }

    /// Short, non-secret identifier of the key (safe to log and compare).
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn chunk_id(&self, data: &[u8]) -> ChunkId {
        ChunkId(*blake3::keyed_hash(&self.id_key, data).as_bytes())
    }

    /// `aad` binds the codec byte, so it cannot be swapped undetected.
    pub fn seal(&self, id: &ChunkId, aad: u8, data: &[u8]) -> Vec<u8> {
        self.cipher
            .encrypt(
                &nonce(id),
                Payload {
                    msg: data,
                    aad: &[aad],
                },
            )
            .expect("AES-GCM-SIV encryption cannot fail for in-memory buffers")
    }

    pub fn open(&self, id: &ChunkId, aad: u8, data: &[u8]) -> Result<Vec<u8>> {
        self.cipher
            .decrypt(
                &nonce(id),
                Payload {
                    msg: data,
                    aad: &[aad],
                },
            )
            .map_err(|_| {
                Error::Corrupt(format!(
                    "chunk {} failed authenticated decryption",
                    id.to_hex()
                ))
            })
    }
}

/// Check (or, for a new store, record) which key the chunk directory uses.
pub fn check_marker(root: &Path, keys: Option<&ChunkKeys>) -> Result<()> {
    let marker = root.join(MARKER);
    let want = keys.map_or("none", |k| k.fingerprint());
    match std::fs::read_to_string(&marker) {
        Ok(have) if have.trim() == want => Ok(()),
        Ok(have) => {
            let have = have.trim();
            Err(Error::InvalidRequest(match (have, keys) {
                ("none", Some(_)) => {
                    "this data directory was created without encryption; it cannot be enabled on existing data".into()
                }
                (_, None) => format!(
                    "this data directory is encrypted (key {have}); start with its --encryption-key-file"
                ),
                _ => format!(
                    "this data directory is encrypted with key {have}, not {want}"
                ),
            }))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // An existing store from before the marker existed is unencrypted.
            let has_chunks = std::fs::read_dir(root)?.any(|e| {
                e.ok().is_some_and(|e| {
                    let n = e.file_name();
                    n.len() == 2 && e.file_type().is_ok_and(|t| t.is_dir())
                })
            });
            if has_chunks && keys.is_some() {
                return Err(Error::InvalidRequest(
                    "this data directory already holds unencrypted chunks; encryption must be chosen when it is created".into(),
                ));
            }
            std::fs::write(&marker, want)?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_and_authenticated() {
        let k = ChunkKeys::new(&[7; 32]);
        let other = ChunkKeys::new(&[8; 32]);
        let data = b"hello hello hello";
        let id = k.chunk_id(data);
        assert_eq!(id, k.chunk_id(data));
        assert_ne!(id, ChunkId::of(data));
        assert_ne!(id, other.chunk_id(data));
        let a = k.seal(&id, 1, data);
        assert_eq!(a, k.seal(&id, 1, data));
        assert!(!a.windows(5).any(|w| w == b"hello"));
        assert_eq!(k.open(&id, 1, &a).unwrap(), data);
        assert!(k.open(&id, 0, &a).is_err());
        assert!(other.open(&id, 1, &a).is_err());
        let mut tampered = a.clone();
        tampered[0] ^= 1;
        assert!(k.open(&id, 1, &tampered).is_err());
    }

    #[test]
    fn marker_refuses_key_changes() {
        let dir = tempfile::tempdir().unwrap();
        let k = ChunkKeys::new(&[1; 32]);
        check_marker(dir.path(), Some(&k)).unwrap();
        check_marker(dir.path(), Some(&k)).unwrap();
        assert!(check_marker(dir.path(), None).is_err());
        assert!(check_marker(dir.path(), Some(&ChunkKeys::new(&[2; 32]))).is_err());

        let plain = tempfile::tempdir().unwrap();
        std::fs::create_dir(plain.path().join("ab")).unwrap();
        assert!(check_marker(plain.path(), Some(&k)).is_err());
        check_marker(plain.path(), None).unwrap();
        assert!(check_marker(plain.path(), Some(&k)).is_err());
    }
}
