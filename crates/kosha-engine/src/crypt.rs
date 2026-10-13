//! Encryption at rest that keeps global dedup, with key rotation.
//!
//! Two kinds of key are involved:
//!
//! * The **id key**, a BLAKE3 key for chunk ids: `id = keyed_hash(content)`.
//!   Identical content still gets one id (dedup works), but nobody without
//!   the key can test whether a guessed file is stored by hashing it. Ids
//!   are in every manifest, so this key never changes. It is derived once,
//!   when the data directory is created, and kept in `chunks/ID_KEY`,
//!   sealed under the current primary key.
//! * **Content keys**, AES-256-GCM-SIV keys for chunk contents, one per
//!   master key in the keyring. New data uses the primary key; older keys
//!   only decrypt. Each encrypted chunk carries the 4-byte tag of its key.
//!   The nonce comes from the chunk id, so encryption is deterministic:
//!   replicas are byte-identical, and GCM-SIV stays secure when equal
//!   plaintexts meet equal nonces (they produce equal ciphertexts, which
//!   the id already reveals). Contents are compressed before encryption.
//!
//! Rotating keys: start every node with the new key as primary and the old
//! one as an old key. Nodes re-encrypt their chunks (and re-seal `ID_KEY`)
//! in the background; once nothing is left on the old key it can be
//! removed.

use std::path::Path;

use aes_gcm_siv::aead::{Aead, KeyInit, Payload};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};

use crate::error::{Error, Result};
use crate::types::ChunkId;

const MARKER: &str = "ENCRYPTION";
const ID_KEY_FILE: &str = "ID_KEY";

/// Set on the codec byte when the payload is encrypted.
pub(crate) const ENCRYPTED: u8 = 0x80;
/// Set together with `ENCRYPTED` when a 4-byte key tag follows the codec.
pub(crate) const TAGGED: u8 = 0x40;

pub type KeyTag = [u8; 4];

fn nonce_of(bytes: &[u8]) -> Nonce {
    let n: [u8; 12] = bytes[..12].try_into().expect("at least 12 bytes");
    Nonce::from(n)
}

fn shard_nonce(id: &ChunkId, index: u8) -> Nonce {
    let mut h = blake3::Hasher::new();
    h.update(b"kosha shard nonce");
    h.update(&id.0);
    h.update(&[index]);
    nonce_of(h.finalize().as_bytes())
}

/// Parse a key file: 64 hex characters, or exactly 32 raw bytes.
pub fn read_key_file(path: &Path) -> Result<[u8; 32]> {
    let raw = std::fs::read(path)?;
    let text = String::from_utf8_lossy(&raw);
    let hex_str = text.trim();
    if hex_str.len() == 64 {
        hex::decode(hex_str)
            .ok()
            .and_then(|v| v.try_into().ok())
            .ok_or_else(|| Error::InvalidRequest("key file is not valid hex".into()))
    } else if raw.len() == 32 {
        Ok(raw.try_into().expect("length checked"))
    } else {
        Err(Error::InvalidRequest(format!(
            "{}: expected 64 hex characters or 32 raw bytes",
            path.display()
        )))
    }
}

#[derive(Clone)]
struct ContentKey {
    tag: KeyTag,
    /// Fingerprint of the master key in the pre-rotation format, used to
    /// recognise data directories created before `ID_KEY` existed.
    legacy_fingerprint: String,
    master: [u8; 32],
    cipher: Aes256GcmSiv,
}

impl ContentKey {
    fn new(master: [u8; 32]) -> Self {
        let enc = blake3::derive_key("kosha 2026 chunk contents", &master);
        let check = blake3::derive_key("kosha 2026 key fingerprint", &master);
        ContentKey {
            tag: check[..4].try_into().expect("4 bytes"),
            legacy_fingerprint: hex::encode(&check[..8]),
            master,
            cipher: Aes256GcmSiv::new(&enc.into()),
        }
    }
}

#[derive(Clone)]
struct IdKey {
    key: [u8; 32],
    fingerprint: String,
    /// Key of the untagged chunks written before tagging existed.
    legacy_tag: Option<KeyTag>,
}

impl IdKey {
    fn derive(master: &[u8; 32], legacy_tag: Option<KeyTag>) -> Self {
        Self::from_key(
            blake3::derive_key("kosha 2026 chunk id", master),
            legacy_tag,
        )
    }

    fn from_key(key: [u8; 32], legacy_tag: Option<KeyTag>) -> Self {
        let fp = blake3::derive_key("kosha 2026 id fingerprint", &key);
        IdKey {
            key,
            fingerprint: hex::encode(&fp[..8]),
            legacy_tag,
        }
    }
}

/// The keyring: the id key plus content keys (the first is the primary).
#[derive(Clone)]
pub struct ChunkKeys {
    keys: Vec<ContentKey>,
    id: IdKey,
    /// Master to derive the id key from when creating a data directory
    /// (defaults to the primary key).
    identity_seed: Option<[u8; 32]>,
}

impl std::fmt::Debug for ChunkKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ChunkKeys(id {}, primary {})",
            self.id.fingerprint,
            hex::encode(self.keys[0].tag)
        )
    }
}

impl ChunkKeys {
    /// A keyring with a single key (which also seeds the id key).
    pub fn new(master: &[u8; 32]) -> Self {
        Self::ring(*master, Vec::new(), None)
    }

    /// `primary` encrypts new data; `old` keys only decrypt. `identity`
    /// seeds the id key of a *new* data directory joining a cluster whose
    /// original key has since been rotated out.
    pub fn ring(primary: [u8; 32], old: Vec<[u8; 32]>, identity: Option<[u8; 32]>) -> Self {
        let mut keys = vec![ContentKey::new(primary)];
        for k in old {
            if keys.iter().all(|e| e.master != k) {
                keys.push(ContentKey::new(k));
            }
        }
        let seed = identity.unwrap_or(primary);
        ChunkKeys {
            id: IdKey::derive(&seed, None),
            keys,
            identity_seed: identity,
        }
    }

    /// Fingerprint of the id key: identical on every node of a cluster and
    /// unchanged by rotation. Safe to log and compare.
    pub fn fingerprint(&self) -> &str {
        &self.id.fingerprint
    }

    pub fn primary_tag(&self) -> KeyTag {
        self.keys[0].tag
    }

    pub fn chunk_id(&self, data: &[u8]) -> ChunkId {
        ChunkId(*blake3::keyed_hash(&self.id.key, data).as_bytes())
    }

    fn key(&self, tag: &KeyTag) -> Result<&ContentKey> {
        self.keys.iter().find(|k| &k.tag == tag).ok_or_else(|| {
            Error::Corrupt(format!(
                "data is encrypted with key {}, which is not configured (add it with --encryption-old-key-file)",
                hex::encode(tag)
            ))
        })
    }

    /// Seal a chunk payload (`codec` is the plain codec byte). Returns the
    /// stored form: codec | key tag | ciphertext.
    pub fn seal_chunk(&self, id: &ChunkId, codec: u8, payload: &[u8]) -> Vec<u8> {
        let k = &self.keys[0];
        let head = [
            codec | ENCRYPTED | TAGGED,
            k.tag[0],
            k.tag[1],
            k.tag[2],
            k.tag[3],
        ];
        let sealed = k
            .cipher
            .encrypt(
                &nonce_of(&id.0),
                Payload {
                    msg: payload,
                    aad: &head,
                },
            )
            .expect("AES-GCM-SIV encryption cannot fail for in-memory buffers");
        let mut out = Vec::with_capacity(head.len() + sealed.len());
        out.extend_from_slice(&head);
        out.extend_from_slice(&sealed);
        out
    }

    /// Open a stored, encrypted chunk. Returns (plain codec, payload).
    pub fn open_chunk(&self, id: &ChunkId, stored: &[u8]) -> Result<(u8, Vec<u8>)> {
        let codec = *stored
            .first()
            .ok_or_else(|| Error::Corrupt("empty chunk".into()))?;
        let (key, aad_len) = if codec & TAGGED != 0 {
            let tag: KeyTag = stored
                .get(1..5)
                .and_then(|t| t.try_into().ok())
                .ok_or_else(|| Error::Corrupt(format!("chunk {} is truncated", id.to_hex())))?;
            (self.key(&tag)?, 5)
        } else {
            // Written before key tags existed: the key that created the
            // data directory.
            let tag = self.id.legacy_tag.ok_or_else(|| {
                Error::Corrupt(format!(
                    "chunk {} has no key tag and no legacy key is known",
                    id.to_hex()
                ))
            })?;
            (self.key(&tag)?, 1)
        };
        let plain = key
            .cipher
            .decrypt(
                &nonce_of(&id.0),
                Payload {
                    msg: &stored[aad_len..],
                    aad: &stored[..aad_len],
                },
            )
            .map_err(|_| {
                Error::Corrupt(format!(
                    "chunk {} failed authenticated decryption",
                    id.to_hex()
                ))
            })?;
        Ok((codec & !(ENCRYPTED | TAGGED), plain))
    }

    /// Whether a stored chunk is encrypted under something other than the
    /// primary key.
    pub fn needs_rotation(&self, stored: &[u8]) -> bool {
        match stored.first() {
            Some(c) if c & ENCRYPTED == 0 => false,
            Some(c) if c & TAGGED != 0 => stored.get(1..5) != Some(&self.keys[0].tag[..]),
            Some(_) => true,
            None => false,
        }
    }

    /// Re-encrypt a stored chunk under the primary key.
    pub fn reseal_chunk(&self, id: &ChunkId, stored: &[u8]) -> Result<Vec<u8>> {
        let (codec, payload) = self.open_chunk(id, stored)?;
        Ok(self.seal_chunk(id, codec, &payload))
    }

    /// Encrypt one erasure-coded shard payload under the primary key.
    pub fn seal_shard(&self, id: &ChunkId, index: u8, payload: &[u8]) -> (KeyTag, Vec<u8>) {
        let k = &self.keys[0];
        let sealed = k
            .cipher
            .encrypt(
                &shard_nonce(id, index),
                Payload {
                    msg: payload,
                    aad: &k.tag,
                },
            )
            .expect("AES-GCM-SIV encryption cannot fail for in-memory buffers");
        (k.tag, sealed)
    }

    pub fn open_shard(
        &self,
        id: &ChunkId,
        index: u8,
        tag: &KeyTag,
        sealed: &[u8],
    ) -> Result<Vec<u8>> {
        self.key(tag)?
            .cipher
            .decrypt(
                &shard_nonce(id, index),
                Payload {
                    msg: sealed,
                    aad: tag,
                },
            )
            .map_err(|_| {
                Error::Corrupt(format!(
                    "shard {index} of chunk {} failed authenticated decryption",
                    id.to_hex()
                ))
            })
    }

    /// Bind the keyring to a data directory: find (or create) its id key,
    /// check the marker, and keep `ID_KEY` sealed under the primary key.
    pub(crate) fn resolve(&self, root: &Path) -> Result<ChunkKeys> {
        let marker = match std::fs::read_to_string(root.join(MARKER)) {
            Ok(m) => Some(m.trim().to_string()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        if marker.as_deref() == Some("none") {
            return Err(Error::InvalidRequest(
                "this data directory was created without encryption; it cannot be enabled on existing data".into(),
            ));
        }
        let id = match std::fs::read(root.join(ID_KEY_FILE)) {
            Ok(raw) => self.open_id_key(&raw)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => match &marker {
                // Created before ID_KEY existed: the marker names the key.
                Some(m) => {
                    let k = self
                        .keys
                        .iter()
                        .find(|k| &k.legacy_fingerprint == m || format!("id:{}", IdKey::derive(&k.master, None).fingerprint) == *m)
                        .ok_or_else(|| {
                            Error::InvalidRequest(format!(
                                "this data directory is encrypted with key {m}; start with that key (as primary or old key)"
                            ))
                        })?;
                    IdKey::derive(&k.master, Some(k.tag))
                }
                None => {
                    if has_chunks(root)? {
                        return Err(Error::InvalidRequest(
                            "this data directory already holds unencrypted chunks; encryption must be chosen when it is created".into(),
                        ));
                    }
                    let seed = self.identity_seed.unwrap_or(self.keys[0].master);
                    IdKey::derive(&seed, None)
                }
            },
            Err(e) => return Err(e.into()),
        };
        let want = format!("id:{}", id.fingerprint);
        if let Some(m) = &marker
            && m.starts_with("id:")
            && *m != want
        {
            return Err(Error::InvalidRequest(format!(
                "this data directory belongs to encryption identity {}, not {}",
                &m[3..],
                id.fingerprint
            )));
        }
        let resolved = ChunkKeys {
            keys: self.keys.clone(),
            id,
            identity_seed: None,
        };
        resolved.store_id_key(root)?;
        if marker.as_deref() != Some(want.as_str()) {
            write_atomic(&root.join(MARKER), want.as_bytes())?;
        }
        Ok(resolved)
    }

    fn open_id_key(&self, raw: &[u8]) -> Result<IdKey> {
        if raw.len() < 4 {
            return Err(Error::Corrupt("ID_KEY is truncated".into()));
        }
        let tag: KeyTag = raw[..4].try_into().expect("4 bytes");
        let key = self.key(&tag).map_err(|_| {
            Error::InvalidRequest(format!(
                "ID_KEY is sealed with key {}, which is not configured (add it with --encryption-old-key-file)",
                hex::encode(tag)
            ))
        })?;
        let plain = key
            .cipher
            .decrypt(
                &nonce_of(b"kosha id key"),
                Payload {
                    msg: &raw[4..],
                    aad: &tag,
                },
            )
            .map_err(|_| Error::Corrupt("ID_KEY failed authenticated decryption".into()))?;
        let id: [u8; 32] = plain
            .get(..32)
            .and_then(|k| k.try_into().ok())
            .ok_or_else(|| Error::Corrupt("ID_KEY is malformed".into()))?;
        let legacy = plain.get(32..36).and_then(|t| t.try_into().ok());
        Ok(IdKey::from_key(id, legacy))
    }

    /// (Re)write `ID_KEY` under the primary key, unless it already is.
    fn store_id_key(&self, root: &Path) -> Result<()> {
        let path = root.join(ID_KEY_FILE);
        let k = &self.keys[0];
        if let Ok(raw) = std::fs::read(&path)
            && raw.get(..4) == Some(&k.tag[..])
        {
            return Ok(());
        }
        let mut plain = self.id.key.to_vec();
        if let Some(t) = self.id.legacy_tag {
            plain.extend_from_slice(&t);
        }
        let sealed = k
            .cipher
            .encrypt(
                &nonce_of(b"kosha id key"),
                Payload {
                    msg: &plain,
                    aad: &k.tag,
                },
            )
            .expect("AES-GCM-SIV encryption cannot fail for in-memory buffers");
        let mut out = k.tag.to_vec();
        out.extend_from_slice(&sealed);
        write_atomic(&path, &out)
    }

    /// Whether `ID_KEY` is sealed under an old key (part of rotation).
    pub(crate) fn id_key_needs_rotation(&self, root: &Path) -> bool {
        std::fs::read(root.join(ID_KEY_FILE))
            .map(|raw| raw.get(..4) != Some(&self.keys[0].tag[..]))
            .unwrap_or(false)
    }

    pub(crate) fn rotate_id_key(&self, root: &Path) -> Result<()> {
        self.store_id_key(root)
    }
}

fn has_chunks(root: &Path) -> Result<bool> {
    Ok(std::fs::read_dir(root)?.any(|e| {
        e.ok()
            .is_some_and(|e| e.file_name().len() == 2 && e.file_type().is_ok_and(|t| t.is_dir()))
    }))
}

fn write_atomic(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, path)?;
    crate::durability::sync_dir(path.parent().expect("has parent").to_path_buf())?;
    Ok(())
}

/// Record that a new data directory stores unencrypted chunks, or check
/// that an existing one does.
pub fn check_unencrypted(root: &Path) -> Result<()> {
    match std::fs::read_to_string(root.join(MARKER)) {
        Ok(m) if m.trim() == "none" => Ok(()),
        Ok(m) => Err(Error::InvalidRequest(format!(
            "this data directory is encrypted (key {}); start with its --encryption-key-file",
            m.trim().trim_start_matches("id:")
        ))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            write_atomic(&root.join(MARKER), b"none")
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
        let a = k.seal_chunk(&id, 1, data);
        assert_eq!(a, k.seal_chunk(&id, 1, data));
        assert!(!a.windows(5).any(|w| w == b"hello"));
        assert_eq!(k.open_chunk(&id, &a).unwrap(), (1, data.to_vec()));
        assert!(other.open_chunk(&id, &a).is_err());
        let mut tampered = a.clone();
        tampered[0] ^= 1; // the codec byte is authenticated too
        assert!(k.open_chunk(&id, &tampered).is_err());
        let mut tampered = a;
        *tampered.last_mut().unwrap() ^= 1;
        assert!(k.open_chunk(&id, &tampered).is_err());
    }

    #[test]
    fn rotation_reseals_under_the_new_key() {
        let old = ChunkKeys::new(&[1; 32]);
        let id = old.chunk_id(b"payload");
        let stored = old.seal_chunk(&id, 0, b"payload");
        let ring = ChunkKeys::ring([2; 32], vec![[1; 32]], None);
        assert!(ring.needs_rotation(&stored));
        let fresh = ring.reseal_chunk(&id, &stored).unwrap();
        assert!(!ring.needs_rotation(&fresh));
        assert_eq!(
            ring.open_chunk(&id, &fresh).unwrap(),
            (0, b"payload".to_vec())
        );
        // The new key alone opens it; the old one alone does not.
        assert!(ChunkKeys::new(&[2; 32]).open_chunk(&id, &fresh).is_ok());
        assert!(old.open_chunk(&id, &fresh).is_err());

        let (tag, sealed) = ring.seal_shard(&id, 3, b"shard");
        assert_eq!(ring.open_shard(&id, 3, &tag, &sealed).unwrap(), b"shard");
        assert!(
            ring.open_shard(&id, 4, &tag, &sealed).is_err(),
            "nonce is per index"
        );
    }

    #[test]
    fn id_key_survives_rotation_and_old_key_removal() {
        let dir = tempfile::tempdir().unwrap();
        let a = ChunkKeys::new(&[1; 32]).resolve(dir.path()).unwrap();
        let id = a.chunk_id(b"x");
        // Rotate: B primary, A old. Same ids; ID_KEY re-sealed under B.
        let ab = ChunkKeys::ring([2; 32], vec![[1; 32]], None)
            .resolve(dir.path())
            .unwrap();
        assert_eq!(ab.chunk_id(b"x"), id);
        assert_eq!(ab.fingerprint(), a.fingerprint());
        assert!(!ab.id_key_needs_rotation(dir.path()));
        // A removed: still the same identity.
        let b = ChunkKeys::new(&[2; 32]).resolve(dir.path()).unwrap();
        assert_eq!(b.chunk_id(b"x"), id);
        // An unrelated key is refused.
        assert!(ChunkKeys::new(&[9; 32]).resolve(dir.path()).is_err());
        // A new directory for a rotated cluster takes the identity seed.
        let joiner = tempfile::tempdir().unwrap();
        let j = ChunkKeys::ring([2; 32], vec![], Some([1; 32]))
            .resolve(joiner.path())
            .unwrap();
        assert_eq!(j.fingerprint(), a.fingerprint());

        let plain = tempfile::tempdir().unwrap();
        std::fs::create_dir(plain.path().join("ab")).unwrap();
        assert!(ChunkKeys::new(&[1; 32]).resolve(plain.path()).is_err());
        check_unencrypted(plain.path()).unwrap();
        assert!(ChunkKeys::new(&[1; 32]).resolve(plain.path()).is_err());
        assert!(check_unencrypted(dir.path()).is_err());
    }
}
