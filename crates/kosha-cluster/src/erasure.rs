//! Reed–Solomon erasure coding of chunks.
//!
//! A chunk's stored form (compressed, and encrypted if enabled) is split
//! into `k` data shards plus `m` parity shards; any `k` of them rebuild it.
//! Each shard is framed with a header that carries its index, the layout,
//! the length of the stored form, and a BLAKE3 checksum of the shard, so a
//! damaged shard is detected and treated as missing.

use bytes::{BufMut, Bytes, BytesMut};
use reed_solomon_erasure::galois_8::ReedSolomon;

const MAGIC: &[u8; 4] = b"KSH1";
pub const HEADER: usize = 48;

/// `k` data + `m` parity shards.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Layout {
    pub k: u8,
    pub m: u8,
}

impl Layout {
    pub fn total(&self) -> usize {
        self.k as usize + self.m as usize
    }

    pub fn parse(s: &str) -> Result<Layout, String> {
        let (k, m) = s
            .split_once('+')
            .ok_or_else(|| format!("erasure layout {s:?}: expected K+M, e.g. 4+2"))?;
        let k: u8 = k.trim().parse().map_err(|_| format!("bad K in {s:?}"))?;
        let m: u8 = m.trim().parse().map_err(|_| format!("bad M in {s:?}"))?;
        if k < 2 || m < 1 || k as usize + m as usize > 32 {
            return Err(format!(
                "erasure layout {s:?}: need K >= 2, M >= 1 and K+M <= 32"
            ));
        }
        Ok(Layout { k, m })
    }
}

impl std::fmt::Display for Layout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}+{}", self.k, self.m)
    }
}

/// A shard after header validation.
pub struct Shard {
    pub index: u8,
    pub layout: Layout,
    pub encoded_len: u32,
    pub payload: Bytes,
}

/// Split `encoded` into framed shards, index order.
pub fn encode(layout: Layout, encoded: &[u8]) -> Vec<Bytes> {
    let k = layout.k as usize;
    let shard_len = encoded.len().div_ceil(k).max(1);
    let mut shards: Vec<Vec<u8>> = (0..layout.total())
        .map(|i| {
            let mut s = vec![0u8; shard_len];
            if i < k {
                let from = (i * shard_len).min(encoded.len());
                let to = ((i + 1) * shard_len).min(encoded.len());
                s[..to - from].copy_from_slice(&encoded[from..to]);
            }
            s
        })
        .collect();
    ReedSolomon::new(k, layout.m as usize)
        .expect("layout validated")
        .encode(&mut shards)
        .expect("shards have equal length");
    shards
        .into_iter()
        .enumerate()
        .map(|(i, payload)| frame(i as u8, layout, encoded.len() as u32, &payload))
        .collect()
}

fn frame(index: u8, layout: Layout, encoded_len: u32, payload: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(HEADER + payload.len());
    b.put_slice(MAGIC);
    b.put_u8(index);
    b.put_u8(layout.k);
    b.put_u8(layout.m);
    b.put_u8(0);
    b.put_u32_le(encoded_len);
    b.put_u32_le(payload.len() as u32);
    b.put_slice(blake3::hash(payload).as_bytes());
    b.put_slice(payload);
    b.freeze()
}

/// Validate a framed shard. `None` if it is malformed or damaged.
pub fn parse(raw: &Bytes) -> Option<Shard> {
    if raw.len() < HEADER || &raw[..4] != MAGIC {
        return None;
    }
    let layout = Layout {
        k: raw[5],
        m: raw[6],
    };
    let encoded_len = u32::from_le_bytes(raw[8..12].try_into().ok()?);
    let shard_len = u32::from_le_bytes(raw[12..16].try_into().ok()?) as usize;
    let payload = raw.slice(HEADER..);
    if payload.len() != shard_len
        || blake3::hash(&payload).as_bytes() != &raw[16..48]
        || raw[4] as usize >= layout.total()
    {
        return None;
    }
    Some(Shard {
        index: raw[4],
        layout,
        encoded_len,
        payload,
    })
}

/// Rebuild the stored form from at least `k` valid shards of one layout.
pub fn decode(shards: Vec<Shard>) -> Result<Vec<u8>, String> {
    let first = shards.first().ok_or("no shards")?;
    let (layout, encoded_len) = (first.layout, first.encoded_len);
    let k = layout.k as usize;
    let mut slots: Vec<Option<Vec<u8>>> = vec![None; layout.total()];
    let mut have = 0;
    for s in shards {
        if s.layout != layout || s.encoded_len != encoded_len {
            continue;
        }
        let slot = &mut slots[s.index as usize];
        if slot.is_none() {
            *slot = Some(s.payload.to_vec());
            have += 1;
        }
    }
    if have < k {
        return Err(format!("only {have} of the {k} shards needed"));
    }
    if slots[..k].iter().any(Option::is_none) {
        ReedSolomon::new(k, layout.m as usize)
            .map_err(|e| e.to_string())?
            .reconstruct_data(&mut slots)
            .map_err(|e| e.to_string())?;
    }
    let mut out = Vec::with_capacity(encoded_len as usize);
    for s in slots.into_iter().take(k) {
        out.extend_from_slice(&s.expect("reconstructed"));
    }
    out.truncate(encoded_len as usize);
    Ok(out)
}

/// Rebuild one specific shard (framed) from at least `k` others.
#[cfg(test)]
pub fn rebuild(shards: Vec<Shard>, index: u8) -> Result<Bytes, String> {
    let first = shards.first().ok_or("no shards")?;
    let (layout, encoded_len) = (first.layout, first.encoded_len);
    let encoded = decode(shards)?;
    debug_assert_eq!(encoded.len(), encoded_len as usize);
    encode(layout, &encoded)
        .into_iter()
        .nth(index as usize)
        .ok_or_else(|| format!("shard index {index} is outside {layout}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn any_k_shards_rebuild_the_chunk() {
        let layout = Layout::parse("4+2").unwrap();
        let data: Vec<u8> = (0..100_003u32).map(|i| (i * 31 % 251) as u8).collect();
        let shards = encode(layout, &data);
        assert_eq!(shards.len(), 6);
        // Every way of losing two shards still decodes.
        for a in 0..6 {
            for b in a + 1..6 {
                let kept: Vec<Shard> = shards
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != a && *i != b)
                    .map(|(_, s)| parse(s).unwrap())
                    .collect();
                assert_eq!(decode(kept).unwrap(), data, "lost {a} and {b}");
            }
        }
        // Three missing is too many.
        let three: Vec<Shard> = shards[..3].iter().map(|s| parse(s).unwrap()).collect();
        assert!(decode(three).is_err());
        // A flipped bit is detected.
        let mut bad = shards[0].to_vec();
        bad[HEADER + 10] ^= 1;
        assert!(parse(&Bytes::from(bad)).is_none());
        // Rebuilding a lost parity shard reproduces it exactly.
        let others: Vec<Shard> = shards[..5].iter().map(|s| parse(s).unwrap()).collect();
        assert_eq!(rebuild(others, 5).unwrap(), shards[5]);
    }

    #[test]
    fn tiny_and_parse_errors() {
        let layout = Layout { k: 3, m: 2 };
        for n in [0usize, 1, 2, 3, 7] {
            let data = vec![9u8; n];
            let shards: Vec<Shard> = encode(layout, &data)
                .iter()
                .skip(2)
                .map(|s| parse(s).unwrap())
                .collect();
            assert_eq!(decode(shards).unwrap(), data);
        }
        assert!(Layout::parse("1+1").is_err());
        assert!(Layout::parse("4").is_err());
        assert_eq!(Layout::parse(" 8 + 3 ").unwrap(), Layout { k: 8, m: 3 });
    }
}
