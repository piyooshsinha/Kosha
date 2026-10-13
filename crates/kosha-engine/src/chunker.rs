//! Streaming content-defined chunking.
//!
//! FastCDC picks cut points from the content itself, so chunk boundaries
//! survive insertions and deletions elsewhere in the stream. We feed it a
//! buffer at least `max` bytes long before taking a cut, which guarantees the
//! cut is identical to what a single pass over the whole object would produce.

use bytes::{Bytes, BytesMut};
use fastcdc::v2020::FastCDC;

#[derive(Clone, Copy, Debug)]
pub struct ChunkerConfig {
    pub min: u32,
    pub avg: u32,
    pub max: u32,
}

impl Default for ChunkerConfig {
    fn default() -> Self {
        ChunkerConfig {
            min: 256 * 1024,
            avg: 1024 * 1024,
            max: 4 * 1024 * 1024,
        }
    }
}

pub struct Chunker {
    cfg: ChunkerConfig,
    buf: BytesMut,
}

impl Chunker {
    pub fn new(cfg: ChunkerConfig) -> Self {
        Chunker {
            cfg,
            buf: BytesMut::new(),
        }
    }

    /// Bytes buffered but not yet emitted as a chunk.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Append data, returning every chunk whose boundary is now final.
    pub fn push(&mut self, data: &[u8]) -> Vec<Bytes> {
        self.buf.extend_from_slice(data);
        let mut out = Vec::new();
        while self.buf.len() >= self.cfg.max as usize {
            let cut = self.first_cut();
            out.push(self.buf.split_to(cut).freeze());
        }
        out
    }

    /// Flush the remainder into chunks.
    pub fn finish(mut self) -> Vec<Bytes> {
        let mut out = Vec::new();
        while !self.buf.is_empty() {
            let cut = self.first_cut();
            out.push(self.buf.split_to(cut).freeze());
        }
        out
    }

    /// Take the remaining buffered bytes without chunking them.
    pub fn take_remainder(mut self) -> Bytes {
        self.buf.split().freeze()
    }

    fn first_cut(&self) -> usize {
        FastCDC::new(&self.buf, self.cfg.min, self.cfg.avg, self.cfg.max)
            .next()
            .map(|c| c.length)
            .unwrap_or(self.buf.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pseudo_random(n: usize, seed: u64) -> Vec<u8> {
        let mut x = seed;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    fn small() -> ChunkerConfig {
        ChunkerConfig {
            min: 1024,
            avg: 4096,
            max: 16384,
        }
    }

    #[test]
    fn streaming_matches_one_shot() {
        let data = pseudo_random(300_000, 7);
        let one_shot: Vec<usize> = FastCDC::new(&data, 1024, 4096, 16384)
            .map(|c| c.length)
            .collect();

        for step in [1usize, 777, 4096, 100_000] {
            let mut c = Chunker::new(small());
            let mut got = Vec::new();
            for piece in data.chunks(step) {
                got.extend(c.push(piece).iter().map(|b| b.len()));
            }
            got.extend(c.finish().iter().map(|b| b.len()));
            assert_eq!(got, one_shot, "step {step}");
        }
    }

    #[test]
    fn insertion_preserves_most_chunks() {
        let data = pseudo_random(400_000, 42);
        let mut edited = data.clone();
        edited.splice(200_000..200_000, b"hello world".iter().copied());

        let ids = |d: &[u8]| -> std::collections::HashSet<[u8; 32]> {
            let mut c = Chunker::new(small());
            let mut v = c.push(d);
            v.extend(c.finish());
            v.iter().map(|b| *blake3::hash(b).as_bytes()).collect()
        };
        let a = ids(&data);
        let b = ids(&edited);
        let shared = a.intersection(&b).count();
        assert!(shared * 10 >= a.len() * 8, "shared {shared} of {}", a.len());
    }
}
