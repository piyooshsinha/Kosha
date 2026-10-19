//! Erasure-coded placement: writing, reading and repairing shards.
//!
//! Shard `i` of a chunk belongs to the `i`-th voter in the chunk's
//! rendezvous ranking. When designated holders are down, their indices are
//! taken over, in order, by the next live voters after the first `k+m`, so
//! a failure moves only the shards of the failed node.

use std::collections::BTreeSet;

use bytes::Bytes;
use kosha_engine::ChunkId;

use crate::erasure::{self, Layout, Shard};
use crate::types::*;
use crate::{Inner, rank_voters};

/// Which shard index `me` should hold, given the ranking and live voters.
pub(crate) fn assignment(
    ranked: &[(NodeId, KoshaNode)],
    live: &BTreeSet<NodeId>,
    layout: Layout,
    me: NodeId,
) -> Option<u8> {
    let total = layout.total().min(ranked.len());
    if let Some(i) = ranked[..total].iter().position(|(n, _)| *n == me) {
        return live.contains(&me).then_some(i as u8);
    }
    let dead: Vec<usize> = (0..total)
        .filter(|i| !live.contains(&ranked[*i].0))
        .collect();
    ranked[total..]
        .iter()
        .filter(|(n, _)| live.contains(n))
        .position(|(n, _)| *n == me)
        .and_then(|j| dead.get(j).map(|i| *i as u8))
}

/// The node that should hold shard `index` (see [`assignment`]).
pub(crate) fn holder(
    ranked: &[(NodeId, KoshaNode)],
    live: &BTreeSet<NodeId>,
    layout: Layout,
    index: u8,
) -> Option<(NodeId, KoshaNode)> {
    ranked
        .iter()
        .find(|(n, _)| assignment(ranked, live, layout, *n) == Some(index))
        .cloned()
}

/// Frame `plain` (the compressed form) as `KSH2` shards, each encrypted
/// under the primary key when encryption at rest is on.
fn encode_shards(
    store: &kosha_engine::ChunkStore,
    id: &ChunkId,
    layout: Layout,
    plain: &[u8],
) -> Vec<Bytes> {
    match store.keys() {
        Some(keys) => erasure::encode_sealed(layout, plain, |i, p| keys.seal_shard(id, i, p)),
        None => erasure::split_v2(layout, plain),
    }
}

/// Decrypt the payloads of encrypted `KSH2` shards; shards that fail are
/// dropped (treated as missing).
fn open_shards(store: &kosha_engine::ChunkStore, id: &ChunkId, shards: Vec<Shard>) -> Vec<Shard> {
    shards
        .into_iter()
        .filter_map(|mut s| {
            let Some(tag) = s.tag else { return Some(s) };
            match store.keys().map(|k| k.open_shard(id, s.index, &tag, &s.payload)) {
                Some(Ok(plain)) => {
                    s.payload = Bytes::from(plain);
                    s.tag = None;
                    Some(s)
                }
                Some(Err(e)) => {
                    tracing::warn!(chunk = %id.to_hex(), shard = s.index, error = %e, "shard could not be decrypted");
                    None
                }
                None => None,
            }
        })
        .collect()
}

/// Re-encrypt this node's `KSH2` shards that are not under the primary key.
/// Returns (rewritten, pending afterwards, legacy `KSH1` shards that can
/// only be read with their original key).
pub(crate) async fn rotate_shards(
    store: &kosha_engine::ChunkStore,
    limit: usize,
) -> kosha_engine::Result<(usize, usize, usize)> {
    let Some(keys) = store.keys().cloned() else {
        return Ok((0, 0, 0));
    };
    let primary = keys.primary_tag();
    let (mut done, mut pending, mut legacy) = (0, 0, 0);
    for (id, index) in store.shard_files()? {
        let Some(raw) = store.get_shard(&id, index).await? else {
            continue;
        };
        let Some(shard) = erasure::parse(&raw) else {
            continue;
        };
        match (shard.version, shard.tag) {
            (1, _) => {
                legacy += 1;
                continue;
            }
            (_, Some(tag)) if tag != primary => {}
            _ => continue,
        }
        if done >= limit {
            pending += 1;
            continue;
        }
        let tag = shard.tag.expect("checked");
        let plain = keys.open_shard(&id, index, &tag, &shard.payload)?;
        let (new_tag, sealed) = keys.seal_shard(&id, index, &plain);
        let framed = erasure::frame_v2(
            index,
            shard.layout,
            shard.encoded_len,
            Some(new_tag),
            &sealed,
        );
        store.replace_shard(id, index, framed).await?;
        done += 1;
    }
    Ok((done, pending, legacy))
}

impl Inner {
    /// The erasure layout for a new chunk of `len` bytes, if it should be
    /// erasure coded: large enough, and enough voters to spread it.
    pub(crate) fn ec_layout(&self, len: usize, voters: usize) -> Option<Layout> {
        self.cfg.erasure.filter(|l| {
            !self.cfg.erasure_cold_only && len >= self.cfg.erasure_min_len && voters >= l.total()
        })
    }

    async fn store_shard(
        &self,
        node: &(NodeId, KoshaNode),
        id: ChunkId,
        index: u8,
        shard: Bytes,
    ) -> kosha_engine::Result<()> {
        if node.0 == self.cfg.node_id {
            self.engine.chunk_store().put_shard(id, index, shard).await
        } else {
            self.client
                .put_shard(&node.1.addr, &id.to_hex(), index, shard)
                .await
                .map_err(|e| {
                    kosha_engine::Error::Internal(format!("shard {index} to node {}: {e}", node.0))
                })
        }
    }

    /// Encode a chunk and place every shard. Returns the stored size and
    /// the node that took each shard.
    pub(crate) async fn place_shards(
        &self,
        id: ChunkId,
        data: Bytes,
        layout: Layout,
    ) -> kosha_engine::Result<(u64, Vec<(u8, NodeId)>)> {
        let store = self.engine.chunk_store();
        let (stored, shards) = tokio::task::spawn_blocking(move || {
            store.compress_external(&data).map(|plain| {
                (
                    plain.len() as u64,
                    encode_shards(&store, &id, layout, &plain),
                )
            })
        })
        .await??;
        let ranked = rank_voters(self, &id);
        let first = futures::future::join_all(
            shards
                .iter()
                .enumerate()
                .map(|(i, s)| self.store_shard(&ranked[i], id, i as u8, s.clone())),
        )
        .await;
        // Shards whose designated node failed go to the next nodes down
        // the ranking, one shard per node.
        let mut spare = ranked.iter().skip(layout.total());
        let mut placed = Vec::with_capacity(shards.len());
        for (i, r) in first.into_iter().enumerate() {
            let Err(mut err) = r else {
                placed.push((i as u8, ranked[i].0));
                continue;
            };
            loop {
                let Some(node) = spare.next() else {
                    return Err(kosha_engine::Error::Internal(format!(
                        "could not place shard {i} of chunk {}: {err}",
                        id.to_hex()
                    )));
                };
                match self.store_shard(node, id, i as u8, shards[i].clone()).await {
                    Ok(()) => {
                        placed.push((i as u8, node.0));
                        break;
                    }
                    Err(e) => err = e,
                }
            }
        }
        Ok((stored, placed))
    }

    async fn shard_from(
        &self,
        node: &(NodeId, KoshaNode),
        id: &ChunkId,
        index: u8,
    ) -> Option<Shard> {
        let raw = if node.0 == self.cfg.node_id {
            self.engine
                .chunk_store()
                .get_shard(id, index)
                .await
                .ok()??
        } else {
            self.client
                .get_shard(&node.1.addr, &id.to_hex(), index)
                .await
                .ok()??
        };
        let shard = erasure::parse(&raw);
        if shard.is_none() {
            tracing::warn!(node = node.0, chunk = %id.to_hex(), index, "damaged shard ignored");
        }
        shard
    }

    /// Gather at least `k` valid shards: the data shards from their
    /// designated nodes first, then whatever any voter holds.
    pub(crate) async fn collect_shards(&self, id: &ChunkId, skip: Option<u8>) -> Vec<Shard> {
        let ranked = rank_voters(self, id);
        let mut got: Vec<Shard> = Vec::new();
        if let Some(layout) = self.cfg.erasure {
            let k = (layout.k as usize).min(ranked.len());
            got = futures::future::join_all(
                (0..k)
                    .filter(|i| Some(*i as u8) != skip)
                    .map(|i| self.shard_from(&ranked[i], id, i as u8)),
            )
            .await
            .into_iter()
            .flatten()
            .collect();
            if got.len() >= layout.k as usize {
                return got;
            }
        }
        // Ask every voter what it holds.
        let inventories = futures::future::join_all(ranked.iter().map(|node| async move {
            let held = if node.0 == self.cfg.node_id {
                self.engine.chunk_store().shard_indices(id)
            } else {
                self.client
                    .shard_indices(&node.1.addr, &id.to_hex())
                    .await
                    .unwrap_or_default()
            };
            (node.clone(), held)
        }))
        .await;
        for (node, held) in inventories {
            for index in held {
                if Some(index) == skip || got.iter().any(|s| s.index == index) {
                    continue;
                }
                if let Some(s) = self.shard_from(&node, id, index).await {
                    got.push(s);
                }
                if got
                    .first()
                    .is_some_and(|f| got.len() >= f.layout.k as usize)
                {
                    return got;
                }
            }
        }
        got
    }

    /// Read an erasure-coded chunk.
    pub(crate) async fn fetch_shards(&self, id: ChunkId, len: u32) -> kosha_engine::Result<Bytes> {
        let shards = self.collect_shards(&id, None).await;
        if shards.is_empty() {
            return Err(kosha_engine::Error::Corrupt(format!(
                "chunk {} has no shards",
                id.to_hex()
            )));
        }
        let store = self.engine.chunk_store();
        tokio::task::spawn_blocking(move || {
            let shards = open_shards(&store, &id, shards);
            let encoded = erasure::decode(shards)
                .map_err(|e| kosha_engine::Error::Corrupt(format!("chunk {}: {e}", id.to_hex())))?;
            store.decode_external(id, len, encoded)
        })
        .await?
    }

    /// Rebuild shard `index` from the others. The chunk is decoded and
    /// verified against its content address before the shard is derived
    /// from it, so repair never spreads bad data.
    pub(crate) async fn rebuild_shard(&self, id: &ChunkId, len: u32, index: u8) -> Option<Bytes> {
        let shards = self.collect_shards(id, Some(index)).await;
        let layout = shards.first()?.layout;
        let store = self.engine.chunk_store();
        let id = *id;
        tokio::task::spawn_blocking(move || {
            // Rebuild in the format of the surviving shards.
            let v2 = shards.iter().filter(|s| s.version == 2).count()
                >= shards.iter().filter(|s| s.version == 1).count();
            let shards = open_shards(&store, &id, shards);
            let encoded = erasure::decode(shards).ok()?;
            if let Err(e) = store.decode_external(id, len, encoded.clone()) {
                tracing::warn!(chunk = %id.to_hex(), error = %e, "rebuilt chunk failed verification");
                return None;
            }
            let all = if v2 {
                encode_shards(&store, &id, layout, &encoded)
            } else {
                erasure::encode(layout, &encoded)
            };
            all.into_iter().nth(index as usize)
        })
        .await
        .ok()
        .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranked(n: u64) -> Vec<(NodeId, KoshaNode)> {
        (1..=n)
            .map(|i| {
                (
                    i,
                    KoshaNode {
                        addr: format!("n{i}"),
                        api: String::new(),
                    },
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn shards_rotate_independently_and_mixed_keys_decode() {
        use kosha_engine::{ChunkKeys, Engine, EngineConfig, SyncMode};
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = ([0xAAu8; 32], [0xBBu8; 32]);
        let open = |keys: ChunkKeys| {
            let mut cfg = EngineConfig::new(dir.path());
            cfg.sync = SyncMode::None;
            cfg.encryption = Some(Arc::new(keys));
            Engine::open(cfg).unwrap()
        };
        let layout = Layout { k: 3, m: 2 };
        let data: Vec<u8> = (0..50_000u32).map(|i| (i * 7 % 256) as u8).collect();
        let (id, len) = {
            let e = open(ChunkKeys::new(&a));
            let store = e.chunk_store();
            let id = e.chunk_id(&data);
            let plain = store.compress_external(&data).unwrap();
            for (i, s) in encode_shards(&store, &id, layout, &plain)
                .into_iter()
                .enumerate()
            {
                store.put_shard(id, i as u8, s).await.unwrap();
            }
            (id, data.len() as u32)
        };
        let e = open(ChunkKeys::ring(b, vec![a], None));
        let store = e.chunk_store();
        let tag_b = ChunkKeys::new(&b).primary_tag();
        // Rotate only two shards first: a mix of keys still decodes.
        assert_eq!(rotate_shards(&store, 2).await.unwrap(), (2, 3, 0));
        let read = |store: Arc<kosha_engine::ChunkStore>| async move {
            let mut shards = Vec::new();
            for i in 0..5u8 {
                shards.push(
                    erasure::parse(&store.get_shard(&id, i).await.unwrap().unwrap()).unwrap(),
                );
            }
            let tags: Vec<_> = shards.iter().map(|s| s.tag.unwrap()).collect();
            let encoded = erasure::decode(open_shards(&store, &id, shards)).unwrap();
            (tags, store.decode_external(id, len, encoded).unwrap())
        };
        let (tags, got) = read(store.clone()).await;
        assert_eq!(got.as_ref(), data.as_slice());
        assert_eq!(tags.iter().filter(|t| **t == tag_b).count(), 2);
        assert_eq!(rotate_shards(&store, 100).await.unwrap(), (3, 0, 0));
        drop(e);
        // B alone decodes all of them.
        let e = open(ChunkKeys::new(&b));
        let (tags, got) = read(e.chunk_store()).await;
        assert!(tags.iter().all(|t| *t == tag_b));
        assert_eq!(got.as_ref(), data.as_slice());
    }

    #[test]
    fn assignments_move_only_the_failed_nodes_shards() {
        let r = ranked(6);
        let l = Layout { k: 2, m: 2 };
        let all: BTreeSet<NodeId> = (1..=6).collect();
        let got: Vec<_> = (1..=6).map(|n| assignment(&r, &all, l, n)).collect();
        assert_eq!(got, [Some(0), Some(1), Some(2), Some(3), None, None]);
        // Node 2 (shard 1) dies: node 5 takes over shard 1; nobody else moves.
        let live: BTreeSet<NodeId> = [1, 3, 4, 5, 6].into();
        let got: Vec<_> = (1..=6).map(|n| assignment(&r, &live, l, n)).collect();
        assert_eq!(got, [Some(0), None, Some(2), Some(3), Some(1), None]);
        assert_eq!(holder(&r, &live, l, 1).unwrap().0, 5);
        // Two die.
        let live: BTreeSet<NodeId> = [2, 4, 5, 6].into();
        let got: Vec<_> = (1..=6).map(|n| assignment(&r, &live, l, n)).collect();
        assert_eq!(got, [None, Some(1), None, Some(3), Some(0), Some(2)]);
    }
}
