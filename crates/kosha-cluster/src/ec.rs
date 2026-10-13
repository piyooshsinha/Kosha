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

impl Inner {
    /// The erasure layout for a new chunk of `len` bytes, if it should be
    /// erasure coded: large enough, and enough voters to spread it.
    pub(crate) fn ec_layout(&self, len: usize, voters: usize) -> Option<Layout> {
        self.cfg
            .erasure
            .filter(|l| len >= self.cfg.erasure_min_len && voters >= l.total())
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

    /// Encode a chunk and place every shard. Returns the stored size.
    pub(crate) async fn place_shards(
        &self,
        id: ChunkId,
        data: Bytes,
        layout: Layout,
    ) -> kosha_engine::Result<u64> {
        let store = self.engine.chunk_store();
        let shards = tokio::task::spawn_blocking(move || {
            store
                .encode_external(&id, &data)
                .map(|encoded| (encoded.len() as u64, erasure::encode(layout, &encoded)))
        })
        .await??;
        let (stored, shards) = shards;
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
        for (i, r) in first.into_iter().enumerate() {
            let Err(mut err) = r else { continue };
            loop {
                let Some(node) = spare.next() else {
                    return Err(kosha_engine::Error::Internal(format!(
                        "could not place shard {i} of chunk {}: {err}",
                        id.to_hex()
                    )));
                };
                match self.store_shard(node, id, i as u8, shards[i].clone()).await {
                    Ok(()) => break,
                    Err(e) => err = e,
                }
            }
        }
        Ok(stored)
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
            let encoded = erasure::decode(shards).ok()?;
            if let Err(e) = store.decode_external(id, len, encoded.clone()) {
                tracing::warn!(chunk = %id.to_hex(), error = %e, "rebuilt chunk failed verification");
                return None;
            }
            erasure::encode(layout, &encoded).into_iter().nth(index as usize)
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
