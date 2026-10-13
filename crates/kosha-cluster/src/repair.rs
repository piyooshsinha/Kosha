//! Background re-replication and rebalancing.
//!
//! Every node independently maintains its own disk; no coordination is
//! needed because placement is a pure function (rendezvous ranking) of the
//! chunk id and the voter set, and chunks are immutable.
//!
//! For each chunk referenced by committed metadata:
//!
//! * **Repair.** If this node is among the top `replication` *live* voters
//!   for the chunk but has no copy, it fetches one from a peer (verifying the
//!   content hash) and stores it. When a node dies, the next-ranked live node
//!   becomes responsible and the replica count is restored without waiting
//!   for a read.
//! * **Rebalance.** If this node holds a copy but is among the top
//!   `replication` of neither *all* voters nor *live* voters, the copy is
//!   surplus (e.g. a fallback replica written while a designated node was
//!   down). It is deleted only after every designated node is live and
//!   confirms it holds the chunk, so the replica count never drops below
//!   target.
//!
//! Erasure-coded chunks are kept per shard instead: each node rebuilds the
//! shard index it is responsible for (see `ec::assignment`) from any `k`
//! others, and drops shards it holds for others once their holder confirms.
//!
//! Chunks not yet referenced by metadata (in-flight uploads) are never
//! touched here; the garbage collector owns those.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::StreamExt;
use kosha_engine::ChunkId;
use serde::Serialize;

use crate::types::*;
use crate::{Inner, rank_voters};

const PAGE: usize = 1000;
const CONCURRENCY: usize = 8;
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Default)]
pub(crate) struct RepairStats {
    pub rounds: AtomicU64,
    pub scanned: AtomicU64,
    pub repaired: AtomicU64,
    pub surplus_removed: AtomicU64,
    pub failed: AtomicU64,
    pub last_round_ms: AtomicU64,
}

/// Outcome of one repair round on this node.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RepairReport {
    pub scanned: u64,
    pub repaired: u64,
    pub surplus_removed: u64,
    pub failed: u64,
    pub live_voters: usize,
    pub voters: usize,
}

enum Action {
    Keep,
    Repaired,
    Removed,
    Failed,
}

impl Inner {
    /// Voters that answer a health probe right now (always includes self).
    async fn live_voters(&self) -> BTreeSet<NodeId> {
        let m = self.raft.metrics().borrow().clone();
        let membership = m.membership_config.membership();
        let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
        let probes = membership
            .nodes()
            .filter(|(id, _)| voters.contains(id))
            .map(|(id, node)| {
                let (id, addr) = (*id, node.addr.clone());
                async move {
                    if id == self.cfg.node_id {
                        return Some(id);
                    }
                    self.client
                        .health(&addr, PROBE_TIMEOUT)
                        .await
                        .ok()
                        .map(|_| id)
                }
            });
        futures::future::join_all(probes)
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// Keep this node's shard of an erasure-coded chunk. `None` means the
    /// chunk turned out to be replicated (written while the cluster was too
    /// small), so the replica logic applies.
    async fn maintain_shards(
        &self,
        id: ChunkId,
        len: u32,
        layout: crate::erasure::Layout,
        ranked: &[(NodeId, KoshaNode)],
        live: &BTreeSet<NodeId>,
    ) -> Option<Action> {
        let store = self.engine.chunk_store();
        let me = self.cfg.node_id;
        let local = store.shard_indices(&id);
        let mine = crate::ec::assignment(ranked, live, layout, me);
        if local.is_empty() && mine.is_none() {
            return None;
        }
        if let Some(s) = mine
            && !local.contains(&s)
        {
            if local.is_empty() {
                // Does any voter hold shards of it at all?
                let hex = id.to_hex();
                let any = futures::future::join_all(
                    ranked
                        .iter()
                        .filter(|(n, _)| *n != me && live.contains(n))
                        .map(|(_, node)| self.client.shard_indices(&node.addr, &hex)),
                )
                .await
                .into_iter()
                .any(|r| r.is_ok_and(|v| !v.is_empty()));
                if !any {
                    return None;
                }
            }
            return Some(match self.rebuild_shard(&id, len, s).await {
                Some(shard) => match store.put_shard(id, s, shard).await {
                    Ok(()) => Action::Repaired,
                    Err(e) => {
                        tracing::warn!(chunk = %id.to_hex(), shard = s, error = %e, "storing rebuilt shard failed");
                        Action::Failed
                    }
                },
                None => {
                    tracing::warn!(chunk = %id.to_hex(), shard = s, "not enough shards to rebuild");
                    Action::Failed
                }
            });
        }
        // Shards this node holds but is not responsible for: drop each once
        // its responsible node is live and confirms it.
        let mut removed = false;
        for t in local.into_iter().filter(|t| Some(*t) != mine) {
            let Some((h, node)) = crate::ec::holder(ranked, live, layout, t) else {
                continue;
            };
            if h == me {
                continue;
            }
            let confirmed = self
                .client
                .shard_indices(&node.addr, &id.to_hex())
                .await
                .is_ok_and(|v| v.contains(&t));
            if confirmed && store.remove_shard(&id, t).unwrap_or(false) {
                removed = true;
            }
        }
        Some(if removed {
            Action::Removed
        } else {
            Action::Keep
        })
    }

    async fn maintain_chunk(
        &self,
        id: ChunkId,
        len: u32,
        coded: bool,
        live: &BTreeSet<NodeId>,
    ) -> Action {
        let ranked = rank_voters(self, &id);
        if coded && let Some(layout) = self.cfg.erasure {
            // Converted from replicas: shards only; full copies are gone.
            return self
                .maintain_shards(id, len, layout, &ranked, live)
                .await
                .unwrap_or(Action::Keep);
        }
        // Written as shards: no full copy anywhere (see `ec_layout`).
        if let Some(layout) = self
            .cfg
            .erasure
            .filter(|_| !self.cfg.erasure_cold_only && len as usize >= self.cfg.erasure_min_len)
            && !self.engine.chunk_store().has(&id)
            && let Some(action) = self.maintain_shards(id, len, layout, &ranked, live).await
        {
            return action;
        }
        let want = self.cfg.replication.clamp(1, ranked.len().max(1));
        let me = self.cfg.node_id;
        let designated_all: Vec<&(NodeId, KoshaNode)> = ranked.iter().take(want).collect();
        let designated_live: Vec<&(NodeId, KoshaNode)> = ranked
            .iter()
            .filter(|(n, _)| live.contains(n))
            .take(want)
            .collect();
        let store = self.engine.chunk_store();
        let have = store.has(&id);
        let mine_live = designated_live.iter().any(|(n, _)| *n == me);
        let mine_all = designated_all.iter().any(|(n, _)| *n == me);

        if mine_live && !have {
            for (n, node) in ranked.iter().filter(|(n, _)| *n != me && live.contains(n)) {
                match self.client.get_chunk(&node.addr, &id.to_hex(), len).await {
                    Ok(data) if data.len() == len as usize && store.id_of(&data) == id => {
                        return match store.put_replica(id, data).await {
                            Ok(_) => Action::Repaired,
                            Err(e) => {
                                tracing::warn!(chunk = %id.to_hex(), error = %e, "repair write failed");
                                Action::Failed
                            }
                        };
                    }
                    Ok(_) => {
                        tracing::warn!(node = n, chunk = %id.to_hex(), "peer replica failed verification")
                    }
                    Err(_) => {}
                }
            }
            tracing::warn!(chunk = %id.to_hex(), "no live replica found to repair from");
            return Action::Failed;
        }

        if have && !mine_live && !mine_all {
            // Surplus: drop it only when every designated node is live and
            // confirms its copy.
            for (n, node) in &designated_all {
                if !live.contains(n) {
                    return Action::Keep;
                }
                match self.client.has_chunk(&node.addr, &id.to_hex()).await {
                    Ok(true) => {}
                    _ => return Action::Keep,
                }
            }
            return match store.remove_unpinned(&id) {
                Ok(true) => Action::Removed,
                Ok(false) => Action::Keep,
                Err(e) => {
                    tracing::warn!(chunk = %id.to_hex(), error = %e, "removing surplus replica failed");
                    Action::Failed
                }
            };
        }
        Action::Keep
    }

    /// One full pass over every referenced chunk.
    pub(crate) async fn repair_round(&self) -> RepairReport {
        let started = Instant::now();
        let live = self.live_voters().await;
        let voters = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .count();
        let mut report = RepairReport {
            live_voters: live.len(),
            voters,
            ..Default::default()
        };
        let mut after = None;
        loop {
            let page = match self.engine.referenced_chunks(after, PAGE) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "repair: listing chunks failed");
                    break;
                }
            };
            let Some(&(last, _, _)) = page.last() else {
                break;
            };
            after = Some(last);
            let live = &live;
            let actions: Vec<Action> = futures::stream::iter(page)
                .map(|(id, len, coded)| self.maintain_chunk(id, len, coded, live))
                .buffer_unordered(CONCURRENCY)
                .collect()
                .await;
            for a in actions {
                report.scanned += 1;
                match a {
                    Action::Keep => {}
                    Action::Repaired => report.repaired += 1,
                    Action::Removed => report.surplus_removed += 1,
                    Action::Failed => report.failed += 1,
                }
            }
        }
        let s = &self.repair_stats;
        s.rounds.fetch_add(1, Ordering::Relaxed);
        s.scanned.fetch_add(report.scanned, Ordering::Relaxed);
        s.repaired.fetch_add(report.repaired, Ordering::Relaxed);
        s.surplus_removed
            .fetch_add(report.surplus_removed, Ordering::Relaxed);
        s.failed.fetch_add(report.failed, Ordering::Relaxed);
        s.last_round_ms
            .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        if report.repaired + report.surplus_removed + report.failed > 0 {
            tracing::info!(?report, "repair round");
        }
        report
    }
}

/// Run repair rounds forever, every `interval`.
pub(crate) fn spawn(
    inner: std::sync::Weak<Inner>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            let Some(inner) = inner.upgrade() else { return };
            if inner.raft.is_initialized().await.unwrap_or(false) {
                inner.repair_round().await;
            }
            drop::<Arc<Inner>>(inner);
        }
    })
}
