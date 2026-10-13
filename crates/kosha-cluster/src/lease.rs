//! Leader leases for linearizable reads without a quorum round-trip.
//!
//! `Raft::ensure_linearizable` proves leadership by heartbeating every voter
//! and waiting for a quorum, on every call. That is correct but makes every
//! read cost a cluster round-trip.
//!
//! **Lease argument.** Record `t0` just before a successful
//! `ensure_linearizable`. A quorum of voters then accepted a heartbeat sent
//! after `t0` in our term, and openraft voters reject vote requests for
//! `election_timeout_max` after hearing from the leader. No other leader can
//! be elected before `t0 + election_timeout_max`, so until then the commit
//! index this node knows is the cluster's.
//!
//! We serve reads from the lease until `t0 + 0.8 × election_timeout_min`,
//! which is below `election_timeout_max` with margin for clock-rate drift
//! between nodes, and only while:
//! * this node is still the committed leader in the term the lease was taken in;
//! * the commit index belongs to that term, so the leader has committed its own
//!   no-op and knows the true commit point.
//!
//! When the lease has expired, one caller renews it (single-flight) while the
//! others wait and then reuse it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use openraft::error::{CheckIsLeaderError, RaftError};
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::Inner;
use crate::types::*;

#[derive(Clone, Copy)]
struct Lease {
    until: Instant,
    term: u64,
}

pub(crate) struct ReadLease {
    enabled: bool,
    duration: Duration,
    current: Mutex<Option<Lease>>,
    renew: tokio::sync::Mutex<()>,
    pub lease_reads: AtomicU64,
    pub quorum_reads: AtomicU64,
}

pub(crate) type ReadIndexError = RaftError<NodeId, CheckIsLeaderError<NodeId, KoshaNode>>;

impl ReadLease {
    pub fn new(enabled: bool, election_timeout_min_ms: u64) -> Self {
        ReadLease {
            enabled,
            duration: Duration::from_millis(election_timeout_min_ms * 4 / 5),
            current: Mutex::new(None),
            renew: tokio::sync::Mutex::new(()),
            lease_reads: AtomicU64::new(0),
            quorum_reads: AtomicU64::new(0),
        }
    }
}

impl Inner {
    /// The read index under a valid lease, or `None` if the lease cannot be used.
    async fn lease_index(&self) -> Option<Option<u64>> {
        let lease = (*self.lease.current.lock())?;
        if Instant::now() >= lease.until {
            return None;
        }
        let me = self.cfg.node_id;
        let (committed, vote) = self
            .raft
            .with_raft_state(|st| (st.committed, *st.vote_ref()))
            .await
            .ok()?;
        let leader = vote.leader_id();
        if !vote.is_committed() || leader.voted_for() != Some(me) || leader.get_term() != lease.term
        {
            return None;
        }
        match committed {
            Some(c) if c.leader_id.get_term() == lease.term => Some(Some(c.index)),
            _ => None,
        }
    }

    async fn current_term(&self) -> Option<u64> {
        self.raft
            .with_raft_state(|st| st.vote_ref().leader_id().get_term())
            .await
            .ok()
    }

    /// Leader-side read index: the log index a read must observe.
    /// `ForwardToLeader` if this node is not the leader.
    pub(crate) async fn leader_read_index(&self) -> Result<Option<u64>, ReadIndexError> {
        if self.lease.enabled {
            if let Some(idx) = self.lease_index().await {
                self.lease.lease_reads.fetch_add(1, Ordering::Relaxed);
                return Ok(idx);
            }
            let _renewing = self.lease.renew.lock().await;
            if let Some(idx) = self.lease_index().await {
                self.lease.lease_reads.fetch_add(1, Ordering::Relaxed);
                return Ok(idx);
            }
            let t0 = Instant::now();
            let term = self.current_term().await;
            let read = self.raft.ensure_linearizable().await?;
            self.lease.quorum_reads.fetch_add(1, Ordering::Relaxed);
            if let Some(term) = term {
                *self.lease.current.lock() = Some(Lease {
                    until: t0 + self.lease.duration,
                    term,
                });
            }
            return Ok(read.map(|l| l.index));
        }
        let read = self.raft.ensure_linearizable().await?;
        self.lease.quorum_reads.fetch_add(1, Ordering::Relaxed);
        Ok(read.map(|l| l.index))
    }
}
