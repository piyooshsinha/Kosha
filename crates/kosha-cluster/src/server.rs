//! Internal endpoint: Raft RPCs, leader services and chunk replication.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use kosha_engine::ChunkId;
use openraft::ChangeMembers;
use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use serde::Deserialize;

use crate::Inner;
use crate::rpc::{JoinRequest, LeaderReply, NodeInfo, Proposed, SECRET_HEADER, decode, encode};
use crate::types::*;

type St = State<Arc<Inner>>;

pub fn router(inner: Arc<Inner>) -> Router {
    Router::new()
        .route("/raft/append", post(append))
        .route("/raft/vote", post(vote))
        .route("/raft/snapshot", post(snapshot))
        .route("/raft/propose", post(propose))
        .route("/raft/read-index", post(read_index))
        .route("/cluster/join", post(join))
        .route("/cluster/status", get(cluster_status))
        .route("/cluster/remove", post(remove))
        .route("/cluster/update-node", post(update_node))
        .route("/cluster/info", post(node_info))
        .route(
            "/cluster/flows",
            post(|State(inner): St, body: Bytes| async move {
                let limit = decode::<usize>(&body).unwrap_or(usize::MAX);
                msgpack(&inner.local_flows(limit))
            }),
        )
        .route("/health", get(|| async { "ok" }))
        .route(
            "/chunk/{id}",
            get(get_chunk).put(put_chunk).head(head_chunk),
        )
        .route("/shard/{id}", get(list_shards))
        .route("/shard/{id}/{index}", get(get_shard).put(put_shard))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(inner.clone(), authenticate))
        .with_state(inner)
}

async fn authenticate(State(inner): St, req: Request, next: Next) -> Response {
    let given = req
        .headers()
        .get(SECRET_HEADER)
        .map(|v| v.as_bytes())
        .unwrap_or_default();
    let want = inner.cfg.secret.as_bytes();
    let ok =
        given.len() == want.len() && given.iter().zip(want).fold(0u8, |a, (x, y)| a | (x ^ y)) == 0;
    if !ok {
        return (StatusCode::UNAUTHORIZED, "bad cluster secret").into_response();
    }
    next.run(req).await
}

fn msgpack(v: &impl serde::Serialize) -> Response {
    (StatusCode::OK, encode(v)).into_response()
}

fn bad_request(e: impl std::fmt::Display) -> Response {
    (StatusCode::BAD_REQUEST, e.to_string()).into_response()
}

async fn append(State(inner): St, body: Bytes) -> Response {
    let req: AppendEntriesRequest<TypeConfig> = match decode(&body) {
        Ok(r) => r,
        Err(e) => return bad_request(e),
    };
    msgpack(&inner.raft.append_entries(req).await)
}

async fn vote(State(inner): St, body: Bytes) -> Response {
    let req: VoteRequest<NodeId> = match decode(&body) {
        Ok(r) => r,
        Err(e) => return bad_request(e),
    };
    msgpack(&inner.raft.vote(req).await)
}

async fn snapshot(State(inner): St, body: Bytes) -> Response {
    let req: InstallSnapshotRequest<TypeConfig> = match decode(&body) {
        Ok(r) => r,
        Err(e) => return bad_request(e),
    };
    msgpack(&inner.raft.install_snapshot(req).await)
}

async fn propose(State(inner): St, body: Bytes) -> Response {
    let batch: CommandBatch = match decode(&body) {
        Ok(c) => c,
        Err(e) => return bad_request(e),
    };
    match inner.raft.client_write(batch).await {
        Ok(resp) => msgpack(&LeaderReply::Ok(Proposed {
            results: resp.data.0,
            index: resp.log_id.index,
        })),
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
            msgpack(&LeaderReply::<Proposed>::NotLeader(
                f.leader_id.zip(f.leader_node),
            ))
        }
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

async fn read_index(State(inner): St) -> Response {
    match inner.leader_read_index().await {
        Ok(index) => msgpack(&LeaderReply::Ok(index)),
        Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => msgpack(
            &LeaderReply::<Option<u64>>::NotLeader(f.leader_id.zip(f.leader_node)),
        ),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

async fn join(State(inner): St, body: Bytes) -> Response {
    type Reply = LeaderReply<Result<(), String>>;
    let req: JoinRequest = match decode(&body) {
        Ok(r) => r,
        Err(e) => return bad_request(e),
    };
    let ours = inner.engine.encryption_fingerprint();
    if req.encryption.as_deref() != ours {
        let show = |k: Option<&str>| k.map_or("none".to_string(), |k| format!("key {k}"));
        return msgpack(&Reply::Ok(Err(format!(
            "encryption mismatch: the cluster uses {}, the joining node {}",
            show(ours),
            show(req.encryption.as_deref())
        ))));
    }
    if req.erasure != inner.cfg.erasure {
        let show = |l: Option<crate::erasure::Layout>| {
            l.map_or("replication".to_string(), |l| format!("erasure {l}"))
        };
        return msgpack(&Reply::Ok(Err(format!(
            "erasure coding mismatch: the cluster uses {}, the joining node {}",
            show(inner.cfg.erasure),
            show(req.erasure)
        ))));
    }
    // Catch the new node up as a learner first, then promote it.
    match inner.raft.add_learner(req.id, req.node.clone(), true).await {
        Ok(_) => {}
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
            return msgpack(&Reply::NotLeader(f.leader_id.zip(f.leader_node)));
        }
        Err(e) => return (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
    match inner
        .raft
        .change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([req.id])), false)
        .await
    {
        Ok(_) => {
            tracing::info!(node = req.id, addr = %req.node.addr, "node joined as voter");
            msgpack(&Reply::Ok(Ok(())))
        }
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
            msgpack(&Reply::NotLeader(f.leader_id.zip(f.leader_node)))
        }
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

async fn node_info(State(inner): St) -> Response {
    let (members, leader) = {
        let m = inner.raft.metrics().borrow().clone();
        let members = m
            .membership_config
            .membership()
            .nodes()
            .map(|(id, n)| (*id, n.addr.clone()))
            .collect();
        (members, m.current_leader)
    };
    msgpack(&NodeInfo {
        id: inner.cfg.node_id,
        node: KoshaNode {
            addr: inner.cfg.advertise.clone(),
            api: inner.cfg.api_addr.clone(),
        },
        encryption: inner.engine.encryption_fingerprint().map(str::to_string),
        erasure: inner.cfg.erasure,
        initialized: inner.raft.is_initialized().await.unwrap_or(false),
        members,
        leader,
    })
}

/// Replace a member's advertised API address. The cluster address is left
/// alone: peers would lose a node whose record pointed somewhere wrong.
async fn update_node(State(inner): St, body: Bytes) -> Response {
    type Reply = LeaderReply<Result<(), String>>;
    let (id, node): (NodeId, KoshaNode) = match decode(&body) {
        Ok(r) => r,
        Err(e) => return bad_request(e),
    };
    let recorded = inner
        .raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .get_node(&id)
        .cloned();
    match recorded {
        None => return msgpack(&Reply::Ok(Err(format!("node {id} is not a member")))),
        Some(r) if r.addr != node.addr => {
            return msgpack(&Reply::Ok(Err(format!(
                "node {id} is recorded at cluster address {}, not {}; only the API address can change",
                r.addr, node.addr
            ))));
        }
        Some(_) => {}
    }
    match inner
        .raft
        .change_membership(ChangeMembers::SetNodes([(id, node)].into()), false)
        .await
    {
        Ok(_) => msgpack(&Reply::Ok(Ok(()))),
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
            msgpack(&Reply::NotLeader(f.leader_id.zip(f.leader_node)))
        }
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

async fn remove(State(inner): St, body: Bytes) -> Response {
    let id: NodeId = match decode(&body) {
        Ok(id) => id,
        Err(e) => return bad_request(e),
    };
    match inner.remove_voter(id).await {
        Ok(reply) => msgpack(&reply),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e).into_response(),
    }
}

async fn cluster_status(State(inner): St) -> Response {
    axum::Json(crate::status(&inner)).into_response()
}

fn parse_id(hex_id: &str) -> Option<ChunkId> {
    let bytes = hex::decode(hex_id).ok()?;
    Some(ChunkId(bytes.try_into().ok()?))
}

async fn put_chunk(State(inner): St, Path(id): Path<String>, body: Bytes) -> Response {
    let Some(id) = parse_id(&id) else {
        return bad_request("invalid chunk id");
    };
    match inner.engine.chunk_store().put_replica(id, body).await {
        Ok(c) => msgpack(&c.stored_len),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn head_chunk(State(inner): St, Path(id): Path<String>) -> Response {
    match parse_id(&id) {
        Some(id) if inner.engine.chunk_store().has(&id) => StatusCode::OK.into_response(),
        Some(_) => StatusCode::NOT_FOUND.into_response(),
        None => bad_request("invalid chunk id"),
    }
}

#[derive(Deserialize)]
struct LenQuery {
    len: u32,
}

async fn get_chunk(
    State(inner): St,
    Path(id): Path<String>,
    Query(q): Query<LenQuery>,
) -> Response {
    let Some(id) = parse_id(&id) else {
        return bad_request("invalid chunk id");
    };
    let store = inner.engine.chunk_store();
    if !store.has(&id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    match store.get(&id, q.len).await {
        Ok(b) => Response::new(Body::from(b)),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn put_shard(
    State(inner): St,
    Path((id, index)): Path<(String, u8)>,
    body: Bytes,
) -> Response {
    let Some(id) = parse_id(&id) else {
        return bad_request("invalid chunk id");
    };
    // Refuse damaged or mislabelled shards before they reach the disk.
    match crate::erasure::parse(&body) {
        Some(s) if s.index == index => {}
        _ => return bad_request("shard failed verification"),
    }
    match inner.engine.chunk_store().put_shard(id, index, body).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_shard(State(inner): St, Path((id, index)): Path<(String, u8)>) -> Response {
    let Some(id) = parse_id(&id) else {
        return bad_request("invalid chunk id");
    };
    match inner.engine.chunk_store().get_shard(&id, index).await {
        Ok(Some(b)) => Response::new(Body::from(b)),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn list_shards(State(inner): St, Path(id): Path<String>) -> Response {
    match parse_id(&id) {
        Some(id) => msgpack(&inner.engine.chunk_store().shard_indices(&id)),
        None => bad_request("invalid chunk id"),
    }
}
