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
use kosha_engine::{ChunkId, Command};
use openraft::ChangeMembers;
use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use serde::Deserialize;

use crate::Inner;
use crate::rpc::{JoinRequest, LeaderReply, Proposed, SECRET_HEADER, decode, encode};
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
        .route("/chunk/{id}", get(get_chunk).put(put_chunk))
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
    let cmd: Command = match decode(&body) {
        Ok(c) => c,
        Err(e) => return bad_request(e),
    };
    match inner.raft.client_write(cmd).await {
        Ok(resp) => match resp.data.0 {
            Some(result) => msgpack(&LeaderReply::Ok(Proposed {
                result,
                index: resp.log_id.index,
            })),
            None => (StatusCode::INTERNAL_SERVER_ERROR, "missing result").into_response(),
        },
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
            msgpack(&LeaderReply::<Proposed>::NotLeader(
                f.leader_id.zip(f.leader_node),
            ))
        }
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

async fn read_index(State(inner): St) -> Response {
    match inner.raft.ensure_linearizable().await {
        Ok(log_id) => msgpack(&LeaderReply::Ok(log_id.map(|l| l.index))),
        Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => msgpack(
            &LeaderReply::<Option<u64>>::NotLeader(f.leader_id.zip(f.leader_node)),
        ),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

async fn join(State(inner): St, body: Bytes) -> Response {
    let req: JoinRequest = match decode(&body) {
        Ok(r) => r,
        Err(e) => return bad_request(e),
    };
    // Catch the new node up as a learner first, then promote it.
    match inner.raft.add_learner(req.id, req.node.clone(), true).await {
        Ok(_) => {}
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
            return msgpack(&LeaderReply::<()>::NotLeader(
                f.leader_id.zip(f.leader_node),
            ));
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
            msgpack(&LeaderReply::Ok(()))
        }
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => msgpack(
            &LeaderReply::<()>::NotLeader(f.leader_id.zip(f.leader_node)),
        ),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
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
