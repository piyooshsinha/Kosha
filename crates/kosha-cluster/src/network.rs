//! openraft network adapter over the internal HTTP transport.

use std::time::Duration;

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::rpc::{Client, RpcError};
use crate::types::*;

pub struct NetworkFactory {
    pub client: Client,
}

pub struct Peer {
    client: Client,
    target: NodeId,
    addr: String,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Peer;

    async fn new_client(&mut self, target: NodeId, node: &KoshaNode) -> Peer {
        Peer {
            client: self.client.clone(),
            target,
            addr: node.addr.clone(),
        }
    }
}

type RpcResult<T, E = openraft::error::Infallible> =
    Result<T, RPCError<NodeId, KoshaNode, RaftError<NodeId, E>>>;

impl Peer {
    async fn call<Req, Resp, E>(&self, path: &str, req: &Req, opt: &RPCOption) -> RpcResult<Resp, E>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
        E: std::error::Error + serde::de::DeserializeOwned,
    {
        let timeout = opt.hard_ttl().max(Duration::from_millis(100));
        let reply: Result<Resp, RaftError<NodeId, E>> = self
            .client
            .call(&self.addr, path, req, timeout)
            .await
            .map_err(|e| match e {
                RpcError::Unreachable(_) => RPCError::Unreachable(Unreachable::new(&e)),
                other => RPCError::Network(NetworkError::new(&other)),
            })?;
        reply.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

impl RaftNetwork<TypeConfig> for Peer {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<AppendEntriesResponse<NodeId>> {
        self.call("/raft/append", &rpc, &option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<InstallSnapshotResponse<NodeId>, InstallSnapshotError> {
        self.call("/raft/snapshot", &rpc, &option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> RpcResult<VoteResponse<NodeId>> {
        self.call("/raft/vote", &rpc, &option).await
    }
}
