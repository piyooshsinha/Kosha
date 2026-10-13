import { useEffect, useState } from "react";
import type { ClusterStatus, S3 } from "../api/s3.ts";
import type { PushToast } from "./Toasts.tsx";

export function ClusterView({ s3, toast }: { s3: S3; toast: PushToast }) {
  const [status, setStatus] = useState<ClusterStatus | null>(null);
  const [removing, setRemoving] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [updated, setUpdated] = useState<Date | null>(null);

  useEffect(() => {
    let live = true;
    const load = () =>
      s3
        .cluster()
        .then((s) => {
          if (!live) return;
          setStatus(s);
          setError(null);
          setUpdated(new Date());
        })
        .catch((e) => live && setError(String(e.message ?? e)));
    load();
    const t = setInterval(load, 2000);
    return () => {
      live = false;
      clearInterval(t);
    };
  }, [s3]);

  if (!status) return error ? <div className="error-box">{error}</div> : <div className="muted pad">Loading…</div>;

  if (status.mode === "single-node") {
    return (
      <>
        <header className="page-head">
          <div>
            <h1>Cluster</h1>
            <p className="muted">This server runs as a single node.</p>
          </div>
        </header>
        <div className="card">
          <h2>Start a cluster</h2>
          <p>Run each node with a unique id and a shared secret. Bootstrap the first one, then join the rest through it:</p>
          <pre className="code">{`export KOSHA_CLUSTER_SECRET=$(openssl rand -hex 24)
kosha --node-id 1 --cluster-listen 10.0.0.1:9100 --init
kosha --node-id 2 --cluster-listen 10.0.0.2:9100 --join 10.0.0.1:9100
kosha --node-id 3 --cluster-listen 10.0.0.3:9100 --join 10.0.0.1:9100`}</pre>
        </div>
      </>
    );
  }

  const nodes = status.nodes ?? [];
  const voters = nodes.filter((n) => n.voter).length;
  const quorum = Math.floor(voters / 2) + 1;
  const lastIndex = status.last_log_index ?? 0;
  const isLeader = status.leader === status.node_id;
  const barrier = (status.lease_reads ?? 0) + (status.quorum_reads ?? 0);
  const leasePct = barrier ? Math.round(((status.lease_reads ?? 0) / barrier) * 100) : null;
  const repair = status.repair;

  async function remove(id: number) {
    const warn =
      id === status!.node_id
        ? `Remove node ${id} — the node serving this console — from the cluster?`
        : `Remove node ${id} from the cluster?`;
    if (!confirm(`${warn}\n\nIts chunks will be re-replicated to the remaining nodes. Stop the process after removal.`)) return;
    setRemoving(id);
    try {
      await s3.removeNode(id);
      toast("ok", `Node ${id} removed; re-replication runs in the background`);
    } catch (e) {
      toast("error", (e as Error).message);
    } finally {
      setRemoving(null);
    }
  }
  const leaderNode = nodes.find((n) => n.id === status.leader);

  return (
    <>
      <header className="page-head">
        <div>
          <h1>Cluster</h1>
          <p className="muted">
            Raft term {status.term} · {voters} voters (quorum {quorum}, tolerates {voters - quorum} failure
            {voters - quorum === 1 ? "" : "s"}) · {status.replication_factor}× chunk replication
            {updated && <> · updated {updated.toLocaleTimeString()}</>}
          </p>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}

      <section className="tiles">
        <div className="tile">
          <div className="tile-label">Leader</div>
          <div className="tile-value">{status.leader != null ? `Node ${status.leader}` : "electing…"}</div>
          <div className="tile-sub">{status.leader === status.node_id ? "this node" : "this console is on node " + status.node_id}</div>
        </div>
        <div className="tile">
          <div className="tile-label">Log index</div>
          <div className="tile-value">{status.last_log_index ?? "—"}</div>
          <div className="tile-sub">applied {status.last_applied ?? "—"}</div>
        </div>
        <div className="tile">
          <div className="tile-label">Snapshot</div>
          <div className="tile-value">{status.snapshot_index ?? "none"}</div>
          <div className="tile-sub">last compacted index</div>
        </div>
        <div className="tile">
          <div className="tile-label">Lease reads</div>
          <div className="tile-value">{isLeader && leasePct != null ? `${leasePct}%` : "—"}</div>
          <div className="tile-sub">
            {isLeader ? `${(status.lease_reads ?? 0).toLocaleString()} without a quorum round-trip` : "tracked on the leader"}
          </div>
        </div>
        {repair && (
          <div className="tile">
            <div className="tile-label">Background repair</div>
            <div className="tile-value">{repair.repaired.toLocaleString()}</div>
            <div className="tile-sub">
              replicas restored · {repair.surplus_removed.toLocaleString()} surplus removed · {repair.rounds} passes
              {repair.failed ? ` · ${repair.failed} failed` : ""}
            </div>
          </div>
        )}
      </section>

      <div className="card flush">
        <table className="table">
          <thead>
            <tr>
              <th>Node</th>
              <th>Role</th>
              <th>S3 endpoint</th>
              <th>Cluster address</th>
              {isLeader && <th className="right">Replicated to</th>}
              <th className="right" />
            </tr>
          </thead>
          <tbody>
            {nodes.map((n) => {
              const role = n.id === status.leader ? "leader" : n.voter ? "follower" : "learner";
              const lag = n.matched != null ? lastIndex - n.matched : null;
              return (
                <tr key={n.id}>
                  <td>
                    <strong>Node {n.id}</strong>
                    {n.id === status.node_id && <span className="tag">you</span>}
                  </td>
                  <td>
                    <span className={`badge ${role}`}>{role}</span>
                  </td>
                  <td className="mono">{n.api || "—"}</td>
                  <td className="mono">{n.addr}</td>
                  {isLeader && (
                    <td className="right mono">
                      {n.matched != null ? (
                        <>
                          {n.matched}
                          {lag ? <span className="lag"> −{lag}</span> : <span className="ok"> ✓</span>}
                        </>
                      ) : (
                        <span className="lag">unreachable</span>
                      )}
                    </td>
                  )}
                  <td className="right">
                    {n.voter && voters > 1 && (
                      <button className="ghost small danger" disabled={removing !== null} onClick={() => remove(n.id)}>
                        {removing === n.id ? "Removing…" : "Remove"}
                      </button>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {!isLeader && leaderNode?.api && (
        <p className="fine">
          Per-node replication progress is tracked by the leader.{" "}
          <a href={`${location.protocol}//${leaderNode.api}/_kosha/ui/#/cluster`}>Open the console on node {leaderNode.id}</a>{" "}
          to see it (you will sign in there separately).
        </p>
      )}
    </>
  );
}
