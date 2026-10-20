import { useEffect, useState } from "react";
import type { ClusterStatus, S3 } from "../api/s3.ts";
import type { PushToast } from "./Toasts.tsx";
import { ClusterMap, type MapNode } from "./ClusterMap.tsx";

export function ClusterView({ s3, toast }: { s3: S3; toast: PushToast }) {
  const [status, setStatus] = useState<ClusterStatus | null>(null);
  const [removing, setRemoving] = useState<number | null>(null);
  const [newAddr, setNewAddr] = useState("");
  const [adding, setAdding] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [updated, setUpdated] = useState<Date | null>(null);
  // Which members answered a direct probe (the leader keeps a stopped
  // follower's last replicated index, so that alone cannot show it is down).
  const [reachable, setReachable] = useState<Map<number, boolean>>(new Map());

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
    const probe = () =>
      s3
        .flows(0)
        .then((v) => live && v.nodes && setReachable(new Map(v.nodes.map((n) => [n.id, n.reachable]))))
        .catch(() => {});
    load();
    probe();
    const t = setInterval(load, 2000);
    const p = setInterval(probe, 4000);
    return () => {
      live = false;
      clearInterval(t);
      clearInterval(p);
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
          <p>
            To try one on a single Docker host, use <code>docker-compose.cluster.yml</code> from the repository (see the
            README). On separate machines, run each node with a unique id and a shared secret, bootstrap the first one
            and join the rest through it:
          </p>
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
    if (!confirm(`${warn}\n\nIts chunks will be re-replicated to the remaining nodes. While its process runs, you can add it back with Add a node.`)) return;
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
  async function add(e: React.FormEvent) {
    e.preventDefault();
    setAdding(true);
    try {
      const id = await s3.addNode(newAddr.trim());
      toast("ok", `Node ${id} added; it catches up in the background`);
      setNewAddr("");
    } catch (err) {
      toast("error", (err as Error).message);
    } finally {
      setAdding(false);
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

      <div className="card map-card">
        <div className="map-head">
          <h2>Topology</h2>
          <a className="fine" href="#/flow">
            See how data reaches the nodes →
          </a>
        </div>
        <ClusterMap
          leader={status.leader ?? null}
          nodes={nodes.map(
            (n): MapNode => ({
              id: n.id,
              role:
                reachable.get(n.id) === false || (isLeader && n.matched == null && n.id !== status.leader)
                  ? "down"
                  : n.id === status.leader
                    ? "leader"
                    : n.voter
                      ? "follower"
                      : "learner",
              sub: n.id === status.leader ? `log ${lastIndex}` : n.matched != null ? `log ${n.matched}` : "",
              api: n.api || n.addr,
              you: n.id === status.node_id,
              lag: isLeader && n.matched != null ? lastIndex - n.matched : null,
            }),
          )}
        />
      </div>

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
                      {reachable.get(n.id) === false && n.id !== status.leader ? (
                        <span className="lag">unreachable{n.matched != null ? ` (had ${n.matched})` : ""}</span>
                      ) : n.matched != null ? (
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
      <div className="card add-node">
        <div>
          <h2>Add a node</h2>
          <p className="fine">
            Start Kosha on the new machine with this cluster&rsquo;s secret, its own node id and no <code>--init</code> or <code>--join</code>,
            then enter the address of its cluster port. A removed node that is still running can be added back the same
            way.
          </p>
        </div>
        <form className="inline-form" onSubmit={add}>
          <input
            value={newAddr}
            onChange={(e) => setNewAddr(e.target.value)}
            placeholder="host:9100"
            aria-label="New node's cluster address"
            spellCheck={false}
          />
          <button className="primary" disabled={adding || !newAddr.trim()}>
            {adding ? "Adding…" : "Add node"}
          </button>
        </form>
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
