import { useEffect, useMemo, useState } from "react";
import type { Bucket, Flow, FlowsView, S3 } from "../api/s3.ts";
import { bytes } from "../format.ts";
import type { PushToast } from "./Toasts.tsx";

const flowKey = (f: Flow) => `${f.node}:${f.seq}:${f.committed_ms}`;
const short = (id: string) => id.slice(0, 10);
const ms = (n: number) => (n < 1 ? "<1 ms" : `${Math.round(n)} ms`);
const TONES = ["var(--tone-a)", "var(--tone-b)", "var(--tone-c)"];
const MAX_CHUNK_ROWS = 8;

function ago(t: number, now: number) {
  const s = Math.max(0, Math.round((now - t) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return new Date(t).toLocaleTimeString();
}

/** Random bytes, so the test object does not compress or deduplicate. */
function randomBlob(size: number): Blob {
  const parts: BlobPart[] = [];
  for (let left = size; left > 0; ) {
    const n = Math.min(left, 65536);
    parts.push(crypto.getRandomValues(new Uint8Array(n)));
    left -= n;
  }
  return new Blob(parts, { type: "application/octet-stream" });
}

export function DataFlow({ s3, toast }: { s3: S3; toast: PushToast }) {
  const [view, setView] = useState<FlowsView | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [follow, setFollow] = useState(true);
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    let live = true;
    const load = () =>
      s3
        .flows()
        .then((v) => {
          if (!live) return;
          setView(v);
          setError(null);
          setNow(Date.now());
        })
        .catch((e) => live && setError(String(e.message ?? e)));
    load();
    const t = setInterval(load, 2000);
    return () => {
      live = false;
      clearInterval(t);
    };
  }, [s3]);

  const flows = useMemo(() => view?.flows ?? [], [view]);
  const current = useMemo(() => {
    if (!flows.length) return null;
    if (follow || !selected) return flows[0];
    return flows.find((f) => flowKey(f) === selected) ?? flows[0];
  }, [flows, follow, selected]);

  if (!view) return error ? <div className="error-box">{error}</div> : <div className="muted pad">Loading…</div>;

  if (view.mode === "single-node") {
    return (
      <>
        <header className="page-head">
          <div>
            <h1>Data flow</h1>
            <p className="muted">How writes travel through a cluster.</p>
          </div>
        </header>
        <div className="card">
          <h2>This server runs as a single node</h2>
          <p>
            Every object is split into content-defined chunks, which are deduplicated, compressed and written to this
            node's disk; the metadata is committed to its local database. Start a cluster to watch chunks being
            replicated across nodes and metadata going through the Raft log.
          </p>
        </div>
      </>
    );
  }

  return (
    <>
      <header className="page-head">
        <div>
          <h1>Data flow</h1>
          <p className="muted">
            How each write reaches the cluster: chunks to their replica nodes, metadata through the Raft log. Recorded
            on the node that received the write; updates live.
          </p>
        </div>
      </header>
      {error && <div className="error-box">{error}</div>}

      <TestWrite s3={s3} toast={toast} onSent={() => setFollow(true)} />

      <div className="flow-layout">
        <div className="card flush flow-list">
          <div className="flow-list-head">
            <strong>Recent writes</strong>
            <label className="fine">
              <input type="checkbox" checked={follow} onChange={(e) => setFollow(e.target.checked)} /> follow latest
            </label>
          </div>
          {flows.length === 0 && <div className="empty">No writes yet. Upload a file or send a test write.</div>}
          {flows.map((f) => {
            const k = flowKey(f);
            const active = current && flowKey(current) === k;
            return (
              <button
                key={k}
                className={`flow-item${active ? " active" : ""}`}
                onClick={() => {
                  setSelected(k);
                  setFollow(false);
                }}
              >
                <span className="flow-op">{f.op}</span>
                <span className="flow-key" title={`${f.bucket}/${f.key}`}>
                  {f.bucket}
                  {f.key ? `/${f.key}` : ""}
                </span>
                <span className="fine">
                  {f.size ? bytes(f.size) + " · " : ""}via node {f.node} · {ago(f.committed_ms, now)}
                  {f.error ? " · failed" : ""}
                </span>
              </button>
            );
          })}
        </div>
        <div className="flow-detail">{current ? <FlowDetail flow={current} view={view} /> : null}</div>
      </div>
    </>
  );
}

function TestWrite({ s3, toast, onSent }: { s3: S3; toast: PushToast; onSent: () => void }) {
  const [buckets, setBuckets] = useState<Bucket[] | null>(null);
  const [bucket, setBucket] = useState("");
  const [size, setSize] = useState(3 * 1024 * 1024);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    s3.listBuckets()
      .then((b) => {
        setBuckets(b);
        if (b.length) setBucket((cur) => cur || b[0].name);
      })
      .catch(() => setBuckets([]));
  }, [s3]);

  async function send() {
    setBusy(true);
    try {
      let target = bucket;
      if (!target) {
        target = "flow-demo";
        await s3.createBucket(target);
        setBuckets([{ name: target, created: new Date().toISOString() }]);
        setBucket(target);
      }
      const key = `flow-demo/${new Date().toISOString().replace(/[:.]/g, "-")}.bin`;
      await s3.upload(target, key, randomBlob(size));
      onSent();
      toast("ok", `Wrote ${bytes(size)} to ${target}/${key}`);
    } catch (e) {
      toast("error", (e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="card test-write">
      <div>
        <h2>Send a test write</h2>
        <p className="fine">
          Uploads random bytes to this node and shows where they went. Objects of 8 KiB or less are stored inside the
          metadata; larger ones are split into chunks of about 1 MiB.
        </p>
      </div>
      <div className="inline-form">
        {buckets && buckets.length > 0 ? (
          <select value={bucket} onChange={(e) => setBucket(e.target.value)} aria-label="Bucket">
            {buckets.map((b) => (
              <option key={b.name}>{b.name}</option>
            ))}
          </select>
        ) : (
          <span className="fine">bucket “flow-demo” will be created</span>
        )}
        <select value={size} onChange={(e) => setSize(Number(e.target.value))} aria-label="Size">
          <option value={4096}>4 KiB (inline)</option>
          <option value={512 * 1024}>512 KiB (1 chunk)</option>
          <option value={3 * 1024 * 1024}>3 MiB (a few chunks)</option>
          <option value={16 * 1024 * 1024}>16 MiB (many chunks)</option>
        </select>
        <button className="primary" disabled={busy || buckets === null} onClick={send}>
          {busy ? "Sending…" : "Send"}
        </button>
      </div>
    </div>
  );
}

type Row =
  | { kind: "msg"; from: number; to: number[]; label: string; detail?: string; tone: string; dashed?: boolean }
  | { kind: "note"; at: number; label: string; detail?: string }
  | { kind: "applied"; index: number }
  | { kind: "more"; label: string };

/** Column 0 is the client; node ids map to columns 1..n. */
function FlowDetail({ flow, view }: { flow: Flow; view: FlowsView }) {
  const [replay, setReplay] = useState(0);
  const nodeIds = view.nodes.map((n) => n.id).sort((a, b) => a - b);
  const col = (id: number) => nodeIds.indexOf(id) + 1;
  const CLIENT = 0;
  const leader = flow.leader ?? view.leader;

  const rows: Row[] = [];
  const target = `${flow.bucket}${flow.key ? "/" + flow.key : ""}`;
  rows.push({
    kind: "msg",
    from: CLIENT,
    to: [col(flow.node)],
    label: `${flow.op} ${target.length > 44 ? target.slice(0, 20) + "…" + target.slice(-22) : target}`,
    detail: flow.size ? bytes(flow.size) : undefined,
    tone: "var(--text)",
  });
  if (flow.chunk_count > 0) {
    rows.push({
      kind: "note",
      at: col(flow.node),
      label: `split into ${flow.chunk_count} chunk${flow.chunk_count === 1 ? "" : "s"}`,
      detail: "FastCDC boundaries · BLAKE3 ids",
    });
  } else if (flow.inline_bytes > 0) {
    rows.push({ kind: "note", at: col(flow.node), label: "stored inline in the metadata", detail: "≤ 8 KiB, no chunks" });
  }
  flow.chunks.slice(0, MAX_CHUNK_ROWS).forEach((c, i) => {
    const shards = c.shards.length > 0;
    const to = shards ? c.shards.map(([, n]) => col(n)) : c.replicas.map(col);
    rows.push({
      kind: "msg",
      from: col(flow.node),
      to,
      label: shards
        ? `chunk ${short(c.id)} → ${c.layout} shards`
        : `chunk ${short(c.id)} → ${c.replicas.length} replica${c.replicas.length === 1 ? "" : "s"}`,
      detail: `${bytes(c.len)} · ${ms(c.end_ms - c.start_ms)}${c.failed.length ? ` · node ${c.failed.join(", ")} failed` : ""}`,
      tone: TONES[i % TONES.length],
    });
  });
  const hidden = flow.chunk_count - flow.reused_chunks - Math.min(flow.chunks.length, MAX_CHUNK_ROWS);
  if (hidden > 0) rows.push({ kind: "more", label: `… ${hidden} more chunk${hidden === 1 ? "" : "s"} placed the same way` });
  if (flow.reused_chunks > 0) {
    rows.push({
      kind: "note",
      at: col(flow.node),
      label: `${flow.reused_chunks} chunk${flow.reused_chunks === 1 ? "" : "s"} reused`,
      detail: "already stored; no data copied",
    });
  }
  if (leader != null && flow.log_index != null) {
    if (flow.forwarded) {
      rows.push({
        kind: "msg",
        from: col(flow.node),
        to: [col(leader)],
        label: `${flow.op} command → leader`,
        detail: flow.batch > 1 ? `forwarded · batched with ${flow.batch - 1} more` : "forwarded",
        tone: "var(--accent)",
      });
    } else {
      rows.push({
        kind: "note",
        at: col(leader),
        label: "this node is the leader",
        detail: flow.batch > 1 ? `batched with ${flow.batch - 1} more` : "proposes directly",
      });
    }
    rows.push({
      kind: "msg",
      from: col(leader),
      to: nodeIds.filter((n) => n !== leader).map(col),
      label: `Raft log entry #${flow.log_index}`,
      detail: `committed by a majority · ${ms(flow.committed_ms - flow.proposed_ms)}`,
      tone: "var(--tone-c)",
      dashed: true,
    });
    rows.push({ kind: "applied", index: flow.log_index });
  }
  rows.push({
    kind: "msg",
    from: col(flow.node),
    to: [CLIENT],
    label: flow.error ? `failed: ${flow.error}` : "200 OK",
    detail: `${ms(flow.committed_ms - flow.started_ms)} on the server`,
    tone: flow.error ? "var(--danger)" : "var(--ok)",
  });

  const cols = nodeIds.length + 1;
  const COL_W = 150;
  const LEFT = 96;
  const TOP = 64;
  const ROW_H = 54;
  const width = LEFT * 2 + COL_W * (cols - 1);
  const height = TOP + rows.length * ROW_H + 16;
  const x = (c: number) => LEFT + c * COL_W;
  const applied = new Map(view.nodes.map((n) => [n.id, n]));

  return (
    <div className="card">
      <div className="flow-detail-head">
        <div>
          <h2>
            {flow.op} <span className="mono">{target}</span>
          </h2>
          <p className="fine">
            Received by node {flow.node} · {new Date(flow.started_ms).toLocaleTimeString()}
            {flow.log_index != null && <> · log index {flow.log_index}</>} · replication factor {view.replication_factor}
            {view.erasure && <> · erasure coding {view.erasure}</>}
          </p>
        </div>
        <button className="ghost small" onClick={() => setReplay((r) => r + 1)}>
          Replay
        </button>
      </div>
      <div className="seq-scroll">
        <svg key={`${flowKey(flow)}:${replay}`} className="seq" viewBox={`0 0 ${width} ${height}`} width={width} role="img" aria-label="Sequence of the write">
          <defs>
            {["text", "accent", "tone-a", "tone-b", "tone-c", "ok", "danger"].map((t) => (
              <marker key={t} id={`seq-${t}`} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
                <path d="M0,0 L10,5 L0,10 z" fill={`var(--${t})`} />
              </marker>
            ))}
          </defs>
          {Array.from({ length: cols }, (_, c) => {
            const id = nodeIds[c - 1];
            const isLeader = c > 0 && id === leader;
            return (
              <g key={c}>
                <line x1={x(c)} y1={TOP - 14} x2={x(c)} y2={height - 8} className="seq-life" />
                <rect x={x(c) - 56} y={8} width={112} height={40} rx={8} className={`seq-head${isLeader ? " leader" : ""}${c === 0 ? " client" : ""}`} />
                <text x={x(c)} y={26} textAnchor="middle" className="seq-head-name">
                  {c === 0 ? "Client" : `Node ${id}`}
                </text>
                <text x={x(c)} y={41} textAnchor="middle" className="seq-head-sub">
                  {c === 0 ? "S3 API" : isLeader ? "leader" : applied.get(id)?.reachable === false ? "unreachable" : "follower"}
                </text>
              </g>
            );
          })}
          {rows.map((r, i) => {
            const y = TOP + i * ROW_H;
            const style = { animationDelay: `${i * 0.35}s` };
            if (r.kind === "note") {
              return (
                <g key={i} className="seq-step" style={style}>
                  <rect x={x(r.at) - 86} y={y + 6} width={172} height={38} rx={6} className="seq-note" />
                  <text x={x(r.at)} y={y + 22} textAnchor="middle" className="seq-label">
                    {r.label}
                  </text>
                  {r.detail && (
                    <text x={x(r.at)} y={y + 37} textAnchor="middle" className="seq-detail">
                      {r.detail}
                    </text>
                  )}
                </g>
              );
            }
            if (r.kind === "more") {
              return (
                <text key={i} x={width / 2} y={y + 28} textAnchor="middle" className="seq-detail seq-step" style={style}>
                  {r.label}
                </text>
              );
            }
            if (r.kind === "applied") {
              return (
                <g key={i} className="seq-step" style={style}>
                  {nodeIds.map((id, c) => {
                    const n = applied.get(id);
                    const done = n?.last_applied != null && n.last_applied >= r.index;
                    return (
                      <g key={id}>
                        <circle cx={x(c + 1)} cy={y + 20} r={11} className={done ? "seq-applied" : "seq-pending"} />
                        <text x={x(c + 1)} y={y + 24.5} textAnchor="middle" className="seq-check">
                          {done ? "✓" : "…"}
                        </text>
                        <text x={x(c + 1)} y={y + 46} textAnchor="middle" className="seq-detail">
                          {done ? "applied" : n?.reachable === false ? "unreachable" : "applying"}
                        </text>
                      </g>
                    );
                  })}
                </g>
              );
            }
            const marker = `url(#seq-${r.tone.replace(/^var\(--(.*)\)$/, "$1")})`;
            const fx = x(r.from);
            const wantX = r.to.length === 1 ? (fx + x(r.to[0])) / 2 : fx + (Math.max(...r.to.map(x)) > fx ? 70 : -70);
            // Keep the label inside the diagram (about 6.6 px per character).
            const half = (r.label.length * 6.6) / 2 + 4;
            const labelX = Math.min(Math.max(wantX, half), width - half);
            return (
              <g key={i} className="seq-step" style={style}>
                <text x={labelX} y={y + 16} textAnchor="middle" className="seq-label">
                  {r.label}
                </text>
                {r.detail && (
                  <text x={labelX} y={y + 48} textAnchor="middle" className="seq-detail">
                    {r.detail}
                  </text>
                )}
                {r.to.map((t, j) => {
                  if (t === r.from) {
                    // Stored on the node itself.
                    return (
                      <g key={j}>
                        <circle cx={fx} cy={y + 29} r={7} fill={r.tone} />
                        <text x={fx} y={y + 49} className="seq-detail" textAnchor="middle">
                          local
                        </text>
                      </g>
                    );
                  }
                  const tx = x(t);
                  const dir = tx > fx ? 1 : -1;
                  return (
                    <line
                      key={j}
                      x1={fx + dir * 4}
                      y1={y + 29}
                      x2={tx - dir * 6}
                      y2={y + 29}
                      stroke={r.tone}
                      strokeWidth={2}
                      strokeDasharray={r.dashed ? "6 4" : undefined}
                      markerEnd={marker}
                      className="seq-arrow"
                    />
                  );
                })}
              </g>
            );
          })}
        </svg>
      </div>
      {flow.chunks.length > 0 && (
        <table className="table compact">
          <thead>
            <tr>
              <th>Chunk</th>
              <th className="right">Size</th>
              <th className="right">On disk</th>
              <th>Stored on</th>
              <th className="right">Time</th>
            </tr>
          </thead>
          <tbody>
            {flow.chunks.map((c, i) => (
              <tr key={c.id + i}>
                <td className="mono" title={c.id}>
                  <span className="dot" style={{ background: TONES[i % TONES.length] }} />
                  {short(c.id)}…
                </td>
                <td className="right mono">{bytes(c.len)}</td>
                <td className="right mono">{bytes(c.stored)}</td>
                <td>
                  {c.shards.length
                    ? c.shards.map(([s, n]) => `shard ${s} → node ${n}`).join(", ")
                    : c.replicas.map((n) => `node ${n}`).join(", ")}
                  {c.failed.length > 0 && <span className="lag"> (node {c.failed.join(", ")} failed)</span>}
                </td>
                <td className="right mono">{ms(c.end_ms - c.start_ms)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <p className="fine flow-explain">
        Each chunk goes to the {view.replication_factor} nodes that rank highest for its id under rendezvous hashing, so
        any node can compute where a chunk lives without a lookup table. The object's metadata (its list of chunks)
        becomes one Raft log entry: the leader commits it once a majority has it, and every node applies it, so a read
        on any node sees the write.
      </p>
    </div>
  );
}
