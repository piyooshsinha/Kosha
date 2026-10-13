import { useEffect, useState } from "react";
import type { ClusterStatus, S3, Stats } from "../api/s3.ts";
import { bytes, count, ratio } from "../format.ts";

export function Overview({ s3 }: { s3: S3 }) {
  const [stats, setStats] = useState<Stats | null>(null);
  const [cluster, setCluster] = useState<ClusterStatus | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    const load = () =>
      Promise.all([s3.stats(), s3.cluster()])
        .then(([s, c]) => {
          if (!live) return;
          setStats(s);
          setCluster(c);
          setError(null);
        })
        .catch((e) => live && setError(String(e.message ?? e)));
    load();
    const t = setInterval(load, 5000);
    return () => {
      live = false;
      clearInterval(t);
    };
  }, [s3]);

  if (error) return <div className="error-box">{error}</div>;
  if (!stats) return <div className="muted pad">Loading…</div>;

  const saved = Math.max(0, stats.logical_bytes - stats.physical_bytes);
  const savedPct = stats.logical_bytes ? (saved / stats.logical_bytes) * 100 : 0;
  const max = Math.max(stats.logical_bytes, 1);
  const clustered = cluster && cluster.mode !== "single-node";

  return (
    <>
      <header className="page-head">
        <div>
          <h1>Overview</h1>
          <p className="muted">
            {clustered
              ? `Node ${cluster!.node_id} of a ${cluster!.nodes?.length ?? 0}-node cluster · ${cluster!.state}`
              : "Single node"}{" "}
            · chunk I/O via <strong>{stats.io_backend}</strong>
          </p>
        </div>
      </header>

      <section className="tiles">
        <Tile label="Objects" value={count(stats.objects)} sub={`${count(stats.buckets)} buckets`} />
        <Tile label="Data stored by clients" value={bytes(stats.logical_bytes)} sub="logical size" />
        <Tile label="On disk" value={bytes(stats.physical_bytes)} sub={clustered ? "per replica" : "after dedup + compression"} />
        <Tile
          label="Space saved"
          value={`${savedPct.toFixed(1)}%`}
          sub={`${bytes(saved)} · ${ratio(stats.overall_ratio)} overall`}
          accent
        />
      </section>

      <section className="card">
        <h2>Where the savings come from</h2>
        <div className="bars">
          <Bar label="Logical" value={stats.logical_bytes} max={max} tone="a" note="what clients uploaded" />
          <Bar label="After dedup" value={stats.unique_bytes} max={max} tone="b" note={`${ratio(stats.dedup_ratio)} from content-defined chunking`} />
          <Bar
            label="After compression"
            value={stats.physical_bytes}
            max={max}
            tone="c"
            note={`${ratio(stats.compression_ratio)} from adaptive zstd`}
          />
        </div>
        <p className="fine">
          {count(stats.chunks)} unique chunks. Identical chunks are stored once across every object and bucket, and
          compression is applied only where it actually shrinks data.
        </p>
      </section>
    </>
  );
}

function Tile({ label, value, sub, accent }: { label: string; value: string; sub: string; accent?: boolean }) {
  return (
    <div className={`tile${accent ? " accent" : ""}`}>
      <div className="tile-label">{label}</div>
      <div className="tile-value">{value}</div>
      <div className="tile-sub">{sub}</div>
    </div>
  );
}

function Bar({ label, value, max, tone, note }: { label: string; value: number; max: number; tone: string; note: string }) {
  const pct = Math.max(0.5, (value / max) * 100);
  return (
    <div className="bar-row">
      <div className="bar-label">{label}</div>
      <div className="bar-track">
        <div className={`bar-fill tone-${tone}`} style={{ width: `${pct}%` }} />
      </div>
      <div className="bar-value">{bytes(value)}</div>
      <div className="bar-note">{note}</div>
    </div>
  );
}
