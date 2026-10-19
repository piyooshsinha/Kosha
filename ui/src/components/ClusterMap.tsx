/** A live map of the cluster: every member, the leader, and the Raft
 * replication links from the leader to each follower. */

export interface MapNode {
  id: number;
  role: "leader" | "follower" | "learner" | "down";
  /** Second line under the node name, e.g. "log 812". */
  sub: string;
  /** S3 endpoint, shown on the third line. */
  api: string;
  you: boolean;
  /** Entries this follower is behind the leader (leader view only). */
  lag?: number | null;
}

const W = 640;
const H = 290;
const BOX_W = 176;
const BOX_H = 70;

function positions(n: number): { x: number; y: number }[] {
  if (n === 1) return [{ x: W / 2, y: H / 2 }];
  const rx = n <= 3 ? 200 : 225;
  const ry = n <= 3 ? 78 : 95;
  // Start at the top so the leader (sorted first) sits above the others.
  return Array.from({ length: n }, (_, i) => {
    const a = -Math.PI / 2 + (i * 2 * Math.PI) / n;
    return { x: W / 2 + rx * Math.cos(a), y: H / 2 + ry * Math.sin(a) + (n === 2 ? 0 : 10) };
  });
}

/** Where the segment from a box centre towards (tx, ty) leaves the box. */
function edge(x: number, y: number, tx: number, ty: number) {
  const dx = tx - x;
  const dy = ty - y;
  const s = Math.min(Math.abs((BOX_W / 2 + 6) / (dx || 1e-9)), Math.abs((BOX_H / 2 + 6) / (dy || 1e-9)));
  return { x: x + dx * s, y: y + dy * s };
}

export function ClusterMap({ nodes, leader }: { nodes: MapNode[]; leader: number | null }) {
  const sorted = [...nodes].sort((a, b) => (a.id === leader ? -1 : b.id === leader ? 1 : a.id - b.id));
  const pos = positions(sorted.length);
  const at = new Map(sorted.map((n, i) => [n.id, pos[i]]));
  const lp = leader != null ? at.get(leader) : undefined;

  return (
    <svg className="cluster-map" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Cluster map">
      <defs>
        <marker id="map-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0,0 L10,5 L0,10 z" fill="var(--tone-c)" />
        </marker>
      </defs>
      {lp &&
        sorted
          .filter((n) => n.id !== leader)
          .map((n) => {
            const p = at.get(n.id)!;
            const a = edge(lp.x, lp.y, p.x, p.y);
            const b = edge(p.x, p.y, lp.x, lp.y);
            const down = n.role === "down";
            // Label beside the middle of the link, on the side away from the
            // centre of the map so it never sits on another link.
            const dx = b.x - a.x;
            const dy = b.y - a.y;
            const len = Math.hypot(dx, dy) || 1;
            let nx = -dy / len;
            let ny = dx / len;
            const cx0 = (a.x + b.x) / 2;
            const cy0 = (a.y + b.y) / 2;
            if (nx * (cx0 - W / 2) + ny * (cy0 - H / 2) < 0) {
              nx = -nx;
              ny = -ny;
            }
            const mx = cx0 + nx * 16;
            const my = cy0 + ny * 16 + 4;
            const anchor = Math.abs(nx) < 0.3 ? "middle" : nx > 0 ? "start" : "end";
            const label = down ? "unreachable" : n.lag == null ? "log replication" : n.lag ? `${n.lag} behind` : "in sync";
            return (
              <g key={n.id}>
                <line
                  x1={a.x}
                  y1={a.y}
                  x2={b.x}
                  y2={b.y}
                  className={down ? "map-link down" : "map-link"}
                  markerEnd={down ? undefined : "url(#map-arrow)"}
                />
                <text x={mx} y={my} className={`map-label${n.lag ? " lagging" : ""}`} textAnchor={anchor}>
                  {label}
                </text>
              </g>
            );
          })}
      {sorted.map((n) => {
        const p = at.get(n.id)!;
        return (
          <g key={n.id} className={`map-node ${n.role}`} transform={`translate(${p.x - BOX_W / 2} ${p.y - BOX_H / 2})`}>
            <rect width={BOX_W} height={BOX_H} rx={10} />
            <text x={14} y={23} className="map-name">
              Node {n.id}
              {n.you ? " · you" : ""}
            </text>
            <text x={14} y={42} className="map-sub">
              {n.role === "down" ? "unreachable" : n.sub ? `${n.role} · ${n.sub}` : n.role}
            </text>
            <text x={14} y={59} className="map-api">
              {n.api}
            </text>
            {n.role === "leader" && <circle cx={BOX_W - 16} cy={16} r={5} className="map-crown" />}
          </g>
        );
      })}
    </svg>
  );
}
