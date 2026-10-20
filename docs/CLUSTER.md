# Clusters and monitoring

Running Kosha on several nodes, adding and removing nodes, and monitoring.

## Run a cluster

Every node serves the full S3 API, so clients can use any node.

- **Metadata** is replicated through Raft.
- **Chunks** are stored on `--replication` nodes (default 2), chosen by
  rendezvous hashing.
- **Fault tolerance**: a 3-node cluster survives the loss of any one node.

On three machines (10.0.0.1–3):

```sh
# the same on every node
export KOSHA_ACCESS_KEY=admin KOSHA_SECRET_KEY=change-me-please
export KOSHA_CLUSTER_SECRET=<a long random string, e.g. openssl rand -hex 24>

# node 1 bootstraps the cluster
kosha --node-id 1 --data-dir /var/lib/kosha --listen 0.0.0.0:9000 \
      --cluster-listen 10.0.0.1:9100 --init

# nodes 2 and 3 join through any existing member
kosha --node-id 2 --data-dir /var/lib/kosha --listen 0.0.0.0:9000 \
      --cluster-listen 10.0.0.2:9100 --join 10.0.0.1:9100
kosha --node-id 3 --data-dir /var/lib/kosha --listen 0.0.0.0:9000 \
      --cluster-listen 10.0.0.3:9100 --join 10.0.0.1:9100
```

Things to know:

- `--init` and `--join` only matter the first time. A restarted node rejoins
  from its own Raft state, so the same command line is safe to reuse.
- Each node needs a unique `--node-id`, and `--cluster-listen` must be
  reachable from the other nodes. Behind NAT, set `--cluster-advertise` to
  the address peers should dial.
- **Writes** to a follower are forwarded to the leader. The follower answers
  only after it has applied the write itself, so you always read your own
  writes.
- **Reads** are linearizable on every node: a read never returns data older
  than a write that already completed anywhere in the cluster. The leader
  serves them from a **leader lease** instead of a quorum round-trip per read
  (in benchmarks, over 99% of read barriers). `--no-lease-reads` turns this
  off if your clocks are unreliable.
- **Failure**: if the leader dies, the others elect a new one within about
  1–2 seconds and keep serving. When the dead node comes back, it catches up
  from the log, or from a snapshot if the log was compacted.
- **Self-healing**: every node runs a background repairer
  (`--repair-interval-secs`, default 60). When a node stops answering, the
  next-ranked live nodes copy the chunks it held, so every chunk gets back to
  `--replication` copies without waiting for reads. Surplus copies, for
  example fallback replicas written while a node was down, are removed once
  the designated nodes confirm they have theirs.
- **Adding a node**: start it with the cluster secret, a new `--node-id` and
  neither `--init` nor `--join`, then enter its cluster address
  (`--cluster-listen`/`--cluster-advertise`, e.g. `10.0.0.4:9100`) under
  **Add a node** on the console's Cluster page, or `POST
  /_kosha/cluster/nodes` with `{"address": "10.0.0.4:9100"}`. Kosha asks the
  node who it is, refuses one that belongs to another cluster or uses a
  different encryption key or erasure layout, then adds it as a voter; it
  catches up in the background. (Starting it with `--join 10.0.0.1:9100`
  does the same without the console.)
- **Removing a node**: click **Remove** in the console's Cluster page, or
  call `DELETE /_kosha/cluster/nodes/<id>` (SigV4-signed) on any node. Its
  data is re-replicated in the background. Stop the process afterwards, or
  add it back later with **Add a node** while it still runs.
- **A removed node** notices within a few seconds, even if it was a
  follower that never saw its own removal (it asks its peers). It then
  stops calling elections, answers S3 requests with an immediate `503`
  ("node N was removed from the cluster"), reports `503` on
  `/_kosha/health`, and its console says so and links to the members.
  The remaining nodes elect their own leader and keep serving.
- Every add and remove is logged on the node that received it, with the
  access key, the client address and any `X-Forwarded-For`
  (`cluster membership: node removed node=3 key="admin" source=…`).
- **Which node am I talking to?** Clients don't need to know: every node
  serves every request, so put the nodes behind one address (a load
  balancer or DNS name) and let it pick a live one. Have the load
  balancer check `/_kosha/health` so it also skips nodes that were
  removed, for example with Caddy:

  ```
  reverse_proxy 10.0.0.1:9000 10.0.0.2:9000 10.0.0.3:9000 {
      lb_policy first
      health_uri /_kosha/health
      health_interval 5s
      fail_duration 10s
      flush_interval -1
  }
  ```

  Every response carries `x-kosha-node: <id>`, so you can see which node served a request
  (`curl -sI http://<server>:9000/_kosha/health | grep x-kosha-node`), and
  the console's Data flow page shows which node received each write.
- Internal traffic (port 9100) is authenticated by the shared secret. Without
  `--cluster-tls-*` it is plain HTTP, so **either enable mutual TLS
  ([Security](SECURITY.md#cluster-mutual-tls)) or keep it on a private network.**

### Erasure coding

By default each chunk is stored as `--replication` full copies. With
`--erasure K+M` (the same on every node), chunks of at least
`--erasure-min-chunk` bytes (default 128 KiB) are instead split into K data
and M parity shards on K+M different nodes. Any K shards rebuild the chunk.

| | Storage | Survives |
|---|---|---|
| `--replication 2` | 2× | 1 failed node |
| `--replication 3` | 3× | 2 failed nodes |
| `--erasure 2+1` | 1.5× | 1 failed node |
| `--erasure 4+2` | 1.5× | 2 failed nodes |
| `--erasure 8+3` | 1.375× | 3 failed nodes |

- **What gets split** is the chunk's stored form, already compressed and
  encrypted. Every shard carries a BLAKE3 checksum, so a damaged shard is
  detected and treated as missing.
- **Healthy reads** fetch only the K data shards and concatenate them, with
  no decoding.
- **Degraded reads** use parity, and the result is verified against the
  chunk's address.
- **Repair:** when a node dies, the next node in the chunk's ranking rebuilds
  its shards from any K others. Only that node's shards move, and the
  repaired data is re-verified like everything else.
- **Small chunks**, and clusters with fewer than K+M nodes, use replication.
  Reads try both layouts, so changing the setting later is safe.
- **For self-healing**, run at least K+M+1 nodes, so a failed node's shards
  have somewhere to go.
- A node with a different `--erasure` setting is refused when it joins.

**Hot replicated, cold erasure coded.** With `--erasure K+M
--erasure-cold-only`:

- **Writes** stay replicated, so hot reads come from a full copy.
- **When a chunk goes cold** (every object using it is in a cold class, via
  lifecycle, a cold upload, or access-based tiering), the leader converts it
  to shards in the background, and every node drops its full copy.
- **No external tier needed:** this is the cold tier when no
  `--cold-tier-endpoint` is configured, so cold storage classes and
  Intelligent-Tiering work without one.
- **Reads** use the layout recorded in metadata, so hot reads never probe
  for shards.
- **Tested:** in a 6-node test, an object went from 2 replicas (2×) to 3+2
  shards (1.67×). It read correctly on every node, and repair rebuilt a dead
  node's shards.

Measured on 4 nodes (24 × 16 MiB objects, same VM, alternating runs):

| | Disk used | PUT | GET |
|---|---|---|---|
| `--replication 2` | 774 MB | 97–101 MiB/s | 681–693 MiB/s |
| `--erasure 2+1` | **585 MB** (−24%) | 95–106 MiB/s | 550–571 MiB/s |

GETs are ~18% slower because every chunk is gathered from two nodes; writes
are unchanged.

In a test, a node was SIGKILLed while holding 50 MB of 2+1 data. Reads stayed
correct, and repair rebuilt its 33 shards on the remaining nodes (back to
1.5×).

### Try a cluster on one machine

```sh
scripts/dev-cluster.sh start      # 3 nodes: S3 on :9001-9003, console on http://127.0.0.1:9001/_kosha/ui/
scripts/dev-cluster.sh kill 1     # SIGKILL node 1 and watch a new leader take over
scripts/dev-cluster.sh restart 1  # it rejoins and catches up
scripts/dev-cluster.sh stop

# remove a node for good (any node accepts the request)
curl -X DELETE --aws-sigv4 "aws:amz:us-east-1:s3" --user admin:admin-secret-key \
     http://127.0.0.1:9001/_kosha/cluster/nodes/3
```

## Monitoring

| Endpoint | Auth | Returns |
|---|---|---|
| `GET /_kosha/health` | none | `ok`, for load balancer and liveness probes; `503` on a node that was removed from the cluster |
| `GET /_kosha/stats` | SigV4 | objects, logical/unique/physical bytes, dedup and compression ratios, I/O backend |
| `GET /_kosha/metrics` | SigV4, or none with `--metrics-public` | Prometheus: requests and latency histograms per S3 operation, bytes in/out, storage gauges, group commit/sync batching, Raft state, follower lag, lease/quorum reads, repair counters, pending/delivered/failed events |
| `GET /_kosha/cluster` | SigV4 | role, term, leader, log/applied indexes, members, replication progress, lease vs quorum reads, background repair counters |
| `POST /_kosha/cluster/nodes` | SigV4 | adds a running node by its cluster address: `{"address": "host:9100"}` → `{"id": 4}` |
| `DELETE /_kosha/cluster/nodes/{id}` | SigV4 | removes a voter (refuses non-voters and the last voter) |
| `/_kosha/admin/users[/{key}]` | SigV4 | user management (see [Security](SECURITY.md#users-and-policies)) |

Every `/_kosha/` endpoint except `health` (and `metrics` with
`--metrics-public`) requires a **root** key; user keys get `403`.

```sh
curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user admin:change-me-please http://localhost:9000/_kosha/stats
# {"io_backend":"io_uring","buckets":2,"objects":7,"logical_bytes":832569357,"unique_bytes":292552704,
#  "physical_bytes":292552944,"chunks":240,"dedup_ratio":2.846,"compression_ratio":1.000,"overall_ratio":2.846}
```

Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: kosha
    metrics_path: /_kosha/metrics
    static_configs: [{ targets: ["10.0.0.1:9000", "10.0.0.2:9000", "10.0.0.3:9000"] }]
```

(start the nodes with `--metrics-public`). Logs go to stderr and are
filtered with `RUST_LOG`. Per-request lines are at debug level, for example
`RUST_LOG=kosha_s3=debug`; at the default level the metrics are the
per-request record.
