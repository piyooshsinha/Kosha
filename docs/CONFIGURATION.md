# Configuration

Every server flag and its environment variable.

## Configuration

| Flag | Env | Default | |
|---|---|---|---|
| `--listen` | `KOSHA_LISTEN` | `127.0.0.1:9000` | S3 API + console |
| `--data-dir` | `KOSHA_DATA_DIR` | `./data` | metadata, chunks, Raft log |
| `--access-key` / `--secret-key` | `KOSHA_ACCESS_KEY` / `KOSHA_SECRET_KEY` | none | required unless `--anonymous` |
| `--anonymous` | `KOSHA_ANONYMOUS` | off | no auth, for local development only |
| `--region` | `KOSHA_REGION` | `us-east-1` | reported region (signatures for any region are accepted) |
| `--domain` | `KOSHA_DOMAIN` | none | enables virtual-hosted style, `bucket.<domain>` |
| `--compression-level` | `KOSHA_COMPRESSION_LEVEL` | `3` | zstd level, `0` disables |
| `--sync` | `KOSHA_SYNC` | `group` | chunk durability: `group` (shared `syncfs`), `file` (fsync file + directory), `none` |
| `--no-fsync` | `KOSHA_NO_FSYNC` | off | shorthand for `--sync none` (metadata commits stay durable) |
| `--metrics-public` | `KOSHA_METRICS_PUBLIC` | off | serve `/_kosha/metrics` without authentication |
| `--io` | `KOSHA_IO` | `auto` | `auto`, `uring` (required), or `threads` |
| `--io-direct` | `KOSHA_IO_DIRECT` | off | `O_DIRECT` with registered buffers for large chunk files (io_uring only) |
| `--io-direct-min` | `KOSHA_IO_DIRECT_MIN` | `262144` | smallest chunk file that uses `O_DIRECT` |
| `--node-id` | `KOSHA_NODE_ID` | none | enables cluster mode |
| `--cluster-listen` | `KOSHA_CLUSTER_LISTEN` | `127.0.0.1:9100` | internal Raft + replication port |
| `--cluster-advertise` | `KOSHA_CLUSTER_ADVERTISE` | `--cluster-listen` | address peers dial |
| `--api-advertise` | `KOSHA_API_ADVERTISE` | `--listen` | S3 address shown in the console; a changed value is sent to the cluster on restart |
| `--cluster-secret` | `KOSHA_CLUSTER_SECRET` | none | shared secret, at least 16 characters |
| `--init` / `--join <addr>` | `KOSHA_INIT` / `KOSHA_JOIN` | | bootstrap, or join through any member |
| `--replication` | `KOSHA_REPLICATION` | `2` | copies of each chunk |
| `--gc-grace-secs` | `KOSHA_GC_GRACE_SECS` | `900` | cluster: keep unreferenced chunks this long |
| `--repair-interval-secs` | `KOSHA_REPAIR_INTERVAL_SECS` | `60` | cluster: background re-replication/rebalancing pass (`0` disables) |
| `--erasure K+M` | `KOSHA_ERASURE` | none | cluster: Reed–Solomon erasure coding instead of replication for large chunks |
| `--erasure-cold-only` | `KOSHA_ERASURE_COLD_ONLY` | off | cluster: replicate new chunks, erasure code them once cold |
| `--erasure-min-chunk` | `KOSHA_ERASURE_MIN_CHUNK` | `131072` | cluster: smaller chunks stay replicated |
| `--no-lease-reads` | `KOSHA_NO_LEASE_READS` | off | cluster: prove leadership with a quorum round-trip on every read |
| `--snapshot-every` | `KOSHA_SNAPSHOT_EVERY` | `10000` | cluster: take a Raft snapshot after this many log entries |
| `--raft-logs-to-keep` | `KOSHA_RAFT_LOGS_TO_KEEP` | `1000` | cluster: log entries kept behind a snapshot, for slow followers |
| `--tls-cert` / `--tls-key` | `KOSHA_TLS_CERT` / `KOSHA_TLS_KEY` | none | serve S3 and the console over HTTPS |
| `--encryption-key-file` | `KOSHA_ENCRYPTION_KEY_FILE` | none | encrypt object data at rest (64 hex chars); the primary key for new data |
| `--encryption-old-key-file` | `KOSHA_ENCRYPTION_OLD_KEY_FILES` | none | previous key(s) during [key rotation](SECURITY.md#encryption-at-rest) (repeatable) |
| `--encryption-identity-key-file` | `KOSHA_ENCRYPTION_IDENTITY_KEY_FILE` | none | a cluster's original key, for a new node joining after rotation |
| `--cluster-tls-cert` / `--cluster-tls-key` / `--cluster-tls-ca` | `KOSHA_CLUSTER_TLS_*` | none | mutual TLS for all internal cluster traffic |
| `--cold-tier-endpoint` / `--cold-tier-bucket` / `--cold-tier-access-key` / `--cold-tier-secret-key` | `KOSHA_COLD_TIER_*` | none | S3-compatible cold tier (see [Cold tier](GUIDE.md#cold-tier)) |
| `--cold-tier-prefix` / `--cold-tier-region` | | `kosha-cold/` / `us-east-1` | key prefix and signing region in the cold bucket |
| `--offload-interval-secs` | `KOSHA_OFFLOAD_INTERVAL_SECS` | `60` | how often newly cold chunks are copied to the cold tier |
| `--notify-webhook name=url` | `KOSHA_NOTIFY_WEBHOOKS` (comma-separated) | none | event notification target (repeatable; same list on every node) |
| `--lifecycle-interval-secs` | `KOSHA_LIFECYCLE_INTERVAL_SECS` | `3600` | how often lifecycle rules run (leader only in a cluster; `0` disables) |
