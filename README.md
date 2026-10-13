# Kosha

**Kosha** (कोश, "treasury") is an S3-compatible object store written in Rust.

It speaks the S3 API, so existing tools and SDKs work unchanged. Underneath, it is
not a clone of S3's design. Kosha stores data the way modern backup and
content-addressed systems do, replicates it with Raft, and drives the disk
with io_uring.

![Kosha console](docs/images/console-overview.png)

| | Classic object store | Kosha |
|---|---|---|
| Unit of storage | whole object / fixed blocks | **content-defined chunks** (FastCDC) |
| Identity | key → file | key → manifest of **BLAKE3**-addressed chunks |
| Duplicate data | stored again | **stored once, globally** (across objects, buckets, multipart parts) |
| Server-side copy | re-reads and re-writes bytes | **metadata only** (a 256 MiB copy takes ~5 ms) |
| Multipart complete | concatenates part files | **concatenates manifests**, no data moved |
| Compression | none or always | **adaptive** zstd: probes each chunk, skips incompressible data |
| Integrity | optional checksums | **every chunk verified on every read**, including reads from other nodes |
| Disk I/O | blocking syscalls on a thread pool | **io_uring** (falls back to threads where it's unavailable) |
| Metadata | files / ad-hoc database | **Raft-replicated ACID state machine** (openraft + redb) |
| Consistency | eventually consistent or proprietary | **linearizable** reads and writes on any node (ReadIndex) |
| Concurrency control | last writer wins | adds **conditional writes** (`If-None-Match: *`, `If-Match`) evaluated in the Raft log |
| Admin UI | separate product | **built-in web console**, compiled into the binary |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design and the roadmap.

## Quick start: single node

```sh
(cd ui && npm ci && npm run build)    # optional: embeds the web console
cargo build --release
KOSHA_ACCESS_KEY=admin KOSHA_SECRET_KEY=change-me-please \
  ./target/release/kosha --data-dir ./data --listen 0.0.0.0:9000
```

Open the console at `http://localhost:9000/_kosha/ui/`, or use any S3 client:

```sh
export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=change-me-please AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:9000 s3 mb s3://photos
aws --endpoint-url http://localhost:9000 s3 cp ./holiday.jpg s3://photos/2026/holiday.jpg
```

```python
import boto3
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="admin", aws_secret_access_key="change-me-please")
s3.put_object(Bucket="photos", Key="hello.txt", Body=b"hi")
```

## Cluster

Every node serves the full S3 API. Metadata is replicated through Raft, and
each chunk is stored on `--replication` nodes (default 2) chosen by
rendezvous hashing. With 3 nodes the cluster tolerates one node failure.

```sh
export KOSHA_ACCESS_KEY=admin KOSHA_SECRET_KEY=change-me-please
export KOSHA_CLUSTER_SECRET=$(openssl rand -hex 24)        # same on every node

kosha --node-id 1 --listen 0.0.0.0:9000 --cluster-listen 10.0.0.1:9100 --init
kosha --node-id 2 --listen 0.0.0.0:9000 --cluster-listen 10.0.0.2:9100 --join 10.0.0.1:9100
kosha --node-id 3 --listen 0.0.0.0:9000 --cluster-listen 10.0.0.3:9100 --join 10.0.0.1:9100
```

`--init` and `--join` only matter the first time: a restarted node rejoins
from its own Raft state. To run three nodes on one machine for development:

```sh
scripts/dev-cluster.sh start      # nodes on :9001-9003, console on :9001
scripts/dev-cluster.sh kill 1     # SIGKILL a node and watch the leader change
scripts/dev-cluster.sh restart 1
scripts/dev-cluster.sh stop
```

What was verified with three real processes:
- The AWS CLI wrote a 25 MB multipart upload through a follower and read it
  back from another node.
- The leader was killed with SIGKILL. A new leader was elected, old data
  stayed readable, and new writes succeeded.
- The killed node was restarted and caught up.

Internal traffic is plain HTTP authenticated by the shared secret. Keep the
cluster port on a private network.

## Web console

The console is a React app embedded in the binary and served at `/_kosha/ui/`.
It talks to the ordinary S3 API: requests are SigV4-signed in the browser, so
there are no extra admin endpoints and no server-side sessions. It provides:

- An overview of storage efficiency (dedup and compression savings), the I/O
  backend, and cluster role.
- A bucket and object browser: drag-and-drop uploads with progress, folders,
  object details, downloads, expiring share links, and bulk delete.
- A cluster view: leader, term, log and applied indexes, and per-node
  replication progress, refreshed live.
- Light and dark themes, and layouts that work at phone width.

| Object browser | Cluster (dark) |
|---|---|
| ![](docs/images/console-browser.png) | ![](docs/images/console-cluster-dark.png) |

## io_uring

On Linux, chunk files are read and written through io_uring. One ring per core
(up to 8), each driven by its own thread, runs every file operation as a small
state machine: `openat → write → fsync → close` or `openat → read → close`.
Hashing and compression stay on the blocking pool.

`cargo run --release -p kosha-engine --example iobench` compares the two
backends. One run on the 4-core development VM with 4 rings, all rows from the
same run:

| Workload | Threads | io_uring |
|---|---|---|
| 4 KiB writes | 10.9k ops/s | **45.8k ops/s** (4.2×) |
| 4 KiB writes + fsync | 8.5k ops/s | **10.3k ops/s** (1.2×) |
| 4 KiB reads | 112k ops/s | **147k ops/s** (1.3×) |
| 1 MiB writes | 416 MB/s | **658 MB/s** (1.6×) |
| 1 MiB writes + fsync | 413 MB/s | **651 MB/s** (1.6×) |
| 1 MiB reads (page cache) | 4.8 GB/s | **6.2 GB/s** (1.3×) |

Results vary between runs on a shared VM. The fsync rows vary most: across
repeated runs, 1 MiB + fsync ranged from 1.6× to 3.1× in io_uring's favour.
With fewer rings than cores, page-cache reads can be *slower* than the thread
pool, because they complete inline on the ring thread. That is why Kosha
defaults to one ring per core.

Docker's default seccomp profile and `kernel.io_uring_disabled` block io_uring.
`--io auto` (the default) detects this and falls back to threads. `/_kosha/stats`
and the console report which backend is active.

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
| `--no-fsync` | `KOSHA_NO_FSYNC` | off | faster, but recent writes may be lost on power failure |
| `--io` | `KOSHA_IO` | `auto` | `auto`, `uring` (required), or `threads` |
| `--node-id` | `KOSHA_NODE_ID` | none | enables cluster mode |
| `--cluster-listen` | `KOSHA_CLUSTER_LISTEN` | `127.0.0.1:9100` | internal Raft + replication port |
| `--cluster-advertise` | `KOSHA_CLUSTER_ADVERTISE` | `--cluster-listen` | address peers dial |
| `--api-advertise` | `KOSHA_API_ADVERTISE` | `--listen` | S3 address shown in the console |
| `--cluster-secret` | `KOSHA_CLUSTER_SECRET` | none | shared secret, at least 16 characters |
| `--init` / `--join <addr>` | `KOSHA_INIT` / `KOSHA_JOIN` | | bootstrap, or join through any member |
| `--replication` | `KOSHA_REPLICATION` | `2` | copies of each chunk |
| `--gc-grace-secs` | `KOSHA_GC_GRACE_SECS` | `900` | cluster: keep unreferenced chunks this long |

Logging uses `RUST_LOG`, for example `RUST_LOG=debug`.

## S3 compatibility

Tested against the **AWS SDK for Rust** (in CI), **boto3**, the **AWS CLI**,
**curl `--aws-sigv4`**, and the console's own SigV4 signer.

| Area | Supported |
|---|---|
| Auth | SigV4 headers, SigV4 presigned URLs, streaming `aws-chunked` with per-chunk signature verification, unsigned-payload trailers, SigV2 presigned URLs (boto3's default) |
| Integrity | `x-amz-content-sha256` verified, `Content-MD5`, `x-amz-checksum-crc32` (header or trailer) |
| Buckets | Create, Delete, Head, List, GetBucketLocation, GetBucketVersioning (unversioned), GetBucketAcl; "not configured" responses for policy/CORS/lifecycle/tagging/encryption/... |
| Objects | Put, Get (single range, conditional, `response-*` overrides), Head, Delete, DeleteObjects, CopyObject (COPY/REPLACE metadata, copy-source conditions), user metadata and standard headers |
| Listing | ListObjectsV2 (prefix, delimiter, continuation, start-after, `encoding-type=url`), ListObjects v1 |
| Multipart | Create, UploadPart, UploadPartCopy (with range), Complete, Abort, ListParts, ListMultipartUploads |
| Writes | Conditional `If-None-Match: *` / `If-Match` on Put, Copy and Complete |

Not implemented yet: versioning, object lock, bucket policies/IAM, lifecycle,
SSE, tagging writes, SigV2 header auth. Unsupported operations return `501 NotImplemented`.

## Layout

```
crates/
  kosha-engine    storage engine: chunking, chunk store, io_uring, metadata state machine, GC
  kosha-s3        S3 protocol: routing, SigV4/SigV2, aws-chunked, XML
  kosha-cluster   Raft (openraft): log store, state machine, network, placement
  kosha-server    the `kosha` binary (embeds the console)
ui/               web console (React + TypeScript + Vite)
```

## Development

```sh
(cd ui && npm ci && npm test && npm run build)
cargo test --workspace     # engine, S3 + AWS SDK, and 3-node cluster tests
cargo clippy --workspace --all-targets -- -D warnings
(cd ui && npm run dev)     # console with hot reload, proxied to a server on :9000
```

## License

Apache-2.0
