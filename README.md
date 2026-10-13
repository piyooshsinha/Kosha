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
| Server-side copy | re-reads and re-writes bytes | **metadata only** (a 256 MiB copy takes ~3 ms) |
| Multipart complete | concatenates part files | **concatenates manifests**, no data moved |
| Compression | none or always | **adaptive** zstd: probes each chunk, skips incompressible data |
| Integrity | optional checksums | **every chunk verified on every read**, including reads from other nodes |
| Disk I/O | blocking syscalls on a thread pool | **io_uring** (falls back to threads where it's unavailable) |
| Metadata | files / ad-hoc database | **Raft-replicated ACID state machine** (openraft + redb) |
| Consistency | eventually consistent or proprietary | **linearizable** reads and writes on any node (ReadIndex) |
| Concurrency control | last writer wins | adds **conditional writes** (`If-None-Match: *`, `If-Match`) evaluated in the Raft log |
| Admin UI | separate product | **built-in web console**, compiled into the binary |

**Contents:**
[Install](#1-install) ·
[Run a server](#2-run-a-single-node) ·
[Read and write data](#3-read-and-write-data) ·
[Web console](#4-web-console) ·
[Cluster](#5-run-a-cluster) ·
[Monitoring](#6-monitoring) ·
[Performance](#performance) ·
[Configuration](#configuration) ·
[S3 compatibility](#s3-compatibility) ·
[Architecture](docs/ARCHITECTURE.md)

---

## 1. Install

You need **Rust 1.85 or newer**. **Node.js 20 or newer** is optional and only
needed to build the web console. Linux is recommended (io_uring); other
platforms use the thread-pool I/O backend.

```sh
git clone https://github.com/piyooshsinha/Kosha && cd Kosha
(cd ui && npm ci && npm run build)   # optional: builds the console that gets embedded
cargo build --release                # produces ./target/release/kosha
```

## 2. Run a single node

```sh
export KOSHA_ACCESS_KEY=admin
export KOSHA_SECRET_KEY=change-me-please
./target/release/kosha --data-dir ./data --listen 0.0.0.0:9000
```

The log tells you which I/O backend was chosen and where the console is:

```
INFO kosha: storage engine ready io="io_uring"
INFO kosha: Kosha is ready listen=0.0.0.0:9000 console=http://0.0.0.0:9000/_kosha/ui/ mode="single-node"
```

Everything lives in `--data-dir`: `meta.redb` holds the metadata,
`chunks/` the content-addressed data, and `raft/` the Raft log in cluster
mode. Stop the server with Ctrl-C; it shuts down gracefully.

For local experiments you can skip authentication with `--anonymous`. Never
use it on a network you don't control.

## 3. Read and write data

Kosha works with S3 clients as-is. Point them at the server with path-style
addressing. Any region name is accepted.

### AWS CLI

```sh
export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=change-me-please AWS_DEFAULT_REGION=us-east-1
alias s3k='aws --endpoint-url http://localhost:9000 s3'

s3k mb s3://photos                                   # create a bucket
s3k cp ./holiday.jpg s3://photos/2026/holiday.jpg    # upload (large files use multipart automatically)
s3k ls s3://photos --recursive                       # list
s3k cp s3://photos/2026/holiday.jpg ./copy.jpg       # download
s3k sync ./website s3://photos/site/                 # sync a directory
s3k cp s3://photos/2026/holiday.jpg s3://photos/backup.jpg   # server-side copy: metadata only
s3k presign s3://photos/2026/holiday.jpg --expires-in 3600   # shareable link
s3k rm s3://photos/2026/holiday.jpg
```

### Python (boto3)

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="change-me-please",
    region_name="us-east-1",
)
s3.put_object(Bucket="photos", Key="hello.txt", Body=b"hi", ContentType="text/plain",
              Metadata={"owner": "me"})
print(s3.get_object(Bucket="photos", Key="hello.txt")["Body"].read())
s3.upload_file("big.iso", "photos", "isos/big.iso")          # multipart for large files
url = s3.generate_presigned_url("get_object", Params={"Bucket": "photos", "Key": "hello.txt"})

# Conditional create: succeeds only if the key does not exist yet (a simple lock).
s3.put_object(Bucket="photos", Key="locks/job-42", Body=b"", IfNoneMatch="*")
```

### Rust (aws-sdk-s3)

```rust
let conf = aws_sdk_s3::Config::builder()
    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
    .region(aws_sdk_s3::config::Region::new("us-east-1"))
    .endpoint_url("http://localhost:9000")
    .credentials_provider(aws_sdk_s3::config::Credentials::new("admin", "change-me-please", None, None, "kosha"))
    .force_path_style(true)
    .build();
let s3 = aws_sdk_s3::Client::from_conf(conf);
s3.put_object().bucket("photos").key("a.txt")
    .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello")).send().await?;
```

### curl

curl can sign requests itself:

```sh
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user admin:change-me-please \
     -T ./report.pdf http://localhost:9000/photos/report.pdf
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user admin:change-me-please \
     http://localhost:9000/photos/report.pdf -o report.pdf
```

## 4. Web console

Open **`http://<server>:9000/_kosha/ui/`** and sign in with the access key and
secret. The console talks to the ordinary S3 API: requests are SigV4-signed in
your browser, so there are no extra admin endpoints and no server-side
sessions. It works over plain HTTP too.

- **Overview**: object counts, logical vs on-disk size, and how much dedup and
  compression saved. Also shows the I/O backend and cluster role.
- **Buckets and object browser**: create and delete buckets, browse folders,
  drag-and-drop uploads with progress, object details, downloads, expiring
  share links (1 hour / 1 day / 7 days), and bulk delete.
- **Cluster**: leader, Raft term, log and applied indexes, and per-node
  replication progress, refreshed every 2 seconds.

| Object browser | Cluster (dark mode) |
|---|---|
| ![](docs/images/console-browser.png) | ![](docs/images/console-cluster-dark.png) |

## 5. Run a cluster

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
  than a write that already completed anywhere in the cluster.
- **Failure**: if the leader dies, the others elect a new one within about
  1–2 seconds and keep serving. When the dead node comes back, it catches up
  from the log, or from a snapshot if the log was compacted.
- Internal traffic (port 9100) is plain HTTP authenticated by the shared
  secret. **Keep it on a private network.**

### Try a cluster on one machine

```sh
scripts/dev-cluster.sh start      # 3 nodes: S3 on :9001-9003, console on http://127.0.0.1:9001/_kosha/ui/
scripts/dev-cluster.sh kill 1     # SIGKILL node 1 and watch a new leader take over
scripts/dev-cluster.sh restart 1  # it rejoins and catches up
scripts/dev-cluster.sh stop
```

## 6. Monitoring

| Endpoint | Auth | Returns |
|---|---|---|
| `GET /_kosha/health` | none | `ok`, for load balancer and liveness probes |
| `GET /_kosha/stats` | SigV4 | objects, logical/unique/physical bytes, dedup and compression ratios, I/O backend |
| `GET /_kosha/cluster` | SigV4 | role, term, leader, log and applied indexes, members and replication progress |

```sh
curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user admin:change-me-please http://localhost:9000/_kosha/stats
# {"io_backend":"io_uring","buckets":2,"objects":7,"logical_bytes":832569357,"unique_bytes":292552704,
#  "physical_bytes":292552944,"chunks":240,"dedup_ratio":2.846,"compression_ratio":1.000,"overall_ratio":2.846}
```

Logs go to stderr and are filtered with `RUST_LOG`, for example
`RUST_LOG=kosha_cluster=debug`.

---

## Performance

All numbers come from the 4-core / 15 GB development VM, with **the load
generator and every server process sharing those same 4 cores**. In the
cluster runs, three Kosha nodes and the client run on one machine. Treat the
numbers as a floor: on dedicated hardware the server gets all the CPU, and
cluster nodes stop competing with each other.

Unless noted otherwise:
- fsync is on, the I/O backend is io_uring, and objects are random
  (incompressible and unique, so dedup cannot help);
- the client is the official **AWS SDK for Rust**, which signs every payload
  with SHA-256;
- every GET is checked byte-for-byte against what was written, and every run
  finished with **0 errors**.

### Single node: throughput under load

| Object size | Concurrency | PUT | GET |
|---|---|---|---|
| 4 KiB | 32 | **1,160 req/s** · p50 27 ms · p99 61 ms | **4,120 req/s** · p50 3.8 ms · p99 10.7 ms |
| 64 KiB | 32 | **880 req/s** (55 MiB/s) · p50 35 ms · p99 81 ms | **3,670 req/s** (229 MiB/s) · p50 6.1 ms · p99 12.6 ms |
| 1 MiB | 16 | **143 MiB/s** · p50 87 ms · p99 168 ms | **851 MiB/s** · p50 17 ms · p99 38 ms |
| 32 MiB | 4 | **141 MiB/s** · p50 744 ms | **586 MiB/s** · p50 200 ms |

### Single node: one request at a time (latency)

| Object size | PUT p50 / p99 | GET p50 / p99 |
|---|---|---|
| 4 KiB | **1.7 ms** / 4.0 ms | **0.58 ms** / 1.5 ms |
| 1 MiB | 26.6 ms / 35.9 ms | **2.9 ms** / 4.6 ms |

### Single node: one large stream (curl, `UNSIGNED-PAYLOAD`)

| Operation | Result |
|---|---|
| PUT 256 MiB, unique data | 245–340 MB/s (two runs) |
| PUT 256 MiB, data already stored (dedup) | 342 MB/s |
| GET 256 MiB (chunks still in page cache) | **1.14–1.17 GB/s** |
| Server-side COPY 256 MiB | **2.9 ms** (metadata only) |

### 3-node cluster (replication factor 2)

Writes go through Raft: the leader fsyncs the log entry and waits for a
majority to do the same. Each chunk is also written to 2 nodes. Reads use
ReadIndex, so a follower read includes one round-trip to the leader.

| Object size | Concurrency | PUT via leader | GET via follower |
|---|---|---|---|
| 4 KiB | 32 | **687 req/s** · p50 43 ms · p99 65 ms | **3,800 req/s** · p50 6.4 ms · p99 26 ms |
| 64 KiB | 32 | **513 req/s** (32 MiB/s) · p50 53 ms | **2,240 req/s** (140 MiB/s) · p50 11 ms |
| 1 MiB | 16 | **102 MiB/s** · p50 119 ms | **562 MiB/s** · p50 26 ms |
| 32 MiB | 4 | **125 MiB/s** | **543 MiB/s** |

Writing through a follower costs little extra: 638 req/s for 4 KiB and
125 MiB/s for 1 MiB. With one request at a time, a 4 KiB PUT takes **6.0 ms**
p50 through the leader and 7.0 ms through a follower. A linearizable 4 KiB GET
on a follower takes **1.2 ms** p50.

### What limits these numbers

- **Small writes are limited by metadata commits.** Each PUT is one
  serialized, durable redb transaction (a Raft log append plus apply in a
  cluster). Objects of 8 KiB or less are stored inline and never touch a chunk
  file, and 4 KiB PUTs still top out around 1,100/s. `--no-fsync` (which only
  affects chunk files) did not change that. Up to 32 concurrent requests,
  latency grows with load while throughput stays flat, the signature of a
  serialized commit path. The fix is **group commit**: batch concurrent
  commands into one transaction. The state machine already applies batches,
  so this is the next optimization on the roadmap.
- **Large writes are CPU-bound on this VM.** The SDK hashes every payload
  with SHA-256, and the server re-verifies it and also computes MD5 (for the
  ETag) and BLAKE3 (for the chunk address), on the same 4 cores as the client.
  That is why identical-content 1 MiB PUTs run only modestly faster
  (164 vs 143 MiB/s) even though dedup skips the disk write. The
  single-stream curl upload with `UNSIGNED-PAYLOAD` skips the SHA-256 and
  reaches 245–340 MB/s.
- **Reads scale well.** Range reads touch only the chunks they need,
  `read_ahead` chunks are fetched concurrently, and io_uring serves warm
  chunks at over 1 GB/s on a single stream.

### Reproduce

```sh
cargo build --release -p kosha-server -p kosha-s3 --examples
AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=change-me-please \
  ./target/release/examples/s3bench --endpoint http://127.0.0.1:9000 \
  --size 64KiB --count 3000 --concurrency 32
# cluster: --endpoint http://127.0.0.1:9001 --read-endpoint http://127.0.0.1:9003
# dedup:   add --duplicate to upload identical content
```

`s3bench` writes `count` objects, reads them all back, verifies the content,
and prints req/s, MiB/s, p50/p99/max latency and errors for each phase.

### io_uring vs thread pool

On Linux, chunk files are read and written through io_uring. One ring per
core (up to 8), each driven by its own thread, runs every file operation as a
small state machine: `openat → write → fsync → close` or
`openat → read → close`. Hashing and compression stay on the blocking pool.
`cargo run --release -p kosha-engine --example iobench` compares the two
backends directly on chunk-sized files. One run with 4 rings:

| Workload | Threads | io_uring |
|---|---|---|
| 4 KiB writes | 10.9k ops/s | **45.8k ops/s** (4.2×) |
| 4 KiB writes + fsync | 8.5k ops/s | **10.3k ops/s** (1.2×) |
| 4 KiB reads | 112k ops/s | **147k ops/s** (1.3×) |
| 1 MiB writes | 416 MB/s | **658 MB/s** (1.6×) |
| 1 MiB writes + fsync | 413 MB/s | **651 MB/s** (1.6×) |
| 1 MiB reads (page cache) | 4.8 GB/s | **6.2 GB/s** (1.3×) |

Results vary between runs on a shared VM. Across repeated runs, 1 MiB + fsync
ranged from 1.6× to 3.1× in io_uring's favour. With fewer rings than cores,
page-cache reads can be *slower* than the thread pool, because they complete
inline on the ring thread. That is why Kosha defaults to one ring per core.

Docker's default seccomp profile and `kernel.io_uring_disabled` block
io_uring. `--io auto` (the default) detects this and falls back to threads;
`/_kosha/stats` and the console report which backend is active.

---

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
| `--no-fsync` | `KOSHA_NO_FSYNC` | off | skip fsync of chunk files (metadata commits stay durable) |
| `--io` | `KOSHA_IO` | `auto` | `auto`, `uring` (required), or `threads` |
| `--node-id` | `KOSHA_NODE_ID` | none | enables cluster mode |
| `--cluster-listen` | `KOSHA_CLUSTER_LISTEN` | `127.0.0.1:9100` | internal Raft + replication port |
| `--cluster-advertise` | `KOSHA_CLUSTER_ADVERTISE` | `--cluster-listen` | address peers dial |
| `--api-advertise` | `KOSHA_API_ADVERTISE` | `--listen` | S3 address shown in the console |
| `--cluster-secret` | `KOSHA_CLUSTER_SECRET` | none | shared secret, at least 16 characters |
| `--init` / `--join <addr>` | `KOSHA_INIT` / `KOSHA_JOIN` | | bootstrap, or join through any member |
| `--replication` | `KOSHA_REPLICATION` | `2` | copies of each chunk |
| `--gc-grace-secs` | `KOSHA_GC_GRACE_SECS` | `900` | cluster: keep unreferenced chunks this long |

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
  kosha-s3        S3 protocol: routing, SigV4/SigV2, aws-chunked, XML (+ s3bench example)
  kosha-cluster   Raft (openraft): log store, state machine, network, placement
  kosha-server    the `kosha` binary (embeds the console)
ui/               web console (React + TypeScript + Vite)
scripts/          dev-cluster.sh
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
