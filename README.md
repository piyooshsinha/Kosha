# Kosha

Kosha (कोश, "treasury") is a small S3-compatible object store written in Rust.
It runs as one binary, on one machine or as a cluster, and includes a web
console. Data is split into content-defined chunks, so identical data is
stored once.

![Kosha console](docs/images/console-overview.png)

## Status

Kosha is an early-stage project, not a production system.

- It has been tested on one VM and one home server. It has no production
  history and no external review.
- The on-disk format and the admin API may still change between versions,
  without a migration path.
- Every metadata change goes through one Raft leader. A 3-node test cluster
  handled about 1,200–1,400 small PUTs per second.
- Many S3 features are missing (see [Limitations](#limitations)).

Use it for experiments, home labs and data you also keep elsewhere.

## What it does

- **S3 API** for the AWS CLI, boto3 and the AWS SDKs: buckets, objects,
  multipart uploads, versioning, Object Lock, lifecycle rules, tagging, CORS,
  presigned URLs and bucket policies. Details: [S3 compatibility](docs/S3-COMPATIBILITY.md).
- **Deduplication and compression:** each unique chunk is stored once across
  all objects, buckets and versions. Chunks are compressed with zstd when it
  helps. Copying an object only copies metadata.
- **Clustering:** metadata is replicated with Raft. Chunks are stored on 2
  nodes by default, or erasure coded. Nodes can be added and removed from
  the console, and lost copies are restored in the background.
- **Web console** for buckets, files, share links, users and the cluster,
  with a view of where each write went.
- **Security:** users with IAM-style policies, HTTPS, mutual TLS between
  nodes, and encryption at rest that keeps deduplication.
- **Disk I/O** through io_uring on Linux, with a thread-pool fallback.

## Quick start

With Docker, one node:

```sh
git clone https://github.com/piyooshsinha/Kosha && cd Kosha
printf 'KOSHA_ACCESS_KEY=admin\nKOSHA_SECRET_KEY=%s\n' "$(openssl rand -hex 20)" > .env
docker compose up -d --build
```

Open `http://localhost:9000/_kosha/ui/` and sign in with the keys in `.env`.

Three nodes on one host (ports 9000–9002):

```sh
printf 'KOSHA_ACCESS_KEY=admin\nKOSHA_SECRET_KEY=%s\nKOSHA_CLUSTER_SECRET=%s\nKOSHA_HOST=%s\n' \
  "$(openssl rand -hex 20)" "$(openssl rand -hex 24)" "$(hostname -I | awk '{print $1}')" > .env
docker compose -f docker-compose.cluster.yml up -d --build
```

From source, with Rust 1.85+ (and optionally Node.js 20+ for the console):

```sh
(cd ui && npm ci && npm run build)
cargo build --release
KOSHA_ACCESS_KEY=admin KOSHA_SECRET_KEY=change-me-please \
  ./target/release/kosha --data-dir ./data --listen 0.0.0.0:9000
```

To install it as a systemd service, see the [user guide](docs/GUIDE.md#as-a-service-home-server-nuc-vm).

## Connect a client

Any S3 client needs the endpoint, an access key and secret, any region name,
and path-style addressing:

```sh
export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=change-me-please AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:9000 s3 mb s3://photos
aws --endpoint-url http://localhost:9000 s3 cp ./holiday.jpg s3://photos/
aws --endpoint-url http://localhost:9000 s3 sync ~/Documents s3://photos/docs/
```

Give each app its own user and keys (console → **Users**) instead of the
root keys. Examples for boto3, the Rust SDK and curl are in the
[user guide](docs/GUIDE.md#read-and-write-data).

## Web console

The console is served at `/_kosha/ui/` on every node:

- **Overview:** stored size and dedup savings.
- **Buckets:** buckets and their policies.
- **Object browser:** upload, browse, download and share links.
- **Users:** keys and policies.
- **Cluster:** members and leader; add and remove nodes.
- **Data flow:** where each write's chunks and metadata went.

A walkthrough with screenshots is in the [user guide](docs/GUIDE.md#how-to-use-kosha).

| Object browser | Cluster |
|---|---|
| ![](docs/images/console-browser.png) | ![](docs/images/console-cluster.png) |

## Running a cluster

- Any node serves any request. Put the nodes behind one address and have the
  load balancer check `GET /_kosha/health`. A removed node answers it with
  `503`. Every response carries `x-kosha-node` with the id of the node that
  served it.
- A 3-node cluster keeps working when any one node is down. A new leader is
  elected in 1–2 seconds.
- **Add a node:** start it with the cluster secret and a new node id, then
  enter its cluster address under **Cluster → Add a node**.
- **Remove a node:** click **Remove**. Its data is copied to the other
  nodes, and the removed node stops serving.
- Every add and remove is logged with the caller's key and address.

Details: [clusters and monitoring](docs/CLUSTER.md).

## Limitations

- **Maturity:** one developer, tested on one VM and one home server, no
  production use.
- **Scale:** all metadata goes through one Raft group, and no load beyond
  the [benchmarks](docs/BENCHMARKS.md) has been tested.
- **Not implemented:**
  - SSE-KMS, SSE-C, and the AWS IAM, STS and ACL-grant APIs;
  - bucket replication, static website hosting and S3 Select;
  - notification targets other than webhooks.

  Unsupported operations return `501 NotImplemented`.
- **Operations:**
  - Kosha has no built-in backup of its own data. Back up the data directory
    or replicate to another store.
  - There are no releases yet (version 0.1.0): you upgrade by rebuilding
    from `main`.
- **Platforms:** tested on Linux x86-64, natively and in Docker.

## Documentation

| | |
|---|---|
| [User guide](docs/GUIDE.md) | installing, S3 clients, versioning, lifecycle, cold tier, notifications, console walkthrough |
| [Clusters and monitoring](docs/CLUSTER.md) | multi-node setup, adding and removing nodes, erasure coding, metrics |
| [Security](docs/SECURITY.md) | users and policies, HTTPS, cluster TLS, encryption at rest and key rotation |
| [Configuration](docs/CONFIGURATION.md) | every flag and environment variable |
| [S3 compatibility](docs/S3-COMPATIBILITY.md) | supported operations and headers |
| [Architecture](docs/ARCHITECTURE.md) | how storage, metadata and replication work |
| [Benchmarks](docs/BENCHMARKS.md) | throughput and latency, compared with MinIO and RustFS |
| [Fault testing](docs/TESTING.md) | crash, power-cut, partition and clock-drift tests |
| [Comparison](docs/COMPARISON.md) | Kosha vs Amazon S3, MinIO, RustFS and Garage |

## Development

```sh
(cd ui && npm ci && npm test && npm run build)
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
scripts/dev-cluster.sh start   # 3 local nodes on :9001-9003, admin / admin-secret-key
```

The code is organised as follows:

- `crates/kosha-engine`: chunking, chunk store, encryption, io_uring and the
  metadata state machine.
- `crates/kosha-s3`: the S3 API and authentication.
- `crates/kosha-cluster`: Raft and replication.
- `crates/kosha-server`: the binary.
- `ui/`: the console.

## License

Apache-2.0
