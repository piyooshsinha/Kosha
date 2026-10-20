# Comparison

How Kosha differs from Amazon S3 and from other self-hosted S3 servers. Kosha is far younger than all of them.

## Kosha vs Amazon S3

Kosha speaks the S3 API, so the AWS CLI, boto3 and the AWS SDKs work against
it unchanged (see [S3 compatibility](S3-COMPATIBILITY.md#s3-compatibility) for what is tested).
It is not a copy of Amazon S3, though: S3 is a managed service you rent, and
Kosha is software you run.

**Where Kosha differs**

| | Amazon S3 | Kosha |
|---|---|---|
| Where it runs | AWS Regions | your own machines or any cloud (Linux recommended) |
| What you pay | storage per GB-month, requests, and data transferred out | the hardware and network you provide; no per-request charges |
| Duplicate data | no deduplication: every object is stored and billed at its full size | each unique chunk is stored once, across all objects and buckets |
| Compression | none on the server: you are billed for the bytes you upload | adaptive zstd per chunk, transparent to clients |
| Server-side copy | the copy is a separate object, stored and billed at its full size | metadata only: the copy shares its chunks with the source |
| Versions | every retained version is billed | versions share chunks; a new version costs only its changed bytes |
| Encryption at rest | SSE-S3 (the default), SSE-KMS, DSSE-KMS, SSE-C | your own key file; encryption keeps deduplication; online key rotation; no KMS integration |
| Consistency | strong read-after-write; conditional writes with `If-None-Match` and `If-Match` | linearizable through Raft; the same conditional writes, evaluated in the Raft log |
| Cold storage | Glacier Instant Retrieval reads in milliseconds; Glacier Flexible Retrieval needs a restore that takes minutes to hours, Deep Archive up to 12 hours (48 for bulk) | a cold tier on *any* S3-compatible bucket; reads are transparent and `RestoreObject` completes at once |
| Intelligent-Tiering | a monthly monitoring fee for each object of 128 KB or more | built in, no fee; access is tracked in the replicated metadata |
| Redundancy | set by the storage class: at least three Availability Zones, or one for the One Zone classes | your choice per cluster: N replicas, or Reed–Solomon K+M (for example 4+2 = 1.5× storage) |
| Monitoring | CloudWatch: daily storage metrics free, request metrics billed | Prometheus metrics, `/_kosha/stats` and a built-in web console |
| Source | proprietary service | open source (Apache-2.0), one binary |

**What Amazon S3 has that Kosha does not**

- **Scale and durability as a service.** S3 is designed for 99.999999999%
  (11 nines) durability and is operated for you. A Kosha cluster is as
  durable and as available as the nodes, disks and sites you give it, and
  you operate it.
- **Metadata scale.** All Kosha metadata goes through one Raft group. In our
  benchmarks on a shared 4-core VM, a 3-node cluster sustained 1,180–1,430
  4 KiB PUTs per second. That is far below S3's scale (see the
  [roadmap](ARCHITECTURE.md#roadmap)).
- **The wider AWS ecosystem.** Kosha has no AWS IAM API, roles or STS
  credentials (users are managed in the console or `/_kosha/admin/users`),
  no SSE-KMS or SSE-C, no ACLs that grant access to others (use policies),
  no cross-region replication, no static website hosting, and no S3
  Inventory or Storage Lens. Notifications go to webhooks, not SQS, SNS,
  Lambda or EventBridge. Unsupported operations return `501 NotImplemented`.

**When to choose which**

- **Choose Kosha** to keep data on your own hardware, to avoid per-request
  and egress charges, for data with many duplicates (backups, VM images,
  datasets, build artifacts, versioned documents), or for sites without
  cloud access.
- **Choose S3** when you want no operations work, AWS-managed durability at
  any scale, or integration with other AWS services.
- **Use both:** point Kosha's [cold tier](GUIDE.md#cold-tier) at an S3 bucket.
  Hot data stays local, and cold chunks go to S3 already deduplicated,
  compressed and encrypted.

Sources (checked October 2026):
[S3 storage classes](https://aws.amazon.com/s3/storage-classes/),
[Glacier storage classes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/glacier-storage-classes.html),
[S3 pricing](https://aws.amazon.com/s3/pricing/),
[conditional writes (If-None-Match)](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes),
[conditional writes (If-Match)](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes),
[billing for versions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/aws-billing-reports.html),
[CloudWatch metrics](https://docs.aws.amazon.com/AmazonS3/latest/userguide/cloudwatch-monitoring.html).

## Kosha vs MinIO, RustFS and Garage

The self-hosted S3 servers closest to Kosha are MinIO (Go), RustFS (Rust)
and Garage (Rust). In one sentence each:

- **MinIO** and **RustFS** erasure code each object across a set of drives
  and keep its metadata (`xl.meta`) next to the data.
- **Garage** splits objects into fixed-size blocks and replicates them
  across sites, with no leader.
- **Kosha** splits data into content-defined chunks, stores each unique chunk
  once, and keeps all metadata in one Raft-replicated database.

Every statement about them was checked in October 2026 against their own
documentation or source code (linked below). For measured speed, see
[Compared with RustFS and MinIO](BENCHMARKS.md#compared-with-rustfs-and-minio): that
benchmark covers single-node, single-drive setups only. Garage was not
benchmarked.

| | MinIO | RustFS | Garage | Kosha |
|---|---|---|---|---|
| Language, license | Go, AGPLv3 | Rust, Apache-2.0 | Rust, AGPLv3 | Rust, Apache-2.0 |
| Status | community edition marked "no longer maintained" (last commit February 2026); source only, no binaries | 1.0.0 tagged 16 September 2026 | active; v2.4.1 released September 2026; in production at Deuxfleurs since 2020 | new; tested on a single VM, not yet run in production |
| Data layout | object erasure coded across a drive set; optional S2 compression | same model as MinIO; optional compression | fixed-size blocks, replicated; optional zstd | content-defined chunks (FastCDC), addressed by BLAKE3; adaptive zstd |
| Deduplication | none documented | none documented | identical blocks; an insert shifts every later block, so they stop matching | **global**, across objects, buckets, versions and parts; after an insert, most chunks still match |
| Metadata | `xl.meta` with each object; distributed locks (dsync) | same model as MinIO | CRDT tables with quorum reads and writes, no Raft | **one Raft-replicated database** (openraft + redb) |
| Consistency | strict read-after-write and list-after-write (on xfs, zfs or btrfs, per its docs) | not stated in its docs | read-after-write by default; can be relaxed with `consistency_mode` | **linearizable** on any node; conditional writes evaluated in the Raft log |
| Copy to a new key | data is rewritten (metadata only when copying onto itself) | data is rewritten (metadata only when copying onto itself) | metadata only in most cases: the copy references the same blocks | metadata only |
| Multipart complete | part files are renamed into place, no copy | part files are renamed into place, no copy | the parts' blocks are linked into the object, no copy | manifests are concatenated, no copy |
| New version of an object | stored in full | stored in full | no versioning | stores only the changed chunks |
| Redundancy | erasure coding | erasure coding | replication | replication or erasure coding, or replicated while hot and erasure coded once cold |
| Encryption at rest | SSE with a KMS through KES | SSE with a KMS (Vault, AWS KMS) | SSE-C (client-supplied keys) only | dedup-preserving encryption with your own key file, online rotation |
| Tiering | lifecycle transition to remote tiers (cloud or another MinIO) | lifecycle transition to remote S3 | none | chunk-level cold tier on any S3 bucket, access-based demotion and promotion |
| Disk I/O | `O_DIRECT` writes in multi-drive setups, on drives that support it | standard reads and writes; opt-in io_uring reads and `O_DIRECT` reads | standard reads and writes | io_uring for reads and writes; opt-in `O_DIRECT` |
| Web console | object browser; admin screens removed in 2025 | included | none in the project (CLI and admin API) | built into the binary |

**Where they are ahead of Kosha**

- **Maturity.** MinIO has been widely deployed for years, and Garage has run
  in production at Deuxfleurs since 2020. Kosha has no production history.
- **Metadata scale.** Every Kosha metadata write goes through one Raft
  leader: 1,180–1,430 4 KiB PUTs per second in our 3-node benchmarks on a
  shared 4-core VM. MinIO and RustFS keep metadata with each object instead
  of in a central database, and Garage has no leader, so none of them
  funnels metadata through one node.
- **Large uploads.** In our single-node benchmark MinIO uploaded 32 MiB
  objects faster than Kosha (161 vs 139 MiB/s median) and matched it on
  1 MiB uploads. Kosha also hashes every chunk and probes it for
  compression. Multi-drive erasure-coded setups, which MinIO and RustFS are
  built for, were not benchmarked.
- **Enterprise features.** RustFS has KMS integration (Vault, AWS KMS),
  OIDC single sign-on, site and bucket replication, S3 Select, bucket quotas,
  audit logging, Swift, WebDAV and FTPS access, opt-in SFTP, and S3 Tables
  with an Iceberg catalog (preview). Kosha has none of these.
- **Spread-out clusters.** Garage is designed for clusters spread over
  several sites, has no leader, and serves static websites. It lacks
  versioning, Object Lock, bucket policies and notifications, which Kosha
  has.

**Which one to pick**

- **Kosha:** data that repeats (backups, VM images, datasets, build
  artifacts, versioned documents). Also when you need linearizable
  consistency, or encryption at rest that keeps deduplication.
- **RustFS:** a maintained MinIO-style server with an enterprise feature set.
- **Garage:** a small, low-maintenance cluster spread over homes or sites.
- **MinIO:** existing deployments. The community edition is marked
  unmaintained and ships as source only, so you build and patch it yourself.

Sources (checked October 2026):
MinIO [README](https://github.com/minio/minio#readme),
[consistency](https://github.com/minio/minio/blob/master/docs/distributed/README.md),
[compression](https://github.com/minio/minio/blob/master/docs/compression/README.md),
[lifecycle transition](https://github.com/minio/minio/blob/master/docs/bucket/lifecycle/README.md),
[KMS](https://github.com/minio/minio/blob/master/docs/kms/README.md),
[CopyObject](https://github.com/minio/minio/blob/master/cmd/object-handlers.go),
[console change](https://blocksandfiles.com/2025/06/19/minio-removes-management-features-from-basic-community-edition-object-storage-code/);
RustFS [README](https://github.com/rustfs/rustfs#readme),
[disk I/O](https://github.com/rustfs/rustfs/blob/main/crates/ecstore/src/disk/local.rs),
[CopyObject](https://github.com/rustfs/rustfs/blob/main/rustfs/src/app/object/copy.rs);
Garage [S3 compatibility](https://garagehq.deuxfleurs.fr/documentation/reference-manual/s3-compatibility/),
[features](https://garagehq.deuxfleurs.fr/documentation/reference-manual/features/),
[consistency_mode](https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#consistency_mode),
[CopyObject](https://github.com/deuxfleurs-org/garage/blob/main-v2/src/api/s3/copy.rs).
