# Benchmarks

Measured on a shared 4-core VM; treat the numbers as indicative, not as a guarantee.

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

### Group commit and lease reads: before and after

Measured back-to-back on the same VM, same client, same workload
(4 KiB objects, 32 concurrent requests unless noted):

| | Before | After | |
|---|---|---|---|
| Single node, PUT | 987 req/s · p50 31 ms | **3,193 req/s** · p50 6.4 ms | **3.2×** |
| Single node, PUT at 128 concurrent | 895 req/s · p50 140 ms | **3,209 req/s** · p50 21 ms | **3.6×** |
| 3-node cluster, PUT via leader | 529–626 req/s · p50 ~50 ms | **1,432 req/s** · p50 19 ms | **2.3–2.7×** |
| 3-node cluster, PUT via follower | 638 req/s | **1,220 req/s** | **1.9×** |
| Leader read barriers needing a quorum round-trip | 100% | **0.2%** (31 of 17,200) | lease |
| Leader GET, one request at a time | p50 1.15 ms | **p50 0.87 ms** | −24% |

- **Before group commit**, throughput stayed flat from 32 to 128 concurrent
  requests while latency grew, the signature of a serialized commit path. On
  the cluster leader, a probe showed every Raft append carried exactly one
  entry.
- **Now**, single-node writes form batches in one durable redb transaction,
  and cluster writes put a batch of commands into one Raft entry. Batches
  form from load alone, so a lone request still commits immediately.
- **Lease reads** save one quorum round-trip per read on the leader, and a
  follower read now costs a single round-trip to the leader. On loopback a
  round-trip is ~0.3 ms, so the gain here is small. Across racks or
  availability zones it is a full network RTT per read.

### Single node: throughput under load

| Object size | Concurrency | PUT | GET |
|---|---|---|---|
| 4 KiB | 32 | **2,760 req/s** · p50 6.9 ms | **3,900 req/s** · p50 4.3 ms · p99 9.9 ms |
| 4 KiB | 128 | **2,830 req/s** · p50 24 ms | 2,650 req/s · p50 22 ms |
| 64 KiB | 32 | **840 req/s** (53 MiB/s) · p50 28 ms | **3,000 req/s** (188 MiB/s) · p50 7.5 ms |
| 1 MiB | 16 | **121 MiB/s** · p50 84 ms | **693 MiB/s** · p50 20 ms · p99 44 ms |
| 32 MiB | 4 | **117 MiB/s** · p50 962 ms | **562 MiB/s** · p50 217 ms |

### Single node: one request at a time (latency)

| Object size | PUT p50 / p99 | GET p50 / p99 |
|---|---|---|
| 4 KiB | **3.0 ms** / 8.6 ms | **0.77 ms** / 2.1 ms |
| 1 MiB | 19 ms / 29 ms | **4.2 ms** / 9.9 ms |

### Single node: one large stream (curl, `UNSIGNED-PAYLOAD`)

| Operation | Result |
|---|---|
| PUT 256 MiB, unique data | 245–340 MB/s (two runs) |
| PUT 256 MiB, data already stored (dedup) | 342 MB/s |
| GET 256 MiB (chunks still in page cache) | **1.14–1.17 GB/s** |
| Server-side COPY 256 MiB | **2.9 ms** (metadata only) |

### 3-node cluster (replication factor 2)

Writes go through Raft: a batch of commands becomes one log entry, which the
leader fsyncs and replicates to a majority. Each chunk is also written to 2
nodes. Follower reads make one round-trip to the leader; the leader answers
from its lease.

| Object size | Concurrency | PUT via leader | GET via follower |
|---|---|---|---|
| 4 KiB | 32 | **1,180 req/s** · p50 23 ms · p99 41 ms | **3,700 req/s** · p50 5.5 ms · p99 25 ms |
| 64 KiB | 32 | **416 req/s** (26 MiB/s) · p50 67 ms | **2,440 req/s** (153 MiB/s) · p50 9.6 ms |
| 1 MiB | 16 | **93 MiB/s** · p50 133 ms | **678 MiB/s** · p50 21 ms |
| 32 MiB | 4 | **99 MiB/s** | **299 MiB/s** |

Through a follower, 4 KiB PUTs run at 1,100 req/s. With one request at a
time:

- 4 KiB PUT: **7.7 ms** p50.
- Linearizable 4 KiB GET on a follower: **1.1 ms** p50.
- 4 KiB GET on the leader, served from its lease: **0.72 ms** p50.

Run-to-run variance on this shared VM is about ±15% for most rows, more for
large objects in the cluster.

### What limits these numbers now

- **Large writes are CPU-bound on this VM.** The SDK hashes every payload
  with SHA-256, and the server re-verifies it and also computes MD5 (for the
  ETag) and BLAKE3 (for the chunk address), on the same 4 cores as the client
  (and, in the cluster, two other nodes). Identical-content 1 MiB PUTs, where
  dedup skips the disk write entirely, are only modestly faster
  (164 vs 143 MiB/s in an earlier run). The single-stream curl upload with
  `UNSIGNED-PAYLOAD` skips the SHA-256 and reaches 245–340 MB/s.
- **Chunk durability is not the limit.** Each chunk is written to a temp
  file, renamed into place, and made durable *together with its directory
  entry* before the PUT is acknowledged. With `--sync group` (the default)
  concurrent writers share one `syncfs`. Measured on 64 KiB PUTs:
  `group` 860–904 req/s, `none` (no sync at all) 772–886 req/s,
  `file` (fsync each file and directory) 736–749 req/s. Correct durability is
  therefore effectively free. An earlier version of this README guessed that
  per-chunk fsync limited 64 KiB writes; the `none` row shows the limit is
  CPU.
- **Small writes** now run at about the same rate as small reads, so the
  client, the HTTP stack and SigV4 verification share the remaining cost.

### Compared with RustFS and MinIO

All three servers ran on the same VM (4 cores shared with the client, 15 GB
RAM, ext4 on a virtio disk), one at a time, each with a fresh data
directory on the same disk and the same client: `s3bench` (AWS SDK for
Rust, which signs every payload with SHA-256), writing random objects and
checking every GET byte for byte (the same client as in the sections
above). There were three rounds: the first in the order Kosha, RustFS,
MinIO, the other two in reverse. The table shows the median and, in
brackets, the range. Every run finished with **0 errors**.

- **Versions:** Kosha built from this repository (server code as of commit
  `31df1b3`); RustFS 1.0.1 (official release
  binary); MinIO built from its final community commit (February 2026).
- **Setup:** single node, single drive, default settings. In single-drive
  mode MinIO and RustFS do not erasure code, and Kosha does not replicate,
  so none of the three has redundancy here. Multi-drive and multi-node
  setups were not measured.
- **Durability as configured by default** (read from each project's
  source): Kosha syncs chunk data, directory entries and metadata before
  acknowledging (`--sync group`). RustFS's default `strict` mode does the
  same. MinIO `fdatasync`s each data file but not directory entries.

| Workload | Kosha | RustFS | MinIO |
|---|---|---|---|
| PUT 4 KiB, 32 concurrent | **2,770** req/s [2,711–2,892] · p50 6.8 ms | 657 req/s [595–669] · p50 46 ms | 923 req/s [915–1,087] · p50 30 ms |
| GET 4 KiB, 32 concurrent | **3,810** req/s [3,768–4,122] | 3,210 req/s [3,120–3,428] | 3,171 req/s [2,936–3,303] |
| PUT 64 KiB, 32 concurrent | **1,104** req/s [1,084–1,111] | 508 req/s [484–537] | 765 req/s [763–873] |
| GET 64 KiB, 32 concurrent | **3,222** req/s [3,164–3,552] | 1,941 req/s [1,877–2,084] | 2,664 req/s [2,598–2,737] |
| PUT 1 MiB, 16 concurrent | 132 MiB/s [131–139] | 108 MiB/s [106–108] | 135 MiB/s [127–136] |
| GET 1 MiB, 16 concurrent | **959** MiB/s [872–1,058] | 792 MiB/s [753–804] | 795 MiB/s [764–806] |
| Server-side COPY 1 MiB, 16 concurrent | **3,207** copies/s [3,019–3,796] · p50 4.0 ms | 159 copies/s [148–163] · p50 102 ms | 199 copies/s [196–210] · p50 74 ms |
| PUT 32 MiB, 4 concurrent | 139 MiB/s [138–145] | 135 MiB/s [132–142] | **161** MiB/s [159–162] |
| GET 32 MiB, 4 concurrent | 556 MiB/s [535–568] | 516 MiB/s [464–549] | 531 MiB/s [522–545] |
| Disk used by 1,000 identical 1 MiB objects | **0 MiB** (already stored) | 1,000 MiB | 1,000 MiB |
| Data directory after every workload | **2.8 GiB** | 4.6 GiB | 4.6 GiB |
| Peak server memory (RSS) | **345–365 MiB** | 720–769 MiB | 627–630 MiB |

What the numbers show, and what they don't:

- **Small writes** are where Kosha leads most: at 4 KiB, 4–4.9× RustFS
  and 2.5–3.2× MinIO; at 64 KiB, about 2.2× and 1.4×. Group commit lets concurrent writes share one durable
  transaction and one `syncfs`. Under `strace`, 500 4 KiB PUTs plus
  100 1 MiB PUTs cost Kosha 343 sync calls and MinIO 600.
- **RustFS's durability mode is not the main cost.** With
  `RUSTFS_DURABILITY_MODE=relaxed`, which RustFS documents as matching
  MinIO's default (we confirmed it cut sync calls 7× for 500 PUTs), RustFS
  reached 761 req/s at 4 KiB and 622 req/s at 64 KiB in one round.
- **Large uploads** are limited by CPU: the client's SHA-256 signing and
  the servers' own hashing share the same 4 cores. MinIO is fastest at
  32 MiB and level with Kosha at 1 MiB.
- **Copies** in Kosha are metadata only. RustFS and MinIO rewrite the data
  when copying to a new key.
- **Deduplication** is the best case here: identical objects. Real data
  deduplicates less, and random data (every other row) not at all.
- **GETs** read data written moments earlier, so they mostly come from the
  page cache.
- **ext4.** MinIO's documentation recommends XFS and warns that ext4 does
  not honour all of its consistency assumptions. These results are for
  ext4 only.

Reproduce (RustFS and MinIO binaries are not included):

```sh
cargo build --release --bins --examples
GOBIN=$PWD/bin go install github.com/minio/minio@latest   # MinIO is source-only
# RustFS: download a release binary from github.com/rustfs/rustfs/releases
BENCH_DIR=/path/on/the/disk/to/test scripts/compare-bench.sh \
  target/release/kosha ./rustfs ./bin/minio 3
```

### Cost of encryption at rest

Same VM, single node, back-to-back runs with and without
`--encryption-key-file`:

| Workload | Plain | Encrypted | |
|---|---|---|---|
| 4 KiB PUT, 32 concurrent | 3,260 req/s | 1,300 req/s | small objects become chunk files instead of inline metadata |
| 4 KiB GET, 32 concurrent | 4,450 req/s | 3,860 req/s | |
| 64 KiB PUT / GET | 940 / 3,210 req/s | 1,064 / 3,180 req/s | within noise |
| 16 MiB PUT / GET, 4 concurrent | 139 / 875 MiB/s | 146 / 667 MiB/s | decryption costs ~24% on large reads |

These runs also found a bug that affected unencrypted servers too. The S3
listener did not set `TCP_NODELAY`, so objects stored as chunks but smaller
than a few TCP segments waited ~40 ms on Nagle's algorithm and delayed ACKs.
Encrypted 4 KiB GETs went from 714 to 3,860 req/s with the fix. Every
listener now sets it.

### Reproduce

```sh
cargo build --release --bins --examples
AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=change-me-please \
  ./target/release/examples/s3bench --endpoint http://127.0.0.1:9000 \
  --size 64KiB --count 3000 --concurrency 32
# cluster: --endpoint http://127.0.0.1:9001 --read-endpoint http://127.0.0.1:9003
# dedup:   add --duplicate to upload identical content
# copy:    add --copy for a phase that server-side copies every object
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

#### Direct I/O for large chunks (`--io-direct`)

With `--io-direct`, chunk files of at least `--io-direct-min` bytes (default
256 KiB) are written and read with `O_DIRECT`, bypassing the page cache. Each
ring has a pool of 4 KiB-aligned buffers, registered with the kernel
(`READ_FIXED`/`WRITE_FIXED`) when the locked-memory limit allows and used as
plain aligned buffers otherwise. A write sends its aligned prefix directly and
the unaligned tail through the page cache. If the filesystem refuses
`O_DIRECT` (tmpfs, some overlay setups), the ring turns it off and carries on
buffered. When every buffer is busy, a request goes through the page cache;
`kosha_direct_fallbacks_total` counts these.

On this VM (virtio disk, ext4) direct I/O is **not faster**. Per run, with
1 MiB files and the page cache dropped before each pass (`COLD=1`, see
`iobench`):

| 1 MiB files | io_uring | io_uring + `--io-direct` |
|---|---|---|
| writes + fsync | 570–620 MB/s | 390–410 MB/s |
| reads (cold cache) | 1.7–2.0 GB/s | 1.3–2.0 GB/s |
| page cache left by 1.5 GB of writes and reads | 1.5 GB | 0.15 GB (from fallbacks) |

What it buys is cache isolation. Large chunk traffic no longer evicts the
metadata database and the small, hot chunks from memory, and the node's
memory use does not grow with the data it streams. Turn it on for storage
nodes on fast NVMe that move mostly large objects. Leave it off (the default)
otherwise, in particular for workloads that re-read the same large objects,
which the page cache would serve.

Docker's default seccomp profile and `kernel.io_uring_disabled` block
io_uring. `--io auto` (the default) detects this and falls back to threads;
`/_kosha/stats` and the console report which backend is active.
