# Kosha architecture

Kosha has two halves with a hard boundary between them:

```
            S3 clients (SDKs, CLI, boto3, rclone, ...)
                              │  HTTP
┌─────────────────────────────▼──────────────────────────────┐
│ kosha-s3      routing · SigV4/V2 · aws-chunked · XML        │  protocol
└─────────────────────────────┬──────────────────────────────┘
                              │  Engine API (async Rust)
┌─────────────────────────────▼──────────────────────────────┐
│ kosha-engine                                                │
│   ObjectWriter ── FastCDC ── BLAKE3 ── adaptive zstd ──┐    │
│                                                        ▼    │  storage
│   Metadata (redb, ACID)               Chunk store (files)   │
│   buckets · manifests · refcounts     ab/cd/<blake3 hash>   │
│   multipart state · stats             verified on read      │
└─────────────────────────────────────────────────────────────┘
```

The engine knows nothing about HTTP. Other front ends (gRPC, a FUSE mount, an
admin API) can be added later without touching storage code.

## Write path

1. The body is streamed. Nothing is buffered whole. `aws-chunked` framing is
   decoded on the fly, and each chunk's signature is verified in the SigV4
   chain. The payload SHA-256 and CRC32 are computed while the bytes flow.
2. `ObjectWriter` runs FastCDC (min 256 KiB, avg 1 MiB, max 4 MiB). Cut points
   depend on content, so inserting bytes in the middle of an object changes
   only the chunks around the edit.
3. Each chunk is handed to a blocking worker (up to `write_parallelism` at once). The
   worker hashes it with BLAKE3, *pins* it, and skips the write entirely if the
   chunk already exists. Otherwise it probes compressibility on a 64 KiB sample
   at zstd level 1, compresses only if that saves more than 5%, writes to a temp
   file, and renames the file into place.
4. One metadata transaction then checks preconditions, increments chunk
   refcounts, swaps in the new manifest, decrements the old manifest's chunks,
   and updates stats. All of these commit together or not at all.
5. After the commit, the pins are released and any chunk whose refcount reached zero is
   garbage-collected.

Objects of 8 KiB or less never reach the chunk store. Their bytes are stored in
the manifest.

### Garbage collection safety

A chunk can be referenced by an upload that has written it but not committed
yet. The rules:

- A writer pins a chunk *before* checking whether the chunk file exists, and
  keeps the pin until *after* its metadata commit.
- The collector deletes a file only while holding the pin lock, and only after
  it has confirmed in a fresh read transaction that the chunk is unpinned and
  unreferenced.

So a chunk is never deleted while an upload is about to reference it. The test
`concurrent_put_delete_keeps_shared_chunks` stresses this interleaving.

## Read path

The manifest is fetched in one read transaction (MVCC, so it never blocks writers).
A range request maps to the segments that overlap it. Only those chunks are read,
and `read_ahead` of them are fetched concurrently. Each chunk is decompressed and
its BLAKE3 hash re-checked before any byte reaches the client. Silent disk
corruption becomes an error instead of bad data.

## Metadata model (redb tables)

| Table | Key | Value |
|---|---|---|
| `buckets` | name | created time |
| `objects` | `bucket \0 key` | manifest: size, ETag, attrs, segments |
| `chunks` | BLAKE3 id | refcount, raw length, stored length |
| `uploads` / `upload_index` / `parts` | upload id / `bucket \0 key \0 id` / `id \0 part#` | multipart state |
| `meta` | `stats` | object / byte / chunk counters |

The `bucket \0 key` encoding makes the byte order of the B-tree identical to S3
listing order. A delimiter listing *seeks past* each common prefix instead of
scanning every key under it, so `ls` on a directory with millions of entries
costs O(entries returned), not O(keys under the prefix).

## Why these choices

- **Content-defined chunking + BLAKE3.** Dedup is a free side effect of
  integrity checking, and it covers the workloads that dominate object storage:
  backups, VM and container images, datasets with versions, build artifacts.
- **Manifests instead of files.** Copy, rename-by-copy, multipart completion and
  (later) snapshots all become metadata operations.
- **An embedded ACID store** instead of sidecar files. One crash-safe commit per
  operation means there are no partial states to repair after a crash.
- **Rust + Tokio.** Memory safety without a GC, predictable latency, and
  zero-copy `Bytes` from the socket to the chunker.

## Known limitations (v0.1)

- Single node. No replication or erasure coding yet (see the roadmap).
- If an upload is aborted mid-stream, chunk files it wrote can be left on disk with
  no reference. They are harmless but use space until an orphan scrub exists.
- A read that races with deletion of the same object can fail mid-stream.
- `ListObjects` decodes each listed manifest. Very large manifests should get a
  separate summary table.
- `ListMultipartUploads` and `ListParts` paginate only partially.

## Roadmap

**Phase 1: single node, production hardening**
- Orphan-chunk scrub and an online integrity scrubber (re-verify every chunk in the background).
- Hot-chunk cache (adaptive LRU) and `io_uring` I/O on Linux.
- Object versioning (manifests are immutable, so versions are cheap) and object lock (WORM).
- Bucket policies and multiple users. Prometheus metrics.

**Phase 2: distributed**
- Metadata replicated with Raft (openraft) and sharded by bucket/key range.
- Chunk placement via rendezvous hashing over failure domains. Reed–Solomon
  erasure coding (for example 8+3) for cold chunks, replication for hot ones.
- Because chunks are immutable and content-addressed, replication needs no
  coordination: a chunk is the same bytes everywhere, and repairs are idempotent.

**Phase 3: smart tiering**
- Access-frequency tracking per chunk, with transparent demotion to a cold tier
  (HDD or another S3) and promotion on read.
- Similarity-aware placement and compression dictionaries trained per bucket.
- Event notifications and change feeds (the metadata log is already ordered).
