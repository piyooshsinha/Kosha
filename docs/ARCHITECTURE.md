# Kosha architecture

```
                 S3 clients (SDKs, CLI, boto3, rclone, …)     browser console
                                   │ HTTP :9000                │ SigV4 in the browser
┌──────────────────────────────────▼───────────────────────────▼──────────────┐
│ kosha-s3      routing · SigV4/V2 · aws-chunked · XML        /_kosha/ui (React)│
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ Engine API
┌──────────────────────────────────▼──────────────────────────────────────────┐
│ kosha-engine                                                                 │
│  ObjectWriter: FastCDC → BLAKE3 → adaptive zstd ──► ChunkPlacement            │
│  Commands (CreateBucket, PutObject, …) ──────────► Consensus                  │
│  MetaStore (redb): deterministic state machine     ChunkStore + io_uring     │
└──────────────┬─────────────────────────────────────────────┬────────────────┘
   single node │ apply locally, store chunks locally          │ cluster
               ▼                                              ▼
                              ┌──────────────────────────────────────────────┐
                              │ kosha-cluster      internal HTTP :9100        │
                              │  openraft: log (redb) + state machine         │
                              │  leader forwarding · ReadIndex reads          │
                              │  rendezvous chunk placement · read-repair     │
                              └──────────────────────────────────────────────┘
```

The engine knows nothing about HTTP or Raft. It defines two seams:

- **`Consensus`** orders metadata commands. If no consensus is attached,
  commands are applied directly to the local state machine. In a cluster they
  go through the Raft log.
- **`ChunkPlacement`** decides where chunk bytes live. If no placement is
  attached, chunks go to the local store. In a cluster they go to a
  rendezvous-hashed replica set.

## Metadata as a deterministic state machine

Every mutation is a serializable `Command`: `CreateBucket`, `PutObject`,
`CopyObject`, `CompleteUpload`, and so on. `MetaStore::apply` applies a batch
of commands in one redb transaction. The rules that make replication safe:

1. Commands carry everything that is not a pure function of prior state:
   timestamps, upload ids, and on-disk chunk sizes. Replaying the same log
   gives byte-identical metadata on every node.
2. Each command checks all of its preconditions before mutating anything. A
   rejected command (`NoSuchBucket`, `PreconditionFailed`, …) is a *result*
   that is replicated like any other, not a state-machine failure. It leaves
   no trace and does not abort the rest of the batch.
3. The consensus layer's own state (last applied log id and membership) is
   written in the same transaction as the commands. After a crash the state
   machine is never ahead of or behind what it claims.

Conditional writes (`If-None-Match: *`) are therefore checked *in log order*.
If three nodes race to create the same key, exactly one wins. The cluster test
`conflicting_conditional_writes_on_different_nodes` checks this.

## Write path

1. The body is streamed, never buffered whole. `aws-chunked` framing is
   decoded on the fly, each chunk's signature is verified in the SigV4 chain,
   and SHA-256, MD5 and CRC32 are computed as the bytes flow.
2. FastCDC (min 256 KiB, avg 1 MiB, max 4 MiB) cuts chunks at
   content-defined boundaries, so an edit only changes the chunks around it.
3. Each chunk is hashed with BLAKE3 on the blocking pool. Up to
   `write_parallelism` chunks are in flight per upload.
   - **Single node:** the chunk is *pinned*, the write is skipped if the
     content already exists (dedup), otherwise it is compressed if a 64 KiB
     probe shows that pays off, then written to a temp file and renamed into
     place through the I/O backend.
   - **Cluster:** the chunk is sent to the top `replication` nodes in
     rendezvous order, in parallel. An unreachable node is skipped and the
     next-ranked node takes the replica.
4. A `PutObject` command is proposed. On a follower it is forwarded to the
   leader. The follower then waits until it has applied the entry itself, so
   the client reads its own write on whichever node it uses.
5. After the commit, pins are released. Chunks whose refcount reached zero
   are collected (see GC below).

Objects of 8 KiB or less are stored inline in the manifest and never touch
the chunk store.

## Read path

1. **Linearizable barrier.** On the leader, `ensure_linearizable` confirms
   leadership with a quorum. On a follower, the leader returns its commit
   index (ReadIndex) and the follower waits until it has applied that index.
   A read issued after a write completed, on any node, sees that write.
2. The manifest is read from the local redb (MVCC, never blocks writers). A
   range request maps only to the chunks it overlaps, and `read_ahead` chunks
   are fetched concurrently.
3. Each chunk is read locally if present. Otherwise it is fetched from the
   replicas in rendezvous order. Every chunk is BLAKE3-verified wherever it
   came from. A node that *should* hold a chunk but does not (for example, it
   was down during the write) stores the copy it fetched: **read-repair**.

## Chunk placement

Each chunk's replica set is the top `replication` voters ranked by
`BLAKE3(chunk_id ‖ node_id)` (rendezvous / highest-random-weight hashing).
Every node computes the same ranking without any coordination or placement
table. When a node is added or removed, only the chunks that rank that node
in their top set are affected.

Chunks are immutable and content-addressed, so replication needs no
consensus of its own: any copy of a chunk is the same bytes, and repairs are
idempotent.

## Garbage collection

A chunk file may be deleted only when **all** of these hold:

- No committed metadata references it (refcount 0 in the replicated state).
- It is not **pinned** by an in-flight upload on this node. Pins are taken
  *before* the dedup existence check and released *after* the metadata
  commit.
- Its mtime is older than the **grace period** (cluster default 15 min).
  Storing a chunk, or hitting an existing copy, refreshes the mtime. This
  protects chunks that an upload coordinated by *another* node has replicated
  here but not yet committed.

The check runs under the pin lock, so it cannot interleave with a dedup
check. Chunks freed by a command are collected right after it is applied. A
sweeper re-checks every chunk file every 10 minutes, which reclaims orphans
from aborted uploads and chunks deferred by the grace period.

## Raft details

- The log is stored in redb (`raft/raft-log.redb`). Each append is one fsynced
  transaction, and openraft's flush callback fires only after the commit.
- A snapshot is a full export of the metadata tables plus the consensus state,
  read in one redb transaction. openraft can build snapshots concurrently with
  `apply`, so the snapshot's log id is taken from the same transaction as the
  data, never from memory. A node that joins after the log was compacted
  installs the snapshot. The test `late_joiner_catches_up_from_snapshot`
  covers this.
- Joining: a new node asks any member, gets redirected to the leader, is added
  as a learner (and catches up), and is then promoted to voter.
- Internal endpoints (`/raft/*`, `/chunk/*`, `/cluster/*`) are msgpack over
  HTTP on a separate port. They are authenticated with a shared secret that
  is compared in constant time.

## io_uring

`IoBackend::Uring` runs one ring per core (up to 8), each owned by a driver
thread. Requests arrive over a channel, and an eventfd read posted on the ring
wakes the driver, so new work is picked up while other I/O is in flight. Each
request is a state machine (`openat → write* → fsync → close`,
`openat → read* → close`, or `renameat`) with short reads and writes
resubmitted. More requests than ring slots wait in a backlog. `IoMode::Auto`
probes the kernel for every opcode it needs and falls back to the blocking
thread pool when io_uring is disabled.

## Metadata tables (redb)

| Table | Key | Value |
|---|---|---|
| `buckets` | name | created time |
| `objects` | `bucket \0 key` | manifest: size, ETag, attrs, segments |
| `chunks` | BLAKE3 id | refcount, raw length, stored length |
| `uploads` / `upload_index` / `parts` | upload id / `bucket \0 key \0 id` / `id \0 part#` | multipart state |
| `meta` | `stats`, `sm_state` | counters, consensus state |

The `bucket \0 key` encoding makes B-tree order equal S3 listing order. A
delimiter listing *seeks past* each common prefix, so its cost is
O(entries returned) rather than O(keys under the prefix).

## Known limitations

- **No background rebalancing or re-replication.** After membership changes
  or a permanent node loss, chunks stay where they were placed. Reads still
  find them, because nodes are tried in rank order and then all voters, and
  read-repair restores copies on designated nodes when they are read. But an
  unread chunk on a dead node stays under-replicated.
- **Snapshots are built in memory** and sent whole. This is fine for
  millions of objects. Very large namespaces need streaming snapshots.
- **Every linearizable read costs a quorum round-trip** (plus a leader
  round-trip on followers). Leader leases would remove this.
- **Internal traffic is not encrypted.** Use a private network until mTLS
  lands.
- Removing a node is available in the library (`Cluster::remove_node`) but
  not yet from the CLI or console.
- A read racing with deletion of the same object can fail mid-stream.
- `ListMultipartUploads` does not paginate.

## Roadmap

**Next**
- Background re-replication and rebalancing driven by the rendezvous ranking.
- Leader-lease reads, mTLS between nodes, node removal from the console.
- Object versioning (manifests are immutable, so versions are cheap) and
  object lock (WORM).
- Users, access keys and bucket policies stored in the replicated state machine.
- Prometheus metrics. io_uring with registered buffers and `O_DIRECT` for
  large chunks.

**Later**
- Reed–Solomon erasure coding for cold chunks (for example 8+3), with
  replication for hot ones.
- Metadata sharding: multiple Raft groups by bucket or key range.
- Tiering: track access frequency per chunk, demote to a cold tier
  (HDD or another S3) and promote on read.
- Per-bucket trained zstd dictionaries and similarity-aware placement.
- Event notifications from the ordered metadata log.
