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

## Exactly-once writes

Cluster writes can be retried. For example, a follower forwards a batch, the
leader commits it, the reply is lost, and the follower sends it again. To
make retries harmless, every command gets a 128-bit request id when its
batch forms, and the id stays fixed across retries.

- The state machine records each id's result in the `requests` table, in
  the same transaction as the command.
- A re-delivered id returns the remembered result and changes nothing.
- The memory is a window of the most recent 100,000 ids, evicted in log
  order, so every replica evicts identically. It is part of snapshots.
- Remembered object results omit the chunk manifest to stay small.

Tests: `requests_apply_at_most_once` (state machine) and
`retried_batches_apply_exactly_once` (the same batch committed twice through
Raft yields one bucket and two successes).

## Group commit

Every metadata mutation must be durable before it is acknowledged, and
durable commits are serialized. If each request commits on its own,
throughput is capped by the fsync rate no matter how many clients there are.
Kosha batches instead:

- **Single node.** A dedicated committer thread owns the write path. While
  one transaction is being made durable, new commands queue up, and the next
  transaction applies all of them (up to 512).
- **Cluster.** Each node has a proposer. Queued commands become one Raft
  entry, a `CommandBatch` of up to 256 commands. A follower forwards the whole
  batch to the leader in one RPC.

Batches form from load alone: a lone request commits immediately, with no
timer. This is safe because a command rejected by a precondition leaves no
trace and does not affect its batch-mates (rule 2 above). For example, 64
racing `If-None-Match: *` creates of one key in a single batch still produce
exactly one winner (`group_commit_preserves_per_command_semantics`). Callers
keep their chunk pins until their own reply arrives, which happens after the
batch commits.

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

1. **Linearizable barrier.** The leader determines a read index: its commit
   index, after proving it is still leader (see *Leader leases*). A follower
   asks the leader for it (ReadIndex) and waits until it has applied that
   index. A read issued after a write completed, on any node, sees that
   write.
2. The manifest is read from the local redb (MVCC, never blocks writers). A
   range request maps only to the chunks it overlaps, and `read_ahead` chunks
   are fetched concurrently.
3. Each chunk is read locally if present. Otherwise it is fetched from the
   replicas in rendezvous order. Every chunk is BLAKE3-verified wherever it
   came from. A node that *should* hold a chunk but does not (for example, it
   was down during the write) stores the copy it fetched: **read-repair**.

## Leader leases

`ensure_linearizable` proves leadership by heartbeating every voter and
waiting for a quorum, on *every* call. Kosha keeps a lease instead
(`kosha-cluster/src/lease.rs`):

- Record `t0` just before a successful `ensure_linearizable`. A quorum then
  accepted a heartbeat sent after `t0`, and openraft voters refuse to vote for
  `election_timeout_max` after hearing from a leader. So no other leader can
  exist before `t0 + election_timeout_max`.
- Reads are served from the lease until `t0 + 0.8 × election_timeout_min`,
  a margin for clock-rate drift between nodes. Each lease read requires that
  this node is still the committed leader *in the lease's term*, and that the
  commit index belongs to that term (the leader has committed its own no-op).
- An expired lease is renewed single-flight: one caller does the quorum check
  while concurrent readers wait and reuse the result.

Over 99% of leader read barriers in benchmarks are lease hits. The leader
counts lease and quorum reads separately, and `/_kosha/cluster` reports them.
`--no-lease-reads` disables leases for environments where bounded clock
drift cannot be assumed.

## Chunk placement

Each chunk's replica set is the top `replication` voters ranked by
`BLAKE3(chunk_id ‖ node_id)` (rendezvous / highest-random-weight hashing).
Every node computes the same ranking without any coordination or placement
table. When a node is added or removed, only the chunks that rank that node
in their top set are affected.

Chunks are immutable and content-addressed, so replication needs no
consensus of its own: any copy of a chunk is the same bytes, and repairs are
idempotent.

## Background repair and rebalancing

Every node runs a repairer (`kosha-cluster/src/repair.rs`, every
`--repair-interval-secs`). It looks only at its own disk and needs no
coordination. Each pass probes which voters are live, then walks every chunk
referenced by committed metadata, in pages:

- **Repair.** If this node is among the top `replication` *live* voters for
  a chunk and lacks a copy, it fetches one from a live peer, verifies the
  BLAKE3 address, and stores it. When a node dies, the next-ranked live
  node takes over its share and the replica count is restored without
  waiting for reads.
- **Rebalance.** If this node holds a copy but is in the top `replication`
  of neither all voters nor live voters, the copy is surplus. It is deleted
  only after every designated node is live and confirms its copy (HEAD), and
  only if no local upload has it pinned. The replica count never drops below
  target.

Chunks not yet referenced by metadata are never touched, so in-flight
uploads are safe. Tests cover both directions with exact counts:
`repair_restores_replicas_after_a_node_dies` and
`repair_removes_surplus_replicas`.

## Removing nodes

`DELETE /_kosha/cluster/nodes/{id}` (SigV4) and the console's **Remove**
button work from any node: a follower forwards the request to the leader.
The leader refuses non-voters and refuses to remove the last voter. After
the membership change commits, rendezvous rankings no longer include the
node, and background repair moves its share of chunks to the remaining
nodes.

## Chunk durability

A chunk is written to a temp file and renamed into place. To survive power
loss, both the file data and the rename (a directory change) must be durable
before the metadata that references the chunk commits. `--sync` picks how:

- **`group`** (default): write and rename without syncing, then wait for a
  shared `syncfs(2)` of the chunk filesystem. Concurrent writers share one
  call. A writer that arrives while a sync is running waits for the next
  one, because its writes may postdate it. Measured: 1,335 chunk writes made
  durable by 731 `syncfs` calls in one run.
- **`file`**: fsync the file, rename, fsync the directory.
- **`none`**: no syncs (metadata is still durable).

Earlier versions fsynced the file but not the directory, so a rename could
be lost on power failure while metadata already referenced the chunk. Both
`group` and `file` close that gap.

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

## Versioning

Each version is a row in the `versions` table keyed by
`bucket \0 key \0 (u64::MAX - seq)` (big-endian), so a key's versions sort
newest first and ListObjectVersions is a single ordered scan. `seq` comes
from a counter in the state machine, so every node assigns the same version
ids; ids are shown as 16 hex digits, or `null` for versions written while
versioning was off or suspended.

`objects` keeps mirroring the latest non-marker version, so unversioned
reads and listings cost exactly what they did before. In a versioned bucket
the version rows own the chunk references; deleting a version releases them
and recomputes `objects` from the next-newest version. Because versions are
manifests over shared chunks, a new version of a 1 GiB file that changes a
few MiB stores only those chunks.

Turning versioning on migrates existing objects to `null` versions in the
same transaction. Keys may not contain NUL, which keeps the key encoding
unambiguous.

## Object Lock

Retention (`GOVERNANCE` or `COMPLIANCE` with a retain-until date) and legal
hold are stored on each version. Every check runs inside the state machine
against the command's own timestamp, so all replicas reach the same verdict:

- A version under retention or legal hold cannot be deleted, and a locked
  `null` version cannot be overwritten.
- `GOVERNANCE` can be bypassed with `x-amz-bypass-governance-retention`.
  `COMPLIANCE` cannot be bypassed, shortened or removed.
- Legal hold blocks deletion even with the bypass header.
- Lock-enabled buckets are always versioned, and versioning cannot be
  suspended on them.

## Lifecycle

A pass (every `--lifecycle-interval-secs`, on the leader only) evaluates
each bucket's rules and *proposes ordinary commands*: DeleteObjects for
expired current objects, noncurrent versions and lone delete markers, and
AbortUpload for stale multipart uploads. Lifecycle therefore gets
replication, ordering and Object Lock checks for free; locked versions are
reported as refused and left in place. A version counts as noncurrent from
the moment its successor was written.

## Erasure coding

With `--erasure K+M`, a chunk of at least `erasure_min_len` bytes is placed
as shards instead of copies. The writing node:

1. encodes the chunk to its stored form (compression, then encryption);
2. splits it into K equal data shards and computes M parity shards
   (Reed–Solomon over GF(2^8));
3. frames each shard with a 48-byte header: index, K, M, the stored-form
   length, and a BLAKE3 checksum of the shard;
4. sends shard `i` to the `i`-th voter of the chunk's rendezvous ranking.
   A node that fails gets its shard handed to the next node beyond K+M.

Shards are files named `<id>.s<i>` beside full chunk files. The same pins,
grace period, sweeper, eviction and garbage collection apply to them.

**Reads** prefer a local full copy, then the K data shards from their
designated nodes (concatenation, no decoding). If any are missing or fail
their checksum, the reader asks every voter which shards it holds,
reconstructs from any K, and verifies the chunk against its content address.

**Ownership:** the shard index a node is responsible for is a pure function
of the ranking and the set of live voters (`ec::assignment`). Rank `i < K+M`
owns index `i` while it is live. Indices of dead designated nodes go, in
order, to the live nodes ranked after K+M. One failure therefore moves only
the failed node's shards.

**Repair:** each node rebuilds its own missing index from any K others.
Holders of shards they are no longer responsible for drop them once the
responsible node is live and confirms its copy.

**Mixed layouts:** chunks written while fewer than K+M voters existed are
replicated. Repair recognises them because no voter holds shards of them,
and reads always try both layouts.

## Cold tier

Tiering works on chunks, so it composes with dedup. Each chunk record keeps
`cold_refs`, the number of its references held by objects in a cold storage
class, next to `refs`. The rules:

- **Transition** (lifecycle rules for current or noncurrent versions, or a
  cold class requested on PUT/COPY/CompleteMultipartUpload) sets the
  version's `storage_class` and increments `cold_refs` on its chunks, in the same command as the write when requested
  on upload. A chunk with `cold_refs == refs` and no remote copy goes into
  the `tier_queue` table. Dropping the last *hot* reference does the same.
- **Offload** runs on the leader. It reads each queued chunk (locally or from a
  replica, verified), re-encodes it to its stored form (deterministic
  compression, then encryption if enabled), and uploads it under a fresh key
  `<id>.<random>`. It then commits `ChunkOffloaded { id, key }`.
- **Eviction**: applying that command records `remote = key`. If the chunk
  is still cold-only it is *evicted*, and every node deletes its local copy
  in `apply_and_collect`. The same pin lock that protects garbage collection
  protects an upload that is writing the same content at that moment.
- **Reads** go to the local store or the replicas first. On a miss, a chunk
  with a remote key is fetched from the cold store and decoded with the same
  verification as a local read.
- **Repair** walks only chunks that need local replicas (`chunk_page` skips
  evicted ones).
- **Hot again**: a new upload of the same content writes a local copy again
  and adds a hot reference, so that chunk is served locally. Copies are
  STANDARD.
- **Deletion**: when `refs` reaches 0, the remote key goes to a garbage list
  and is deleted. Because every upload uses a fresh key, a late delete can
  only remove the copy it was meant for, never a newer upload of the same
  content. If an upload finishes after its chunk was freed, or twice across
  a leader change, the extra key is garbage too.

The cold store is a small trait (`put`/`get`/`delete`). The server
implements it with a minimal SigV4 client, so any S3-compatible service
works, including another Kosha.

## Event notifications

Notifications are produced by the state machine, not by request handlers.
Before a command is applied, `events::Source` records what it is (put, copy,
complete, delete, tagging, and whether lifecycle issued it). If the command
succeeds, `Tx::emit` matches the result against the bucket's rules and
appends one `Event` per match to the `events` table, keyed
`target \0 seq`, in the same transaction. A global `event_seq` counter gives
every event a unique, increasing sequencer.

So every replica holds identical queues, events cannot be lost between a
change and its notification, and their order is the log order.

Delivery is outside the engine. On the leader, one task per webhook target
reads the oldest pending events, posts them one at a time, and proposes
`AckEvents { target, upto }` for the prefix that succeeded. On a failure it
backs off (0.5 s doubling to 60 s) and retries the same event, so order is
kept. After a leader change the new leader resumes from the replicated queue.

An event delivered but not yet acknowledged when the leader dies is sent
again, which makes delivery at least once. A queue is capped at 100,000
events per target: beyond that the oldest are dropped and counted in
`kosha_events_dropped_total`.

## Authentication and authorization

Every request is authenticated first (SigV4 header, presigned or streaming;
SigV2 header or presigned; or unsigned), which yields a principal: a root
key, a user, or anonymous. Root keys come from the command line and skip
authorization. Users live in the replicated `users` table, so any node can
look up a secret locally on every request; `PutUser`/`DeleteUser` are
ordinary state-machine commands.

Authorization maps the request to IAM actions and resources (`iam::required`)
and evaluates the user's identity policy and the bucket's policy together:
an explicit `Deny` in either wins, an `Allow` in either grants, and nothing
matching denies. Two checks need the handler's help, because they depend on
the request body or a second resource: `DeleteObjects` checks
`s3:DeleteObject` per key, and copies check `s3:GetObject` on the source.
Policies are validated when stored and parsed again when evaluated (they are
small; parsing is microseconds). Root and development-mode requests skip
building the condition context entirely.

## Encryption at rest

Encryption has to coexist with content addressing: chunk ids are the dedup
key, the replication unit, and the integrity check. Kosha keeps all three
roles by making encryption *deterministic per content*:

- `id = BLAKE3-keyed(K_id, plaintext)`. Equal content gets equal ids, so dedup
  works across objects, buckets and users. Unlike a plain hash, the id does
  not let anyone without the key test for known content.
- `stored = AES-256-GCM-SIV(K_enc, nonce = id[..12], aad = codec byte,
  zstd(plaintext))`. GCM-SIV is misuse-resistant: repeating a nonce with the
  same plaintext only repeats the ciphertext, which the id already reveals.
  Replicas are therefore byte-identical.
- `K_id` and `K_enc` are derived from one master key with BLAKE3's KDF, and a
  short fingerprint (a third derivation) is written to `chunks/ENCRYPTION`.
  Opening a store with a different key, without a key, or adding a key to an
  unencrypted store fails at startup. Joining a cluster compares
  fingerprints.
- Reads decrypt (authenticating the bytes), decompress, and re-derive the
  keyed id, so corruption and tampering are both caught.
- Inline objects would put plaintext into the metadata, so encrypted engines
  set `inline_max = 0`.

Metadata is not encrypted: names, sizes, tags, user metadata and user
secrets are readable in redb and in the Raft log.

## Transport security

The S3 listener and the cluster listener share `tls::TlsListener`. It
accepts TCP connections and runs each TLS handshake on its own task with a
timeout, so a slow or malicious client cannot stall the accept loop. It
implements `axum::serve::Listener`, so graceful shutdown and `ConnectInfo`
(for `aws:SourceIp`) work as with plain TCP. With `--cluster-tls-*`, the
cluster server requires client certificates signed by the CA, and the RPC
client presents its own certificate and verifies the server's name against
the CA. The shared secret is still checked on top of mTLS. Every listener
sets `TCP_NODELAY`.

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
| `chunks` | BLAKE3 id | refcount, cold refcount, raw length, stored length, cold-tier key |
| `uploads` / `upload_index` / `parts` | upload id / `bucket \0 key \0 id` / `id \0 part#` | multipart state |
| `tier_queue` | chunk id | chunks waiting to be offloaded |
| `events` | `target \0 seq` | undelivered notifications |
| `users` | access key | user: secret, name, identity policy, enabled |
| `versions` | `bucket \0 key \0 (MAX - seq)` | version: manifest or delete marker, lock state |
| `requests` / `request_seq` | request id / sequence | results of recent writes, for exactly-once retries |
| `meta` | `stats`, `sm_state` | counters, consensus state |

The `bucket \0 key` encoding makes B-tree order equal S3 listing order. A
delimiter listing *seeks past* each common prefix, so its cost is
O(entries returned) rather than O(keys under the prefix).

## Known limitations

- **Repair scans all chunk metadata every pass.** This is O(chunks) per
  node per interval. That is fine up to millions of chunks; beyond that it
  needs an incremental, membership-change-driven scan.
- **Lease reads assume bounded clock-rate drift** between nodes (the 20%
  margin). Turn them off with `--no-lease-reads` where that does not hold.
- **Snapshots are built in memory** and sent whole. This is fine for
  millions of objects. Very large namespaces need streaming snapshots.
- **Internal traffic is encrypted only with `--cluster-tls-*`.** Without it,
  keep port 9100 on a private network.
- **Metadata is not encrypted at rest**, including users' secret keys (they
  are needed in plaintext to verify signatures). Protect the data directory.
- **Policies cover S3 actions only.** There is no IAM API, no groups or roles,
  and no STS session credentials; ACLs are read-only.
- **Encryption cannot be turned on for an existing data directory.** Migrate
  by copying the data to a new encrypted node or cluster.
- **`--sync group` flushes the whole chunk filesystem.** Put the data
  directory on a filesystem Kosha does not share with write-heavy neighbours,
  or use `--sync file`.
- A read racing with deletion of the same object can fail mid-stream.
- **Lifecycle passes scan every rule's prefix** and look up tags per object
  when a rule filters on tags. Expiration does not support transitions to
  other storage classes.

## Roadmap

**Done since v0.1:** group commit (single node and Raft), leader-lease
reads, background re-replication and rebalancing, node removal from the
console and API, ListMultipartUploads pagination.

Also done: exactly-once retried writes (request ids), group sync for chunk
data, Prometheus metrics, object and bucket tagging, bucket CORS.
Versioning, Object Lock (retention and legal hold) and lifecycle rules.
Users, identity and bucket policies (with anonymous access), SigV2 header
auth, HTTPS, mutual TLS between nodes, and dedup-preserving encryption at
rest. Event notifications to webhooks from the replicated metadata log.
A chunk-level cold tier on any S3-compatible service. Reed–Solomon erasure
coding across nodes.

**Next**
- io_uring with registered buffers and `O_DIRECT` for large chunks.
- Key rotation for encryption at rest (re-encrypting chunks in the
  background under a new key).

**Later**
- Erasure-coding chunks in the background once they turn cold, instead of
  choosing the layout at write time.
- Access-driven tiering: demote chunks by read frequency rather than by
  lifecycle rules, and promote hot chunks back on read.

**Deliberately not built (yet)**

- **Metadata sharding (multiple Raft groups).** One Raft group already
  carries ~1,400 commits/s of 4 KiB PUTs on a shared 4-core VM (group commit
  batches them into far fewer log entries), and metadata is tiny next to
  data. Sharding would add cross-shard transactions for `CopyObject` and
  versioned deletes, a placement directory, and per-shard leaders and
  leases. That cost is justified only once a measured workload saturates the
  single group. The state machine is already deterministic and key-ordered,
  so splitting by bucket later is a contained change.
- **Per-bucket trained zstd dictionaries.** Dictionaries pay off for many
  small, similar records (JSON events, log lines). Kosha's chunks are
  256 KiB–4 MiB, where zstd already finds the redundancy inside each chunk,
  and the adaptive probe already skips incompressible data. Dictionaries
  would also add a versioned, replicated dictionary store that every read
  depends on. They are worth building only with evidence from a workload of
  small, similar objects.
