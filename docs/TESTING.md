# Fault testing

Crash, power-cut, partition and clock-drift tests, and how to run them.

## Fault testing

Three test programs check Kosha under faults: process crashes, power cuts,
and network partitions with clock drift. Each one also checks that it
really catches the faults it looks for: crashing on purpose, deleting data,
or (for partitions) driving a clock far enough out of spec to cause stale
reads. The results below are from the 4-core development VM, with the
server code at this commit.

### Process crashes (`crashtest`)

```sh
cargo build --release --bins --examples
target/release/examples/crashtest --kosha target/release/kosha --rounds 50
target/release/examples/crashtest --kosha target/release/kosha --cluster --chaos --rounds 30
# pass server flags after --, e.g. -- --erasure 2+1 or -- --encryption-key-file key.hex
```

Twelve writers each own 16 keys and change them one operation at a time:
PUTs from 0 bytes to 12 MiB (inline objects, chunk boundaries, multipart
uploads, partly shared content so deduplicated chunks are exercised),
DELETEs and server-side COPYs. Every round kills a server with `SIGKILL`
at a random moment, restarts it, and then checks every key on every node,
twice:

- an acknowledged operation must be visible, byte for byte;
- an operation that failed or was cut off may be visible or not, but never
  half-applied. It may also appear later, because Raft can still commit
  its log entry after a restart, but never after a later acknowledged
  write to the same key;
- all nodes must agree, and listings must show exactly the keys that exist.

In a cluster, writers keep writing to the surviving nodes while one is
down. With `--chaos`, a third of the rounds kill two of the three nodes
(losing the majority), and a third crash a node again while it recovers.
`--sabotage` deletes a chunk file after each crash, to show that the test
does catch lost data.

| Setup | Crashes | Acknowledged operations | Errors |
|---|---|---|---|
| Single node, default | 50 | 50,625 | **0** |
| Single node, `--sync file` | 30 | 23,932 | **0** |
| Single node, encrypted | 30 | 22,601 | **0** |
| 3-node cluster, 2 replicas | 30 | 20,444 | **0** |
| 3-node cluster, erasure coding 2+1 | 30 | 16,859 | **0** |
| 3-node cluster, encrypted | 20 | 9,382 | **0** |
| 3-node cluster, `--chaos` (9 majority losses, 5 crashes during recovery) | 30 | 15,709 | **0** |
| Single node, `--sabotage` (one chunk file deleted per crash) | 3 | 3,366 | 6 (expected: reads of the deleted chunks fail) |

### Power cuts (`crashtest --faultfs`)

`SIGKILL` stops the process, but the kernel survives and still writes out
what the process had not synced. To simulate losing power, `kosha-faultfs`
(in this repository) is a FUSE filesystem that keeps everything a program
has not made durable in memory and throws it away on a simulated power
cut:

- **File data** is durable only after `fsync`/`fdatasync` of that file.
- **Directory changes** (create, mkdir, rename, unlink, rmdir) are undone
  unless they were made durable. With `--fs-model posix` (strict POSIX) a
  change is durable only once its directory has been fsynced. With
  `--fs-model journal` any later fsync makes it durable, approximating the
  metadata journal of ext4 and XFS.

```sh
sudo target/release/examples/crashtest --kosha target/release/kosha \
  --faultfs target/release/kosha-faultfs --cluster --power-all --rounds 30 -- --sync file
```

Every node's data directory is a separate faultfs mount, and every crash
is a `SIGKILL` followed by a power cut of that node's disk. `--power-all`
cuts power to all nodes at once. Each cut is reported (for example: dropped about 68 KiB of unsynced data and
undid 1,305 directory changes), which shows that it is not vacuous. The tests use `--sync file`, because the kernel
does not pass `syncfs` (which the default `--sync group` relies on) to
FUSE filesystems. So **`--sync group` is not covered** by these power-cut
tests.

| Setup | Power cuts | Acknowledged operations | Errors |
|---|---|---|---|
| Single node, strict POSIX | 30 | 7,177 | **0** |
| Single node, strict POSIX, nothing synced before the first cut | 20 | 4,610 | **0** |
| Single node, journal model | 30 | 7,859 | **0** |
| Single node, strict POSIX, encrypted | 20 | 4,389 | **0** |
| 3-node cluster, one node per cut, strict POSIX | 20 | 5,663 | **0** |
| 3-node cluster, one node per cut, journal model | 20 | 5,790 | **0** |
| 3-node cluster, all nodes at once, strict POSIX | 30 | 6,563 | **0** |
| 3-node cluster, all nodes at once, journal model | 30 | 5,948 | **0** |
| 3-node cluster, all nodes at once, journal model, a Raft snapshot every 40 entries | 30 | 4,761 | **0** |
| 3-node cluster, erasure coding 2+1, strict POSIX | 15 | 3,658 | **0** |

The power-cut tests found three bugs, all fixed in this commit:

1. **New directories were not durable** (`--sync file`). Kosha fsynced
   each chunk file and the directory it is in, but not the parents of
   directories it had just created (`chunks/ab/`, `chunks/ab/cd/`) or the
   data directory itself. On a strictly POSIX filesystem a power cut could
   remove the new directory and every chunk inside it. The previous
   release failed the strict POSIX test with 3,394 errors in 10 power cuts;
   it passed the journal model, so on ext4 and XFS this was most likely
   harmless in practice.
2. **The Raft snapshot was written without `fsync`.** Under the journal
   model, a power cut after a snapshot left an empty `raft-snapshot.bin`,
   and the node then refused to start ("failed to fill whole buffer").
3. **Reads could go back in time after a whole-cluster power cut.** openraft
   restores a node that was leader as leader again, in the same term,
   without a new entry. Its commit index can then be behind what a
   follower has already applied, so a read on the leader could return an
   older value than a read a moment earlier on the follower. Kosha's lease
   check ("the commit index belongs to my term") does not detect this. A
   leader now commits an entry of its own before serving its first read.
   This was seen once in about 100 all-node power cuts before the fix and
   not in the 176 since, so the evidence for this fix is the Raft argument
   more than the test. It can only expose writes the leader had not yet
   acknowledged, since a leader acknowledges after applying and restarts
   with at least its applied index.

Two other failures during development were bugs in the test tools, not in
Kosha: the first version of faultfs ignored file timestamp updates (which
Kosha's garbage collection grace period relies on), and the first version
of `crashtest` assumed a failed write could never appear later.

### Network partitions and clock drift (`partitiontest`)

```sh
sudo target/release/examples/partitiontest --kosha target/release/kosha --seconds 300
sudo target/release/examples/partitiontest --kosha target/release/kosha --seconds 120 --isolate-leader
```

Each node runs as its own user id, and links between nodes are cut with
`iptables` rules that match the sending process (`-m owner`), so clients
can still reach every node. A nemesis keeps changing the network: one node
isolated, a "bridge" (A–B and B–C up, A–C cut), a one-way cut, or healed;
`--isolate-leader` always isolates the current leader. Writers PUT and
DELETE unique values, readers GET from random nodes, and every operation is
recorded with its start and end time. The checker then verifies, per key,
that every read returns a value that was written, that no read returns a
value older than a write acknowledged before the read began, and that
reads never go back in time. Writes that failed or timed out may take
effect at any later point. The checker has unit tests for each of these
cases (`cargo test --example partitiontest -p kosha-s3`).

| Run | Faults | Writes | Reads | Errors |
|---|---|---|---|---|
| Random faults, 5 minutes | 31 | 18,228 | 116,982 | **0** |
| Isolate the leader, 2 minutes | 16 | 4,852 | 24,980 | **0** |

It also found a startup bug: a cluster node panicked when it could not read
the system's CA certificates, even with plain-HTTP cluster traffic (the
HTTP client loaded them anyway). Fixed in this commit.

**Clock drift.** Leader leases let the leader answer reads without a
quorum round-trip. They rely on clocks running at similar *rates*
(absolute time does not matter). The leader holds a lease for 0.8 ×
`election_timeout_min` of its own time, while openraft voters refuse to
elect anyone else for `election_timeout_max` of theirs. With the defaults
(1–2 s) that is safe while the leader's clock runs at least 0.4× as fast as
its followers'. `--clock-rate N=R` runs a node under libfaketime with its
clocks at R× real speed. Node 1, the first leader, was isolated with a slow
clock:

| Leader clock rate | Runs | Errors |
|---|---|---|
| 0.5× (inside the bound) | 4 | **0** |
| 0.25× (outside the bound) | 4 | 0: the stale-read window was too short to hit |
| 0.1× (far outside) | 3 | **359 stale reads in 1 run**, as expected |
| 0.1× with `--no-lease-reads` | 4 | **0** |

So the checker does catch real stale reads, and leases are safe well beyond
normal hardware clock drift (parts per million). A long process pause on
the leader (a VM freeze, for example) has the same effect as a slow clock.
Use `--no-lease-reads` where that is a concern.

**What these tests do not cover.** Packet delay, reordering and
duplication: `tc` is not available on the test VM, so cuts are drops only.
`--sync group` under power cuts (see above). Torn writes inside a sector,
and disks that lie about `fsync`. Everything runs on one machine.

Short versions of the crash, power-cut and partition tests run in CI on every push.
