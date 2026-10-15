//! Crash-consistency test: SIGKILL Kosha under load and check that every
//! acknowledged write survives, byte for byte.
//!
//! ```sh
//! cargo build --release --bins --examples
//! target/release/examples/crashtest --kosha target/release/kosha --rounds 50
//! target/release/examples/crashtest --kosha target/release/kosha --cluster --rounds 50
//! ```
//!
//! Each writer owns a set of keys and changes them one operation at a time
//! (PUT of various sizes, multipart upload, DELETE, server-side COPY), so the
//! expected state of every key is known. Each round runs the writers, kills a
//! server (the only one, or a random cluster node) in the middle of the
//! traffic, restarts it and then checks every key on every node:
//!
//! - an acknowledged operation must be visible;
//! - an operation that failed or was cut off may be visible or not, but
//!   never partially (the body must match one of the two states exactly);
//! - listings must agree with the keys that exist;
//! - on a second check, only failed writes may still appear (Raft can
//!   commit a cut-off entry after a restart); they are counted, and an
//!   acknowledged write must never be undone by one.
//!
//! Extra arguments after `--` are passed to every Kosha process, e.g.
//! `-- --sync file` or `-- --erasure 2+1`. With `--chaos` (cluster only),
//! some rounds kill two of the three nodes (losing the majority) and some
//! kill a node again while it is recovering.
//!
//! `--faultfs <kosha-faultfs binary>` puts every node's data directory on
//! a `kosha-faultfs` mount and turns each crash into a **power cut**: data
//! the node did not fsync is lost and undurable directory changes are
//! undone (`--fs-model posix|journal` picks the rules). Use it with
//! `-- --sync file`: the kernel does not pass `syncfs` (used by the default
//! `--sync group`) to FUSE filesystems. `--power-all` cuts power to every
//! node at once. The setup (bucket creation) is made durable before the
//! first round unless `--no-settle` is given.
//!
//! `--sabotage` deletes a random
//! chunk file after every crash, to check that the test catches damage.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;

const AK: &str = "crashadmin";
const SK: &str = "crash-test-secret-key";
const BUCKET: &str = "crash";
const PART: usize = 5 * 1024 * 1024;

struct Args {
    kosha: PathBuf,
    dir: PathBuf,
    cluster: bool,
    rounds: usize,
    writers: usize,
    keys: usize,
    seed: u64,
    sabotage: bool,
    chaos: bool,
    faultfs: Option<PathBuf>,
    fs_model: String,
    power_all: bool,
    settle: bool,
    stop_on_error: bool,
    extra: Vec<String>,
}

fn args() -> Args {
    let mut a = Args {
        kosha: "target/release/kosha".into(),
        dir: std::env::temp_dir().join("kosha-crashtest"),
        cluster: false,
        rounds: 20,
        writers: 12,
        keys: 16,
        seed: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
        sabotage: false,
        chaos: false,
        faultfs: None,
        fs_model: "posix".into(),
        power_all: false,
        settle: true,
        stop_on_error: false,
        extra: vec![],
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--kosha" => a.kosha = val().into(),
            "--dir" => a.dir = val().into(),
            "--cluster" => a.cluster = true,
            "--sabotage" => a.sabotage = true,
            "--chaos" => a.chaos = true,
            "--faultfs" => a.faultfs = Some(val().into()),
            "--fs-model" => a.fs_model = val(),
            "--power-all" => a.power_all = true,
            "--no-settle" => a.settle = false,
            "--stop-on-error" => a.stop_on_error = true,
            "--rounds" => a.rounds = val().parse().expect("rounds"),
            "--writers" => a.writers = val().parse().expect("writers"),
            "--keys" => a.keys = val().parse().expect("keys"),
            "--seed" => a.seed = val().parse().expect("seed"),
            "--" => a.extra = it.by_ref().collect(),
            other => panic!("unknown flag {other}"),
        }
    }
    if a.dir.is_relative() {
        a.dir = std::env::current_dir().unwrap().join(&a.dir);
    }
    a
}

/// Small deterministic PRNG (xorshift64*).
#[derive(Clone)]
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// What a key holds: nothing, or the content identified by `seed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Absent,
    Present(u64),
}

/// Object sizes cover inline objects, chunk boundaries and multipart.
fn size_of(seed: u64) -> usize {
    const SIZES: [usize; 10] = [
        0,
        1,
        700,
        8 * 1024 - 1,
        8 * 1024 + 1,
        65_536 + 3,
        300_000,
        1_048_576,
        2 * 1_048_576 + 17,
        12 * 1_048_576 + 5,
    ];
    SIZES[(seed % SIZES.len() as u64) as usize]
}

/// Content for `seed`. A run of repeated blocks makes some chunks shared
/// between objects, so deduplicated chunks are exercised too.
fn body(seed: u64) -> Bytes {
    let size = size_of(seed);
    let mut out = Vec::with_capacity(size);
    let mut rng = Rng::new(seed);
    let shared = Rng::new(seed % 7).next().to_le_bytes();
    while out.len() < size {
        if rng.below(4) == 0 {
            for _ in 0..1024 {
                out.extend_from_slice(&shared);
            }
        } else {
            for _ in 0..1024 {
                out.extend_from_slice(&rng.next().to_le_bytes());
            }
        }
    }
    out.truncate(size);
    Bytes::from(out)
}

struct Key {
    name: String,
    acked: State,
    /// An operation that failed or was cut off: it may or may not have
    /// been applied. The key is not touched again until it is resolved.
    pending: Option<State>,
    /// Failed writes that were not visible when checked but may still be
    /// applied later: Raft can commit a cut-off entry after a restart (a
    /// leader that restarts keeps its term and its log). They can no longer
    /// appear once a later write to the key is acknowledged.
    maybe: Vec<State>,
    /// Every value written to the key (seed, acknowledged), for reports.
    past: Vec<(u64, bool)>,
}

struct Worker {
    id: usize,
    rng: Rng,
    keys: Vec<Key>,
    acked: u64,
    failed: u64,
    /// A few failure messages from the current round, for the report.
    samples: Vec<String>,
}

fn client(port: u16) -> Client {
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(Credentials::new(AK, SK, None, None, "crash"))
        .force_path_style(true)
        // No retries: every attempt's outcome is tracked here.
        .retry_config(RetryConfig::disabled())
        .timeout_config(
            timeout::TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(60))
                .connect_timeout(Duration::from_secs(2))
                .build(),
        )
        .build();
    Client::from_conf(conf)
}

struct Node {
    id: u64,
    s3: u16,
    child: Option<Child>,
    /// The node's kosha-faultfs mount, with `--faultfs`.
    fs: Option<Child>,
}

struct Cluster {
    a: Arc<Args>,
    nodes: Vec<Node>,
    starts: u64,
}

impl Cluster {
    fn new(a: Arc<Args>) -> Cluster {
        let n = if a.cluster { 3 } else { 1 };
        let nodes = (1..=n)
            .map(|id| Node {
                id,
                s3: 9400 + id as u16,
                child: None,
                fs: None,
            })
            .collect();
        Cluster {
            a,
            nodes,
            starts: 0,
        }
    }

    fn start(&mut self, i: usize) {
        let a = self.a.clone();
        let node = &mut self.nodes[i];
        let data = a.dir.join(format!("node{}", node.id));
        std::fs::create_dir_all(&data).unwrap();
        if let Some(bin) = &a.faultfs
            && node.fs.is_none()
        {
            let disk = a.dir.join(format!("node{}.disk", node.id));
            std::fs::create_dir_all(&disk).unwrap();
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(a.dir.join(format!("node{}.fs.log", node.id)))
                .unwrap();
            let fs = Command::new(bin)
                .arg(&disk)
                .arg(&data)
                .arg("--model")
                .arg(&a.fs_model)
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .spawn()
                .expect("start kosha-faultfs");
            node.fs = Some(fs);
            let deadline = Instant::now() + Duration::from_secs(10);
            while !mounted(&data) {
                assert!(
                    Instant::now() < deadline,
                    "faultfs did not mount {}",
                    data.display()
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(a.dir.join(format!("node{}.log", node.id)))
            .unwrap();
        let mut cmd = Command::new(&a.kosha);
        cmd.arg("--data-dir")
            .arg(&data)
            .arg("--listen")
            .arg(format!("127.0.0.1:{}", node.s3))
            .env("KOSHA_ACCESS_KEY", AK)
            .env("KOSHA_SECRET_KEY", SK)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log));
        if a.cluster {
            cmd.arg("--node-id")
                .arg(node.id.to_string())
                .arg("--cluster-listen")
                .arg(format!("127.0.0.1:{}", 9500 + node.id))
                .env("KOSHA_CLUSTER_SECRET", "crash-test-cluster-secret")
                .env("KOSHA_GC_GRACE_SECS", "5");
            // Nodes with cluster state ignore these on restart.
            if node.id == 1 {
                cmd.arg("--init");
            } else {
                cmd.arg("--join").arg("127.0.0.1:9501");
            }
        }
        cmd.args(&a.extra);
        node.child = Some(cmd.spawn().expect("start kosha"));
        self.starts += 1;
    }

    /// Crash a node: SIGKILL it and, with `--faultfs`, cut its power.
    fn kill(&mut self, i: usize) {
        if let Some(mut c) = self.nodes[i].child.take() {
            let _ = c.kill(); // SIGKILL
            let _ = c.wait();
        }
        if let Some(mut fs) = self.nodes[i].fs.take() {
            signal(&fs, libc::SIGUSR1);
            let _ = fs.wait();
            let data = self.a.dir.join(format!("node{}", self.nodes[i].id));
            while mounted(&data) {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }

    /// Make everything on every node's disk durable (like time passing).
    fn settle(&self) {
        for n in &self.nodes {
            if let Some(fs) = &n.fs {
                signal(fs, libc::SIGUSR2);
            }
        }
        std::thread::sleep(Duration::from_millis(300));
    }

    /// The last power-cut report of a node's faultfs.
    fn power_report(&self, i: usize) -> Option<String> {
        let log =
            std::fs::read_to_string(self.a.dir.join(format!("node{}.fs.log", self.nodes[i].id)))
                .ok()?;
        log.lines()
            .rev()
            .find(|l| l.contains("power cut:"))
            .map(|l| {
                l.trim_start_matches("kosha-faultfs: power cut: ")
                    .to_string()
            })
    }

    fn ports(&self) -> Vec<u16> {
        self.nodes.iter().map(|n| n.s3).collect()
    }
}

impl Drop for Cluster {
    /// Never leave servers (or mounts) behind, even when the test panics.
    fn drop(&mut self) {
        for n in &mut self.nodes {
            if let Some(mut c) = n.child.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
            if let Some(mut fs) = n.fs.take() {
                signal(&fs, libc::SIGTERM);
                let _ = fs.wait();
            }
        }
    }
}

fn signal(c: &Child, sig: i32) {
    unsafe { libc::kill(c.id() as i32, sig) };
}

fn mounted(path: &std::path::Path) -> bool {
    std::fs::read_to_string("/proc/mounts")
        .map(|m| {
            m.lines()
                .any(|l| l.split(' ').nth(1) == Some(path.to_str().unwrap_or("")))
        })
        .unwrap_or(false)
}

/// Waits until every node answers S3 requests that need the cluster
/// (a bucket listing is a linearizable read).
async fn wait_ready(ports: &[u16]) {
    let deadline = Instant::now() + Duration::from_secs(60);
    for &p in ports {
        let c = client(p);
        loop {
            if c.list_buckets().send().await.is_ok() {
                break;
            }
            assert!(Instant::now() < deadline, "node on :{p} did not come up");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

async fn read(c: &Client, key: &str) -> Result<Option<Bytes>, String> {
    match c.get_object().bucket(BUCKET).key(key).send().await {
        Ok(out) => Ok(Some(
            out.body
                .collect()
                .await
                .map_err(|e| e.to_string())?
                .into_bytes(),
        )),
        Err(e) => {
            if e.as_service_error().map(|s| s.is_no_such_key()) == Some(true) {
                Ok(None)
            } else {
                Err(format!("{e:?}"))
            }
        }
    }
}

/// Which of `candidates` the body matches.
fn classify(got: &Option<Bytes>, candidates: &[State]) -> Option<State> {
    candidates.iter().copied().find(|s| match (s, got) {
        (State::Absent, None) => true,
        (State::Present(seed), Some(b)) => *b == body(*seed),
        _ => false,
    })
}

async fn put(c: &Client, key: &str, seed: u64) -> Result<(), String> {
    let data = body(seed);
    if data.len() < 2 * PART {
        return c
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map(drop)
            .map_err(|e| format!("{e:?}"));
    }
    let up = c
        .create_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .map_err(|e| format!("{e:?}"))?;
    let id = up.upload_id().unwrap_or_default().to_string();
    let mut parts = vec![];
    for (i, piece) in data.chunks(PART).enumerate() {
        let n = i as i32 + 1;
        let r = c
            .upload_part()
            .bucket(BUCKET)
            .key(key)
            .upload_id(&id)
            .part_number(n)
            .body(ByteStream::from(Bytes::copy_from_slice(piece)))
            .send()
            .await
            .map_err(|e| format!("{e:?}"))?;
        parts.push(
            CompletedPart::builder()
                .part_number(n)
                .e_tag(r.e_tag().unwrap_or_default())
                .build(),
        );
    }
    c.complete_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .upload_id(&id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .map(drop)
        .map_err(|e| format!("{e:?}"))
}

async fn run_worker(
    mut w: Worker,
    ports: Arc<Vec<u16>>,
    stop: Arc<AtomicBool>,
    seq: Arc<AtomicU64>,
) -> Worker {
    let clients: Vec<Client> = ports.iter().map(|&p| client(p)).collect();
    while !stop.load(Ordering::Relaxed) {
        let ready: Vec<usize> = (0..w.keys.len())
            .filter(|&i| w.keys[i].pending.is_none())
            .collect();
        if ready.is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let k = ready[w.rng.below(ready.len() as u64) as usize];
        let c = &clients[w.rng.below(clients.len() as u64) as usize];
        let name = w.keys[k].name.clone();
        let roll = w.rng.below(10);
        let (next, result) = if roll == 0 {
            (
                State::Absent,
                c.delete_object()
                    .bucket(BUCKET)
                    .key(&name)
                    .send()
                    .await
                    .map(drop)
                    .map_err(|e| format!("{e:?}")),
            )
        } else if roll == 1 {
            // Copy from another of this worker's keys with a known state.
            let src = ready[w.rng.below(ready.len() as u64) as usize];
            match w.keys[src].acked {
                State::Present(seed) if src != k => {
                    let from = format!("{BUCKET}/{}", w.keys[src].name);
                    (
                        State::Present(seed),
                        c.copy_object()
                            .bucket(BUCKET)
                            .key(&name)
                            .copy_source(from)
                            .send()
                            .await
                            .map(drop)
                            .map_err(|e| format!("{e:?}")),
                    )
                }
                _ => continue,
            }
        } else {
            let mut seed = (w.id as u64) << 48 | seq.fetch_add(1, Ordering::Relaxed);
            // Mostly small objects, so rounds stay quick.
            if size_of(seed) >= 2 * PART && w.rng.below(4) != 0 {
                seed += 1;
            }
            (State::Present(seed), put(c, &name, seed).await)
        };
        if let State::Present(seed) = next {
            w.keys[k].past.push((seed, result.is_ok()));
        }
        match result {
            Ok(()) => {
                w.keys[k].acked = next;
                w.keys[k].maybe.clear();
                w.acked += 1;
            }
            Err(e) => {
                if w.samples.len() < 2 {
                    w.samples.push(short(&e));
                }
                if next != w.keys[k].acked {
                    w.keys[k].pending = Some(next);
                }
                w.failed += 1;
            }
        }
    }
    w
}

/// Checks every key on every node; resolves pending operations.
/// Checks every key on every node; resolves pending operations. Returns the
/// violations and how many earlier failed writes turned up late.
async fn verify(workers: &mut [Worker], ports: &[u16], round: usize) -> (Vec<String>, usize) {
    let clients: Vec<Client> = ports.iter().map(|&p| client(p)).collect();
    let mut errors = vec![];
    let mut late = 0;
    for w in workers.iter_mut() {
        for key in w.keys.iter_mut() {
            let mut candidates = vec![key.acked];
            candidates.extend(key.pending);
            candidates.extend(key.maybe.iter().copied());
            let mut seen = None;
            for (n, c) in clients.iter().enumerate() {
                let got = match read(c, &key.name).await {
                    Ok(g) => g,
                    Err(e) => {
                        errors.push(format!(
                            "round {round}: {} on node {}: {e}",
                            key.name,
                            n + 1
                        ));
                        continue;
                    }
                };
                match classify(&got, &candidates) {
                    None => errors.push(format!(
                        "round {round}: {} on node {}: expected {:?}, got {}",
                        key.name,
                        n + 1,
                        candidates,
                        match &got {
                            None => "no object".to_string(),
                            Some(b) =>
                                match key.past.iter().rposition(|(seed, _)| *b == body(*seed)) {
                                    Some(i) => format!(
                                        "an older value: write {} of {} to this key ({})",
                                        i + 1,
                                        key.past.len(),
                                        if key.past[i].1 {
                                            "acknowledged"
                                        } else {
                                            "not acknowledged"
                                        }
                                    ),
                                    None => format!("{} unexpected bytes", b.len()),
                                },
                        }
                    )),
                    Some(s) => {
                        // Nodes are read one after another, so a failed
                        // write landing late may show up part-way through
                        // (old value first, then the new one, never back).
                        let landed = |v: State| key.pending == Some(v) || key.maybe.contains(&v);
                        if let Some(prev) = seen
                            && prev != s
                            && !(prev == key.acked && landed(s))
                        {
                            errors.push(format!(
                                "round {round}: {}: nodes disagree ({prev:?} vs {s:?})",
                                key.name
                            ));
                        }
                        seen = Some(s);
                    }
                }
            }
            if let Some(s) = seen {
                if s != key.acked && key.pending != Some(s) && key.maybe.contains(&s) {
                    late += 1;
                    key.maybe.clear();
                }
                if let Some(p) = key.pending.take()
                    && p != s
                {
                    key.maybe.push(p);
                }
                key.acked = s;
            }
        }
        // Listing must show exactly the keys that exist.
        let want: BTreeSet<String> = w
            .keys
            .iter()
            .filter(|k| k.acked != State::Absent)
            .map(|k| k.name.clone())
            .collect();
        for (n, c) in clients.iter().enumerate() {
            let mut got = BTreeSet::new();
            let mut token = None;
            loop {
                let r = c
                    .list_objects_v2()
                    .bucket(BUCKET)
                    .prefix(format!("w{:02}/", w.id))
                    .set_continuation_token(token.clone())
                    .send()
                    .await;
                match r {
                    Ok(r) => {
                        got.extend(
                            r.contents()
                                .iter()
                                .filter_map(|o| o.key().map(String::from)),
                        );
                        token = r.next_continuation_token().map(String::from);
                        if token.is_none() {
                            break;
                        }
                    }
                    Err(e) => {
                        errors.push(format!("round {round}: list on node {}: {e:?}", n + 1));
                        break;
                    }
                }
            }
            if got != want {
                errors.push(format!(
                    "round {round}: listing w{:02}/ on node {}: missing {:?}, extra {:?}",
                    w.id,
                    n + 1,
                    want.difference(&got).collect::<Vec<_>>(),
                    got.difference(&want).collect::<Vec<_>>()
                ));
            }
        }
    }
    (errors, late)
}

/// Print each node's Raft state (debugging aid, `CRASHTEST_DEBUG=1`).
fn raft_state(ports: &[u16], when: &str) {
    for &p in ports {
        let out = Command::new("curl")
            .args([
                "-s",
                "--aws-sigv4",
                "aws:amz:us-east-1:s3",
                "--user",
                &format!("{AK}:{SK}"),
            ])
            .arg(format!("http://127.0.0.1:{p}/_kosha/cluster"))
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
            .unwrap_or_default();
        let pick = |k: &str| {
            out.find(&format!("\"{k}\":"))
                .map(|i| {
                    out[i..]
                        .chars()
                        .take_while(|&c| c != ',' && c != '}')
                        .collect::<String>()
                })
                .unwrap_or_default()
        };
        println!(
            "  [{when}] :{p} {} {} {} {} {}",
            pick("role"),
            pick("term"),
            pick("leader"),
            pick("last_log_index"),
            pick("last_applied")
        );
    }
}

/// The interesting part of an SDK error.
fn short(e: &str) -> String {
    for marker in ["message: Some(\"", "code: Some(\"", "source: "] {
        if let Some(i) = e.find(marker) {
            return e[i..].chars().take(140).collect();
        }
    }
    e.chars().take(140).collect()
}

/// Chunk files under a node's data directory (`chunks/ab/cd/<hex>`).
fn chunk_files(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut out = vec![];
    let mut stack = vec![dir.join("chunks")];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                if e.file_name() != "tmp" {
                    stack.push(p);
                }
            } else if e.file_name().len() == 64 {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

#[tokio::main]
async fn main() {
    let a = Arc::new(args());
    let _ = std::fs::remove_dir_all(&a.dir);
    std::fs::create_dir_all(&a.dir).unwrap();
    println!(
        "crashtest: {} node(s), {} rounds, {} writers x {} keys, seed {}, extra args {:?}",
        if a.cluster { 3 } else { 1 },
        a.rounds,
        a.writers,
        a.keys,
        a.seed,
        a.extra
    );
    let mut cluster = Cluster::new(a.clone());
    for i in 0..cluster.nodes.len() {
        cluster.start(i);
        if i == 0 && a.cluster {
            wait_ready(&cluster.ports()[..1]).await;
        }
    }
    let ports = Arc::new(cluster.ports());
    wait_ready(&ports).await;
    let admin = client(ports[0]);
    admin
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("create bucket");
    if a.settle {
        cluster.settle();
    }

    let mut rng = Rng::new(a.seed);
    let seq = Arc::new(AtomicU64::new(1));
    let mut workers: Vec<Worker> = (0..a.writers)
        .map(|id| Worker {
            id,
            rng: Rng::new(a.seed ^ (id as u64 + 1).wrapping_mul(0x5851F42D4C957F2D)),
            keys: (0..a.keys)
                .map(|k| Key {
                    name: format!("w{id:02}/k{k:03}"),
                    acked: State::Absent,
                    pending: None,
                    maybe: vec![],
                    past: vec![],
                })
                .collect(),
            acked: 0,
            failed: 0,
            samples: vec![],
        })
        .collect();
    let mut all_errors = vec![];
    let mut late_total = 0;
    let started = Instant::now();
    for round in 1..=a.rounds {
        let stop = Arc::new(AtomicBool::new(false));
        let tasks: Vec<_> = workers
            .drain(..)
            .map(|w| tokio::spawn(run_worker(w, ports.clone(), stop.clone(), seq.clone())))
            .collect();
        // Kill somewhere between 0.2 and 3 s into the traffic.
        tokio::time::sleep(Duration::from_millis(200 + rng.below(2800))).await;
        let n = cluster.nodes.len();
        let first = rng.below(n as u64) as usize;
        let mut victims = vec![first];
        // Chaos: a third of the rounds lose the majority.
        if a.chaos && n > 1 && rng.below(3) == 0 {
            victims.push((first + 1 + rng.below(n as u64 - 1) as usize) % n);
        }
        if a.power_all {
            victims = (0..n).collect();
        }
        for (i, &v) in victims.iter().enumerate() {
            if i > 0 {
                tokio::time::sleep(Duration::from_millis(rng.below(500))).await;
            }
            cluster.kill(v);
        }
        // In a cluster, keep writing to the survivors for a while.
        if a.cluster {
            tokio::time::sleep(Duration::from_millis(500 + rng.below(1500))).await;
        }
        stop.store(true, Ordering::Relaxed);
        let drain = Instant::now();
        for t in tasks {
            workers.push(t.await.unwrap());
        }
        let drained = drain.elapsed();
        if a.sabotage {
            let dir = a.dir.join(format!("node{}", cluster.nodes[first].id));
            let files = chunk_files(&dir);
            if !files.is_empty() {
                let f = &files[rng.below(files.len() as u64) as usize];
                println!("  sabotage: deleting {}", f.display());
                let _ = std::fs::remove_file(f);
            }
        }
        let restart = Instant::now();
        for &v in &victims {
            cluster.start(v);
        }
        // Chaos: a third of the rounds crash a node again during recovery.
        let mut recovery_kill = None;
        if a.chaos && rng.below(3) == 0 {
            tokio::time::sleep(Duration::from_millis(100 + rng.below(1500))).await;
            let v = rng.below(n as u64) as usize;
            cluster.kill(v);
            cluster.start(v);
            recovery_kill = Some(cluster.nodes[v].id);
        }
        wait_ready(&ports).await;
        let recovered = restart.elapsed();
        let victim_ids: Vec<u64> = victims.iter().map(|&v| cluster.nodes[v].id).collect();
        let (mut errors, mut late) = verify(&mut workers, &ports, round).await;
        let debug = std::env::var_os("CRASHTEST_DEBUG").is_some();
        if debug {
            raft_state(&ports, "after first check");
        }
        // Check again: only failed writes may still change what is seen.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        if debug {
            raft_state(&ports, "before second check");
        }
        // The second check accepts only the same values, or failed writes
        // landing late; an acknowledged write never disappears.
        let (e2, l2) = verify(&mut workers, &ports, round).await;
        errors.extend(e2);
        late += l2;
        late_total += late;
        let acked: u64 = workers.iter().map(|w| w.acked).sum();
        let failed: u64 = workers.iter().map(|w| w.failed).sum();
        let present = workers
            .iter()
            .flat_map(|w| &w.keys)
            .filter(|k| k.acked != State::Absent)
            .count();
        println!(
            "round {round:>3}: killed node {}{}; in-flight requests ended in {:.1} s, all nodes ready {:.1} s after restart; totals: {acked} acknowledged ops, {failed} failed or cut off; {present} objects; {late} failed writes landed late; {} errors",
            victim_ids
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join("+"),
            recovery_kill
                .map(|i| format!(", then node {i} during recovery"))
                .unwrap_or_default(),
            drained.as_secs_f64(),
            recovered.as_secs_f64(),
            errors.len()
        );
        if a.faultfs.is_some() {
            for &v in &victims {
                if let Some(r) = cluster.power_report(v) {
                    println!("  node {} power cut: {r}", cluster.nodes[v].id);
                }
            }
        }
        for e in errors.iter().take(10) {
            println!("  {e}");
        }
        if std::env::var_os("CRASHTEST_VERBOSE").is_some() {
            let mut kinds = std::collections::BTreeMap::new();
            for w in workers.iter_mut() {
                for s in w.samples.drain(..) {
                    *kinds.entry(s).or_insert(0) += 1;
                }
            }
            for (k, n) in kinds {
                println!("  cut off ({n}x): {k}");
            }
        } else {
            workers.iter_mut().for_each(|w| w.samples.clear());
        }
        let failed_round = !errors.is_empty();
        all_errors.extend(errors);
        if failed_round && a.stop_on_error {
            println!(
                "stopping at the first failed round; data kept in {}",
                a.dir.display()
            );
            break;
        }
    }
    for i in 0..cluster.nodes.len() {
        cluster.kill(i);
    }
    let acked: u64 = workers.iter().map(|w| w.acked).sum();
    println!(
        "done in {:.0} s: {} crashes, {acked} acknowledged operations, {late_total} failed writes landed late, {} errors",
        started.elapsed().as_secs_f64(),
        a.rounds,
        all_errors.len()
    );
    if !all_errors.is_empty() {
        std::process::exit(1);
    }
}
