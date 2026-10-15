//! Network-partition test: cut links between cluster nodes under load and
//! check that every read and write stays linearizable.
//!
//! ```sh
//! cargo build --release --bins --examples
//! sudo target/release/examples/partitiontest --kosha target/release/kosha --seconds 120
//! ```
//!
//! Needs root: each node runs as its own user id, and links are cut with
//! iptables rules that match the sending process's uid (`-m owner`), in a
//! chain of their own (`KOSHA_PARTITION`) that is removed afterwards.
//!
//! Writers each own some keys and PUT or DELETE them one at a time with
//! unique values; readers GET random keys from random nodes. Every
//! operation is recorded with its start and end time. Meanwhile a nemesis
//! keeps changing the network: one node isolated, a "bridge" (A-B and B-C
//! up, A-C cut), a one-way cut, or everything healed. At the end the
//! network is healed and every key is read from every node.
//!
//! The history is then checked, per key:
//!
//! - every read returns a value some write actually wrote (or no object);
//! - no read returns a value older than one whose write had completed
//!   before the read began (no stale reads, no lost acknowledged writes);
//! - no read returns an older value than a read that completed before it
//!   began (reads never go back in time).
//!
//! A write that failed or timed out may have been applied at any later
//! moment, so it never counts against a read.
//!
//! `--isolate-leader` makes every fault isolate whichever node currently
//! leads (the case leader leases must get right).
//!
//! `--clock-rate N=R` runs node N with its clocks running at R times real
//! speed, through libfaketime (`--faketime <libfaketime.so.1>`), to check
//! how leader leases behave with clock drift.

use std::collections::HashMap;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout};
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use parking_lot::Mutex;

const AK: &str = "partadmin";
const SK: &str = "partition-test-secret-key";
const BUCKET: &str = "partition";
const CHAIN: &str = "KOSHA_PARTITION";
const NODES: usize = 3;

struct Args {
    kosha: PathBuf,
    dir: PathBuf,
    seconds: u64,
    writers: usize,
    keys_per_writer: usize,
    readers: usize,
    seed: u64,
    faketime: Option<PathBuf>,
    clock_rates: Vec<(usize, f64)>,
    /// Client timeout per operation (a timed-out write counts as failed).
    op_timeout: Duration,
    isolate_leader: bool,
    extra: Vec<String>,
}

fn args() -> Args {
    let mut a = Args {
        kosha: "target/release/kosha".into(),
        dir: std::env::temp_dir().join("kosha-partitiontest"),
        seconds: 120,
        writers: 6,
        keys_per_writer: 5,
        readers: 8,
        seed: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
        faketime: None,
        clock_rates: vec![],
        op_timeout: Duration::from_secs(5),
        isolate_leader: false,
        extra: vec![],
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--kosha" => a.kosha = val().into(),
            "--dir" => a.dir = val().into(),
            "--seconds" => a.seconds = val().parse().expect("seconds"),
            "--writers" => a.writers = val().parse().expect("writers"),
            "--keys" => a.keys_per_writer = val().parse().expect("keys"),
            "--readers" => a.readers = val().parse().expect("readers"),
            "--seed" => a.seed = val().parse().expect("seed"),
            "--faketime" => a.faketime = Some(val().into()),
            "--isolate-leader" => a.isolate_leader = true,
            "--op-timeout" => {
                a.op_timeout = Duration::from_secs_f64(val().parse().expect("seconds"))
            }
            "--clock-rate" => {
                let v = val();
                let (n, r) = v.split_once('=').expect("--clock-rate N=RATE");
                a.clock_rates
                    .push((n.parse().expect("node"), r.parse().expect("rate")));
            }
            "--" => a.extra = it.by_ref().collect(),
            other => panic!("unknown flag {other}"),
        }
    }
    if a.dir.is_relative() {
        a.dir = std::env::current_dir().unwrap().join(&a.dir);
    }
    a
}

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

fn uid(node: usize) -> u32 {
    42001 + node as u32
}
fn s3_port(node: usize) -> u16 {
    9701 + node as u16
}
fn raft_port(node: usize) -> u16 {
    9801 + node as u16
}

// ------------------------------------------------------------------ network

fn iptables(args: &[&str]) -> bool {
    Command::new("iptables")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn net_setup() {
    net_teardown();
    assert!(iptables(&["-N", CHAIN]), "iptables -N failed (root needed)");
    assert!(iptables(&["-I", "OUTPUT", "-j", CHAIN]));
}

fn net_teardown() {
    while iptables(&["-D", "OUTPUT", "-j", CHAIN]) {}
    iptables(&["-F", CHAIN]);
    iptables(&["-X", CHAIN]);
}

fn heal() {
    iptables(&["-F", CHAIN]);
}

/// Drop everything node `from` sends to node `to` (new connections and
/// established ones: Kosha's peer traffic all goes to the raft port).
fn cut_one_way(from: usize, to: usize) {
    let u = uid(from).to_string();
    let p = raft_port(to).to_string();
    assert!(iptables(&[
        "-A",
        CHAIN,
        "-p",
        "tcp",
        "-m",
        "owner",
        "--uid-owner",
        &u,
        "--dport",
        &p,
        "-j",
        "DROP",
    ]));
}

fn cut(a: usize, b: usize) {
    cut_one_way(a, b);
    cut_one_way(b, a);
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    Healthy,
    Isolate(usize),
    Bridge { middle: usize },
    OneWay { from: usize, to: usize },
}

impl Fault {
    fn apply(self) {
        heal();
        match self {
            Fault::Healthy => {}
            Fault::Isolate(n) => {
                for o in (0..NODES).filter(|&o| o != n) {
                    cut(n, o);
                }
            }
            Fault::Bridge { middle } => {
                let ends: Vec<usize> = (0..NODES).filter(|&o| o != middle).collect();
                cut(ends[0], ends[1]);
            }
            Fault::OneWay { from, to } => cut_one_way(from, to),
        }
    }

    fn describe(self) -> String {
        match self {
            Fault::Healthy => "healthy".into(),
            Fault::Isolate(n) => format!("node {} isolated", n + 1),
            Fault::Bridge { middle } => format!("bridge through node {}", middle + 1),
            Fault::OneWay { from, to } => format!("node {} -> node {} dropped", from + 1, to + 1),
        }
    }
}

// ------------------------------------------------------------------ nodes

struct Nodes {
    a: Arc<Args>,
    children: Vec<Option<Child>>,
}

impl Nodes {
    fn start(&mut self, n: usize) {
        let a = &self.a;
        let data = a.dir.join(format!("node{}", n + 1));
        std::fs::create_dir_all(&data).unwrap();
        let _ = Command::new("chown")
            .arg("-R")
            .arg(format!("{0}:{0}", uid(n)))
            .arg(&data)
            .status();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(a.dir.join(format!("node{}.log", n + 1)))
            .unwrap();
        let mut cmd = Command::new(&a.kosha);
        cmd.arg("--node-id")
            .arg((n + 1).to_string())
            .arg("--data-dir")
            .arg(&data)
            .arg("--listen")
            .arg(format!("127.0.0.1:{}", s3_port(n)))
            .arg("--cluster-listen")
            .arg(format!("127.0.0.1:{}", raft_port(n)))
            .env("KOSHA_ACCESS_KEY", AK)
            .env("KOSHA_SECRET_KEY", SK)
            .env("KOSHA_CLUSTER_SECRET", "partition-test-cluster-secret")
            .uid(uid(n))
            .gid(uid(n))
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log));
        if n == 0 {
            cmd.arg("--init");
        } else {
            cmd.arg("--join").arg(format!("127.0.0.1:{}", raft_port(0)));
        }
        if let Some(&(_, rate)) = a.clock_rates.iter().find(|(i, _)| *i == n + 1) {
            let lib = a.faketime.as_ref().expect("--clock-rate needs --faketime");
            cmd.env("LD_PRELOAD", lib)
                .env("FAKETIME", format!("+0 x{rate}"))
                .env("DONT_FAKE_MONOTONIC", "0")
                .env("FAKETIME_DONT_RESET", "1");
        }
        cmd.args(&a.extra);
        self.children[n] = Some(cmd.spawn().expect("start kosha"));
    }
}

impl Drop for Nodes {
    fn drop(&mut self) {
        for c in self.children.iter_mut().flatten() {
            let _ = c.kill();
            let _ = c.wait();
        }
        net_teardown();
    }
}

fn client(port: u16, op_timeout: Duration) -> Client {
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(Credentials::new(AK, SK, None, None, "partition"))
        .force_path_style(true)
        .retry_config(RetryConfig::disabled())
        .timeout_config(
            timeout::TimeoutConfig::builder()
                .operation_timeout(op_timeout)
                .connect_timeout(Duration::from_secs(2))
                .build(),
        )
        .build();
    Client::from_conf(conf)
}

async fn wait_ready(ports: &[u16]) {
    let deadline = Instant::now() + Duration::from_secs(60);
    for &p in ports {
        let c = client(p, Duration::from_secs(5));
        while c.list_buckets().send().await.is_err() {
            assert!(Instant::now() < deadline, "node on :{p} did not come up");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

// ------------------------------------------------------------------ history

/// Value 0 is "no object" (the initial state, or a delete).
type Value = u64;

#[derive(Clone, Copy)]
struct Write {
    value: Value,
    start: f64,
    /// `None`: failed or timed out (may take effect at any later time).
    end: Option<f64>,
}

#[derive(Clone, Copy)]
struct Read {
    value: Value,
    start: f64,
    end: f64,
    node: usize,
}

#[derive(Default)]
struct KeyHistory {
    writes: Vec<Write>,
    reads: Vec<Read>,
}

struct History {
    t0: Instant,
    keys: Mutex<HashMap<String, KeyHistory>>,
}

impl History {
    fn now(&self) -> f64 {
        self.t0.elapsed().as_secs_f64()
    }
}

fn body(key: &str, value: Value) -> Bytes {
    // Mostly small (inline) bodies, some larger ones that become chunks.
    let n = if value.is_multiple_of(5) { 40_000 } else { 64 };
    let mut b = format!("{key}={value};").into_bytes();
    while b.len() < n {
        b.extend_from_slice(&value.to_le_bytes());
    }
    b.truncate(n);
    Bytes::from(b)
}

/// The value a body was written with, if it is intact.
fn parse(key: &str, b: &[u8]) -> Option<Value> {
    let end = b.iter().position(|&c| c == b';')?;
    let head = std::str::from_utf8(&b[..end]).ok()?;
    let v: Value = head.strip_prefix(key)?.strip_prefix('=')?.parse().ok()?;
    (body(key, v) == b).then_some(v)
}

/// Stands for a body that matches no write (corruption), so the checker
/// reports it.
const CORRUPT: Value = u64::MAX - 1;

/// Check one key's history. Returns descriptions of violations.
fn check(key: &str, h: &KeyHistory) -> Vec<String> {
    let mut errs = vec![];
    // Candidate writes for a value: deletes all write 0, plus the initial 0.
    let initial = Write {
        value: 0,
        start: f64::NEG_INFINITY,
        end: Some(f64::NEG_INFINITY),
    };
    let writes_of = |v: Value| -> Vec<Write> {
        let mut w: Vec<Write> = h.writes.iter().copied().filter(|w| w.value == v).collect();
        if v == 0 {
            w.push(initial);
        }
        w
    };
    // Acknowledged writes, for "a newer write had completed" checks.
    let acked: Vec<Write> = h
        .writes
        .iter()
        .copied()
        .filter(|w| w.end.is_some())
        .collect();
    // A read is explained by candidate write `c` if `c` started before the
    // read ended and no acknowledged write strictly after `c` (started
    // after `c` ended) completed before the read started.
    let explains = |c: &Write, r: &Read| -> bool {
        if c.start >= r.end {
            return false;
        }
        let c_end = c.end.unwrap_or(f64::INFINITY);
        !acked
            .iter()
            .any(|w| w.end.unwrap() < r.start && w.start > c_end)
    };
    // The newest write a read can be attributed to (for monotonicity).
    let mut attributed: Vec<(Read, f64)> = vec![];
    for r in &h.reads {
        let cands = writes_of(r.value);
        if cands.is_empty() {
            errs.push(if r.value == CORRUPT {
                format!("{key}: node {} returned a corrupt body", r.node + 1)
            } else {
                format!(
                    "{key}: node {} read value {} that was never written",
                    r.node + 1,
                    r.value
                )
            });
            continue;
        }
        let ok: Vec<&Write> = cands.iter().filter(|c| explains(c, r)).collect();
        if ok.is_empty() {
            let newest = acked
                .iter()
                .filter(|w| w.end.unwrap() < r.start)
                .max_by(|a, b| a.start.total_cmp(&b.start))
                .map(|w| w.value)
                .unwrap_or(0);
            errs.push(format!(
                "{key}: stale read on node {} at {:.3}-{:.3}s: got {}, but {} had been acknowledged before the read began",
                r.node + 1,
                r.start,
                r.end,
                r.value,
                newest
            ));
            continue;
        }
        // Latest possible linearization point of the observed write.
        let latest = ok
            .iter()
            .map(|c| c.end.unwrap_or(f64::INFINITY))
            .fold(f64::NEG_INFINITY, f64::max);
        attributed.push((*r, latest));
    }
    // Monotonic reads: if R1 ended before R2 began, R2 must not be explained
    // only by writes that ended before R1's write began.
    let starts_of = |v: Value| -> f64 {
        writes_of(v)
            .iter()
            .map(|w| w.start)
            .fold(f64::INFINITY, f64::min)
    };
    for (r1, _) in &attributed {
        for (r2, r2_latest) in &attributed {
            if r1.end < r2.start && r1.value != r2.value && *r2_latest < starts_of(r1.value) {
                errs.push(format!(
                    "{key}: read went back in time: node {} saw {} (ended {:.3}s), then node {} saw older {} (began {:.3}s)",
                    r1.node + 1,
                    r1.value,
                    r1.end,
                    r2.node + 1,
                    r2.value,
                    r2.start
                ));
                break;
            }
        }
    }
    errs
}

// ------------------------------------------------------------------ main

#[tokio::main]
async fn main() {
    let a = Arc::new(args());
    let _ = std::fs::remove_dir_all(&a.dir);
    std::fs::create_dir_all(&a.dir).unwrap();
    println!(
        "partitiontest: {} s, {} writers x {} keys, {} readers, seed {}, clock rates {:?}, extra args {:?}",
        a.seconds, a.writers, a.keys_per_writer, a.readers, a.seed, a.clock_rates, a.extra
    );
    net_setup();
    let mut nodes = Nodes {
        a: a.clone(),
        children: (0..NODES).map(|_| None).collect(),
    };
    nodes.start(0);
    wait_ready(&[s3_port(0)]).await;
    for n in 1..NODES {
        nodes.start(n);
    }
    let ports: Vec<u16> = (0..NODES).map(s3_port).collect();
    wait_ready(&ports).await;
    client(ports[0], Duration::from_secs(10))
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("create bucket");

    let hist = Arc::new(History {
        t0: Instant::now(),
        keys: Mutex::new(HashMap::new()),
    });
    let stop = Arc::new(AtomicBool::new(false));
    let next_value = Arc::new(AtomicU64::new(1));
    let stats = Arc::new([
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
    ]); // writes ok, writes failed, reads ok, reads failed
    let all_keys: Vec<String> = (0..a.writers)
        .flat_map(|w| (0..a.keys_per_writer).map(move |k| format!("w{w}/k{k}")))
        .collect();
    let op_timeout = a.op_timeout;

    let mut tasks = vec![];
    for w in 0..a.writers {
        let (hist, stop, next_value, stats, ports) = (
            hist.clone(),
            stop.clone(),
            next_value.clone(),
            stats.clone(),
            ports.clone(),
        );
        let keys: Vec<String> = (0..a.keys_per_writer)
            .map(|k| format!("w{w}/k{k}"))
            .collect();
        let mut rng = Rng::new(a.seed ^ (w as u64 + 1).wrapping_mul(0x5851F42D4C957F2D));
        tasks.push(tokio::spawn(async move {
            let clients: Vec<Client> = ports.iter().map(|&p| client(p, op_timeout)).collect();
            while !stop.load(Ordering::Relaxed) {
                let key = &keys[rng.below(keys.len() as u64) as usize];
                let c = &clients[rng.below(clients.len() as u64) as usize];
                let value = if rng.below(8) == 0 {
                    0
                } else {
                    next_value.fetch_add(1, Ordering::Relaxed)
                };
                let start = hist.now();
                let ok = if value == 0 {
                    c.delete_object()
                        .bucket(BUCKET)
                        .key(key)
                        .send()
                        .await
                        .is_ok()
                } else {
                    c.put_object()
                        .bucket(BUCKET)
                        .key(key)
                        .body(ByteStream::from(body(key, value)))
                        .send()
                        .await
                        .is_ok()
                };
                let end = hist.now();
                stats[if ok { 0 } else { 1 }].fetch_add(1, Ordering::Relaxed);
                hist.keys
                    .lock()
                    .entry(key.clone())
                    .or_default()
                    .writes
                    .push(Write {
                        value,
                        start,
                        end: ok.then_some(end),
                    });
            }
        }));
    }
    for r in 0..a.readers {
        let (hist, stop, stats, ports, keys) = (
            hist.clone(),
            stop.clone(),
            stats.clone(),
            ports.clone(),
            all_keys.clone(),
        );
        let mut rng = Rng::new(a.seed ^ (r as u64 + 101).wrapping_mul(0x5851F42D4C957F2D));
        tasks.push(tokio::spawn(async move {
            let clients: Vec<Client> = ports.iter().map(|&p| client(p, op_timeout)).collect();
            while !stop.load(Ordering::Relaxed) {
                let key = &keys[rng.below(keys.len() as u64) as usize];
                let node = rng.below(clients.len() as u64) as usize;
                let start = hist.now();
                let got = read(&clients[node], key).await;
                let end = hist.now();
                match got {
                    Ok(v) => {
                        stats[2].fetch_add(1, Ordering::Relaxed);
                        hist.keys
                            .lock()
                            .entry(key.clone())
                            .or_default()
                            .reads
                            .push(Read {
                                value: v.unwrap_or(u64::MAX),
                                start,
                                end,
                                node,
                            });
                    }
                    Err(_) => {
                        stats[3].fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    // Nemesis.
    let mut rng = Rng::new(a.seed);
    let deadline = Instant::now() + Duration::from_secs(a.seconds);
    let mut faults = 0;
    while Instant::now() < deadline {
        let f = if a.isolate_leader {
            match leader().await {
                Some(l) => Fault::Isolate(l),
                None => Fault::Healthy,
            }
        } else {
            match rng.below(4) {
                0 => Fault::Isolate(rng.below(NODES as u64) as usize),
                1 => Fault::Bridge {
                    middle: rng.below(NODES as u64) as usize,
                },
                2 => {
                    let from = rng.below(NODES as u64) as usize;
                    let to = (from + 1 + rng.below(NODES as u64 - 1) as usize) % NODES;
                    Fault::OneWay { from, to }
                }
                _ => Fault::Healthy,
            }
        };
        let before = [0, 1, 2, 3].map(|i| stats[i].load(Ordering::Relaxed));
        f.apply();
        if !matches!(f, Fault::Healthy) {
            faults += 1;
        }
        let hold = Duration::from_millis(3000 + rng.below(6000));
        tokio::time::sleep(hold).await;
        let after = [0, 1, 2, 3].map(|i| stats[i].load(Ordering::Relaxed));
        println!(
            "{:>6.1}s  {:<28} {:>4.1}s: {} writes ok, {} failed; {} reads ok, {} failed",
            hist.now(),
            f.describe(),
            hold.as_secs_f64(),
            after[0] - before[0],
            after[1] - before[1],
            after[2] - before[2],
            after[3] - before[3]
        );
        heal();
        tokio::time::sleep(Duration::from_millis(1000 + rng.below(2000))).await;
    }
    heal();
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }

    // Final reads of every key on every node, after healing.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let final_clients: Vec<Client> = ports
        .iter()
        .map(|&p| client(p, Duration::from_secs(30)))
        .collect();
    let mut final_errors = vec![];
    for key in &all_keys {
        for (n, c) in final_clients.iter().enumerate() {
            let start = hist.now();
            match read(c, key).await {
                Ok(v) => {
                    let end = hist.now();
                    hist.keys
                        .lock()
                        .entry(key.clone())
                        .or_default()
                        .reads
                        .push(Read {
                            value: v.unwrap_or(u64::MAX),
                            start,
                            end,
                            node: n,
                        });
                }
                Err(e) => final_errors.push(format!("{key} on node {}: {e}", n + 1)),
            }
        }
    }

    let keys = std::mem::take(&mut *hist.keys.lock());
    let mut errors: Vec<String> = final_errors;
    let (mut nw, mut nr) = (0, 0);
    for (key, mut h) in keys {
        // Map "no object" to value 0 for the checker.
        for r in &mut h.reads {
            if r.value == u64::MAX {
                r.value = 0;
            }
        }
        nw += h.writes.len();
        nr += h.reads.len();
        errors.extend(check(&key, &h));
    }
    let s = [0, 1, 2, 3].map(|i| stats[i].load(Ordering::Relaxed));
    println!(
        "done: {faults} network faults; {} writes acknowledged, {} failed or timed out; {} reads, {} failed; {nw} writes and {nr} reads checked; {} linearizability errors",
        s[0],
        s[1],
        s[2],
        s[3],
        errors.len()
    );
    for e in errors.iter().take(20) {
        println!("  {e}");
    }
    drop(nodes);
    if !errors.is_empty() {
        std::process::exit(1);
    }
}

/// The current leader (0-based), as the nodes report it.
async fn leader() -> Option<usize> {
    for n in 0..NODES {
        let out = tokio::process::Command::new("curl")
            .args([
                "-s",
                "-m",
                "2",
                "--aws-sigv4",
                "aws:amz:us-east-1:s3",
                "--user",
            ])
            .arg(format!("{AK}:{SK}"))
            .arg(format!("http://127.0.0.1:{}/_kosha/cluster", s3_port(n)))
            .output()
            .await
            .ok()?;
        let body = String::from_utf8_lossy(&out.stdout);
        if let Some(i) = body.find("\"leader\":") {
            let v: String = body[i + 9..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if let Ok(id) = v.parse::<usize>()
                && (1..=NODES).contains(&id)
            {
                return Some(id - 1);
            }
        }
    }
    None
}

/// GET a key: `Ok(Some(value))`, `Ok(None)` for no object, `Err` on failure.
async fn read(c: &Client, key: &str) -> Result<Option<Value>, String> {
    match c.get_object().bucket(BUCKET).key(key).send().await {
        Ok(out) => {
            let b = out
                .body
                .collect()
                .await
                .map_err(|e| e.to_string())?
                .into_bytes();
            Ok(Some(parse(key, &b).unwrap_or(CORRUPT)))
        }
        Err(e) => {
            if e.as_service_error().map(|s| s.is_no_such_key()) == Some(true) {
                Ok(None)
            } else {
                Err(format!("{e:?}").chars().take(200).collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(value: Value, start: f64, end: Option<f64>) -> Write {
        Write { value, start, end }
    }
    fn r(value: Value, start: f64, end: f64) -> Read {
        Read {
            value,
            start,
            end,
            node: 0,
        }
    }

    #[test]
    fn accepts_a_linearizable_history() {
        let h = KeyHistory {
            writes: vec![w(1, 0.0, Some(1.0)), w(2, 2.0, Some(3.0))],
            reads: vec![
                r(0, -1.0, -0.5),
                r(1, 1.5, 1.8),
                r(1, 2.1, 2.2),
                r(2, 2.5, 2.6),
                r(2, 4.0, 4.1),
            ],
        };
        assert!(check("k", &h).is_empty(), "{:?}", check("k", &h));
    }

    #[test]
    fn catches_a_stale_read() {
        let h = KeyHistory {
            writes: vec![w(1, 0.0, Some(1.0)), w(2, 2.0, Some(3.0))],
            reads: vec![r(1, 4.0, 4.1)],
        };
        assert_eq!(check("k", &h).len(), 1);
    }

    #[test]
    fn catches_a_value_never_written_and_corruption() {
        let h = KeyHistory {
            writes: vec![w(1, 0.0, Some(1.0))],
            reads: vec![r(7, 2.0, 2.1), r(CORRUPT, 2.0, 2.1)],
        };
        assert_eq!(check("k", &h).len(), 2);
    }

    #[test]
    fn catches_reads_going_back_in_time() {
        // Write 2 is concurrent with both reads, so each read alone is fine,
        // but the second read must not return the older value.
        let h = KeyHistory {
            writes: vec![w(1, 0.0, Some(1.0)), w(2, 2.0, Some(10.0))],
            reads: vec![r(2, 3.0, 3.5), r(1, 4.0, 4.5)],
        };
        assert_eq!(check("k", &h).len(), 1, "{:?}", check("k", &h));
    }

    #[test]
    fn a_failed_write_may_land_any_time_later() {
        let h = KeyHistory {
            writes: vec![w(1, 0.0, Some(1.0)), w(2, 2.0, None)],
            reads: vec![r(1, 3.0, 3.1), r(2, 50.0, 50.1)],
        };
        assert!(check("k", &h).is_empty(), "{:?}", check("k", &h));
    }

    #[test]
    fn bodies_round_trip_and_reject_damage() {
        let b = body("w1/k2", 42);
        assert_eq!(parse("w1/k2", &b), Some(42));
        assert_eq!(parse("w1/k3", &b), None);
        let mut d = b.to_vec();
        let n = d.len() - 1;
        d[n] ^= 1;
        assert_eq!(parse("w1/k2", &d), None);
        let big = body("k", 5);
        assert_eq!(big.len(), 40_000);
        assert_eq!(parse("k", &big), Some(5));
    }
}
