//! S3 load generator built on the official AWS SDK.
//!
//! ```sh
//! cargo run --release -p kosha-s3 --example s3bench -- \
//!     --endpoint http://127.0.0.1:9000 --size 64KiB --count 2000 --concurrency 32
//! ```
//!
//! Writes `count` objects of `size` bytes, then reads them all back, and
//! reports throughput and latency percentiles for each phase. Pass
//! `--read-endpoint` to read from a different node than the one written to.
//! Credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use futures::StreamExt;

struct Args {
    endpoint: String,
    read_endpoint: Option<String>,
    bucket: String,
    size: usize,
    count: usize,
    concurrency: usize,
    unique: bool,
}

fn parse_size(s: &str) -> usize {
    let s = s.trim();
    let (num, mult) = [
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
        ("B", 1),
    ]
    .iter()
    .find_map(|(suf, m)| s.strip_suffix(suf).map(|n| (n, *m)))
    .unwrap_or((s, 1));
    num.trim().parse::<usize>().expect("size") * mult
}

fn args() -> Args {
    let mut a = Args {
        endpoint: "http://127.0.0.1:9000".into(),
        read_endpoint: None,
        bucket: "bench".into(),
        size: 64 * 1024,
        count: 1000,
        concurrency: 32,
        unique: true,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--endpoint" => a.endpoint = val(),
            "--read-endpoint" => a.read_endpoint = Some(val()),
            "--bucket" => a.bucket = val(),
            "--size" => a.size = parse_size(&val()),
            "--count" => a.count = val().parse().expect("count"),
            "--concurrency" => a.concurrency = val().parse().expect("concurrency"),
            "--duplicate" => a.unique = false,
            other => panic!("unknown flag {other}"),
        }
    }
    a
}

fn client(endpoint: &str) -> Client {
    let key = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "admin".into());
    let secret =
        std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "admin-secret-key".into());
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new(key, secret, None, None, "bench"))
        .force_path_style(true)
        .build();
    Client::from_conf(conf)
}

/// Incompressible bytes; `seed` makes each object unique so dedup cannot help.
fn payload(size: usize, seed: u64) -> Bytes {
    let mut x = seed.wrapping_mul(0x9E3779B97F4A7C15) | 1;
    let mut v = Vec::with_capacity(size);
    while v.len() < size {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.extend_from_slice(&x.to_le_bytes());
    }
    v.truncate(size);
    Bytes::from(v)
}

fn report(phase: &str, a: &Args, elapsed: Duration, mut lat: Vec<Duration>, errors: usize) {
    lat.sort();
    let pct = |p: f64| {
        lat.get(((lat.len() as f64 * p) as usize).min(lat.len().saturating_sub(1)))
            .copied()
            .unwrap_or_default()
    };
    let secs = elapsed.as_secs_f64();
    println!(
        "{phase:<5} {:>9} x {:>6} c={:<3} {:>9.0} ops/s {:>8.1} MiB/s   p50 {:>7.2} ms  p99 {:>7.2} ms  max {:>7.2} ms  errors {}",
        human(a.size),
        a.count,
        a.concurrency,
        lat.len() as f64 / secs,
        (lat.len() * a.size) as f64 / secs / (1 << 20) as f64,
        pct(0.50).as_secs_f64() * 1e3,
        pct(0.99).as_secs_f64() * 1e3,
        lat.last().copied().unwrap_or_default().as_secs_f64() * 1e3,
        errors
    );
}

fn human(n: usize) -> String {
    match n {
        n if n >= 1 << 20 && n % (1 << 20) == 0 => format!("{}MiB", n >> 20),
        n if n >= 1 << 10 && n % (1 << 10) == 0 => format!("{}KiB", n >> 10),
        n => format!("{n}B"),
    }
}

#[tokio::main]
async fn main() {
    let a = Arc::new(args());
    let writer = client(&a.endpoint);
    let reader = client(a.read_endpoint.as_deref().unwrap_or(&a.endpoint));
    let _ = writer.create_bucket().bucket(&a.bucket).send().await;
    let run = format!(
        "{:x}",
        Instant::now().elapsed().as_nanos() ^ std::process::id() as u128
    );

    // Pre-generate bodies so the client's own CPU work is not measured.
    let bodies: Vec<Bytes> = (0..a.count.min(256))
        .map(|i| payload(a.size, if a.unique { i as u64 + 1 } else { 1 }))
        .collect();
    let body_for = |i: usize| -> Bytes {
        if a.unique && a.count > bodies.len() {
            payload(a.size, i as u64 + 1)
        } else {
            bodies[i % bodies.len()].clone()
        }
    };
    let bodies: Vec<Bytes> = (0..a.count).map(body_for).collect();

    for phase in ["PUT", "GET"] {
        let start = Instant::now();
        let results: Vec<Result<Duration, String>> = futures::stream::iter(0..a.count)
            .map(|i| {
                let (w, r, a, run, body) = (
                    writer.clone(),
                    reader.clone(),
                    a.clone(),
                    run.clone(),
                    bodies[i].clone(),
                );
                async move {
                    let key = format!("{run}/{i:08}");
                    let t = Instant::now();
                    if phase == "PUT" {
                        w.put_object()
                            .bucket(&a.bucket)
                            .key(&key)
                            .body(ByteStream::from(body))
                            .send()
                            .await
                            .map_err(|e| format!("{e:?}"))?;
                    } else {
                        let out = r
                            .get_object()
                            .bucket(&a.bucket)
                            .key(&key)
                            .send()
                            .await
                            .map_err(|e| format!("{e:?}"))?;
                        let got = out
                            .body
                            .collect()
                            .await
                            .map_err(|e| e.to_string())?
                            .into_bytes();
                        if got != body {
                            return Err(format!("{key}: content mismatch"));
                        }
                    }
                    Ok(t.elapsed())
                }
            })
            .buffer_unordered(a.concurrency)
            .collect()
            .await;
        let elapsed = start.elapsed();
        let errors: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
        if let Some(e) = errors.first() {
            eprintln!("first error: {e}");
        }
        report(
            phase,
            &a,
            elapsed,
            results
                .iter()
                .filter_map(|r| r.as_ref().ok().copied())
                .collect(),
            errors.len(),
        );
    }
}
