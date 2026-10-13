//! Compare chunk I/O backends: `cargo run --release --example iobench -- <dir>`
//!
//! `COLD=1` drops the page cache before each read pass (needs root), so reads
//! come from the device as they would for data that is not hot, and reports
//! how much page cache each backend left behind.
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use kosha_engine::{DIRECT_STATS, DirectConfig, IoBackend, IoMode};

#[tokio::main]
async fn main() {
    let dir = std::path::PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| "/tmp/kosha-iobench".into()),
    );
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    for (size, count, conc) in [(4096usize, 20_000usize, 64usize), (1 << 20, 1_500, 32)] {
        for fsync in [false, true] {
            let count = if fsync { count / 10 } else { count };
            // Direct I/O only applies to large files.
            let mut variants = vec![(IoMode::Threads, None), (IoMode::Uring, None)];
            if size >= DirectConfig::default().min_size {
                // One buffer per in-flight request, so none fall back.
                let per_ring = conc.div_ceil(rings());
                variants.push((
                    IoMode::Uring,
                    Some(DirectConfig {
                        buffers: per_ring,
                        ..DirectConfig::default()
                    }),
                ));
            }
            for (mode, direct) in variants {
                let io = IoBackend::with_direct(mode, rings(), direct).unwrap();
                let fallbacks = DIRECT_STATS.fallbacks.load(Relaxed);
                let cold = std::env::var_os("COLD").is_some();
                if cold {
                    drop_caches();
                }
                let cached_before = page_cache();
                let data = vec![7u8; size];
                let t = Instant::now();
                run(&io, &dir, count, conc, |io, p| {
                    let d = data.clone();
                    async move { io.write_file(p, d, fsync).await.unwrap() }
                })
                .await;
                let w = t.elapsed();
                let cached_after_write = page_cache();
                if cold {
                    drop_caches();
                }
                let t = Instant::now();
                run(&io, &dir, count, conc, |io, p| async move {
                    io.read_file(p, size + 1).await.unwrap();
                })
                .await;
                let r = t.elapsed();
                let cached_after_read = page_cache();
                let name = if direct.is_some() {
                    "uring+direct"
                } else {
                    io.name()
                };
                println!(
                    "{:>8} x {:>6} fsync={:<5} {:<12} write {:>8.0} ops/s ({:>7.1} MB/s)   read {:>8.0} ops/s ({:>7.1} MB/s)",
                    size,
                    count,
                    fsync,
                    name,
                    count as f64 / w.as_secs_f64(),
                    (count * size) as f64 / w.as_secs_f64() / 1e6,
                    count as f64 / r.as_secs_f64(),
                    (count * size) as f64 / r.as_secs_f64() / 1e6,
                );
                if cold {
                    println!(
                        "{:>40} page cache grew {} MB on write, {} MB on read",
                        "",
                        cached_after_write.saturating_sub(cached_before) >> 20,
                        cached_after_read.saturating_sub(cached_before) >> 20,
                    );
                }
                let fell_back = DIRECT_STATS.fallbacks.load(Relaxed) - fallbacks;
                if direct.is_some() && fell_back > 0 {
                    println!("{:>40} ({fell_back} ops fell back to buffered I/O)", "");
                }
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn rings() -> usize {
    std::env::var("RINGS")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(2)
}

fn drop_caches() {
    // SAFETY: sync(2) has no preconditions.
    unsafe { libc::sync() };
    if let Err(e) = std::fs::write("/proc/sys/vm/drop_caches", "3") {
        eprintln!("COLD=1 needs root to drop the page cache: {e}");
    }
}

/// Bytes in the page cache (`Cached` in /proc/meminfo), or 0.
fn page_cache() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|m| {
            m.lines()
                .find_map(|l| l.strip_prefix("Cached:"))
                .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        })
        .map_or(0, |kb| kb * 1024)
}

async fn run<F, Fut>(io: &IoBackend, dir: &std::path::Path, count: usize, conc: usize, f: F)
where
    F: Fn(IoBackend, std::path::PathBuf) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    use futures::StreamExt;
    futures::stream::iter(0..count)
        .map(|i| tokio::spawn(f(io.clone(), dir.join(format!("f{i}")))))
        .buffer_unordered(conc)
        .for_each(|r| async { r.unwrap() })
        .await;
}
