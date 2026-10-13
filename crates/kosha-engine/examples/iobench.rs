//! Compare chunk I/O backends: `cargo run --release --example iobench -- <dir>`
use std::time::Instant;

use kosha_engine::{IoBackend, IoMode};

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
            for mode in [IoMode::Threads, IoMode::Uring] {
                let rings = std::env::var("RINGS")
                    .ok()
                    .and_then(|r| r.parse().ok())
                    .unwrap_or(2);
                let io = IoBackend::new(mode, rings).unwrap();
                let data = vec![7u8; size];
                let t = Instant::now();
                run(&io, &dir, count, conc, |io, p| {
                    let d = data.clone();
                    async move { io.write_file(p, d, fsync).await.unwrap() }
                })
                .await;
                let w = t.elapsed();
                let t = Instant::now();
                run(&io, &dir, count, conc, |io, p| async move {
                    io.read_file(p, size + 1).await.unwrap();
                })
                .await;
                let r = t.elapsed();
                println!(
                    "{:>8} x {:>6} fsync={:<5} {:<8} write {:>8.0} ops/s ({:>7.1} MB/s)   read {:>8.0} ops/s ({:>7.1} MB/s)",
                    size,
                    count,
                    fsync,
                    io.name(),
                    count as f64 / w.as_secs_f64(),
                    (count * size) as f64 / w.as_secs_f64() / 1e6,
                    count as f64 / r.as_secs_f64(),
                    (count * size) as f64 / r.as_secs_f64() / 1e6,
                );
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
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
