//! How chunk files are made durable before an upload is acknowledged.
//!
//! A chunk is written to a temp file and renamed into place. For the chunk to
//! survive a power failure, both the file's data *and* the rename (a change
//! to the parent directory) must reach stable storage before the metadata
//! that references it commits.
//!
//! * [`SyncMode::File`] fsyncs the file, renames it, then fsyncs the parent
//!   directory: two fsyncs per chunk.
//! * [`SyncMode::Group`] writes and renames without syncing, then waits for a
//!   shared `syncfs(2)` of the chunk filesystem. Concurrent writers share one
//!   sync, the data-path equivalent of group commit. A writer that arrives
//!   while a sync is running waits for the *next* one, because its writes
//!   may postdate the running sync. Linux only.
//! * [`SyncMode::None`] skips both (fast, not crash-safe for chunk data).

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};

use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SyncMode {
    None,
    File,
    #[default]
    Group,
}

impl std::str::FromStr for SyncMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "none" => Ok(SyncMode::None),
            "file" => Ok(SyncMode::File),
            "group" => Ok(SyncMode::Group),
            other => Err(format!("unknown sync mode {other:?} (group, file, none)")),
        }
    }
}

type Waiter = oneshot::Sender<std::io::Result<()>>;

/// Batches `syncfs` calls on one filesystem.
pub struct GroupSync {
    tx: Option<mpsc::Sender<Waiter>>,
    thread: Option<std::thread::JoinHandle<()>>,
    pub stats: Arc<SyncStats>,
}

/// Writers that asked for durability vs `syncfs` calls actually made.
#[derive(Default, Debug)]
pub struct SyncStats {
    pub requests: AtomicU64,
    pub syncs: AtomicU64,
}

impl GroupSync {
    pub fn start(root: &Path) -> std::io::Result<Self> {
        let dir = std::fs::File::open(root)?;
        let (tx, rx) = mpsc::channel::<Waiter>();
        let stats = Arc::new(SyncStats::default());
        let counters = stats.clone();
        let thread = std::thread::Builder::new()
            .name("kosha-syncfs".into())
            .spawn(move || {
                while let Ok(first) = rx.recv() {
                    let mut waiters = vec![first];
                    while let Ok(w) = rx.try_recv() {
                        waiters.push(w);
                    }
                    counters
                        .requests
                        .fetch_add(waiters.len() as u64, Ordering::Relaxed);
                    counters.syncs.fetch_add(1, Ordering::Relaxed);
                    let r = syncfs(&dir);
                    for w in waiters {
                        let _ = w.send(match &r {
                            Ok(()) => Ok(()),
                            Err(e) => Err(std::io::Error::new(e.kind(), e.to_string())),
                        });
                    }
                }
            })?;
        Ok(GroupSync {
            tx: Some(tx),
            thread: Some(thread),
            stats,
        })
    }

    /// Wait until everything written before this call is durable.
    pub async fn sync(&self) -> std::io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .as_ref()
            .expect("sender lives until drop")
            .send(tx)
            .map_err(|_| std::io::Error::other("sync thread stopped"))?;
        rx.await
            .map_err(|_| std::io::Error::other("sync thread dropped the request"))?
    }
}

impl Drop for GroupSync {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

#[cfg(target_os = "linux")]
fn syncfs(f: &std::fs::File) -> std::io::Result<()> {
    if unsafe { libc::syncfs(f.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn syncfs(_f: &std::fs::File) -> std::io::Result<()> {
    Err(std::io::Error::other("group sync requires Linux"))
}

/// fsync a directory so renames inside it are durable.
pub fn sync_dir(dir: PathBuf) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// `create_dir_all`, but every directory it creates is made durable: its
/// parent is fsynced after the `mkdir`. Without this, a power cut can lose a
/// new directory, and every file in it, even after those files and the
/// directory itself were fsynced.
pub fn create_dir_all_durable(dir: &std::path::Path) -> std::io::Result<()> {
    let mut missing = vec![];
    let mut cur = dir;
    while std::fs::metadata(cur).is_err() {
        missing.push(cur);
        match cur.parent() {
            Some(p) if !p.as_os_str().is_empty() => cur = p,
            _ => break,
        }
    }
    for d in missing.into_iter().rev() {
        match std::fs::create_dir(d) {
            Ok(()) => {}
            // Created concurrently: someone else syncs it.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
        let parent = match d.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        };
        sync_dir(parent)?;
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_share_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let g = Arc::new(GroupSync::start(dir.path()).unwrap());
        let mut tasks = Vec::new();
        for i in 0..200 {
            let (g, p) = (g.clone(), dir.path().join(format!("f{i}")));
            tasks.push(tokio::spawn(async move {
                std::fs::write(p, b"data").unwrap();
                g.sync().await.unwrap();
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let requests = g.stats.requests.load(Ordering::Relaxed);
        let syncs = g.stats.syncs.load(Ordering::Relaxed);
        assert_eq!(requests, 200);
        assert!(syncs < requests, "{syncs} syncs for {requests} writers");
    }
}
