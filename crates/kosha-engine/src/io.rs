//! Chunk file I/O backends.
//!
//! * [`IoBackend::Uring`]: Linux io_uring. A dedicated thread owns each ring
//!   and drives every request as a small state machine
//!   (open → write → fsync → close, or open → read → close), so a single
//!   thread keeps hundreds of file operations in flight with one
//!   `io_uring_enter` per batch instead of one blocking syscall per step.
//! * [`IoBackend::Threads`]: portable fallback on Tokio's blocking pool.
//!
//! `IoMode::Auto` probes io_uring at startup (it is often disabled by
//! container seccomp profiles or `kernel.io_uring_disabled`) and falls back
//! to threads when the ring or any needed opcode is unavailable.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum IoMode {
    /// io_uring when available, otherwise threads.
    #[default]
    Auto,
    /// Require io_uring; fail to start without it.
    Uring,
    /// Always use the blocking thread pool.
    Threads,
}

impl std::str::FromStr for IoMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "auto" => Ok(IoMode::Auto),
            "uring" | "io_uring" | "io-uring" => Ok(IoMode::Uring),
            "threads" => Ok(IoMode::Threads),
            other => Err(format!("unknown io mode {other:?} (auto, uring, threads)")),
        }
    }
}

#[derive(Clone)]
pub enum IoBackend {
    Threads,
    #[cfg(target_os = "linux")]
    Uring(Arc<uring::UringPool>),
}

impl IoBackend {
    pub fn new(mode: IoMode, rings: usize) -> std::io::Result<Self> {
        match mode {
            IoMode::Threads => Ok(IoBackend::Threads),
            #[cfg(target_os = "linux")]
            IoMode::Auto | IoMode::Uring => match uring::UringPool::new(rings.max(1)) {
                Ok(pool) => Ok(IoBackend::Uring(Arc::new(pool))),
                Err(e) if mode == IoMode::Auto => {
                    tracing::warn!(error = %e, "io_uring unavailable, using thread pool for chunk I/O");
                    Ok(IoBackend::Threads)
                }
                Err(e) => Err(e),
            },
            #[cfg(not(target_os = "linux"))]
            IoMode::Auto => Ok(IoBackend::Threads),
            #[cfg(not(target_os = "linux"))]
            IoMode::Uring => Err(std::io::Error::other("io_uring requires Linux")),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            IoBackend::Threads => "threads",
            #[cfg(target_os = "linux")]
            IoBackend::Uring(_) => "io_uring",
        }
    }

    /// Create (or truncate) `path`, write `data`, optionally fsync.
    pub async fn write_file(
        &self,
        path: PathBuf,
        data: Vec<u8>,
        fsync: bool,
    ) -> std::io::Result<()> {
        match self {
            IoBackend::Threads => {
                blocking(move || {
                    use std::io::Write;
                    let mut f = std::fs::File::create(&path)?;
                    f.write_all(&data)?;
                    if fsync {
                        f.sync_all()?;
                    }
                    Ok(())
                })
                .await
            }
            #[cfg(target_os = "linux")]
            IoBackend::Uring(pool) => pool
                .submit(uring::Op::write(&path, data, fsync)?)
                .await
                .map(drop),
        }
    }

    /// Read a whole file. `size_hint` should be at least the file size.
    pub async fn read_file(&self, path: PathBuf, size_hint: usize) -> std::io::Result<Vec<u8>> {
        match self {
            IoBackend::Threads => blocking(move || std::fs::read(&path)).await,
            #[cfg(target_os = "linux")]
            IoBackend::Uring(pool) => pool.submit(uring::Op::read(&path, size_hint)?).await,
        }
    }

    pub async fn rename(&self, from: PathBuf, to: PathBuf) -> std::io::Result<()> {
        match self {
            IoBackend::Threads => blocking(move || std::fs::rename(&from, &to)).await,
            #[cfg(target_os = "linux")]
            IoBackend::Uring(pool) => pool.submit(uring::Op::rename(&from, &to)?).await.map(drop),
        }
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(std::io::Error::other)?
}

#[allow(dead_code)]
fn cpath(p: &Path) -> std::io::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::CString::new(p.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "path contains NUL"))
}

#[allow(dead_code)]
struct RoundRobin(AtomicUsize);

#[allow(dead_code)]
impl RoundRobin {
    fn next(&self, n: usize) -> usize {
        self.0.fetch_add(1, Ordering::Relaxed) % n
    }
}

#[cfg(target_os = "linux")]
pub mod uring {
    use std::collections::VecDeque;
    use std::ffi::CString;
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;

    use io_uring::{IoUring, Probe, opcode, squeue, types};
    use tokio::sync::oneshot;

    use super::{RoundRobin, cpath};

    const RING_ENTRIES: u32 = 256;
    /// One SQE per in-flight op plus the eventfd wakeup read.
    const MAX_IN_FLIGHT: usize = RING_ENTRIES as usize - 1;
    const WAKE: u64 = u64::MAX;
    /// Largest single read/write submitted at once.
    const IO_SLICE: usize = 1 << 30;

    pub struct Op {
        kind: Kind,
        reply: Option<oneshot::Sender<io::Result<Vec<u8>>>>,
    }

    enum Kind {
        Write {
            path: CString,
            data: Vec<u8>,
            fsync: bool,
        },
        Read {
            path: CString,
            buf: Vec<u8>,
        },
        Rename {
            from: CString,
            to: CString,
        },
    }

    impl Op {
        pub fn write(path: &Path, data: Vec<u8>, fsync: bool) -> io::Result<Op> {
            Ok(Op {
                kind: Kind::Write {
                    path: cpath(path)?,
                    data,
                    fsync,
                },
                reply: None,
            })
        }

        pub fn read(path: &Path, size_hint: usize) -> io::Result<Op> {
            Ok(Op {
                kind: Kind::Read {
                    path: cpath(path)?,
                    buf: Vec::with_capacity(size_hint.max(4096)),
                },
                reply: None,
            })
        }

        pub fn rename(from: &Path, to: &Path) -> io::Result<Op> {
            Ok(Op {
                kind: Kind::Rename {
                    from: cpath(from)?,
                    to: cpath(to)?,
                },
                reply: None,
            })
        }
    }

    struct Ring {
        tx: mpsc::Sender<Op>,
        wake: OwnedFd,
    }

    pub struct UringPool {
        rings: Vec<Ring>,
        rr: RoundRobin,
    }

    impl UringPool {
        pub fn new(n: usize) -> io::Result<Self> {
            let mut rings = Vec::with_capacity(n);
            for i in 0..n {
                let ring = IoUring::new(RING_ENTRIES)?;
                let mut probe = Probe::new();
                ring.submitter().register_probe(&mut probe)?;
                for code in [
                    opcode::OpenAt::CODE,
                    opcode::Read::CODE,
                    opcode::Write::CODE,
                    opcode::Fsync::CODE,
                    opcode::Close::CODE,
                    opcode::RenameAt::CODE,
                ] {
                    if !probe.is_supported(code) {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            format!("io_uring opcode {code} is not supported by this kernel"),
                        ));
                    }
                }
                let efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
                if efd < 0 {
                    return Err(io::Error::last_os_error());
                }
                let wake = unsafe { OwnedFd::from_raw_fd(efd) };
                let wake_reader = wake.try_clone()?;
                let (tx, rx) = mpsc::channel();
                std::thread::Builder::new()
                    .name(format!("kosha-uring-{i}"))
                    .spawn(move || {
                        if let Err(e) = Driver::new(ring, rx, wake_reader).run() {
                            tracing::error!(error = %e, "io_uring driver stopped");
                        }
                    })?;
                rings.push(Ring { tx, wake });
            }
            tracing::info!(rings = n, "chunk I/O using io_uring");
            Ok(UringPool {
                rings,
                rr: RoundRobin(AtomicUsize::new(0)),
            })
        }

        pub async fn submit(&self, mut op: Op) -> io::Result<Vec<u8>> {
            let (tx, rx) = oneshot::channel();
            op.reply = Some(tx);
            let ring = &self.rings[self.rr.next(self.rings.len())];
            ring.tx
                .send(op)
                .map_err(|_| io::Error::other("io_uring driver is not running"))?;
            let one: u64 = 1;
            let n = unsafe {
                libc::write(
                    ring.wake.as_raw_fd(),
                    &one as *const u64 as *const libc::c_void,
                    8,
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            rx.await
                .map_err(|_| io::Error::other("io_uring driver dropped the request"))?
        }
    }

    impl Drop for UringPool {
        fn drop(&mut self) {
            // Dropping the senders ends each driver loop at its next wakeup.
            for ring in self.rings.drain(..) {
                drop(ring.tx);
                let one: u64 = 1;
                unsafe {
                    libc::write(
                        ring.wake.as_raw_fd(),
                        &one as *const u64 as *const libc::c_void,
                        8,
                    );
                }
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum Stage {
        Open,
        Data,
        Fsync,
        Close,
        Rename,
    }

    struct InFlight {
        op: Op,
        stage: Stage,
        fd: i32,
        /// Bytes transferred so far.
        done: usize,
        error: Option<io::Error>,
    }

    struct Driver {
        ring: IoUring,
        rx: mpsc::Receiver<Op>,
        wake: OwnedFd,
        wake_buf: Box<u64>,
        slots: Vec<Option<InFlight>>,
        free: Vec<usize>,
        backlog: VecDeque<Op>,
        live: usize,
        closed: bool,
    }

    impl Driver {
        fn new(ring: IoUring, rx: mpsc::Receiver<Op>, wake: OwnedFd) -> Self {
            Driver {
                ring,
                rx,
                wake,
                wake_buf: Box::new(0),
                slots: (0..MAX_IN_FLIGHT).map(|_| None).collect(),
                free: (0..MAX_IN_FLIGHT).rev().collect(),
                backlog: VecDeque::new(),
                live: 0,
                closed: false,
            }
        }

        fn push(&mut self, sqe: squeue::Entry) -> io::Result<()> {
            loop {
                // Safety: every buffer and path referenced by `sqe` is owned by
                // an `InFlight` slot (or `wake_buf`) that outlives the operation.
                if unsafe { self.ring.submission().push(&sqe) }.is_ok() {
                    return Ok(());
                }
                self.ring.submit()?;
            }
        }

        fn arm_wake(&mut self) -> io::Result<()> {
            let sqe = opcode::Read::new(
                types::Fd(self.wake.as_raw_fd()),
                &mut *self.wake_buf as *mut u64 as *mut u8,
                8,
            )
            .build()
            .user_data(WAKE);
            self.push(sqe)
        }

        fn run(mut self) -> io::Result<()> {
            self.arm_wake()?;
            loop {
                self.accept();
                if self.closed && self.live == 0 {
                    return Ok(());
                }
                self.ring.submit_and_wait(1)?;
                let completions: Vec<(u64, i32)> = self
                    .ring
                    .completion()
                    .map(|c| (c.user_data(), c.result()))
                    .collect();
                for (ud, res) in completions {
                    if ud == WAKE {
                        if !self.closed {
                            self.arm_wake()?;
                        }
                        continue;
                    }
                    self.advance(ud as usize, res)?;
                }
            }
        }

        /// Move queued requests into free slots and start them.
        fn accept(&mut self) {
            loop {
                match self.rx.try_recv() {
                    Ok(op) => self.backlog.push_back(op),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        self.closed = true;
                        break;
                    }
                }
            }
            while let Some(slot) = self.free.last().copied() {
                let Some(op) = self.backlog.pop_front() else {
                    break;
                };
                self.free.pop();
                let stage = match op.kind {
                    Kind::Rename { .. } => Stage::Rename,
                    _ => Stage::Open,
                };
                self.slots[slot] = Some(InFlight {
                    op,
                    stage,
                    fd: -1,
                    done: 0,
                    error: None,
                });
                self.live += 1;
                if let Err(e) = self.issue(slot) {
                    self.finish(slot, Err(e));
                }
            }
        }

        /// Submit the SQE for the slot's current stage.
        fn issue(&mut self, slot: usize) -> io::Result<()> {
            let f = self.slots[slot].as_mut().expect("live slot");
            let ud = slot as u64;
            let sqe = match (f.stage, &mut f.op.kind) {
                (Stage::Open, Kind::Write { path, .. }) => {
                    opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
                        .flags(libc::O_CREAT | libc::O_WRONLY | libc::O_TRUNC | libc::O_CLOEXEC)
                        .mode(0o644)
                        .build()
                }
                (Stage::Open, Kind::Read { path, .. }) => {
                    opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
                        .flags(libc::O_RDONLY | libc::O_CLOEXEC)
                        .build()
                }
                (Stage::Data, Kind::Write { data, .. }) => {
                    let rest = &data[f.done..];
                    opcode::Write::new(
                        types::Fd(f.fd),
                        rest.as_ptr(),
                        rest.len().min(IO_SLICE) as u32,
                    )
                    .offset(f.done as u64)
                    .build()
                }
                (Stage::Data, Kind::Read { buf, .. }) => {
                    if buf.capacity() == buf.len() {
                        buf.reserve(buf.capacity().max(4096));
                    }
                    let spare = buf.capacity() - buf.len();
                    let ptr = unsafe { buf.as_mut_ptr().add(buf.len()) };
                    opcode::Read::new(types::Fd(f.fd), ptr, spare.min(IO_SLICE) as u32)
                        .offset(f.done as u64)
                        .build()
                }
                (Stage::Fsync, _) => opcode::Fsync::new(types::Fd(f.fd)).build(),
                (Stage::Close, _) => opcode::Close::new(types::Fd(f.fd)).build(),
                (Stage::Rename, Kind::Rename { from, to }) => opcode::RenameAt::new(
                    types::Fd(libc::AT_FDCWD),
                    from.as_ptr(),
                    types::Fd(libc::AT_FDCWD),
                    to.as_ptr(),
                )
                .build(),
                (stage, _) => unreachable!("invalid stage {stage:?}"),
            };
            self.push(sqe.user_data(ud))
        }

        fn advance(&mut self, slot: usize, res: i32) -> io::Result<()> {
            let f = self.slots[slot].as_mut().expect("completion for live slot");
            let failed = res < 0;
            if failed && f.error.is_none() {
                f.error = Some(io::Error::from_raw_os_error(-res));
            }
            let next = match (f.stage, &mut f.op.kind) {
                (Stage::Rename, _) | (Stage::Open, _) if failed => None,
                (Stage::Rename, _) => None,
                (Stage::Open, _) => {
                    f.fd = res;
                    Some(Stage::Data)
                }
                (Stage::Data, _) if failed => Some(Stage::Close),
                (Stage::Data, Kind::Write { data, fsync, .. }) => {
                    f.done += res as usize;
                    if res == 0 && f.done < data.len() {
                        f.error = Some(io::Error::new(io::ErrorKind::WriteZero, "short write"));
                        Some(Stage::Close)
                    } else if f.done < data.len() {
                        Some(Stage::Data)
                    } else if *fsync {
                        Some(Stage::Fsync)
                    } else {
                        Some(Stage::Close)
                    }
                }
                (Stage::Data, Kind::Read { buf, .. }) => {
                    if res == 0 {
                        Some(Stage::Close)
                    } else {
                        unsafe { buf.set_len(buf.len() + res as usize) };
                        f.done += res as usize;
                        Some(Stage::Data)
                    }
                }
                (Stage::Fsync, _) => Some(Stage::Close),
                (Stage::Close, _) => None,
                (stage, _) => unreachable!("invalid stage {stage:?}"),
            };
            match next {
                Some(stage) => {
                    f.stage = stage;
                    if let Err(e) = self.issue(slot) {
                        self.finish(slot, Err(e));
                    }
                }
                None => {
                    let f = self.slots[slot].as_mut().expect("live slot");
                    let result = match f.error.take() {
                        Some(e) => Err(e),
                        None => Ok(match &mut f.op.kind {
                            Kind::Read { buf, .. } => std::mem::take(buf),
                            _ => Vec::new(),
                        }),
                    };
                    self.finish(slot, result);
                }
            }
            Ok(())
        }

        fn finish(&mut self, slot: usize, result: io::Result<Vec<u8>>) {
            if let Some(mut f) = self.slots[slot].take() {
                if let Some(reply) = f.op.reply.take() {
                    let _ = reply.send(result);
                }
                self.free.push(slot);
                self.live -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn exercise(io: IoBackend) {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i * 7) as u8).collect();
        io.write_file(a.clone(), data.clone(), true).await.unwrap();
        io.rename(a.clone(), b.clone()).await.unwrap();
        // Undersized hint: the reader must grow its buffer.
        assert_eq!(io.read_file(b.clone(), 1000).await.unwrap(), data);
        assert_eq!(io.read_file(b.clone(), data.len() + 1).await.unwrap(), data);
        let missing = io.read_file(a, 10).await.unwrap_err();
        assert_eq!(missing.kind(), std::io::ErrorKind::NotFound);

        // Many concurrent operations (more than the ring holds at once).
        let mut tasks = Vec::new();
        for i in 0..600 {
            let io = io.clone();
            let p = dir.path().join(format!("f{i}"));
            tasks.push(tokio::spawn(async move {
                let body = format!("file {i}").repeat(i % 50 + 1).into_bytes();
                io.write_file(p.clone(), body.clone(), i % 3 == 0)
                    .await
                    .unwrap();
                assert_eq!(io.read_file(p, 16).await.unwrap(), body);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    }

    #[tokio::test]
    async fn threads_backend() {
        exercise(IoBackend::new(IoMode::Threads, 1).unwrap()).await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn uring_backend() {
        match IoBackend::new(IoMode::Uring, 2) {
            Ok(io) => {
                assert_eq!(io.name(), "io_uring");
                exercise(io).await
            }
            Err(e) => eprintln!("skipping: io_uring unavailable here: {e}"),
        }
    }
}
