//! A FUSE filesystem that simulates power cuts, for testing durability.
//!
//! ```sh
//! kosha-faultfs <backing-dir> <mountpoint> [--model journal|posix]
//! ```
//!
//! Everything is passed through to `<backing-dir>`, except that nothing is
//! durable until the program asks for it:
//!
//! - **File data** (writes, truncation) is kept in memory until `fsync` or
//!   `fdatasync` of that file.
//! - **Directory changes** (create, mkdir, unlink, rmdir, rename) are applied
//!   at once but logged so they can be undone. With `--model posix` (the
//!   default) a change becomes durable only when its directory is fsynced
//!   (for a rename, the destination directory). With `--model journal` any
//!   fsync makes every earlier directory change durable, like the metadata
//!   journal of ext4 or XFS.
//!
//! Signals control it:
//!
//! - `SIGUSR1`: **power cut.** Unsynced data is discarded, undurable
//!   directory changes are undone (newest first), and the filesystem is
//!   unmounted. Kill the program using it *before* sending this.
//! - `SIGUSR2`: make everything durable, like `sync`.
//! - `SIGTERM` / `SIGINT`: make everything durable and unmount.
//!
//! `syncfs(2)` is not forwarded to FUSE filesystems by the kernel, so
//! programs that rely on it cannot be tested with this tool.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::{CString, OsStr};
use std::fs::{self, File, OpenOptions};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, UNIX_EPOCH};

use fuser::{
    Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo,
    LockOwner, MountOption, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
    SessionACL, TimeOrNow, WriteFlags,
};

const PAGE: u64 = 4096;
const TTL: Duration = Duration::ZERO;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Model {
    Posix,
    Journal,
}

/// Unsynced contents of one file.
struct Dirty {
    pages: BTreeMap<u64, Vec<u8>>,
    /// Logical size.
    size: u64,
    /// Bytes of the backing file still valid (the smallest size since the
    /// last sync); beyond it, data not in `pages` reads as zeros.
    floor: u64,
}

/// A directory change that can still be undone.
enum Op {
    Create(PathBuf),
    Mkdir(PathBuf),
    Unlink {
        path: PathBuf,
        trash: PathBuf,
    },
    Rmdir(PathBuf),
    Rename {
        from: PathBuf,
        to: PathBuf,
        displaced: Option<PathBuf>,
    },
}

struct Logged {
    op: Op,
    /// Directories that must be fsynced before the change is durable.
    pending: BTreeSet<PathBuf>,
}

struct Inner {
    backing: PathBuf,
    trash: PathBuf,
    model: Model,
    paths: HashMap<u64, PathBuf>,
    inos: HashMap<PathBuf, u64>,
    next_ino: u64,
    files: HashMap<u64, File>,
    dirty: HashMap<u64, Dirty>,
    log: Vec<Logged>,
    trash_seq: u64,
    stats: Stats,
}

#[derive(Default)]
struct Stats {
    synced_files: u64,
    synced_dirs: u64,
}

fn parent_of(p: &Path) -> PathBuf {
    p.parent().map(Path::to_path_buf).unwrap_or_default()
}

impl Inner {
    fn real(&self, rel: &Path) -> PathBuf {
        self.backing.join(rel)
    }

    fn ino_for(&mut self, rel: &Path) -> u64 {
        if let Some(&i) = self.inos.get(rel) {
            return i;
        }
        let i = self.next_ino;
        self.next_ino += 1;
        self.paths.insert(i, rel.to_path_buf());
        self.inos.insert(rel.to_path_buf(), i);
        i
    }

    fn path(&self, ino: INodeNo) -> Result<PathBuf, Errno> {
        self.paths.get(&ino.0).cloned().ok_or(Errno::ENOENT)
    }

    fn child(&self, parent: INodeNo, name: &OsStr) -> Result<PathBuf, Errno> {
        Ok(self.path(parent)?.join(name))
    }

    fn forget_path(&mut self, rel: &Path) {
        if let Some(i) = self.inos.remove(rel) {
            self.paths.remove(&i);
        }
    }

    /// Re-point `from` and everything below it to `to`.
    fn move_paths(&mut self, from: &Path, to: &Path) {
        let moved: Vec<(PathBuf, u64)> = self
            .inos
            .iter()
            .filter(|(p, _)| p.starts_with(from))
            .map(|(p, i)| (p.clone(), *i))
            .collect();
        for (p, i) in moved {
            self.inos.remove(&p);
            let rest = p.strip_prefix(from).unwrap();
            let np = if rest.as_os_str().is_empty() {
                to.to_path_buf()
            } else {
                to.join(rest)
            };
            self.paths.insert(i, np.clone());
            self.inos.insert(np, i);
        }
    }

    fn file(&mut self, ino: u64) -> Result<&File, Errno> {
        if !self.files.contains_key(&ino) {
            let rel = self.paths.get(&ino).cloned().ok_or(Errno::ENOENT)?;
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.real(&rel))
                .map_err(Errno::from)?;
            self.files.insert(ino, f);
        }
        Ok(&self.files[&ino])
    }

    fn backing_len(&mut self, ino: u64) -> Result<u64, Errno> {
        Ok(self.file(ino)?.metadata().map_err(Errno::from)?.len())
    }

    fn dirty_mut(&mut self, ino: u64) -> Result<&mut Dirty, Errno> {
        if !self.dirty.contains_key(&ino) {
            let len = self.backing_len(ino)?;
            self.dirty.insert(
                ino,
                Dirty {
                    pages: BTreeMap::new(),
                    size: len,
                    floor: len,
                },
            );
        }
        Ok(self.dirty.get_mut(&ino).unwrap())
    }

    /// One page as currently visible (dirty copy or backing, zero-filled).
    fn page(&mut self, ino: u64, idx: u64) -> Result<Vec<u8>, Errno> {
        if let Some(d) = self.dirty.get(&ino)
            && let Some(p) = d.pages.get(&idx)
        {
            return Ok(p.clone());
        }
        let floor = match self.dirty.get(&ino) {
            Some(d) => d.floor,
            None => self.backing_len(ino)?,
        };
        let mut buf = vec![0u8; PAGE as usize];
        let start = idx * PAGE;
        if start < floor {
            let n = (floor - start).min(PAGE) as usize;
            let f = self.file(ino)?;
            let mut got = 0;
            while got < n {
                let r = f
                    .read_at(&mut buf[got..n], start + got as u64)
                    .map_err(Errno::from)?;
                if r == 0 {
                    break;
                }
                got += r;
            }
        }
        Ok(buf)
    }

    fn size(&mut self, ino: u64) -> Result<u64, Errno> {
        match self.dirty.get(&ino) {
            Some(d) => Ok(d.size),
            None => self.backing_len(ino),
        }
    }

    fn read(&mut self, ino: u64, off: u64, len: u32) -> Result<Vec<u8>, Errno> {
        let size = self.size(ino)?;
        if off >= size {
            return Ok(vec![]);
        }
        let end = (off + len as u64).min(size);
        if !self.dirty.contains_key(&ino) {
            let mut buf = vec![0u8; (end - off) as usize];
            let f = self.file(ino)?;
            let mut got = 0;
            while got < buf.len() {
                let r = f
                    .read_at(&mut buf[got..], off + got as u64)
                    .map_err(Errno::from)?;
                if r == 0 {
                    break;
                }
                got += r;
            }
            buf.truncate(got);
            return Ok(buf);
        }
        let mut out = Vec::with_capacity((end - off) as usize);
        let mut pos = off;
        while pos < end {
            let idx = pos / PAGE;
            let page = self.page(ino, idx)?;
            let from = (pos - idx * PAGE) as usize;
            let to = ((end - idx * PAGE).min(PAGE)) as usize;
            out.extend_from_slice(&page[from..to]);
            pos = idx * PAGE + to as u64;
        }
        Ok(out)
    }

    fn write(&mut self, ino: u64, off: u64, data: &[u8]) -> Result<(), Errno> {
        self.dirty_mut(ino)?;
        let mut pos = off;
        let end = off + data.len() as u64;
        while pos < end {
            let idx = pos / PAGE;
            let mut page = self.page(ino, idx)?;
            let from = (pos - idx * PAGE) as usize;
            let to = ((end - idx * PAGE).min(PAGE)) as usize;
            let src = (pos - off) as usize;
            page[from..to].copy_from_slice(&data[src..src + (to - from)]);
            self.dirty.get_mut(&ino).unwrap().pages.insert(idx, page);
            pos = idx * PAGE + to as u64;
        }
        let d = self.dirty.get_mut(&ino).unwrap();
        d.size = d.size.max(end);
        Ok(())
    }

    fn truncate(&mut self, ino: u64, size: u64) -> Result<(), Errno> {
        let d = self.dirty_mut(ino)?;
        d.size = size;
        d.floor = d.floor.min(size);
        let keep = size.div_ceil(PAGE);
        d.pages.retain(|&i, _| i < keep);
        if !size.is_multiple_of(PAGE)
            && let Some(p) = d.pages.get_mut(&(size / PAGE))
        {
            p[(size % PAGE) as usize..].fill(0);
        }
        // A partial page that is only in the backing file must be copied so
        // its tail reads as zeros if the file grows again.
        if !size.is_multiple_of(PAGE) && !self.dirty[&ino].pages.contains_key(&(size / PAGE)) {
            let mut page = self.page(ino, size / PAGE)?;
            page[(size % PAGE) as usize..].fill(0);
            self.dirty
                .get_mut(&ino)
                .unwrap()
                .pages
                .insert(size / PAGE, page);
        }
        Ok(())
    }

    /// Make one file's data durable.
    fn sync_file(&mut self, ino: u64) -> Result<(), Errno> {
        if let Some(d) = self.dirty.remove(&ino) {
            let f = self.file(ino)?;
            for (&idx, page) in &d.pages {
                let start = idx * PAGE;
                if start >= d.size {
                    continue;
                }
                let n = (d.size - start).min(PAGE) as usize;
                f.write_all_at(&page[..n], start).map_err(Errno::from)?;
            }
            f.set_len(d.size).map_err(Errno::from)?;
            self.stats.synced_files += 1;
        }
        if self.model == Model::Journal {
            self.commit_log();
        }
        Ok(())
    }

    fn sync_dir(&mut self, dir: &Path) {
        self.stats.synced_dirs += 1;
        if self.model == Model::Journal {
            self.commit_log();
            return;
        }
        for l in &mut self.log {
            l.pending.remove(dir);
        }
        let (done, rest): (Vec<_>, Vec<_>) = self.log.drain(..).partition(|l| l.pending.is_empty());
        self.log = rest;
        for l in done {
            self.forget_trash(&l.op);
        }
    }

    fn commit_log(&mut self) {
        for l in std::mem::take(&mut self.log) {
            self.forget_trash(&l.op);
        }
    }

    fn forget_trash(&self, op: &Op) {
        match op {
            Op::Unlink { trash, .. }
            | Op::Rename {
                displaced: Some(trash),
                ..
            } => {
                let _ = fs::remove_file(trash).or_else(|_| fs::remove_dir_all(trash));
            }
            _ => {}
        }
    }

    fn log(&mut self, op: Op, dirs: &[PathBuf]) {
        self.log.push(Logged {
            op,
            pending: dirs.iter().cloned().collect(),
        });
    }

    fn move_to_trash(&mut self, rel: &Path) -> Result<PathBuf, Errno> {
        self.trash_seq += 1;
        let t = self.trash.join(self.trash_seq.to_string());
        fs::rename(self.real(rel), &t).map_err(Errno::from)?;
        Ok(t)
    }

    fn attr(&mut self, ino: u64) -> Result<FileAttr, Errno> {
        let rel = self.paths.get(&ino).cloned().ok_or(Errno::ENOENT)?;
        let m = fs::symlink_metadata(self.real(&rel)).map_err(Errno::from)?;
        let kind = if m.is_dir() {
            FileType::Directory
        } else {
            FileType::RegularFile
        };
        let size = if m.is_dir() { m.len() } else { self.size(ino)? };
        let t = |s: i64, ns: i64| UNIX_EPOCH + Duration::new(s.max(0) as u64, ns as u32);
        Ok(FileAttr {
            ino: INodeNo(ino),
            size,
            blocks: size.div_ceil(512),
            atime: t(m.atime(), m.atime_nsec()),
            mtime: t(m.mtime(), m.mtime_nsec()),
            ctime: t(m.ctime(), m.ctime_nsec()),
            crtime: UNIX_EPOCH,
            kind,
            perm: (m.mode() & 0o7777) as u16,
            nlink: m.nlink() as u32,
            uid: m.uid(),
            gid: m.gid(),
            rdev: 0,
            blksize: PAGE as u32,
            flags: 0,
        })
    }

    /// Simulated power loss: discard unsynced data, undo undurable changes.
    fn power_cut(&mut self) -> String {
        let dropped: u64 = self
            .dirty
            .values()
            .map(|d| d.pages.len() as u64 * PAGE)
            .sum();
        let files = self.dirty.len();
        self.dirty.clear();
        self.files.clear();
        let undone = self.log.len();
        let log = std::mem::take(&mut self.log);
        let verbose = std::env::var_os("FAULTFS_VERBOSE").is_some();
        for l in log.into_iter().rev() {
            let r = |p: &Path| self.backing.join(p);
            if verbose {
                eprintln!("kosha-faultfs: undo {}", describe(&l.op));
            }
            let _ = match l.op {
                Op::Create(p) => fs::remove_file(r(&p)),
                Op::Mkdir(p) => fs::remove_dir_all(r(&p)),
                Op::Rmdir(p) => fs::create_dir(r(&p)),
                Op::Unlink { path, trash } => fs::rename(trash, r(&path)),
                Op::Rename {
                    from,
                    to,
                    displaced,
                } => fs::rename(r(&to), r(&from)).and_then(|_| match displaced {
                    Some(t) => fs::rename(t, r(&to)),
                    None => Ok(()),
                }),
            };
        }
        let _ = fs::remove_dir_all(&self.trash);
        format!(
            "power cut: dropped ~{} KiB of unsynced data in {files} files, undid {undone} directory changes \
             ({} file syncs, {} directory syncs since mount)",
            dropped / 1024,
            self.stats.synced_files,
            self.stats.synced_dirs
        )
    }

    fn sync_all(&mut self) {
        let inos: Vec<u64> = self.dirty.keys().copied().collect();
        for i in inos {
            let _ = self.sync_file(i);
        }
        self.commit_log();
    }
}

fn describe(op: &Op) -> String {
    match op {
        Op::Create(p) => format!("create {}", p.display()),
        Op::Mkdir(p) => format!("mkdir {}", p.display()),
        Op::Rmdir(p) => format!("rmdir {}", p.display()),
        Op::Unlink { path, .. } => format!("unlink {}", path.display()),
        Op::Rename { from, to, .. } => format!("rename {} -> {}", from.display(), to.display()),
    }
}

struct FaultFs(Arc<Mutex<Inner>>);

impl FaultFs {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn cstr(p: &Path) -> CString {
    CString::new(p.as_os_str().as_bytes()).unwrap()
}

impl Filesystem for FaultFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let mut s = self.lock();
        let res = (|| {
            let rel = s.child(parent, name)?;
            fs::symlink_metadata(s.real(&rel)).map_err(Errno::from)?;
            let ino = s.ino_for(&rel);
            s.attr(ino)
        })();
        match res {
            Ok(a) => reply.entry(&TTL, &a, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.lock().attr(ino.0) {
            Ok(a) => reply.attr(&TTL, &a),
            Err(e) => reply.error(e),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let mut s = self.lock();
        let res = (|| {
            let rel = s.path(ino)?;
            if let Some(m) = mode {
                fs::set_permissions(s.real(&rel), fs::Permissions::from_mode(m))
                    .map_err(Errno::from)?;
            }
            if uid.is_some() || gid.is_some() {
                let r = unsafe {
                    libc::chown(
                        cstr(&s.real(&rel)).as_ptr(),
                        uid.unwrap_or(u32::MAX),
                        gid.unwrap_or(u32::MAX),
                    )
                };
                if r != 0 {
                    return Err(Errno::from(std::io::Error::last_os_error()));
                }
            }
            if let Some(n) = size {
                s.truncate(ino.0, n)?;
            }
            // Timestamps pass straight through (programs use them, e.g. a
            // garbage collector's grace period); their durability is not
            // modelled.
            if atime.is_some() || mtime.is_some() {
                let ts = |t: Option<TimeOrNow>| match t {
                    None => libc::timespec {
                        tv_sec: 0,
                        tv_nsec: libc::UTIME_OMIT,
                    },
                    Some(TimeOrNow::Now) => libc::timespec {
                        tv_sec: 0,
                        tv_nsec: libc::UTIME_NOW,
                    },
                    Some(TimeOrNow::SpecificTime(t)) => {
                        let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
                        libc::timespec {
                            tv_sec: d.as_secs() as libc::time_t,
                            tv_nsec: d.subsec_nanos() as libc::c_long,
                        }
                    }
                };
                let times = [ts(atime), ts(mtime)];
                let r = unsafe {
                    libc::utimensat(
                        libc::AT_FDCWD,
                        cstr(&s.real(&rel)).as_ptr(),
                        times.as_ptr(),
                        0,
                    )
                };
                if r != 0 {
                    return Err(Errno::from(std::io::Error::last_os_error()));
                }
            }
            s.attr(ino.0)
        })();
        match res {
            Ok(a) => reply.attr(&TTL, &a),
            Err(e) => reply.error(e),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let mut s = self.lock();
        let res = (|| {
            let rel = s.child(parent, name)?;
            fs::create_dir(s.real(&rel)).map_err(Errno::from)?;
            let _ = fs::set_permissions(s.real(&rel), fs::Permissions::from_mode(mode & 0o7777));
            s.log(Op::Mkdir(rel.clone()), &[parent_of(&rel)]);
            let ino = s.ino_for(&rel);
            s.attr(ino)
        })();
        match res {
            Ok(a) => reply.entry(&TTL, &a, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let mut s = self.lock();
        let res = (|| {
            let rel = s.child(parent, name)?;
            let trash = s.move_to_trash(&rel)?;
            s.log(
                Op::Unlink {
                    path: rel.clone(),
                    trash,
                },
                &[parent_of(&rel)],
            );
            s.forget_path(&rel);
            Ok(())
        })();
        match res {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let mut s = self.lock();
        let res = (|| {
            let rel = s.child(parent, name)?;
            fs::remove_dir(s.real(&rel)).map_err(Errno::from)?;
            s.log(Op::Rmdir(rel.clone()), &[parent_of(&rel)]);
            s.forget_path(&rel);
            Ok(())
        })();
        match res {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let mut s = self.lock();
        let res = (|| {
            let from = s.child(parent, name)?;
            let to = s.child(newparent, newname)?;
            if flags.bits() & libc::RENAME_EXCHANGE != 0 {
                return Err(Errno::EINVAL);
            }
            let exists = fs::symlink_metadata(s.real(&to)).is_ok();
            if exists && flags.bits() & libc::RENAME_NOREPLACE != 0 {
                return Err(Errno::EEXIST);
            }
            fs::symlink_metadata(s.real(&from)).map_err(Errno::from)?;
            let displaced = if exists {
                let t = s.move_to_trash(&to)?;
                s.forget_path(&to);
                Some(t)
            } else {
                None
            };
            fs::rename(s.real(&from), s.real(&to)).map_err(Errno::from)?;
            s.move_paths(&from, &to);
            s.log(
                Op::Rename {
                    from: from.clone(),
                    to: to.clone(),
                    displaced,
                },
                &[parent_of(&to)],
            );
            Ok(())
        })();
        match res {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let mut s = self.lock();
        let res = (|| {
            s.file(ino.0)?;
            if flags.0 & libc::O_TRUNC != 0 {
                s.truncate(ino.0, 0)?;
            }
            Ok(())
        })();
        match res {
            Ok(()) => reply.opened(FileHandle(0), FopenFlags::empty()),
            Err(e) => reply.error(e),
        }
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let mut s = self.lock();
        let res = (|| {
            let rel = s.child(parent, name)?;
            let real = s.real(&rel);
            let existed = fs::symlink_metadata(&real).is_ok();
            if existed && flags & libc::O_EXCL != 0 {
                return Err(Errno::EEXIST);
            }
            if !existed {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(mode & 0o7777)
                    .open(&real)
                    .map_err(Errno::from)?;
                s.log(Op::Create(rel.clone()), &[parent_of(&rel)]);
            }
            let ino = s.ino_for(&rel);
            if existed && flags & libc::O_TRUNC != 0 {
                s.truncate(ino, 0)?;
            }
            s.attr(ino)
        })();
        match res {
            Ok(a) => reply.created(&TTL, &a, Generation(0), FileHandle(0), FopenFlags::empty()),
            Err(e) => reply.error(e),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        match self.lock().read(ino.0, offset, size) {
            Ok(d) => reply.data(&d),
            Err(e) => reply.error(e),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        match self.lock().write(ino.0, offset, data) {
            Ok(()) => reply.written(data.len() as u32),
            Err(e) => reply.error(e),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        // close() does not make anything durable.
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.lock().sync_file(ino.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let mut s = self.lock();
        let res = (|| {
            let rel = s.path(ino)?;
            let mut entries = vec![
                (ino.0, FileType::Directory, OsStr::new(".").to_os_string()),
                (ino.0, FileType::Directory, OsStr::new("..").to_os_string()),
            ];
            let mut names: Vec<_> = fs::read_dir(s.real(&rel))
                .map_err(Errno::from)?
                .flatten()
                .map(|e| {
                    (
                        e.file_name(),
                        e.file_type().map(|t| t.is_dir()).unwrap_or(false),
                    )
                })
                .collect();
            names.sort();
            for (n, dir) in names {
                let i = s.ino_for(&rel.join(&n));
                let kind = if dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                };
                entries.push((i, kind, n));
            }
            Ok(entries)
        })();
        match res {
            Ok(entries) => {
                for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(INodeNo(ino), (i + 1) as u64, kind, name) {
                        break;
                    }
                }
                reply.ok()
            }
            Err(e) => reply.error(e),
        }
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let mut s = self.lock();
        match s.path(ino) {
            Ok(p) => {
                s.sync_dir(&p);
                reply.ok()
            }
            Err(e) => reply.error(e),
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let s = self.lock();
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(cstr(&s.backing).as_ptr(), &mut st) } != 0 {
            return reply.error(Errno::from(std::io::Error::last_os_error()));
        }
        reply.statfs(
            st.f_blocks,
            st.f_bfree,
            st.f_bavail,
            st.f_files,
            st.f_ffree,
            st.f_bsize as u32,
            st.f_namemax as u32,
            st.f_frsize as u32,
        );
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: fuser::AccessFlags, reply: ReplyEmpty) {
        reply.ok();
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let usage = "usage: kosha-faultfs <backing-dir> <mountpoint> [--model posix|journal]";
    let backing = fs::canonicalize(args.next().expect(usage)).expect("backing dir");
    let mountpoint = PathBuf::from(args.next().expect(usage));
    let mut model = Model::Posix;
    while let Some(a) = args.next() {
        match (a.as_str(), args.next().as_deref()) {
            ("--model", Some("posix")) => model = Model::Posix,
            ("--model", Some("journal")) => model = Model::Journal,
            _ => panic!("{usage}"),
        }
    }
    let trash = backing.with_file_name(format!(
        "{}.faultfs-trash",
        backing.file_name().unwrap().to_string_lossy()
    ));
    let _ = fs::remove_dir_all(&trash);
    fs::create_dir_all(&trash).expect("trash dir");

    // Handle signals synchronously in this thread; block them everywhere.
    let mut set: libc::sigset_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::sigemptyset(&mut set);
        for sig in [libc::SIGUSR1, libc::SIGUSR2, libc::SIGTERM, libc::SIGINT] {
            libc::sigaddset(&mut set, sig);
        }
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }

    let mut inner = Inner {
        backing,
        trash,
        model,
        paths: HashMap::new(),
        inos: HashMap::new(),
        next_ino: 2,
        files: HashMap::new(),
        dirty: HashMap::new(),
        log: vec![],
        trash_seq: 0,
        stats: Stats::default(),
    };
    inner.paths.insert(1, PathBuf::new());
    inner.inos.insert(PathBuf::new(), 1);
    let state = Arc::new(Mutex::new(inner));
    let mut cfg = Config::default();
    cfg.mount_options = vec![
        MountOption::FSName("kosha-faultfs".into()),
        MountOption::RW,
        MountOption::DefaultPermissions,
    ];
    cfg.acl = SessionACL::All;
    let session =
        fuser::spawn_mount(FaultFs(state.clone()), &mountpoint, &cfg).expect("mount faultfs");
    eprintln!(
        "kosha-faultfs: mounted on {} (model {model:?})",
        mountpoint.display()
    );

    loop {
        let mut sig = 0;
        unsafe { libc::sigwait(&set, &mut sig) };
        let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
        match sig {
            libc::SIGUSR2 => {
                s.sync_all();
                eprintln!("kosha-faultfs: synced everything");
            }
            libc::SIGUSR1 => {
                let report = s.power_cut();
                eprintln!("kosha-faultfs: {report}");
                // Detach while still holding the lock, so no request is
                // served from the undone state.
                unsafe { libc::umount2(cstr(&mountpoint).as_ptr(), libc::MNT_DETACH) };
                std::process::exit(0);
            }
            _ => {
                s.sync_all();
                drop(s);
                drop(session);
                eprintln!("kosha-faultfs: unmounted");
                return;
            }
        }
    }
}
