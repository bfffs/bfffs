// vim: tw=80
//! FUSE filesystem access
// Some conversions are only necessary for one of FreeBSD 11 or 12.  After libc
// makes the switchover, delete this line and clean them up.
#![allow(clippy::useless_conversion)]
#![allow(clippy::unnecessary_cast)]

use std::{
    collections::hash_map::HashMap,
    ffi::{OsStr, OsString},
    num::NonZeroU32,
    os::unix::ffi::OsStrExt,
    pin::Pin,
    slice,
    sync::{Arc, Mutex},
    time::Duration,
};

use bfffs_core::fs::{
    self,
    ExtAttr,
    ExtAttrNamespace,
    FileData,
    FileDataMut,
    SeekWhence,
    Timespec,
};
use bytes::Bytes;
use cfg_if::cfg_if;
use fuse3::{
    raw::{
        reply::{
            DirectoryEntry,
            DirectoryEntryPlus,
            FileAttr,
            ReplyAttr,
            ReplyCreated,
            ReplyData,
            ReplyDirectory,
            ReplyEntry,
            ReplyInit,
            ReplyLSeek,
            ReplyStatFs,
            ReplyWrite,
            ReplyXAttr,
        },
        Filesystem,
        Request,
    },
    FileType,
    SetAttr,
    Timestamp,
};
use futures::{Stream, TryFutureExt, TryStreamExt};

cfg_if! {
    if #[cfg(test)] {
        mod mock;
        use self::mock::MockFs as Fs;
    } else {
        use bfffs_core::fs::Fs;
    }
}

#[cfg(test)]
mod tests;

// TODO: move these definitions into fuse3
pub const FUSE_FALLOC_FL_KEEP_SIZE: u32 = 0x1;
pub const FUSE_FALLOC_FL_PUNCH_HOLE: u32 = 0x2;

/// FUSE's handle to an BFFFS filesystem.  One per mountpoint.
///
/// This object lives in the synchronous domain, and spawns commands into the
/// Tokio domain.
pub struct FuseFs {
    fs:    Arc<Fs>,
    /// Basically a vnode cache for FuseFS.  It must always be in sync with
    /// the real vnode cache in the kernel.  It is an error to drop an entry
    /// from here if its `lookup_count` is non-zero.
    // Note: it is OK to copy an entry from the files cache and drop the guard,
    // because the only methods that ever mutate an entry are:
    // * forget: the kernel will ensure that it isn't called concurrently
    //    with any others.
    // * lookup: only increments lookup_count
    // and the only thing that ever cares about lookup_count is unlink, which
    // only cares about zero vs nonzero.
    // TODO: consider using chashmap instead.
    // NB: lock discipline: lock files before names
    files: Mutex<HashMap<u64, FileDataMut>>,
    /// A private namecache, indexed by the parent inode and the final
    /// component of the path name.
    names: Mutex<HashMap<(u64, OsString), u64>>,
}

impl FuseFs {
    // Allow the kernel to cache attributes and entries for an unlimited amount
    // of time, since all changes will come through the kernel.
    const TTL: Duration = Duration::from_secs(u64::MAX);

    fn cache_file(&self, parent_ino: u64, name: &OsStr, fd: FileDataMut) {
        let name_key = (parent_ino, name.to_owned());
        let old_ino = self.names.lock().unwrap().insert(name_key, fd.ino());
        if old_ino.is_some() {
            // XXX Can reach this panic after a FORGET when NFS looks up "."
            panic!(
                "Create of an existing file: {}/{:?} was {} now {}",
                parent_ino,
                name,
                old_ino.unwrap(),
                fd.ino()
            );
        }
        let mut files_guard = self.files.lock().unwrap();
        // Normally the inode should not be in the files cache at this point.
        // But it may be if:
        // * It's a hard link of a file we've already looked up
        // * The NFS server is performing a lookup for "." or ".."
        // * The NFS server previously performed a lookup for ".", and now it
        //   (or another file system client) is performing a normal lookup for
        //   the same file.
        files_guard
            .entry(fd.ino())
            .and_modify(|ofd| ofd.lookup_count += fd.lookup_count)
            .or_insert(fd);
    }

    fn cache_name(&self, parent_ino: u64, name: &OsStr, ino: u64) {
        let name_key = (parent_ino, name.to_owned());
        assert!(
            self.names.lock().unwrap().insert(name_key, ino).is_none(),
            "Link over an existing file"
        );
    }

    /// Private helper for getattr-like operations
    async fn do_getattr(&self, fd: &FileData) -> Result<FileAttr, i32> {
        match self.fs.getattr(fd).await {
            Ok(attr) => {
                let kind = match attr.mode.file_type() {
                    libc::S_IFIFO => FileType::NamedPipe,
                    libc::S_IFCHR => FileType::CharDevice,
                    libc::S_IFDIR => FileType::Directory,
                    libc::S_IFBLK => FileType::BlockDevice,
                    libc::S_IFREG => FileType::RegularFile,
                    libc::S_IFLNK => FileType::Symlink,
                    libc::S_IFSOCK => FileType::Socket,
                    _ => {
                        panic!(
                            "Unknown file type 0o{:o}",
                            attr.mode.file_type()
                        )
                    }
                };
                let reply_attr = FileAttr {
                    ino: attr.ino,
                    size: attr.size,
                    blocks: attr.bytes << 9,
                    atime: Timestamp::new(attr.atime.sec, attr.atime.nsec),
                    mtime: Timestamp::new(attr.mtime.sec, attr.mtime.nsec),
                    ctime: Timestamp::new(attr.ctime.sec, attr.ctime.nsec),
                    kind,
                    perm: attr.mode.perm(),
                    nlink: attr.nlink as u32,
                    uid: attr.uid,
                    gid: attr.gid,
                    rdev: attr.rdev as u32,
                    blksize: attr.blksize,
                };
                Ok(reply_attr)
            }
            Err(e) => Err(e),
        }
    }

    pub fn new(fs: Arc<Fs>) -> Self {
        FuseFs::from(fs)
    }

    /// Actually send a ReplyEntry
    fn reply_entry(&self, attr: FileAttr) -> ReplyEntry {
        ReplyEntry {
            ttl: Self::TTL,
            attr,
            generation: 0,
        }
    }

    /// Private helper for FUSE methods that return a `ReplyEntry`
    async fn handle_new_entry(
        &self,
        r: Result<FileDataMut, i32>,
        parent_ino: u64,
        name: &OsStr,
    ) -> fuse3::Result<ReplyEntry> {
        // FUSE combines the function of VOP_GETATTR with many other VOPs.
        let fd = r?;
        let r2 = match self.do_getattr(&fd.handle()).await {
            Ok(file_attr) => {
                self.cache_file(parent_ino, name, fd);
                Ok(file_attr)
            }
            Err(e) => {
                self.fs.inactive(fd).await;
                Err(e)
            }
        };
        match r2 {
            Ok(file_attr) => Ok(self.reply_entry(file_attr)),
            Err(e) => Err(e.into()),
        }
    }

    /// Split a packed xattr name of the form "namespace.name" into its
    /// components
    fn split_xattr_name(packed_name: &OsStr) -> (ExtAttrNamespace, &OsStr) {
        // FUSE packs namespace into the name, separated by a "."
        let mut groups =
            packed_name.as_bytes().splitn(2, |&b| b == b'.').take(2);
        let ns_str = OsStr::from_bytes(groups.next().unwrap());
        let ns = if ns_str == "user" {
            ExtAttrNamespace::User
        } else if ns_str == "system" {
            ExtAttrNamespace::System
        } else {
            panic!("Unknown namespace {ns_str:?}")
        };
        let name = OsStr::from_bytes(groups.next().unwrap());
        (ns, name)
    }

    #[allow(clippy::if_same_then_else)]
    fn uncache_name(&self, parent_ino: u64, name: &OsStr) {
        let name_key = (parent_ino, name.to_owned());
        // TODO: handle multiply linked files
        if let Some(_ino) = self.names.lock().unwrap().remove(&name_key) {
            /* FORGET will come separately */
        } else {
            /* Removing uncached entries is OK */
        };
    }
}

#[cfg(test)]
impl Default for FuseFs {
    fn default() -> Self {
        FuseFs::new(Arc::new(Fs::default()))
    }
}

impl Filesystem for FuseFs {
    // TODO: implement readdirplus
    type DirEntryPlusStream<'a> =
        Pin<Box<dyn Stream<Item = fuse3::Result<DirectoryEntryPlus>> + Send + 'a>>;
    type DirEntryStream<'a> =
        Pin<Box<dyn Stream<Item = fuse3::Result<DirectoryEntry>> + Send + 'a>>;

    async fn init(&self, _req: Request) -> fuse3::Result<ReplyInit> {
        Ok(ReplyInit{
            max_write: NonZeroU32::new(1<<24).unwrap()
        })
    }

    // FreeBSD's VOP_CREATE doesn't forward the open(2) flags, so the kernel
    // hardcodes them to O_CREAT | O_RDWR.  O_CREAT is implied by FUSE_CREATE,
    // and O_RDWR doesn't matter to the FS layer, so bfffs ignores those flags.
    async fn create(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _flags: u32,
    ) -> fuse3::Result<ReplyCreated> {
        let parent_fd = self
            .files
            .lock()
            .unwrap()
            .get(&parent)
            .expect("create before lookup of parent directory")
            .handle();

        // FUSE combines the functions of VOP_CREATE and VOP_GETATTR
        // into one.
        let perm = (mode & 0o7777) as u16;
        let fd = self
            .fs
            .create(&parent_fd, name, perm, req.uid, req.gid)
            .await?;
        let r = self.do_getattr(&fd.handle()).await;
        if r.is_ok() {
            self.cache_file(parent, name, fd);
        } else {
            self.fs.inactive(fd).await;
        }
        match r {
            Ok(file_attr) => {
                // The generation number is only used for filesystems exported
                // by NFS, and is only needed if the filesystem reuses deleted
                // inodes.  BFFFS does not reuse deleted inodes.
                let generation = 0;
                Ok(ReplyCreated {
                    ttl: Self::TTL,
                    attr: file_attr,
                    generation,
                    fh: 0,
                    flags: 0,
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn destroy(&self, _req: Request) {
        self.fs.sync().await
    }

    async fn fallocate(
        &self,
        _req: Request,
        ino: u64,
        _fh: u64,
        offs: u64,
        len: u64,
        mode: u32,
    ) -> fuse3::Result<()> {
        if mode != FUSE_FALLOC_FL_KEEP_SIZE | FUSE_FALLOC_FL_PUNCH_HOLE {
            Err(libc::EOPNOTSUPP.into())
        } else {
            let fd = self
                .files
                .lock()
                .unwrap()
                .get(&ino)
                .expect("deallocate before lookup or after forget")
                .handle();
            self.fs
                .deallocate(&fd, offs, len)
                .await
                .map_err(fuse3::Errno::from)
        }
    }

    async fn forget(&self, _req: Request, ino: u64, nlookup: u64) {
        // XXX will FUSE_FORGET ever be sent with nlookup less than the actual
        // lookup count?  Not as far as I know.
        // TODO: figure out how to expire entries from the name cache, too
        if ino == 1 {
            // Special case: since fusefs never does a lookup for the root
            // inode, its FORGETs may be "unmatched"
            return;
        }
        let mut fd = self
            .files
            .lock()
            .unwrap()
            .remove(&ino)
            .expect("Forget before lookup or double-forget");
        fd.lookup_count -= nlookup;
        assert_eq!(fd.lookup_count, 0, "Partial forgets are not yet handled");
        self.fs.inactive(fd).await;
    }

    async fn fsync(
        &self,
        _req: Request,
        ino: u64,
        _fh: u64,
        _datasync: bool,
    ) -> fuse3::Result<()> {
        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("fsync before lookup or after forget")
            .handle();
        self.fs.fsync(&fd).await.map_err(fuse3::Errno::from)
    }

    async fn getattr(
        &self,
        _req: Request,
        ino: u64,
        _fh: Option<u64>,
        _flags: u32,
    ) -> fuse3::Result<ReplyAttr> {
        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("getattr before lookup or after forget")
            .handle();
        match self.do_getattr(&fd).await {
            Ok(attr) => {
                Ok(ReplyAttr {
                    ttl: Self::TTL,
                    attr,
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn getxattr(
        &self,
        _req: Request,
        ino: u64,
        packed_name: &OsStr,
        size: u32,
    ) -> fuse3::Result<ReplyXAttr> {
        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("getxattr before lookup or after forget")
            .handle();
        let (ns, name) = FuseFs::split_xattr_name(packed_name);
        if size == 0 {
            match self.fs.getextattrlen(&fd, ns, name).await {
                Ok(len) => Ok(ReplyXAttr::Size(len)),
                Err(e) => Err(e.into()),
            }
        } else {
            match self.fs.getextattr(&fd, ns, name).await {
                // data copy
                Ok(buf) => {
                    if buf.len() <= size as usize {
                        // XXX Data copy!  see
                        // https://github.com/Sherlock-Holo/fuse3/issues/9
                        let bytes = Bytes::copy_from_slice(&buf[..]);
                        Ok(ReplyXAttr::Data(bytes))
                    } else {
                        Err(libc::ERANGE.into())
                    }
                }
                Err(e) => Err(e.into()),
            }
        }
    }

    async fn link(
        &self,
        _req: Request,
        ino: u64,
        parent: u64,
        name: &OsStr,
    ) -> fuse3::Result<ReplyEntry> {
        let (parent_fd, fd) = {
            let guard = self.files.lock().unwrap();
            let parent_fd = guard
                .get(&parent)
                .expect("link before lookup or after forget")
                .handle();
            let fd = guard
                .get(&ino)
                .expect("link before lookup or after forget")
                .handle();
            (parent_fd, fd)
        };
        let ino = fd.ino();
        match self.fs.link(&parent_fd, &fd, name).await {
            Ok(_) => {
                match self.do_getattr(&fd).await {
                    Ok(file_attr) => {
                        self.cache_name(parent, name, ino);
                        Ok(self.reply_entry(file_attr))
                    }
                    Err(e) => Err(e.into()),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn lookup(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
    ) -> fuse3::Result<ReplyEntry> {
        let (oparent_fd, grandparent_fd, oino) = {
            let files_guard = self.files.lock().unwrap();
            let oparent_fd = files_guard.get(&parent);
            if oparent_fd.is_none() {
                // An NFS server may call VFS_VGET, which fusefs(4) implements
                // as a lookup for ".".  That may happen even if the parent
                // hasn't been looked up yet.  But for any other name, the
                // parent had better be in the ino cache.
                assert!(
                    name == r".",
                    "lookup of child before lookup of parent"
                );
            }
            let grandparent_fd = oparent_fd
                .and_then(FileDataMut::parent)
                .and_then(|gino| files_guard.get(&gino))
                .map(FileDataMut::handle);
            let names_guard = self.names.lock().unwrap();
            let oino = names_guard.get(&(parent, name.to_owned())).cloned();
            (oparent_fd.map(FileDataMut::handle), grandparent_fd, oino)
        };
        match (oparent_fd, oino) {
            (Some(parent_fd), Some(ino)) => {
                let ofd = self
                    .files
                    .lock()
                    .unwrap()
                    .get(&ino)
                    .map(FileDataMut::handle);
                match ofd {
                    Some(fd) => {
                        // Name and inode are cached
                        match self.do_getattr(&fd).await {
                            Ok(file_attr) => {
                                self.files
                                    .lock()
                                    .unwrap()
                                    .get_mut(&ino)
                                    .unwrap()
                                    .lookup_count += 1;
                                Ok(self.reply_entry(file_attr))
                            }
                            Err(e) => Err(e.into()),
                        }
                    }
                    None => {
                        // Only name is cached
                        let r = self
                            .fs
                            .lookup(grandparent_fd.as_ref(), &parent_fd, name)
                            .and_then(|fd| {
                                async move {
                                    match self.do_getattr(&fd.handle()).await {
                                        Ok(file_attr) => {
                                            let ino = fd.ino();
                                            let mut g =
                                                self.files.lock().unwrap();
                                            assert!(
                                                g.insert(ino, fd).is_none(),
                                                "Inode number reuse detected"
                                            );
                                            Ok(file_attr)
                                        }
                                        Err(e) => {
                                            self.fs.inactive(fd).await;
                                            Err(e)
                                        }
                                    }
                                }
                            })
                            .await;
                        match r {
                            Ok(file_attr) => Ok(self.reply_entry(file_attr)),
                            Err(e) => Err(e.into()),
                        }
                    }
                }
            }
            (Some(parent_fd), None) => {
                // Name is not cached
                let r = self
                    .fs
                    .lookup(grandparent_fd.as_ref(), &parent_fd, name)
                    .await;
                self.handle_new_entry(r, parent, name).await
            }
            (None, _) => {
                // An NFS-style lookup for "."
                let r = self.fs.ilookup(parent).await;
                self.handle_new_entry(r, parent, name).await
            }
        }
    }

    /// Get a list of all of the file's extended attributes
    ///
    /// # Parameters
    ///
    /// - `size`:   Maximum size to return.  If `0`, then `listxattr` will
    ///             return the size of buffer needed, but no data.
    ///
    /// # Returns
    ///
    /// All of the file's extended attributes, concatenated and packed in the
    /// form `<NAMESPACE>.<NAME>\0`.
    async fn listxattr(
        &self,
        _req: Request,
        ino: u64,
        size: u32,
    ) -> fuse3::Result<ReplyXAttr> {
        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("listxattr before lookup or after forget")
            .handle();
        if size == 0 {
            let f = |extattr: &ExtAttr| {
                let name = extattr.name();
                let prefix_len = match extattr.namespace() {
                    ExtAttrNamespace::User => b"user.".len(),
                    ExtAttrNamespace::System => b"system.".len(),
                } as u32;
                prefix_len + name.as_bytes().len() as u32 + 1
            };
            match self.fs.listextattrlen(&fd, f).await {
                Ok(len) => Ok(ReplyXAttr::Size(len)),
                Err(e) => Err(e.into()),
            }
        } else {
            let f = |buf: &mut Vec<u8>, extattr: &ExtAttr| {
                let s = match extattr.namespace() {
                    ExtAttrNamespace::User => &b"user."[..],
                    ExtAttrNamespace::System => &b"system."[..],
                };
                buf.extend_from_slice(s);
                buf.extend_from_slice(extattr.name().as_bytes());
                buf.push(b'\0');
            };
            match self.fs.listextattr(&fd, size, f).await {
                Ok(buf) => {
                    if buf.len() <= size as usize {
                        // data copy
                        let bytes = Bytes::copy_from_slice(&buf[..]);
                        Ok(ReplyXAttr::Data(bytes))
                    } else {
                        Err(libc::ERANGE.into())
                    }
                }
                Err(e) => Err(e.into()),
            }
        }
    }

    async fn lseek(
        &self,
        _req: Request,
        ino: u64,
        _fh: u64,
        offset: u64,
        whence: u32,
    ) -> fuse3::Result<ReplyLSeek> {
        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("lseek before lookup or after forget")
            .handle();
        let whence = match whence as i32 {
            libc::SEEK_HOLE => SeekWhence::Hole,
            libc::SEEK_DATA => SeekWhence::Data,
            _ => return Err(libc::EINVAL.into()),
        };
        self.fs
            .lseek(&fd, offset, whence)
            .map_ok(|offset| ReplyLSeek { offset })
            .map_err(fuse3::Errno::from)
            .await
    }

    async fn mkdir(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> fuse3::Result<ReplyEntry> {
        let parent_fd = self
            .files
            .lock()
            .unwrap()
            .get(&parent)
            .expect("mkdir of child before lookup of parent")
            .handle();
        let perm = (mode & 0o7777) as u16;
        let r = self
            .fs
            .mkdir(&parent_fd, name, perm, req.uid, req.gid)
            .await;
        self.handle_new_entry(r, parent, name).await
    }

    async fn mknod(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> fuse3::Result<ReplyEntry> {
        let parent_fd = self
            .files
            .lock()
            .unwrap()
            .get(&parent)
            .expect("mknod of child before lookup of parent")
            .handle();
        let perm = (mode & 0o7777) as u16;
        let r = match mode as u16 & libc::S_IFMT {
            libc::S_IFIFO => {
                self.fs
                    .mkfifo(&parent_fd, name, perm, req.uid, req.gid)
                    .await
            }
            libc::S_IFCHR => {
                self.fs
                    .mkchar(
                        &parent_fd,
                        name,
                        perm,
                        req.uid,
                        req.gid,
                        rdev.into(),
                    )
                    .await
            }
            libc::S_IFBLK => {
                self.fs
                    .mkblock(
                        &parent_fd,
                        name,
                        perm,
                        req.uid,
                        req.gid,
                        rdev.into(),
                    )
                    .await
            }
            libc::S_IFSOCK => {
                self.fs
                    .mksock(&parent_fd, name, perm, req.uid, req.gid)
                    .await
            }
            _ => Err(libc::EOPNOTSUPP),
        };
        self.handle_new_entry(r, parent, name).await
    }

    async fn read(
        &self,
        _req: Request,
        ino: u64,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> fuse3::Result<ReplyData> {
        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("read before lookup or after forget")
            .handle();
        match self.fs.read(&fd, offset, size as usize).await {
            Ok(sglist) => {
                // Vectored data requires an additional data copy, thanks to
                // https://github.com/Sherlock-Holo/fuse3/issues/13
                let mut v = Vec::with_capacity(size as usize);
                for iov in sglist.into_iter() {
                    v.extend_from_slice(&iov[..]);
                }
                Ok(ReplyData::from(Bytes::from(v)))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        ino: u64,
        _fh: u64,
        soffset: i64,
    ) -> fuse3::Result<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("read before lookup or after forget")
            .handle();
        let stream = self
            .fs
            .readdir(&fd, soffset)
            .map_ok(move |(dirent, offset)| {
                assert!(offset as u64 >= soffset as u64);
                let kind = match dirent.d_type {
                    libc::DT_FIFO => FileType::NamedPipe,
                    libc::DT_CHR => FileType::CharDevice,
                    libc::DT_DIR => FileType::Directory,
                    libc::DT_BLK => FileType::BlockDevice,
                    libc::DT_REG => FileType::RegularFile,
                    libc::DT_LNK => FileType::Symlink,
                    libc::DT_SOCK => FileType::Socket,
                    e => panic!("Unknown dirent type {e:?}"),
                };
                let nameptr = dirent.d_name.as_ptr() as *const u8;
                let namelen = usize::from(dirent.d_namlen);
                let name = unsafe { slice::from_raw_parts(nameptr, namelen) };
                DirectoryEntry {
                    inode: dirent.d_fileno.into(),
                    kind,
                    name: OsStr::from_bytes(name).to_owned(),
                    offset,
                }
            })
            .map_err(fuse3::Errno::from);
        let entries = Box::pin(stream);
        Ok(ReplyDirectory { entries })
    }

    async fn readlink(
        &self,
        _req: Request,
        ino: u64,
    ) -> fuse3::Result<ReplyData> {
        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("readlink before lookup or after forget")
            .handle();
        match self.fs.readlink(&fd).await {
            Ok(path) => {
                Ok(ReplyData::from(Bytes::copy_from_slice(path.as_bytes())))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn removexattr(
        &self,
        _req: Request,
        ino: u64,
        packed_name: &OsStr,
    ) -> fuse3::Result<()> {
        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("removexattr before lookup or after forget")
            .handle();
        let (ns, name) = FuseFs::split_xattr_name(packed_name);
        self.fs
            .deleteextattr(&fd, ns, name)
            .map_err(fuse3::Errno::from)
            .await
    }

    async fn rename(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
    ) -> fuse3::Result<()> {
        let (src_ino, new_ino) = {
            let names_guard = self.names.lock().unwrap();
            let src_ino = *names_guard
                .get(&(parent, name.to_owned()))
                .expect("rename before lookup or after forget of source");
            let new_ino =
                names_guard.get(&(newparent, newname.to_owned())).cloned();
            (src_ino, new_ino)
        };
        let (parent_fd, newparent_fd, newname_key, src_fd) = {
            let files_guard = self.files.lock().unwrap();
            let parent_fd = files_guard
                .get(&parent)
                .expect("rename before lookup or after forget of parent")
                .handle();
            let newparent_fd = files_guard
                .get(&newparent)
                .expect("rename before lookup or after forget of new parent")
                .handle();
            let newname_key = (newparent, newname.to_owned());

            // Dirloop check
            let src_fd = files_guard
                .get(&src_ino)
                .expect("rename before lookup or after forget of source")
                .handle();
            let mut fd = files_guard
                .get(&newparent)
                .expect("Uncached destination directory");
            loop {
                match fd.parent() {
                    None => {
                        // Root directory, or not a directory
                        break;
                    }
                    Some(ino) if src_ino == ino => {
                        // Dirloop detected!
                        return Err(libc::EINVAL.into());
                    }
                    // Keep recursing
                    _ => {
                        fd = files_guard
                            .get(&fd.parent().unwrap())
                            .expect("Uncached parent directory");
                    }
                }
            }
            (parent_fd, newparent_fd, newname_key, src_fd)
        };

        match self
            .fs
            .rename(&parent_fd, &src_fd, name, &newparent_fd, new_ino, newname)
            .await
        {
            Ok(ino) => {
                assert_eq!(ino, src_ino);
                // Remove the cached destination file, if any
                self.uncache_name(newparent, newname);
                // Remove the cached source name (but not inode)
                let name_key = (parent, name.to_owned());
                {
                    let mut names_guard = self.names.lock().unwrap();
                    let cache_ino = names_guard.remove(&name_key).unwrap();
                    assert_eq!(ino, cache_ino);
                    // And cache it in the new location
                    names_guard.insert(newname_key, cache_ino);
                }
                // Reparent the moved file
                if let Some(fd) = self.files.lock().unwrap().get_mut(&ino) {
                    fd.reparent(newparent);
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn rmdir(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
    ) -> fuse3::Result<()> {
        let parent_fd = self
            .files
            .lock()
            .unwrap()
            .get(&parent)
            .expect("rmdir before lookup or after forget")
            .handle();
        match self.fs.rmdir(&parent_fd, name).await {
            Ok(()) => {
                self.uncache_name(parent, name);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn setattr(
        &self,
        _req: Request,
        ino: u64,
        _fh: Option<u64>,
        set_attr: SetAttr,
    ) -> fuse3::Result<ReplyAttr> {
        fn stamp2spec(ts: Timestamp) -> Timespec {
            Timespec::new(ts.sec, ts.nsec)
        }

        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("setattr before lookup or after forget")
            .handle();
        let attr = fs::SetAttr {
            perm:      set_attr.mode.map(|m| m & 0o7777),
            uid:       set_attr.uid,
            gid:       set_attr.gid,
            size:      set_attr.size,
            atime:     set_attr.atime.map(stamp2spec),
            mtime:     set_attr.mtime.map(stamp2spec),
            ctime:     set_attr.ctime.map(stamp2spec),
            birthtime: None,
            flags:     None,
        };
        self.fs.setattr(&fd, attr).await?;
        let r = self.do_getattr(&fd).await;
        // FUSE combines the functions of VOP_SETATTR and VOP_GETATTR
        // into one.
        match r {
            Ok(attr) => {
                Ok(ReplyAttr {
                    ttl: Self::TTL,
                    attr,
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn setxattr(
        &self,
        _req: Request,
        ino: u64,
        packed_name: &OsStr,
        value: &[u8],
        _flags: u32,
        _position: u32,
    ) -> fuse3::Result<()> {
        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("setxattr before lookup or after forget")
            .handle();
        let (ns, name) = FuseFs::split_xattr_name(packed_name);
        match self.fs.setextattr(&fd, ns, name, value).await {
            Ok(()) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn statfs(
        &self,
        _req: Request,
        _ino: u64,
    ) -> fuse3::Result<ReplyStatFs> {
        match self.fs.statvfs().await {
            Ok(statvfs) => {
                Ok(ReplyStatFs {
                    blocks:  statvfs.f_blocks,
                    bfree:   statvfs.f_bfree,
                    bavail:  statvfs.f_bavail,
                    files:   statvfs.f_files,
                    ffree:   statvfs.f_ffree,
                    bsize:   statvfs.f_bsize as u32,
                    namelen: statvfs.f_namemax as u32,
                    frsize:  statvfs.f_frsize as u32,
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn symlink(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        link: &OsStr,
    ) -> fuse3::Result<ReplyEntry> {
        // Weirdly, FUSE doesn't supply the symlink's mode.  Use a sensible
        // default.
        let perm = 0o755;
        let parent_fd = self
            .files
            .lock()
            .unwrap()
            .get(&parent)
            .expect("symlink before lookup or after forget")
            .handle();
        let r = self
            .fs
            .symlink(&parent_fd, name, perm, req.uid, req.gid, link)
            .await;
        self.handle_new_entry(r, parent, name).await
    }

    async fn unlink(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
    ) -> fuse3::Result<()> {
        let oino = self
            .names
            .lock()
            .unwrap()
            .get(&(parent, name.to_owned()))
            .cloned();
        let r = match oino {
            None => {
                // Name has lookup count of 0; therefore it must not be open
                let parent_fd = self
                    .files
                    .lock()
                    .unwrap()
                    .get(&parent)
                    .expect("unlink before lookup or after forget")
                    .handle();
                self.fs.unlink(&parent_fd, None, name).await
            }
            Some(ino) => {
                let (parent_fd, fd) = {
                    let fguard = self.files.lock().unwrap();
                    let parent_fd = fguard
                        .get(&parent)
                        .expect("unlink before lookup or after forget")
                        .handle();
                    let fd = fguard.get(&ino).map(FileDataMut::handle);
                    (parent_fd, fd)
                };
                self.fs.unlink(&parent_fd, fd.as_ref(), name).await
            }
        };
        match r {
            Ok(()) => {
                self.uncache_name(parent, name);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn write(
        &self,
        _req: Request,
        ino: u64,
        _fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyWrite> {
        let fd = self
            .files
            .lock()
            .unwrap()
            .get(&ino)
            .expect("write before lookup or after forget")
            .handle();
        match self.fs.write(&fd, offset, data, flags).await {
            Ok(lsize) => Ok(ReplyWrite { written: lsize }),
            Err(e) => Err(e.into()),
        }
    }
}

impl From<Arc<Fs>> for FuseFs {
    fn from(fs: Arc<Fs>) -> Self {
        let mut files = HashMap::default();
        let names = HashMap::default();
        // fusefs(5) looks up the root inode (see fuse_vfsop_root).  Prepopulate
        // it into the cache.
        files.insert(1, fs.root());
        FuseFs {
            fs,
            files: Mutex::new(files),
            names: Mutex::new(names),
        }
    }
}
