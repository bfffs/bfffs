// vim: tw=80
//! FUSE filesystem access

use async_trait::async_trait;
use bfffs_core::database::*;
use bfffs_core::{
    RID,
    database::TreeID,
    fs::{self, ExtAttr, ExtAttrNamespace, FileData, Timespec}
};
use bytes::Bytes;
use cfg_if::cfg_if;
use fuse3::{
    FileType,
    SetAttr,
    Timestamp,
    raw::{
        Filesystem,
        reply::{
            DirectoryEntry, DirectoryEntryPlus, FileAttr, ReplyAttr,
            ReplyCreated, ReplyData, ReplyDirectory, ReplyEntry, ReplyStatFs,
            ReplyWrite, ReplyXAttr,
        },
        Request
    }
};
use futures::{Stream, TryFutureExt, TryStreamExt};
use std::{
    collections::hash_map::HashMap,
    ffi::{OsString, OsStr},
    os::unix::ffi::OsStrExt,
    pin::Pin,
    slice,
    sync::{Arc, Mutex},
    time::Duration
};

cfg_if! {
    if #[cfg(test)] {
        mod mock;
        use self::mock::MockFs as Fs;
    } else {
        use bfffs_core::fs::Fs;
    }
}

/// FUSE's handle to an BFFFS filesystem.  One per mountpoint.
///
/// This object lives in the synchronous domain, and spawns commands into the
/// Tokio domain.
pub struct FuseFs {
    fs: Fs,
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
    files: Mutex<HashMap<u64, FileData>>,
    /// A private namecache, indexed by the parent inode and the final
    /// component of the path name.
    names: Mutex<HashMap<(u64, OsString), u64>>
}

impl FuseFs {
    // Allow the kernel to cache attributes and entries for an unlimited amount
    // of time, since all changes will come through the kernel.
    const TTL: Duration = Duration::from_secs(u64::MAX);

    fn cache_file(&self, parent_ino: u64, name: &OsStr, fd: FileData) {
        let name_key = (parent_ino, name.to_owned());
        assert!(self.names.lock().unwrap().insert(name_key, fd.ino()).is_none(),
            "Create of an existing file");
        assert!(self.files.lock().unwrap().insert(fd.ino(), fd).is_none(),
            "Inode number reuse detected");
    }

    fn cache_name(&self, parent_ino: u64, name: &OsStr, ino: u64) {
        let name_key = (parent_ino, name.to_owned());
        assert!(self.names.lock().unwrap().insert(name_key, ino).is_none(),
            "Link over an existing file");
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
                    _ => panic!("Unknown file type 0o{:o}",
                                attr.mode.file_type())
                };
                // The generation number is only used for filesystems exported
                // by NFS, and is only needed if the filesystem reuses deleted
                // inodes.  BFFFS does not reuse deleted inodes.
                let generation = 0;
                let reply_attr = FileAttr {
                    ino: attr.ino,
                    size: attr.size,
                    blocks: attr.blocks,
                    atime: Timestamp::new(attr.atime.sec, attr.atime.nsec),
                    mtime: Timestamp::new(attr.mtime.sec, attr.mtime.nsec),
                    ctime: Timestamp::new(attr.ctime.sec, attr.ctime.nsec),
                    kind,
                    perm: attr.mode.perm(),
                    nlink: attr.nlink as u32,
                    uid: attr.uid,
                    gid: attr.gid,
                    rdev: attr.rdev,
                    blksize: attr.blksize,
                    generation
                };
                Ok(reply_attr)
            },
            Err(e) => Err(e)
        }
    }

    pub async fn new(database: Arc<Database>, tree: TreeID)
        -> Self
    {
        let fs = Fs::new(database, tree).await;
        FuseFs::from(fs)
    }

    /// Actually send a ReplyEntry
    fn reply_entry(&self, attr: FileAttr) -> ReplyEntry {
        ReplyEntry { ttl: Self::TTL, attr, generation: 0 }
    }

    /// Private helper for FUSE methods that return a `ReplyEntry`
    async fn handle_new_entry(&self, r: Result<FileData, i32>, parent_ino: u64,
                        name: &OsStr) -> fuse3::Result<ReplyEntry>
    {
        // FUSE combines the function of VOP_GETATTR with many other VOPs.
        let fd = r?;
        let r2 = match self.do_getattr(&fd).await {
            Ok(file_attr) => {
                self.cache_file(parent_ino, name, fd);
                Ok(file_attr)
            },
            Err(e) => {
                self.fs.inactive(fd).await;
                Err(e)
            }
        };
        match r2 {
            Ok(file_attr) => Ok(self.reply_entry(file_attr)),
            Err(e) => Err(e.into())
        }
    }

    /// Split a packed xattr name of the form "namespace.name" into its
    /// components
    fn split_xattr_name(packed_name: &OsStr) -> (ExtAttrNamespace, &OsStr) {
        // FUSE packs namespace into the name, separated by a "."
        let mut groups = packed_name.as_bytes()
            .splitn(2, |&b| b == b'.')
            .take(2);
        let ns_str = OsStr::from_bytes(groups.next().unwrap());
        let ns = if ns_str == "user" {
            ExtAttrNamespace::User
        } else if ns_str == "system" {
            ExtAttrNamespace::System
        } else {
            panic!("Unknown namespace {:?}", ns_str)
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

#[async_trait]
impl Filesystem for FuseFs {
    type DirEntryStream = Pin<Box<dyn Stream<Item = fuse3::Result<DirectoryEntry>> + Send>>;
    // TODO: implement readdirplus
    type DirEntryPlusStream = Pin<Box<dyn Stream<Item = fuse3::Result<DirectoryEntryPlus>> + Send>>;

    async fn init(&self, _req: Request) -> fuse3::Result<()> {
        Ok(())
    }

    // FreeBSD's VOP_CREATE doesn't forward the open(2) flags, so the kernel
    // hardcodes them to O_CREAT | O_RDWR.  O_CREAT is implied by FUSE_CREATE,
    // and O_RDWR doesn't matter to the FS layer, so bfffs ignores those flags.
    async fn create(&self, req: Request, parent: u64, name: &OsStr,
              mode: u32, _flags: u32) -> fuse3::Result<ReplyCreated>
    {
        let parent_fd = *self.files.lock().unwrap().get(&parent)
            .expect("create before lookup of parent directory");

        // FUSE combines the functions of VOP_CREATE and VOP_GETATTR
        // into one.
        let perm = (mode & 0o7777) as u16;
        let fd = self.fs.create(&parent_fd, name, perm, req.uid, req.gid)
            .await?;
        let r = self.do_getattr(&fd).await;
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
                    flags: 0
                })
            },
            Err(e) => Err(e.into())
        }
    }

    async fn destroy(&self, _req: Request) {
        self.fs.sync().await
    }

    async fn forget(&self, _req: Request, ino: u64, nlookup: u64) {
        // XXX will FUSE_FORGET ever be sent with nlookup less than the actual
        // lookup count?  Not as far as I know.
        // TODO: figure out how to expire entries from the name cache, too
        let mut fd = self.files.lock().unwrap().remove(&ino)
            .expect("Forget before lookup or double-forget");
        fd.lookup_count -= nlookup;
        assert_eq!(fd.lookup_count, 0, "Partial forgets are not yet handled");
        self.fs.inactive(fd).await;
    }

    async fn fsync(&self, _req: Request, ino: u64, _fh: u64, _datasync: bool)
        -> fuse3::Result<()>
    {
        let fd = *self.files.lock().unwrap().get(&ino)
            .expect("fsync before lookup or after forget");
        self.fs.fsync(&fd).await
            .map_err(fuse3::Errno::from)
    }

    async fn getattr(&self, _req: Request, ino: u64, _fh: Option<u64>,
                     _flags: u32)
        -> fuse3::Result<ReplyAttr>
    {
        let fd = *self.files.lock().unwrap().get(&ino)
            .expect("getattr before lookup or after forget");
        match self.do_getattr(&fd).await {
            Ok(attr) => Ok(ReplyAttr{ttl: Self::TTL, attr}),
            Err(e) => Err(e.into())
        }
    }

    async fn getxattr(&self, _req: Request, ino: u64, packed_name: &OsStr,
                size: u32)
        -> fuse3::Result<ReplyXAttr>
    {
        let fd = *self.files.lock().unwrap().get(&ino)
            .expect("getxattr before lookup or after forget");
        let (ns, name) = FuseFs::split_xattr_name(packed_name);
        if size == 0 {
            match self.fs.getextattrlen(&fd, ns, name).await {
                Ok(len) => Ok(ReplyXAttr::Size(len)),
                Err(e) => Err(e.into())
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
                },
                Err(e) => Err(e.into())
            }
        }
    }

    async fn link(&self, _req: Request, ino: u64, parent: u64, name: &OsStr)
        -> fuse3::Result<ReplyEntry>
    {
        let (parent_fd, fd) = {
            let guard = self.files.lock().unwrap();
            let parent_fd = *guard.get(&parent)
                .expect("link before lookup or after forget");
            let fd = *guard.get(&ino)
                .expect("link before lookup or after forget");
            (parent_fd, fd)
        };
        let ino = fd.ino();
        match self.fs.link(&parent_fd, &fd, name).await {
            Ok(_) => {
                match self.do_getattr(&fd).await {
                    Ok(file_attr) => {
                        self.cache_name(parent, name, ino);
                        Ok(self.reply_entry(file_attr))
                    },
                    Err(e) => Err(e.into())
                }
            },
            Err(e) => Err(e.into())
        }
    }

    async fn lookup(&self, _req: Request, parent: u64, name: &OsStr)
        -> fuse3::Result<ReplyEntry>
    {
        let (parent_fd, grandparent_fd, oino) = {
            let files_guard = self.files.lock().unwrap();
            let parent_fd = *(files_guard.get(&parent)
                .expect("lookup of child before lookup of parent"));
            let grandparent_fd = files_guard.get(&parent).cloned();
            let names_guard = self.names.lock().unwrap();
            let oino = names_guard.get(&(parent, name.to_owned())).cloned();
            (parent_fd, grandparent_fd, oino)
        };
        match oino {
            Some(ino) => {
                let ofd = self.files.lock().unwrap().get(&ino).cloned();
                match ofd {
                    Some(fd) => {
                        // Name and inode are cached
                        match self.do_getattr(&fd).await {
                            Ok(file_attr) => {
                                self.files.lock().unwrap()
                                    .get_mut(&ino).unwrap().lookup_count += 1;
                                Ok(self.reply_entry(file_attr))
                            },
                            Err(e) => Err(e.into())
                        }
                    },
                    None => {
                        // Only name is cached
                        let r = self.fs.lookup(grandparent_fd.as_ref(),
                            &parent_fd, name)
                        .and_then(|fd| async move {
                            match self.do_getattr(&fd).await {
                                Ok(file_attr) => {
                                    let ino = fd.ino();
                                    let mut g = self.files.lock().unwrap();
                                    assert!(g.insert(ino, fd).is_none(),
                                        "Inode number reuse detected");
                                    Ok(file_attr)
                                },
                                Err(e) => {
                                    self.fs.inactive(fd).await;
                                    Err(e)
                                }
                            }
                        }).await;
                        match r {
                            Ok(file_attr) => Ok(self.reply_entry(file_attr)),
                            Err(e) => Err(e.into())
                        }
                    }
                }
            },
            None => {
                // Name is not cached
                let r = self.fs.lookup(grandparent_fd.as_ref(), &parent_fd,
                    name).await;
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
    async fn listxattr(&self, _req: Request, ino: u64, size: u32)
        -> fuse3::Result<ReplyXAttr>
    {
        let fd = *self.files.lock().unwrap().get(&ino)
            .expect("listxattr before lookup or after forget");
        if size == 0 {
            let f = |extattr: &ExtAttr<RID>| {
                let name = extattr.name();
                let prefix_len = match extattr.namespace() {
                    ExtAttrNamespace::User => b"user.".len(),
                    ExtAttrNamespace::System => b"system.".len(),
                } as u32;
                prefix_len + name.as_bytes().len() as u32 + 1
            };
            match self.fs.listextattrlen(&fd, f).await {
                Ok(len) => Ok(ReplyXAttr::Size(len)),
                Err(e) => Err(e.into())
            }
        } else {
            let f = |buf: &mut Vec<u8>, extattr: &ExtAttr<RID>| {
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
                },
                Err(e) => Err(e.into())
            }
        }
    }

    async fn mkdir(&self, req: Request, parent: u64, name: &OsStr, mode: u32,
                   _umask: u32) -> fuse3::Result<ReplyEntry>
    {
        let parent_fd = *self.files.lock().unwrap().get(&parent)
            .expect("mkdir of child before lookup of parent");
        let perm = (mode & 0o7777) as u16;
        let r = self.fs.mkdir(&parent_fd, name, perm, req.uid, req.gid).await;
        self.handle_new_entry(r, parent, name).await
    }

    async fn mknod(&self, req: Request, parent: u64, name: &OsStr, mode: u32,
             rdev: u32) -> fuse3::Result<ReplyEntry>
    {
        let parent_fd = *self.files.lock().unwrap().get(&parent)
            .expect("mknod of child before lookup of parent");
        let perm = (mode & 0o7777) as u16;
        let r = match mode as u16 & libc::S_IFMT {
            libc::S_IFIFO =>
                self.fs.mkfifo(&parent_fd, name, perm, req.uid, req.gid).await,
            libc::S_IFCHR =>
                self.fs.mkchar(&parent_fd, name, perm, req.uid, req.gid,
                    rdev).await,
            libc::S_IFBLK =>
                self.fs.mkblock(&parent_fd, name, perm, req.uid, req.gid,
                    rdev).await,
            libc::S_IFSOCK =>
                self.fs.mksock(&parent_fd, name, perm, req.uid, req.gid).await,
            _ => Err(libc::EOPNOTSUPP)
        };
        self.handle_new_entry(r, parent, name).await
    }

    async fn read(&self, _req: Request, ino: u64, _fh: u64, offset: u64,
            size: u32) -> fuse3::Result<ReplyData>
    {
        let fd = *self.files.lock().unwrap().get(&ino)
            .expect("read before lookup or after forget");
        match self.fs.read(&fd, offset, size as usize).await {
            Ok(sglist) => {
                // Vectored data requires an additional data copy, thanks to
                // https://github.com/Sherlock-Holo/fuse3/issues/13
                let mut v = Vec::with_capacity(size as usize);
                for iov in sglist.into_iter() {
                    v.extend_from_slice(&iov[..]);
                }
                Ok(ReplyData::from(Bytes::from(v)))
            },
            Err(e) => Err(e.into())
        }
    }

    async fn readdir(&self, _req: Request, ino: u64, _fh: u64, offset: i64)
        -> fuse3::Result<ReplyDirectory<Self::DirEntryStream>>
    {
        let fd = *self.files.lock().unwrap().get(&ino)
            .expect("read before lookup or after forget");
        let stream = self.fs.readdir(&fd, offset)
            .map_ok(|(dirent, _offset)| {
                // XXX BUG.  fuse3 currently doesn't handle the offset value
                // correctly.  TODO: fix it.
                // https://github.com/Sherlock-Holo/fuse3/issues/12
                let kind = match dirent.d_type {
                    libc::DT_FIFO => FileType::NamedPipe,
                    libc::DT_CHR => FileType::CharDevice,
                    libc::DT_DIR => FileType::Directory,
                    libc::DT_BLK => FileType::BlockDevice,
                    libc::DT_REG => FileType::RegularFile,
                    libc::DT_LNK => FileType::Symlink,
                    libc::DT_SOCK => FileType::Socket,
                    e => panic!("Unknown dirent type {:?}", e)
                };
                let nameptr = dirent.d_name.as_ptr() as *const u8;
                let namelen = usize::from(dirent.d_namlen);
                let name = unsafe{slice::from_raw_parts(nameptr, namelen)};
                DirectoryEntry {
                    inode: dirent.d_fileno.into(),
                    kind,
                    name: OsStr::from_bytes(name).to_owned()
                }
            }).map_err(fuse3::Errno::from);
        let entries = Box::pin(stream);
        Ok(ReplyDirectory{
            entries
        })
    }

    async fn readlink(&self, _req: Request, ino: u64)
        -> fuse3::Result<ReplyData>
    {
        let fd = *self.files.lock().unwrap().get(&ino)
            .expect("readlink before lookup or after forget");
        match self.fs.readlink(&fd).await {
            Ok(path) => Ok(ReplyData::from(Bytes::copy_from_slice(path.as_bytes()))),
            Err(e) => Err(e.into())
        }
    }

    async fn removexattr(&self, _req: Request, ino: u64, packed_name: &OsStr)
        -> fuse3::Result<()>
    {
        let fd = *self.files.lock().unwrap().get(&ino)
            .expect("removexattr before lookup or after forget");
        let (ns, name) = FuseFs::split_xattr_name(packed_name);
        self.fs.deleteextattr(&fd, ns, name)
            .map_err(fuse3::Errno::from)
            .await
    }

    async fn rename(&self, _req: Request, parent: u64, name: &OsStr,
        newparent: u64, newname: &OsStr)
        -> fuse3::Result<()>
    {
        let (src_ino, new_ino) = {
            let names_guard = self.names.lock().unwrap();
            let src_ino = *names_guard.get(&(parent, name.to_owned()))
                .expect("rename before lookup or after forget of source");
            let new_ino = names_guard.get(&(newparent, newname.to_owned()))
                .cloned();
            (src_ino, new_ino)
        };
        let (parent_fd, newparent_fd, newname_key, src_fd) = {
            let files_guard = self.files.lock().unwrap();
            let parent_fd = *files_guard.get(&parent)
                .expect("rename before lookup or after forget of parent");
            let newparent_fd = *files_guard.get(&newparent)
                .expect("rename before lookup or after forget of new parent");
            let newname_key = (newparent, newname.to_owned());

            // Dirloop check
            let src_fd = *files_guard.get(&src_ino)
                .expect("rename before lookup or after forget of source");
            let mut fd = *files_guard.get(&newparent)
                .expect("Uncached destination directory");
            loop {
                match fd.parent() {
                    None => {
                        // Root directory, or not a directory
                        break;
                    },
                    Some(ino) if src_ino == ino => {
                        // Dirloop detected!
                        return Err(libc::EINVAL.into());
                    },
                    // Keep recursing
                    _ => {
                        fd = *files_guard.get(&fd.parent().unwrap())
                            .expect("Uncached parent directory");
                    }
                }
            }
            (parent_fd, newparent_fd, newname_key, src_fd)
        };

        match self.fs.rename(&parent_fd, &src_fd, name, &newparent_fd,
            new_ino, newname).await
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
            },
            Err(e) => Err(e.into())
        }
    }

    async fn rmdir(&self, _req: Request, parent: u64, name: &OsStr)
        -> fuse3::Result<()>
    {
        let parent_fd = *self.files.lock().unwrap().get(&parent)
            .expect("rmdir before lookup or after forget");
        match self.fs.rmdir(&parent_fd, name).await {
            Ok(()) => {
                self.uncache_name(parent, name);
                Ok(())
            },
            Err(e) => Err(e.into())
        }
    }

    async fn setattr(&self, _req: Request, ino: u64, _fh: Option<u64>,
               set_attr: SetAttr)
        -> fuse3::Result<ReplyAttr>
    {
        fn stamp2spec(ts: Timestamp) -> Timespec {
            Timespec::new(ts.sec, ts.nsec)
        }

        let fd = *self.files.lock().unwrap().get(&ino)
            .expect("setattr before lookup or after forget");
        let attr = fs::SetAttr {
            perm: set_attr.mode.map(|m| (m & 0o7777) as u16),
            uid: set_attr.uid,
            gid: set_attr.gid,
            size: set_attr.size,
            atime: set_attr.atime.map(stamp2spec),
            mtime:  set_attr.mtime.map(stamp2spec),
            ctime:  set_attr.ctime.map(stamp2spec),
            birthtime: None,
            flags: None
        };
        self.fs.setattr(&fd, attr).await?;
        let r = self.do_getattr(&fd).await;
        // FUSE combines the functions of VOP_SETATTR and VOP_GETATTR
        // into one.
        match r {
            Ok(attr) => Ok(ReplyAttr{ttl: Self::TTL, attr}),
            Err(e) => Err(e.into())
        }
    }

    async fn setxattr(&self, _req: Request, ino: u64, packed_name: &OsStr,
                value: &[u8], _flags: u32, _position: u32)
        -> fuse3::Result<()>
    {
        let fd = *self.files.lock().unwrap().get(&ino)
            .expect("setxattr before lookup or after forget");
        let (ns, name) = FuseFs::split_xattr_name(packed_name);
        match self.fs.setextattr(&fd, ns, name, value).await {
            Ok(()) => Ok(()),
            Err(e) => Err(e.into())
        }
    }

    async fn statfs(&self, _req: Request, _ino: u64)
        -> fuse3::Result<ReplyStatFs>
    {
        match self.fs.statvfs().await {
            Ok(statvfs) => Ok(ReplyStatFs {
                blocks: statvfs.f_blocks,
                bfree: statvfs.f_bfree,
                bavail: statvfs.f_bavail,
                files: statvfs.f_files,
                ffree: statvfs.f_ffree,
                bsize: statvfs.f_bsize as u32,
                namelen: statvfs.f_namemax as u32,
                frsize: statvfs.f_frsize as u32,
            }),
            Err(e) => Err(e.into())
        }
    }

    async fn symlink(&self, req: Request, parent: u64, name: &OsStr,
               link: &OsStr) -> fuse3::Result<ReplyEntry>
    {
        // Weirdly, FUSE doesn't supply the symlink's mode.  Use a sensible
        // default.
        let perm = 0o755;
        let parent_fd = *self.files.lock().unwrap().get(&parent)
            .expect("symlink before lookup or after forget");
        let r = self.fs.symlink(&parent_fd, name, perm, req.uid, req.gid, link)
            .await;
        self.handle_new_entry(r, parent, name).await
    }

    async fn unlink(&self, _req: Request, parent: u64, name: &OsStr)
        -> fuse3::Result<()>
    {
        let oino = self.names.lock().unwrap()
            .get(&(parent, name.to_owned()))
            .cloned();
        let r = match oino {
            None => {
                // Name has lookup count of 0; therefore it must not be open
                let parent_fd = *self.files.lock().unwrap().get(&parent)
                    .expect("unlink before lookup or after forget");
                self.fs.unlink(&parent_fd, None, name).await
            },
            Some(ino) => {
                let (parent_fd, fd) = {
                    let fguard = self.files.lock().unwrap();
                    let parent_fd = *fguard.get(&parent)
                        .expect("unlink before lookup or after forget");
                    let fd = fguard.get(&ino).cloned();
                    (parent_fd, fd)
                };
                self.fs.unlink(&parent_fd, fd.as_ref(), name).await
            }
        };
        match r {
            Ok(()) => {
                self.uncache_name(parent, name);
                Ok(())
            },
            Err(e) => Err(e.into())
        }
    }

    async fn write(&self, _req: Request, ino: u64, _fh: u64, offset: u64,
             data: &[u8], flags: u32) -> fuse3::Result<ReplyWrite>
    {
        let fd = *self.files.lock().unwrap().get(&ino)
            .expect("write before lookup or after forget");
        match self.fs.write(&fd, offset, data, flags).await {
            Ok(lsize) => Ok(ReplyWrite{written: lsize}),
            Err(e) => Err(e.into())
        }
    }
}

impl From<Fs> for FuseFs {
    fn from(fs: Fs) -> Self {
        let mut files = HashMap::default();
        let names = HashMap::default();
        // fusefs(5) never seems to lookup the root inode.  Prepopulate it into
        // the cache
        files.insert(1, fs.root());
        FuseFs{
            fs,
            files: Mutex::new(files),
            names: Mutex::new(names)
        }
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {

use super::*;
use bfffs_core::fs::{FileData, GetAttr, Mode};
use futures::FutureExt;
use mockall::{Sequence, predicate};
use std::mem;

fn assert_cached(fusefs: &FuseFs, parent_ino: u64, name: &OsStr, ino: u64) {
    assert!(fusefs.files.lock().unwrap().contains_key(&ino));
    let key = (parent_ino, name.to_owned());
    assert_eq!(Some(&ino), fusefs.names.lock().unwrap().get(&key));
}

fn assert_not_cached(fusefs: &FuseFs, parent_ino: u64, name: &OsStr,
                     ino: Option<u64>)
{
    if let Some(i) = ino {
        assert!(!fusefs.files.lock().unwrap().contains_key(&i));
    }
    let key = (parent_ino, name.to_owned());
    assert!(!fusefs.names.lock().unwrap().contains_key(&key));
}

fn make_mock_fs() -> FuseFs {
    let mut mock_fs = Fs::default();
    mock_fs.expect_root().returning(|| FileData::new_for_tests(None, 1));
    FuseFs::from(mock_fs)
}

mod create {
    use super::*;

    const FLAGS: u32 = (libc::O_CREAT | libc::O_RDWR) as u32;

    #[test]
    fn enotdir() {
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;

        let request = Request { uid: 12345, gid: 12345, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_create()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::always(),
                predicate::always(),
            ).returning(|_, _, _, _, _| Err(libc::ENOTDIR));

        fusefs.files.lock().unwrap()
            .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.create(request, parent, name, mode.into(), FLAGS)
            .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ENOTDIR.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    // The file gets created successfully, but fetching its attributes fails
    #[test]
    fn getattr_eio() {
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let ino = 43;

        let request = Request { uid: 12345, gid: 12345, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_create()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::always(),
                predicate::always(),
            ).returning(move |_, _, _, _, _|
                Ok(FileData::new_for_tests(None, ino))
            );
        fusefs.fs.expect_getattr()
            .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
            .return_const(Err(libc::EIO));
        fusefs.fs.expect_inactive()
            .withf(move |fd| fd.ino() == ino)
            .times(1)
            .return_const(());

        fusefs.files.lock().unwrap()
            .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.create(request, parent, name, mode.into(), FLAGS)
            .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
        assert_not_cached(&fusefs, parent, name, Some(ino));
    }

    #[test]
    fn ok() {
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request { uid, gid, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_create()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::always(),
                predicate::always(),
            ).returning(move |_, _, _, _, _|
                Ok(FileData::new_for_tests(None, ino))
            );
        fusefs.fs.expect_getattr()
            .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
            .return_const(Ok(GetAttr {
                ino,
                size: 0,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFREG),
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                blksize: 131072,
                flags: 0
            }));

        fusefs.files.lock().unwrap()
            .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.create(request, parent, name, mode.into(), FLAGS)
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_cached(&fusefs, parent, name, ino);
    }
}

mod removexattr {
    use super::*;

    #[test]
    fn enoattr() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_deleteextattr()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino() == ino &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| Err(libc::ENOATTR));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.removexattr(request, ino, packed_name)
            .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ENOATTR.into()));
    }

    #[test]
    fn ok() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_deleteextattr()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino() == ino &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| Ok(()));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.removexattr(request, ino, packed_name)
          .now_or_never().unwrap();
        assert_eq!(reply, Ok(()));
    }
}

mod forget {
    use super::*;

    #[test]
    fn one() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"foo.txt");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_lookup()
            .times(1)
            .with(
                predicate::always(),
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name)
            ).returning(move |_, _, _|
                Ok(FileData::new_for_tests(None, ino))
            );
        fusefs.fs.expect_getattr()
            .with( predicate::function(move |fd: &FileData| fd.ino() == ino))
            .times(1)
            .return_const(Ok(GetAttr {
                ino,
                size,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFREG),
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                blksize: 4096,
                flags: 0
            }));
        fusefs.fs.expect_inactive()
            .withf(move |fd| fd.ino() == ino)
            .times(1)
            .return_const(());

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.lookup(request, parent, name)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply.attr.ino, ino);
        // forget does not return a Result
        fusefs.forget(request, ino, 1).now_or_never().unwrap();
        assert!(!fusefs.files.lock().unwrap().contains_key(&ino))
    }
}

mod fsync {
    use super::*;

    #[test]
    fn eio() {
        let ino = 42;
        let fh = 0xdeadbeef;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_fsync()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
            ).return_const(Err(libc::EIO));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.fsync(request, ino, fh, false)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
    }

    #[test]
    fn ok() {
        let ino = 42;
        let fh = 0xdeadbeef;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_fsync()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
            ).return_const(Ok(()));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.fsync(request, ino, fh, false)
          .now_or_never().unwrap();
        assert!(reply.is_ok());
    }
}

mod getattr {
    use super::*;

    #[test]
    fn enoent() {
        let ino = 42;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_getattr()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
            .return_const(Err(libc::ENOENT));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.getattr(request, ino, None, 0)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ENOENT.into()));
    }

    #[test]
    fn ok() {
        let ino = 42;
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_getattr()
            .with( predicate::function(move |fd: &FileData| fd.ino() == ino))
            .times(1)
            .return_const(Ok(GetAttr {
                ino,
                size,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFREG),
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                blksize: 8192,
                flags: 0
            }));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.getattr(request, ino, None, 0)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, size);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 0);
        assert_eq!(reply.attr.blksize, 8192);
    }
}

mod getxattr {
    use super::*;
    use divbuf::DivBufShared;

    #[test]
    fn length_enoattr() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 0;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_getextattrlen()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino() == ino &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| Err(libc::ENOATTR));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.getxattr(request, ino, packed_name, wantsize)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ENOATTR.into()));
    }

    #[test]
    fn length_ok() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 0;
        let size = 16;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_getextattrlen()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino() == ino &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| Ok(size));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.getxattr(request, ino, packed_name, wantsize)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply, ReplyXAttr::Size(size));
    }

    #[test]
    fn value_enoattr() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 80;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_getextattr()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino() == ino &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).return_const(Err(libc::ENOATTR));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.getxattr(request, ino, packed_name, wantsize)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ENOATTR.into()));
    }

    // The FUSE protocol requires a file system to return ERANGE if the
    // attribute's value can't fit in the size requested by the client.
    // That's contrary to how FreeBSD's getextattr(2) works and contrary to how
    // BFFFS's Fs::getextattr works.  It's also hard to trigger during normal
    // use, because the kernel first asks for the size of the attribute.  So
    // during normal use, this error can only be the result of a race.
    #[test]
    fn value_erange() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 16;
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_getextattr()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino() == ino &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| {
                let dbs = DivBufShared::from(&v[..]);
                Ok(dbs.try_const().unwrap())
            });

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.getxattr(request, ino, packed_name, wantsize)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ERANGE.into()));
    }

    #[test]
    fn value_ok() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"user.md5");
        let wantsize = 80;
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_getextattr()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino() == ino &&
                *ns == ExtAttrNamespace::User &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| {
                let dbs = DivBufShared::from(&v[..]);
                Ok(dbs.try_const().unwrap())
            });

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.getxattr(request, ino, packed_name, wantsize)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply, ReplyXAttr::Data(Bytes::copy_from_slice(v)));
    }
}

mod link {
    use super::*;

    // POSIX stupidly requires link(2) to return EPERM for directories.  EISDIR
    // would've been a better choice
    #[test]
    fn eperm() {
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let ino = 43;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_link()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(name),
            ).return_const(Err(libc::EPERM));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.link(request, ino, parent, name)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    // link completes successfully, but the subsequent getattr does not
    #[test]
    fn getattr_eio() {
        let mut seq = Sequence::new();
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let ino = 43;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_link()
            .times(1)
            .in_sequence(&mut seq)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(name),
            ).return_const(Ok(()));
        fusefs.fs.expect_getattr()
            .with( predicate::function(move |fd: &FileData| fd.ino() == ino))
            .times(1)
            .in_sequence(&mut seq)
            .return_const(Err(libc::EIO));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.link(request, ino, parent, name)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    #[test]
    fn ok() {
        let mut seq = Sequence::new();
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let ino = 43;
        let size = 1024;
        let mode = 0o644;
        let uid = 123;
        let gid = 456;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_link()
            .times(1)
            .in_sequence(&mut seq)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(name),
            ).return_const(Ok(()));
        fusefs.fs.expect_getattr()
            .with( predicate::function(move |fd: &FileData| fd.ino() == ino))
            .times(1)
            .in_sequence(&mut seq)
            .return_const(Ok(GetAttr {
                ino,
                size,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFREG),
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                blksize: 16384,
                flags: 0
            }));


        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.link(request, ino, parent, name)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.blksize, 16384);
        assert_eq!(reply.attr.rdev, 0);
        assert_cached(&fusefs, parent, name, ino);
    }
}

mod listxattr {
    use super::*;
    use bfffs_core::fs_tree::{InlineExtAttr, InlineExtent};

    #[test]
    fn length_eperm() {
        let ino = 42;
        let wantsize = 0;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_listextattrlen()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::always()
            ).returning(|_ino, _f| Err(libc::EPERM));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.listxattr(request, ino, wantsize)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
    }

    #[test]
    fn length_ok() {
        let ino = 42;
        let wantsize = 0;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_listextattrlen()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::always()
            ).returning(|_ino, f| {
                Ok(
                    f(&ExtAttr::Inline(InlineExtAttr {
                        namespace: ExtAttrNamespace::System,
                        name: OsString::from("md5"),
                        extent: InlineExtent::default()
                    })) +
                    f(&ExtAttr::Inline(InlineExtAttr {
                        namespace: ExtAttrNamespace::User,
                        name: OsString::from("icon"),
                        extent: InlineExtent::default()
                    }))
                )
            });

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.listxattr(request, ino, wantsize)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply, ReplyXAttr::Size(21));
    }

    #[test]
    fn list_eperm() {
        let ino = 42;
        let wantsize = 1024;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_listextattr()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(wantsize),
                predicate::always()
            ).returning(|_ino, _size, _f| Err(libc::EPERM));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.listxattr(request, ino, wantsize)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
    }

    // The list of attributes doesn't fit in the space requested.  This is most
    // likely due to a race; an attribute was added after the kernel requested
    // the size of the attribute list.
    #[test]
    fn list_erange() {
        let ino = 42;
        let wantsize = 10;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_listextattr()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(wantsize),
                predicate::always()
            ).returning(|_ino, wantsize, f| {
                let mut buf = Vec::with_capacity(wantsize as usize);
                let md5 = ExtAttr::Inline(InlineExtAttr {
                    namespace: ExtAttrNamespace::System,
                    name: OsString::from("md5"),
                    extent: InlineExtent::default()
                });
                let icon = ExtAttr::Inline(InlineExtAttr {
                    namespace: ExtAttrNamespace::User,
                    name: OsString::from("icon"),
                    extent: InlineExtent::default()
                });
                f(&mut buf, &md5);
                f(&mut buf, &icon);
                Ok(buf)
            });

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.listxattr(request, ino, wantsize)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ERANGE.into()));
    }

    #[test]
    fn list_ok() {
        let ino = 42;
        let wantsize = 1024;
        let expected = b"system.md5\0user.icon\0";

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_listextattr()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(wantsize),
                predicate::always()
            ).returning(|_ino, wantsize, f| {
                let mut buf = Vec::with_capacity(wantsize as usize);
                let md5 = ExtAttr::Inline(InlineExtAttr {
                    namespace: ExtAttrNamespace::System,
                    name: OsString::from("md5"),
                    extent: InlineExtent::default()
                });
                let icon = ExtAttr::Inline(InlineExtAttr {
                    namespace: ExtAttrNamespace::User,
                    name: OsString::from("icon"),
                    extent: InlineExtent::default()
                });
                f(&mut buf, &md5);
                f(&mut buf, &icon);
                Ok(buf)
            });

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.listxattr(request, ino, wantsize)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply, ReplyXAttr::Data(Bytes::copy_from_slice(expected)));
    }
}

mod lookup {
    use super::*;

    #[test]
    fn cached() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"foo.txt");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_getattr()
            .with( predicate::function(move |fd: &FileData| fd.ino() == ino))
            .times(1)
            .return_const(Ok(GetAttr {
                ino,
                size,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFREG),
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                blksize: 32768,
                flags: 0
            }));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(None, parent));
        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(Some(1), ino));
        fusefs.names.lock().unwrap().insert((parent, name.to_owned()), ino);
        let reply = fusefs.lookup(request, parent, name)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, size);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 0);
        assert_eq!(reply.attr.blksize, 32768);
    }

    #[test]
    fn enoent() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo.txt");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_lookup()
            .times(1)
            .with(
                predicate::always(),
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name)
            ).returning(|_, _, _| Err(libc::ENOENT));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(None, parent));
        let reply = fusefs.lookup(request, parent, name)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ENOENT.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    // The file's name is cached, but its FileData is not
    #[test]
    fn name_cached() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"foo.txt");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_lookup()
            .times(1)
            .with(
                predicate::always(),
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name)
            ).returning(move |_, _, _|
                Ok(FileData::new_for_tests(None, ino))
            );
        fusefs.fs.expect_getattr()
            .with( predicate::function(move |fd: &FileData| fd.ino() == ino))
            .times(1)
            .return_const(Ok(GetAttr {
                ino,
                size,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFREG),
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                blksize: 4096,
                flags: 0
            }));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        fusefs.names.lock().unwrap().insert((parent, name.to_owned()),
                             ino);
        let reply = fusefs.lookup(request, parent, name)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, size);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 0);
        assert_eq!(reply.attr.blksize, 4096);
        assert_cached(&fusefs, parent, name, ino);
    }

    // The file's name is cached, but its FileData is not, and getattr returns
    // an error.  We must not leak the FileData.
    #[test]
    fn name_cached_getattr_io() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"foo.txt");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_lookup()
            .times(1)
            .with(
                predicate::always(),
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name)
            ).returning(move |_, _, _|
                Ok(FileData::new_for_tests(None, ino))
            );
        fusefs.fs.expect_getattr()
            .with( predicate::function(move |fd: &FileData| fd.ino() == ino))
            .times(1)
            .return_const(Err(libc::EIO));
        fusefs.fs.expect_inactive()
            .withf(move |fd| fd.ino() == ino)
            .return_const(());

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        fusefs.names.lock().unwrap().insert((parent, name.to_owned()), ino);
        let reply = fusefs.lookup(request, parent, name)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
        assert!(!fusefs.files.lock().unwrap().contains_key(&ino));
    }

    #[test]
    fn ok() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"foo.txt");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_lookup()
            .times(1)
            .with(
                predicate::always(),
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name)
            ).returning(move |_, _, _|
                Ok(FileData::new_for_tests(None, ino))
            );
        fusefs.fs.expect_getattr()
            .with( predicate::function(move |fd: &FileData| fd.ino() == ino))
            .times(1)
            .return_const(Ok(GetAttr {
                ino,
                size,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFREG),
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                blksize: 4096,
                flags: 0
            }));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.lookup(request, parent, name)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, size);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 0);
        assert_eq!(reply.attr.blksize, 4096);
        assert_cached(&fusefs, parent, name, ino);
    }
}

mod mkdir {
    use super::*;

    #[test]
    fn eperm() {
        let mode: u16 = 0o755;
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request { uid, gid, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_mkdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
            ).returning(|_, _, _, _, _| Err(libc::EPERM));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.mkdir(request, parent, name,
            (libc::S_IFDIR | mode).into(), 0)
            .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    // The mkdir succeeds, but fetching attributes afterwards fails
    #[test]
    fn getattr_eio() {
        let mode: u16 = 0o755;
        let name = OsStr::from_bytes(b"foo");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request { uid, gid, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_mkdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
            ).returning(move |_, _, _, _, _|
                Ok(FileData::new_for_tests(Some(parent), ino))
            );
        fusefs.fs.expect_getattr()
            .times(1)
            .return_const(Err(libc::EIO));
        fusefs.fs.expect_inactive()
            .withf(move |fd| fd.ino() == ino)
            .times(1)
            .return_const(());

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.mkdir(request, parent, name,
            (libc::S_IFDIR | mode).into(), 0)
            .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    #[test]
    fn ok() {
        let mode: u16 = 0o755;
        let name = OsStr::from_bytes(b"foo");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request { uid, gid, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_mkdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
            ).returning(move |_, _, _, _, _|
                Ok(FileData::new_for_tests(Some(parent), ino))
            );
        fusefs.fs.expect_getattr()
            .times(1)
            .return_const(Ok(GetAttr {
                ino,
                size: 0,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFDIR),
                nlink: 1,
                uid,
                gid,
                blksize: 4096,
                rdev: 0,
                flags: 0
            }));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.mkdir(request, parent, name,
            (libc::S_IFDIR | mode).into(), 0)
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::Directory);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 0);
        assert_eq!(reply.attr.blksize, 4096);
        assert_cached(&fusefs, parent, name, ino);
    }
}

mod mknod {
    use super::*;

    #[test]
    fn blk() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;
        let rdev = 69;

        let request = Request { uid, gid, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_mkblock()
            .times(1)
            .in_sequence(&mut seq)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
                predicate::eq(rdev)
            ).returning(move |_, _, _, _, _, _|
                Ok(FileData::new_for_tests(None, ino))
            );
        fusefs.fs.expect_getattr()
            .times(1)
            .in_sequence(&mut seq)
            .return_const(Ok(GetAttr {
                ino,
                size: 0,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFBLK),
                nlink: 1,
                uid,
                gid,
                rdev,
                blksize: 4096,
                flags: 0
            }));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.mknod(request, parent, name,
            (libc::S_IFBLK | mode).into(), rdev)
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::BlockDevice);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 69);
        assert_cached(&fusefs, parent, name, ino);
    }

    #[test]
    fn char() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;
        let rdev = 69;

        let request = Request { uid, gid, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_mkchar()
            .times(1)
            .in_sequence(&mut seq)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
                predicate::eq(rdev)
            ).returning(move |_, _, _, _, _, _|
                Ok(FileData::new_for_tests(None, ino))
            );
        fusefs.fs.expect_getattr()
            .times(1)
            .in_sequence(&mut seq)
            .return_const(Ok(GetAttr {
                ino,
                size: 0,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFCHR),
                nlink: 1,
                uid,
                gid,
                rdev,
                blksize: 4096,
                flags: 0
            }));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.mknod(request, parent, name,
            (libc::S_IFCHR | mode).into(), rdev)
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::CharDevice);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 69);
        assert_cached(&fusefs, parent, name, ino);
    }

    #[test]
    fn eperm() {
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.pipe");
        let parent = 42;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request { uid, gid, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_mkfifo()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
            ).returning(|_, _, _, _, _| Err(libc::EPERM));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.mknod(request, parent, name,
            (libc::S_IFIFO | mode).into(), 0)
            .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    #[test]
    fn fifo() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.pipe");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request { uid, gid, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_mkfifo()
            .times(1)
            .in_sequence(&mut seq)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
            ).returning(move |_, _, _, _, _| Ok(FileData::new_for_tests(None, ino)));
        fusefs.fs.expect_getattr()
            .times(1)
            .in_sequence(&mut seq)
            .return_const(Ok(GetAttr {
                ino,
                size: 0,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFIFO),
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                blksize: 4096,
                flags: 0
            }));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.mknod(request, parent, name,
            (libc::S_IFIFO | mode).into(), 0)
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::NamedPipe);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_cached(&fusefs, parent, name, ino);
    }

    #[test]
    fn sock() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.sock");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request { uid, gid, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_mksock()
            .times(1)
            .in_sequence(&mut seq)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
            ).returning(move |_, _, _, _, _| Ok(FileData::new_for_tests(None, ino)));
        fusefs.fs.expect_getattr()
            .times(1)
            .in_sequence(&mut seq)
            .return_const(Ok(GetAttr {
                ino,
                size: 0,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFSOCK),
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                blksize: 4096,
                flags: 0
            }));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.mknod(request, parent, name,
            (libc::S_IFSOCK | mode).into(), 0)
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::Socket);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_cached(&fusefs, parent, name, ino);
    }
}

mod read {
    use super::*;
    use bfffs_core::SGList;
    use divbuf::*;

    #[test]
    fn eio() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 0;
        let len = 1024;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_read()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(ofs as u64),
                predicate::eq(len as usize),
            ).return_const(Err(libc::EIO));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.read(request, ino, fh, ofs, len)
          .now_or_never().unwrap()
          .err().unwrap();
        assert_eq!(reply, libc::EIO.into());
    }

    // A Read past eof should return nothing
    #[test]
    fn eof() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 2048;
        let len = 1024;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_read()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(ofs as u64),
                predicate::eq(len as usize),
            ).return_const(Ok(SGList::new()));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.read(request, ino, fh, ofs, len)
          .now_or_never().unwrap()
          .unwrap();
        assert!(reply.data.is_empty());
    }

    // A read of one block or fewer
    #[test]
    fn small() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 2048;
        let len = 1024;
        const DATA: &[u8] = &[0u8, 1, 2, 3, 4, 5];

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_read()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(ofs as u64),
                predicate::eq(len as usize),
            ).returning(|_ino, _ofs, _len| {
                let dbs = DivBufShared::from(DATA);
                let db = dbs.try_const().unwrap();
                Ok(vec![db])
            });

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.read(request, ino, fh, ofs, len)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(&reply.data[..], DATA);
    }

    // A large read from multiple blocks will use a scatter-gather list
    #[test]
    fn large() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 2048;
        let len = 1024;
        const DATA0: &[u8] = &[0u8, 1, 2, 3, 4, 5];
        const DATA1: &[u8] = &[6u8, 7, 8, 9, 10, 11];

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_read()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(ofs as u64),
                predicate::eq(len as usize),
            ).returning(|_ino, _ofs, _len| {
                let dbs0 = DivBufShared::from(DATA0);
                let db0 = dbs0.try_const().unwrap();
                let dbs1 = DivBufShared::from(DATA1);
                let db1 = dbs1.try_const().unwrap();
                Ok(vec![db0, db1])
            });

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.read(request, ino, fh, ofs, len)
          .now_or_never().unwrap()
          .unwrap();
        // fuse3 doesn't work with scatter-gather reads; we have to
        // copy the buffers into one
        // https://github.com/Sherlock-Holo/fuse3/issues/13
        assert_eq!(&reply.data[0..6], DATA0);
        assert_eq!(&reply.data[6..12], DATA1);
    }
}

mod readdir {
    use super::*;
    use futures::stream;

    /// A directory containing one file of every file type recognized by
    /// rust-fuse
    // libc's ino type could be either u32 or u64, depending on which
    // version of freebsd we're targeting.
    #[allow(clippy::useless_conversion)]
    #[test]
    fn all_file_types() {
        let fh = 0xdeadbeef;
        // libc's ino type could be either u32 or u64, depending on which
        // version of freebsd we're targeting.
        let ofs = 0;
        let mut dotname = [0; 256];
        dotname[0] = '.' as libc::c_char;
        let dot_ino = 42u32;
        let dot_ofs = 0;
        let mut regname = [0; 256];
        regname[0] = 'r' as libc::c_char;
        let reg_ino = 43u32;
        let reg_ofs = 1;
        let mut charname = [0; 256];
        charname[0] = 'c' as libc::c_char;
        let char_ino = 43u32;
        let char_ofs = 2;
        let mut blockname = [0; 256];
        blockname[0] = 'b' as libc::c_char;
        let block_ino = 43u32;
        let block_ofs = 3;
        let mut pipename = [0; 256];
        pipename[0] = 'p' as libc::c_char;
        let pipe_ino = 43u32;
        let pipe_ofs = 4;
        let mut symlinkname = [0; 256];
        symlinkname[0] = 'l' as libc::c_char;
        let symlink_ino = 43u32;
        let symlink_ofs = 5;
        let mut sockname = [0; 256];
        sockname[0] = 's' as libc::c_char;
        let sock_ino = 43u32;
        let sock_ofs = 6;
        let contents = vec![
            Ok((
                libc::dirent {
                    d_fileno: dot_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type: libc::DT_DIR,
                    d_name: dotname,
                    d_namlen: 1
                },
                dot_ofs
            )),
            Ok((
                libc::dirent {
                    d_fileno: reg_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type: libc::DT_REG,
                    d_name: regname,
                    d_namlen: 1
                },
                reg_ofs
            )),
            Ok((
                libc::dirent {
                    d_fileno: char_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type: libc::DT_CHR,
                    d_name: charname,
                    d_namlen: 1
                },
                char_ofs
            )),
            Ok((
                libc::dirent {
                    d_fileno: block_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type: libc::DT_BLK,
                    d_name: blockname,
                    d_namlen: 1
                },
                block_ofs
            )),
            Ok((
                libc::dirent {
                    d_fileno: pipe_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type: libc::DT_FIFO,
                    d_name: pipename,
                    d_namlen: 1
                },
                pipe_ofs
            )),
            Ok((
                libc::dirent {
                    d_fileno: symlink_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type: libc::DT_LNK,
                    d_name: symlinkname,
                    d_namlen: 1
                },
                symlink_ofs
            )),
            Ok((
                libc::dirent {
                    d_fileno: sock_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type: libc::DT_SOCK,
                    d_name: sockname,
                    d_namlen: 1
                },
                sock_ofs
            )),
        ];

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_readdir()
            .times(1)
            .with(
                predicate::function(move |h: &FileData|
                                    u64::from(dot_ino) == h.ino()
                ),predicate::eq(ofs),
            ).return_once(move |_, _| Box::pin(
                    stream::iter(contents.into_iter())
            ));

        fusefs.files.lock().unwrap()
          .insert(dot_ino.into(),
            FileData::new_for_tests(Some(1), dot_ino.into()));
        let reply = fusefs.readdir(request, dot_ino.into(), fh, ofs)
            .now_or_never().unwrap()    // Result<_>
            .unwrap()                   // ReplyDirectory
            .entries                    // Stream
            .try_collect::<Vec<_>>()    // Future<Result<Vec<_>>>
            .now_or_never().unwrap()    // Result<Vec<_>>
            .unwrap();                  // Vec<_>>
        assert_eq!(reply[0].inode, u64::from(dot_ino));
        assert_eq!(reply[0].kind, FileType::Directory);
        assert_eq!(reply[0].name, OsStr::from_bytes(b"."));
        assert_eq!(reply[1].inode, u64::from(reg_ino));
        assert_eq!(reply[1].kind, FileType::RegularFile);
        assert_eq!(reply[1].name, OsStr::from_bytes(b"r"));
        assert_eq!(reply[2].inode, u64::from(char_ino));
        assert_eq!(reply[2].kind, FileType::CharDevice);
        assert_eq!(reply[2].name, OsStr::from_bytes(b"c"));
        assert_eq!(reply[3].inode, u64::from(block_ino));
        assert_eq!(reply[3].kind, FileType::BlockDevice);
        assert_eq!(reply[3].name, OsStr::from_bytes(b"b"));
        assert_eq!(reply[4].inode, u64::from(pipe_ino));
        assert_eq!(reply[4].kind, FileType::NamedPipe);
        assert_eq!(reply[4].name, OsStr::from_bytes(b"p"));
        assert_eq!(reply[5].inode, u64::from(symlink_ino));
        assert_eq!(reply[5].kind, FileType::Symlink);
        assert_eq!(reply[5].name, OsStr::from_bytes(b"l"));
        assert_eq!(reply[6].inode, u64::from(sock_ino));
        assert_eq!(reply[6].kind, FileType::Socket);
        assert_eq!(reply[6].name, OsStr::from_bytes(b"s"));
        assert_eq!(reply.len(), 7);
}

    #[test]
    fn eio() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 0;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_readdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(ofs),
            ).returning(|_, _| Box::pin(
                    stream::iter(vec![Err(libc::EIO)].into_iter())
            ));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(Some(1), ino));
        let reply = fusefs.readdir(request, ino, fh, ofs)
            .now_or_never().unwrap()  // Result<_>
            .unwrap()                 // ReplyDirectory
            .entries                  // Stream
            .try_collect::<Vec<_>>()  // Future<Result<Vec<_>>>
            .now_or_never().unwrap(); // Result<Vec<_>>
        assert_eq!(reply, Err(libc::EIO.into()));
    }

    /// A directory containing nothing but "." and ".."
    // libc's ino type could be either u32 or u64, depending on which
    // version of freebsd we're targeting.
    #[allow(clippy::useless_conversion)]
    #[test]
    fn empty() {
        let fh = 0xdeadbeef;
        let ino = 42u32;
        let parent = 41u32;
        let ofs = 0;
        let mut dotname = [0; 256];
        dotname[0] = '.' as libc::c_char;
        let mut dotdotname = [0; 256];
        dotdotname[0] = '.' as libc::c_char;
        dotdotname[1] = '.' as libc::c_char;
        let contents = vec![
            Ok((
                libc::dirent {
                    d_fileno: ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type: libc::DT_DIR,
                    d_name: dotname,
                    d_namlen: 1
                },
                0
            )),
            Ok((
                libc::dirent {
                    d_fileno: parent.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type: libc::DT_DIR,
                    d_name: dotdotname,
                    d_namlen: 2
                },
                1
            ))
        ];

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_readdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData|
                                    u64::from(ino) == fd.ino()
                ), predicate::eq(ofs),
            ).return_once(move |_, _| Box::pin(
                    stream::iter(contents.into_iter())
            ));

        fusefs.files.lock().unwrap()
          .insert(ino.into(),
            FileData::new_for_tests(Some(1), ino.into()));
        let reply = fusefs.readdir(request, ino.into(), fh, ofs)
            .now_or_never().unwrap()    // Result<_>
            .unwrap()                   // ReplyDirectory
            .entries                    // Stream
            .try_collect::<Vec<_>>()    // Future<Result<Vec<_>>>
            .now_or_never().unwrap()    // Result<Vec<_>>
            .unwrap();                  // Vec<_>>
        assert_eq!(reply[0].inode, u64::from(ino));
        assert_eq!(reply[0].kind, FileType::Directory);
        assert_eq!(reply[0].name, OsStr::from_bytes(b"."));
        assert_eq!(reply[1].inode, u64::from(parent));
        assert_eq!(reply[1].kind, FileType::Directory);
        assert_eq!(reply[1].name, OsStr::from_bytes(b".."));
        assert_eq!(reply.len(), 2);
    }

    /// If fuse3's internal buffer runs out of space, it will terminate early
    /// and drop the stream.  Nothing bad should happen.
    // libc's ino type could be either u32 or u64, depending on which
    // version of freebsd we're targeting.
    #[allow(clippy::useless_conversion)]
    #[test]
    fn out_of_space() {
        let fh = 0xdeadbeef;
        let ino = 42u32;
        let parent = 41u32;
        let ofs = 0;
        let mut dotname = [0; 256];
        dotname[0] = '.' as libc::c_char;
        let mut dotdotname = [0; 256];
        dotdotname[0] = '.' as libc::c_char;
        dotdotname[1] = '.' as libc::c_char;
        let contents = vec![
            Ok((
                libc::dirent {
                    d_fileno: ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type: libc::DT_DIR,
                    d_name: dotname,
                    d_namlen: 1
                },
                0
            )),
            Ok((
                libc::dirent {
                    d_fileno: parent.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type: libc::DT_DIR,
                    d_name: dotdotname,
                    d_namlen: 2
                },
                1
            ))
        ];

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_readdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData|
                                    u64::from(ino) == fd.ino()
                ), predicate::eq(ofs),
            ).return_once(move |_, _| Box::pin(
                    stream::iter(contents.into_iter())
            ));

        fusefs.files.lock().unwrap()
          .insert(ino.into(),
            FileData::new_for_tests(Some(1), ino.into()));
        let mut entries = fusefs.readdir(request, ino.into(), fh, ofs)
            .now_or_never().unwrap()    // Result<_>
            .unwrap()                   // ReplyDirectory
            .entries;                   // Stream
        let first_dirent = entries.try_next()
            .now_or_never().unwrap()
            .unwrap().unwrap();
        assert_eq!(first_dirent.inode, u64::from(ino));
        assert_eq!(first_dirent.kind, FileType::Directory);
        assert_eq!(first_dirent.name, OsStr::from_bytes(b"."));
        drop(entries);
    }
}

mod readlink {
    use super::*;

    #[test]
    fn enoent() {
        let ino = 42;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_readlink()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
            .return_const(Err(libc::ENOENT));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.readlink(request, ino)
          .now_or_never().unwrap()
          .err()
          .unwrap();
        assert_eq!(reply, libc::ENOENT.into());
    }

    #[test]
    fn ok() {
        let ino = 42;
        let name = OsStr::from_bytes(b"some_file.txt");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_readlink()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
            .return_const(Ok(name.to_owned()));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.readlink(request, ino)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply.data, Bytes::copy_from_slice(name.as_bytes()))
    }
}

mod rename {
    use super::*;

    // Rename a directory
    #[test]
    fn dir() {
        let parent = 42;
        let newparent = 43;
        let ino = 44;
        let name = OsStr::from_bytes(b"foo");
        let newname = OsStr::from_bytes(b"bar");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_rename()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(name),
                predicate::function(move |fd: &FileData| fd.ino() == newparent),
                predicate::eq(None),
                predicate::eq(newname),
            ).return_once(move |_, _, _, _, _, _| Ok(ino));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        fusefs.files.lock().unwrap()
          .insert(newparent,
            FileData::new_for_tests(Some(1), newparent));
        fusefs.names.lock().unwrap().insert((parent, name.to_owned()), ino);
        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(Some(parent), ino));
        let reply = fusefs.rename(request, parent, name, newparent, newname)
          .now_or_never().unwrap();
        assert!(reply.is_ok());
        assert_not_cached(&fusefs, parent, name, None);
        assert_eq!(Some(newparent), fusefs.files.lock().unwrap().get(&ino).unwrap().parent());
    }

    // Rename fails because the src is a directory but the dst is not.
    #[test]
    fn enotdir() {
        let parent = 42;
        let newparent = 43;
        let ino = 44;
        let dst_ino = 45;
        let name = OsStr::from_bytes(b"foo");
        let newname = OsStr::from_bytes(b"bar");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_rename()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(name),
                predicate::function(move |fd: &FileData| fd.ino() == newparent),
                predicate::eq(Some(dst_ino)),
                predicate::eq(newname),
            ).return_const(Err(libc::ENOTDIR));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        fusefs.files.lock().unwrap()
          .insert(newparent,
            FileData::new_for_tests(None, newparent));
        fusefs.names.lock().unwrap().insert((parent, name.to_owned()), ino);
        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(Some(parent), ino));
        fusefs.files.lock().unwrap()
          .insert(dst_ino,
            FileData::new_for_tests(Some(newparent), dst_ino));
        fusefs.names.lock().unwrap().insert((newparent, newname.to_owned()), dst_ino);
        let reply = fusefs.rename(request, parent, name, newparent, newname)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ENOTDIR.into()));
        assert_cached(&fusefs, newparent, newname, dst_ino);
        assert_cached(&fusefs, parent, name, ino);
    }

    // It should not be possible to create directory loops
    #[test]
    fn dirloop() {
        let parent = 42;
        let child = 43;
        let name = OsStr::from_bytes(b"parent");

        let request = Request::default();

        let fusefs = make_mock_fs();

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        fusefs.files.lock().unwrap()
          .insert(child,
            FileData::new_for_tests(Some(parent), child));
        fusefs.names.lock().unwrap().insert((1, name.to_owned()), parent);
        let reply = fusefs.rename(request, 1, name, child, name)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EINVAL.into()));
        assert_not_cached(&fusefs, child, name, None);
        assert_cached(&fusefs, 1, name, parent);
        assert_eq!(Some(1), fusefs.files.lock().unwrap().get(&parent).unwrap().parent());
    }

    // It should not be possible to create directory loops
    #[test]
    fn dirloop_grandchild() {
        let grandparent = 41;
        let parent = 42;
        let child = 43;
        let name = OsStr::from_bytes(b"grandparent");

        let request = Request::default();

        let fusefs = make_mock_fs();

        fusefs.files.lock().unwrap()
          .insert(grandparent,
            FileData::new_for_tests(Some(1), grandparent));
        fusefs.files.lock().unwrap()
          .insert(parent,
            FileData::new_for_tests(Some(grandparent), parent));
        fusefs.files.lock().unwrap()
          .insert(child,
            FileData::new_for_tests(Some(parent), child));
        fusefs.names.lock().unwrap().insert((1, name.to_owned()), grandparent);
        let reply = fusefs.rename(request, 1, name, child, name)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EINVAL.into()));
        assert_not_cached(&fusefs, child, name, None);
        assert_cached(&fusefs, 1, name, grandparent);
        assert_eq!(Some(1), fusefs.files.lock().unwrap().get(&grandparent).unwrap().parent());
    }

    // Rename a regular file
    #[test]
    fn regular_file() {
        let parent = 42;
        let newparent = 43;
        let ino = 44;
        let name = OsStr::from_bytes(b"foo");
        let newname = OsStr::from_bytes(b"bar");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_rename()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(name),
                predicate::function(move |fd: &FileData| fd.ino() == newparent),
                predicate::eq(None),
                predicate::eq(newname),
            ).return_const(Ok(ino));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        fusefs.files.lock().unwrap()
          .insert(newparent,
            FileData::new_for_tests(Some(1), newparent));
        fusefs.names.lock().unwrap().insert((parent, name.to_owned()), ino);
        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.rename(request, parent, name, newparent, newname)
          .now_or_never().unwrap();
        assert!(reply.is_ok());
        assert_not_cached(&fusefs, parent, name, None);
        assert_eq!(None, fusefs.files.lock().unwrap().get(&ino).unwrap().parent());
    }
}

mod rmdir {
    use super::*;

    #[test]
    fn enotdir() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_rmdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name)
            ).returning(move |_, _| Err(libc::ENOTDIR));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.rmdir(request, parent, name)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ENOTDIR.into()));
    }

    #[test]
    fn ok() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_rmdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name)
            ).returning(move |_, _| Ok(()));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.rmdir(request, parent, name)
          .now_or_never().unwrap();
        assert!(reply.is_ok());
        assert_not_cached(&fusefs, parent, name, None);
    }
}

mod setattr {
    use super::*;

    #[test]
    fn eperm() {
        let mode: u16 = 0o644;
        let ino = 42;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_setattr()
            .times(1)
            .withf(move |fd, attr| {
                fd.ino() == ino &&
                attr.size.is_none() &&
                attr.atime.is_none() &&
                attr.mtime.is_none() &&
                attr.ctime.is_none() &&
                attr.birthtime.is_none() &&
                attr.perm == Some(mode) &&
                attr.uid.is_none() &&
                attr.gid.is_none() &&
                attr.flags.is_none()
            }).return_const(Err(libc::EPERM));

        let attr = SetAttr {
            mode: Some(mode),
            .. Default::default()
        };
        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.setattr(request, ino, None, attr)
            .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
    }

    #[test]
    fn chmod() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        let ino = 42;
        let size = 500;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_setattr()
            .times(1)
            .in_sequence(&mut seq)
            .withf(move |fd, attr| {
                fd.ino() == ino &&
                attr.size.is_none() &&
                attr.atime.is_none() &&
                attr.mtime.is_none() &&
                attr.ctime.is_none() &&
                attr.birthtime.is_none() &&
                attr.perm == Some(mode) &&
                attr.uid.is_none() &&
                attr.gid.is_none() &&
                attr.flags.is_none()
            }).return_const(Ok(()));
        fusefs.fs.expect_getattr()
            .times(1)
            .in_sequence(&mut seq)
            .return_const(Ok(GetAttr {
                ino,
                size,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFREG),
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                blksize: 16384,
                flags: 0
            }));

        let attr = SetAttr {
            mode: Some(mode),
            .. Default::default()
        };
        fusefs.files.lock().unwrap()
            .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.setattr(request, ino, None, attr)
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, size);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.blksize, 16384);
    }
}

mod setxattr {
    use super::*;

    #[test]
    fn value_erofs() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_setextattr()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(ExtAttrNamespace::System),
                predicate::eq(OsStr::from_bytes(b"md5")),
                predicate::eq(&v[..])
            ).return_const(Err(libc::EROFS));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.setxattr(request, ino, packed_name, v, 0, 0)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EROFS.into()));
    }

    #[test]
    fn value_ok() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_setextattr()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(ExtAttrNamespace::System),
                predicate::eq(OsStr::from_bytes(b"md5")),
                predicate::eq(&v[..])
            ).return_const(Ok(()));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.setxattr(request, ino, packed_name, v, 0, 0)
          .now_or_never().unwrap();
        assert!(reply.is_ok());
    }
}

mod statfs {
    use super::*;

    #[test]
    fn eio() {
        let ino = 42;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_statvfs()
            .times(1)
            .return_const(Err(libc::EIO));

        let reply = fusefs.statfs(request, ino)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
    }

    #[test]
    fn ok() {
        let ino = 42;

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_statvfs()
            .times(1)
            .return_const(Ok(libc::statvfs {
                f_bavail: 300000,
                f_bfree: 200000,
                f_blocks: 100000,
                f_favail: 30000,
                f_ffree: 20000,
                f_files: 10000,
                f_bsize: 4096,
                f_flag: 0,
                f_frsize: 512,
                f_fsid: 0,
                f_namemax: 1000,
            }));

        let reply = fusefs.statfs(request, ino)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply.blocks, 100000);
        assert_eq!(reply.bfree, 200000);
        assert_eq!(reply.bavail, 300000);
        assert_eq!(reply.files, 10000);
        assert_eq!(reply.ffree, 20000);
        assert_eq!(reply.bsize, 4096);
        assert_eq!(reply.namelen, 1000);
        assert_eq!(reply.frsize, 512);
    }
}

mod symlink {
    use super::*;

    #[test]
    fn eloop() {
        let name = OsStr::from_bytes(b"foo");
        let mode: u16 = 0o755;
        let parent = 42;

        let request = Request { uid: 12345, gid: 12345, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_symlink()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::always(),
                predicate::always(),
                predicate::eq(name),
            ).returning(|_, _, _, _, _, _| Err(libc::ELOOP));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.symlink(request, parent, name, name)
            .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ELOOP.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    #[test]
    fn ok() {
        let name = OsStr::from_bytes(b"foo");
        let mode: u16 = 0o755;
        let parent = 42;
        let ino = 43;
        let uid = 12345;
        let gid = 54321;

        let request = Request { uid, gid, .. Default::default() };

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_symlink()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::eq(name),
                predicate::eq(mode),
                predicate::always(),
                predicate::always(),
                predicate::eq(name),
            ).returning(move |_, _, _, _, _, _|
                Ok(FileData::new_for_tests(None, ino))
            );
        fusefs.fs.expect_getattr()
            .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
            .return_const(Ok(GetAttr {
                ino,
                size: 0,
                blocks: 0,
                atime: Timespec{sec: 0, nsec: 0},
                mtime: Timespec{sec: 0, nsec: 0},
                ctime: Timespec{sec: 0, nsec: 0},
                birthtime: Timespec{sec: 0, nsec: 0},
                mode: Mode(mode | libc::S_IFLNK),
                nlink: 1,
                uid,
                gid,
                blksize: 4096,
                rdev: 0,
                flags: 0
            }));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.symlink(request, parent, name, name)
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::Symlink);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
    }
}

mod unlink {
    use super::*;

    #[test]
    fn eisdir() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_unlink()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::always(),
                predicate::eq(name)
            ).returning(move |_, _, _| Err(libc::EISDIR));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.unlink(request, parent, name)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EISDIR.into()));
    }

    #[test]
    fn ok() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_unlink()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == parent),
                predicate::always(),
                predicate::eq(name)
            ).returning(move |_, _, _| Ok(()));

        fusefs.files.lock().unwrap()
          .insert(parent, FileData::new_for_tests(Some(1), parent));
        let reply = fusefs.unlink(request, parent, name)
          .now_or_never().unwrap();
        assert!(reply.is_ok());
        assert_not_cached(&fusefs, parent, name, None);
    }
}

mod write {
    use super::*;

    #[test]
    fn erofs() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 0;
        const DATA: &[u8] = &[0u8, 1, 2, 3, 4, 5];

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_write()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(ofs as u64),
                predicate::eq(DATA),
                predicate::always()
            ).return_const(Err(libc::EROFS));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.write(request, ino, fh, ofs, DATA, 0)
          .now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EROFS.into()));
    }

    // A read of one block or fewer
    #[test]
    fn ok() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 2048;
        const DATA: &[u8] = &[0u8, 1, 2, 3, 4, 5];

        let request = Request::default();

        let mut fusefs = make_mock_fs();
        fusefs.fs.expect_write()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino() == ino),
                predicate::eq(ofs as u64),
                predicate::eq(DATA),
                predicate::always()
            ).return_const(Ok(DATA.len() as u32));

        fusefs.files.lock().unwrap()
          .insert(ino, FileData::new_for_tests(None, ino));
        let reply = fusefs.write(request, ino, fh, ofs, DATA, 0)
          .now_or_never().unwrap()
          .unwrap();
        assert_eq!(reply.written, DATA.len() as u32);
    }
}

}
// LCOV_EXCL_STOP
