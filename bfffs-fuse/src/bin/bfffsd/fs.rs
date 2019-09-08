// vim: tw=80
//! FUSE filesystem access

use bfffs::common::database::*;
use bfffs::common::{
    RID,
    database::TreeID,
    fs::{ExtAttr, ExtAttrNamespace, FileData, SetAttr}
};
use cfg_if::cfg_if;
use fuse::{FileAttr, FileType };
use libc;
use std::{
    ffi::{OsString, OsStr},
    os::unix::ffi::OsStrExt,
    path::Path,
    slice,
    sync::Arc
};
use time::Timespec;
use tokio_io_pool;

cfg_if! {
    if #[cfg(test)] {
        mod mock;
        use self::mock::Filesystem;
        use self::mock::MockFs as Fs;
        use self::mock::MockRequest as Request;
        use self::mock::MockReplyAttr as ReplyAttr;
        use self::mock::MockReplyCreate as ReplyCreate;
        use self::mock::MockReplyData as ReplyData;
        use self::mock::MockReplyDirectory as ReplyDirectory;
        use self::mock::MockReplyEmpty as ReplyEmpty;
        use self::mock::MockReplyEntry as ReplyEntry;
        use self::mock::MockReplyStatfs as ReplyStatfs;
        use self::mock::MockReplyWrite as ReplyWrite;
        use self::mock::MockReplyXattr as ReplyXattr;
        pub use self::mock::mount;
    } else {
        use fuse::{Filesystem, ReplyAttr, ReplyCreate, ReplyData,
                   ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyStatfs,
                   ReplyWrite, ReplyXattr, Request};
        use bfffs::common::fs::Fs;
        pub use fuse::mount;
    }
}

/// FUSE's handle to an BFFFS filesystem.  One per mountpoint.
///
/// This object lives in the synchronous domain, and spawns commands into the
/// Tokio domain.
pub struct FuseFs {
    fs: Fs,
}

impl FuseFs {
    /// Private helper for getattr-like operations
    fn do_getattr(&self, fd: &FileData) -> Result<FileAttr, i32> {
        match self.fs.getattr(fd) {
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
                let reply_attr = FileAttr {
                    ino: attr.ino,
                    size: attr.size,
                    blocks: attr.blocks,
                    atime: attr.atime,
                    mtime: attr.mtime,
                    ctime: attr.ctime,
                    crtime: attr.birthtime,
                    kind,
                    perm: attr.mode.perm(),
                    nlink: attr.nlink as u32,
                    uid: attr.uid,
                    gid: attr.gid,
                    rdev: attr.rdev,
                    flags: attr.flags as u32
                };
                Ok(reply_attr)
            },
            Err(e) => Err(e)
        }
    }

    pub fn new(database: Arc<Database>, handle: tokio_io_pool::Handle,
               tree: TreeID) -> Self
    {
        let fs = Fs::new(database, handle, tree);
        FuseFs{fs}
    }

    /// Private helper for FUSE methods that take a `ReplyEntry`
    fn reply_entry(&self, r: Result<FileData, i32>, reply: ReplyEntry) {
        // FUSE combines the function of VOP_GETATTR with many other VOPs.
        match r.and_then(|fd| self.do_getattr(&fd)) {
            Ok(file_attr) => {
                // The generation number is only used for filesystems exported
                // by NFS, and is only needed if the filesystem reuses deleted
                // inodes.  BFFFS does not reuse deleted inodes.
                let gen = 0;
                let ttl = Timespec { sec: 0, nsec: 0 };
                reply.entry(&ttl, &file_attr, gen)
            },
            Err(e) => {
                reply.error(e)
            }
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
        let ns = if ns_str == OsString::from("user") {
            ExtAttrNamespace::User
        } else if ns_str == OsString::from("system") {
            ExtAttrNamespace::System
        } else {
            panic!("Unknown namespace {:?}", ns_str)
        };
        let name = OsStr::from_bytes(groups.next().unwrap());
        (ns, name)
    }
}

impl Filesystem for FuseFs {
    fn create(&mut self, req: &Request, parent: u64, name: &OsStr,
              mode: u32, _flags: u32, reply: ReplyCreate) {
        let ttl = Timespec { sec: 0, nsec: 0 };
        // XXX Here and elsewhere, constructing a FileData is just temporary
        // until bfffs-fuse learns how to store them.  Eventually bfffs-fuse
        // should never construct a FileData itself.
        let parent_fd = FileData{ino: parent};

        // FUSE combines the functions of VOP_CREATE and VOP_GETATTR
        // into one.
        let perm = (mode & 0o7777) as u16;
        match self.fs.create(&parent_fd, name, perm, req.uid(), req.gid())
            .and_then(|fd| self.do_getattr(&fd)) {
            Ok(file_attr) => {
                // The generation number is only used for filesystems exported
                // by NFS, and is only needed if the filesystem reuses deleted
                // inodes.  BFFFS does not reuse deleted inodes.
                let gen = 0;
                reply.created(&ttl, &file_attr, gen, 0, 0)
            },
            Err(e) => {
                reply.error(e)
            }
        }
    }

    fn destroy(&mut self, _req: &Request) {
        self.fs.sync()
    }

    fn fsync(&mut self, _req: &Request, ino: u64, _fh: u64, _datasync: bool,
             reply: ReplyEmpty)
    {
        let fd = FileData{ino};
        match self.fs.fsync(&fd) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e)
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let ttl = Timespec { sec: 0, nsec: 0 };
        let fd = FileData{ino};
        match self.do_getattr(&fd) {
            Ok(file_attr) => reply.attr(&ttl, &file_attr),
            Err(errno) => reply.error(errno)
        }
    }

    fn getxattr(&mut self, _req: &Request, ino: u64, packed_name: &OsStr,
                size: u32, reply: ReplyXattr)
    {
        let fd = FileData{ino};
        let (ns, name) = FuseFs::split_xattr_name(packed_name);
        if size == 0 {
            match self.fs.getextattrlen(&fd, ns, name) {
                Ok(len) => reply.size(len),
                Err(errno) => reply.error(errno)
            }
        } else {
            match self.fs.getextattr(&fd, ns, name) {
                // data copy
                Ok(buf) => {
                    if buf.len() <= size as usize {
                        reply.data(&buf[..])
                    } else {
                        reply.error(libc::ERANGE)
                    }
                },
                Err(errno) => reply.error(errno)
            }
        }
    }

    fn link(&mut self, _req: &Request, parent: u64, ino: u64,
            name: &OsStr, reply: ReplyEntry)
    {
        let fd = FileData{ino};
        let parent_fd = FileData{ino: parent};
        match self.fs.link(&parent_fd, &fd, name) {
            Ok(_) => self.reply_entry(Ok(fd), reply),
            Err(e) => reply.error(e)
        }
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr,
               reply: ReplyEntry)
    {
        let parent_fd = FileData{ino: parent};
        self.reply_entry(self.fs.lookup(&parent_fd, name), reply);
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
    fn listxattr(&mut self, _req: &Request, ino: u64, size: u32,
                 reply: ReplyXattr)
    {
        let fd = FileData{ino};
        if size == 0 {
            let f = |extattr: &ExtAttr<RID>| {
                let name = extattr.name();
                let prefix_len = match extattr.namespace() {
                    ExtAttrNamespace::User => b"user.".len(),
                    ExtAttrNamespace::System => b"system.".len(),
                } as u32;
                prefix_len + name.as_bytes().len() as u32 + 1
            };
            match self.fs.listextattrlen(&fd, f) {
                Ok(len) => reply.size(len),
                Err(errno) => reply.error(errno)
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
            match self.fs.listextattr(&fd, size, f) {
                Ok(buf) => {
                    if buf.len() <= size as usize {
                        // data copy
                        reply.data(&buf[..])
                    } else {
                        reply.error(libc::ERANGE)
                    }
                },
                Err(errno) => reply.error(errno)
            }
        }
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32,
                 reply: ReplyEntry)
    {
        let parent_fd = FileData{ino: parent};
        let perm = (mode & 0o7777) as u16;
        let r = self.fs.mkdir(&parent_fd, name, perm, req.uid(), req.gid());
        self.reply_entry(r, reply);
    }

    fn mknod(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32,
             rdev: u32, reply: ReplyEntry)
    {
        let parent_fd = FileData{ino: parent};
        let perm = (mode & 0o7777) as u16;
        let r = match mode as u16 & libc::S_IFMT {
            libc::S_IFIFO =>
                self.fs.mkfifo(&parent_fd, name, perm, req.uid(), req.gid()),
            libc::S_IFCHR =>
                self.fs.mkchar(&parent_fd, name, perm, req.uid(), req.gid(),
                    rdev),
            libc::S_IFBLK =>
                self.fs.mkblock(&parent_fd, name, perm, req.uid(), req.gid(),
                    rdev),
            libc::S_IFSOCK =>
                self.fs.mksock(&parent_fd, name, perm, req.uid(), req.gid()),
            _ => Err(libc::EOPNOTSUPP)
        };
        self.reply_entry(r, reply);
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64,
            size: u32, reply: ReplyData)
    {
        let fd = FileData{ino};
        match self.fs.read(&fd, offset as u64, size as usize) {
            Ok(ref sglist) if sglist.is_empty() => reply.data(&[]),
            Ok(sglist) => {
                if sglist.len() == 1 {
                    reply.data(&sglist[0][..])
                } else {
                    // Vectored data requires an additional data copy, thanks to
                    // https://github.com/zargony/rust-fuse/issues/120
                    let total_len = sglist.iter()
                        .map(|iovec| iovec.len())
                        .sum();
                    let mut v = Vec::with_capacity(total_len);
                    for iov in sglist.into_iter() {
                        v.extend_from_slice(&iov[..]);
                    }
                    reply.data(&v[..])
                }
            }
            Err(errno) => reply.error(errno)
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64,
               mut reply: ReplyDirectory)
    {
        let fd = FileData{ino};
        for v in self.fs.readdir(&fd, offset) {
            match v {
                Ok((dirent, offset)) => {
                    let ft = match dirent.d_type {
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
                    let r = reply.add(dirent.d_fileno.into(), offset, ft,
                        OsStr::from_bytes(name));
                    if r {
                        // Out of space in the reply buffer.  
                        break
                    }
                },
                Err(e) => {
                    reply.error(e);
                    return;
                }
            }
        }
        reply.ok();
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        let fd = FileData{ino};
        match self.fs.readlink(&fd) {
            Ok(path) => reply.data(&path.as_bytes()),
            Err(errno) => reply.error(errno)
        }
    }

    fn removexattr(&mut self, _req: &Request, ino: u64, packed_name: &OsStr,
                   reply: ReplyEmpty)
    {
        let fd = FileData{ino};
        let (ns, name) = FuseFs::split_xattr_name(packed_name);
        match self.fs.deleteextattr(&fd, ns, name) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno)
        }
    }

    // Note: rename is vulnerable to directory loops when linked against fuse2.
    // rust-fuse can't yet use fuse3.  See also:
    // https://github.com/zargony/rust-fuse/pull/97
    // https://github.com/libfuse/libfuse/commit/d105faf
    fn rename(&mut self, _req: &Request, parent: u64, name: &OsStr,
        newparent: u64, newname: &OsStr, reply: ReplyEmpty)
    {
        let parent_fd = FileData{ino: parent};
        let newparent_fd = FileData{ino: newparent};
        match self.fs.rename(&parent_fd, name, &newparent_fd, newname) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno)
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr,
             reply: ReplyEmpty)
    {
        let parent_fd = FileData{ino: parent};
        match self.fs.rmdir(&parent_fd, name) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno)
        }
    }

    fn setattr(&mut self,
               _req: &Request,
               ino: u64,
               mode: Option<u32>,
               uid: Option<u32>,
               gid: Option<u32>,
               size: Option<u64>,
               atime: Option<Timespec>,
               mtime: Option<Timespec>,
               _fh: Option<u64>,
               crtime: Option<Timespec>,
               chgtime: Option<Timespec>,
               _bkuptime: Option<Timespec>,
               flags: Option<u32>,
               reply: ReplyAttr)
    {
        let fd = FileData{ino};
        let attr = SetAttr {
            perm: mode.map(|m| (m & 0o7777) as u16),
            uid,
            gid,
            size,
            atime,
            mtime,
            ctime: chgtime,
            birthtime: crtime,
            flags: flags.map(u64::from)
        };
        let r = self.fs.setattr(&fd, attr)
        .and_then(|_| self.do_getattr(&fd));
        // FUSE combines the functions of VOP_SETATTR and VOP_GETATTR
        // into one.
        match r {
            Ok(file_attr) => {
                let ttl = Timespec { sec: 0, nsec: 0 };
                reply.attr(&ttl, &file_attr)
            },
            Err(e) => {
                reply.error(e)
            }
        }
    }

    fn setxattr(&mut self, _req: &Request, ino: u64, packed_name: &OsStr,
                value: &[u8], _flags: u32, _position: u32, reply: ReplyEmpty)
    {
        let fd = FileData{ino};
        let (ns, name) = FuseFs::split_xattr_name(packed_name);
        match self.fs.setextattr(&fd, ns, name, value) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno)
        }
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        match self.fs.statvfs() {
            Ok(statvfs) =>
                reply.statfs(statvfs.f_blocks, statvfs.f_bfree,
                             statvfs.f_bavail, statvfs.f_files, statvfs.f_ffree,
                             statvfs.f_bsize as u32, statvfs.f_namemax as u32,
                             statvfs.f_frsize as u32),
            Err(e) => reply.error(e)
        };
    }

    fn symlink(&mut self, req: &Request, parent: u64, name: &OsStr,
               link: &Path, reply: ReplyEntry)
    {
        // Weirdly, FUSE doesn't supply the symlink's mode.  Use a sensible
        // default.
        let perm = 0o755;
        let parent_fd = FileData{ino: parent};
        let r = self.fs.symlink(&parent_fd, name, perm, req.uid(), req.gid(),
                                link.as_os_str());
        self.reply_entry(r, reply);
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr,
              reply: ReplyEmpty)
    {
        let parent_fd = FileData{ino: parent};
        match self.fs.unlink(&parent_fd, name) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno)
        }
    }

    fn write(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64,
             data: &[u8], flags: u32, reply: ReplyWrite)
    {
        let fd = FileData{ino};
        match self.fs.write(&fd, offset as u64, data, flags) {
            Ok(lsize) => reply.written(lsize),
            Err(errno) => reply.error(errno)
        }
    }
}

// LCOV_EXCL_START
#[cfg(test)]
impl From<Fs> for FuseFs {
    fn from(fs: Fs) -> Self {
        Self{fs}
    }
}

#[cfg(test)]
mod t {

use super::*;
use bfffs::common::fs::{FileData, GetAttr, Mode};
use mockall::{PredicateStrExt, Sequence, predicate};
use std::mem;

mod create {
    use super::*;

    #[test]
    fn enotdir() {
        let mode: u16 = 0o644;
        const NAME: &'static str = "foo.txt";
        let parent_ino = 42;

        let mut request = Request::default();
        request.expect_uid().return_const(12345u32);
        request.expect_gid().return_const(12345u32);

        let mut reply = ReplyCreate::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ENOTDIR))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_create()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent_ino),
                predicate::str::contains(NAME).from_utf8(),
                predicate::eq(mode),
                predicate::always(),
                predicate::always(),
            ).returning(|_, _, _, _, _| Err(libc::ENOTDIR));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.create(&request, parent_ino, &OsString::from(NAME), mode.into(),
            0, reply);
    }

    #[test]
    fn ok() {
        let mode: u16 = 0o644;
        const NAME: &'static str = "foo.txt";
        let parent_ino = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let mut request = Request::default();
        request.expect_uid().return_const(uid);
        request.expect_gid().return_const(gid);

        let mut reply = ReplyCreate::new();
        reply.expect_created()
            .times(1)
            .withf(move |_ttl, attr, _gen, _fh, _flags| {
                attr.ino == ino &&
                attr.size == 0 &&
                attr.kind == FileType::RegularFile &&
                attr.perm == mode &&
                attr.nlink == 1 &&
                attr.uid == uid &&
                attr.gid == gid
            }).return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_create()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent_ino),
                predicate::str::contains(NAME).from_utf8(),
                predicate::eq(mode),
                predicate::always(),
                predicate::always(),
            ).returning(move |_, _, _, _, _| Ok(FileData{ino}));
        mock_fs.expect_getattr()
            .with(predicate::function(move |fd: &FileData| fd.ino == ino))
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
                flags: 0
            }));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.create(&request, parent_ino, &OsString::from(NAME), mode.into(),
            0, reply);
    }
}

mod removexattr {
    use super::*;

    #[test]
    fn enoattr() {
        let inode = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ENOATTR))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_deleteextattr()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino == inode &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| Err(libc::ENOATTR));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.removexattr(&request, inode, packed_name, reply);
    }

    #[test]
    fn ok() {
        let inode = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_ok()
            .times(1)
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_deleteextattr()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino == inode &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| Ok(()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.removexattr(&request, inode, packed_name, reply);
    }
}

mod fsync {
    use super::*;

    #[test]
    fn eio() {
        let ino = 42;
        let fh = 0xdeadbeef;

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::EIO))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_fsync()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == ino),
            ).return_const(Err(libc::EIO));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.fsync(&request, ino, fh, false, reply);
    }

    #[test]
    fn ok() {
        let ino = 42;
        let fh = 0xdeadbeef;

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_ok()
            .times(1)
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_fsync()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == ino),
            ).return_const(Ok(()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.fsync(&request, ino, fh, false, reply);
    }
}

mod getattr {
    use super::*;

    #[test]
    fn enoent() {
        let ino = 42;

        let request = Request::default();
        let mut reply = ReplyAttr::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ENOENT))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_getattr()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino == ino))
            .return_const(Err(libc::ENOENT));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.getattr(&request, ino, reply);
    }

    #[test]
    fn ok() {
        let ino = 42;
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();
        let mut reply = ReplyAttr::new();

        let mut mock_fs = Fs::default();
        mock_fs.expect_getattr()
            .with( predicate::function(move |fd: &FileData| fd.ino == ino))
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
                flags: 0
            }));
        reply.expect_attr()
            .times(1)
            .withf(move |_ttl, attr| {
                attr.ino == ino &&
                attr.size == size &&
                attr.kind == FileType::RegularFile &&
                attr.perm == mode &&
                attr.nlink == 1 &&
                attr.uid == uid &&
                attr.gid == gid &&
                attr.rdev == 0
            }).return_const(());

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.getattr(&request, ino, reply);
    }
}

mod getxattr {
    use super::*;
    use divbuf::DivBufShared;

    #[test]
    fn length_enoattr() {
        let inode = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 0;

        let request = Request::default();

        let mut reply = ReplyXattr::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ENOATTR))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_getextattrlen()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino == inode &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| Err(libc::ENOATTR));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.getxattr(&request, inode, packed_name, wantsize, reply);
    }

    #[test]
    fn length_ok() {
        let inode = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 0;
        let size = 16;

        let request = Request::default();

        let mut reply = ReplyXattr::new();
        reply.expect_size()
            .times(1)
            .with(predicate::eq(size))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_getextattrlen()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino == inode &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| Ok(size));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.getxattr(&request, inode, packed_name, wantsize, reply);
    }

    #[test]
    fn value_enoattr() {
        let inode = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 80;

        let request = Request::default();

        let mut reply = ReplyXattr::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ENOATTR))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_getextattr()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino == inode &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).return_const(Err(libc::ENOATTR));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.getxattr(&request, inode, packed_name, wantsize, reply);
    }

    // The FUSE protocol requires a file system to return ERANGE if the
    // attribute's value can't fit in the size requested by the client.
    // That's contrary to how FreeBSD's getextattr(2) works and contrary to how
    // BFFFS's Fs::getextattr works.  It's also hard to trigger during normal
    // use, because the kernel first asks for the size of the attribute.  So
    // during normal use, this error can only be the result of a race.
    #[test]
    fn value_erange() {
        let inode = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 16;
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let mut reply = ReplyXattr::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ERANGE))
            .return_const(());
        reply.expect_data()
            .times(0);

        let mut mock_fs = Fs::default();
        mock_fs.expect_getextattr()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino == inode &&
                *ns == ExtAttrNamespace::System &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| {
                let dbs = DivBufShared::from(&v[..]);
                Ok(dbs.try_const().unwrap())
            });

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.getxattr(&request, inode, packed_name, wantsize, reply);
    }

    #[test]
    fn value_ok() {
        let inode = 42;
        let packed_name = OsStr::from_bytes(b"user.md5");
        let wantsize = 80;
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let mut reply = ReplyXattr::new();
        reply.expect_data()
            .times(1)
            .with(predicate::eq(&v[..]))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_getextattr()
            .times(1)
            .withf(move |fd: &FileData, ns: &ExtAttrNamespace, name: &OsStr| {
                fd.ino == inode &&
                *ns == ExtAttrNamespace::User &&
                name == OsStr::from_bytes(b"md5")
            }).returning(move |_, _, _| {
                let dbs = DivBufShared::from(&v[..]);
                Ok(dbs.try_const().unwrap())
            });

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.getxattr(&request, inode, packed_name, wantsize, reply);
    }
}

mod link {
    use super::*;

    // POSIX stupidly requires link(2) to return EPERM for directories.  EISDIR
    // would've been a better choice
    #[test]
    fn eperm() {
        const NAME: &'static [u8] = b"foo";
        let parent = 42;
        let ino = 43;

        let request = Request::default();

        let mut reply = ReplyEntry::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::EPERM))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_link()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::function(move |fd: &FileData| fd.ino == ino),
                predicate::eq(OsStr::from_bytes(NAME)),
            ).return_const(Err(libc::EPERM));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.link(&request, parent, ino, OsStr::from_bytes(NAME), reply);
    }

    #[test]
    fn ok() {
        let mut seq = Sequence::new();
        const NAME: &'static [u8] = b"foo";
        let parent = 42;
        let ino = 43;
        let size = 1024;
        let mode = 0o644;
        let uid = 123;
        let gid = 456;

        let request = Request::default();

        let mut reply = ReplyEntry::new();
        reply.expect_entry()
            .times(1)
            .withf(move |_ttl, attr, _gen| {
                attr.ino == ino &&
                attr.size == size &&
                attr.kind == FileType::RegularFile &&
                attr.perm == mode &&
                attr.nlink == 1 &&
                attr.uid == uid &&
                attr.gid == gid &&
                attr.rdev == 0
            }).return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_link()
            .times(1)
            .in_sequence(&mut seq)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::function(move |fd: &FileData| fd.ino == ino),
                predicate::eq(OsStr::from_bytes(NAME)),
            ).return_const(Ok(()));
        mock_fs.expect_getattr()
            .with( predicate::function(move |fd: &FileData| fd.ino == ino))
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
                flags: 0
            }));


        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.link(&request, parent, ino, OsStr::from_bytes(NAME), reply);
    }
}

mod listxattr {
    use super::*;
    use bfffs::common::fs_tree::{InlineExtAttr, InlineExtent};

    #[test]
    fn length_eperm() {
        let inode = 42;
        let wantsize = 0;

        let request = Request::default();

        let mut reply = ReplyXattr::new();
        reply.expect_error()
            .times(1)
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_listextattrlen()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == inode),
                predicate::always()
            ).returning(|_ino, _f| Err(libc::EPERM));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.listxattr(&request, inode, wantsize, reply);
    }

    #[test]
    fn length_ok() {
        let inode = 42;
        let wantsize = 0;

        let request = Request::default();

        let mut reply = ReplyXattr::new();
        reply.expect_size()
            .times(1)
            .with(predicate::eq(21))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_listextattrlen()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == inode),
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

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.listxattr(&request, inode, wantsize, reply);
    }

    #[test]
    fn list_eperm() {
        let inode = 42;
        let wantsize = 1024;

        let request = Request::default();

        let mut reply = ReplyXattr::new();
        reply.expect_error()
            .times(1)
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_listextattr()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino == inode),
                predicate::eq(wantsize),
                predicate::always()
            ).returning(|_ino, _size, _f| Err(libc::EPERM));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.listxattr(&request, inode, wantsize, reply);
    }

    // The list of attributes doesn't fit in the space requested.  This is most
    // likely due to a race; an attribute was added after the kernel requested
    // the size of the attribute list.
    #[test]
    fn list_erange() {
        let inode = 42;
        let wantsize = 10;

        let request = Request::default();

        let mut reply = ReplyXattr::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ERANGE))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_listextattr()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino == inode),
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

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.listxattr(&request, inode, wantsize, reply);
    }

    #[test]
    fn list_ok() {
        let inode = 42;
        let wantsize = 1024;
        let expected = b"system.md5\0user.icon\0";

        let request = Request::default();

        let mut reply = ReplyXattr::new();
        reply.expect_data()
            .times(1)
            .with(predicate::eq(&expected[..]))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_listextattr()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino == inode),
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

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.listxattr(&request, inode, wantsize, reply);
    }
}

mod lookup {
    use super::*;

    #[test]
    fn enoent() {
        let parent = 42;
        let name = b"foo.txt";

        let request = Request::default();
        let mut reply = ReplyEntry::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ENOENT))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_lookup()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(OsStr::from_bytes(&name[..]))
            ).returning(|_, _| Err(libc::ENOENT));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.lookup(&request, parent, OsStr::from_bytes(&name[..]), reply);
    }

    #[test]
    fn ok() {
        let parent = 42;
        let ino = 43;
        let name = b"foo.txt";
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();
        let mut reply = ReplyEntry::new();

        let mut mock_fs = Fs::default();
        mock_fs.expect_lookup()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(OsStr::from_bytes(&name[..]))
            ).returning(move |_, _| Ok(FileData{ino}));
        mock_fs.expect_getattr()
            .with( predicate::function(move |fd: &FileData| fd.ino == ino))
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
                flags: 0
            }));
        reply.expect_entry()
            .times(1)
            .withf(move |_ttl, attr, _gen| {
                attr.ino == ino &&
                attr.size == size &&
                attr.kind == FileType::RegularFile &&
                attr.perm == mode &&
                attr.nlink == 1 &&
                attr.uid == uid &&
                attr.gid == gid &&
                attr.rdev == 0
            }).return_const(());

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.lookup(&request, parent, OsStr::from_bytes(&name[..]), reply);
    }
}

mod mkdir {
    use super::*;

    #[test]
    fn eperm() {
        let mode: u16 = 0o755;
        const NAME: &'static [u8; 3] = b"foo";
        let parent = 42;
        let uid = 12345u32;
        let gid = 54321u32;

        let mut request = Request::default();
        request.expect_uid().return_const(uid);
        request.expect_gid().return_const(gid);

        let mut reply = ReplyEntry::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::EPERM))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_mkdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(OsStr::from_bytes(NAME)),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
            ).returning(|_, _, _, _, _| Err(libc::EPERM));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.mkdir(&request, parent, OsStr::from_bytes(NAME),
            (libc::S_IFDIR | mode).into(), reply);
    }

    #[test]
    fn ok() {
        let mode: u16 = 0o755;
        const NAME: &'static [u8; 3] = b"foo";
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let mut request = Request::default();
        request.expect_uid().return_const(uid);
        request.expect_gid().return_const(gid);

        let mut reply = ReplyEntry::new();
        reply.expect_entry()
            .times(1)
            .withf(move |_ttl, attr, _gen| {
                attr.ino == ino &&
                attr.size == 0 &&
                attr.kind == FileType::Directory &&
                attr.perm == mode &&
                attr.nlink == 1 &&
                attr.uid == uid &&
                attr.gid == gid
            }).return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_mkdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(OsStr::from_bytes(NAME)),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
            ).returning(move |_, _, _, _, _| Ok(FileData{ino}));
        mock_fs.expect_getattr()
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
                rdev: 0,
                flags: 0
            }));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.mkdir(&request, parent, OsStr::from_bytes(NAME),
            (libc::S_IFDIR | mode).into(), reply);
    }
}

mod mknod {
    use super::*;

    #[test]
    fn blk() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        const NAME: &'static str = "foo";
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;
        let rdev = 69;

        let mut request = Request::default();
        request.expect_uid().return_const(uid);
        request.expect_gid().return_const(gid);

        let mut reply = ReplyEntry::new();
        reply.expect_entry()
            .times(1)
            .withf(move |_ttl, attr, _gen| {
                attr.ino == ino &&
                attr.size == 0 &&
                attr.kind == FileType::BlockDevice &&
                attr.perm == mode &&
                attr.nlink == 1 &&
                attr.uid == uid &&
                attr.gid == gid &&
                attr.rdev == rdev
            }).return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_mkblock()
            .times(1)
            .in_sequence(&mut seq)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::str::contains(NAME).from_utf8(),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
                predicate::eq(rdev)
            ).returning(move |_, _, _, _, _, _| Ok(FileData{ino}));
        mock_fs.expect_getattr()
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
                rdev: rdev,
                flags: 0
            }));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.mknod(&request, parent, &OsString::from(NAME),
            (libc::S_IFBLK | mode).into(), rdev, reply);
    }

    #[test]
    fn char() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        const NAME: &'static str = "foo";
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;
        let rdev = 69;

        let mut request = Request::default();
        request.expect_uid().return_const(uid);
        request.expect_gid().return_const(gid);

        let mut reply = ReplyEntry::new();
        reply.expect_entry()
            .times(1)
            .withf(move |_ttl, attr, _gen| {
                attr.ino == ino &&
                attr.size == 0 &&
                attr.kind == FileType::CharDevice &&
                attr.perm == mode &&
                attr.nlink == 1 &&
                attr.uid == uid &&
                attr.gid == gid &&
                attr.rdev == rdev
            }).return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_mkchar()
            .times(1)
            .in_sequence(&mut seq)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::str::contains(NAME).from_utf8(),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
                predicate::eq(rdev)
            ).returning(move |_, _, _, _, _, _| Ok(FileData{ino}));
        mock_fs.expect_getattr()
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
                rdev: rdev,
                flags: 0
            }));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.mknod(&request, parent, &OsString::from(NAME),
            (libc::S_IFCHR | mode).into(), rdev, reply);
    }

    #[test]
    fn eperm() {
        let mode: u16 = 0o644;
        const NAME: &'static str = "foo.pipe";
        let parent = 42;
        let uid = 12345u32;
        let gid = 54321u32;

        let mut request = Request::default();
        request.expect_uid().return_const(uid);
        request.expect_gid().return_const(gid);

        let mut reply = ReplyEntry::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::EPERM))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_mkfifo()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::str::contains(NAME).from_utf8(),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
            ).returning(|_, _, _, _, _| Err(libc::EPERM));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.mknod(&request, parent, &OsString::from(NAME),
            (libc::S_IFIFO | mode).into(), 0, reply);
    }

    #[test]
    fn fifo() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        const NAME: &'static str = "foo.pipe";
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let mut request = Request::default();
        request.expect_uid().return_const(uid);
        request.expect_gid().return_const(gid);

        let mut reply = ReplyEntry::new();
        reply.expect_entry()
            .times(1)
            .withf(move |_ttl, attr, _gen| {
                attr.ino == ino &&
                attr.size == 0 &&
                attr.kind == FileType::NamedPipe &&
                attr.perm == mode &&
                attr.nlink == 1 &&
                attr.uid == uid &&
                attr.gid == gid
            }).return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_mkfifo()
            .times(1)
            .in_sequence(&mut seq)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::str::contains(NAME).from_utf8(),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
            ).returning(move |_, _, _, _, _| Ok(FileData{ino}));
        mock_fs.expect_getattr()
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
                flags: 0
            }));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.mknod(&request, parent, &OsString::from(NAME),
            (libc::S_IFIFO | mode).into(), 0, reply);
    }

    #[test]
    fn sock() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        const NAME: &'static str = "foo.sock";
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let mut request = Request::default();
        request.expect_uid().return_const(uid);
        request.expect_gid().return_const(gid);

        let mut reply = ReplyEntry::new();
        reply.expect_entry()
            .times(1)
            .withf(move |_ttl, attr, _gen| {
                attr.ino == ino &&
                attr.size == 0 &&
                attr.kind == FileType::Socket &&
                attr.perm == mode &&
                attr.nlink == 1 &&
                attr.uid == uid &&
                attr.gid == gid
            }).return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_mksock()
            .times(1)
            .in_sequence(&mut seq)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::str::contains(NAME).from_utf8(),
                predicate::eq(mode),
                predicate::eq(uid),
                predicate::eq(gid),
            ).returning(move |_, _, _, _, _| Ok(FileData{ino}));
        mock_fs.expect_getattr()
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
                flags: 0
            }));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.mknod(&request, parent, &OsString::from(NAME),
            (libc::S_IFSOCK | mode).into(), 0, reply);
    }
}

mod read {
    use super::*;
    use bfffs::common::SGList;
    use divbuf::*;

    #[test]
    fn eio() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 0;
        let len = 1024;

        let request = Request::default();
        let mut reply = ReplyData::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::EIO))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_read()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == ino),
                predicate::eq(ofs as u64),
                predicate::eq(len as usize),
            ).return_const(Err(libc::EIO));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.read(&request, ino, fh, ofs, len, reply);
    }

    // A Read past eof should return nothing
    #[test]
    fn eof() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 2048;
        let len = 1024;

        let request = Request::default();
        let mut reply = ReplyData::new();
        reply.expect_data()
            .times(1)
            .withf(|buf| buf.is_empty())
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_read()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == ino),
                predicate::eq(ofs as u64),
                predicate::eq(len as usize),
            ).return_const(Ok(SGList::new()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.read(&request, ino, fh, ofs, len, reply);
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
        let mut reply = ReplyData::new();
        reply.expect_data()
            .times(1)
            .with(predicate::eq(DATA))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_read()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == ino),
                predicate::eq(ofs as u64),
                predicate::eq(len as usize),
            ).returning(|_ino, _ofs, _len| {
                let dbs = DivBufShared::from(DATA);
                let db = dbs.try_const().unwrap();
                Ok(vec![db])
            });

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.read(&request, ino, fh, ofs, len, reply);
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
        let mut reply = ReplyData::new();
        reply.expect_data()
            .times(1)
            .withf(|d| {
                // rust-fuse doesn't work with scatter-gather reads; we have to
                // copy the buffers into one
                // https://github.com/zargony/rust-fuse/issues/120
                &d[0..6] == DATA0 && &d[6..12] == DATA1
            }).return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_read()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == ino),
                predicate::eq(ofs as u64),
                predicate::eq(len as usize),
            ).returning(|_ino, _ofs, _len| {
                let dbs0 = DivBufShared::from(DATA0);
                let db0 = dbs0.try_const().unwrap();
                let dbs1 = DivBufShared::from(DATA1);
                let db1 = dbs1.try_const().unwrap();
                Ok(vec![db0, db1])
            });

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.read(&request, ino, fh, ofs, len, reply);
    }
}

mod readdir {
    use super::*;

    // A directory containing one file of every file type recognized by
    // rust-fuse
    #[test]
    fn all_file_types() {
        let fh = 0xdeadbeef;
        // libc's ino type could be either u32 or u64, depending on which
        // version of freebsd we're targeting.
        let ofs = 0;
        let mut dotname = [0 as libc::c_char; 256];
        dotname[0] = '.' as libc::c_char;
        let dot_ino = 42u32;
        let dot_ofs = 0;
        let mut regname = [0 as libc::c_char; 256];
        regname[0] = 'r' as libc::c_char;
        let reg_ino = 43u32;
        let reg_ofs = 1;
        let mut charname = [0 as libc::c_char; 256];
        charname[0] = 'c' as libc::c_char;
        let char_ino = 43u32;
        let char_ofs = 2;
        let mut blockname = [0 as libc::c_char; 256];
        blockname[0] = 'b' as libc::c_char;
        let block_ino = 43u32;
        let block_ofs = 3;
        let mut pipename = [0 as libc::c_char; 256];
        pipename[0] = 'p' as libc::c_char;
        let pipe_ino = 43u32;
        let pipe_ofs = 4;
        let mut symlinkname = [0 as libc::c_char; 256];
        symlinkname[0] = 'l' as libc::c_char;
        let symlink_ino = 43u32;
        let symlink_ofs = 5;
        let mut sockname = [0 as libc::c_char; 256];
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
        let mut reply = ReplyDirectory::new();
        reply.expect_add::<&OsStr>()
            .times(1)
            .with(
                predicate::eq(u64::from(dot_ino)),
                predicate::eq(dot_ofs),
                predicate::eq(FileType::Directory),
                predicate::eq(OsStr::from_bytes(b"."))
            ).return_const(false);
        reply.expect_add::<&OsStr>()
            .times(1)
            .with(
                predicate::eq(u64::from(reg_ino)),
                predicate::eq(reg_ofs),
                predicate::eq(FileType::RegularFile),
                predicate::eq(OsStr::from_bytes(b"r"))
            ).return_const(false);
        reply.expect_add::<&OsStr>()
            .times(1)
            .with(
                predicate::eq(u64::from(pipe_ino)),
                predicate::eq(pipe_ofs),
                predicate::eq(FileType::NamedPipe),
                predicate::eq(OsStr::from_bytes(b"p"))
            ).return_const(false);
        reply.expect_add::<&OsStr>()
            .times(1)
            .with(
                predicate::eq(u64::from(char_ino)),
                predicate::eq(char_ofs),
                predicate::eq(FileType::CharDevice),
                predicate::eq(OsStr::from_bytes(b"c"))
            ).return_const(false);
        reply.expect_add::<&OsStr>()
            .times(1)
            .with(
                predicate::eq(u64::from(block_ino)),
                predicate::eq(block_ofs),
                predicate::eq(FileType::BlockDevice),
                predicate::eq(OsStr::from_bytes(b"b"))
            ).return_const(false);
        reply.expect_add::<&OsStr>()
            .times(1)
            .with(
                predicate::eq(u64::from(symlink_ino)),
                predicate::eq(symlink_ofs),
                predicate::eq(FileType::Symlink),
                predicate::eq(OsStr::from_bytes(b"l"))
            ).return_const(false);
        reply.expect_add::<&OsStr>()
            .times(1)
            .with(
                predicate::eq(u64::from(sock_ino)),
                predicate::eq(sock_ofs),
                predicate::eq(FileType::Socket),
                predicate::eq(OsStr::from_bytes(b"s"))
            ).return_const(false);
        reply.expect_ok()
            .times(1)
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_readdir()
            .times(1)
            .with(
                predicate::function(move |h: &FileData|
                                    u64::from(dot_ino) == h.ino
                ),predicate::eq(ofs),
            ).return_once(move |_, _| Box::new(contents.into_iter()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.readdir(&request, dot_ino.into(), fh, ofs, reply);
    }

    #[test]
    fn eio() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 0;

        let request = Request::default();
        let mut reply = ReplyDirectory::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::EIO))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_readdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == ino),
                predicate::eq(ofs),
            ).returning(|_, _| Box::new(vec![Err(libc::EIO)].into_iter()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.readdir(&request, ino, fh, ofs, reply);
    }

    // A directory containing nothing but "." and ".."
    #[test]
    fn empty() {
        let fh = 0xdeadbeef;
        // libc's ino type could be either u32 or u64, depending on which
        // version of freebsd we're targeting.
        let ino = 42u32;
        let parent = 41u32;
        let ofs = 0;
        let mut dotname = [0 as libc::c_char; 256];
        dotname[0] = '.' as libc::c_char;
        let mut dotdotname = [0 as libc::c_char; 256];
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
        let mut reply = ReplyDirectory::new();
        reply.expect_add::<&OsStr>()
            .times(1)
            .with(
                predicate::eq(u64::from(ino)),
                predicate::eq(0),
                predicate::eq(FileType::Directory),
                predicate::eq(OsStr::from_bytes(b"."))
            ).return_const(false);
        reply.expect_add::<&OsStr>()
            .times(1)
            .with(
                predicate::eq(u64::from(parent)),
                predicate::eq(1),
                predicate::eq(FileType::Directory),
                predicate::eq(OsStr::from_bytes(b".."))
            ).return_const(false);
        reply.expect_ok()
            .times(1)
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_readdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData|
                                    u64::from(ino) == fd.ino
                ), predicate::eq(ofs),
            ).return_once(move |_, _| Box::new(contents.into_iter()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.readdir(&request, ino.into(), fh, ofs, reply);
    }

    // If the buffer provided by fuse runs out of space, we should terminate
    // early.
    #[test]
    fn out_of_space() {
        let fh = 0xdeadbeef;
        // libc's ino type could be either u32 or u64, depending on which
        // version of freebsd we're targeting.
        let ino = 42u32;
        let parent = 41u32;
        let ofs = 0;
        let mut dotname = [0 as libc::c_char; 256];
        dotname[0] = '.' as libc::c_char;
        let mut dotdotname = [0 as libc::c_char; 256];
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
        let mut reply = ReplyDirectory::new();
        reply.expect_add::<&OsStr>()
            .times(1)
            .with(
                predicate::eq(u64::from(ino)),
                predicate::eq(0),
                predicate::eq(FileType::Directory),
                predicate::eq(OsStr::from_bytes(b"."))
            ).return_const(true);
        reply.expect_ok()
            .times(1)
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_readdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData|
                                    u64::from(ino) == fd.ino
                ), predicate::eq(ofs),
            ).return_once(move |_, _| Box::new(contents.into_iter()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.readdir(&request, ino.into(), fh, ofs, reply);
    }
}

mod readlink {
    use super::*;

    #[test]
    fn enoent() {
        let ino = 42;

        let request = Request::default();
        let mut reply = ReplyData::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ENOENT))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_readlink()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino == ino))
            .return_const(Err(libc::ENOENT));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.readlink(&request, ino, reply);
    }

    #[test]
    fn ok() {
        let ino = 42;
        let name = b"some_file.txt";

        let request = Request::default();
        let mut reply = ReplyData::new();
        reply.expect_data()
            .times(1)
            .with(predicate::eq(&name[..]))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_readlink()
            .times(1)
            .with(predicate::function(move |fd: &FileData| fd.ino == ino))
            .return_const(Ok(OsStr::from_bytes(&name[..]).to_owned()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.readlink(&request, ino, reply);
    }
}

mod rename {
    use super::*;

    #[test]
    fn enotdir() {
        let parent = 42;
        let newparent = 43;
        let name = OsStr::from_bytes(b"foo");
        let newname = OsStr::from_bytes(b"bar");

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ENOTDIR))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_rename()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(name),
                predicate::function(move |fd: &FileData| fd.ino == newparent),
                predicate::eq(newname),
            ).return_const(Err(libc::ENOTDIR));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.rename(&request, parent, name, newparent, newname, reply);
    }

    #[test]
    fn ok() {
        let parent = 42;
        let newparent = 43;
        let name = OsStr::from_bytes(b"foo");
        let newname = OsStr::from_bytes(b"bar");

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_ok()
            .times(1)
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_rename()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(name),
                predicate::function(move |fd: &FileData| fd.ino == newparent),
                predicate::eq(newname),
            ).return_const(Ok(()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.rename(&request, parent, name, newparent, newname, reply);
    }
}

mod rmdir {
    use super::*;

    #[test]
    fn enotdir() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ENOTDIR))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_rmdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(name)
            ).returning(move |_, _| Err(libc::ENOTDIR));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.rmdir(&request, parent, name, reply);
    }

    #[test]
    fn ok() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_ok()
            .times(1)
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_rmdir()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(name)
            ).returning(move |_, _| Ok(()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.rmdir(&request, parent, name, reply);
    }
}

mod setattr {
    use super::*;

    #[test]
    fn eperm() {
        let mode: u16 = 0o644;
        let ino = 42;

        let request = Request::default();

        let mut reply = ReplyAttr::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::EPERM))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_setattr()
            .times(1)
            .withf(move |fd, attr| {
                fd.ino == ino &&
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

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.setattr(&request, ino, Some(mode as u32), None, None, None, None,
            None, None, None, None, None, None, reply);
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

        let mut reply = ReplyAttr::new();
        reply.expect_attr()
            .times(1)
            .withf(move |_ttl, attr| {
                attr.ino == ino &&
                attr.size == size &&
                attr.kind == FileType::RegularFile &&
                attr.perm == mode &&
                attr.nlink == 1 &&
                attr.uid == uid &&
                attr.gid == gid
            }).return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_setattr()
            .times(1)
            .in_sequence(&mut seq)
            .withf(move |fd, attr| {
                fd.ino == ino &&
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
        mock_fs.expect_getattr()
            .times(1)
            .in_sequence(&mut seq)
            .return_const(Ok(GetAttr {
                ino,
                size: size,
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
                flags: 0
            }));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.setattr(&request, ino, Some(mode as u32), None, None, None, None,
            None, None, None, None, None, None, reply);
    }
}

mod setxattr {
    use super::*;

    #[test]
    fn value_erofs() {
        let inode = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::EROFS))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_setextattr()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == inode),
                predicate::eq(ExtAttrNamespace::System),
                predicate::eq(OsStr::from_bytes(b"md5")),
                predicate::eq(&v[..])
            ).return_const(Err(libc::EROFS));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.setxattr(&request, inode, packed_name, v, 0, 0, reply);
    }

    #[test]
    fn value_ok() {
        let inode = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_ok()
            .times(1)
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_setextattr()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == inode),
                predicate::eq(ExtAttrNamespace::System),
                predicate::eq(OsStr::from_bytes(b"md5")),
                predicate::eq(&v[..])
            ).return_const(Ok(()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.setxattr(&request, inode, packed_name, v, 0, 0, reply);
    }
}

mod statfs {
    use super::*;

    #[test]
    fn eio() {
        let ino = 42;

        let request = Request::default();

        let mut reply = ReplyStatfs::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::EIO))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_statvfs()
            .times(1)
            .return_const(Err(libc::EIO));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.statfs(&request, ino, reply);
    }

    #[test]
    fn ok() {
        let ino = 42;

        let request = Request::default();

        let mut reply = ReplyStatfs::new();
        reply.expect_statfs()
            .times(1)
            .with(
                predicate::eq(100000),
                predicate::eq(200000),
                predicate::eq(300000),
                predicate::eq(10000),
                predicate::eq(20000),
                predicate::eq(4096),
                predicate::eq(1000),
                predicate::eq(512),
            ).return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_statvfs()
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

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.statfs(&request, ino, reply);
    }
}

mod symlink {
    use super::*;

    #[test]
    fn eloop() {
        const NAME: &'static [u8] = b"foo";
        let mode: u16 = 0o755;
        let parent = 42;

        let mut request = Request::default();
        request.expect_uid().return_const(12345u32);
        request.expect_gid().return_const(12345u32);

        let mut reply = ReplyEntry::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::ELOOP))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_symlink()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(OsStr::from_bytes(NAME)),
                predicate::eq(mode),
                predicate::always(),
                predicate::always(),
                predicate::eq(OsStr::from_bytes(NAME)),
            ).returning(|_, _, _, _, _, _| Err(libc::ELOOP));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.symlink(&request, parent, OsStr::from_bytes(NAME),
            Path::new(OsStr::from_bytes(NAME)), reply);
    }

    #[test]
    fn ok() {
        const NAME: &'static [u8] = b"foo";
        let mode: u16 = 0o755;
        let parent = 42;
        let ino = 43;
        let uid = 12345;
        let gid = 54321;

        let mut request = Request::default();
        request.expect_uid().return_const(uid);
        request.expect_gid().return_const(gid);

        let mut reply = ReplyEntry::new();
        reply.expect_entry()
            .times(1)
            .withf(move |_ttl, attr, _gen| {
                attr.ino == ino &&
                attr.size == 0 &&
                attr.kind == FileType::Symlink &&
                attr.perm == mode &&
                attr.nlink == 1 &&
                attr.uid == uid &&
                attr.gid == gid
            }).return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_symlink()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(OsStr::from_bytes(NAME)),
                predicate::eq(mode),
                predicate::always(),
                predicate::always(),
                predicate::eq(OsStr::from_bytes(NAME)),
            ).returning(move |_, _, _, _, _, _| Ok(FileData{ino}));
        mock_fs.expect_getattr()
            .with(predicate::function(move |fd: &FileData| fd.ino == ino))
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
                rdev: 0,
                flags: 0
            }));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.symlink(&request, parent, OsStr::from_bytes(NAME),
            Path::new(OsStr::from_bytes(NAME)), reply);
    }
}

mod unlink {
    use super::*;

    #[test]
    fn eisdir() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::EISDIR))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_unlink()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(name)
            ).returning(move |_, _| Err(libc::EISDIR));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.unlink(&request, parent, name, reply);
    }

    #[test]
    fn ok() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let mut reply = ReplyEmpty::new();
        reply.expect_ok()
            .times(1)
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_unlink()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == parent),
                predicate::eq(name)
            ).returning(move |_, _| Ok(()));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.unlink(&request, parent, name, reply);
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
        let mut reply = ReplyWrite::new();
        reply.expect_error()
            .times(1)
            .with(predicate::eq(libc::EROFS))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_write()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == ino),
                predicate::eq(ofs as u64),
                predicate::eq(DATA),
                predicate::always()
            ).return_const(Err(libc::EROFS));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.write(&request, ino, fh, ofs, DATA, 0, reply);
    }

    // A read of one block or fewer
    #[test]
    fn ok() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 2048;
        const DATA: &[u8] = &[0u8, 1, 2, 3, 4, 5];

        let request = Request::default();
        let mut reply = ReplyWrite::new();
        reply.expect_written()
            .times(1)
            .with(predicate::eq(DATA.len() as u32))
            .return_const(());

        let mut mock_fs = Fs::default();
        mock_fs.expect_write()
            .times(1)
            .with(
                predicate::function(move |fd: &FileData| fd.ino == ino),
                predicate::eq(ofs as u64),
                predicate::eq(DATA),
                predicate::always()
            ).return_const(Ok(DATA.len() as u32));

        let mut fusefs = FuseFs::from(mock_fs);
        fusefs.write(&request, ino, fh, ofs, DATA, 0, reply);
    }
}

}
// LCOV_EXCL_STOP
