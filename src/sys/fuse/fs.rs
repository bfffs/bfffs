// vim: tw=80
//! FUSE filesystem access

#[cfg(not(test))] use crate::common::database::*;
#[cfg(test)] use crate::common::database_mock::DatabaseMock as Database;
use crate::common::database::TreeID;
use crate::common::fs::{Fs, SetAttr};
use fuse::*;
use libc;
use std::{
    ffi::OsStr,
    os::unix::ffi::OsStrExt,
    path::Path,
    slice,
    sync::Arc
};
use time::Timespec;
use tokio_io_pool;

/// FUSE's handle to an BFFFS filesystem.  One per mountpoint.
///
/// This object lives in the synchronous domain, and spawns commands into the
/// Tokio domain.
pub struct FuseFs {
    fs: Fs,
}

impl FuseFs {
    /// Private helper for getattr-like operations
    fn do_getattr(&self, ino: u64) -> Result<FileAttr, i32> {
        match self.fs.getattr(ino) {
            Ok(attr) => {
                let kind = match attr.mode & libc::S_IFMT {
                    libc::S_IFIFO => FileType::NamedPipe,
                    libc::S_IFCHR => FileType::CharDevice,
                    libc::S_IFDIR => FileType::Directory,
                    libc::S_IFBLK => FileType::BlockDevice,
                    libc::S_IFREG => FileType::RegularFile,
                    libc::S_IFLNK => FileType::Symlink,
                    libc::S_IFSOCK => FileType::Socket,
                    _ => panic!("Unknown file mode {:?}", attr.mode)
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
                    perm: (attr.mode & 0o7777) as u16,
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
}

impl Filesystem for FuseFs {
    fn create(&mut self, _req: &Request, parent: u64, name: &OsStr,
              mode: u32, _flags: u32, reply: ReplyCreate) {
        let ttl = Timespec { sec: 0, nsec: 0 };

        // FUSE combines the functions of VOP_CREATE and VOP_GETATTR
        // into one.
        match self.fs.create(parent, name, mode)
            .and_then(|ino| self.do_getattr(ino)) {
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

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let ttl = Timespec { sec: 0, nsec: 0 };
        match self.do_getattr(ino) {
            Ok(file_attr) => reply.attr(&ttl, &file_attr),
            Err(errno) => reply.error(errno)
        }
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr,
               reply: ReplyEntry)
    {
        let ttl = Timespec { sec: 0, nsec: 0 };
        // FUSE combines the functions of VOP_LOOKUP and VOP_GETATTR
        // into one.
        match self.fs.lookup(parent, name)
            .and_then(|ino| self.do_getattr(ino)) {
            Ok(file_attr) => {
                // The generation number is only used for filesystems exported
                // by NFS, and is only needed if the filesystem reuses deleted
                // inodes.  BFFFS does not reuse deleted inodes.
                let gen = 0;
                reply.entry(&ttl, &file_attr, gen)
            },
            Err(e) => {
                reply.error(e)
            }
        }
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32,
                 reply: ReplyEntry)
    {
        let ttl = Timespec { sec: 0, nsec: 0 };
        // FUSE combines the functions of VOP_MKDIR and VOP_GETATTR
        // into one.
        match self.fs.mkdir(parent, name, mode)
            .and_then(|ino| self.do_getattr(ino)) {
            Ok(file_attr) => {
                // The generation number is only used for filesystems exported
                // by NFS, and is only needed if the filesystem reuses deleted
                // inodes.  BFFFS does not reuse deleted inodes.
                let gen = 0;
                reply.entry(&ttl, &file_attr, gen)
            },
            Err(e) => {
                reply.error(e)
            }
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64,
            size: u32, reply: ReplyData)
    {
        match self.fs.read(ino, offset as u64, size as usize) {
            Ok(sglist) => reply.data(&sglist[0][..]),
            Err(errno) => reply.error(errno)
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64,
               mut reply: ReplyDirectory)
    {
        for v in self.fs.readdir(ino, fh, offset) {
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
        match self.fs.readlink(ino) {
            Ok(path) => reply.data(&path.as_bytes()),
            Err(errno) => reply.error(errno)
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr,
             reply: ReplyEmpty) {
        match self.fs.rmdir(parent, name) {
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
        let attr = SetAttr {
            mode: mode.map(|m| m as u16),
            uid,
            gid,
            size,
            atime,
            mtime,
            ctime: chgtime,
            birthtime: crtime,
            flags: flags.map(|f| f as u64)
        };
        let r = self.fs.setattr(ino, attr)
        .and_then(|_| self.do_getattr(ino));
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

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        let statvfs = self.fs.statvfs();
        reply.statfs(statvfs.f_blocks, statvfs.f_bfree, statvfs.f_bavail,
                     statvfs.f_files, statvfs.f_ffree, statvfs.f_bsize as u32,
                     statvfs.f_namemax as u32, statvfs.f_frsize as u32);
    }

    fn symlink(&mut self, _req: &Request, parent: u64, name: &OsStr,
               link: &Path, reply: ReplyEntry)
    {
        let ttl = Timespec { sec: 0, nsec: 0 };
        // Weirdly, FUSE doesn't supply the symlink's mode.  Use a sensible
        // default.
        let mode = 0o755;
        // FUSE combines the functions of VOP_MKDIR and VOP_GETATTR
        // into one.
        match self.fs.symlink(parent, name, mode, link.as_os_str())
            .and_then(|ino| self.do_getattr(ino))
        {
            Ok(file_attr) => {
                // The generation number is only used for filesystems exported
                // by NFS, and is only needed if the filesystem reuses deleted
                // inodes.  BFFFS does not reuse deleted inodes.
                let gen = 0;
                reply.entry(&ttl, &file_attr, gen)
            },
            Err(e) => reply.error(e)
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr,
              reply: ReplyEmpty)
    {
        match self.fs.unlink(parent, name) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno)
        }
    }

    fn write(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64,
             data: &[u8], flags: u32, reply: ReplyWrite)
    {
        match self.fs.write(ino, offset as u64, data, flags) {
            Ok(lsize) => reply.written(lsize),
            Err(errno) => reply.error(errno)
        }
    }
}
