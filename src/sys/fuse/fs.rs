// vim: tw=80
//! FUSE filesystem access

use common::database::*;
use common::fs::Fs;
use fuse::*;
use libc;
use std::{
    ffi::OsStr,
    os::unix::ffi::OsStrExt,
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
            Err(e) => Err(e.into())
        }
    }

    pub fn new(database: Arc<Database>, runtime: tokio_io_pool::Runtime,
               tree: TreeID)
        -> Self
    {
        let fs = Fs::new(database, runtime, tree);
        FuseFs{fs}
    }
}

impl Filesystem for FuseFs {
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
                    assert!(!r, "TODO: deal with full FUSE ReplyDirectory buffers");
                },
                Err(e) => {
                    reply.error(e);
                    return;
                }
            }
        }
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        let statvfs = self.fs.statvfs();
        reply.statfs(statvfs.f_blocks, statvfs.f_bfree, statvfs.f_bavail,
                     statvfs.f_files, statvfs.f_ffree, statvfs.f_bsize as u32,
                     statvfs.f_namemax as u32, statvfs.f_frsize as u32);
    }
}
