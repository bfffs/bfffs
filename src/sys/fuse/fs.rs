// vim: tw=80
//! FUSE filesystem access

use common::database::*;
use common::fs::Fs;
use fuse::*;
use libc;
use nix::Error;
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
        match self.fs.getattr(ino) {
            Ok(inode) => {
                let kind = match inode.mode & libc::S_IFMT {
                    libc::S_IFIFO => FileType::NamedPipe,
                    libc::S_IFCHR => FileType::CharDevice,
                    libc::S_IFDIR => FileType::Directory,
                    libc::S_IFBLK => FileType::BlockDevice,
                    libc::S_IFREG => FileType::RegularFile,
                    libc::S_IFLNK => FileType::Symlink,
                    libc::S_IFSOCK => FileType::Socket,
                    _ => panic!("Unknown file mode {:?}", inode.mode)
                };
                let attr = FileAttr {
                    ino,
                    size: inode.size,
                    blocks: 0,
                    atime: inode.atime,
                    mtime: inode.mtime,
                    ctime: inode.ctime,
                    crtime: inode.birthtime,
                    kind,
                    perm: (inode.mode & 0xFFFF) as u16,
                    nlink: inode.nlink as u32,
                    uid: inode.uid,
                    gid: inode.gid,
                    rdev: 0,    // ???
                    flags: inode.flags as u32
                };
                reply.attr(&ttl, &attr)
            },
            Err(e) => {
                let errno = match e {
                    Error::Sys(errno) => errno as i32,
                    _ => libc::EINVAL
                };
                reply.error(errno)
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
