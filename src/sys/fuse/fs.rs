// vim: tw=80
//! FUSE filesystem access

use common::database::*;
use common::fs::Fs;
use fuse::*;
use libc;
use nix::errno;
use std::{
    ffi::OsStr,
    os::unix::ffi::OsStrExt,
    slice,
    sync::Arc
};
use time::Timespec;
use tokio_io_pool;

const EPOCH_TIME: Timespec = Timespec { sec: 0, nsec: 0};
const ROOT_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: EPOCH_TIME,
    mtime: EPOCH_TIME,
    ctime: EPOCH_TIME,
    crtime: EPOCH_TIME,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 0,
    gid: 0,
    rdev: 0,    // device number
    flags: 0
};

/// FUSE's handle to an ArkFS filesystem.  One per mountpoint.
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
        match ino {
            1 => {
                // FUSE hardcodes inode 1 to the root
                reply.attr(&ttl, &ROOT_ATTR)
            },
            _ => reply.error(errno::Errno::ENOENT as i32)
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
