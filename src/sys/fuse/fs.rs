// vim: tw=80
//! FUSE filesystem access

use common::database::*;
use common::fs::Fs;
use fuse::*;
use nix::errno;
use std::sync::Arc;
use time::Timespec;
use tokio::runtime::current_thread;

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
    pub fn new(database: Arc<Database>, runtime: current_thread::Runtime,
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

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        let statvfs = self.fs.statvfs();
        reply.statfs(statvfs.f_blocks, statvfs.f_bfree, statvfs.f_bavail,
                     statvfs.f_files, statvfs.f_ffree, statvfs.f_bsize as u32,
                     statvfs.f_namemax as u32, statvfs.f_frsize as u32);
    }
}
