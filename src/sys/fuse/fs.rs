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
        let result = self.fs.statfs();

        // NB: these are the fields of FUSE's statfs structure, which is
        // actually closer to struct statvfs than struct statfs.
        let blocks = result.0;          // total blocks in filesystem
        let files = u64::max_value();   // total file nodes in filesystem
        let ffree = files;              // free file nodes in filesystem
        let frsize = 4096;              // fragment size
        let bsize = 4096;               // optimal transfer block size
        let allocated = result.1;
        // free blocks available to unpriveleged user
        let bavail = blocks - allocated;
        let bfree = bavail;             // free blocks in fs

        // TODO: use real values below, instead of dummy values
        let namelen = 255;              // max length of filenames

        reply.statfs(blocks, bfree, bavail, files, ffree, bsize, namelen,
                     frsize);
    }
}
