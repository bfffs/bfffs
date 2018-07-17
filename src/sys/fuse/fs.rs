// vim: tw=80
//! FUSE filesystem access

use common::*;
use common::database::*;
use fuse::*;
use futures::future;
use nix::Error;
use std::sync::Arc;
use tokio::runtime::current_thread;

/// FUSE's handle to an ArkFS filesystem.  One per mountpoint.
pub struct FS {
    db: Arc<Database>,
    tree: TreeID,
    // TODO: wrap Runtime in ARC so it can be shared by multiple filesystems
    runtime: current_thread::Runtime
}

impl FS {
    pub fn new(database: Arc<Database>, runtime: current_thread::Runtime, tree: TreeID)
        -> Self
    {
        FS{db: database, runtime, tree}
    }
}

impl Filesystem for FS {
    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        let fut = self.db.fsread(self.tree, |dataset| {
            future::ok::<LbaT, Error>(dataset.size())
        });
        // NB: these are the fields of FUSE's statfs structure, which is
        // actually closer to struct statvfs than struct statfs.
        let blocks = self.runtime.block_on(fut).unwrap();   // total blocks in filesystem
        let files = u64::max_value();   // total file nodes in filesystem
        let ffree = files;              // free file nodes in filesystem
        let frsize = 4096;              // fragment size
        let bsize = 4096;               // optimal transfer block size

        // TODO: use real values below, instead of dummy values
        let bfree = blocks / 4 * 3;     // free blocks in fs
        let bavail = bfree;     // free blocks available to unpriveleged user
        let namelen = 255; // max length of filenames

        reply.statfs(blocks, bfree, bavail, files, ffree, bsize, namelen, frsize);
    }
}
