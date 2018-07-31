// vim: tw=80
//! Common VFS implementation

use common::database::*;
use futures::{Future, IntoFuture, sync::oneshot};
use libc::statvfs;
use std::sync::Arc;
use tokio_io_pool;

/// Generic Filesystem layer.
///
/// Bridges the synchronous with Tokio domains, and the system-independent with
/// system-dependent filesystem interfaces.
pub struct Fs {
    db: Arc<Database>,
    // TODO: wrap Runtime in ARC so it can be shared by multiple filesystems
    runtime: tokio_io_pool::Runtime,
    tree: TreeID,
}

impl Fs {
    pub fn new(database: Arc<Database>, runtime: tokio_io_pool::Runtime,
               tree: TreeID) -> Self
    {
        Fs{db: database, runtime, tree}
    }
}

impl Fs {
    pub fn statvfs(&self) -> statvfs {
        let (tx, rx) = oneshot::channel::<statvfs>();
        self.runtime.spawn(
            self.db.fsread(self.tree, move |dataset| {
                let blocks = dataset.size();
                let allocated = dataset.allocated();
                let r = statvfs {
                    f_bavail: blocks - allocated,
                    f_bfree: blocks - allocated,
                    f_blocks: blocks,
                    f_favail: u64::max_value(),
                    f_ffree: u64::max_value(),
                    f_files: u64::max_value(),
                    f_bsize: 4096,
                    f_flag: 0,
                    f_frsize: 4096,
                    f_fsid: 0,
                    f_namemax: 255,
                };
                tx.send(r).ok().expect("Fs::statvfs: send failed");
                Ok(()).into_future()
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }
}
