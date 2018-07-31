// vim: tw=80
//! Common VFS implementation

use common::*;
use common::database::*;
use futures::{Future, IntoFuture, sync::oneshot};
use std::sync::Arc;
use tokio::runtime::current_thread;

/// Generic Filesystem layer.
///
/// Bridges the synchronous with Tokio domains, and the system-independent with
/// system-dependent filesystem interfaces.
pub struct Fs {
    db: Arc<Database>,
    // TODO: wrap Runtime in ARC so it can be shared by multiple filesystems
    runtime: current_thread::Runtime,
    tree: TreeID,
}

impl Fs {
    pub fn new(database: Arc<Database>, runtime: current_thread::Runtime,
               tree: TreeID) -> Self
    {
        Fs{db: database, runtime, tree}
    }
}

impl Fs {
    pub fn statfs(&mut self) -> (LbaT, LbaT) {
        let (tx, rx) = oneshot::channel::<(LbaT, LbaT)>();
        self.runtime.spawn(self.db.fsread(self.tree, move |dataset| {
            tx.send((dataset.size(), dataset.allocated())).unwrap();
            Ok(()).into_future()
        }).map_err(|e| panic!("{:?}", e)));
        rx.wait().unwrap()
    }
}
