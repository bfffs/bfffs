// vim: tw=80

//! The Database layer owns all of the Datasets
//!
//! Clients use the Database to obtain references to the Datasets.  The Database
//! also owns the Forest and manages Transactions.

use common::*;
use common::dataset::*;
use common::idml::*;
use common::tree::*;
use futures::{Future, IntoFuture};
use nix::{Error, errno};
#[cfg(not(test))] use std::path::Path;
use std::sync::Arc;
use tokio::executor::SpawnError;
#[cfg(not(test))] use tokio::reactor::Handle;

// TODO: define real keys and values for filesystems
type FSKey = u32;
type FSValue = u32;

type ReadOnlyFilesystem = ReadOnlyDataset<FSKey, FSValue>;
type ReadWriteFilesystem = ReadWriteDataset<FSKey, FSValue>;

/// Keys into the Forest
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum TreeID {
    /// A filesystem, snapshot, or clone
    Fs(u32)
}

impl MinValue for TreeID {
    fn min_value() -> Self {
        TreeID::Fs(u32::min_value())
    }
}

struct Inner {
    dummy: Arc<ITree<FSKey, FSValue>>,
    idml: Arc<IDML>,
}

impl Inner {
    fn ro_filesystem(&self, _tree_id: TreeID) -> ReadOnlyFilesystem
    {
        ReadOnlyFilesystem::new(self.dummy.clone())
    }

    fn rw_filesystem(&self, _tree_id: TreeID, _txg: TxgT) -> ReadWriteFilesystem
    {
        unimplemented!()
    }
}

pub struct Database {
    // TODO: store all filesystem and device trees
    inner: Arc<Inner>,
}

impl<'a> Database {
    #[cfg(not(test))]
    pub fn open<P>(poolname: String, paths: Vec<P>, handle: Handle)
        -> impl Future<Item=Self, Error=Error>
        where P: AsRef<Path> + 'static
    {
        IDML::open(poolname, paths, handle).map(|idml| {
            let arc_idml = Arc::new(idml);
            let dummy = Arc::new(ITree::create(arc_idml.clone()));
            let inner = Arc::new(Inner{dummy, idml: arc_idml});
            Database{inner}
        })
    }

    /// Perform a read-only operation on a Filesystem
    pub fn fsread<F, B, E, R>(&self, tree_id: TreeID, f: F)
        -> Result<impl Future<Item = R, Error = Error>, SpawnError>
        where F: FnOnce(ReadOnlyFilesystem) -> B + 'static,
              B: IntoFuture<Item = R, Error = Error> + 'static,
              R: 'static
    {
        let ds = self.inner.ro_filesystem(tree_id);
        Ok(f(ds).into_future())
    }

    /// Finish the current transaction group and start a new one.
    pub fn sync_transaction(&'a self) -> impl Future<Item=(), Error=Error> + 'a
    {
        // TODO: flush the trees before the first sync_all
        self.inner.idml.sync_transaction(move |txg|{
            self.inner.dummy.flush(txg)
            .and_then(move |_| {
                    self.inner.idml.write_label(txg)
                })
            })
    }

    /// Perform a read-write operation on a Filesystem
    ///
    /// All operations conducted by the supplied closure will be completed
    /// within the same Pool transaction group.  Thus, after a power failure and
    /// recovery, either all will have completed, or none will have.
    pub fn fswrite<F, B, E, R>(&'a self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadWriteFilesystem) -> B + 'a,
              B: Future<Item = R, Error = Error> + 'a,
    {
        let inner = self.inner.clone();
        self.inner.idml.txg()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |txg| {
                let ds = inner.rw_filesystem(tree_id, *txg);
                f(ds)
            })
    }
}
