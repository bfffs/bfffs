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

type ITree<K, V> = Tree<RID, IDML, K, V>;

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
}

impl Inner {
    fn ro_dataset<K: Key, V: Value>(&self, _tree_id: TreeID)
        -> ReadOnlyDataset<K, V>
    {
        unimplemented!()
    }

    fn rw_dataset<K: Key, V: Value>(&self, _tree_id: TreeID, _txg: TxgT)
        -> ReadWriteDataset<K, V>
    {
        unimplemented!()
    }
}

pub struct Database {
    // TODO: store all filesystem and device trees
    _dummy: ITree<u32, u32>,
    idml: Arc<IDML>,
    inner: Arc<Inner>,
}

impl<'a> Database {
    #[cfg(not(test))]
    pub fn open<P>(poolname: String, paths: Vec<P>, handle: Handle)
        -> impl Future<Item=Self, Error=Error>
        where P: AsRef<Path> + 'static
    {
        IDML::open(poolname, paths, handle).map(|idml| {
            let inner = Arc::new(Inner{});
            let arc_idml = Arc::new(idml);
            let _dummy = ITree::create(arc_idml.clone());
            Database{_dummy, idml: arc_idml, inner}
        })
    }

    /// Perform a read-only operation on a Dataset
    pub fn read<F, B, E, K, R, V>(&self, tree_id: TreeID, f: F)
        -> Result<impl Future<Item = R, Error = Error>, SpawnError>
        where F: FnOnce(ReadOnlyDataset<K, V>) -> B + 'static,
              B: IntoFuture<Item = R, Error = Error> + 'static,
              K: Key,
              V: Value,
              R: 'static
    {
        let ds = self.inner.ro_dataset::<K, V>(tree_id);
        Ok(f(ds).into_future())
    }

    /// Finish the current transaction group and start a new one.
    pub fn sync_transaction(&'a self) -> impl Future<Item=(), Error=Error> + 'a
    {
        self.idml.sync_transaction(move |txg|{
            // TODO: flush all indirect trees
            self.idml.write_label(txg)
        })
    }

    /// Perform a read-write operation on a Dataset
    ///
    /// All operations conducted by the supplied closure will be completed
    /// within the same Pool transaction group.  Thus, after a power failure and
    /// recovery, either all will have completed, or none will have.
    pub fn write<F, B, E, K, R, V>(&'a self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadWriteDataset<K, V>) -> B + 'a,
              B: Future<Item = R, Error = Error> + 'a,
              K: Key,
              V: Value,
    {
        let inner = self.inner.clone();
        self.idml.txg()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |txg| {
                let ds = inner.rw_dataset::<K, V>(tree_id, *txg);
                f(ds)
            })
    }
}
