// vim: tw=80

//! The Database layer owns all of the Datasets
//!
//! Clients use the Database to obtain references to the Datasets.  The Database
//! also owns the Forest and manages Transactions.

use common::*;
use common::dataset::*;
use common::dml::DML;
use common::ddml::DRP;
use common::idml::*;
use common::tree::*;
use futures::{Future, IntoFuture};
use futures_locks::RwLock;
use nix::{Error, errno};
//use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;
use tokio::executor::SpawnError;

/// Keys into the Forest
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum TreeID {
    /// A filesystem, snapshot, or clone
    Subvolume(u64)
}

impl MinValue for TreeID {
    fn min_value() -> Self {
        TreeID::Subvolume(u64::min_value())
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
    _forest: DTree<TreeID, DRP>,
    idml: Arc<IDML>,
    inner: Arc<Inner>,
    transaction: RwLock<TxgT>,
}

impl<'a> Database {
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

    pub fn sync(&'a self) -> impl Future<Item=(), Error=Error> + 'a
    {
        self.transaction.write()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |_txg| {
                // TODO: force all trees to flush
                self.idml.sync_all()
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
        self.transaction.read()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |txg| {
                let ds = inner.rw_dataset::<K, V>(tree_id, *txg);
                f(ds)
            })
    }
}
