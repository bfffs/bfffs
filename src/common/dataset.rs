// vim: tw=80

//! Dataset layer
//!
//! An individual dataset is a file system, or a snapshot, or a block device, or
//! a specialized key-value store.  Datasets may be created, destroyed, cloned,
//! and snapshotted.  The also support the same CRUD operations as Trees.

use common::*;
use common::dml::{Compression, DML};
use common::tree::{Key, Value};
use futures::{Future, Stream};
use std::{
    borrow::Borrow,
    fmt::Debug,
    ops::RangeBounds,
    sync::Arc
};

#[cfg(not(test))] use common::tree::Tree;
#[cfg(test)] use common::tree_mock::TreeMock as Tree;
#[cfg(not(test))] use common::idml::IDML;
#[cfg(test)] use common::idml_mock::IDMLMock as IDML;

pub type ITree<K, V> = Tree<RID, IDML, K, V>;

/// Inner Dataset structure, not directly exposed to user
struct Dataset<K: Key, V: Value>  {
    idml: Arc<IDML>,
    tree: Arc<ITree<K, V>>
}

impl<K: Key, V: Value> Dataset<K, V> {
    fn allocated(&self) -> LbaT {
        self.idml.allocated()
    }

    fn delete_blob(&self, ridp: &RID, txg: TxgT)
        -> impl Future<Item=(), Error=Error>
    {
        self.idml.delete(ridp, txg)
    }

    fn dump_trees(&self) -> impl Future<Item=(), Error=Error> {
        let idml_fut = self.idml.dump_trees();
        self.tree.dump()
            .and_then(move |_| idml_fut)
    }

    fn get(&self, k: K) -> impl Future<Item=Option<V>, Error=Error>
    {
        self.tree.get(k)
    }

    /// Read directly from the IDML, bypassing the Tree
    fn get_blob(&self, ridp: &RID)
        -> impl Future<Item=Box<DivBuf>, Error=Error> + Send
    {
        self.idml.get::<DivBufShared, DivBuf>(ridp)
    }

    fn insert(&self, txg: TxgT, k: K, v: V)
        -> impl Future<Item=Option<V>, Error=Error>
    {
        self.tree.insert(k, v, txg)
    }

    fn last_key(&self) -> impl Future<Item=Option<K>, Error=Error>
    {
        self.tree.last_key()
    }

    fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>) -> Self {
        Dataset{idml, tree}
    }

    /// Write directly to the IDML, bypassing the Tree
    fn put_blob(&self, dbs: DivBufShared, compression: Compression, txg: TxgT)
        -> impl Future<Item=RID, Error=Error> + Send
    {
        self.idml.put(dbs, compression, txg)
    }

    fn range<R, T>(&self, range: R) -> impl Stream<Item=(K, V), Error=Error>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static
    {
        self.tree.range(range)
    }

    pub fn range_delete<R, T>(&self, range: R, txg: TxgT)
        -> impl Future<Item=(), Error=Error> + Send
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + Send + 'static,
              T: Debug + Ord + Clone + Send + 'static
    {
        self.tree.range_delete(range, txg)
    }

    fn remove(&self, k: K, txg: TxgT)
        -> impl Future<Item=Option<V>, Error=Error> + Send
    {
        self.tree.remove(k, txg)
    }

    fn size(&self) -> LbaT {
        self.idml.size()
    }
}

/// A dataset handle with read-only access
pub struct ReadOnlyDataset<K: Key, V: Value>  {
    dataset: Dataset<K, V>
}

impl<K: Key, V: Value> ReadOnlyDataset<K, V> {
    pub fn allocated(&self) -> LbaT {
        self.dataset.allocated()
    }

    pub fn get(&self, k: K) -> impl Future<Item=Option<V>, Error=Error>
    {
        self.dataset.get(k)
    }

    /// Read directly from the IDML, bypassing the Tree
    pub fn get_blob(&self, ridp: &RID)
        -> impl Future<Item=Box<DivBuf>, Error=Error> + Send
    {
        self.dataset.get_blob(ridp)
    }

    pub fn range<R, T>(&self, range: R) -> impl Stream<Item=(K, V), Error=Error>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static
    {
        self.dataset.range(range)
    }

    pub fn last_key(&self) -> impl Future<Item=Option<K>, Error=Error>
    {
        self.dataset.last_key()
    }

    pub fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>) -> Self {
        ReadOnlyDataset{dataset: Dataset::new(idml, tree)}
    }

    pub fn size(&self) -> LbaT {
        self.dataset.size()
    }
}

/// A dataset handle with read/write access
pub struct ReadWriteDataset<K: Key, V: Value>  {
    dataset: Dataset<K, V>,
    txg: TxgT
}

impl<K: Key, V: Value> ReadWriteDataset<K, V> {
    pub fn allocated(&self) -> LbaT {
        self.dataset.allocated()
    }

    pub fn delete_blob(&self, ridp: &RID) -> impl Future<Item=(), Error=Error> {
        self.dataset.delete_blob(ridp, self.txg)
    }

    pub fn dump_trees(&self) -> impl Future<Item=(), Error=Error> {
        self.dataset.dump_trees()
    }

    pub fn get(&self, k: K) -> impl Future<Item=Option<V>, Error=Error>
    {
        self.dataset.get(k)
    }

    /// Read directly from the IDML, bypassing the Tree
    pub fn get_blob(&self, ridp: &RID)
        -> impl Future<Item=Box<DivBuf>, Error=Error> + Send
    {
        self.dataset.get_blob(ridp)
    }

    pub fn insert(&self, k: K, v: V)
        -> impl Future<Item=Option<V>, Error=Error>
    {
        self.dataset.insert(self.txg, k, v)
    }

    pub fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>, txg: TxgT) -> Self {
        ReadWriteDataset{dataset: Dataset::new(idml, tree), txg}
    }

    /// Write directly to the IDML, bypassing the Tree
    pub fn put_blob(&self, dbs: DivBufShared, compression: Compression)
        -> impl Future<Item=RID, Error=Error> + Send
    {
        self.dataset.put_blob(dbs, compression, self.txg)
    }

    pub fn range<R, T>(&self, range: R) -> impl Stream<Item=(K, V), Error=Error>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static
    {
        self.dataset.range(range)
    }

    pub fn range_delete<R, T>(&self, range: R)
        -> impl Future<Item=(), Error=Error> + Send
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + Send + 'static,
              T: Debug + Ord + Clone + Send + 'static
    {
        self.dataset.range_delete(range, self.txg)
    }

    pub fn remove(&self, k: K)
        -> impl Future<Item=Option<V>, Error=Error> + Send
    {
        self.dataset.remove(k, self.txg)
    }
}
