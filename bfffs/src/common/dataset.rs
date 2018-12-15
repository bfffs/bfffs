// vim: tw=80

//! Dataset layer
//!
//! An individual dataset is a file system, or a snapshot, or a block device, or
//! a specialized key-value store.  Datasets may be created, destroyed, cloned,
//! and snapshotted.  The also support the same CRUD operations as Trees.

use crate::{
    boxfut,
    common::{
        *,
        dml::{Compression, DML},
        tree::{Key, Value}
    }
};
use futures::Future;
use std::{
    borrow::Borrow,
    fmt::Debug,
    ops::RangeBounds,
    sync::Arc
};

#[cfg(not(test))] use crate::common::tree::{self, Tree};
#[cfg(test)] use crate::common::tree_mock::TreeMock as Tree;
#[cfg(not(test))] use crate::common::idml::IDML;
#[cfg(test)] use crate::common::idml_mock::IDMLMock as IDML;
#[cfg(test)] use futures::{Poll, Stream};
#[cfg(test)] use std::marker::PhantomData;

pub type ITree<K, V> = Tree<RID, IDML, K, V>;

/// Return type of `Dataset::range`
#[cfg(not(test))]
pub type RangeQuery<K, T, V> = tree::RangeQuery<RID, IDML, K, T, V>;
#[cfg(test)]
pub struct RangeQuery<K, T, V> {
    pub s: Box<dyn Stream<Item=(K, V), Error=Error> + Send>,
    pub phantom: PhantomData<T>
}

#[cfg(test)]
impl<K: Key, T, V: Value>  Stream for RangeQuery<K, T, V> {
    type Item=(K, V);
    type Error=Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.s.poll()
    }
}

/// A Dataset that can be read from
pub trait ReadDataset<K: Key, V: Value> {
    fn get(&self, k: K) -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>;

    /// Read directly from the IDML, bypassing the Tree
    fn get_blob(&self, rid: RID)
        -> Box<dyn Future<Item=Box<DivBuf>, Error=Error> + Send>;

    fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static;
}

/// Inner Dataset structure, not directly exposed to user
struct Dataset<K: Key, V: Value>  {
    idml: Arc<IDML>,
    tree: Arc<ITree<K, V>>
}

impl<K: Key, V: Value> Dataset<K, V> {
    fn allocated(&self) -> LbaT {
        self.idml.allocated()
    }

    fn delete_blob(&self, rid: RID, txg: TxgT)
        -> impl Future<Item=(), Error=Error>
    {
        self.idml.delete(&rid, txg)
    }

    fn get(&self, k: K) -> impl Future<Item=Option<V>, Error=Error>
    {
        self.tree.get(k)
    }

    /// Read directly from the IDML, bypassing the Tree
    fn get_blob(&self, rid: RID)
        -> Box<dyn Future<Item=Box<DivBuf>, Error=Error> + Send>
    {
        self.idml.get::<DivBufShared, DivBuf>(&rid)
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

    #[cfg(not(test))]
    fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static
    {
        self.tree.range(range)
    }
    #[cfg(test)]
    fn range<R, T>(&self, _range: R) -> RangeQuery<K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static
    {
        // Should never be called.  In test mode, use DatasetMock::range
        // instead.
        unimplemented!()
    }

    fn range_delete<R, T>(&self, range: R, txg: TxgT)
        -> impl Future<Item=(), Error=Error> + Send
        where K: Borrow<T>,
              R: Debug + Clone + RangeBounds<T> + Send + 'static,
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

impl<K: Key, V: Value> ReadDataset<K, V> for ReadOnlyDataset<K, V> {
    fn get(&self, k: K) -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>
    {
        boxfut!(self.dataset.get(k))
    }

    /// Read directly from the IDML, bypassing the Tree
    fn get_blob(&self, rid: RID)
        -> Box<dyn Future<Item=Box<DivBuf>, Error=Error> + Send>
    {
        self.dataset.get_blob(rid)
    }

    fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static
    {
        self.dataset.range(range)
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

    pub fn delete_blob(&self, rid: RID) -> impl Future<Item=(), Error=Error> {
        self.dataset.delete_blob(rid, self.txg)
    }

    pub fn insert(&self, k: K, v: V)
        -> impl Future<Item=Option<V>, Error=Error> + Send
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

    pub fn range_delete<R, T>(&self, range: R)
        -> impl Future<Item=(), Error=Error> + Send
        where K: Borrow<T>,
              R: Debug + Clone + RangeBounds<T> + Send + 'static,
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

impl<K: Key, V: Value> ReadDataset<K, V> for ReadWriteDataset<K, V> {
    fn get(&self, k: K) -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>
    {
        boxfut!(self.dataset.get(k))
    }

    /// Read directly from the IDML, bypassing the Tree
    fn get_blob(&self, rid: RID)
        -> Box<dyn Future<Item=Box<DivBuf>, Error=Error> + Send>
    {
        self.dataset.get_blob(rid)
    }

    fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static
    {
        self.dataset.range(range)
    }
}

impl<K, V> AsRef<ReadWriteDataset<K, V>> for ReadWriteDataset<K, V>
    where K: Key, V: Value
{
    fn as_ref(&self) -> &Self {
        self
    }
}
