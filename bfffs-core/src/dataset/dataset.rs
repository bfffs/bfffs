// vim: tw=80

use crate::{
    dml::DML,
    idml::IDML,
    tree::{CreditRequirements, Key, Value},
    types::*,
    writeback::Credit
};
use divbuf::{DivBuf, DivBufShared};
use futures::{Future, FutureExt};
use std::{
    borrow::Borrow,
    fmt::Debug,
    ops::RangeBounds,
    pin::Pin,
    sync::Arc
};
use super::{ITree, RangeQuery, ReadDataset};

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
        -> impl Future<Output=Result<(), Error>>
    {
        self.idml.delete(&rid, txg)
    }

    fn get(&self, k: K) -> impl Future<Output=Result<Option<V>, Error>>
    {
        self.tree.get(k)
    }

    /// Read directly from the IDML, bypassing the Tree
    fn get_blob(&self, rid: RID)
        -> Pin<Box<dyn Future<Output=Result<Box<DivBuf>, Error>> + Send>>
    {
        self.idml.get::<DivBufShared, DivBuf>(&rid)
    }

    fn insert(&self, txg: TxgT, k: K, v: V, credit: Credit)
        -> impl Future<Output=Result<Option<V>, Error>>
    {
        self.tree.clone().insert(k, v, txg, credit)
    }

    fn last_key(&self) -> impl Future<Output=Result<Option<K>, Error>>
    {
        self.tree.last_key()
    }

    fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>) -> Self {
        Dataset{idml, tree}
    }

    #[cfg(not(test))]
    fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Debug + Ord + Clone + Send + 'static
    {
        self.tree.range(range)
    }
    #[cfg(test)]
    fn range<R, T>(&self, _range: R) -> RangeQuery<K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Debug + Ord + Clone + Send + 'static
    {
        // Should never be called.  In test mode, use DatasetMock::range
        // instead.
        unimplemented!()
    }

    fn range_delete<R, T>(&self, range: R, txg: TxgT, credit: Credit)
        -> impl Future<Output=Result<(), Error>> + Send
        where K: Borrow<T>,
              R: Debug + Clone + RangeBounds<T> + Send + 'static,
              T: Debug + Ord + Clone + Send + 'static
    {
        self.tree.clone().range_delete(range, txg, credit)
    }

    fn remove(&self, k: K, txg: TxgT, credit: Credit)
        -> impl Future<Output=Result<Option<V>, Error>> + Send
    {
        self.tree.clone().remove(k, txg, credit)
    }

    fn remove_blob(&self, rid: RID, txg: TxgT)
        -> impl Future<Output=Result<Box<DivBufShared>, Error>> + Send
    {
        self.idml.pop::<DivBufShared, DivBuf>(&rid, txg)
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

    pub fn last_key(&self) -> impl Future<Output=Result<Option<K>, Error>> + Send
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
    fn get(&self, k: K)
        -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>
    {
        self.dataset.get(k).boxed()
    }

    /// Read directly from the IDML, bypassing the Tree
    fn get_blob(&self, rid: RID)
        -> Pin<Box<dyn Future<Output=Result<Box<DivBuf>, Error>> + Send>>
    {
        self.dataset.get_blob(rid)
    }

    fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Debug + Ord + Clone + Send + 'static
    {
        self.dataset.range(range)
    }
}

/// A dataset handle with read/write access
pub struct ReadWriteDataset<K: Key, V: Value>  {
    cr: CreditRequirements,
    credit: Credit,
    dataset: Dataset<K, V>,
    txg: TxgT
}

impl<K: Key, V: Value> ReadWriteDataset<K, V> {
    pub fn delete_blob(&self, rid: RID)
        -> impl Future<Output=Result<(), Error>> + Send
    {
        self.dataset.delete_blob(rid, self.txg)
    }

    pub fn insert(&self, k: K, v: V)
        -> impl Future<Output=Result<Option<V>, Error>> + Send
    {
        let want = self.cr.insert + v.allocated_space();
        let credit = self.credit.atomic_split(want);
        self.dataset.insert(self.txg, k, v, credit)
    }

    pub fn new(
        idml: Arc<IDML>,
        tree: Arc<ITree<K, V>>,
        txg: TxgT,
        credit: Credit
    ) -> Self
    {
        ReadWriteDataset{
            cr: tree.credit_requirements(),
            credit,
            dataset: Dataset::new(idml, tree),
            txg
        }
    }

    pub fn range_delete<R, T>(&self, range: R)
        -> impl Future<Output=Result<(), Error>> + Send
        where K: Borrow<T>,
              R: Debug + Clone + RangeBounds<T> + Send + 'static,
              T: Debug + Ord + Clone + Send + 'static
    {
        let credit = self.credit.atomic_split(self.cr.range_delete);
        self.dataset.range_delete(range, self.txg, credit)
    }

    pub fn remove(&self, k: K)
        -> impl Future<Output=Result<Option<V>, Error>> + Send
    {
        let credit = self.credit.atomic_split(self.cr.remove);
        self.dataset.remove(k, self.txg, credit)
    }

    pub fn remove_blob(&self, rid: RID)
        -> impl Future<Output=Result<Box<DivBufShared>, Error>> + Send
    {
        self.dataset.remove_blob(rid, self.txg)
    }
}

impl<K: Key, V: Value> ReadDataset<K, V> for ReadWriteDataset<K, V> {
    fn get(&self, k: K)
        -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>
    {
        self.dataset.get(k).boxed()
    }

    /// Read directly from the IDML, bypassing the Tree
    fn get_blob(&self, rid: RID)
        -> Pin<Box<dyn Future<Output=Result<Box<DivBuf>, Error>> + Send>>
    {
        self.dataset.get_blob(rid)
    }

    fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Debug + Ord + Clone + Send + 'static
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

impl<K, V> Drop for ReadWriteDataset<K, V>
    where K: Key, V: Value
{
    fn drop(&mut self) {
        self.dataset.idml.repay(self.credit.take())
    }
}
