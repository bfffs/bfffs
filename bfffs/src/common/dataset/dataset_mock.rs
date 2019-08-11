// vim: tw=80
// LCOV_EXCL_START

use crate::common::{
    *,
    dml::Compression,
    tree::{Key, Value},
};
use futures::Future;
use mockall::mock;
use std::{
    borrow::Borrow,
    fmt::Debug,
    ops::RangeBounds,
    sync::Arc
};
use super::*;

mock! {
    pub ReadOnlyDataset<K: Key, V: Value> {
        fn allocated(&self) -> LbaT;
        fn last_key(&self)
            -> Box<dyn Future<Item=Option<K>, Error=Error> + Send>;
        fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>)
            -> ReadOnlyDataset<K, V>;
        fn size(&self) -> LbaT;
    }
    trait ReadDataset<K: Key, V: Value> {
        fn get(&self, k: K)
            -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>;
        fn get_blob(&self, rid: RID)
            -> Box<dyn Future<Item=Box<DivBuf>, Error=Error> + Send>;
        fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
            where K: Borrow<T>,
                  R: RangeBounds<T> + 'static,
                  T: Ord + Clone + Send + 'static;
    }
}

mock! {
    pub ReadWriteDataset<K, V>
        where K: Key,
              V: Value
    {
        fn allocated(&self) -> LbaT;
        fn delete_blob(&self, rid: RID)
            -> Box<dyn Future<Item=(), Error=Error> + Send>;
        fn insert(&self, k: K, v: V)
            -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>;
        fn last_key(&self)
            -> Box<dyn Future<Item=Option<K>, Error=Error> + Send>;
        fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>, txg: TxgT)
            -> ReadWriteDataset<K, V>;
        fn put_blob(&self, dbs: DivBufShared, compression: Compression)
            -> Box<dyn Future<Item=RID, Error=Error> + Send>;
        fn range_delete<R, T>(&self, range: R)
            -> Box<dyn Future<Item=(), Error=Error> + Send>
            where K: Borrow<T>,
                  R: Debug + Clone + RangeBounds<T> + Send + 'static,
                  T: Debug + Ord + Clone + Send + 'static;
        fn remove(&self, k: K)
            -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>;
        fn remove_blob(&self, rid: RID)
            -> Box<dyn Future<Item=Box<DivBufShared>, Error=Error> + Send>;
        fn size(&self) -> LbaT;
    }
    trait ReadDataset<K: Key, V: Value> {
        fn get(&self, k: K)
            -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>;
        fn get_blob(&self, rid: RID)
            -> Box<dyn Future<Item=Box<DivBuf>, Error=Error> + Send>;
        fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
            where K: Borrow<T>,
                  R: RangeBounds<T> + 'static,
                  T: Ord + Clone + Send + 'static;
    }
}

impl<K, V> AsRef<MockReadWriteDataset<K, V>> for MockReadWriteDataset<K, V>
    where K: Key, V: Value
{
    fn as_ref(&self) -> &Self {
        self
    }
}
// LCOV_EXCL_STOP
