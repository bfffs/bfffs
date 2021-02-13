// vim: tw=80
// LCOV_EXCL_START

use crate::{
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
    pin::Pin,
    sync::Arc
};
use super::*;

mock! {
    pub ReadOnlyDataset<K: Key, V: Value> {
        pub fn allocated(&self) -> LbaT;
        pub fn last_key(&self)
            -> Pin<Box<dyn Future<Output=Result<Option<K>, Error>> + Send>>;
        pub fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>)
            -> ReadOnlyDataset<K, V>;
        pub fn size(&self) -> LbaT;
    }
    impl<K: Key, V: Value> ReadDataset<K, V> for ReadOnlyDataset<K, V> {
        fn get(&self, k: K)
            -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>;
        fn get_blob(&self, rid: RID)
            -> Pin<Box<dyn Future<Output=Result<Box<DivBuf>, Error>> + Send>>;
        fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
            where K: Borrow<T>,
                  R: RangeBounds<T> + 'static,
                  T: Debug + Ord + Clone + Send + 'static;
    }
}

mock! {
    pub ReadWriteDataset<K, V>
        where K: Key,
              V: Value
    {
        pub fn allocated(&self) -> LbaT;
        pub fn delete_blob(&self, rid: RID)
            -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>;
        pub fn insert(&self, k: K, v: V)
            -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>;
        pub fn last_key(&self)
            -> Pin<Box<dyn Future<Output=Result<Option<K>, Error>> + Send>>;
        pub fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>, txg: TxgT)
            -> ReadWriteDataset<K, V>;
        pub fn put_blob(&self, dbs: DivBufShared, compression: Compression)
            -> Pin<Box<dyn Future<Output=Result<RID, Error>> + Send>>;
        pub fn range_delete<R, T>(&self, range: R)
            -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>
            where K: Borrow<T>,
                  R: Debug + Clone + RangeBounds<T> + Send + 'static,
                  T: Debug + Ord + Clone + Send + 'static;
        pub fn remove(&self, k: K)
            -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>;
        pub fn remove_blob(&self, rid: RID)
            -> Pin<Box<dyn Future<Output=Result<Box<DivBufShared>, Error>>
                + Send
            >>;
        pub fn size(&self) -> LbaT;
    }
    impl<K: Key, V: Value> ReadDataset<K, V> for ReadWriteDataset<K, V> {
        fn get(&self, k: K)
            -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>;
        fn get_blob(&self, rid: RID)
            -> Pin<Box<dyn Future<Output=Result<Box<DivBuf>, Error>> + Send>>;
        fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
            where K: Borrow<T>,
                  R: RangeBounds<T> + 'static,
                  T: Debug + Ord + Clone + Send + 'static;
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
