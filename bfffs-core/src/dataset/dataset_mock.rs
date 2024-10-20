// vim: tw=80
// LCOV_EXCL_START

use crate::{
    idml::IDML,
    tree::{Key, Value},
    types::*,
    writeback::Credit
};
use divbuf::{DivBuf, DivBufShared};
use futures::Future;
use mockall::mock;
use std::{
    borrow::Borrow,
    fmt::Debug,
    ops::RangeBounds,
    pin::Pin,
    sync::Arc
};
use super::{ITree, RangeQuery, ReadDataset, ReadOnlyDataset, ReadWriteDataset};

mock! {
    pub ReadOnlyDataset<K: Key, V: Value> {
        pub fn last_key(&self)
            -> Pin<Box<dyn Future<Output=Result<Option<K>>> + Send>>;
        pub fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>)
            -> ReadOnlyDataset<K, V>;
        pub fn size(&self) -> Pin<Box<dyn Future<Output=LbaT> + Send>>;
        pub fn used(&self) -> Pin<Box<dyn Future<Output=LbaT> + Send>>;
    }
    impl<K: Key, V: Value> ReadDataset<K, V> for ReadOnlyDataset<K, V> {
        fn get(&self, k: K)
            -> Pin<Box<dyn Future<Output=Result<Option<V>>> + Send>>;
        fn get_blob(&self, rid: RID)
            -> Pin<Box<dyn Future<Output=Result<Box<DivBuf>>> + Send>>;
        // False positive, probably related to Mockall.
        // https://github.com/rust-lang/rust-clippy/issues/12552
        #[allow(clippy::multiple_bound_locations)]
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
        pub fn borrow_credit(&self, _size: usize)
            -> impl Future<Output=Credit> + Send;
        pub fn insert(&self, k: K, v: V)
            -> Pin<Box<dyn Future<Output=Result<Option<V>>> + Send>>;
        pub fn last_key(&self)
            -> Pin<Box<dyn Future<Output=Result<Option<K>>> + Send>>;
        pub fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>, txg: TxgT,
            credit: Credit) -> ReadWriteDataset<K, V>;
        pub fn range_delete<R, T>(&self, range: R)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
            where K: Borrow<T>,
                  R: Debug + Clone + RangeBounds<T> + Send + 'static,
                  T: Debug + Ord + Clone + Send + 'static;
        pub fn remove(&self, k: K)
            -> Pin<Box<dyn Future<Output=Result<Option<V>>> + Send>>;
        pub fn remove_blob(&self, rid: RID)
            -> Pin<Box<dyn Future<Output=Result<Box<DivBufShared>>>
                + Send
            >>;
        pub fn repay_credit(&self, credit: Credit);
        pub fn size(&self) -> LbaT;
    }
    impl<K: Key, V: Value> ReadDataset<K, V> for ReadWriteDataset<K, V> {
        fn get(&self, k: K)
            -> Pin<Box<dyn Future<Output=Result<Option<V>>> + Send>>;
        fn get_blob(&self, rid: RID)
            -> Pin<Box<dyn Future<Output=Result<Box<DivBuf>>> + Send>>;
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
