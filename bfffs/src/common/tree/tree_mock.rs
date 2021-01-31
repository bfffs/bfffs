// vim: tw=80
// LCOV_EXCL_START

use crate::common::{
    *,
    Error, TxgT,
    dml::*,
    tree::*
};
use futures::{
    Future,
    Stream
};
use mockall::mock;
use std::{
    borrow::Borrow,
    io,
    ops::{Range, RangeBounds},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll}
};

// RangeQuery can't be automock'd because
mock! {
    pub RangeQuery<A, D, K, T, V> {}
    impl<A, D, K, T, V> Stream for RangeQuery<A, D, K, T, V>
        where A: Addr,
              D: DML<Addr=A> + 'static,
              K: Key + Borrow<T>,
              T: Ord + Clone + Send + 'static,
              V: Value
    {
        type Item = Result<(K, V), Error>;

        fn poll_next<'a>(mut self: Pin<&mut Self>, cx: &mut Context<'a>)
            -> Poll<Option<Result<(K, V), Error>>>;
    }
}

mock! {
    pub Tree<A, D, K, V>
        where A: Addr,
              D: DML<Addr=A> + 'static,
              K: Key,
              V: Value
    {
        pub fn check(&self)
            -> Pin<Box<dyn Future<Output=Result<bool, Error>> + Send>>;
        pub fn clean_zone(&self, pbas: Range<PBA>, txgs: Range<TxgT>, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>;
        pub fn create(dml: Arc<D>, seq: bool, lzratio: f32, izratio: f32)
            -> MockTree<A, D, K, V>;
        pub fn dump(&self, f: &mut (dyn io::Write + 'static))
            -> Result<(), Error>;
        pub fn flush(&self, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>;
        pub fn get(&self, k: K)
            -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>;
        pub fn insert(&self, k: K, v: V, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>;
        pub fn is_dirty(&self) -> bool;
        pub fn last_key(&self)
            -> Pin<Box<dyn Future<Output=Result<Option<K>, Error>> + Send>>;
        pub fn open(dml: Arc<D>, seq: bool, on_disk: TreeOnDisk<A>)
            -> MockTree<A, D, K, V>;
        pub fn range<R, T>(&self, range: R) -> RangeQuery<A, D, K, T, V>
            where K: Borrow<T>,
                  R: RangeBounds<T> + 'static,
                  T: Ord + Clone + Send + 'static;
        pub fn range_delete<R, T>(&self, range: R, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>
            where K: Borrow<T>,
                  R: RangeBounds<T> + 'static,
                  T: Ord + Clone + Send + 'static;
        pub fn remove(&self, k: K, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>;
        pub fn serialize(&self) -> Result<TreeOnDisk<A>, Error>;
    }
}
// LCOV_EXCL_STOP
