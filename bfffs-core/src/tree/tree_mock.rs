// vim: tw=80
// LCOV_EXCL_START

use crate::{
    Result,
    TxgT,
    dml::*,
    tree::*,
    writeback::Credit
};
use futures::{
    Future,
    Stream
};
use lazy_static::lazy_static;
use mockall::mock;
use std::{
    borrow::Borrow,
    fmt::Debug,
    io,
    ops::{Range, RangeBounds},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
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
        type Item = Result<(K, V)>;

        fn poll_next<'a>(mut self: Pin<&mut Self>, cx: &mut Context<'a>)
            -> Poll<Option<Result<(K, V)>>>;
    }
}

lazy_static! {
    pub static ref OPEN_MTX: Mutex<()> = Mutex::new(());
}

mock! {
    pub Tree<A, D, K, V>
        where A: Addr,
              D: DML<Addr=A> + 'static,
              K: Key,
              V: Value
    {
        pub async fn check(self: Arc<Self>) -> Result<bool>;
        pub async fn clean_zone(self: Arc<Self>, pbas: Range<PBA>,
                                txgs: Range<TxgT>, txg: TxgT)
            -> Result<()>;
        pub fn create(dml: Arc<D>, seq: bool, lzratio: f32, izratio: f32)
            -> MockTree<A, D, K, V>;
        pub fn credit_requirements(&self) -> CreditRequirements;
        pub async fn dump<'a>(&self, f: &'a mut dyn io::Write)
            -> Result<()>;
        pub async fn flush(self: Arc<Self>, txg: TxgT) -> Result<()>;
        pub fn get(&self, k: K)
            -> Pin<Box<dyn Future<Output=Result<Option<V>>> + Send>>;
        pub async fn insert(self: Arc<Self>, k: K, v: V, txg: TxgT,
                            credit: Credit)
            -> Result<Option<V>>;
        pub fn is_dirty(&self) -> bool;
        pub fn last_key(&self)
            -> Pin<Box<dyn Future<Output=Result<Option<K>>> + Send>>;
        pub fn open(dml: Arc<D>, seq: bool, on_disk: TreeOnDisk<A>)
            -> MockTree<A, D, K, V>;
        pub fn range<R, T>(&self, range: R) -> RangeQuery<A, D, K, T, V>
            where K: Borrow<T>,
                  R: RangeBounds<T> + 'static,
                  T: Debug + Ord + Clone + Send + 'static;
        pub async fn range_delete<R, T>(self: Arc<Self>, range: R, txg: TxgT,
            credit: Credit)
            -> Result<()>
            where K: Borrow<T>,
                  R: RangeBounds<T> + 'static,
                  T: Ord + Clone + Send + 'static;
        pub async fn remove(self: Arc<Self>, k: K, txg: TxgT, credit: Credit)
            -> Result<Option<V>>;
        pub fn serialize(&self) -> Result<TreeOnDisk<A>>;
    }
}
// LCOV_EXCL_STOP
