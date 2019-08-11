// vim: tw=80
// LCOV_EXCL_START

use crate::common::*;
use crate::common::{Error, TxgT};
use crate::common::dml::*;
use crate::common::tree::*;
use futures::Future;
use mockall::mock;
use std::{
    borrow::Borrow,
    io,
    ops::{Range, RangeBounds},
    sync::Arc
};

mock! {
    pub Tree<A, D, K, V>
        where A: Addr,
              D: DML<Addr=A> + 'static,
              K: Key,
              V: Value
    {
        fn check(&self) -> Box<dyn Future<Item=bool, Error=Error> + Send>;
        fn clean_zone(&self, pbas: Range<PBA>, txgs: Range<TxgT>, txg: TxgT)
            -> Box<dyn Future<Item=(), Error=Error> + Send>;
        fn create(dml: Arc<D>, seq: bool, lzratio: f32, izratio: f32)
            -> MockTree<A, D, K, V>;
        fn dump(&self, f: &mut (dyn io::Write + 'static)) -> Result<(), Error>;
        fn flush(&self, txg: TxgT)
            -> Box<dyn Future<Item=(), Error=Error> + Send>;
        fn get(&self, k: K)
            -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>;
        fn insert(&self, k: K, v: V, txg: TxgT)
            -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>;
        fn last_key(&self)
            -> Box<dyn Future<Item=Option<K>, Error=Error> + Send>;
        fn open(dml: Arc<D>, seq: bool, on_disk: TreeOnDisk<A>)
            -> MockTree<A, D, K, V>;
        fn range<R, T>(&self, range: R) -> RangeQuery<A, D, K, T, V>
            where K: Borrow<T>,
                  R: RangeBounds<T> + 'static,
                  T: Ord + Clone + Send + 'static;
        fn range_delete<R, T>(&self, range: R, txg: TxgT)
            -> Box<dyn Future<Item=(), Error=Error> + Send>
            where K: Borrow<T>,
                  R: RangeBounds<T> + 'static,
                  T: Ord + Clone + Send + 'static;
        fn remove(&self, k: K, txg: TxgT)
            -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>;
        fn serialize(&self) -> Result<TreeOnDisk<A>, Error>;
    }
}
// LCOV_EXCL_STOP
