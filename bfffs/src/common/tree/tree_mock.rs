// vim: tw=80
// LCOV_EXCL_START

use crate::common::*;
use crate::common::{Error, TxgT};
use crate::common::dml::*;
use crate::common::tree::*;
use futures::{Async, Future, Poll, Stream};
use simulacrum::*;
use std::{
    borrow::Borrow,
    io,
    marker::PhantomData,
    ops::{Range, RangeBounds},
    sync::Arc
};

#[derive(Default)]
pub struct RangeQueryMock<A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A>,
          K: Key + Borrow<T>,
          T: Ord + Clone + Send,
          V: Value
{
    a: PhantomData<A>,
    d: PhantomData<D>,
    k: PhantomData<K>,
    t: PhantomData<T>,
    v: PhantomData<V>
}

impl<A, D, K, T, V> Stream for RangeQueryMock<A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A> + 'static,
          K: Key + Borrow<T>,
          T: Ord + Clone + Send + 'static,
          V: Value
{
    type Item = (K, V);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::Ready(None))
    }
}

pub struct TreeMock<A: Addr, D: DML<Addr=A>, K: Key, V: Value> {
    e: Expectations,
    a: PhantomData<A>,
    d: PhantomData<D>,
    k: PhantomData<K>,
    v: PhantomData<V>
}

impl<A: Addr, D: DML<Addr=A> + 'static, K: Key, V: Value> TreeMock<A, D, K, V> {
    pub fn check(&self) -> impl Future<Item=bool, Error=Error> + Send {
        self.e.was_called_returning::<(),
            Box<dyn Future<Item=bool, Error=Error> + Send>>
            ("check", ())
    }

    pub fn clean_zone(&self, pbas: Range<PBA>, txgs: Range<TxgT>, txg: TxgT)
        -> impl Future<Item=(), Error=Error> + Send
    {
        self.e.was_called_returning::<(Range<PBA>, Range<TxgT>, TxgT),
            Box<dyn Future<Item=(), Error=Error> + Send>>
            ("clean_zone", (pbas, txgs, txg))
    }

    pub fn expect_clean_zone(&mut self)
        -> Method<(Range<PBA>, Range<TxgT>, TxgT),
                   Box<dyn Future<Item=(), Error=Error> + Send>>
    {
        self.e.expect::<(Range<PBA>, Range<TxgT>, TxgT),
            Box<dyn Future<Item=(), Error=Error> + Send>>("clean_zone")
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn create(_dml: Arc<D>, _seq: bool, _lzratio: f32, _izratio: f32)
        -> Self
    {
        Self::default()
    }

    // No need to allow this method to be mocked; it's just for debugging
    pub fn dump(&self, _f: &mut io::Write) -> Result<(), Error> {
        Ok(())
    }

    pub fn flush(&self, txg: TxgT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        self.e.was_called_returning::<TxgT,
            Box<dyn Future<Item=(), Error=Error> + Send>>
            ("flush", txg)
    }

    pub fn expect_flush(&mut self) -> Method<TxgT,
        Box<dyn Future<Item=(), Error=Error> + Send>>
    {
        self.e.expect::<TxgT,
            Box<dyn Future<Item=(), Error=Error> + Send>>("flush")
    }

    pub fn get(&self, k: K) -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>
    {
        self.e.was_called_returning::<K,
            Box<dyn Future<Item=Option<V>, Error=Error> + Send>>
            ("get", k)
    }

    pub fn expect_get(&mut self) -> Method<K,
        Box<dyn Future<Item=Option<V>, Error=Error> + Send>>
    {
        self.e.expect::<K,
            Box<dyn Future<Item=Option<V>, Error=Error> + Send>>("get")
    }

    pub fn insert(&self, k: K, v: V, txg: TxgT)
        -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>
    {
        self.e.was_called_returning::<(K, V, TxgT),
            Box<dyn Future<Item=Option<V>, Error=Error> + Send>>
            ("insert", (k, v, txg))
    }

    pub fn expect_insert(&mut self) -> Method<(K, V, TxgT),
        Box<dyn Future<Item=Option<V>, Error=Error> + Send>>
    {
        self.e.expect::<(K, V, TxgT),
            Box<dyn Future<Item=Option<V>, Error=Error> + Send>>("insert")
    }

    pub fn last_key(&self) -> Box<dyn Future<Item=Option<K>, Error=Error> + Send> {
        self.e.was_called_returning::<(),
            Box<dyn Future<Item=Option<K>, Error=Error> + Send>>
            ("last_key", ())
    }

    pub fn expect_last_key(&mut self) -> Method<(),
        Box<dyn Future<Item=Option<K>, Error=Error> + Send>>
    {
        self.e.expect::<(),
            Box<dyn Future<Item=Option<K>, Error=Error> + Send>>("last_key")
    }

    //pub fn new() -> Self {
        //Self {
            //e: Expectations::new(),
            //a: PhantomData,
            //d: PhantomData,
            //k: PhantomData,
            //v: PhantomData,
        //}
    //}

    #[allow(clippy::needless_pass_by_value)]
    pub fn open(_dml: Arc<D>, _seq: bool, _on_disk: TreeOnDisk<A>) -> Self {
        Self::default()
    }

    pub fn range<R, T>(&self, range: R) -> RangeQueryMock<A, D, K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static
    {
        self.e.was_called_returning::<R, RangeQueryMock<A, D, K, T, V>>
            ("range", range)
    }

    pub fn expect_range<R, T>(&mut self)
        -> Method<R, RangeQueryMock<A, D, K, T, V>>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static
    {
        self.e.expect::<R, RangeQueryMock<A, D, K, T, V>>("range")
    }

    pub fn range_delete<R, T>(&self, range: R, txg: TxgT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static
    {
        self.e.was_called_returning::<(R, TxgT),
            Box<dyn Future<Item=(), Error=Error> + Send>>
            ("range_delete", (range, txg))
    }

    pub fn expect_range_delete<R, T>(&mut self)
        -> Method<(R, TxgT), Box<dyn Future<Item=(), Error=Error> + Send>>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static
    {
        self.e.expect::<(R, TxgT),
            Box<dyn Future<Item=(), Error=Error> + Send>>("range_delete")
    }

    pub fn remove(&self, k: K, txg: TxgT)
        -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>
    {
        self.e.was_called_returning::<(K, TxgT),
            Box<dyn Future<Item=Option<V>, Error=Error> + Send>>
            ("remove", (k, txg))
    }

    pub fn expect_remove(&mut self) -> Method<(K, TxgT),
        Box<dyn Future<Item=Option<V>, Error=Error> + Send>>
    {
        self.e.expect::<(K, TxgT),
            Box<dyn Future<Item=Option<V>, Error=Error> + Send>>("remove")
    }

    pub fn serialize(&self) -> Result<TreeOnDisk<A>, Error> {
        self.e.was_called_returning::<(), Result<TreeOnDisk<A>, Error>>
        ("serialize", ())
    }

    pub fn expect_serialize(&mut self)
        -> Method<(), Result<TreeOnDisk<A>, Error>>
    {
        self.e.expect::<(), Result<TreeOnDisk<A>, Error>>("serialize")
    }

    pub fn then(&mut self) -> &mut Self {
        self.e.then();
        self
    }
}

impl<A: Addr, D: DML<Addr=A> + 'static, K: Key, V: Value> Default for 
    TreeMock<A, D, K, V>
{
    fn default() -> Self {
        Self {
            e: Expectations::new(),
            a: PhantomData,
            d: PhantomData,
            k: PhantomData,
            v: PhantomData,
        }
    }
}

// XXX totally unsafe!  But Simulacrum doesn't support mocking Send traits.  So
// we have to cheat.  This works as long as RangeQueryMock and TreeMock are only
// used in single-threaded unit tests.
unsafe impl<A, D, K, T, V> Send for RangeQueryMock<A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A> + 'static,
          K: Key + Borrow<T>,
          T: Ord + Clone + Send + 'static,
          V: Value
{}
unsafe impl<A: Addr, D: DML<Addr=A>, K: Key, V: Value> Send for TreeMock<A, D, K, V> {}
unsafe impl<A: Addr, D: DML<Addr=A>, K: Key, V: Value> Sync for TreeMock<A, D, K, V> {}
// LCOV_EXCL_STOP