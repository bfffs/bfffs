// vim: tw=80
// LCOV_EXCL_START
use crate::common::{ClusterT, Error, LbaT, TxgT, ZoneT};
use crate::common::cache::{Cacheable, CacheRef};
use crate::common::dml::*;
use crate::common::ddml::*;
use crate::common::label::*;
use futures::{Future, Stream};
use simulacrum::*;
use std::{
    any,
    borrow::Borrow,
};

#[derive(Default)]
pub struct DDMLMock {
    e: Expectations,
}

impl DDMLMock {
    pub fn allocated(&self) -> LbaT {
        self.e.was_called_returning::<(), LbaT>
            ("allocated", ())
    }

    pub fn expect_allocated(&mut self) -> Method<(), LbaT> {
        self.e.expect::<(), LbaT>("allocated")
    }

    pub fn assert_clean_zone(&self, cluster: ClusterT, zone: ZoneT, txg: TxgT) {
        self.e.was_called_returning::<(ClusterT, ZoneT, TxgT), ()>
            ("assert_clean_zone", (cluster, zone, txg))
    }

    pub fn expect_assert_clean_zone(&mut self)
        -> Method<(ClusterT, ZoneT, TxgT), ()>
    {
        self.e.expect::<(ClusterT, ZoneT, TxgT), ()>("assert_clean_zone")
    }

    pub fn expect_delete(&mut self) -> Method<(*const DRP, TxgT),
        Box<Future<Item=(), Error=Error> + Send>>
    {
        self.e.expect::<(*const DRP, TxgT),
            Box<Future<Item=(), Error=Error> + Send>>
            ("delete")
    }

    pub fn delete_direct(&self, drp: &DRP, txg: TxgT)
        -> Box<Future<Item=(), Error=Error> + Send>
    {
        self.e.was_called_returning::<(*const DRP, TxgT),
            Box<Future<Item=(), Error=Error> + Send>>
            ("delete_direct", (drp as *const DRP, txg))
    }

    pub fn expect_delete_direct(&mut self) -> Method<(*const DRP, TxgT),
        Box<Future<Item=(), Error=Error> + Send>>
    {
        self.e.expect::<(*const DRP, TxgT),
            Box<Future<Item=(), Error=Error> + Send>>
            ("delete_direct")
    }

    pub fn expect_get<R: CacheRef>(&mut self) -> Method<*const DRP,
        Box<Future<Item=Box<R>, Error=Error> + Send>>
    {
        self.e.expect::<*const DRP,
                        Box<Future<Item=Box<R>, Error=Error> + Send>>
            ("get")
    }

    pub fn expect_pop<T: Cacheable, R:CacheRef>(&mut self)
        -> Method<(*const DRP, TxgT),
                  Box<Future<Item=Box<T>, Error=Error> + Send>>
    {
        self.e.expect::<(*const DRP, TxgT),
                        Box<Future<Item=Box<T>, Error=Error> + Send>>
            ("pop")
    }

    // Due to a bug in Simulacrum, mocking DDML::put is way overcomplicated.
    //
    // Simulacrum can't mock a single generic method with different type
    // parameters more than once in the same test.  The workaround is to
    // dynamically construct the method name.
    // https://github.com/pcsm/simulacrum/issues/55"]
    pub fn expect_put<T: Cacheable>(&mut self) -> Method<(T, Compression, TxgT),
        Box<Future<Item=DRP, Error=Error> + Send>>
    {
        self.e.expect::<(T, Compression, TxgT),
                        Box<Future<Item=DRP, Error=Error> + Send>>
            (&DDMLMock::generic_method_name::<T>())
    }

    pub fn expect_sync_all(&mut self)
        -> Method<TxgT, Box<Future<Item=(), Error=Error> + Send>>
    {
        self.e.expect::<TxgT,
                        Box<Future<Item=(), Error=Error> + Send>>("sync_all")
    }

    fn generic_method_name<T: 'static + ?Sized>() -> String {
        format!("put::<{:?}>", any::TypeId::of::<T>())
    }

    pub fn get_direct<T: Cacheable>(&self, drp: &DRP)
        -> Box<Future<Item=Box<T>, Error=Error> + Send> {
        self.e.was_called_returning::<*const DRP,
            Box<Future<Item=Box<T>, Error=Error> + Send>>
            ("get_direct", drp as *const DRP)
    }

    pub fn expect_get_direct<T: Cacheable>(&mut self) -> Method<*const DRP,
        Box<Future<Item=Box<T>, Error=Error> + Send>>
    {
        self.e.expect::<*const DRP,
                        Box<Future<Item=Box<T>, Error=Error> + Send>>
            ("get_direct")
    }

    pub fn list_closed_zones(&self)
        -> Box<Stream<Item=ClosedZone, Error=Error> + Send>
    {
        self.e.was_called_returning::<(),
            Box<Stream<Item=ClosedZone, Error=Error> + Send>>
            ("list_closed_zones", ())
    }

    pub fn expect_list_closed_zones(&mut self)
        -> Method<(), Box<Stream<Item=ClosedZone, Error=Error> + Send>>
    {
        self.e.expect::<(), Box<Stream<Item=ClosedZone, Error=Error> + Send>>
            ("list_closed_zones")
    }

    pub fn pop_direct<T: Cacheable>(&self, drp: &DRP)
        -> Box<Future<Item=Box<T>, Error=Error> + Send>
    {
        self.e.was_called_returning::<*const DRP,
            Box<Future<Item=Box<T>, Error=Error> + Send>>
            ("pop_direct", drp as *const DRP)
    }

    pub fn expect_pop_direct<T: Cacheable>(&mut self) -> Method<*const DRP,
        Box<Future<Item=Box<T>, Error=Error> + Send>>
    {
        self.e.expect::<*const DRP,
                        Box<Future<Item=Box<T>, Error=Error> + Send>>
            ("pop_direct")
    }

    pub fn put_direct<T>(&self, cacheref: &T, compression: Compression,
                         txg: TxgT)
        -> Box<Future<Item=DRP, Error=Error> + Send>
        where T: Borrow<CacheRef> + 'static
    {
        let ptr = cacheref as *const T;
        self.e.was_called_returning::<(*const T, Compression, TxgT),
            Box<Future<Item=DRP, Error=Error> + Send>>
            ("put_direct", (ptr, compression, txg))
    }

    pub fn expect_put_direct<T>(&mut self) -> Method<(*const T, Compression, TxgT),
        Box<Future<Item=DRP, Error=Error> + Send>>
        where T: Borrow<CacheRef> + 'static
    {
        self.e.expect::<(*const T, Compression, TxgT),
                        Box<Future<Item=DRP, Error=Error> + Send>>
            ("put_direct")
    }

    pub fn shutdown(&self) {
        self.e.was_called_returning::<(), ()>("shutdown", ())
    }

    pub fn size(&self) -> LbaT {
        self.e.was_called_returning::<(), LbaT>("size", ())
    }

    pub fn expect_size(&mut self) -> Method<(), LbaT> {
        self.e.expect::<(), LbaT>("size")
    }

    pub fn then(&mut self) -> &mut Self {
        self.e.then();
        self
    }

    pub fn write_label(&self, labeller: LabelWriter)
        -> impl Future<Item=(), Error=Error> + Send
    {
        self.e.was_called_returning::<LabelWriter,
                                      Box<Future<Item=(), Error=Error> + Send>>
            ("write_label", labeller)
    }

    pub fn expect_write_label(&mut self)
        -> Method<LabelWriter, Box<Future<Item=(), Error=Error> + Send>>
    {
        self.e.expect::<LabelWriter, Box<Future<Item=(), Error=Error> + Send>>
            ("write_label")
    }
}

impl DML for DDMLMock {
    type Addr = DRP;

    fn delete(&self, drp: &DRP, txg: TxgT)
        -> Box<Future<Item=(), Error=Error> + Send>
    {
        self.e.was_called_returning::<(*const DRP, TxgT),
            Box<Future<Item=(), Error=Error> + Send>>
            ("delete", (drp as *const DRP, txg))
    }

    fn evict(&self, drp: &DRP) {
        self.e.was_called::<*const DRP, ()>("evict", drp as *const DRP)
    }

    fn get<T: Cacheable, R: CacheRef>(&self, drp: &DRP)
        -> Box<Future<Item=Box<R>, Error=Error> + Send>
    {
        self.e.was_called_returning::<*const DRP,
            Box<Future<Item=Box<R>, Error=Error> + Send>>
            ("get", drp as *const DRP)
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, drp: &DRP, txg: TxgT)
        -> Box<Future<Item=Box<T>, Error=Error> + Send>
    {
        self.e.was_called_returning::<(*const DRP, TxgT),
            Box<Future<Item=Box<T>, Error=Error> + Send>>
            ("pop", (drp as *const DRP, txg))
    }

    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                         txg:TxgT)
        -> Box<Future<Item=DRP, Error=Error> + Send>
    {
        let method_name = DDMLMock::generic_method_name::<T>();
        self.e.was_called_returning::<(T, Compression, TxgT),
            (Box<Future<Item=DRP, Error=Error> + Send>)>
            (&method_name, (cacheable, compression, txg))
    }

    fn sync_all(&self, txg: TxgT) -> Box<Future<Item=(), Error=Error> + Send>
    {
        self.e.was_called_returning::<TxgT,
                                      Box<Future<Item=(), Error=Error> + Send>>
            ("sync_all", txg)
    }
}

// XXX totally unsafe!  But Simulacrum doesn't support mocking Send traits.  So
// we have to cheat.  This works as long as DDMLMock is only used in
// single-threaded unit tests.
unsafe impl Send for DDMLMock {}
unsafe impl Sync for DDMLMock {}
// LCOV_EXCL_STOP
