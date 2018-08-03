// vim: tw=80
// LCOV_EXCL_START
use common::{LbaT, TxgT};
use common::dml::*;
use common::ddml::*;
use common::label::*;
use futures::{Future, Stream};
use nix::Error;
use simulacrum::*;

pub struct DDMLMock {
    e: Expectations
}

impl DDMLMock {
    pub fn allocated(&self) -> LbaT {
        self.e.was_called_returning::<(), LbaT>
            ("allocated", ())
    }

    pub fn expect_allocated(&mut self) -> Method<(), LbaT> {
        self.e.expect::<(), LbaT>("allocated")
    }

    pub fn new() -> Self {
        Self {
            e: Expectations::new()
        }
    }

    pub fn expect_delete(&mut self) -> Method<(*const DRP, TxgT),
        Box<Future<Item=(), Error=Error>>>
    {
        self.e.expect::<(*const DRP, TxgT),
            Box<Future<Item=(), Error=Error>>>
            ("delete")
    }

    pub fn expect_get<R: CacheRef>(&mut self) -> Method<*const DRP,
        Box<Future<Item=Box<R>, Error=Error>>>
    {
        self.e.expect::<*const DRP, Box<Future<Item=Box<R>, Error=Error>>>
            ("get")
    }

    pub fn expect_pop<T: Cacheable, R:CacheRef>(&mut self)
        -> Method<(*const DRP, TxgT), Box<Future<Item=Box<T>, Error=Error>>>
    {
        self.e.expect::<(*const DRP, TxgT),
                        Box<Future<Item=Box<T>, Error=Error>>>
            ("pop")
    }

    pub fn expect_put<T: Cacheable>(&mut self) -> Method<(T, Compression, TxgT),
        Box<Future<Item=DRP, Error=Error>>>
    {
        self.e.expect::<(T, Compression, TxgT),
                        Box<Future<Item=DRP, Error=Error>>>
            ("put")
    }

    pub fn expect_sync_all(&mut self)
        -> Method<TxgT, Box<Future<Item=(), Error=Error>>>
    {
        self.e.expect::<TxgT, Box<Future<Item=(), Error=Error>>>("sync_all")
    }

    pub fn get_direct<T: Cacheable>(&self, drp: &DRP)
        -> Box<Future<Item=Box<T>, Error=Error>> {
        self.e.was_called_returning::<*const DRP,
            Box<Future<Item=Box<T>, Error=Error>>>
            ("get_direct", drp as *const DRP)
    }

    pub fn expect_get_direct<T: Cacheable>(&mut self) -> Method<*const DRP,
        Box<Future<Item=Box<T>, Error=Error>>>
    {
        self.e.expect::<*const DRP, Box<Future<Item=Box<T>, Error=Error>>>
            ("get_direct")
    }

    pub fn list_closed_zones(&self)
        -> Box<Stream<Item=ClosedZone, Error=Error>>
    {
        self.e.was_called_returning::<(),
            Box<Stream<Item=ClosedZone, Error=Error>>>
            ("list_closed_zones", ())
    }

    pub fn expect_list_closed_zones(&mut self)
        -> Method<(), Box<Stream<Item=ClosedZone, Error=Error>>>
    {
        self.e.expect::<(), Box<Stream<Item=ClosedZone, Error=Error>>>
            ("list_closed_zones")
    }

    pub fn pop_direct<T: Cacheable>(&self, drp: &DRP)
        -> Box<Future<Item=Box<T>, Error=Error>>
    {
        self.e.was_called_returning::<*const DRP,
            Box<Future<Item=Box<T>, Error=Error>>>
            ("pop_direct", drp as *const DRP)
    }

    pub fn expect_pop_direct<T: Cacheable>(&mut self) -> Method<*const DRP,
        Box<Future<Item=Box<T>, Error=Error>>>
    {
        self.e.expect::<*const DRP, Box<Future<Item=Box<T>, Error=Error>>>
            ("pop_direct")
    }

    pub fn put_direct<T: Cacheable>(&self, cacheable: T,
                                    compression: Compression, txg: TxgT)
        -> Box<Future<Item=(DRP, T), Error=Error>>
    {
        self.e.was_called_returning::<(T, Compression, TxgT),
                                      Box<Future<Item=(DRP, T), Error=Error>>>
            ("put_direct", (cacheable, compression, txg))
    }

    pub fn expect_put_direct<T: Cacheable>(&mut self)
        -> Method<(T, Compression, TxgT),
        Box<Future<Item=(DRP, T), Error=Error>>>
    {
        self.e.expect::<(T, Compression, TxgT),
                        Box<Future<Item=(DRP, T), Error=Error>>>
            ("put_direct")
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
        -> impl Future<Item=(), Error=Error>
    {
        self.e.was_called_returning::<LabelWriter,
                                      Box<Future<Item=(), Error=Error>>>
            ("write_label", labeller)
    }

    pub fn expect_write_label(&mut self)
        -> Method<LabelWriter, Box<Future<Item=(), Error=Error>>>
    {
        self.e.expect::<LabelWriter, Box<Future<Item=(), Error=Error>>>
            ("write_label")
    }
}

impl DML for DDMLMock {
    type Addr = DRP;

    fn delete(&self, drp: &DRP, txg: TxgT)
        -> Box<Future<Item=(), Error=Error> + Send>
    {
        self.e.was_called_returning::<(*const DRP, TxgT),
            Box<Future<Item=(), Error=Error>>>
            ("delete", (drp as *const DRP, txg))
    }

    fn evict(&self, drp: &DRP) {
        self.e.was_called::<*const DRP, ()>("evict", drp as *const DRP)
    }

    fn get<T: Cacheable, R: CacheRef>(&self, drp: &DRP)
        -> Box<Future<Item=Box<R>, Error=Error> + Send>
    {
        self.e.was_called_returning::<*const DRP,
            Box<Future<Item=Box<R>, Error=Error>>>
            ("get", drp as *const DRP)
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, drp: &DRP, txg: TxgT)
        -> Box<Future<Item=Box<T>, Error=Error> + Send>
    {
        self.e.was_called_returning::<(*const DRP, TxgT),
            Box<Future<Item=Box<T>, Error=Error>>>
            ("pop", (drp as *const DRP, txg))
    }

    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                         txg:TxgT)
        -> Box<Future<Item=DRP, Error=Error> + Send>
    {
        self.e.was_called_returning::<(T, Compression, TxgT),
                                      (Box<Future<Item=DRP, Error=Error>>)>
            ("put", (cacheable, compression, txg))
    }

    fn sync_all(&self, txg: TxgT) -> Box<Future<Item=(), Error=Error> + Send>
    {
        self.e.was_called_returning::<TxgT, Box<Future<Item=(), Error=Error>>>
            ("sync_all", txg)
    }
}
// LCOV_EXCL_STOP
