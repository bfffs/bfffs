// vim: tw=80
// LCOV_EXCL_START
use common::*;
use common::dml::*;
//use common::ddml::*;
use common::idml::*;
use futures::{Future, Stream};
use nix::Error;
use simulacrum::*;

pub struct IDMLMock {
    e: Expectations
}

impl IDMLMock {
    pub fn clean_zone(&self, zone: ClosedZone, txg: TxgT)
        -> impl Future<Item=(), Error=Error>
    {
         self.e.was_called_returning::<(ClosedZone, TxgT),
            Box<Future<Item=(), Error=Error>>>
            ("clean_zone", (zone, txg))
    }

    pub fn expect_clean_zone(&mut self) -> Method<(ClosedZone, TxgT),
        Box<Future<Item=(), Error=Error>>>
    {
        self.e.expect::<(ClosedZone, TxgT), Box<Future<Item=(), Error=Error>>>
            ("clean_zone")
    }

    pub fn expect_delete(&mut self) -> Method<(*const RID, TxgT),
        Box<Future<Item=(), Error=Error>>>
    {
        self.e.expect::<(*const RID, TxgT),
            Box<Future<Item=(), Error=Error>>>
            ("delete")
    }

    pub fn expect_get<R: CacheRef>(&mut self) -> Method<*const RID,
        Box<Future<Item=Box<R>, Error=Error>>>
    {
        self.e.expect::<*const RID, Box<Future<Item=Box<R>, Error=Error>>>
            ("get")
    }

    pub fn expect_list_closed_zones(&mut self)
        -> Method<(), Box<Stream<Item=ClosedZone, Error=Error>>>
    {
        self.e.expect::<(), Box<Stream<Item=ClosedZone, Error=Error>>>
            ("list_closed_zones")
    }

    pub fn expect_pop<T: Cacheable, R:CacheRef>(&mut self)
        -> Method<(*const RID, TxgT), Box<Future<Item=Box<T>, Error=Error>>>
    {
        self.e.expect::<(*const RID, TxgT),
                        Box<Future<Item=Box<T>, Error=Error>>>
            ("pop")
    }

    pub fn expect_put<T: Cacheable>(&mut self) -> Method<(T, Compression, TxgT),
        Box<Future<Item=RID, Error=Error>>>
    {
        self.e.expect::<(T, Compression, TxgT),
                        Box<Future<Item=RID, Error=Error>>>
            ("put")
    }

    pub fn expect_sync_all(&mut self)
        -> Method<TxgT, Box<Future<Item=(), Error=Error>>>
    {
        self.e.expect::<TxgT, Box<Future<Item=(), Error=Error>>>("sync_all")
    }

    pub fn expect_sync_transaction(&mut self)
        -> Method<(), Box<Future<Item=(), Error=Error>>>
    {
        self.e.expect::<(),
                        Box<Future<Item=(), Error=Error>>>("sync_transaction")
    }

    pub fn expect_txg(&mut self)
        -> Method<(), Box<Future<Item=&'static TxgT, Error=Error>>>
    {
        self.e.expect::<(), Box<Future<Item=&'static TxgT, Error=Error>>>("txg")
    }

    pub fn list_closed_zones(&self)
        -> Box<Stream<Item=ClosedZone, Error=Error>>
    {
        self.e.was_called_returning::<(),
            Box<Stream<Item=ClosedZone, Error=Error>>>
            ("list_closed_zones", ())
    }

    pub fn new() -> Self {
        Self {
            e: Expectations::new()
        }
    }

    pub fn txg(&self) -> Box<Future<Item=&'static TxgT, Error=Error>> {
        self.e.was_called_returning::<(),
            Box<Future<Item=&'static TxgT, Error=Error>>>
            ("txg", ())
    }

    pub fn sync_transaction(&self) -> impl Future<Item=(), Error=Error> {
        self.e.was_called_returning::<(), Box<Future<Item=(), Error=Error>>>
            ("sync_transaction", ())
    }

    pub fn then(&mut self) -> &mut Self {
        self.e.then();
        self
    }
}

impl DML for IDMLMock {
    type Addr = RID;

    fn delete(&self, rid: &RID, txg: TxgT)
        -> Box<Future<Item=(), Error=Error>>
    {
        self.e.was_called_returning::<(*const RID, TxgT),
            Box<Future<Item=(), Error=Error>>>
            ("delete", (rid as *const RID, txg))
    }

    fn evict(&self, rid: &RID) {
        self.e.was_called::<*const RID, ()>("evict", rid as *const RID)
    }

    fn get<T: Cacheable, R: CacheRef>(&self, rid: &RID)
        -> Box<Future<Item=Box<R>, Error=Error>> {
        self.e.was_called_returning::<*const RID,
            Box<Future<Item=Box<R>, Error=Error>>>
            ("get", rid as *const RID)
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, rid: &RID, txg: TxgT)
        -> Box<Future<Item=Box<T>, Error=Error>>
    {
        self.e.was_called_returning::<(*const RID, TxgT),
            Box<Future<Item=Box<T>, Error=Error>>>
            ("pop", (rid as *const RID, txg))
    }

    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                         txg:TxgT)
        -> Box<Future<Item=RID, Error=Error>>
    {
        self.e.was_called_returning::<(T, Compression, TxgT),
                                      Box<Future<Item=RID, Error=Error>>>
            ("put", (cacheable, compression, txg))
    }

    fn sync_all(&self, txg: TxgT) -> Box<Future<Item=(), Error=Error>> {
        self.e.was_called_returning::<TxgT, Box<Future<Item=(), Error=Error>>>
            ("sync_all", txg)
    }
}
// LCOV_EXCL_STOP
