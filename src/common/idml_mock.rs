// vim: tw=80
// LCOV_EXCL_START
use common::*;
use common::dml::*;
use common::idml::*;
use common::label::*;
use futures::{Future, IntoFuture, Stream};
use simulacrum::*;

pub struct IDMLMock {
    e: Expectations
}

impl IDMLMock {
    // This method is impossible to perfectly mock with Simulacrum, because f is
    // typically a closure, and closures cannot be named.  If f were Boxed, then
    // it would work.  But I don't want to impose that limitation on the
    // production code.  Instead, we'll use special logic in advance_transaction
    // and only mock the txg used.
    pub fn advance_transaction<B, F>(&self, f: F)
        -> impl Future<Item=(), Error=Error>
        where F: FnOnce(TxgT) -> B + Send + 'static,
              B: IntoFuture<Item = (), Error = Error>
    {
         let txg = self.e.was_called_returning::<(), TxgT>
             ("advance_transaction", ());
         f(txg).into_future()
    }

    pub fn expect_advance_transaction(&mut self) -> Method<(), TxgT>
    {
        self.e.expect::<(), TxgT>("advance_transaction")
    }

    pub fn allocated(&self) -> LbaT {
         self.e.was_called_returning::<(), LbaT> ("allocated", ())
    }

    pub fn expect_allocated(&mut self) -> Method<(), LbaT> {
        self.e.expect::<(), LbaT>("allocated")
    }

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
        -> Method<TxgT, Box<Future<Item=(), Error=Error> + Send>>
    {
        self.e.expect::<TxgT, Box<Future<Item=(), Error=Error> + Send>>
            ("sync_all")
    }

    pub fn expect_sync_transaction(&mut self)
        -> Method<(), Box<Future<Item=(), Error=Error>>>
    {
        self.e.expect::<(),
                        Box<Future<Item=(), Error=Error>>>("sync_transaction")
    }

    pub fn expect_txg(&mut self)
        -> Method<(), Box<Future<Item=&'static TxgT, Error=Error> + Send>>
    {
        self.e.expect::<(), Box<Future<Item=&'static TxgT, Error=Error> + Send>>
            ("txg")
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

    pub fn size(&self) -> LbaT {
         self.e.was_called_returning::<(), LbaT> ("size", ())
    }

    pub fn expect_size(&mut self) -> Method<(), LbaT> {
        self.e.expect::<(), LbaT>("size")
    }

    pub fn txg(&self) -> Box<Future<Item=&'static TxgT, Error=Error> + Send> {
        self.e.was_called_returning::<(),
            Box<Future<Item=&'static TxgT, Error=Error> + Send>>
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

    pub fn write_label(&self, labeller: LabelWriter, txg: TxgT)
        -> Box<Future<Item=(), Error=Error> + Send>
    {
        self.e.was_called_returning::<(LabelWriter, TxgT),
            Box<Future<Item=(), Error=Error> + Send>>
            ("write_label", (labeller, txg))
    }

    pub fn expect_write_label(&mut self)
        -> Method<(LabelWriter, TxgT), Box<Future<Item=(), Error=Error> + Send>>
    {
        self.e.expect::<(LabelWriter, TxgT),
            Box<Future<Item=(), Error=Error> + Send>>
            ("write_label")
    }

}

impl DML for IDMLMock {
    type Addr = RID;

    fn delete(&self, rid: &RID, txg: TxgT)
        -> Box<Future<Item=(), Error=Error> + Send>
    {
        self.e.was_called_returning::<(*const RID, TxgT),
            Box<Future<Item=(), Error=Error> + Send>>
            ("delete", (rid as *const RID, txg))
    }

    fn evict(&self, rid: &RID) {
        self.e.was_called::<*const RID, ()>("evict", rid as *const RID)
    }

    fn get<T: Cacheable, R: CacheRef>(&self, rid: &RID)
        -> Box<Future<Item=Box<R>, Error=Error> + Send>
    {
        self.e.was_called_returning::<*const RID,
            Box<Future<Item=Box<R>, Error=Error> + Send>>
            ("get", rid as *const RID)
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, rid: &RID, txg: TxgT)
        -> Box<Future<Item=Box<T>, Error=Error> + Send>
    {
        self.e.was_called_returning::<(*const RID, TxgT),
            Box<Future<Item=Box<T>, Error=Error> + Send>>
            ("pop", (rid as *const RID, txg))
    }

    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                         txg:TxgT)
        -> Box<Future<Item=RID, Error=Error> + Send>
    {
        self.e.was_called_returning::<(T, Compression, TxgT),
                                      Box<Future<Item=RID, Error=Error> + Send>>
            ("put", (cacheable, compression, txg))
    }

    fn sync_all(&self, txg: TxgT) -> Box<Future<Item=(), Error=Error> + Send> {
        self.e.was_called_returning::<TxgT,
                                      Box<Future<Item=(), Error=Error> + Send>>
            ("sync_all", txg)
    }
}

// XXX totally unsafe!  But Simulacrum doesn't support mocking Send traits.  So
// we have to cheat.  This works as long as IDMLMock is only used in
// single-threaded unit tests.
unsafe impl Send for IDMLMock {}
unsafe impl Sync for IDMLMock {}
// LCOV_EXCL_STOP
