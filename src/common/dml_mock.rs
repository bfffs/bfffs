// vim: tw=80
// LCOV_EXCL_START
use crate::common::{Error, TxgT};
use crate::common::cache::{Cacheable, CacheRef};
use crate::common::dml::*;
use futures::Future;
use simulacrum::*;
use std::any;

/// A very simple mock object that implements `DML` and nothing else.
pub struct DMLMock {
    e: Expectations,
}

impl DMLMock {
    pub fn expect_delete(&mut self) -> Method<(*const u32, TxgT),
        Box<Future<Item=(), Error=Error> + Send>>
    {
        self.e.expect::<(*const u32, TxgT),
            Box<Future<Item=(), Error=Error> + Send>>
            ("delete")
    }

    pub fn expect_get<R: CacheRef>(&mut self) -> Method<*const u32,
        Box<Future<Item=Box<R>, Error=Error> + Send>>
    {
        self.e.expect::<*const u32,
                        Box<Future<Item=Box<R>, Error=Error> + Send>>
            ("get")
    }

    pub fn expect_pop<T: Cacheable, R:CacheRef>(&mut self)
        -> Method<(*const u32, TxgT),
                  Box<Future<Item=Box<T>, Error=Error> + Send>>
    {
        self.e.expect::<(*const u32, TxgT),
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
        Box<Future<Item=u32, Error=Error> + Send>>
    {
        self.e.expect::<(T, Compression, TxgT),
                        Box<Future<Item=u32, Error=Error> + Send>>
            (&DMLMock::generic_method_name::<T>())
    }

    fn generic_method_name<T: 'static + ?Sized>() -> String {
        format!("put::<{:?}>", any::TypeId::of::<T>())
    }

    pub fn new() -> Self {
        Self {
            e: Expectations::new(),
        }
    }

    pub fn then(&mut self) -> &mut Self {
        self.e.then();
        self
    }
}

impl DML for DMLMock {
    type Addr = u32;

    fn delete(&self, addr: &u32, txg: TxgT)
        -> Box<Future<Item=(), Error=Error> + Send>
    {
        self.e.was_called_returning::<(*const u32, TxgT),
            Box<Future<Item=(), Error=Error> + Send>>
            ("delete", (addr as *const u32, txg))
    }

    fn evict(&self, addr: &u32) {
        self.e.was_called::<*const u32, ()>("evict", addr as *const u32)
    }

    fn get<T: Cacheable, R: CacheRef>(&self, addr: &u32)
        -> Box<Future<Item=Box<R>, Error=Error> + Send>
    {
        self.e.was_called_returning::<*const u32,
            Box<Future<Item=Box<R>, Error=Error> + Send>>
            ("get", addr as *const u32)
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, addr: &u32, txg: TxgT)
        -> Box<Future<Item=Box<T>, Error=Error> + Send>
    {
        self.e.was_called_returning::<(*const u32, TxgT),
            Box<Future<Item=Box<T>, Error=Error> + Send>>
            ("pop", (addr as *const u32, txg))
    }

    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                         txg:TxgT)
        -> Box<Future<Item=u32, Error=Error> + Send>
    {
        let method_name = DMLMock::generic_method_name::<T>();
        self.e.was_called_returning::<(T, Compression, TxgT),
            (Box<Future<Item=u32, Error=Error> + Send>)>
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
// we have to cheat.  This works as long as DMLMock is only used in
// single-threaded unit tests.
unsafe impl Send for DMLMock {}
unsafe impl Sync for DMLMock {}
// LCOV_EXCL_STOP
