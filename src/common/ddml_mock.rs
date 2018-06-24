// LCOV_EXCL_START
use common::dml::*;
use common::ddml::*;
use futures::Future;
use nix::Error;
use simulacrum::*;

#[cfg(test)]
pub struct DDMLMock {
    e: Expectations
}
#[cfg(test)]
impl DDMLMock {
    pub fn new() -> Self {
        Self {
            e: Expectations::new()
        }
    }

    pub fn expect_delete(&mut self) -> Method<*const DRP, ()> {
        self.e.expect::<*const DRP, ()>("delete")
    }

    pub fn expect_get<R: CacheRef>(&mut self) -> Method<*const DRP,
        Box<Future<Item=Box<R>, Error=Error>>>
    {
        self.e.expect::<*const DRP, Box<Future<Item=Box<R>, Error=Error>>>
            ("get")
    }

    pub fn expect_pop<T: Cacheable, R:CacheRef>(&mut self) -> Method<*const DRP,
        Box<Future<Item=Box<T>, Error=Error>>>
    {
        self.e.expect::<*const DRP, Box<Future<Item=Box<T>, Error=Error>>>
            ("pop")
    }

    pub fn expect_put<T: Cacheable>(&mut self) -> Method<(T, Compression),
        (DRP, Box<Future<Item=(), Error=Error>>)>
    {
        self.e.expect::<(T, Compression),
                        (DRP, Box<Future<Item=(), Error=Error>>)>
            ("put")
    }

    pub fn expect_sync_all(&mut self)
        -> Method<(), Box<Future<Item=(), Error=Error>>>
    {
        self.e.expect::<(), Box<Future<Item=(), Error=Error>>>("sync_all")
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
                                    compression: Compression)
        -> (DRP, Box<Future<Item=T, Error=Error>>)
    {
        self.e.was_called_returning::<(T, Compression),
                                      (DRP, Box<Future<Item=T, Error=Error>>)>
            ("put_direct", (cacheable, compression))
    }

    pub fn expect_put_direct<T: Cacheable>(&mut self)
        -> Method<(T, Compression),
        (DRP, Box<Future<Item=T, Error=Error>>)>
    {
        self.e.expect::<(T, Compression),
                        (DRP, Box<Future<Item=T, Error=Error>>)>
            ("put_direct")
    }

    pub fn then(&mut self) -> &mut Self {
        self.e.then();
        self
    }
}

#[cfg(test)]
impl DML for DDMLMock {
    type Addr = DRP;

    fn delete(&self, drp: &DRP) {
        self.e.was_called::<*const DRP, ()>("delete", drp as *const DRP)
    }

    fn evict(&self, drp: &DRP) {
        self.e.was_called::<*const DRP, ()>("evict", drp as *const DRP)
    }

    fn get<T: Cacheable, R: CacheRef>(&self, drp: &DRP)
        -> Box<Future<Item=Box<R>, Error=Error>> {
        self.e.was_called_returning::<*const DRP,
            Box<Future<Item=Box<R>, Error=Error>>>
            ("get", drp as *const DRP)
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, drp: &DRP)
        -> Box<Future<Item=Box<T>, Error=Error>>
    {
        self.e.was_called_returning::<*const DRP,
            Box<Future<Item=Box<T>, Error=Error>>>
            ("pop", drp as *const DRP)
    }

    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression)
        -> (DRP, Box<Future<Item=(), Error=Error>>)
    {
        self.e.was_called_returning::<(T, Compression),
                                      (DRP, Box<Future<Item=(), Error=Error>>)>
            ("put", (cacheable, compression))
    }

    fn sync_all(&self) -> Box<Future<Item=(), Error=Error>> {
        self.e.was_called_returning::<(), Box<Future<Item=(), Error=Error>>>
            ("sync_all", ())
    }
}
// LCOV_EXCL_STOP

