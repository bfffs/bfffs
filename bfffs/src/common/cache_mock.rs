// vim: tw=80
// LCOV_EXCL_START
use crate::common::cache::*;
use simulacrum::*;

#[derive(Default)]
pub struct CacheMock {
    e: Expectations,
}
impl CacheMock {
    pub fn get<T: CacheRef>(&mut self, key: &Key) -> Option<Box<T>> {
        self.e.was_called_returning::<*const Key, Option<Box<T>>>
            ("get", key as *const Key)
    }

    pub fn expect_get<T: CacheRef>(&mut self) -> Method<*const Key, Option<Box<T>>> {
        self.e.expect::<*const Key, Option<Box<T>>>("get")
    }

    pub fn get_ref(&self, key: &Key) -> Option<Box<dyn CacheRef>> {
        self.e.was_called_returning::<*const Key, Option<Box<dyn CacheRef>>>
            ("get_ref", key as *const Key)
    }

    pub fn expect_get_ref(&mut self)
        -> Method<*const Key, Option<Box<dyn CacheRef>>>
    {
        self.e.expect::<*const Key, Option<Box<dyn CacheRef>>>("get_ref")
    }

    pub fn insert(&mut self, key: Key, buf: Box<dyn Cacheable>) {
        self.e.was_called_returning::<(Key, Box<dyn Cacheable>), ()>
            ("insert", (key, buf))
    }

    pub fn expect_insert(&mut self) -> Method<(Key, Box<dyn Cacheable>), ()> {
        self.e.expect::<(Key, Box<dyn Cacheable>), ()>("insert")
    }

    pub fn remove(&mut self, key: &Key) -> Option<Box<dyn Cacheable>> {
        self.e.was_called_returning::<*const Key, Option<Box<dyn Cacheable>>>
            ("remove", key as *const Key)
    }

    pub fn expect_remove(&mut self)
        -> Method<*const Key, Option<Box<dyn Cacheable>>>
    {
        self.e.expect::<*const Key, Option<Box<dyn Cacheable>>>("remove")
    }

    pub fn size(&self) -> usize {
        self.e.was_called_returning::<(), usize>("size", ())
    }

    pub fn then(&mut self) -> &mut Self {
        self.e.then();
        self
    }
}

// XXX totally unsafe!  But Simulacrum doesn't support mocking Send traits.  So
// we have to cheat.  This works as long as CacheMock is only used in
// single-threaded unit tests.
unsafe impl Send for CacheMock {}
// LCOV_EXCL_STOP
