#![recursion_limit="256"]   // galvanic_test hits the recursion limit
// https://github.com/mindsbackyard/galvanic-test/pull/13
#![allow(clippy::unnecessary_mut_passed)]

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

mod common;

/// Helper to generate the runtime used by most tests
fn basic_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}
