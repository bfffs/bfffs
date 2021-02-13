#![type_length_limit="1640349"]
#![recursion_limit="256"]   // galvanic_test hits the recursion limit
// https://github.com/mindsbackyard/galvanic-test/pull/13
#![allow(clippy::unnecessary_mut_passed)]

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

/// Helper to generate the runtime used by most tests
fn basic_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}

mod clean_zone;
mod cluster;
mod database;
mod device_manager;
mod ddml;
mod fs;
mod idml;
mod pool;
mod raid;
mod vdev_block;
mod vdev_file;
