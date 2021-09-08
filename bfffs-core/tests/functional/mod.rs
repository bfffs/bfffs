// Temporarily allow this warning to reduce merge conflicts between the rstest
// and fuse3 branches.
#![allow(clippy::module_inception)]

// rstest_reuse must be imported at the crate root for macro reasons
// https://github.com/la10736/rstest/issues/128
use rstest_reuse;

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

/// Helper to generate the runtime used by most tests
fn basic_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
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
