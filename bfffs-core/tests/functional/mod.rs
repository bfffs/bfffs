// Temporarily allow this warning to reduce merge conflicts between the rstest
// and fuse3 branches.
#![allow(clippy::module_inception)]

// rstest_reuse must be imported at the crate root for macro reasons
// https://github.com/la10736/rstest/issues/128
#![allow(clippy::single_component_path_imports)]
use rstest_reuse;

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

mod clean_zone;
mod cluster;
mod controller;
mod database;
mod device_manager;
mod ddml;
mod fs;
mod idml;
mod mirror;
mod pool;
mod raid;
mod util;
mod vdev_block;
mod vdev_file;

use util::{Gnop, Md, PoolBuilder};
