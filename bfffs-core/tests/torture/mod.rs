use std::{
    env,
    str::FromStr,
};

#[macro_use]
pub mod util {
    include!("../functional/util.rs");
}

mod fs;
mod vdev_raid;

use util::PoolBuilder;

fn test_scale() -> f64 {
    env::var("BFFFS_TORTURE_SCALE")
        .map(|s| f64::from_str(&s)
             .expect("BFFFS_TORTURE_SCALE must be a float")
         ).unwrap_or(1.0)
}
