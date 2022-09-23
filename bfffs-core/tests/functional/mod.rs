// Temporarily allow this warning to reduce merge conflicts between the rstest
// and fuse3 branches.
#![allow(clippy::module_inception)]

// rstest_reuse must be imported at the crate root for macro reasons
// https://github.com/la10736/rstest/issues/128
#![allow(clippy::single_component_path_imports)]
use rstest_reuse;

use std::{
    num::NonZeroU64,
    path::PathBuf
};

use itertools::Itertools;
use tempfile::{Builder, TempDir};

use bfffs_core::{
    cluster::Cluster,
    mirror::Mirror,
    pool::Pool,
};

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

/// Helper to create a fresh pool
#[derive(Debug)]
struct PoolBuilder {
    /// Number of disks
    n: usize,
    /// Disks per mirror
    m: usize,
    /// Mirrors per RAID stripe
    k: i16,
    /// RAID redundancy level
    f: i16,
    /// Number of clusters in the pool
    nclusters: usize,
    /// Size of each file in bytes
    fsize: u64,
    /// RAID chunk size
    cs: Option<NonZeroU64>,
    /// Pool name
    name: &'static str,
    /// LBAs per zone
    zone_size: Option<NonZeroU64>
}

impl PoolBuilder {
    fn build(&self) -> (TempDir, Vec<PathBuf>, Pool) {
        let mirrors_per_cluster = (self.n / self.m) / self.nclusters;
        assert_eq!(self.n % self.m, 0);
        assert!(mirrors_per_cluster >= self.k as usize);
        assert!(self.k > self.f);
        assert_eq!((self.n / self.m) % self.nclusters, 0);

        let tempdir = Builder::new()
            .prefix("bfffs_functional_test")
            .tempdir()
            .unwrap();
        let paths = (0..self.n).map(|i| {
            let fname = format!("{}/vdev.{}", tempdir.path().display(), i);
            let file = std::fs::File::create(&fname).unwrap();
            file.set_len(self.fsize).unwrap();
            PathBuf::from(fname)
        }).collect::<Vec<_>>();
        let clusters = paths.chunks(self.m)
            .map(|mpaths| {
                Mirror::create(mpaths, self.zone_size).unwrap()
            })
            .chunks(mirrors_per_cluster)
            .into_iter()
            .map(|miter| {
                let mirrors = miter.collect::<Vec<_>>();
                assert_eq!(mirrors.len(), mirrors_per_cluster);
                let raid = bfffs_core::raid::create(self.cs, self.k, self.f,
                                                    mirrors);
                Cluster::create(raid)
            }).collect::<Vec<_>>();
        let pool = Pool::create(String::from(self.name), clusters);
        (tempdir, paths, pool)
    }

    fn chunksize(&mut self, cs: u64) -> &mut Self {
        self.cs = NonZeroU64::new(cs);
        self
    }

    fn disks(&mut self, n: usize) -> &mut Self {
        self.n = n;
        self
    }

    fn fsize(&mut self, fsize: u64) -> &mut Self {
        self.fsize = fsize;
        self
    }

    fn mirror_size(&mut self, m: usize) -> &mut Self {
        self.m = m;
        self
    }

    fn name(&mut self, name: &'static str) -> &mut Self {
        self.name = name;
        self
    }

    fn nclusters(&mut self, nclusters: usize) -> &mut Self {
        self.nclusters = nclusters;
        self
    }

    fn new() -> Self {
        Self {
            n: 1,
            m: 1,
            k: 1,
            f: 0,
            nclusters: 1,
            fsize: 1 << 30,  // 1GB
            cs: None,
            name: "functional_test_pool",
            zone_size: None
        }
    }

    fn redundancy_level(&mut self, f: i16) -> &mut Self {
        self.f = f;
        self
    }

    fn stripe_size(&mut self, k: i16) -> &mut Self {
        self.k = k;
        self
    }

    fn zone_size(&mut self, z: u64) -> &mut Self {
        self.zone_size = NonZeroU64::new(z);
        self
    }
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
mod vdev_block;
mod vdev_file;
