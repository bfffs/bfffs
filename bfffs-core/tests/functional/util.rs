use std::{
    ffi::OsStr,
    io,
    num::NonZeroU64,
    os::unix::ffi::OsStrExt,
    path::{PathBuf, Path},
    process::Command
};

use itertools::Itertools;
use tempfile::{Builder, TempDir};

use bfffs_core::{
    cluster::Cluster,
    mirror::Mirror,
    pool::Pool,
};

#[macro_export]
macro_rules! require_root {
    () => {
        if ! ::nix::unistd::Uid::current().is_root() {
            use ::std::io::Write;

            let stderr = ::std::io::stderr();
            let mut handle = stderr.lock();
            writeln!(handle, "{} requires root privileges.  Skipping test.",
                concat!(::std::module_path!(), "::", function_name!()))
                .unwrap();
            return;
        }
    }
}

/// An md(4) device.
pub struct Md(pub PathBuf);
impl Md {
    pub fn new() -> io::Result<Self> {
        let output = Command::new("mdconfig")
            .args(["-a", "-t",  "swap", "-s", "64m"])
            .output()?;
        // Strip the trailing "\n"
        let l = output.stdout.len() - 1;
        let mddev = OsStr::from_bytes(&output.stdout[0..l]);
        let pb = Path::new("/dev").join(mddev);
        Ok(Self(pb))
    }

    pub fn as_path(&self) -> &Path {
        self.0.as_path()
    }
}
impl Drop for Md {
    fn drop(&mut self) {
        Command::new("mdconfig")
            .args(["-d", "-u"])
            .arg(&self.0)
            .output()
            .expect("failed to deallocate md(4) device");
    }
}

/// Helper to create a fresh pool
#[derive(Debug)]
pub struct PoolBuilder {
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
    pub fn build(&self) -> (TempDir, Vec<PathBuf>, Pool) {
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

    pub fn chunksize(&mut self, cs: u64) -> &mut Self {
        self.cs = NonZeroU64::new(cs);
        self
    }

    pub fn disks(&mut self, n: usize) -> &mut Self {
        self.n = n;
        self
    }

    pub fn fsize(&mut self, fsize: u64) -> &mut Self {
        self.fsize = fsize;
        self
    }

    pub fn mirror_size(&mut self, m: usize) -> &mut Self {
        self.m = m;
        self
    }

    pub fn name(&mut self, name: &'static str) -> &mut Self {
        self.name = name;
        self
    }

    pub fn nclusters(&mut self, nclusters: usize) -> &mut Self {
        self.nclusters = nclusters;
        self
    }

    pub fn new() -> Self {
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

    pub fn redundancy_level(&mut self, f: i16) -> &mut Self {
        self.f = f;
        self
    }

    pub fn stripe_size(&mut self, k: i16) -> &mut Self {
        self.k = k;
        self
    }

    pub fn zone_size(&mut self, z: u64) -> &mut Self {
        self.zone_size = NonZeroU64::new(z);
        self
    }
}

impl Default for PoolBuilder {
    fn default() -> Self {
        Self::new()
    }
}
