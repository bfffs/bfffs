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

//use bfffs_core::{
    //cluster::Cluster,
    //mirror::Mirror,
    //pool::Pool,
//};

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

#[macro_export]
macro_rules! assert_bufeq {
    ($left:expr, $right:expr) => {
        if $left != $right {
            let lhex = ::hexdump::hexdump_iter($left)
                .fold(String::new(), |mut acc, l| {
                    acc.push_str(&*l);
                    acc.push('\n');
                    acc
                });
            let rhex = ::hexdump::hexdump_iter($right)
                .fold(String::new(), |mut acc, l| {
                    acc.push_str(&*l);
                    acc.push('\n');
                    acc
                });
            let lines = prettydiff::diff_lines(lhex.as_str(), rhex.as_str())
                .set_diff_only(true)
                .set_show_lines(false)
                .names(stringify!($left), stringify!($right))
                ;
            lines.prettytable();
            panic!("Miscompare!");
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

/// A gnop(4) device with controllable error probability
pub struct Gnop {
    _md: Md,
    path: PathBuf
}
impl Gnop {
    pub fn new() -> io::Result<Self> {
        let md = Md::new()?;
        let r = Command::new("gnop")
            .arg("create")
            .arg(md.as_path())
            .status()
            .expect("Failed to execute command")
            .success();
        if !r {
            panic!("Failed to create gnop device");
        }
        let mut path = PathBuf::from(md.as_path());
        path.set_extension("nop");
        Ok(Self{_md: md, path})
    }

    pub fn as_path(&self) -> &Path {
        &self.path
    }

    /// Set the probability of failure on read, from 0 to 100 percent.
    pub fn error_prob(&self, prob: i32) {
        let r = Command::new("gnop")
            .args(["configure", "-r"])
            .arg(format!("{}", prob))
            .arg(self.as_path())
            .status()
            .expect("Failed to execute command")
            .success();
        assert!(r, "Failed to configure gnop");
    }
}
impl Drop for Gnop {
    fn drop(&mut self) {
        Command::new("gnop")
            .args(["destroy", "-f"])
            .arg(self.as_path())
            .output()
            .expect("failed to deallocate gnop(4) device");
    }
}

///// Helper to create a fresh pool
//#[derive(Debug)]
//pub struct PoolBuilder {
    ///// Number of disks
    //n: usize,
    ///// Disks per mirror
    //m: usize,
    ///// Mirrors per RAID stripe
    //k: i16,
    ///// RAID redundancy level
    //f: i16,
    ///// Number of clusters in the pool
    //nclusters: usize,
    ///// Size of each file in bytes
    //fsize: u64,
    ///// RAID chunk size
    //cs: Option<NonZeroU64>,
    ///// Pool name
    //name: &'static str,
    ///// LBAs per zone
    //zone_size: Option<NonZeroU64>,
    ///// Use gnop devices instead of files
    //gnop: bool
//}

//pub struct PoolHarness {
    //pub tempdir: TempDir,
    //pub disks_per_mirror: usize,
    //pub paths: Vec<PathBuf>,
    //pub pool: Pool,
    //pub gnops: Vec<Gnop>
//}

//impl PoolBuilder {
    //pub fn build(&self) -> PoolHarness {
        //let mirrors_per_cluster = (self.n / self.m) / self.nclusters;
        //assert_eq!(self.n % self.m, 0);
        //assert!(mirrors_per_cluster >= self.k as usize);
        //assert!(self.k > self.f);
        //assert_eq!((self.n / self.m) % self.nclusters, 0);

        //let tempdir = Builder::new()
            //.prefix("bfffs_functional_test")
            //.tempdir()
            //.unwrap();
        //let mut gnops = Vec::new();
        //let mut paths = Vec::new();
        //if self.gnop {
            //for _ in 0..self.n {
                //let gnop = Gnop::new().unwrap();
                //paths.push(gnop.as_path().to_owned());
                //gnops.push(gnop);
            //}
        //} else {
            //for i in 0..self.n {
                //let fname = format!("{}/vdev.{}", tempdir.path().display(), i);
                //let file = std::fs::File::create(&fname).unwrap();
                //file.set_len(self.fsize).unwrap();
                //paths.push(PathBuf::from(fname));
            //}
        //}
        //let clusters = paths.chunks(self.m)
            //.map(|mpaths| {
                //Mirror::create(mpaths, self.zone_size).unwrap()
            //})
            //.chunks(mirrors_per_cluster)
            //.into_iter()
            //.map(|miter| {
                //let mirrors = miter.collect::<Vec<_>>();
                //assert_eq!(mirrors.len(), mirrors_per_cluster);
                //let raid = bfffs_core::raid::create(self.cs, self.k, self.f,
                                                    //mirrors);
                //Cluster::create(raid)
            //}).collect::<Vec<_>>();
        //let pool = Pool::create(String::from(self.name), clusters);
        //PoolHarness{tempdir, disks_per_mirror: self.m, paths, pool, gnops}
    //}

    //pub fn chunksize(&mut self, cs: u64) -> &mut Self {
        //self.cs = NonZeroU64::new(cs);
        //self
    //}

    //pub fn disks(&mut self, n: usize) -> &mut Self {
        //self.n = n;
        //self
    //}

    //pub fn fsize(&mut self, fsize: u64) -> &mut Self {
        //self.fsize = fsize;
        //self
    //}

    //pub fn mirror_size(&mut self, m: usize) -> &mut Self {
        //self.m = m;
        //self
    //}

    //pub fn name(&mut self, name: &'static str) -> &mut Self {
        //self.name = name;
        //self
    //}

    //pub fn nclusters(&mut self, nclusters: usize) -> &mut Self {
        //self.nclusters = nclusters;
        //self
    //}

    //pub fn gnop(&mut self, gnop: bool) -> &mut Self {
        //self.gnop = gnop;
        //self
    //}

    //pub fn new() -> Self {
        //Self {
            //n: 1,
            //m: 1,
            //k: 1,
            //f: 0,
            //nclusters: 1,
            //fsize: 1 << 30,  // 1GB
            //cs: None,
            //name: "functional_test_pool",
            //zone_size: None,
            //gnop: false
        //}
    //}

    //pub fn redundancy_level(&mut self, f: i16) -> &mut Self {
        //self.f = f;
        //self
    //}

    //pub fn stripe_size(&mut self, k: i16) -> &mut Self {
        //self.k = k;
        //self
    //}

    //pub fn zone_size(&mut self, z: u64) -> &mut Self {
        //self.zone_size = NonZeroU64::new(z);
        //self
    //}
//}

//impl Default for PoolBuilder {
    //fn default() -> Self {
        //Self::new()
    //}
//}
