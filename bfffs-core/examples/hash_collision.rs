// vim: tw=80
//! Generates extended attribute names that result in hash collisions in the
//! B-Tree

use bfffs_core::fs_tree::*;
use dashmap::DashMap;
use clap::Parser;
use rand_xorshift::XorShiftRng;
use rand::{
    Rng,
    SeedableRng,
    distributions::Alphanumeric,
    seq::SliceRandom
};
use std::{
    ffi::{OsStr, OsString},
    os::unix::ffi::OsStrExt,
    sync::{LazyLock, Mutex},
    thread,
    time
};

const NAMESPACES: [ExtAttrNamespace; 2] =
    [ExtAttrNamespace::User, ExtAttrNamespace::System];

#[derive(Debug, Default)]
struct Stats {
    tries: u64,
    collisions: u64
}

impl Stats {
    const fn new() -> Self {
        Stats {tries: 0, collisions: 0}
    }
}

// Don't store the actual namespace and name, because that takes too much RAM.
// Instead, store a seed that can be used to recreate the name and namespace.
// It cuts the throughput, but also cuts the RAM usage.
static HM: LazyLock<DashMap<u64, [u8; 16]>> = LazyLock::new(|| {
    DashMap::with_capacity(4_000_000)
});

static STATS: Mutex<Stats> = Mutex::new(Stats::new());

trait Collidable {
    fn dump(&self) -> String;
    fn new(seed: &[u8; 16]) -> Self;
    fn objkey(&self) -> ObjKey;
}

struct CDirent {
    name: OsString
}

impl Collidable for CDirent {
    fn dump(&self) -> String {
        format!("{:?}", &self.name)
    }

    fn new(seed: &[u8; 16]) -> Self {
        let this_rng = XorShiftRng::from_seed(*seed);
        let v: Vec<u8> = this_rng.sample_iter(&Alphanumeric)
            .take(10)
            .collect();
        let name = OsStr::from_bytes(&v[..]);
        CDirent{name: name.to_owned()}
    }

    fn objkey(&self) -> ObjKey {
        ObjKey::dir_entry(&self.name)
    }
}

struct CExtattr {
    namespace: ExtAttrNamespace,
    name: OsString
}

impl Collidable for CExtattr {
    fn dump(&self) -> String {
        format!("({:?}, {:?})", self.namespace, &self.name)
    }

    fn new(seed: &[u8; 16]) -> Self {
        let mut this_rng = XorShiftRng::from_seed(*seed);
        let v: Vec<u8> = (&mut this_rng).sample_iter(&Alphanumeric)
            .take(10)
            .collect();
        let name = OsStr::from_bytes(&v[..]);
        let ns = NAMESPACES.choose(&mut this_rng).unwrap();
        CExtattr{namespace: *ns, name: name.to_owned()}
    }

    fn objkey(&self) -> ObjKey {
        ObjKey::extattr(self.namespace, &self.name)
    }
}

fn report(collisions: u64, tries: u64) {
    println!("Found {collisions} collisions among {tries} filenames");
}

struct Worker {
    hm: &'static DashMap<u64, [u8; 16]>,
    rng: XorShiftRng,
}

impl Worker {
    fn new(hm: &'static DashMap<u64, [u8; 16]>) -> Self
    {
        let rng = XorShiftRng::from_entropy();
        Worker{hm, rng}
    }

    /// Run forever
    fn run<T: Collidable>(&mut self) {
        loop {
            self.run_once::<T>();
        }
    }

    fn run_once<T: Collidable>(&mut self) {
        let mut tries = 0u64;
        let mut collisions = 0u64;
        for _ in 0..16_384 {
            let seed: [u8; 16] = self.rng.gen();
            let collidable = T::new(&seed);
            let objkey = collidable.objkey();
            tries += 1;
            let offset = objkey.offset();
            // For testing hash_collision.rs itself, shorten the offset
            //let offset = offset & ((1<<52) - 1);
            let v = seed;
            if let Some(old_seed) = self.hm.insert(offset, v) {
                let old_collidable = T::new(&old_seed);
                println!("Hash collision: {} and {} have offset {:?}",
                         collidable.dump(), old_collidable.dump(), offset);
                collisions += 1;
                continue;
            }
        }
        let mut stats = STATS.lock().unwrap();
        stats.tries += tries;
        stats.collisions += collisions;
    }
}

#[derive(Parser, Clone, Copy, Debug)]
/// Generate hash collisions for dirent and extattr storage in BFFFS
struct Cli {
    /// Generate extended attributes instead of directory entries
    #[clap(short = 'x')]
    extattr: bool,
    /// Memory limit in GB
    mem: i64
}

fn main() {
    let cli = Cli::parse();
    unsafe {
        // Limit RAM usage
        let rlimit = libc::rlimit{
            rlim_cur: cli.mem*(1<<30),
            rlim_max: cli.mem*(1<<30)
        };
        libc::setrlimit(libc::RLIMIT_AS, &rlimit);
    }

    let ncpu = num_cpus::get();
    println!("Using {ncpu} threads");
    for _ in 0..ncpu {
        let mut worker = Worker::new(&HM);
        thread::spawn(move || {
            if cli.extattr {
                worker.run::<CExtattr>();
            } else {
                worker.run::<CDirent>();
            }
        });
    }
    let mut last = 0;
    loop {
        thread::sleep(time::Duration::new(1, 0));
        let stats = STATS.lock().unwrap();
        if stats.tries > last {
            report(stats.collisions, stats.tries);
        }
        last = stats.tries;
    }
}
