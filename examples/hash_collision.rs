// vim: tw=80
//! Generates extended attribute names that result in hash collisions in the
//! B-Tree

extern crate bfffs;
extern crate chashmap;
extern crate clap;
extern crate lazy_static;
extern crate libc;
extern crate num_cpus;
extern crate rand;
extern crate rand_xorshift;

use bfffs::common::fs_tree::*;
use chashmap::CHashMap;
use lazy_static::lazy_static;
use rand_xorshift::XorShiftRng;
use rand::{
    FromEntropy,
    Rng,
    SeedableRng,
    distributions::Alphanumeric
};
use std::{
    ffi::{OsStr, OsString},
    os::unix::ffi::OsStrExt,
    sync::mpsc,
    thread,
    time
};

const NAMESPACES: [ExtAttrNamespace; 2] =
    [ExtAttrNamespace::User, ExtAttrNamespace::System];

lazy_static! {
    // CHashMap resizes more slowly than the standard hashmap.  So give it a
    // large size to start.  64M entries takes about 10GB.
    //
    // Don't store the actual namespace and name, because that takes too much
    // RAM.  Instead, store a seed that can be used to recreate the name and
    // namespace.  It cuts the throughput, but also cuts the RAM usage.
    static ref HM: CHashMap<u64, [u8; 16]> =
        CHashMap::with_capacity(4_000_000);
}

fn mkname(seed: &[u8; 16]) -> (ExtAttrNamespace, OsString) {
    let mut this_rng = XorShiftRng::from_seed(*seed);
    let v: Vec<u8> = this_rng.sample_iter(&Alphanumeric)
        .map(|c| c as u8)
        .take(10)
        .collect();
    let name = OsStr::from_bytes(&v[..]);
    let ns = this_rng.choose(&NAMESPACES).unwrap();
    (*ns, name.to_owned())
}

fn report(collisions: u64, tries: u64) {
    println!("Found {} collisions among {} filenames", collisions, tries);
}

struct Worker {
    hm: &'static CHashMap<u64, [u8; 16]>,
    rng: XorShiftRng,
    tx: mpsc::Sender<(u64, u64)>
}

impl Worker {
    fn new(hm: &'static CHashMap<u64, [u8; 16]>,
           tx: mpsc::Sender<(u64, u64)>) -> Self
    {
        let rng = XorShiftRng::from_entropy();
        Worker{hm, rng, tx}
    }

    /// Run forever
    fn run(&mut self) {
        loop {
            let result = self.run_once();
            self.tx.send(result).unwrap();
        }
    }

    fn run_once(&mut self) -> (u64, u64) {
        let mut tries = 0u64;
        let mut collisions = 0u64;
        for _ in 0..10_000 {
            let seed: [u8; 16] = self.rng.gen();
            let (ns, name) = mkname(&seed);
            let objkey = ObjKey::extattr(ns, &name);
            tries += 1;
            if let ObjKey::ExtAttr(offset) = objkey {
                // For testing hash_collision.rs itself, shorten the offset
                //let offset = offset & ((1<<52) - 1);
                let v = seed;
                if let Some(old_seed) = self.hm.insert(offset, v) {
                    let (old_ns, old_name) = mkname(&old_seed);
                    println!("Hash collision: {:?} and {:?} have offset {:?}",
                             (ns, name), (old_ns, old_name), offset);
                    collisions += 1;
                    continue;
                }
            } else {
                unreachable!();
            }
        }
        (tries, collisions)
    }
}

fn main() {
    let app = clap::App::new("hash_collision")
    .about("Generate hash collisions for dirent and extattr storage in BFFFS")
    .arg(clap::Arg::with_name("mem")
         .help("Memory limit in GB")
         .required(true)
    );
    let matches = app.get_matches();
    let limit: i64 = matches.value_of("mem").unwrap().parse().unwrap();
    unsafe {
        // Limit RAM usage
        let rlimit = libc::rlimit{
            rlim_cur: limit*(1<<30),
            rlim_max: limit*(1<<30)
        };
        libc::setrlimit(libc::RLIMIT_AS, &rlimit);
    }

    let mut now = time::Instant::now();
    let mut tries = 0u64;
    let mut collisions = 0u64;
    let ncpu = num_cpus::get();
    println!("Using {} threads", ncpu);
    let (tx, rx) = mpsc::channel();
    for _ in 0..ncpu {
        let mut worker = Worker::new(&HM, tx.clone());
        thread::spawn(move || {
            worker.run();
        });
    }
    for (t, c) in rx {
        collisions += c;
        tries += t;
        if now.elapsed() > time::Duration::new(5, 0) {
            report(collisions, tries);
            now = time::Instant::now();
        }
    }
    report(collisions, tries);
}
