// vim: tw=80
//! Generates extended attribute names that result in hash collisions in the
//! B-Tree

extern crate bfffs;
extern crate chashmap;
extern crate lazy_static;
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
    // large size to start.
    static ref HM: CHashMap<u64, (ExtAttrNamespace, OsString)> = 
        CHashMap::with_capacity(64_000_000);
}

fn report(collisions: u64, tries: u64) {
    println!("Found {} collisions among {} filenames", collisions, tries);
}

struct Worker {
    hm: &'static CHashMap<u64, (ExtAttrNamespace, OsString)>,
    rng: XorShiftRng,
    tx: mpsc::Sender<(u64, u64)>
}

impl Worker {
    fn new(hm: &'static CHashMap<u64, (ExtAttrNamespace, OsString)>,
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
            let v: Vec<u8> = self.rng.sample_iter(&Alphanumeric)
                .map(|c| c as u8)
                .take(10)
                .collect();
            let name = OsStr::from_bytes(&v[..]);
            let ns = self.rng.choose(&NAMESPACES).unwrap();
            let objkey = ObjKey::extattr(*ns, &name);
            tries += 1;
            if let ObjKey::ExtAttr(offset) = objkey {
                let shortoffset = offset;// & ((1<<36) - 1);
                let v = (*ns, name.to_owned());
                if let Some((oldns, oldname)) = self.hm.insert(shortoffset, v) {
                    println!("Hash collision: {:?} and {:?} have offset {:?}",
                             (ns, name), (oldns, oldname), shortoffset);
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
