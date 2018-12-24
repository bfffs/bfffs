// vim: tw=80
//! Compares different compression algorithms on BFFFS metadata
//!
//! This program compares different BLOSC algorithms and settings on binary
//! metadata nodes as produced by `examples/fanout --save`

use histogram::Histogram;
use std::{
    collections::HashMap,
    io::Read,
    time
};

const COMPRESSORS: [blosc::Compressor; 6] = [
    blosc::Compressor::BloscLZ,
    blosc::Compressor::LZ4,
    blosc::Compressor::LZ4HC,
    blosc::Compressor::Snappy,
    blosc::Compressor::Zlib,
    blosc::Compressor::Zstd
];

// For each pair of trees, the first entry is for leaf nodes and the second for
// interior nodes.  The fanouts are generally different.
const DATASETS: [(&str, usize); 6] = [
    (&"alloct.18", 18),
    (&"alloct.49", 49),
    (&"ridt.43", 43),
    (&"ridt.47", 47),
    (&"fs.32", 32),
    (&"fs.36", 32),
];

const SHUFFLES: [(&str, blosc::ShuffleMode); 3] = [
    (&"none", blosc::ShuffleMode::None),
    (&"byte", blosc::ShuffleMode::Byte),
    (&"bit", blosc::ShuffleMode::Bit),
];

fn main() {
    println!("tree      algo    shuffle |      compression ratio      | compression times");
    println!("                          |    min   mean    max stddev |     mean   stddev");
    println!("--------------------------+-----------------------------+------------------");
    for (ds, typesize) in DATASETS.iter() {
        // Compression ratios in parts per thousand
        let mut ratios = Vec::new();
        // Compression times in nanoseconds
        let mut times = Vec::new();
        for _ in SHUFFLES.iter().enumerate() {
            ratios.push(HashMap::new());
            times.push(HashMap::new());
        }
        for z in COMPRESSORS.iter() {
            for (i, _) in SHUFFLES.iter().enumerate() {
                ratios[i].insert(z, Histogram::new());
                times[i].insert(z, Histogram::new());
            }
        }
        let pat = format!("/tmp/fanout/{}.*.bin", ds);
        for path in glob::glob(&pat).unwrap() {
            let pb = path.unwrap();
            let mut f = std::fs::File::open(pb).unwrap();
            let mut buf = Vec::new();
            let lsize = f.read_to_end(&mut buf).unwrap();
            for z in COMPRESSORS.iter() {
                for (i, (_, shufmode)) in SHUFFLES.iter().enumerate() {
                    let start = time::Instant::now();
                    let zbuf = blosc::Context::new()
                        .compressor(*z)
                        .unwrap()
                        .shuffle(*shufmode)
                        .typesize(Some(*typesize))
                        .compress(&buf[0..lsize]);

                    let time = start.elapsed();
                    debug_assert_eq!(time.as_secs(), 0);
                    times[i].get_mut(&z).unwrap()
                        .increment(time.subsec_nanos().into())
                        .unwrap();

                    let csize = zbuf.size();
                    let ratio = csize as f64 / lsize as f64;
                    ratios[i].get_mut(&z).unwrap()
                        .increment((1000.0 * ratio) as u64)
                        .unwrap();
                }
            }
        }
        for z in COMPRESSORS.iter() {
            for (i, (shufname, _)) in SHUFFLES.iter().enumerate() {
                let zname = format!("{:?}", z);

                let ratio = &ratios[i][z];
                let zmin = ratio.minimum().unwrap() as f64 / 10.0;
                let zmean = ratio.mean().unwrap() as f64 / 10.0;
                let zmax = ratio.maximum().unwrap() as f64 / 10.0;
                let zstddev = ratio.stddev().unwrap() as f64 / 10.0;

                let time = &times[i][z];
                let tmean = time.mean().unwrap();
                let tstddev = time.stddev().unwrap();

                print!("{:10}{:8}{:8}| {:5.1}% {:5.1}% {:5.1}% {:5.1}%", ds,
                         zname, shufname, zmin, zmean, zmax, zstddev);
                println!(" | {:6.1}ns {:6.1}ns",
                         tmean, tstddev);
            }
        }
    }
}
