// vim: tw=80
//! Compares different compression algorithms on BFFFS metadata
//!
//! This program compares different BLOSC algorithms and settings on binary
//! metadata nodes as produced by `examples/fanout --save`

use histogram::Histogram;
use std::collections::HashMap;
use std::io::Read;

const COMPRESSORS: [blosc::Compressor; 6] = [
    blosc::Compressor::BloscLZ,
    blosc::Compressor::LZ4,
    blosc::Compressor::LZ4HC,
    blosc::Compressor::Snappy,
    blosc::Compressor::Zlib,
    blosc::Compressor::Zstd
];

const DATASETS: [&'static str; 3] = [
    &"alloct",
    &"ridt",
    &"fs"
];

fn main() {
    println!("                   min   mean    max stddev");
    for ds in DATASETS.iter() {
        let mut histos = HashMap::new();
        for z in COMPRESSORS.iter() {
            histos.insert(z, Histogram::new());
        }
        let pat = format!("/tmp/fanout/{}.*.bin", ds);
        for path in glob::glob(&pat).unwrap() {
            let mut f = std::fs::File::open(path.unwrap()).unwrap();
            let mut buf = Vec::new();
            let lsize = f.read_to_end(&mut buf).unwrap();
            for z in COMPRESSORS.iter() {
                let zbuf = blosc::Context::new()
                    .compressor(*z)
                    .unwrap()
                    .compress(&buf[0..lsize]);
                let csize = zbuf.size();
                let ratio = csize as f64 / lsize as f64;
                // The histogram crate only works with u64 data
                histos.get_mut(&z).unwrap()
                    .increment((1000.0 * ratio) as u64)
                    .unwrap();
            }
        }
        for z in COMPRESSORS.iter() {
            let hs = &histos[z];
            let zname = format!("{:?}", z);
            let min = hs.minimum().unwrap() as f64 / 10.0;
            let mean = hs.mean().unwrap() as f64 / 10.0;
            let max = hs.maximum().unwrap() as f64 / 10.0;
            let stddev = hs.stddev().unwrap() as f64 / 10.0;
            println!("{:8}{:8}{:5.1}% {:5.1}% {:5.1}% {:5.1}%", ds, zname,
                     min, mean, max, stddev);
        }
    }
}
