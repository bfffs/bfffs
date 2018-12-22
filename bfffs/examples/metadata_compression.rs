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

const DATASETS: [(&str, usize); 3] = [
    (&"alloct", 18),
    (&"ridt", 46),
    (&"fs", 32)
];

fn main() {
    println!("                shuffle    min   mean    max stddev");
    for (ds, typesize) in DATASETS.iter() {
        let mut histos = Vec::new();
        for _shuf in &[true, false] {
            histos.push(HashMap::new());
        }
        for z in COMPRESSORS.iter() {
            for shuf in &[true, false] {
                histos[*shuf as usize].insert(z, Histogram::new());
            }
        }
        let pat = format!("/tmp/fanout/{}.*.bin", ds);
        for path in glob::glob(&pat).unwrap() {
            let mut f = std::fs::File::open(path.unwrap()).unwrap();
            let mut buf = Vec::new();
            let lsize = f.read_to_end(&mut buf).unwrap();
            for z in COMPRESSORS.iter() {
                for shuf in &[true, false] {
                    let shufmode = if *shuf {
                        blosc::ShuffleMode::Byte
                    } else {
                        blosc::ShuffleMode::None
                    };
                    let zbuf = blosc::Context::new()
                        .compressor(*z)
                        .unwrap()
                        .shuffle(shufmode)
                        .typesize(Some(*typesize))
                        .compress(&buf[0..lsize]);
                    let csize = zbuf.size();
                    let ratio = csize as f64 / lsize as f64;
                    // The histogram crate only works with u64 data
                    histos[*shuf as usize].get_mut(&z).unwrap()
                        .increment((1000.0 * ratio) as u64)
                        .unwrap();
                }
            }
        }
        for z in COMPRESSORS.iter() {
            for shuf in &[true, false] {
                let hs = &histos[*shuf as usize][z];
                let zname = format!("{:?}", z);
                let min = hs.minimum().unwrap() as f64 / 10.0;
                let mean = hs.mean().unwrap() as f64 / 10.0;
                let max = hs.maximum().unwrap() as f64 / 10.0;
                let stddev = hs.stddev().unwrap() as f64 / 10.0;
                println!("{:8}{:8}{:8}{:5.1}% {:5.1}% {:5.1}% {:5.1}%", ds,
                         zname, shuf, min, mean, max, stddev);
            }
        }
    }
}
