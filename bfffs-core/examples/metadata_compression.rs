// vim: tw=80
//! Compares different compression algorithms on BFFFS metadata
//!
//! This program compares different Compression algorithms and settings on
//! binary metadata nodes as produced by `examples/fanout --save`

use std::{
    collections::HashMap,
    io::Read,
    process,
    time
};

use byteshuffle::shuffle;
use histogram::Histogram;
use lz4_flex::block as lz4;
use zstd::bulk as zstd;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Compressor {
    Lz4Flex,
    Zstd
}

#[derive(Clone, Debug)]
struct Score {
    compressor: Compressor,
    shuffle: &'static str,
    /// Compression ratio in parts per thousand
    ratio: Histogram,
    /// Compression speed in MiBps
    speed: Histogram
}

impl Score {
    fn new(compressor: Compressor, shuffle: &'static str) -> Self {
        let ratio = Histogram::new();
        let speed = Histogram::new();
        Self { compressor, shuffle, ratio, speed}
    }
}

const COMPRESSORS: &[Compressor] = &[
    Compressor::Lz4Flex,
    Compressor::Zstd,
];

// For each pair of trees, the first entry is for leaf nodes and the second for
// interior nodes.  The fanouts are generally different.
const DATASETS: [(&str, usize); 6] = [
    ("alloct.18", 18),
    ("alloct.49", 49),
    ("ridt.43", 43),
    ("ridt.47", 47),
    ("fs.32", 32),
    ("fs.36", 36),
];

enum ShuffleMode {
    None,
    Byte
}

const SHUFFLES: [(&str, ShuffleMode); 2] = [
    ("none", ShuffleMode::None),
    ("byte", ShuffleMode::Byte),
];

fn main() {
    println!("tree      algo           shuffle |      compression ratio      | compression speeds");
    println!("                                 |    min   mean    max stddev |       mean     stddev");
    println!("---------------------------------+-----------------------------+---------------------");
    for (ds, typesize) in DATASETS.iter() {
        let mut found = false;
        let mut scores = HashMap::<(Compressor, &'static str), Score>::new();
        let pat = format!("/tmp/fanout/{ds}.*.bin");
        for path in glob::glob(&pat).unwrap() {
            found = true;
            let pb = path.unwrap();
            let mut f = std::fs::File::open(pb).unwrap();
            let mut buf = Vec::new();
            let lsize = f.read_to_end(&mut buf).unwrap();
            for z in COMPRESSORS.iter() {
                for (shufname, shufmode) in SHUFFLES.iter() {
                    let start = time::Instant::now();
                    let (zbuf, csize_adjust) = match z {
                        Compressor::Lz4Flex => {
                            let zbuf = match shufmode {
                                ShuffleMode::Byte => {
                                    let shuffled = shuffle(*typesize,
                                        &buf[0..lsize]);
                                    lz4::compress(&shuffled[..])
                                },
                                ShuffleMode::None => 
                                    lz4::compress(&buf[0..lsize]),
                            };
                            // Add an extra 5 bytes due to the need for us to
                            // manually tag the compressed buffer.
                            (zbuf, 5)
                        }
                        Compressor::Zstd => {
                            let zbuf = match shufmode {
                                ShuffleMode::Byte => {
                                    let shuffled = shuffle(*typesize,
                                        &buf[0..lsize]);
                                    zstd::compress(&shuffled[..], 0).unwrap()
                                },
                                ShuffleMode::None => 
                                    zstd::compress(&buf[0..lsize], 0).unwrap(),
                            };
                            // Add an extra 5 bytes due to the need for us to
                            // manually tag the compressed buffer.
                            (zbuf, 5)
                        }
                    };
                    let score: &mut Score = scores.entry((*z, shufname))
                        .or_insert_with(|| Score::new(*z, shufname));

                    let time = start.elapsed();
                    debug_assert_eq!(time.as_secs(), 0);
                    let speed = lsize as u64
                        * 1_000_000_000
                        / u64::from(time.subsec_nanos())
                        / 1024 / 1024;
                    score.speed
                        .increment(speed)
                        .unwrap();

                    let csize = zbuf.len() + csize_adjust;
                    let ratio = csize as f64 / lsize as f64;
                    score.ratio
                        .increment((1000.0 * ratio) as u64)
                        .unwrap();
                }
            }
        }
        if !found {
            eprintln!("No data!  Run examples/fanout first");
            process::exit(1);
        }
        let mut scores: Vec<_> = scores.into_iter().collect();
        scores.sort_by(|x, y| {
            (x.1.ratio.mean(), x.1.speed.mean())
                .cmp(&(y.1.ratio.mean(), y.1.speed.mean()))
        });
        for (_, score) in scores.iter() {
            let zname = format!("{:?}", score.compressor);
            let shufname = score.shuffle;
            let zmin = score.ratio.minimum().unwrap() as f64 / 10.0;
            let zmean = score.ratio.mean().unwrap() as f64 / 10.0;
            let zmax = score.ratio.maximum().unwrap() as f64 / 10.0;
            let zstddev = score.ratio.stddev().unwrap() as f64 / 10.0;
            let tmean = score.speed.mean().unwrap();
            let tstddev = score.speed.stddev().unwrap();
            print!("{ds:10}{zname:15}{shufname:8}| {zmin:5.1}% {zmean:5.1}% {zmax:5.1}% {zstddev:5.1}%");
            println!(" | {tmean:5.1} MiBps {tstddev:4.1} MiBps");
        }
    }
}
