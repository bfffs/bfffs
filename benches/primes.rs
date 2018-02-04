#![feature(test)]

extern crate arkfs;
extern crate test;

use arkfs::common::declust::*;
use arkfs::common::prime_s::*;
use test::Bencher;

/// Benchmark the speed of `PrimeS::id2loc` using a largeish layout
#[bench]
fn bench_id2loc(bench: &mut Bencher) {
    let n = 23;
    let k = 19;
    let f = 3;

    let locator = PrimeS::new(n, k, f);

    bench.iter(move || {
        locator.id2loc(ChunkId::Parity(1_234_567, 1))
    });
}
