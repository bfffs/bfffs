#![feature(test)]

extern crate bfffs;
extern crate test;

use bfffs::common::{declust::*, prime_s::*};
use test::Bencher;

/// Benchmark the speed of `PrimeS::id2loc` using a largeish layout
#[bench]
fn bench_primes_id2loc(bench: &mut Bencher) {
    let n = 23;
    let k = 19;
    let f = 3;

    let locator = PrimeS::new(n, k, f);

    bench.iter(move || {
        locator.id2loc(ChunkId::Parity(1_234_567, 1))
    });
}

/// Benchmark the speed of `PrimeS::PrimeSIter::next` using a largeish layout
#[bench]
fn bench_primesiter_next(bench: &mut Bencher) {
    let n = 23;
    let k = 19;
    let f = 3;

    let locator = PrimeS::new(n, k, f);
    let mut iter = locator.iter(ChunkId::Data(0),
                                ChunkId::Data(u64::max_value()));

    bench.iter(|| {
        iter.next()
    });
}
