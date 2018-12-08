#![feature(test)]

use bfffs::common::{declust::*, prime_s::{self, *}};
use test::Bencher;

/// Benchmark the speed of `PrimeS::id2loc` using a largeish layout
#[bench]
fn id2loc(bench: &mut Bencher) {
    let n = 23;
    let k = 19;
    let f = 3;

    let locator = PrimeS::new(n, k, f);

    bench.iter(move || {
        locator.id2loc(ChunkId::Parity(1_234_567, 1))
    });
}

/// Benchmark prime_s::invmod<i16>
#[bench]
fn invmod(bench: &mut Bencher) {
    bench.iter(move || {
        prime_s::invmod(500i16, 523i16)
    });
}

/// Benchmark the speed of `PrimeS::PrimeSIter::next` using a largeish layout
#[bench]
fn iter_next(bench: &mut Bencher) {
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

#[bench]
fn loc2id(bench: &mut Bencher) {
    let n = 23;
    let k = 19;
    let f = 3;

    let locator = PrimeS::new(n, k, f);
    let chunkloc = locator.id2loc(ChunkId::Parity(1_234_567, 1));

    bench.iter(move || {
        locator.loc2id(chunkloc.clone())
    });
}
