// vim: tw=80

use bfffs::common::declust::*;
use bfffs::common::prime_s::*;

// PrimeS should panic if you try to use a composite number of disks
#[test]
#[should_panic]
pub fn composite_disks_panic() {
    PrimeS::new(9, 5, 1);
}

// Exhaustive placement test for a small array
// declust.rb's output:
// D0.0   D0.1   D0.2   C0.0   C0.1   D1.2   C1.0
// C1.1   D2.2   C2.0   D1.0   D1.1   C3.0   D2.0
// D2.1   C4.0   D3.0   C2.1   D3.2   D4.0   C3.1
// D4.2   D5.0   C4.1   D3.1   C5.0   C5.1   D4.1
// C6.0   C6.1   D5.1   D5.2   D6.0   D6.1   D6.2
// D7.0   C7.1   D7.1   D8.2   D7.2   C8.0   C7.0
// C8.1   D8.1   D9.2   C10.0  C9.0   D9.0   D8.0
// D9.1   D10.2  C11.0  D11.0  D10.0  C10.1  C9.1
// D11.2  C12.0  D12.0  C12.1  C11.1  D11.1  D10.1
// C13.0  D13.0  C13.1  D13.1  D12.1  D13.2  D12.2
// D14.0  D15.2  C14.0  D14.1  C15.0  C14.1  D14.2
// C15.1  C17.0  D15.0  D16.2  D16.0  D15.1  C16.0
// D16.1  D18.0  C16.1  C18.0  C17.1  D17.2  D17.0
// D18.2  C19.1  D17.1  D19.0  D18.1  C19.0  C18.1
// C20.0  D20.1  D19.2  C20.1  D20.2  D20.0  D19.1
// D21.0  D21.2  C21.1  C22.0  D21.1  C21.0  D22.2
// C22.1  C23.0  D22.1  D23.0  D23.2  D22.0  C24.0
// D23.1  D24.0  D24.2  C24.1  C25.0  C23.1  D25.0
// D25.2  C25.1  C26.0  D25.1  D26.0  D24.1  C26.1
// C27.0  D26.1  D27.0  D27.2  C27.1  D26.2  D27.1
// D28.0  C28.0  C29.0  D28.2  D29.2  D28.1  C28.1
// C29.1  D29.0  D30.0  C30.0  C31.0  D30.2  D29.1
// D30.1  C30.1  C31.1  D31.0  D32.0  C32.0  D31.2
// D32.2  D31.1  D32.1  C32.1  C33.1  D33.0  C33.0
// C34.0  D33.2  D34.2  D33.1  D34.1  C34.1  D34.0
// D35.0  C36.0  D36.2  C35.1  C35.0  D35.2  D35.1
// C36.1  D37.0  C38.0  D36.1  D36.0  C37.0  D37.2
// D37.1  C38.1  D39.0  D38.2  C37.1  D38.0  C39.0
// D39.2  D39.1  C40.1  C40.0  D38.1  C39.1  D40.0
// C41.0  D41.2  D41.1  D41.0  D40.2  D40.1  C41.1
#[test]
fn exhaustive_7_5_2() {
    let n = 7;
    let k = 5;
    let f = 2;
    let m = k - f;

    let locator = PrimeS::new(n, k, f);

    // Check forward mappings against a precomputed list
    let datachunks: Vec<(u64, i16, i16, u64)> = vec![
        ( 0, 0, 0, 0 ), ( 0, 1, 1, 0 ), ( 0, 2, 2, 0 ),
        ( 1, 0, 3, 1 ), ( 1, 1, 4, 1 ), ( 1, 2, 5, 0 ),
        ( 2, 0, 6, 1 ), ( 2, 1, 0, 2 ), ( 2, 2, 1, 1 ),
        ( 3, 0, 2, 2 ), ( 3, 1, 3, 3 ), ( 3, 2, 4, 2 ),
        ( 4, 0, 5, 2 ), ( 4, 1, 6, 3 ), ( 4, 2, 0, 3 ),
        ( 5, 0, 1, 3 ), ( 5, 1, 2, 4 ), ( 5, 2, 3, 4 ),
        ( 6, 0, 4, 4 ), ( 6, 1, 5, 4 ), ( 6, 2, 6, 4 ),
        ( 7, 0, 0, 5 ), ( 7, 1, 2, 5 ), ( 7, 2, 4, 5 ),
        ( 8, 0, 6, 6 ), ( 8, 1, 1, 6 ), ( 8, 2, 3, 5 ),
        ( 9, 0, 5, 6 ), ( 9, 1, 0, 7 ), ( 9, 2, 2, 6 ),
        ( 10, 0, 4, 7 ), ( 10, 1, 6, 8 ), ( 10, 2, 1, 7 ),
        ( 11, 0, 3, 7 ), ( 11, 1, 5, 8 ), ( 11, 2, 0, 8 ),
        ( 12, 0, 2, 8 ), ( 12, 1, 4, 9 ), ( 12, 2, 6, 9 ),
        ( 13, 0, 1, 9 ), ( 13, 1, 3, 9 ), ( 13, 2, 5, 9 ),
        ( 14, 0, 0, 10 ), ( 14, 1, 3, 10 ), ( 14, 2, 6, 10 ),
        ( 15, 0, 2, 11 ), ( 15, 1, 5, 11 ), ( 15, 2, 1, 10 ),
        ( 16, 0, 4, 11 ), ( 16, 1, 0, 12 ), ( 16, 2, 3, 11 ),
        ( 17, 0, 6, 12 ), ( 17, 1, 2, 13 ), ( 17, 2, 5, 12 ),
        ( 18, 0, 1, 12 ), ( 18, 1, 4, 13 ), ( 18, 2, 0, 13 ),
        ( 19, 0, 3, 13 ), ( 19, 1, 6, 14 ), ( 19, 2, 2, 14 ),
        ( 20, 0, 5, 14 ), ( 20, 1, 1, 14 ), ( 20, 2, 4, 14 ),
        ( 21, 0, 0, 15 ), ( 21, 1, 4, 15 ), ( 21, 2, 1, 15 ),
        ( 22, 0, 5, 16 ), ( 22, 1, 2, 16 ), ( 22, 2, 6, 15 ),
        ( 23, 0, 3, 16 ), ( 23, 1, 0, 17 ), ( 23, 2, 4, 16 ),
        ( 24, 0, 1, 17 ), ( 24, 1, 5, 18 ), ( 24, 2, 2, 17 ),
        ( 25, 0, 6, 17 ), ( 25, 1, 3, 18 ), ( 25, 2, 0, 18 ),
        ( 26, 0, 4, 18 ), ( 26, 1, 1, 19 ), ( 26, 2, 5, 19 ),
        ( 27, 0, 2, 19 ), ( 27, 1, 6, 19 ), ( 27, 2, 3, 19 ),
        ( 28, 0, 0, 20 ), ( 28, 1, 5, 20 ), ( 28, 2, 3, 20 ),
        ( 29, 0, 1, 21 ), ( 29, 1, 6, 21 ), ( 29, 2, 4, 20 ),
        ( 30, 0, 2, 21 ), ( 30, 1, 0, 22 ), ( 30, 2, 5, 21 ),
        ( 31, 0, 3, 22 ), ( 31, 1, 1, 23 ), ( 31, 2, 6, 22 ),
        ( 32, 0, 4, 22 ), ( 32, 1, 2, 23 ), ( 32, 2, 0, 23 ),
        ( 33, 0, 5, 23 ), ( 33, 1, 3, 24 ), ( 33, 2, 1, 24 ),
        ( 34, 0, 6, 24 ), ( 34, 1, 4, 24 ), ( 34, 2, 2, 24 ),
        ( 35, 0, 0, 25 ), ( 35, 1, 6, 25 ), ( 35, 2, 5, 25 ),
        ( 36, 0, 4, 26 ), ( 36, 1, 3, 26 ), ( 36, 2, 2, 25 ),
        ( 37, 0, 1, 26 ), ( 37, 1, 0, 27 ), ( 37, 2, 6, 26 ),
        ( 38, 0, 5, 27 ), ( 38, 1, 4, 28 ), ( 38, 2, 3, 27 ),
        ( 39, 0, 2, 27 ), ( 39, 1, 1, 28 ), ( 39, 2, 0, 28 ),
        ( 40, 0, 6, 28 ), ( 40, 1, 5, 29 ), ( 40, 2, 4, 29 ),
        ( 41, 0, 3, 29 ), ( 41, 1, 2, 29 ), ( 41, 2, 1, 29 ),
    ];
    let paritychunks: Vec<(u64, i16, i16, u64)> = vec![
        ( 0, 0, 3, 0 ), ( 0, 1, 4, 0 ), ( 1, 0, 6, 0 ), ( 1, 1, 0, 1 ),
        ( 2, 0, 2, 1 ), ( 2, 1, 3, 2 ), ( 3, 0, 5, 1 ), ( 3, 1, 6, 2 ),
        ( 4, 0, 1, 2 ), ( 4, 1, 2, 3 ), ( 5, 0, 4, 3 ), ( 5, 1, 5, 3 ),
        ( 6, 0, 0, 4 ), ( 6, 1, 1, 4 ), ( 7, 0, 6, 5 ), ( 7, 1, 1, 5 ),
        ( 8, 0, 5, 5 ), ( 8, 1, 0, 6 ), ( 9, 0, 4, 6 ), ( 9, 1, 6, 7 ),
        ( 10, 0, 3, 6 ), ( 10, 1, 5, 7 ), ( 11, 0, 2, 7 ), ( 11, 1, 4, 8 ),
        ( 12, 0, 1, 8 ), ( 12, 1, 3, 8 ), ( 13, 0, 0, 9 ), ( 13, 1, 2, 9 ),
        ( 14, 0, 2, 10 ), ( 14, 1, 5, 10 ), ( 15, 0, 4, 10 ), ( 15, 1, 0, 11 ),
        ( 16, 0, 6, 11 ), ( 16, 1, 2, 12 ), ( 17, 0, 1, 11 ), ( 17, 1, 4, 12 ),
        ( 18, 0, 3, 12 ), ( 18, 1, 6, 13 ), ( 19, 0, 5, 13 ), ( 19, 1, 1, 13 ),
        ( 20, 0, 0, 14 ), ( 20, 1, 3, 14 ), ( 21, 0, 5, 15 ), ( 21, 1, 2, 15 ),
        ( 22, 0, 3, 15 ), ( 22, 1, 0, 16 ), ( 23, 0, 1, 16 ), ( 23, 1, 5, 17 ),
        ( 24, 0, 6, 16 ), ( 24, 1, 3, 17 ), ( 25, 0, 4, 17 ), ( 25, 1, 1, 18 ),
        ( 26, 0, 2, 18 ), ( 26, 1, 6, 18 ), ( 27, 0, 0, 19 ), ( 27, 1, 4, 19 ),
        ( 28, 0, 1, 20 ), ( 28, 1, 6, 20 ), ( 29, 0, 2, 20 ), ( 29, 1, 0, 21 ),
        ( 30, 0, 3, 21 ), ( 30, 1, 1, 22 ), ( 31, 0, 4, 21 ), ( 31, 1, 2, 22 ),
        ( 32, 0, 5, 22 ), ( 32, 1, 3, 23 ), ( 33, 0, 6, 23 ), ( 33, 1, 4, 23 ),
        ( 34, 0, 0, 24 ), ( 34, 1, 5, 24 ), ( 35, 0, 4, 25 ), ( 35, 1, 3, 25 ),
        ( 36, 0, 1, 25 ), ( 36, 1, 0, 26 ), ( 37, 0, 5, 26 ), ( 37, 1, 4, 27 ),
        ( 38, 0, 2, 26 ), ( 38, 1, 1, 27 ), ( 39, 0, 6, 27 ), ( 39, 1, 5, 28 ),
        ( 40, 0, 3, 28 ), ( 40, 1, 2, 28 ), ( 41, 0, 0, 29 ), ( 41, 1, 6, 29 )
    ];

    for chunk in datachunks {
        let id = ChunkId::Data(chunk.0 * m as u64 + chunk.1 as u64);
        let loc = Chunkloc::new(chunk.2, chunk.3);
        assert_eq!(loc, locator.id2loc(id));
    }
    for chunk in paritychunks {
        let id = ChunkId::Parity(chunk.0 * m as u64, chunk.1);
        let loc = Chunkloc::new(chunk.2, chunk.3);
        assert_eq!(loc, locator.id2loc(id));
    }


    // Now check reverse mappings
    for stripe in 0 .. locator.stripes() {
        // First check data chunks
        for u in 0 .. m {
            let chunkid = ChunkId::Data(u64::from(stripe) * m as u64 + u as u64);
            assert_eq!(locator.loc2id(locator.id2loc(chunkid)), chunkid);
        }

        // Now check parity chunks
        for u in 0 .. f {
            let chunkid = ChunkId::Parity(u64::from(stripe) * m as u64, u);
            assert_eq!(locator.loc2id(locator.id2loc(chunkid)), chunkid);
        }
    }
}

// Exhaustive iterator output for a small array.
// Compare the iterator output against id2loc, which itself is compared against
// the ruby program.
#[test]
fn iter_7_5_2() {
    let n = 7;
    let k = 5;
    let f = 2;
    let reps = 2;
    let m = k - f;

    let locator = PrimeS::new(n, k, f);
    let end = ChunkId::Data(locator.datachunks() * reps);
    let mut iter = locator.iter(ChunkId::Data(0), end);
    let mut iter_data = locator.iter_data(ChunkId::Data(0), end);

    for rep in 0..reps {
        for s in 0..locator.stripes() {
            for a in 0..m {
                let id = ChunkId::Data(rep * locator.datachunks() +
                                       u64::from(s) * m as u64 + a as u64);
                assert_eq!((id, locator.id2loc(id)), iter.next().unwrap());
                assert_eq!((id, locator.id2loc(id)), iter_data.next().unwrap());
            }
            for p in 0..f {
                let id = ChunkId::Parity(rep * locator.datachunks() +
                                         u64::from(s) * m as u64, p);
                assert_eq!((id, locator.id2loc(id)), iter.next().unwrap());
            }
        }
    }
    assert!(iter.next().is_none());
    assert!(iter_data.next().is_none());
}

// Test parallel_read_count for a typical layout
#[test]
fn parallel_read_count() {
    let n = 5;
    let k = 4;
    let f = 2;
    let locator = PrimeS::new(n, k, f);

    assert_eq!(locator.parallel_read_count(1), 2);
    assert_eq!(locator.parallel_read_count(2), 2);
    assert_eq!(locator.parallel_read_count(3), 2);
    assert_eq!(locator.parallel_read_count(4), 2);
    assert_eq!(locator.parallel_read_count(5), 2);
    assert_eq!(locator.parallel_read_count(6), 3);
}

// Test repetition calculations for a simple layout
#[test]
fn repetition() {
    let n = 7;
    let k = 5;
    let f = 2;

    let locator = PrimeS::new(n, k, f);
    // Repetition 0
    assert_eq!(locator.id2loc(ChunkId::Data(0)).offset, 0);
    assert_eq!(locator.id2loc(ChunkId::Parity(0, 0)).offset, 0);
    assert_eq!(locator.id2loc(ChunkId::Parity(0, 1)).offset, 0);
    assert_eq!(locator.loc2id(Chunkloc::new(0, 0)), ChunkId::Data(0));
    // Repetition 1
    assert_eq!(locator.id2loc(ChunkId::Data(126)).offset, 30);
    assert_eq!(locator.id2loc(ChunkId::Parity(126, 0)).offset, 30);
    assert_eq!(locator.id2loc(ChunkId::Parity(126, 1)).offset, 30);
    assert_eq!(locator.loc2id(Chunkloc::new(0, 30)), ChunkId::Data(126));
    // Repetition u32::max_value()
    assert_eq!(locator.id2loc(ChunkId::Data(541_165_879_170)).offset,
        128_849_018_850);
    assert_eq!(locator.id2loc(ChunkId::Parity(541_165_879_170, 0)).offset,
        128_849_018_850);
    assert_eq!(locator.id2loc(ChunkId::Parity(541_165_879_170, 1)).offset,
        128_849_018_850);
    assert_eq!(locator.loc2id(Chunkloc::new(0, 128_849_018_850)),
        ChunkId::Data(541_165_879_170));
}
