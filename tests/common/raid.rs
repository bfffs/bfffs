use arkfs::common::raid::*;
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use rand;
use rand::Rng;

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

// Test basic RAID functionality using a small chunksize
#[test]
pub fn encode_decode() {
    let len = 8;
    let codec = Codec::new(3, 1);
    let mut rng = rand::thread_rng();

    // First, encode
    let mut d0 = vec![0u8;len];
    let mut d1 = vec![0u8;len];
    let mut p0 = vec![0u8;len];
    for i in 0..len {
        d0[i] = rng.gen();
        d1[i] = rng.gen();
    }
    codec.encode(len, &[d0.as_ptr(), d1.as_ptr()], &[p0.as_mut_ptr()]);

    // Now delete column 0 and rebuild
    let mut r0 = vec![0u8;len];
    let mut erasures = FixedBitSet::with_capacity(3);
    erasures.insert(0);
    codec.decode(len, &[d1.as_ptr(), p0.as_ptr()], &[r0.as_mut_ptr()], &erasures);

    // Verify that column was reconstructed correctly
    assert_eq!(d0, r0);
}

// Test basic RAID update functionality using a small chunksize
#[test]
pub fn encode_update_decode() {
    let len = 8;
    let codec = Codec::new(3, 1);
    let mut rng = rand::thread_rng();

    // First, encode
    let mut d0 = vec![0u8;len];
    let mut d1 = vec![0u8;len];
    let mut p0 = vec![0u8;len];
    for i in 0..len {
        d0[i] = rng.gen();
        d1[i] = rng.gen();
    }
    codec.encode_update(len, &d0, &[p0.as_mut_ptr()], 0);
    codec.encode_update(len, &d1, &[p0.as_mut_ptr()], 1);

    // Now delete column 0 and rebuild
    let mut r0 = vec![0u8;len];
    let mut erasures = FixedBitSet::with_capacity(3);
    erasures.insert(0);
    codec.decode(len, &[d1.as_ptr(), p0.as_ptr()], &[r0.as_mut_ptr()], &erasures);

    // Verify that column was reconstructed correctly
    assert_eq!(d0, r0);
}


// Roundtrip data through the codec for various array sizes and erasure sets
#[test]
pub fn comprehensive() {
    let cfgs = [
        (3, 1), (9, 1),
        (4, 2), (10, 2),
        (6, 3), (19, 3),
        (8, 4), (20, 4)
    ];

    let len = 64;
    let maxdata = 28;
    let maxparity = 4;
    let mut rng = rand::thread_rng();
    let mut data = Vec::<Vec<u8>>::new();
    let mut parity = Vec::<Vec<u8>>::new();
    let mut reconstructed = Vec::<Vec<u8>>::new();
    for i in 0..maxdata {
        let mut column = Vec::<u8>::with_capacity(len);
        for j in 0..len {
            column.push(rng.gen());
        }
        data.push(column);
    }
    for i in 0..maxparity {
        let column = vec![0u8; len];
        parity.push(column);
    }
    for i in 0..(maxparity as usize) {
        reconstructed.push(vec![0u8; len]);
    }

    for cfg in cfgs.iter() {
        let m = cfg.0;
        let f = cfg.1;
        let k = m - f;
        let codec = Codec::new(m, f);

        // First encode
        let mut input = Vec::<*const u8>::with_capacity(m as usize);
        for i in 0..(k as usize) {
            input.push(data[i].as_ptr());
        }
        let mut output = Vec::<*mut u8>::with_capacity(f as usize);
        for i in 0..(f as usize) {
            output.push(parity[i].as_mut_ptr());
        }
        codec.encode(len, &input, &output);

        // Iterate over all possible failure combinations
        for erasures_vec in (0..m).combinations(f as usize) {
            // Don't attempt to decode if the only missing columns are parity
            if erasures_vec[0] >= k {
                continue;
            }

            // Decode
            let mut surviving = Vec::<*const u8>::with_capacity(m as usize);
            let mut erasures = FixedBitSet::with_capacity(m as usize);
            for b in &erasures_vec {
                erasures.insert(*b as usize);
            }
            let mut r = 0;
            for i in 0..(k as usize) {
                while erasures.contains(r) {
                    r += 1;
                }
                if r < k as usize {
                    surviving.push(data[r].as_ptr());
                } else {
                    surviving.push(parity[r - k as usize].as_ptr());
                }
                r += 1;
            }
            let data_errs = erasures.count_ones(..k as usize);
            let mut decoded = Vec::<*mut u8>::with_capacity(data_errs as usize);
            for i in 0..(data_errs as usize) {
                decoded.push(reconstructed[i].as_mut_ptr());
            }
            codec.decode(len, &surviving, &decoded, &erasures);

            // Finally, compare
            for i in 0..(data_errs as usize) {
                assert_eq!(&data[erasures_vec[i] as usize], &reconstructed[i],
                    "miscompare for m={:?}, f={:?}, erasures={:?}",
                    m, f, erasures_vec);
            }
        }
    }
}
