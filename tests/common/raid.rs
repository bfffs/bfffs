use arkfs::common::raid::*;
use divbuf::*;
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use rand;
use rand::Rng;

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
    codec.decode(len, &[d1.as_ptr(), p0.as_ptr()], &[r0.as_mut_ptr()],
                 &erasures);

    // Verify that column was reconstructed correctly
    assert_eq!(d0, r0);
}

// Test encoding from discontiguous data columns
#[test]
pub fn encodev() {
    let len = 16;
    let codec = Codec::new(3, 1);
    let mut rng = rand::thread_rng();

    // First, make the reference parity using contiguous encode
    let mut da0 = vec![0u8;len];
    let mut da1 = vec![0u8;len];
    let mut pa0 = vec![0u8;len];
    for i in 0..len {
        da0[i] = rng.gen();
        da1[i] = rng.gen();
    }
    codec.encode(len, &[da0.as_ptr(), da1.as_ptr()], &[pa0.as_mut_ptr()]);

    // Next, split the same data into discontiguous SGLists
    // First segments are identically sized
    let db0p0 = DivBufShared::from(Vec::from(&da0[0..4]));
    let db1p0 = DivBufShared::from(Vec::from(&da1[0..4]));
    // db0 has longer 2nd segment
    let db0p1 = DivBufShared::from(Vec::from(&da0[4..9]));
    let db1p1 = DivBufShared::from(Vec::from(&da1[4..8]));
    // db1 has longer 3rd segment
    let db0p2 = DivBufShared::from(Vec::from(&da0[9..14]));
    let db1p2 = DivBufShared::from(Vec::from(&da1[8..14]));
    // final segments are identically sized
    let db0p3 = DivBufShared::from(Vec::from(&da0[14..len]));
    // final segments are identically sized
    let db1p3 = DivBufShared::from(Vec::from(&da1[14..len]));
    let sgb0 = vec![db0p0.try().unwrap(),
                    db0p1.try().unwrap(),
                    db0p2.try().unwrap(),
                    db0p3.try().unwrap()];
    let sgb1 = vec![db1p0.try().unwrap(),
                    db1p1.try().unwrap(),
                    db1p2.try().unwrap(),
                    db1p3.try().unwrap()];
    let data = vec![sgb0, sgb1];
    let pa1 = vec![0u8;len];
    let mut pslice = [pa1];
    codec.encodev(len, &data, &mut pslice[..]);

    assert_eq!(pa0, pslice[0]);
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
    for _ in 0..maxdata {
        let mut column = Vec::<u8>::with_capacity(len);
        for _ in 0..len {
            column.push(rng.gen());
        }
        data.push(column);
    }
    for _ in 0..maxparity {
        let column = vec![0u8; len];
        parity.push(column);
    }
    for _ in 0..(maxparity as usize) {
        reconstructed.push(vec![0u8; len]);
    }

    for cfg in &cfgs {
        let m = cfg.0;
        let f = cfg.1;
        let k = m - f;
        let codec = Codec::new(m, f);

        // First encode
        let mut input = Vec::<*const u8>::with_capacity(m as usize);
        for x in data.iter().take(k as usize) {
            input.push(x.as_ptr());
        }
        let mut output = Vec::<*mut u8>::with_capacity(f as usize);
        for x in parity.iter_mut().take(f as usize) {
            output.push(x.as_mut_ptr());
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
            let mut skips = 0;
            for i in 0..(k as usize) {
                while erasures.contains(i + skips) {
                    skips += 1;
                }
                let r = i + skips;
                if r < k as usize {
                    surviving.push(data[r].as_ptr());
                } else {
                    surviving.push(parity[r - k as usize].as_ptr());
                }
            }
            let data_errs = erasures.count_ones(..k as usize);
            let mut decoded = Vec::<*mut u8>::with_capacity(data_errs as usize);
            for x in reconstructed.iter_mut().take(data_errs as usize) {
                decoded.push(x.as_mut_ptr());
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
