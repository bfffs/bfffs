use arkfs::common::raid::*;
use fixedbitset::FixedBitSet;
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
pub fn test_new_2plus1() {
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
    codec.decode(len, &[d1.as_ptr(), p0.as_ptr()], &[r0.as_mut_ptr()], erasures);

    // Verify that column was reconstructed correctly
    assert_eq!(d0, r0);
}
