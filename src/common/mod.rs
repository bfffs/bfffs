// vim: tw=80

use divbuf::{DivBuf, DivBufMut, DivBufShared};
use std::hash::Hasher;
use std::ops::{Add, Div, Sub};

pub mod cluster;
pub mod declust;
pub mod dva;
pub mod null_raid;
pub mod prime_s;
pub mod raid;
pub mod label;
pub mod pool;
mod sgcursor;
pub mod vdev;
pub mod vdev_block;
pub mod vdev_leaf;
pub mod vdev_raid;

pub use self::sgcursor::SGCursor;

/// Indexes a `Cluster` within the `Pool`.
pub type ClusterT = u16;

/// Indexes an LBA.  LBAs are always 4096 bytes
pub type LbaT = u64;

/// Indexes a `Vdev`'s Zones.  A Zone is the smallest allocation unit that can
/// be independently erased.
pub type ZoneT = u32;

/// Our `IoVec`.  Unlike the standard library's, ours is reference-counted so it
/// can have more than one owner.
pub type IoVec = DivBuf;

/// Mutable version of `IoVec`.  Uniquely owned.
pub type IoVecMut = DivBufMut;

/// Our scatter-gather list.  A slice of reference-counted `IoVec`s.
pub type SGList = Vec<IoVec>;

/// Mutable version of `SGList`.  Uniquely owned.
pub type SGListMut = Vec<IoVecMut>;

/// LBAs always use 4K LBAs, even if the underlying device supports smaller.
pub const BYTES_PER_LBA: usize = 4096;

/// A Fragment is the smallest amount of space that can be independently
/// allocated.  Several small files can have their fragments packed into a
/// single LBA.
pub const BYTES_PER_FRAGMENT: usize = 256;

lazy_static! {
    /// A read-only buffer of zeros, useful for padding.
    ///
    /// The length is pretty arbitrary.  Code should be able to cope with a
    /// smaller-than-desired `ZERO_REGION`.  A smaller size will have less
    /// impact on the CPU cache.  A larger size will consume fewer CPU cycles
    /// manipulating sglists.
    static ref ZERO_REGION: DivBufShared =
        DivBufShared::from(vec![0u8; BYTES_PER_LBA]);
}

/// Create an SGList full of zeros, with the requested total length
fn zero_sglist(len: usize) -> SGList {
    let zero_region_len = ZERO_REGION.len();
    let zero_bufs = div_roundup(len, zero_region_len);
    let mut sglist = SGList::new();
    for _ in 0..(zero_bufs - 1) {
        sglist.push(ZERO_REGION.try().unwrap())
    }
    sglist.push(ZERO_REGION.try().unwrap().slice_to(
            len - (zero_bufs - 1) * zero_region_len));
    sglist
}

/// "Private" trait; only exists to ensure that div_roundup will fail to compile
/// when used with signed numbers.  It would be nice to use a negative trait
/// bound like "+ !Neg", but Rust doesn't support negative trait bounds.
#[doc(hidden)]
pub trait RoundupAble {}
impl RoundupAble for u8 {}
impl RoundupAble for u16 {}
impl RoundupAble for u32 {}
impl RoundupAble for u64 {}
impl RoundupAble for usize {}

/// Divide two unsigned numbers (usually integers), rounding up.
pub fn div_roundup<T>(dividend: T, divisor: T) -> T
    where T: Add<Output=T> + Copy + Div<Output=T> + From<u8> + RoundupAble +
             Sub<Output=T> {
    (dividend + divisor - T::from(1u8)) / divisor

}

/// hash an `SGList`.
///
/// Unfortunately, hashing a slice is not the same thing as hashing that slice's
/// contents.  Nor is `Vec::hash` for a `Vec` of `Vec`s equivalent to
/// `Vec::hash` for a flat `Vec` with the same contents.
///
/// Ideally we would just `impl Hash for SGList`, but that's not allowed on type
/// aliases.
pub fn hash_sglist<T: AsRef<[u8]>, H: Hasher>(sglist: &Vec<T>, hasher: &mut H) {
    for buf in sglist {
        let s: &[u8] = buf.as_ref();
        hasher.write(s);
    }
}

#[test]
fn test_div_roundup() {
    assert_eq!(div_roundup(5u8, 2u8), 3u8);
    assert_eq!(div_roundup(4u8, 2u8), 2u8);
    assert_eq!(div_roundup(4000u32, 1500u32), 3u32);
}

#[cfg(test)]
macro_rules! test_hash_sglist_helper {
    ( $klass:ident) => {
        let together = vec![0u8, 1, 2, 3, 4, 5];
        let apart = vec![vec![0u8, 1], vec![2u8, 3], vec![4u8, 5]];
        let mut together_hasher = $klass::new();
        let mut apart_hasher = $klass::new();
        let mut single_hasher = $klass::new();
        together_hasher.write(&together[..]);
        hash_sglist(&apart, &mut apart_hasher);
        single_hasher.write_u8(0);
        single_hasher.write_u8(1);
        single_hasher.write_u8(2);
        single_hasher.write_u8(3);
        single_hasher.write_u8(4);
        single_hasher.write_u8(5);
        assert_eq!(together_hasher.finish(), apart_hasher.finish());
    }
}

#[test]
fn test_hash_sglist_default_hasher() {
    use std::collections::hash_map::DefaultHasher;

    test_hash_sglist_helper!(DefaultHasher);
}

#[test]
fn test_hash_sglist_metrohash64() {
    use metrohash::MetroHash64;

    test_hash_sglist_helper!(MetroHash64);
}
