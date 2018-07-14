// vim: tw=80

use divbuf::{DivBuf, DivBufMut, DivBufShared};
use std::{hash::Hasher, ops::{Add, AddAssign, Div, Sub}};

pub mod cache;
#[cfg(test)] mod cache_mock;
pub mod cleaner;
pub mod cluster;
pub mod database;
pub mod dataset;
pub mod ddml;
#[cfg(test)] mod ddml_mock;
pub mod declust;
pub mod dml;
pub mod dva;
pub mod idml;
#[cfg(test)] mod idml_mock;
pub mod label;
pub mod null_raid;
pub mod pool;
pub mod prime_s;
pub mod raid;
mod sgcursor;
pub mod tree;
pub mod vdev;
pub mod vdev_block;
pub mod vdev_leaf;
pub mod vdev_raid;

pub use self::sgcursor::SGCursor;

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
        DivBufShared::from(vec![0u8; 8 * BYTES_PER_LBA]);
}

/// Indexes a `Cluster` within the `Pool`.
pub type ClusterT = u16;

/// Our `IoVec`.  Unlike the standard library's, ours is reference-counted so it
/// can have more than one owner.
pub type IoVec = DivBuf;

/// Mutable version of `IoVec`.  Uniquely owned.
pub type IoVecMut = DivBufMut;

/// Indexes an LBA.  LBAs are always 4096 bytes
pub type LbaT = u64;

/// Transaction numbers.
// 32-bits is enough for 1 per second for 100 years
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd,
         Serialize)]
pub struct TxgT(u32);

impl Add<u32> for TxgT {
    type Output = Self;

    fn add(self, rhs: u32) -> Self::Output {
        TxgT(self.0 + rhs)
    }
}

impl AddAssign<u32> for TxgT {
    fn add_assign(&mut self, rhs: u32) {
        *self = TxgT(self.0 + rhs)
    }
}

impl From<u32> for TxgT {
    fn from(t: u32) -> Self {
        TxgT(t)
    }
}

impl Sub<u32> for TxgT {
    type Output = Self;

    fn sub(self, rhs: u32) -> Self::Output {
        TxgT(self.0 - rhs)
    }
}

/// Physical Block Address.
///
/// Locates a block of storage within a pool.  A block is the smallest amount of
/// data that can be transferred to/from a disk.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, Hash, Ord,
         PartialEq, PartialOrd)]
pub struct PBA {
    cluster: ClusterT,
    lba: LbaT
}

impl PBA {
    fn new(cluster: ClusterT, lba: LbaT) -> Self {
        PBA {cluster, lba}
    }
}

/// Record ID
///
/// Uniquely identifies each indirect record.  Record IDs are never reused.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord,
         Serialize)]
pub struct RID(u64);

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

/// Our scatter-gather list.  A slice of reference-counted `IoVec`s.
pub type SGList = Vec<IoVec>;

/// Mutable version of `SGList`.  Uniquely owned.
pub type SGListMut = Vec<IoVecMut>;

/// Indexes a `Vdev`'s Zones.  A Zone is the smallest allocation unit that can
/// be independently erased.
pub type ZoneT = u32;

/// Checksum an `IoVec`
///
/// See also [`checksum_sglist`](fn.checksum_sglist.html) for an explanation of
/// why this function is necessary.
pub fn checksum_iovec<T: AsRef<[u8]>, H: Hasher>(iovec: &T, hasher: &mut H) {
    hasher.write(iovec.as_ref());
}

/// Checksum an `SGList`.
///
/// Unfortunately, hashing a slice is not the same thing as hashing that slice's
/// contents.  The former includes the length of the hash.  That is deliberate
/// so that, for example, the tuples `([0, 1], [2, 3])` and `([0], [1, 2, 3])`
/// have different hashes.  That property is desirable for example when storing
/// tuples in a hash table.  But for our purposes, we *want* such tuples to
/// compare the same so that a record will have the same hash whether it's
/// written as a single `iovec` or an `SGList`.
///
/// Ideally we would just `impl Hash for SGList`, but that's not allowed on type
/// aliases.
///
/// See Also [Rust issue 5237](https://github.com/rust-lang/rust/issues/5257)
pub fn checksum_sglist<T, H>(sglist: &[T], hasher: &mut H)
    where T: AsRef<[u8]>, H: Hasher {

    for buf in sglist {
        let s: &[u8] = buf.as_ref();
        hasher.write(s);
    }
}

/// Divide two unsigned numbers (usually integers), rounding up.
pub fn div_roundup<T>(dividend: T, divisor: T) -> T
    where T: Add<Output=T> + Copy + Div<Output=T> + From<u8> + RoundupAble +
             Sub<Output=T> {
    (dividend + divisor - T::from(1u8)) / divisor

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

// LCOV_EXCL_START
#[test]
fn test_div_roundup() {
    assert_eq!(div_roundup(5u8, 2u8), 3u8);
    assert_eq!(div_roundup(4u8, 2u8), 2u8);
    assert_eq!(div_roundup(4000u32, 1500u32), 3u32);
}

#[cfg(test)]
macro_rules! test_checksum_sglist_helper {
    ( $klass:ident) => {
        let together = vec![0u8, 1, 2, 3, 4, 5];
        let apart = vec![vec![0u8, 1], vec![2u8, 3], vec![4u8, 5]];
        let mut together_hasher = $klass::new();
        let mut apart_hasher = $klass::new();
        let mut single_hasher = $klass::new();
        together_hasher.write(&together[..]);
        checksum_sglist(&apart, &mut apart_hasher);
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
fn test_checksum_sglist_default_hasher() {
    use std::collections::hash_map::DefaultHasher;

    test_checksum_sglist_helper!(DefaultHasher);
}

#[test]
fn test_checksum_sglist_metrohash64() {
    use metrohash::MetroHash64;

    test_checksum_sglist_helper!(MetroHash64);
}
// LCOV_EXCL_STOP
