// vim: tw=80
//! Common utility functions used throughout BFFFS

use crate::types::*;
use divbuf::DivBufShared;
use lazy_static::lazy_static;
use std::{
    any::TypeId,
    hash::Hasher,
    mem,
    ops::{Add, Bound, Div, RangeBounds, Sub},
};


/// LBAs always use 4K LBAs, even if the underlying device supports smaller.
pub const BYTES_PER_LBA: usize = 4096;

/// Length of the global read-only `ZERO_REGION`
pub const ZERO_REGION_LEN: usize = 8 * BYTES_PER_LBA;

lazy_static! {
    /// A read-only buffer of zeros, useful for padding.
    ///
    /// The length is pretty arbitrary.  Code should be able to cope with a
    /// smaller-than-desired `ZERO_REGION`.  A smaller size will have less
    /// impact on the CPU cache.  A larger size will consume fewer CPU cycles
    /// manipulating sglists.
    pub static ref ZERO_REGION: DivBufShared =
        DivBufShared::from(vec![0u8; ZERO_REGION_LEN]);
}

/// Transmute one type into another, if they're really the same type.
///
/// Useful for implementing generic functions where two different types should
/// be identical in every valid instantiation, but this can't be checked at
/// compile-time.
pub(crate) fn checked_transmute<T: 'static, Q: Copy + 'static>(t: T) -> Q {
    debug_assert_eq!(mem::size_of::<T>(), mem::size_of::<Q>());
    debug_assert_eq!(TypeId::of::<T>(), TypeId::of::<Q>());
    // Safe because we checked type id
    unsafe{ *(&t as *const T as *const Q) }
}

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

/// Prepare an sglist for writing by copying and padding the individual
/// iovecs, if necessary, to ensure that each is a multiple of the
/// blocksize.
pub fn copy_and_pad_sglist(bufs: SGList) -> SGList {
    if bufs.iter().any(|db| db.len() % BYTES_PER_LBA != 0) {
        // We must copy data to make all writes block-sized.  We do it here
        // rather than upstack to minimize the time that the copied data
        // must live.  And we can't do it downstack, because it needs to
        // happen prior to the scheduler's accumulation of adjacent
        // operations.
        // We must copy partial-block divbufs, rather than extend them,
        // because we don't want to modify data that might be in the Cache.
        let mut outlist = SGList::with_capacity(bufs.len());
        let mut accumulator: Option<Vec<u8>> = None;
        for mut db in bufs.into_iter() {
            if db.len() % BYTES_PER_LBA == 0 {
                assert!(accumulator.is_none());
                outlist.push(db);
                continue
            }
            if let Some(ref mut accum) = accumulator {
                if db.len() > BYTES_PER_LBA {
                    unimplemented!();
                }
                // Data copy
                accum.extend(&db[..]);
                if accum.len() % BYTES_PER_LBA == 0 {
                    let dbs = DivBufShared::from(
                        accumulator.take().unwrap()
                    );
                    let db = dbs.try_const().unwrap();
                    outlist.push(db);
                }
            } else {
                if db.len() > BYTES_PER_LBA {
                    let wlen = db.len() & !(BYTES_PER_LBA - 1);
                    outlist.push(db.split_to(wlen));
                }
                // Data copy
                accumulator = Some(Vec::from(&db[..]));
            }
        }
        if let Some(ref mut accum) = accumulator {
            // Must've been an incomplete block.  zero-pad the tail
            let l = div_roundup(accum.len(), BYTES_PER_LBA) * BYTES_PER_LBA;
            // Data copy
            accum.resize(l, 0);
            let dbs = DivBufShared::from(accumulator.take().unwrap());
            let db = dbs.try_const().unwrap();
            outlist.push(db);
        }
        outlist
    } else {
        bufs
    }
}

/// Divide two unsigned numbers (usually integers), rounding up.
pub fn div_roundup<T>(dividend: T, divisor: T) -> T
    where T: Add<Output=T> + Copy + Div<Output=T> + From<u8> + RoundupAble +
             Sub<Output=T> {
    (dividend + divisor - T::from(1u8)) / divisor

}

/// Return the length of data in an sglist, not the number of iovecs
pub fn sglist_len<T>(sglist: &[T]) -> usize
    where T: std::ops::Deref<Target=[u8]>
{
    sglist.iter().fold(0usize, |accumulator, buf| {
        accumulator + buf.len()
    })
}

/// Create an SGList full of zeros, with the requested total length
pub fn zero_sglist(len: usize) -> SGList {
    let zero_region_len = ZERO_REGION.len();
    let zero_bufs = div_roundup(len, zero_region_len);
    let mut sglist = SGList::new();
    for _ in 0..(zero_bufs - 1) {
        sglist.push(ZERO_REGION.try_const().unwrap())
    }
    sglist.push(ZERO_REGION.try_const().unwrap().slice_to(
            len - (zero_bufs - 1) * zero_region_len));
    sglist
}

// Sure would be nice if this were in std
pub trait RangeBoundsExt<T>: RangeBounds<T>
    where T: PartialOrd<T>
{
    fn is_empty(&self) -> bool {
        match (self.start_bound(), self.end_bound()) {
            (Bound::Included(s), Bound::Included(e)) => s > e,
            (Bound::Included(s), Bound::Excluded(e)) => s >= e,
            (Bound::Excluded(s), Bound::Included(e)) => s >= e,
            (Bound::Excluded(s), Bound::Excluded(e)) => s > e,
            _ => false
        }
    }
}

impl<R, T> RangeBoundsExt<T> for R
    where R: RangeBounds<T>,
          T: PartialOrd<T>
{}

// LCOV_EXCL_START
#[cfg(test)]
/// Helper to generate the runtime used by most unit tests
pub fn basic_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
}

#[cfg(test)]
mod t {
use pretty_assertions::assert_eq;
use super::*;

#[test]
fn test_div_roundup() {
    assert_eq!(div_roundup(5u8, 2u8), 3u8);
    assert_eq!(div_roundup(4u8, 2u8), 2u8);
    assert_eq!(div_roundup(4000u32, 1500u32), 3u32);
}

#[cfg(test)]
macro_rules! checksum_sglist_helper {
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
fn checksum_sglist_default_hasher() {
    use std::collections::hash_map::DefaultHasher;

    checksum_sglist_helper!(DefaultHasher);
}

#[test]
fn checksum_sglist_metrohash64() {
    use metrohash::MetroHash64;

    checksum_sglist_helper!(MetroHash64);
}

#[test]
fn test_sglist_len() {
    assert_eq!(0, sglist_len::<&[u8]>(&[]));
    assert_eq!(0, sglist_len::<&[u8]>(&[&[][..]]));
    assert_eq!(1, sglist_len(&[&[42u8][..]]));
    assert_eq!(6, sglist_len(&[&[42u8, 43, 44, 45][..], &[46, 47][..]]));
}

#[test]
fn test_zero_sglist() {
    let sg0 = zero_sglist(100);
    assert_eq!(&sg0[0][..], &[0u8; 100][..]);
    assert_eq!(sg0.len(), 1);

    let sg1 = zero_sglist(ZERO_REGION_LEN + 100);
    assert_eq!(&sg1[0][..], &[0u8; ZERO_REGION_LEN][..]);
    assert_eq!(&sg1[1][..], &[0u8; 100][..]);
    assert_eq!(sg1.len(), 2);
}

#[allow(clippy::reversed_empty_ranges)]
#[test]
fn range_bounds_is_empty() {
    use std::ops::RangeFull;

    // Range
    assert!((0..0).is_empty());
    assert!((1..0).is_empty());
    assert!(!(0..1).is_empty());

    // RangeFrom
    assert!(!(0..).is_empty());

    // RangeFull
    assert!(!<RangeFull as RangeBoundsExt<i32>>::is_empty(&(..)));

    // RangeInclusive
    assert!((1..=0).is_empty());
    assert!((2..=0).is_empty());
    assert!(!(0..=0).is_empty());
    assert!(!(0..=1).is_empty());

    // RangeTo
    assert!(!(..0).is_empty());

    // RangeToInclusive
    assert!(!(..=0).is_empty());
}

}
// LCOV_EXCL_STOP
