// vim: tw=80

use divbuf::{DivBuf, DivBufMut};
use std::ops::{Add, Div, Sub};

pub mod declust;
pub mod dva;
pub mod prime_s;
pub mod raid;
mod sgcursor;
pub mod vdev;
pub mod vdev_block;
pub mod vdev_leaf;
pub mod vdev_raid;
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
pub use self::sgcursor::SGCursor;

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

#[test]
fn test_div_roundup() {
    assert_eq!(div_roundup(5u8, 2u8), 3u8);
    assert_eq!(div_roundup(4u8, 2u8), 2u8);
    assert_eq!(div_roundup(4000u32, 1500u32), 3u32);
}
