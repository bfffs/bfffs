// vim: tw=80

use bytes::{Bytes, BytesMut};

pub mod declust;
pub mod dva;
pub mod prime_s;
pub mod raid;
pub mod vdev;
pub mod vdev_block;
pub mod vdev_leaf;
pub mod vdev_raid;
pub type LbaT = u64;
pub type ZoneT = u32;
/// Our IoVec.  Unlike the standard library's, ours is reference-counted so it
/// can have more than one owner.
pub type IoVec = Bytes;
/// Mutable version of `IoVec`.  Uniquely owned.
pub type IoVecMut = BytesMut;
/// Our scatter-gather list.  A slice of reference-counted `IoVec`s.
pub type SGList = Vec<IoVec>;
/// Mutable version of `SGList`.  Uniquely owned.
pub type SGListMut = Vec<IoVecMut>;
