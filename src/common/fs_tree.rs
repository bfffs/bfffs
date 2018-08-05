// vim: tw=80

//! Data types used by trees representing filesystems

use common::tree::*;
use std::ffi::OsString;

/// The per-object portion of a `FSKey`
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum ObjKey {
    /// A directory entry.
    ///
    /// The value is a 56-bit hash of the entry's name.  This key is only valid
    /// if the object is a directory or file.  If the latter, then the DirEntry
    /// refers to one of the file's extended attributes.
    DirEntry(u64),
    Inode(),
}

bitfield! {
    /// B-Tree keys for a Filesystem tree
    #[derive(Clone, Copy, Deserialize, Eq, PartialEq, PartialOrd, Ord,
             Serialize)]
    pub struct FSKey(u128);
    impl Debug;
    u64;
    pub object, _: 127, 64;
    pub objtype, _: 63, 56;
    pub offset, _: 55, 0;
}

impl FSKey {
    pub fn new(object: u64, objtype: u8, offset: u64) -> Self {
        FSKey(((object as u128) << 64)
              | ((objtype as u128) << 56)
              | (offset as u128))
    }
}

impl MinValue for FSKey {
    fn min_value() -> Self {
        FSKey(0)
    }
}

/// In-memory representation of a directory entry
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Dirent {
    pub ino:    u64,
    pub dtype:  u8,
    pub name:   OsString
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FSValue {
    DirEntry(Dirent),
    Inode(),
}

#[cfg(test)]
mod t {
use super::*;

/// For best locality of reference, keys of the same object should always
/// sort together, keys of the same object and objtype should always sort
/// together, and keys of the same object and objtype should sort according
/// to their offsets
#[test]
fn sort_fskey() {
    let a = FSKey::new(0, 0, 0);
    let b = FSKey::new(0, 1, 0);
    let c = FSKey::new(1, 0, 0);
    let d = FSKey::new(0, 1, 1);
    let e = FSKey::new(0, 2, 0);
    let f = FSKey::new(0, 2, 1);
    let g = FSKey::new(0, 2, 2);
    assert!(a < b && b < c);
    assert!(b < d && d < e);
    assert!(e < f && f < g);
}

}
