// vim: tw=80

//! Data types used by trees representing filesystems

use common::{
    *,
    dml::*,
    tree::*
};
use divbuf::DivBufShared;
use futures::{Future, IntoFuture};
use metrohash::MetroHash64;
use serde::de::DeserializeOwned;
use std::{
    ffi::{OsString, OsStr},
    hash::Hasher,
    mem,
    ops::Range,
    os::unix::ffi::OsStrExt,
    sync::Arc
};
use time::Timespec;

// time::Timespec doesn't derive Serde support.  Do it here.
#[allow(unused)]
#[derive(Serialize, Deserialize)]
#[serde(remote = "Timespec")]
struct TimespecDef {
    sec: i64,
    nsec: i32
}

/// Constants that discriminate different `ObjKey`s.  I don't know of a way to
/// do this within the definition of ObjKey itself.
enum ObjKeyDiscriminant {
    DirEntry = 0,
    Inode = 1,
    Extent = 2,
}

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
    Inode,
    /// File extent
    ///
    /// The value is the logical size of the extent, in bytes.  This key is only
    /// valid if the object is a file or an extended attribute.
    Extent(u64)
}

impl ObjKey {
    /// Create a `ObjKey::DirEntry` object from a pathname
    pub fn dir_entry(name: &OsStr) -> Self {
        if name.as_bytes().contains(&('/' as u8)) {
            panic!("Directory entries may not contain '/'");
        }

        let mut hasher = MetroHash64::new();
        hasher.write(name.as_bytes());
        // TODO: use cuckoo hashing to deal with collisions
        // TODO: use some salt to defend against DOS attacks
        let namehash = hasher.finish() & ( (1<<56) - 1);
        ObjKey::DirEntry(namehash)
    }

    fn discriminant(&self) -> u8 {
        let d = match self {
            ObjKey::DirEntry(_) => ObjKeyDiscriminant::DirEntry,
            ObjKey::Inode => ObjKeyDiscriminant::Inode,
            ObjKey::Extent(_) => ObjKeyDiscriminant::Extent
        };
        d as u8
    }

    fn offset(&self) -> u64 {
        match self {
            ObjKey::DirEntry(x) => *x,
            ObjKey::Inode => 0,
            ObjKey::Extent(x) => *x,
        }
    }
}

bitfield! {
    /// B-Tree keys for a Filesystem tree
    #[derive(Clone, Copy, Deserialize, Eq, PartialEq, PartialOrd, Ord,
             Serialize)]
    pub struct FSKey(u128);
    impl Debug;
    u64; pub object, _: 127, 64;
    u8; pub objtype, _: 63, 56;
    u64; pub offset, _: 55, 0;
}

impl FSKey {
    /// Create a range of `FSKey` that will include all the directory entries
    /// of directory `ino` beginning at `offset`
    pub fn dirent_range(ino: u64, offset: u64) -> Range<Self> {
        let objkey = ObjKey::DirEntry(0);
        let start = FSKey::compose(ino, objkey.discriminant(), offset);
        let end = FSKey::compose(ino, objkey.discriminant() + 1, 0);
        Range{start, end}
    }

    pub fn extent_range(ino: u64) -> Range<Self> {
        let start = FSKey::compose(ino, ObjKeyDiscriminant::Extent as u8, 0);
        let end = FSKey::compose(ino, ObjKeyDiscriminant::Extent as u8 + 1, 0);
        start..end
    }

    pub fn new(object: u64, objkey: ObjKey) -> Self {
        let objtype = objkey.discriminant();
        let offset = objkey.offset();
        FSKey::compose(object, objtype, offset)
    }

    /// Create a range of `FSKey` that will include every item related to the
    /// given object.
    pub fn obj_range(ino: u64) -> Range<Self> {
        let start = FSKey::compose(ino, 0, 0);
        let end = FSKey::compose(ino + 1, 0, 0);
        start..end
    }

    fn compose(object: u64, objtype: u8, offset: u64) -> Self {
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
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Dirent {
    pub ino:    u64,
    pub dtype:  u8,
    pub name:   OsString
}

/// In-memory representation of an Inode
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Inode {
    /// File size in bytes
    pub size:       u64,
    /// Link count
    pub nlink:      u64,
    /// File flags
    pub flags:      u64,
    /// access time
    #[serde(with = "TimespecDef")]
    pub atime:      Timespec,
    /// modification time
    #[serde(with = "TimespecDef")]
    pub mtime:      Timespec,
    /// change time
    #[serde(with = "TimespecDef")]
    pub ctime:      Timespec,
    /// birth time
    #[serde(with = "TimespecDef")]
    pub birthtime:  Timespec,
    /// user id
    pub uid:        u32,
    /// Group id
    pub gid:        u32,
    /// File mode
    pub mode:       u16,
}

mod dbs_serializer {
    use super::*;
    use serde::{de::Deserializer, Serializer};

    pub(super) fn deserialize<'de, DE>(_deserializer: DE)
        -> Result<Arc<DivBufShared>, DE::Error>
        where DE: Deserializer<'de>
    {
        panic!("InlineExtents should be converted to BlobExtents before serializing")
    }

    pub(super) fn serialize<S>(_dbs: &Arc<DivBufShared>, _serializer: S)
        -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        panic!("InlineExtents should be converted to BlobExtents before serializing")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InlineExtent {
    #[serde(with = "dbs_serializer")]
    // The Arc is necessary to make it Clone.
    pub buf: Arc<DivBufShared>
}

impl InlineExtent {
    pub fn new(buf: Arc<DivBufShared>) -> Self {
        InlineExtent{buf}
    }
}

impl PartialEq for InlineExtent {
    fn eq(self: &Self, other: &Self) -> bool {
        self.buf.try().unwrap() == other.buf.try().unwrap()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned"))]
pub struct BlobExtent<A: Addr> {
    pub lsize: u32,
    pub rid: A,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Extent<'a, A: Addr> {
    Inline(&'a InlineExtent),
    Blob(&'a BlobExtent<A>)
}

// This struct isn't really generic.  It should only ever be instantiated with
// A=RID.  However, it's not possible to implement FSValue::flush without either
// making FSValue generic, or using generics specialization.  And generics
// specialization isn't stable yet.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned"))]
pub enum FSValue<A: Addr> {
    DirEntry(Dirent),
    Inode(Inode),
    InlineExtent(InlineExtent),
    BlobExtent(BlobExtent<A>),
    None
}

impl<A: Addr> FSValue<A> {
    pub fn as_extent(&self) -> Option<Extent<A>> {
        if let FSValue::InlineExtent(extent) = self {
            Some(Extent::Inline(extent))
        } else if let FSValue::BlobExtent(extent) = self {
            Some(Extent::Blob(extent))
        } else {
            None
        }
    }

    pub fn as_direntry(&self) -> Option<&Dirent> {
        if let FSValue::DirEntry(direntry) = self {
            Some(direntry)
        } else {
            None
        }
    }

    pub fn as_inode(&self) -> Option<&Inode> {
        if let FSValue::Inode(inode) = self {
            Some(inode)
        } else {
            None
        }
    }

    pub fn as_mut_inode(&mut self) -> Option<&mut Inode> {
        if let FSValue::Inode(ref mut inode) = self {
            Some(inode)
        } else {
            None
        }
    }
}

impl<A: Addr> Value for FSValue<A> {
    fn flush<D>(self, dml: &D, txg: TxgT)
        -> Box<Future<Item=Self, Error=Error> + Send>
        where D: DML, D::Addr: 'static
    {
        if let FSValue::InlineExtent(ie) = self {
            let lsize = ie.buf.len() as u32;
            let dbs = Arc::try_unwrap(ie.buf).unwrap();
            Box::new(dml.put(dbs, Compression::None, txg)
                .map(move |rid: D::Addr| {
                    debug_assert_eq!(mem::size_of::<D::Addr>(),
                                     mem::size_of::<A>());
                    // Safe because D::Addr should always equal A.  If you ever
                    // call this function with any other type for A, then you're
                    // doing something wrong.
                    let rid_a = unsafe{*(&rid as *const D::Addr as *const A)};
                    let be = BlobExtent{lsize, rid: rid_a};
                    FSValue::BlobExtent(be)
                })) as Box<Future<Item=Self, Error=Error> + Send>
        } else {
            Box::new(Ok(self).into_future())
                as Box<Future<Item=Self, Error=Error> + Send>
        }
    }

    fn needs_flush() -> bool {
        true
    }
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
    let a = FSKey::compose(0, 0, 0);
    let b = FSKey::compose(0, 1, 0);
    let c = FSKey::compose(1, 0, 0);
    let d = FSKey::compose(0, 1, 1);
    let e = FSKey::compose(0, 2, 0);
    let f = FSKey::compose(0, 2, 1);
    let g = FSKey::compose(0, 2, 2);
    assert!(a < b && b < c);
    assert!(b < d && d < e);
    assert!(e < f && f < g);
}

}
