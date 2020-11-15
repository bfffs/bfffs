// vim: tw=80

//! Data types used by trees representing filesystems

use bitfield::*;
use crate::{
    common::{
        *,
        dml::*,
        property::*,
        tree::*
    }
};
use divbuf::DivBufShared;
use futures::{Future, FutureExt, TryFutureExt, future};
use libc;
use metrohash::MetroHash64;
use serde::de::DeserializeOwned;
use std::{
    convert::TryFrom,
    ffi::{OsString, OsStr},
    hash::{Hash, Hasher},
    mem,
    ops::{Bound, Range, RangeBounds},
    os::unix::ffi::OsStrExt,
    pin::Pin,
    sync::Arc
};
use time::Timespec;

/// Buffers of this size or larger will be written to disk as blobs.  Smaller
/// buffers will be stored directly in the tree.
const BLOB_THRESHOLD: usize = BYTES_PER_LBA / 4;

// time::Timespec doesn't derive Serde support.  Do it here.
#[allow(unused)]
#[derive(Serialize, Deserialize)]
#[serde(remote = "Timespec")]
struct TimespecDef {
    sec: i64,
    nsec: i32
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum ExtAttrNamespace {
    User = libc::EXTATTR_NAMESPACE_USER as isize,
    System = libc::EXTATTR_NAMESPACE_SYSTEM as isize
}

/// Constants that discriminate different `ObjKey`s.  I don't know of a way to
/// do this within the definition of ObjKey itself.
enum ObjKeyDiscriminant {
    DirEntry = 0,
    Inode = 1,
    Extent = 2,
    ExtAttr = 3,
    Property = 4,
    DyingInode = 5,
}

/// The per-object portion of a `FSKey`
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum ObjKey {
    /// A directory entry.
    ///
    /// The value is a 56-bit hash of the entry's name.  This key is only valid
    /// if the object is a directory.
    DirEntry(u64),
    Inode,

    /// File extent
    ///
    /// The value is the extent's offset into its object, in bytes.  This key is
    /// only valid if the object is a file.
    Extent(u64),

    /// Extended attribute
    ///
    /// The first value is the 56-bit hash of the entry's name and namespace.
    ExtAttr(u64),

    /// A Dataset property.  Only relevant for object 0.
    Property(PropertyName),

    /// Inode number of an open-but-deleted inode.
    ///
    /// The value is a 56-bit hash of the inode number.  This key is only valid
    /// for object 0.
    DyingInode(u64),
}

impl ObjKey {
    /// Create a `ObjKey::DirEntry` object from a pathname
    pub fn dir_entry(name: &OsStr) -> Self {
        if name.as_bytes().contains(&(b'/')) {
            panic!("Directory entries may not contain '/'");
        }

        let mut hasher = MetroHash64::new();
        hasher.write(name.as_bytes());
        // TODO: use some salt to defend against DOS attacks
        let namehash = hasher.finish() & ( (1<<56) - 1);
        ObjKey::DirEntry(namehash)
    }

    pub fn dying_inode(ino: u64) -> Self {
        let mut hasher = MetroHash64::new();
        ino.hash(&mut hasher);
        // TODO: use some salt to defend against DOS attacks
        let inohash = hasher.finish() & ( (1<<56) - 1);
        ObjKey::DyingInode(inohash)
    }

    /// Create a 'ObjKey::ExtAttr' object from an extended attribute name
    pub fn extattr(namespace: ExtAttrNamespace, name: &OsStr) -> Self {
        let mut hasher = MetroHash64::new();
        namespace.hash(&mut hasher);
        hasher.write(name.as_bytes());
        // TODO: use some salt to defend against DOS attacks
        let namehash = hasher.finish() & ( (1<<56) - 1);
        ObjKey::ExtAttr(namehash)
    }

    fn discriminant(&self) -> u8 {
        let d = match self {
            ObjKey::DirEntry(_) => ObjKeyDiscriminant::DirEntry,
            ObjKey::Inode => ObjKeyDiscriminant::Inode,
            ObjKey::Extent(_) => ObjKeyDiscriminant::Extent,
            ObjKey::ExtAttr(_) => ObjKeyDiscriminant::ExtAttr,
            ObjKey::Property(_) => ObjKeyDiscriminant::Property,
            ObjKey::DyingInode(_) => ObjKeyDiscriminant::DyingInode,
        };
        d as u8
    }

    pub fn offset(&self) -> u64 {
        match self {
            ObjKey::DirEntry(x) => *x,
            ObjKey::Inode => 0,
            ObjKey::Extent(x) => *x,
            ObjKey::ExtAttr(x) => *x,
            ObjKey::Property(prop) => *prop as u64,
            ObjKey::DyingInode(x) => *x,
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
    fn compose(object: u64, objtype: u8, offset: u64) -> Self {
        FSKey((u128::from(object) << 64)
              | (u128::from(objtype) << 56)
              | u128::from(offset))
    }

    /// Create a range of `FSKey` that will include all the directory entries
    /// of directory `ino` beginning at `offset`
    pub fn dirent_range(ino: u64, offset: u64) -> Range<Self> {
        let objkey = ObjKey::DirEntry(0);
        let start = FSKey::compose(ino, objkey.discriminant(), offset);
        let end = FSKey::compose(ino, objkey.discriminant() + 1, 0);
        start..end
    }

    /// Create a range of `FSKey` that will include all dying inodes in this
    /// file system
    pub fn dying_inode_range() -> Range<Self> {
        let objkey = ObjKey::DyingInode(0);
        let start = FSKey::compose(0, objkey.discriminant(), 0);
        let end = FSKey::compose(0, objkey.discriminant() + 1, 0);
        start..end
    }

    /// Create a range of `FSKey` that will include all the extended attribute
    /// entries of file `ino`.
    pub fn extattr_range(ino: u64) -> Range<Self> {
        let objkey = ObjKey::ExtAttr(0);
        let start = FSKey::compose(ino, objkey.discriminant(), 0);
        let end = FSKey::compose(ino, objkey.discriminant() + 1, 0);
        start..end
    }

    /// Create a range of `FSKey` that will encompass all of a file's extents
    /// that whose beginnings lie in the range `offsets`
    pub fn extent_range<R>(ino: u64, offsets: R) -> Range<Self>
        where R: RangeBounds<u64>
    {
        let discriminant = ObjKeyDiscriminant::Extent as u8;
        let start = match offsets.start_bound() {
            Bound::Included(s) => {
                FSKey::compose(ino, discriminant, *s)
            },
            Bound::Unbounded => {
                FSKey::compose(ino, discriminant, 0)
            },
            _ => unimplemented!()   // LCOV_EXCL_LINE
        };
        let end = match offsets.end_bound() {
            Bound::Excluded(e) => {
                FSKey::compose(ino, discriminant, *e)
            },
            Bound::Unbounded => {
                FSKey::compose(ino, discriminant + 1, 0)
            },
            _ => unimplemented!()   // LCOV_EXCL_LINE
        };
        start..end
    }

    pub fn is_direntry(&self) -> bool {
        self.objtype() == ObjKeyDiscriminant::DirEntry as u8
    }

    pub fn is_dying_inode(&self) -> bool {
        self.objtype() == ObjKeyDiscriminant::DyingInode as u8
    }

    pub fn is_extattr(&self) -> bool {
        self.objtype() == ObjKeyDiscriminant::ExtAttr as u8
    }

    pub fn is_inode(&self) -> bool {
        self.objtype() == ObjKeyDiscriminant::Inode as u8
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
}

impl TypicalSize for FSKey {
    const TYPICAL_SIZE: usize = 16;
}

impl MinValue for FSKey {
    fn min_value() -> Self {
        FSKey(0)
    }
}

/// `FSValue`s that are stored as in in-BTree hash tables
pub trait HTItem: TryFrom<FSValue<RID>, Error=()> + Clone + Send + Sized + 'static {
    /// Some other type that may be used by the `same` method
    type Aux: Clone + Copy + Send;

    /// Error type that should be returned if the item can't be found
    const ENOTFOUND: Error;

    /// Retrieve this item's aux data.
    fn aux(&self) -> Self::Aux;

    /// Create an `HTItem` from an `FSValue` whose contents are unknown.  It
    /// could be a single item, a bucket, or even a totally different item type.
    fn from_table(fsvalue: Option<FSValue<RID>>) -> HTValue<Self>;

    /// Create a bucket of `HTItem`s from a vector of `Self`.  Used for putting
    /// the bucket back into the Tree.
    fn into_bucket(selves: Vec<Self>) -> FSValue<RID>;

    /// Turn self into a regular `FSValue`, suitable for inserting into the Tree
    fn into_fsvalue(self) -> FSValue<RID>;

    /// Do these values represent the same item, even if their values aren't
    /// identical?
    fn same<T: AsRef<OsStr>>(&self, aux: Self::Aux, name: T) -> bool;
}

/// Return type for
/// [`HTItem::from_table`](trait.HTItem.html#method.from_table)
pub enum HTValue<T: HTItem> {
    Single(T),
    Bucket(Vec<T>),
    Other(FSValue<RID>),
    None
}

/// In-memory representation of a directory entry
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Dirent {
    pub ino:    u64,
    pub dtype:  u8,
    pub name:   OsString
}

impl HTItem for Dirent {
    type Aux = u64;
    const ENOTFOUND: Error = Error::ENOENT;

    fn aux(&self) -> Self::Aux {
        self.ino
    }

    fn from_table(fsvalue: Option<FSValue<RID>>) -> HTValue<Self> {
        match fsvalue {
            Some(FSValue::DirEntry(x)) => HTValue::Single(x),
            Some(FSValue::DirEntries(x)) => HTValue::Bucket(x),
            Some(x) => HTValue::Other(x),
            None => HTValue::None
        }
    }   // LCOV_EXCL_LINE kcov false negative

    fn into_bucket(selves: Vec<Self>) -> FSValue<RID> {
        FSValue::DirEntries(selves)
    }

    fn into_fsvalue(self) -> FSValue<RID> {
        FSValue::DirEntry(self)
    }

    fn same<T: AsRef<OsStr>>(&self, _aux: Self::Aux, name: T) -> bool {
        // We generally don't know the desired ino when calling this method, so
        // just check the name
        self.name == name.as_ref()
    }   // LCOV_EXCL_LINE kcov false negative
}

impl TryFrom<FSValue<RID>> for Dirent {
    type Error = ();

    fn try_from(fsvalue: FSValue<RID>) -> Result<Self, ()> {
        if let FSValue::DirEntry(dirent) = fsvalue {
            Ok(dirent)
        } else {
            Err(())
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DyingInode(u64);

impl DyingInode {
    pub fn ino(&self) -> u64 {
        self.0
    }
}

impl From<u64> for DyingInode {
    fn from(ino: u64) -> Self {
        DyingInode(ino)
    }
}

/// In-memory representation of a small extended attribute
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InlineExtAttr {
    pub namespace: ExtAttrNamespace,
    pub name:   OsString,
    pub extent: InlineExtent
}

impl InlineExtAttr {
    fn flush<A: Addr, D>(self, dml: &D, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<ExtAttr<A>, Error>> + Send>>
        where D: DML, D::Addr: 'static
    {
        let lsize = self.len();
        if lsize > BLOB_THRESHOLD {
            let namespace = self.namespace;
            let name = self.name;
            let dbs = Arc::try_unwrap(self.extent.buf).unwrap();
            let fut = dml.put(dbs, Compression::None, txg)
            .map_ok(move |rid: D::Addr| {
                debug_assert_eq!(mem::size_of::<D::Addr>(),
                                 mem::size_of::<A>());
                // Safe because D::Addr should always equal A.  If you ever
                // call this function with any other type for A, then you're
                // doing something wrong.
                let rid_a = unsafe{*(&rid as *const D::Addr as *const A)};
                let extent = BlobExtent{lsize: lsize as u32, rid: rid_a};
                let bea = BlobExtAttr { namespace, name, extent };
                ExtAttr::Blob(bea)
            });
            fut.boxed()
        } else {
            future::ok(ExtAttr::Inline(self)).boxed()
        }
    }

    fn len(&self) -> usize {
        self.extent.len()
    }
}

/// In-memory representation of a large extended attribute
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned"))]
pub struct BlobExtAttr<A: Addr> {
    pub namespace: ExtAttrNamespace,
    pub name:   OsString,
    pub extent: BlobExtent<A>
}

impl<A: Addr> BlobExtAttr<A> {
    fn flush(self) -> ExtAttr<A>
    {
        ExtAttr::Blob(self)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned"))]
pub enum ExtAttr<A: Addr> {
    Inline(InlineExtAttr),
    Blob(BlobExtAttr<A>)
}

impl<'a, A: Addr> ExtAttr<A> {
    pub fn as_inline(&'a self) -> Option<&'a InlineExtAttr> {
        if let ExtAttr::Inline(x) = self {
            Some(x)
        } else {
            None
        }
    }

    fn flush<D>(self, dml: &D, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<ExtAttr<A>, Error>>
            + Send + 'static>>
        where D: DML + 'static, D::Addr: 'static
    {
        match self {
            ExtAttr::Inline(iea) => iea.flush(dml, txg),
            ExtAttr::Blob(bea) => future::ok(bea.flush()).boxed()
        }
    }

    pub fn name(&'a self) -> &'a OsStr {
        match self {
            ExtAttr::Inline(x) => &x.name,
            ExtAttr::Blob(x) => &x.name,
        }
    }

    pub fn namespace(&self) -> ExtAttrNamespace {
        match self {
            ExtAttr::Inline(x) => x.namespace,
            ExtAttr::Blob(x) => x.namespace,
        }
    }
}

impl HTItem for ExtAttr<RID> {
    type Aux = ExtAttrNamespace;
    const ENOTFOUND: Error = Error::ENOATTR;

    fn aux(&self) -> Self::Aux {
        self.namespace()
    }

    fn from_table(fsvalue: Option<FSValue<RID>>) -> HTValue<Self> {
        match fsvalue {
            Some(FSValue::ExtAttr(x)) => HTValue::Single(x),
            Some(FSValue::ExtAttrs(x)) => HTValue::Bucket(x),
            Some(x) => HTValue::Other(x),
            None => HTValue::None
        }
    }   // LCOV_EXCL_LINE kcov false negative

    fn into_bucket(selves: Vec<Self>) -> FSValue<RID> {
        FSValue::ExtAttrs(selves)
    }

    fn into_fsvalue(self) -> FSValue<RID> {
        FSValue::ExtAttr(self)
    }

    fn same<T: AsRef<OsStr>>(&self, aux: Self::Aux, name: T) -> bool {
        self.namespace() == aux && self.name() == name.as_ref()
    }
}

impl TryFrom<FSValue<RID>> for ExtAttr<RID> {
    type Error = ();

    fn try_from(fsvalue: FSValue<RID>) -> Result<Self, ()> {
        if let FSValue::ExtAttr(extent) = fsvalue {
            Ok(extent)
        } else {
            Err(())
        }
    }
}

/// Field of an `Inode`
// Keep these fields in order!  This is the same order as the OS uses.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FileType {
    /// Fifo
    Fifo,
    /// Character device node
    Char(libc::dev_t),
    /// Directory
    Dir,
    /// Block device node
    Block(libc::dev_t),
    /// Regular file
    ///
    /// The value is the record size in bytes, log base 2.  Once the file is
    /// created, this cannot be changed without rewriting all the file's
    /// records.
    Reg(u8),
    /// Symlink, with its destination
    Link(OsString),
    /// Socket
    Socket,
}

impl FileType {
    /// Return the dtype, as used in directory entries, for this `FileType`
    pub fn dtype(&self) -> u8 {
        match self {
            FileType::Fifo => libc::DT_FIFO,
            FileType::Char(_) => libc::DT_CHR,
            FileType::Dir => libc::DT_DIR,
            FileType::Block(_) => libc::DT_BLK,
            FileType::Reg(_) => libc::DT_REG,
            FileType::Link(_) => libc::DT_LNK,
            FileType::Socket => libc::DT_SOCK,
        }
    }   // LCOV_EXCL_LINE kcov false negative

    /// The file type part of the mode, as returned by stat(2)
    pub fn mode(&self) -> u16 {
        match self {
            FileType::Fifo => libc::S_IFIFO,
            FileType::Char(_) => libc::S_IFCHR,
            FileType::Dir => libc::S_IFDIR,
            FileType::Block(_) => libc::S_IFBLK,
            FileType::Reg(_) => libc::S_IFREG,
            FileType::Link(_) => libc::S_IFLNK,
            FileType::Socket => libc::S_IFSOCK,
        }
    }   // LCOV_EXCL_LINE kcov false negative
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
    /// File permissions, the low twelve bits of mode
    pub perm:       u16,
    /// File type.  Regular, directory, etc
    pub file_type:   FileType
}

impl Inode {
    /// This file's record size in bytes
    pub fn record_size(&self) -> usize {
        if let FileType::Reg(exp) = self.file_type {
            1 << exp
        } else {
            panic!("Only regular files have record sizes")
        }
    }
}

/// This module ought to be unreachable, but must exist to satisfy rustc
mod dbs_serializer {
    use super::*;
    use serde::{de::Deserializer, Serializer};

    pub(super) fn deserialize<'de, DE>(deserializer: DE)
        -> Result<Arc<DivBufShared>, DE::Error>
        where DE: Deserializer<'de>
    {
        Vec::<u8>::deserialize(deserializer)
            .map(|v| Arc::new(DivBufShared::from(v)))
    }

    pub(super) fn serialize<S>(dbs: &Arc<DivBufShared>, serializer: S)
        -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        (&dbs.try_const().unwrap()[..]).serialize(serializer)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InlineExtent {
    #[serde(with = "dbs_serializer")]
    // The Arc is necessary to make it Clone.
    pub buf: Arc<DivBufShared>
}

impl InlineExtent {
    fn flush<A: Addr, D>(self, dml: &D, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<FSValue<A>, Error>>
            + Send + 'static>>
        where D: DML, D::Addr: 'static
    {
        let lsize = self.len();
        if lsize > BLOB_THRESHOLD {
            let dbs = Arc::try_unwrap(self.buf).unwrap();
            let fut = dml.put(dbs, Compression::None, txg)
            .map_ok(move |rid: D::Addr| {
                debug_assert_eq!(mem::size_of::<D::Addr>(),
                                 mem::size_of::<A>());
                // Safe because D::Addr should always equal A.  If you ever
                // call this function with any other type for A, then you're
                // doing something wrong.
                let rid_a = unsafe{*(&rid as *const D::Addr as *const A)};
                let be = BlobExtent{lsize: lsize as u32, rid: rid_a};
                FSValue::BlobExtent(be)
            });
            fut.boxed()
        } else {
            future::ok(FSValue::InlineExtent(self)).boxed()
        }
    }

    fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn new(buf: Arc<DivBufShared>) -> Self {
        InlineExtent{buf}
    }
}

// Useful for the fuse unit tests
impl Default for InlineExtent {
    fn default() -> Self {
        InlineExtent { buf: Arc::new(DivBufShared::with_capacity(0)) }
    }
}

impl PartialEq for InlineExtent {
    fn eq(self: &Self, other: &Self) -> bool {
        self.buf.try_const().unwrap() == other.buf.try_const().unwrap()
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
    /// An Extended Attribute, for inodes >= 1.  Or a dataset User Property, for
    /// inode 0.
    ExtAttr(ExtAttr<A>),
    /// A whole Bucket of `ExtAttr`s, used in case of hash collisions
    ExtAttrs(Vec<ExtAttr<A>>),
    /// A whole Bucket of `Dirent`s, used in case of hash collisions
    DirEntries(Vec<Dirent>),
    /// A native dataset property.
    Property(Property),
    /// Inode number of an open-but-deleted file in this file
    /// system.  Only valid for object 0.
    DyingInode(DyingInode),
    // TODO: hash bucket of DyingInode
    /// For testing purposes only!  Must come last!
    #[cfg(test)]
    #[doc(hidden)]
    Invalid,
}

impl<A: Addr> FSValue<A> {
    pub fn as_direntries(&self) -> Option<&Vec<Dirent>> {
        if let FSValue::DirEntries(direntries) = self {
            Some(direntries)
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

    pub fn as_dying_inode(&self) -> Option<&DyingInode> {
        if let FSValue::DyingInode(di) = self {
            Some(di)
        } else {
            None
        }
    }

    pub fn as_extattr(&self) -> Option<&ExtAttr<A>> {
        if let FSValue::ExtAttr(extent) = self {
            Some(extent)
        } else {
            None
        }
    }

// LCOV_EXCL_START
    #[cfg(test)]
    pub fn as_extattrs(&self) -> Option<&Vec<ExtAttr<A>>> {
        if let FSValue::ExtAttrs(extents) = self {
            Some(extents)
        } else {
            None
        }
    }
// LCOV_EXCL_END

    pub fn as_extent(&self) -> Option<Extent<A>> {
        if let FSValue::InlineExtent(extent) = self {
            Some(Extent::Inline(extent))
        } else if let FSValue::BlobExtent(extent) = self {
            Some(Extent::Blob(extent))
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

#[cfg(test)]
impl Default for FSValue<RID> {
    fn default() -> Self {
        FSValue::Invalid
    }
}

impl<A: Addr> TypicalSize for FSValue<A> {
    // FSValue can have variable size.  But the most common variant is likely to
    // be FSValue::BlobExtent, which has size 16.
    const TYPICAL_SIZE: usize = 16;
}

impl<A: Addr> Value for FSValue<A> {
    fn flush<D>(self, dml: &D, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Self, Error>> + Send + 'static>>
        where D: DML + 'static, D::Addr: 'static
    {
        match self {
            FSValue::InlineExtent(ie) => ie.flush(dml, txg),
            FSValue::ExtAttr(extattr) => {
                let fut = extattr.flush(dml, txg)
                .map_ok(FSValue::ExtAttr);
                Box::pin(fut)
            }
            FSValue::ExtAttrs(v) => {
                let fut = future::try_join_all(
                    v.into_iter().map(|extattr| {
                        extattr.flush(dml, txg)
                    }).collect::<Vec<_>>()
                ).map_ok(FSValue::ExtAttrs);
                fut.boxed()
            },
            _ => future::ok(self).boxed()
        }
    }

    fn needs_flush() -> bool {
        true
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {

use crate::common::idml::IDML;
use pretty_assertions::assert_eq;
use super::*;

// pet kcov
#[test]
fn debug() {
    assert_eq!("DirEntry(0)", format!("{:?}", ObjKey::DirEntry(0)));
    assert_eq!("Extent(0)", format!("{:?}", ObjKey::Extent(0)));
    assert_eq!("ExtAttr(0)", format!("{:?}", ObjKey::ExtAttr(0)));
    assert_eq!("Property(Atime)",
        format!("{:?}", ObjKey::Property(PropertyName::Atime)));
}

#[test]
fn fskey_typical_size() {
    let ok = ObjKey::Extent(0);
    let fsk = FSKey::new(0, ok);
    let v: Vec<u8> = bincode::serialize(&fsk).unwrap();
    assert_eq!(v.len(), FSKey::TYPICAL_SIZE);
}

#[test]
fn fsvalue_typical_size() {
    let fsv: FSValue<RID> = FSValue::BlobExtent(BlobExtent{
        lsize: 0xdead_beef,
        rid: RID(0x0001_0203_0405_0607)
    });
    let v: Vec<u8> = bincode::serialize(&fsv).unwrap();
    assert_eq!(FSValue::<RID>::TYPICAL_SIZE, v.len());
}

/// Long InlineExtAttrs should be converted to BlobExtAttrs during flush
#[test]
fn fsvalue_flush_inline_extattr_long() {
    let rid = RID(999);
    let mut idml = IDML::default();
    idml.expect_put()
        .once()
        .withf(|cacheable: &DivBufShared, _compression, _txg| {
            cacheable.len() == BYTES_PER_LBA
        }).returning(move |_, _, _| future::ok(rid).boxed());
    let txg = TxgT(0);

    let namespace = ExtAttrNamespace::User;
    let name = OsString::from("bar");
    let dbs = Arc::new(DivBufShared::from(vec![42u8; BYTES_PER_LBA]));
    let extent = InlineExtent::new(dbs);
    let iea = InlineExtAttr{namespace, name, extent};
    let unflushed: FSValue<RID> = FSValue::ExtAttr(ExtAttr::Inline(iea));

    let flushed = unflushed.flush(&idml, txg)
        .now_or_never().unwrap()
        .unwrap();

    if let ExtAttr::Inline(_) = flushed.as_extattr().unwrap() {
        panic!("Long extattr should've become a BlobExtattr");
    }
}

/// Short InlineExtAttrs should be left in the B+Tree, not turned into blobs
#[test]
fn fsvalue_flush_inline_extattr_short() {
    let idml = IDML::default();
    let txg = TxgT(0);

    let namespace = ExtAttrNamespace::User;
    let name = OsString::from("bar");
    let dbs = Arc::new(DivBufShared::from(vec![0, 1, 2, 3, 4, 5]));
    let extent = InlineExtent::new(dbs);
    let iea = InlineExtAttr{namespace, name, extent};
    let unflushed: FSValue<RID> = FSValue::ExtAttr(ExtAttr::Inline(iea));

    let flushed = unflushed.flush(&idml, txg)
        .now_or_never().unwrap()
        .unwrap();

    if let ExtAttr::Blob(_) = flushed.as_extattr().unwrap() {
        panic!("Short extattr should remain inline");
    }
}

/// Long InlineExtents should be converted to BlobExtents during flush
#[test]
fn fsvalue_flush_inline_extent_long() {
    let rid = RID(999);
    let mut idml = IDML::default();
    idml.expect_put()
        .once()
        .withf(|cacheable: &DivBufShared, _compression, _txg| {
            cacheable.len() == BYTES_PER_LBA
        }).returning(move |_, _, _| future::ok(rid).boxed());
    let txg = TxgT(0);

    let data = Arc::new(DivBufShared::from(vec![42u8; BYTES_PER_LBA]));
    let ile = InlineExtent::new(data);
    let unflushed: FSValue<RID> = FSValue::InlineExtent(ile);
    let flushed = unflushed.flush(&idml, txg)
        .now_or_never().unwrap()
        .unwrap();

    if let Extent::Inline(_) = flushed.as_extent().unwrap() {
        panic!("Long extent should've become a BlobExtent");
    }
}

/// Short InlineExtents should be left in the B+Tree, not turned into blobs
#[test]
fn fsvalue_flush_inline_extent_short() {
    let idml = IDML::default();
    let txg = TxgT(0);

    let data = Arc::new(DivBufShared::from(vec![0, 1, 2, 3, 4, 5]));
    let ile = InlineExtent::new(data);
    let unflushed: FSValue<RID> = FSValue::InlineExtent(ile);
    let flushed = unflushed.flush(&idml, txg)
        .now_or_never().unwrap()
        .unwrap();

    if let Extent::Blob(_) = flushed.as_extent().unwrap() {
        panic!("Short extent should remain inline");
    }
}

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
// LCOV_EXCL_END
