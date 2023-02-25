// vim: tw=80

//! Data types used by trees representing filesystems

use bitfield::bitfield;
use crate::{
    dml::{Compression, DML},
    property::*,
    tree::{Key, MinValue, Value},
    types::*,
    util::*
};
use divbuf::{DivBufShared, DivBuf};
use futures::{
    Future,
    FutureExt,
    TryFutureExt,
    TryStreamExt,
    future,
    stream::{FuturesOrdered, FuturesUnordered},
    task::{Context, Poll}
};
use metrohash::MetroHash64;
use num_enum::{IntoPrimitive, FromPrimitive};
use pin_project::pin_project;
use serde_derive::{Deserialize, Serialize};
use serde::ser::{Serialize, Serializer, SerializeStruct};
use std::{
    any::Any,
    collections::BTreeMap,
    convert::TryFrom,
    ffi::{OsString, OsStr},
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    mem,
    ops::{Bound, Range, RangeBounds},
    os::unix::ffi::OsStrExt,
    pin::Pin,
    sync::Arc
};

/// Buffers of this size or larger will be written to disk as blobs.  Smaller
/// buffers will be stored directly in the tree.
const BLOB_THRESHOLD: usize = BYTES_PER_LBA / 4;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum ExtAttrNamespace {
    User = libc::EXTATTR_NAMESPACE_USER as isize,
    System = libc::EXTATTR_NAMESPACE_SYSTEM as isize
}

/// Constants that discriminate different `ObjKey`s.  I don't know of a way to
/// do this within the definition of ObjKey itself.
#[derive(Debug, IntoPrimitive, FromPrimitive)]
#[repr(u8)]
enum ObjKeyDiscriminant {
    DirEntry = 0,
    Inode = 1,
    Extent = 2,
    ExtAttr = 3,
    Property = 4,
    DyingInode = 5,
    #[num_enum(default)]
    Unknown = 255
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
        d.into()
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
    #[derive(Clone, Copy, Deserialize, Eq, PartialEq, PartialOrd, Ord)]
    pub struct FSKey(u128);
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
        let discriminant = ObjKeyDiscriminant::Extent.into();
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
        self.objtype() == u8::from(ObjKeyDiscriminant::DirEntry)
    }

    pub fn is_dying_inode(&self) -> bool {
        self.objtype() == u8::from(ObjKeyDiscriminant::DyingInode)
    }

    pub fn is_extattr(&self) -> bool {
        self.objtype() == u8::from(ObjKeyDiscriminant::ExtAttr)
    }

    pub fn is_inode(&self) -> bool {
        self.objtype() == u8::from(ObjKeyDiscriminant::Inode)
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

impl Debug for FSKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let objtype = ObjKeyDiscriminant::from(self.objtype());
        write!(f, "FSKey {{ object: {:#x}, objtype: {:?}, offset: {:#x} }}",
               self.object(), objtype, self.offset())
    }
}

impl Key for FSKey {}

impl TypicalSize for FSKey {
    const TYPICAL_SIZE: usize = 16;
}

impl MinValue for FSKey {
    fn min_value() -> Self {
        FSKey(0)
    }
}

impl Serialize for FSKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where S: Serializer
    {
        if serializer.is_human_readable() {
            let object = self.0 >> 64;
            let objtype = (self.0 >> 56) & 0xFF;
            let offset = self.0 & 0x00FFFFFFFFFFFFFF;
            format!("{object:x}-{objtype}-{offset:014x}")
                .serialize(serializer)
        } else {
            self.0.serialize(serializer)
        }
    }
}

/// `FSValue`s that are stored in in-BTree hash tables
pub trait HTItem: TryFrom<FSValue, Error=()> + Clone + Send + Sized + 'static {
    /// Some other type that may be used by the `same` method
    type Aux: Clone + Copy + Send;

    /// Error type that should be returned if the item can't be found
    const ENOTFOUND: Error;

    /// Retrieve this item's aux data.
    fn aux(&self) -> Self::Aux;

    /// Create an `HTItem` from an `FSValue` whose contents are unknown.  It
    /// could be a single item, a bucket, or even a totally different item type.
    fn from_table(fsvalue: Option<FSValue>) -> HTValue<Self>;

    /// Create a bucket of `HTItem`s from a vector of `Self`.  Used for putting
    /// the bucket back into the Tree.
    #[allow(clippy::wrong_self_convention)]
    fn into_bucket(selves: Vec<Self>) -> FSValue;

    /// Turn self into a regular `FSValue`, suitable for inserting into the Tree
    fn into_fsvalue(self) -> FSValue;

    /// Do these values represent the same item, even if their values aren't
    /// identical?
    fn same<T: AsRef<OsStr>>(&self, aux: Self::Aux, name: T) -> bool;
}

/// Return type for
/// [`HTItem::from_table`](trait.HTItem.html#method.from_table)
pub enum HTValue<T: HTItem> {
    Single(T),
    Bucket(Vec<T>),
    Other(FSValue),
    None
}

fn serialize_dirent_name<S>(name: &OsString, s: S)
    -> std::result::Result<S::Ok, S::Error>
    where S: Serializer
{
    if s.is_human_readable() {
        // When dumping to YAML, print the name as a legible string
        name.to_string_lossy().serialize(s)
    } else {
        // but for Bincode, use the default representation as bytes
        name.serialize(s)
    }
}

/// In-memory representation of a directory entry
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Dirent {
    pub ino:    u64,
    // TODO: serialize as a string when dumping to YAML
    pub dtype:  u8,
    #[serde(serialize_with = "serialize_dirent_name")]
    pub name:   OsString
}

impl Dirent {
    pub fn allocated_space(&self) -> usize {
        self.name.len()
    }
}

impl HTItem for Dirent {
    type Aux = u64;
    const ENOTFOUND: Error = Error::ENOENT;

    fn aux(&self) -> Self::Aux {
        self.ino
    }

    fn from_table(fsvalue: Option<FSValue>) -> HTValue<Self> {
        match fsvalue {
            Some(FSValue::DirEntry(x)) => HTValue::Single(x),
            Some(FSValue::DirEntries(x)) => HTValue::Bucket(x),
            Some(x) => HTValue::Other(x),
            None => HTValue::None
        }
    }

    fn into_bucket(selves: Vec<Self>) -> FSValue {
        FSValue::DirEntries(selves)
    }

    fn into_fsvalue(self) -> FSValue {
        FSValue::DirEntry(self)
    }

    fn same<T: AsRef<OsStr>>(&self, _aux: Self::Aux, name: T) -> bool {
        // We generally don't know the desired ino when calling this method, so
        // just check the name
        self.name == name.as_ref()
    }
}

impl TryFrom<FSValue> for Dirent {
    type Error = ();

    fn try_from(fsvalue: FSValue) -> std::result::Result<Self, ()> {
        if let FSValue::DirEntry(dirent) = fsvalue {
            Ok(dirent)
        } else {
            Err(())
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InlineExtAttr {
    pub namespace: ExtAttrNamespace,
    pub name:   OsString,
    pub extent: InlineExtent
}

impl InlineExtAttr {
    fn flush<D, K>(self, k: K, dml: &D, txg: TxgT)
        -> impl Future<Output=Result<(K, FSValue)>> + Send
        where D: DML, D::Addr: 'static, K: Key
    {
        let lsize = self.len();
        debug_assert!(lsize > BLOB_THRESHOLD);
        let namespace = self.namespace;
        let name = self.name;
        let dbs = Arc::try_unwrap(self.extent.buf).unwrap();
        dml.put(dbs, Compression::None, txg)
        .map_ok(move |rid: D::Addr| {
            let rid_a = checked_transmute(rid);
            let extent = BlobExtent{lsize: lsize as u32, rid: rid_a};
            let bea = BlobExtAttr { namespace, name, extent };
            (k, FSValue::ExtAttr(ExtAttr::Blob(bea)))
        })
    }

    fn len(&self) -> usize {
        self.extent.len()
    }

    pub fn needs_flush(&self) -> bool {
        self.len() > BLOB_THRESHOLD
    }
}

/// In-memory representation of a large extended attribute
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlobExtAttr {
    pub namespace: ExtAttrNamespace,
    pub name:   OsString,
    pub extent: BlobExtent
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ExtAttr {
    Inline(InlineExtAttr),
    Blob(BlobExtAttr)
}

impl<'a> ExtAttr {
    pub fn allocated_space(&self) -> usize {
        const FUDGE: usize = 64;    // Experimentally determined

        match self {
            ExtAttr::Blob(blob_extattr) => blob_extattr.name.len(),
            ExtAttr::Inline(inline_extattr) =>
                inline_extattr.name.len() +
                inline_extattr.extent.len() +
                FUDGE
        }
    }

    pub fn as_inline(&'a self) -> Option<&'a InlineExtAttr> {
        if let ExtAttr::Inline(x) = self {
            Some(x)
        } else {
            None
        }
    }

    fn dpop<D>(self, dml: &D, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Self>>
            + Send + 'static>>
        where D: DML + 'static, D::Addr: 'static
    {
        match self {
            ExtAttr::Blob(bea) => {
                let rid = checked_transmute(bea.extent.rid);
                dml.pop::<DivBufShared, DivBuf>(&rid, txg)
                .map_ok(move |dbs|
                    ExtAttr::Inline(InlineExtAttr {
                        namespace: bea.namespace,
                        name: bea.name,
                        extent: InlineExtent::new(Arc::new(*dbs))
                    })
                ).boxed()
            }
            iea => future::ok(iea).boxed(),
        }
    }

    fn flush<D, K>(self, k: K, dml: &D, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<(K, FSValue)>>
            + Send + 'static>>
        where D: DML + 'static, D::Addr: 'static, K: Key
    {
        match self {
            ExtAttr::Inline(iea) => iea.flush(k, dml, txg).boxed(),
            ExtAttr::Blob(bea) => future::ok(
                (k, FSValue::ExtAttr(ExtAttr::Blob(bea)))
            ).boxed()
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

    pub fn needs_flush(&self) -> bool {
        match self {
            ExtAttr::Inline(iea) => iea.needs_flush(),
            ExtAttr::Blob(_) => false
        }
    }
}

impl HTItem for ExtAttr {
    type Aux = ExtAttrNamespace;
    const ENOTFOUND: Error = Error::ENOATTR;

    fn aux(&self) -> Self::Aux {
        self.namespace()
    }

    fn from_table(fsvalue: Option<FSValue>) -> HTValue<Self> {
        match fsvalue {
            Some(FSValue::ExtAttr(x)) => HTValue::Single(x),
            Some(FSValue::ExtAttrs(x)) => HTValue::Bucket(x),
            Some(x) => HTValue::Other(x),
            None => HTValue::None
        }
    }

    fn into_bucket(selves: Vec<Self>) -> FSValue {
        FSValue::ExtAttrs(selves)
    }

    fn into_fsvalue(self) -> FSValue {
        FSValue::ExtAttr(self)
    }

    fn same<T: AsRef<OsStr>>(&self, aux: Self::Aux, name: T) -> bool {
        self.namespace() == aux && self.name() == name.as_ref()
    }
}

impl TryFrom<FSValue> for ExtAttr {
    type Error = ();

    fn try_from(fsvalue: FSValue) -> std::result::Result<Self, ()> {
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
    }

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
    }
}

/// BFFFS's timestamp data type.  Very close to FUSE's.
///
/// Unlike libc and Nix, the nsec field is 32 bits wide
#[derive(Debug, Copy, Clone, Deserialize, Eq, PartialEq)]
pub struct Timespec {
    pub sec: i64,
    pub nsec: u32
}

impl Timespec {
    pub fn new(sec: i64, nsec: u32) -> Self {
        Timespec { sec, nsec }
    }

    /// Get the current time in the local time zone
    pub(crate) fn now() -> Self {
        let clock_rt = nix::time::ClockId::CLOCK_REALTIME;
        nix::time::clock_gettime(clock_rt).unwrap().into()
    }
}

impl From<nix::sys::time::TimeSpec> for Timespec {
    fn from(ts: nix::sys::time::TimeSpec) -> Self {
        Timespec {
            sec: ts.tv_sec(),
            nsec: ts.tv_nsec() as u32
        }
    }
}

impl Serialize for Timespec {
    fn serialize<S>(&self, serializer: S)
        -> std::result::Result<S::Ok, S::Error>
        where S: Serializer
    {
        use ::time::OffsetDateTime;
        use ::time::format_description::well_known::Rfc3339;

        if serializer.is_human_readable() {
            // When dumping to YAML, print the timestamp as a legible string
            //const FORMAT = time::format_description::parse(
            let odt = OffsetDateTime::from_unix_timestamp_nanos(
                i128::from(self.sec) * 1000000000 + i128::from(self.nsec)
            ).unwrap();
            odt.format(&Rfc3339)
                .unwrap()
                .serialize(serializer)
        } else {
            // But for bincode, encode it compactly
            let mut state = serializer.serialize_struct("Timespec", 3)?;
            state.serialize_field("sec", &self.sec)?;
            state.serialize_field("nsec", &self.nsec)?;
            state.end()
        }
    }
}

/// In-memory representation of an Inode
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Inode {
    /// File size in bytes.
    pub size:       u64,
    /// File's space consumption in bytes
    ///
    /// Includes the size of all extents, both Blob and Inline.  Excludes
    /// extended attributes and the Inode itself.  Ignores the effects of
    /// compression.
    // Compression cannot be considered because both types of extent can be
    // recompressed in contexts where the Inode is not available.  For example,
    // Inodes are compressed when flushing the Tree, and Blobs can be
    // recompressed when cleaning a zone.
    //
    // * Note that ZFS considers compression here, but BtrFS does not.  POSIX is
    //   silent on the issue.
    //
    // * Also note that ZFS includes the inode, but UFS does not.  Again, POSIX
    //   is silent.
    //
    // * ZFS does not include extended attributes.  UFS does. I'm not sure about
    //   other file systems.  BFFFS does not included extended
    //   attributes so that Fs::setextattr won't need to modify the inode.
    pub bytes:      u64,
    /// Link count
    pub nlink:      u64,
    /// File flags
    pub flags:      u64,
    /// access time
    pub atime:      Timespec,
    /// modification time
    pub mtime:      Timespec,
    /// change time
    pub ctime:      Timespec,
    /// birth time
    pub birthtime:  Timespec,
    /// user id
    pub uid:        u32,
    /// Group id
    pub gid:        u32,
    /// File permissions, the low twelve bits of mode
    // TODO: serialize as octal when dumping to YAML
    pub perm:       u16,
    /// File type.  Regular, directory, etc
    pub file_type:   FileType
}

impl Inode {
    /// This file's record size in bytes.
    pub fn record_size(&self) -> Option<usize> {
        if let FileType::Reg(exp) = self.file_type {
            Some(1 << exp)
        } else {
            None
        }
    }
}

/// This module ought to be unreachable, but must exist to satisfy rustc
mod dbs_serializer {
    use divbuf::DivBufShared;
    use serde::{de::Deserializer, Deserialize, Serialize, Serializer};
    use serde::ser::SerializeSeq;
    use std::sync::Arc;

    pub(super) fn deserialize<'de, DE>(deserializer: DE)
        -> std::result::Result<Arc<DivBufShared>, DE::Error>
        where DE: Deserializer<'de>
    {
        Vec::<u8>::deserialize(deserializer)
            .map(|v| Arc::new(DivBufShared::from(v)))
    }

    pub(super) fn serialize<S>(dbs: &Arc<DivBufShared>, serializer: S)
        -> std::result::Result<S::Ok, S::Error>
        where S: Serializer
    {
        if serializer.is_human_readable() {
            let db = dbs.try_const().unwrap();
            let lines = hexdump::hexdump_iter(&db[..]);
            let mut seq = serializer.serialize_seq(Some(lines.len()))?;
            for line in lines {
                seq.serialize_element(&*line)?;
            }
            seq.end()
        }
        else {
            dbs.try_const().unwrap()[..].serialize(serializer)
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InlineExtent {
    #[serde(with = "dbs_serializer")]
    // The Arc is necessary to make it Clone.
    pub buf: Arc<DivBufShared>
}

#[allow(clippy::len_without_is_empty)]  // It isn't needed
impl InlineExtent {
    const FUDGE: usize = 64;    // Experimentally determined

    fn allocated_space(&self) -> usize {
        self.buf.len() + Self::FUDGE
    }

    fn flush<D, K>(self, k: K, dml: &D, txg: TxgT) -> InlineExtentFlush<K>
        where D: DML + 'static, D::Addr: 'static, K: Key
    {
        let lsize = self.len();
        assert!(lsize > BLOB_THRESHOLD);
        let dbs = Arc::try_unwrap(self.buf).unwrap();
        let gfut = dml.put(dbs, Compression::None, txg);
        let g_type_id = gfut.type_id();
        let cfut: Pin<Box<dyn Future<Output=Result<RID>> + Send>> = unsafe {
            // Safe because we compare type ids
            mem::transmute(gfut)
        };
        debug_assert_eq!(g_type_id, cfut.type_id());
        InlineExtentFlush{k, inner: cfut, lsize: lsize as u32}
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn needs_flush(&self) -> bool {
        self.len() > BLOB_THRESHOLD
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

impl Eq for InlineExtent {}

impl PartialEq for InlineExtent {
    fn eq(&self, other: &Self) -> bool {
        self.buf.try_const().unwrap() == other.buf.try_const().unwrap()
    }
}

/// Return type of `InlineExtent::flush`
#[pin_project]
pub struct InlineExtentFlush<K> {
    k: K,
    #[pin]
    inner: Pin<Box<dyn Future<Output=Result<RID>> + Send>>,
    lsize: u32
}

impl<K: Key> Future for InlineExtentFlush<K>
{
    type Output = Result<(K, FSValue)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let proj = self.as_mut().project();
        match proj.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(rid)) => {
                let be = BlobExtent{lsize: *proj.lsize, rid};
                Poll::Ready(Ok((*proj.k, FSValue::BlobExtent(be))))
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlobExtent {
    pub lsize: u32,
    pub rid: RID,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Extent<'a> {
    Inline(&'a InlineExtent),
    Blob(&'a BlobExtent)
}

impl<'a> Extent<'a> {
    /// The length of this Extent, in bytes
    // An extent can never be empty, so there's no point to is_empty()
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        match self {
            Extent::Inline(ie) => ie.len(),
            Extent::Blob(be) => be.lsize as usize
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum FSValue {
    DirEntry(Dirent),
    Inode(Box<Inode>),
    InlineExtent(InlineExtent),
    BlobExtent(BlobExtent),
    /// An Extended Attribute, for inodes >= 1.  Or a dataset User Property, for
    /// inode 0.
    ExtAttr(ExtAttr),
    /// A whole Bucket of `ExtAttr`s, used in case of hash collisions
    ExtAttrs(Vec<ExtAttr>),
    /// A whole Bucket of `Dirent`s, used in case of hash collisions
    DirEntries(Vec<Dirent>),
    /// A native dataset property.
    Property(Property),
    /// Inode number of an open-but-deleted file in this file
    /// system.  Only valid for object 0.
    DyingInode(DyingInode),
    // TODO: hash bucket of DyingInode
    /// Only used temporarily in memory.  Never written to disk.
    /// Must come last!
    #[doc(hidden)]
    #[default]
    Invalid,
}

impl FSValue {
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

    pub fn as_extattr(&self) -> Option<&ExtAttr> {
        if let FSValue::ExtAttr(extent) = self {
            Some(extent)
        } else {
            None
        }
    }

// LCOV_EXCL_START
    #[cfg(test)]
    pub fn as_extattrs(&self) -> Option<&Vec<ExtAttr>> {
        if let FSValue::ExtAttrs(extents) = self {
            Some(extents)
        } else {
            None
        }
    }
// LCOV_EXCL_STOP

    pub fn as_extent(&self) -> Option<Extent> {
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

    //pub fn as_property(&mut self) -> Option<&mut Property> {
        //if let FSValue::Property(ref mut prop) = self {
            //Some(prop)
        //} else {
            //None
        //}
    //}

    /// How much writeback cache space will `nrecs` extents of size `rs` occupy,
    /// in the worst case?
    pub fn extent_space(rs: usize, nrecs: usize) -> usize {
        nrecs * (rs + InlineExtent::FUDGE)
    }

    fn flush<D, K>(self, k: K, dml: &D, txg: TxgT) -> FSValueFlush<K>
        where D: DML + 'static, D::Addr: 'static, K: Key
    {
        match self {
            FSValue::InlineExtent(ie) => {
                FSValueFlush::Ie(ie.flush(k, dml, txg))
            },
            FSValue::ExtAttr(extattr) => {
                FSValueFlush::Ea(extattr.flush(k, dml, txg))
            }
            FSValue::ExtAttrs(v) => {
                // This code is complicated because we optimize for the
                // non-hash-collision case.
                let fut = v.into_iter()
                .map(|extattr| if extattr.needs_flush() {
                        extattr.flush(k, dml, txg)
                        .map_ok(|(_, v)| {
                             if let FSValue::ExtAttr(ea) = v {
                                 ea
                             } else {
                                 panic!("ExtAttr::flush didn't return an \
                                     ExtAttr?");
                             }
                         }).boxed()
                    } else {
                        future::ok(extattr).boxed()
                    }
                ).collect::<FuturesOrdered<_>>()
                .try_collect::<Vec<_>>()
                .map_ok(move |v| (k, FSValue::ExtAttrs(v)))
                .boxed();
                FSValueFlush::Eav(fut)
            },
            _ => unreachable!("Caller should've checked needs_flush()")
        }
    }

    /// Construct a new FSValue::Inode object
    pub fn inode(inode: Inode) -> Self {
        Self::Inode(Box::new(inode))
    }

    pub fn into_property(self) -> Option<Property> {
        if let FSValue::Property(prop) = self {
            Some(prop)
        } else {
            None
        }
    }

    /// How much space should this FSValue contribute to stat.st_blocks, in
    /// bytes?
    pub fn stat_space(&self) -> i64 {
        match self {
            FSValue::InlineExtent(ie) => ie.len() as i64,
            FSValue::BlobExtent(be) => be.lsize.into(),
            _ => 0
        }
    }
}

impl TypicalSize for FSValue {
    // FSValue can have variable size.  But the most common variant is likely to
    // be FSValue::BlobExtent, which has size 16.
    const TYPICAL_SIZE: usize = 16;
}

impl Value for FSValue {
    const NEEDS_FLUSH: bool = true;

    fn allocated_space(&self) -> usize {
        match self {
            FSValue::DirEntry(dirent) => dirent.allocated_space(),
            FSValue::InlineExtent(extent) => extent.allocated_space(),
            FSValue::Inode(inode) => mem::size_of_val(inode.as_ref()),
            FSValue::ExtAttr(extattr) => extattr.allocated_space(),
            FSValue::ExtAttrs(extattrs) =>
                extattrs.capacity() * mem::size_of::<ExtAttr>() +
                extattrs.iter()
                .map(|extattr| extattr.allocated_space())
                .sum::<usize>(),
            FSValue::DirEntries(dirents) =>
                dirents.capacity() * mem::size_of::<Dirent>() +
                dirents.iter()
                .map(|de| de.allocated_space())
                .sum::<usize>(),
            _ => 0
        }
    }

    #[tracing::instrument(skip(self, dml, txg))]
    fn ddrop<D>(&self, dml: &D, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
        where D: DML + 'static, D::Addr: 'static
    {
        match self {
            FSValue::BlobExtent(be) => {
                dml.delete(&checked_transmute(be.rid), txg).boxed()
            },
            FSValue::ExtAttr(ExtAttr::Blob(xattr)) => {
                dml.delete(&checked_transmute(xattr.extent.rid), txg).boxed()
            },
            FSValue::ExtAttrs(v) => {
                let futs = FuturesUnordered::new();
                for xattr in v {
                    if let ExtAttr::Blob(be) = xattr {
                        let rid = checked_transmute(be.extent.rid);
                        futs.push(dml.delete(&rid, txg));
                    }
                }
                futs.try_fold((), |_, _| future::ok(())).boxed()
            },
            _ => future::ok(()).boxed()
        }
    }

    fn dpop<D>(self, dml: &D, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Self>> + Send>>
        where D: DML + 'static, D::Addr: 'static
    {
        match self {
            FSValue::BlobExtent(be) => {
                dml.pop::<DivBufShared, DivBuf>(&checked_transmute(be.rid), txg)
                .map_ok(move |dbs|
                    FSValue::InlineExtent(InlineExtent::new(Arc::new(*dbs)))
                ).boxed()
            }
            FSValue::ExtAttr(ea) => {
                ea.dpop(dml, txg).map_ok(FSValue::ExtAttr).boxed()
            },
            FSValue::ExtAttrs(v) => {
                v.into_iter()
                .map(|extattr| {
                    extattr.dpop(dml, txg)
                })
                .collect::<FuturesOrdered<_>>()
                .try_collect::<Vec<_>>()
                .map_ok(FSValue::ExtAttrs)
                .boxed()
            }
            _ => future::ok(self).boxed()
        }
    }

    fn flush_bt<D, K: Key>(
        mut bt: BTreeMap<K, Self>,
        dml: &D,
        txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<BTreeMap<K, Self>>> + Send>>
        where D: DML + 'static, D::Addr: 'static
    {
        let inner = FuturesUnordered::new();
        for (km, vm) in bt.iter_mut() {
            if vm.needs_flush() {
                // Replace the value with a placeholder, rather than
                // removing the value, so we don't churn the BTreeMap's own
                // allocations.  OTOH, this copies the entire Value.
                let v = mem::take(vm);
                let k = *km;
                inner.push(v.flush(k, dml, txg));
            }
        }
        inner.try_fold(bt, |mut bt, (k, v)| {
            bt.insert(k, v);
            future::ok(bt)
        }).boxed()
    }

    fn needs_flush(&self) -> bool {
        match self {
            FSValue::InlineExtent(ie) => ie.needs_flush(),
            FSValue::ExtAttr(extattr) => extattr.needs_flush(),
            FSValue::ExtAttrs(_extattrs) => true,
            _ => false
        }
    }
}

/// The return type of `FSValue::flush`
#[pin_project(project = FSValueFlushProj)]
pub enum FSValueFlush<K: Key> {
    Ie(#[pin] InlineExtentFlush<K>),
    // The ExtAttr and ExtAttrs types could be unboxed for better performance,
    // but they aren't nearly as common as InlineExtents.
    Ea(#[pin] Pin<Box<dyn Future<Output=Result<(K, FSValue)>> + Send + 'static>>),
    Eav(#[pin] Pin<Box<dyn Future<Output=Result<(K, FSValue)>> + Send + 'static>>),
}

impl<K: Key> Future for FSValueFlush<K> {
    type Output = Result<(K, FSValue)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.as_mut().project() {
            FSValueFlushProj::Ie(fut) => fut.poll(cx),
            FSValueFlushProj::Ea(fut) => fut.poll(cx),
            FSValueFlushProj::Eav(fut) => fut.poll(cx),
        }
    }
}


// LCOV_EXCL_START
#[cfg(test)]
mod t {

use crate::idml::IDML;
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
    assert_eq!("FSKey { object: 0x42, objtype: DirEntry, offset: 0x42 }",
        format!("{:?}", FSKey::new(0x42, ObjKey::DirEntry(66))));
    assert_eq!("FSKey { object: 0x42, objtype: Unknown, offset: 0x0 }",
        format!("{:?}", FSKey::compose(0x42, 254, 0)));
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
    let fsv = FSValue::BlobExtent(BlobExtent{
        lsize: 0xdead_beef,
        rid: RID(0x0001_0203_0405_0607)
    });
    let v: Vec<u8> = bincode::serialize(&fsv).unwrap();
    assert_eq!(FSValue::TYPICAL_SIZE, v.len());
}

/// Print the size of every FSValue variant.  Not really a test, but useful for
/// debugging.
#[test]
fn fsvalue_variant_size() {
    println!("DirEntry:     {} bytes", mem::size_of::<Dirent>());
    println!("Inode:        {} bytes", mem::size_of::<Box<Inode>>());
    println!("InlineExtent: {} bytes", mem::size_of::<InlineExtent>());
    println!("BlobExtent:   {} bytes", mem::size_of::<BlobExtent>());
    println!("ExtAttr:      {} bytes", mem::size_of::<ExtAttr>());
    println!("ExtAttrs:     {} bytes", mem::size_of::<Vec<ExtAttr>>());
    println!("DirEntries:   {} bytes", mem::size_of::<Vec<Dirent>>());
    println!("Property:     {} bytes", mem::size_of::<Property>());
    println!("DyingInode:   {} bytes", mem::size_of::<DyingInode>());
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
    let unflushed = FSValue::ExtAttr(ExtAttr::Inline(iea));

    assert!(unflushed.needs_flush());
    let flushed = unflushed.flush(42u32, &idml, txg)
        .now_or_never().unwrap()
        .unwrap();

    if let ExtAttr::Inline(_) = flushed.1.as_extattr().unwrap() {
        panic!("Long extattr should've become a BlobExtattr");
    }
}

/// Short InlineExtAttrs should be left in the B+Tree, not turned into blobs
#[test]
fn fsvalue_flush_inline_extattr_short() {
    let namespace = ExtAttrNamespace::User;
    let name = OsString::from("bar");
    let dbs = Arc::new(DivBufShared::from(vec![0, 1, 2, 3, 4, 5]));
    let extent = InlineExtent::new(dbs);
    let iea = InlineExtAttr{namespace, name, extent};
    let unflushed = FSValue::ExtAttr(ExtAttr::Inline(iea));

    assert!(!unflushed.needs_flush());
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
    let unflushed = FSValue::InlineExtent(ile);
    assert!(unflushed.needs_flush());
    let flushed = unflushed.flush(42u32, &idml, txg)
        .now_or_never().unwrap()
        .unwrap();

    if let Extent::Inline(_) = flushed.1.as_extent().unwrap() {
        panic!("Long extent should've become a BlobExtent");
    }
}

/// Short InlineExtents should be left in the B+Tree, not turned into blobs
#[test]
fn fsvalue_flush_inline_extent_short() {
    let data = Arc::new(DivBufShared::from(vec![0, 1, 2, 3, 4, 5]));
    let ile = InlineExtent::new(data);
    let unflushed = FSValue::InlineExtent(ile);
    assert!(!unflushed.needs_flush());
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
