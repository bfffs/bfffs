// vim: tw=80

use divbuf::{DivBuf, DivBufMut, DivBufShared};
use enum_primitive_derive::Primitive;
use lazy_static::lazy_static;
use libc;
use nix;
use num_traits::{FromPrimitive, ToPrimitive};
use serde_derive::*;
use std::{
    fmt::{self, Display, Formatter},
    hash::Hasher,
    ops::{Add, AddAssign, Div, Sub}
};

pub mod cache;
#[cfg(test)] mod cache_mock;
pub mod cleaner;
pub mod cluster;
pub mod database;
#[cfg(test)] pub mod database_mock;
pub mod dataset;
#[cfg(test)] mod dataset_mock;
pub mod ddml;
#[cfg(test)] mod dml_mock;
#[cfg(test)] mod ddml_mock;
pub mod declust;
pub mod device_manager;
pub mod dml;
pub mod fs;
pub mod fs_tree;
pub mod idml;
#[cfg(test)] mod idml_mock;
pub mod label;
pub mod null_raid;
pub mod pool;
pub mod prime_s;
pub mod raid;
mod sgcursor;
pub mod tree;
#[cfg(test)] mod tree_mock;
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

/// Length of the global read-only `ZERO_REGION`
pub const ZERO_REGION_LEN: usize = 8 * BYTES_PER_LBA;

lazy_static! {
    /// A read-only buffer of zeros, useful for padding.
    ///
    /// The length is pretty arbitrary.  Code should be able to cope with a
    /// smaller-than-desired `ZERO_REGION`.  A smaller size will have less
    /// impact on the CPU cache.  A larger size will consume fewer CPU cycles
    /// manipulating sglists.
    static ref ZERO_REGION: DivBufShared =
        DivBufShared::from(vec![0u8; ZERO_REGION_LEN]);
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

/// BFFFS's error type.  Basically just an errno
#[derive(Clone, Copy, Debug, Eq, PartialEq, Primitive)]
pub enum Error {
    // Standard errnos
    EPERM           = libc::EPERM as isize,
    ENOENT          = libc::ENOENT as isize,
    ESRCH           = libc::ESRCH as isize,
    EINTR           = libc::EINTR as isize,
    EIO             = libc::EIO as isize,
    ENXIO           = libc::ENXIO as isize,
    E2BIG           = libc::E2BIG as isize,
    ENOEXEC         = libc::ENOEXEC as isize,
    EBADF           = libc::EBADF as isize,
    ECHILD          = libc::ECHILD as isize,
    EDEADLK         = libc::EDEADLK as isize,
    ENOMEM          = libc::ENOMEM as isize,
    EACCES          = libc::EACCES as isize,
    EFAULT          = libc::EFAULT as isize,
    ENOTBLK         = libc::ENOTBLK as isize,
    EBUSY           = libc::EBUSY as isize,
    EEXIST          = libc::EEXIST as isize,
    EXDEV           = libc::EXDEV as isize,
    ENODEV          = libc::ENODEV as isize,
    ENOTDIR         = libc::ENOTDIR as isize,
    EISDIR          = libc::EISDIR as isize,
    EINVAL          = libc::EINVAL as isize,
    ENFILE          = libc::ENFILE as isize,
    EMFILE          = libc::EMFILE as isize,
    ENOTTY          = libc::ENOTTY as isize,
    ETXTBSY         = libc::ETXTBSY as isize,
    EFBIG           = libc::EFBIG as isize,
    ENOSPC          = libc::ENOSPC as isize,
    ESPIPE          = libc::ESPIPE as isize,
    EROFS           = libc::EROFS as isize,
    EMLINK          = libc::EMLINK as isize,
    EPIPE           = libc::EPIPE as isize,
    EDOM            = libc::EDOM as isize,
    ERANGE          = libc::ERANGE as isize,
    EAGAIN          = libc::EAGAIN as isize,
    EINPROGRESS     = libc::EINPROGRESS as isize,
    EALREADY        = libc::EALREADY as isize,
    ENOTSOCK        = libc::ENOTSOCK as isize,
    EDESTADDRREQ    = libc::EDESTADDRREQ as isize,
    EMSGSIZE        = libc::EMSGSIZE as isize,
    EPROTOTYPE      = libc::EPROTOTYPE as isize,
    ENOPROTOOPT     = libc::ENOPROTOOPT as isize,
    EPROTONOSUPPORT = libc::EPROTONOSUPPORT as isize,
    ESOCKTNOSUPPORT = libc::ESOCKTNOSUPPORT as isize,
    ENOTSUP         = libc::ENOTSUP as isize,
    EPFNOSUPPORT    = libc::EPFNOSUPPORT as isize,
    EAFNOSUPPORT    = libc::EAFNOSUPPORT as isize,
    EADDRINUSE      = libc::EADDRINUSE as isize,
    EADDRNOTAVAIL   = libc::EADDRNOTAVAIL as isize,
    ENETDOWN        = libc::ENETDOWN as isize,
    ENETUNREACH     = libc::ENETUNREACH as isize,
    ENETRESET       = libc::ENETRESET as isize,
    ECONNABORTED    = libc::ECONNABORTED as isize,
    ECONNRESET      = libc::ECONNRESET as isize,
    ENOBUFS         = libc::ENOBUFS as isize,
    EISCONN         = libc::EISCONN as isize,
    ENOTCONN        = libc::ENOTCONN as isize,
    ESHUTDOWN       = libc::ESHUTDOWN as isize,
    ETOOMANYREFS    = libc::ETOOMANYREFS as isize,
    ETIMEDOUT       = libc::ETIMEDOUT as isize,
    ECONNREFUSED    = libc::ECONNREFUSED as isize,
    ELOOP           = libc::ELOOP as isize,
    ENAMETOOLONG    = libc::ENAMETOOLONG as isize,
    EHOSTDOWN       = libc::EHOSTDOWN as isize,
    EHOSTUNREACH    = libc::EHOSTUNREACH as isize,
    ENOTEMPTY       = libc::ENOTEMPTY as isize,
    EPROCLIM        = libc::EPROCLIM as isize,
    EUSERS          = libc::EUSERS as isize,
    EDQUOT          = libc::EDQUOT as isize,
    ESTALE          = libc::ESTALE as isize,
    EREMOTE         = libc::EREMOTE as isize,
    EBADRPC         = libc::EBADRPC as isize,
    ERPCMISMATCH    = libc::ERPCMISMATCH as isize,
    EPROGUNAVAIL    = libc::EPROGUNAVAIL as isize,
    EPROGMISMATCH   = libc::EPROGMISMATCH as isize,
    EPROCUNAVAIL    = libc::EPROCUNAVAIL as isize,
    ENOLCK          = libc::ENOLCK as isize,
    ENOSYS          = libc::ENOSYS as isize,
    EFTYPE          = libc::EFTYPE as isize,
    EAUTH           = libc::EAUTH as isize,
    ENEEDAUTH       = libc::ENEEDAUTH as isize,
    EIDRM           = libc::EIDRM as isize,
    ENOMSG          = libc::ENOMSG as isize,
    EOVERFLOW       = libc::EOVERFLOW as isize,
    ECANCELED       = libc::ECANCELED as isize,
    EILSEQ          = libc::EILSEQ as isize,
    ENOATTR         = libc::ENOATTR as isize,
    EDOOFUS         = libc::EDOOFUS as isize,
    EBADMSG         = libc::EBADMSG as isize,
    EMULTIHOP       = libc::EMULTIHOP as isize,
    ENOLINK         = libc::ENOLINK as isize,
    EPROTO          = libc::EPROTO as isize,
    ENOTCAPABLE     = libc::ENOTCAPABLE as isize,
    ECAPMODE        = libc::ECAPMODE as isize,
    ENOTRECOVERABLE = libc::ENOTRECOVERABLE as isize,
    EOWNERDEAD      = libc::EOWNERDEAD as isize,

    //// BFFFS custom error types below
    EUNKNOWN        = 256,
}

impl From<nix::Error> for Error {
    fn from(e: nix::Error) -> Self {
        match e {
            nix::Error::Sys(errno) => Error::from_i32(errno as i32).unwrap(),
            _ => Error::EUNKNOWN
        }
    }
}

impl Into<i32> for Error {
    fn into(self) -> i32 {
        self.to_i32().unwrap()
    }
}

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

impl Into<u32> for TxgT {
    fn into(self) -> u32 {
        self.0
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

impl Display for RID {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
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
fn test_error() {
    assert_eq!(Error::EPERM, Error::from(nix::Error::Sys(nix::errno::Errno::EPERM)));
    assert_eq!(Error::EUNKNOWN, Error::from(nix::Error::InvalidUtf8));
}

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
