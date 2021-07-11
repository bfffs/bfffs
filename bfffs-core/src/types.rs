// vim: tw=80
//! Common type definitions used throughput BFFFS

use divbuf::{DivBuf, DivBufMut};
use enum_primitive_derive::Primitive;
use num_traits::{FromPrimitive, ToPrimitive};
use serde_derive::{Deserialize, Serialize};
use serde::{
    Deserialize,
    Serialize,
    Serializer,
    de::Deserializer,
    ser::SerializeTuple
};
use std::{
    fmt::{self, Display, Formatter},
    ops::{Add, AddAssign, Sub},
};


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

/// Objects that implement this trait have a typical size when serialized with
/// bincode
pub trait TypicalSize {
    const TYPICAL_SIZE: usize;
}

impl TypicalSize for u32 {
    const TYPICAL_SIZE: usize = 4;
}

impl TypicalSize for f32 {
    const TYPICAL_SIZE: usize = 4;
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
    EOPNOTSUPP      = libc::EOPNOTSUPP as isize,
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
    // TODO: Change ECKSUM to EINTEGRITY in FreeBSD 12.1
    ECKSUM          = 257,
}

impl Error {
    // TODO: replace the return type with `!` once that feature is stable.  Then
    // combine these three methods.  It may also need futures::Never from
    // futures-0.2
    // https://github.com/rust-lang/rust/issues/35121
    pub fn unhandled<E: fmt::Debug>(e: E) {
        panic!("Unhandled error {:?}", e)
    }
    //pub fn unhandled_canceled<E: fmt::Debug>(e: E) -> futures::Canceled {
        //panic!("Unhandled error {:?}", e)
    //}
    pub fn unhandled_error<E: fmt::Debug>(e: E) -> Error {
        panic!("Unhandled error {:?}", e)
    }
}

impl From<nix::Error> for Error {
    fn from(e: nix::Error) -> Self {
        match e {
            nix::Error::Sys(errno) => Error::from_i32(errno as i32).unwrap(),
            _ => Error::EUNKNOWN
        }
    }
}

impl From<Error> for i32 {
    fn from(e: Error) -> Self {
        match e {
            Error::EUNKNOWN =>
                panic!("Unknown error codes should never be exposed"),
            // Checksum errors are a special case of I/O errors
            Error::ECKSUM => Error::EIO.to_i32().unwrap(),
            _ => e.to_i32().unwrap()
        }
    }
}

/// Transaction numbers.
// 32-bits is enough for 1 per second for 100 years
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd,
         Serialize)]
pub struct TxgT(pub u32);

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

impl From<TxgT> for u32 {
    fn from(t: TxgT) -> Self {
        t.0
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
    pub cluster: ClusterT,
    pub lba: LbaT
}

impl PBA {
    pub fn new(cluster: ClusterT, lba: LbaT) -> Self {
        PBA {cluster, lba}
    }
}

impl TypicalSize for PBA {
    const TYPICAL_SIZE: usize = 10;
}

/// Record ID
///
/// Uniquely identifies each indirect record.  Record IDs are never reused.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq,
         PartialOrd, Ord, Serialize)]
pub struct RID(pub u64);

impl Display for RID {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl TypicalSize for RID {
    const TYPICAL_SIZE: usize = 8;
}

/// BFFFS UUID type
///
/// This is just like the `Uuid` from the `uuid` crate, except that it
/// serializes as a fixed-size array instead of a slice
///
/// See Also [#329](https://github.com/uuid-rs/uuid/issues/329)
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Uuid(uuid::Uuid);

impl Uuid {
    pub fn new_v4() -> Self {
        Uuid(uuid::Uuid::new_v4())
    }

    pub fn parse_str(input: &str) -> Result<Uuid, uuid::Error> {
        uuid::Uuid::parse_str(input).map(Uuid)
    }
}

impl<'de> Deserialize<'de> for Uuid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        <[u8; 16]>::deserialize(deserializer)
        .map(|v| Uuid(uuid::Uuid::from_bytes(v)))
    }
}

// LCOV_EXCL_START      only used by debugging code
impl Display for Uuid {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}
// LCOV_EXCL_STOP

impl Serialize for Uuid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let bytes = self.0.as_bytes();
        debug_assert_eq!(bytes.len(), 16);
        let mut tup = serializer.serialize_tuple(16)?;
        for b in bytes.iter() {
            tup.serialize_element(&b)?;
        }
        tup.end()
    }
}

/// Our scatter-gather list.  A slice of reference-counted `IoVec`s.
pub type SGList = Vec<IoVec>;

/// Mutable version of `SGList`.  Uniquely owned.
pub type SGListMut = Vec<IoVecMut>;

/// Indexes a `Vdev`'s Zones.  A Zone is the smallest allocation unit that can
/// be independently erased.
pub type ZoneT = u32;

#[cfg(test)]
mod t {
use pretty_assertions::assert_eq;
use super::*;

#[test]
fn test_error() {
    assert_eq!(Error::EPERM, Error::from(nix::Error::Sys(nix::errno::Errno::EPERM)));
    assert_eq!(Error::EUNKNOWN, Error::from(nix::Error::InvalidUtf8));
}

#[test]
fn pba_typical_size() {
    assert_eq!(PBA::TYPICAL_SIZE,
               bincode::serialized_size(&PBA::default()).unwrap() as usize);
}

#[test]
fn rid_typical_size() {
    assert_eq!(RID::TYPICAL_SIZE,
               bincode::serialized_size(&RID::default()).unwrap() as usize);
}

}
// LCOV_EXCL_STOP
