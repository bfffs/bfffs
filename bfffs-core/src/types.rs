// vim: tw=80
//! Common type definitions used throughput BFFFS

use divbuf::{DivBuf, DivBufMut};
use enum_primitive_derive::Primitive;
use num_traits::{FromPrimitive, ToPrimitive};
use serde_derive::{Deserialize, Serialize};
use serde::{
    ser::{Serialize, Serializer},
    de::{Deserialize, Deserializer},
    ser::SerializeTuple
};
use thiserror::Error;
use std::{
    fmt::{self, Display, Formatter},
    io,
    ops::{Add, AddAssign, Sub},
    str::FromStr,
};

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
#[derive(Clone, Copy, Debug, Deserialize, Error, Eq, PartialEq, Primitive, Serialize)]
pub enum Error {
    // Standard errnos
    #[error("Operation not permitted")]
    EPERM           = libc::EPERM as isize,
    #[error("No such file or directory")]
    ENOENT          = libc::ENOENT as isize,
    #[error("No such process")]
    ESRCH           = libc::ESRCH as isize,
    #[error("Interrupted system call")]
    EINTR           = libc::EINTR as isize,
    #[error("Input/output error")]
    EIO             = libc::EIO as isize,
    #[error("Device not configured")]
    ENXIO           = libc::ENXIO as isize,
    #[error("Argument list too long")]
    E2BIG           = libc::E2BIG as isize,
    #[error("Exec format error")]
    ENOEXEC         = libc::ENOEXEC as isize,
    #[error("Bad file descriptor")]
    EBADF           = libc::EBADF as isize,
    #[error("No child processes")]
    ECHILD          = libc::ECHILD as isize,
    #[error("Resource deadlock avoided")]
    EDEADLK         = libc::EDEADLK as isize,
    #[error("Cannot allocate memory")]
    ENOMEM          = libc::ENOMEM as isize,
    #[error("Permission denied")]
    EACCES          = libc::EACCES as isize,
    #[error("Bad address")]
    EFAULT          = libc::EFAULT as isize,
    #[error("Block device required")]
    ENOTBLK         = libc::ENOTBLK as isize,
    #[error("Device busy")]
    EBUSY           = libc::EBUSY as isize,
    #[error("File exists")]
    EEXIST          = libc::EEXIST as isize,
    #[error("Cross-device link")]
    EXDEV           = libc::EXDEV as isize,
    #[error("Operation not supported by device")]
    ENODEV          = libc::ENODEV as isize,
    #[error("Not a directory")]
    ENOTDIR         = libc::ENOTDIR as isize,
    #[error("Is a directory")]
    EISDIR          = libc::EISDIR as isize,
    #[error("Invalid argument")]
    EINVAL          = libc::EINVAL as isize,
    #[error("Too many open files in system")]
    ENFILE          = libc::ENFILE as isize,
    #[error("Too many open files")]
    EMFILE          = libc::EMFILE as isize,
    #[error("Inappropriate ioctl for device")]
    ENOTTY          = libc::ENOTTY as isize,
    #[error("Text file busy")]
    ETXTBSY         = libc::ETXTBSY as isize,
    #[error("File too large")]
    EFBIG           = libc::EFBIG as isize,
    #[error("No space left on device")]
    ENOSPC          = libc::ENOSPC as isize,
    #[error("Illegal seek")]
    ESPIPE          = libc::ESPIPE as isize,
    #[error("Read-only file system")]
    EROFS           = libc::EROFS as isize,
    #[error("Too many links")]
    EMLINK          = libc::EMLINK as isize,
    #[error("Broken pipe")]
    EPIPE           = libc::EPIPE as isize,
    #[error("Numerical argument out of domain")]
    EDOM            = libc::EDOM as isize,
    #[error("Result too large")]
    ERANGE          = libc::ERANGE as isize,
    #[error("Resource temporarily unavailable")]
    EAGAIN          = libc::EAGAIN as isize,
    #[error("Operation now in progress")]
    EINPROGRESS     = libc::EINPROGRESS as isize,
    #[error("Operation already in progress")]
    EALREADY        = libc::EALREADY as isize,
    #[error("Socket operation on non-socket")]
    ENOTSOCK        = libc::ENOTSOCK as isize,
    #[error("Destination address required")]
    EDESTADDRREQ    = libc::EDESTADDRREQ as isize,
    #[error("Message too long")]
    EMSGSIZE        = libc::EMSGSIZE as isize,
    #[error("Protocol wrong type for socket")]
    EPROTOTYPE      = libc::EPROTOTYPE as isize,
    #[error("Protocol not available")]
    ENOPROTOOPT     = libc::ENOPROTOOPT as isize,
    #[error("Protocol not supported")]
    EPROTONOSUPPORT = libc::EPROTONOSUPPORT as isize,
    #[error("Socket type not supported")]
    ESOCKTNOSUPPORT = libc::ESOCKTNOSUPPORT as isize,
    #[error("Operation not supported")]
    EOPNOTSUPP      = libc::EOPNOTSUPP as isize,
    #[error("Protocol family not supported")]
    EPFNOSUPPORT    = libc::EPFNOSUPPORT as isize,
    #[error("Address family not supported by protocol family")]
    EAFNOSUPPORT    = libc::EAFNOSUPPORT as isize,
    #[error("Address already in use")]
    EADDRINUSE      = libc::EADDRINUSE as isize,
    #[error("Can't assign requested address")]
    EADDRNOTAVAIL   = libc::EADDRNOTAVAIL as isize,
    #[error("Network is down")]
    ENETDOWN        = libc::ENETDOWN as isize,
    #[error("Network is unreachable")]
    ENETUNREACH     = libc::ENETUNREACH as isize,
    #[error("Network dropped connection on reset")]
    ENETRESET       = libc::ENETRESET as isize,
    #[error("Software caused connection abort")]
    ECONNABORTED    = libc::ECONNABORTED as isize,
    #[error("Connection reset by peer")]
    ECONNRESET      = libc::ECONNRESET as isize,
    #[error("No buffer space available")]
    ENOBUFS         = libc::ENOBUFS as isize,
    #[error("Socket is already connected")]
    EISCONN         = libc::EISCONN as isize,
    #[error("Socket is not connected")]
    ENOTCONN        = libc::ENOTCONN as isize,
    #[error("Can't send after socket shutdown")]
    ESHUTDOWN       = libc::ESHUTDOWN as isize,
    #[error("Too many references: can't splice")]
    ETOOMANYREFS    = libc::ETOOMANYREFS as isize,
    #[error("Operation timed out")]
    ETIMEDOUT       = libc::ETIMEDOUT as isize,
    #[error("Connection refused")]
    ECONNREFUSED    = libc::ECONNREFUSED as isize,
    #[error("Too many levels of symbolic links")]
    ELOOP           = libc::ELOOP as isize,
    #[error("File name too long")]
    ENAMETOOLONG    = libc::ENAMETOOLONG as isize,
    #[error("Host is down")]
    EHOSTDOWN       = libc::EHOSTDOWN as isize,
    #[error("No route to host")]
    EHOSTUNREACH    = libc::EHOSTUNREACH as isize,
    #[error("Directory not empty")]
    ENOTEMPTY       = libc::ENOTEMPTY as isize,
    #[error("Too many processes")]
    EPROCLIM        = libc::EPROCLIM as isize,
    #[error("Too many users")]
    EUSERS          = libc::EUSERS as isize,
    #[error("Disc quota exceeded")]
    EDQUOT          = libc::EDQUOT as isize,
    #[error("Stale NFS file handle")]
    ESTALE          = libc::ESTALE as isize,
    #[error("Too many levels of remote in path")]
    EREMOTE         = libc::EREMOTE as isize,
    #[error("RPC struct is bad")]
    EBADRPC         = libc::EBADRPC as isize,
    #[error("RPC version wrong")]
    ERPCMISMATCH    = libc::ERPCMISMATCH as isize,
    #[error("RPC prog. not avail")]
    EPROGUNAVAIL    = libc::EPROGUNAVAIL as isize,
    #[error("Program version wrong")]
    EPROGMISMATCH   = libc::EPROGMISMATCH as isize,
    #[error("Bad procedure for program")]
    EPROCUNAVAIL    = libc::EPROCUNAVAIL as isize,
    #[error("No locks available")]
    ENOLCK          = libc::ENOLCK as isize,
    #[error("Function not implemented")]
    ENOSYS          = libc::ENOSYS as isize,
    #[error("Inappropriate file type or format")]
    EFTYPE          = libc::EFTYPE as isize,
    #[error("Authentication error")]
    EAUTH           = libc::EAUTH as isize,
    #[error("Need authenticator.")]
    ENEEDAUTH       = libc::ENEEDAUTH as isize,
    #[error("Identifier removed")]
    EIDRM           = libc::EIDRM as isize,
    #[error("No message of desired type")]
    ENOMSG          = libc::ENOMSG as isize,
    #[error("Value too large to be stored in data type")]
    EOVERFLOW       = libc::EOVERFLOW as isize,
    #[error("Operation canceled")]
    ECANCELED       = libc::ECANCELED as isize,
    #[error("Illegal byte sequence")]
    EILSEQ          = libc::EILSEQ as isize,
    #[error("Attribute not found")]
    ENOATTR         = libc::ENOATTR as isize,
    #[error("Programming error")]
    EDOOFUS         = libc::EDOOFUS as isize,
    #[error("Bad message")]
    EBADMSG         = libc::EBADMSG as isize,
    #[error("Multihop attempted")]
    EMULTIHOP       = libc::EMULTIHOP as isize,
    #[error("Link has been severed")]
    ENOLINK         = libc::ENOLINK as isize,
    #[error("Protocol error")]
    EPROTO          = libc::EPROTO as isize,
    #[error("Capabilities insufficient")]
    ENOTCAPABLE     = libc::ENOTCAPABLE as isize,
    #[error("Not permitted in capability mode")]
    ECAPMODE        = libc::ECAPMODE as isize,
    #[error("State not recoverable")]
    ENOTRECOVERABLE = libc::ENOTRECOVERABLE as isize,
    #[error("Previous owner died")]
    EOWNERDEAD      = libc::EOWNERDEAD as isize,
    #[error("Integrity check failed")]
    EINTEGRITY      = libc::EINTEGRITY as isize,

    //// BFFFS custom error types below
    #[error("Unknown error")]
    EUNKNOWN        = 256,
}

impl Error {
    // TODO: replace the return type with `!` once that feature is stable.  Then
    // combine these three methods.  It may also need futures::Never from
    // futures-0.2
    // https://github.com/rust-lang/rust/issues/35121
    pub fn unhandled<E: fmt::Debug>(e: E) {
        panic!("Unhandled error {e:?}")
    }
    //pub fn unhandled_canceled<E: fmt::Debug>(e: E) -> futures::Canceled {
        //panic!("Unhandled error {e:?}")
    //}
    pub fn unhandled_error<E: fmt::Debug>(e: E) -> Error {
        panic!("Unhandled error {e:?}")
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        e.raw_os_error()
            .and_then(Error::from_i32)
            .unwrap_or(Error::EUNKNOWN)
    }
}

impl From<nix::Error> for Error {
    fn from(e: nix::Error) -> Self {
        Error::from_i32(e as i32).unwrap_or(Error::EUNKNOWN)
    }
}

impl From<Error> for i32 {
    fn from(e: Error) -> Self {
        match e {
            Error::EUNKNOWN =>
                panic!("Unknown error codes should never be exposed"),
            _ => e.to_i32().unwrap()
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

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
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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
// The Uuid crate serializes to a slice, and its maintainers have ruled out ever
// serializing to a fixed-size array instead.
// See Also [Uuid #557](https://github.com/uuid-rs/uuid/issues/557)
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Uuid(uuid::Uuid);

impl Uuid {
    pub const fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }

    pub fn new_v4() -> Self {
        Uuid(uuid::Uuid::new_v4())
    }

    pub fn parse_str(input: &str) -> std::result::Result<Uuid, uuid::Error> {
        uuid::Uuid::parse_str(input).map(Uuid)
    }
}

impl FromStr for Uuid {
    type Err = <uuid::Uuid as FromStr>::Err;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        uuid::Uuid::from_str(s).map(Self)
    }
}

impl<'de> Deserialize<'de> for Uuid {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
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
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
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

// LCOV_EXCL_START
#[cfg(test)]
mod t {
use pretty_assertions::assert_eq;
use super::*;

#[test]
fn test_error() {
    assert_eq!(Error::EPERM, Error::from(nix::errno::Errno::EPERM));
    assert_eq!(Error::EUNKNOWN, Error::from(nix::Error::UnknownErrno));
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

mod uuid {
    use super::*;
    use pretty_assertions::assert_eq;

    const BIN: [u8; 17] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0xFF
    ];
    const STR: &str = "00010203-0405-0607-0809-0a0b0c0d0e0f";

    mod deserialize {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn ok() {
            let uuid: Uuid = bincode::deserialize(&BIN).unwrap();
            let want = Uuid::parse_str(STR).unwrap();
            assert_eq!(uuid, want);
        }

        #[test]
        fn too_short() {
            bincode::deserialize::<Uuid>(&BIN[0..15]).unwrap_err();
        }
    }

    #[test]
    fn serialize() {
        let uuid = Uuid::parse_str(STR).unwrap();
        let buf = bincode::serialize(&uuid).unwrap();
        assert_eq!(buf.len(), 16);
        assert_eq!(&buf[..], &BIN[0..16]);
    }
}
}
// LCOV_EXCL_STOP
