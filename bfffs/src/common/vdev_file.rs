// vim: tw=80

use crate::common::{*, label::*, vdev::*};
use divbuf::{DivBufShared, DivBuf};
use futures::{Async, IntoFuture, Future, Poll, future};
#[cfg(test)] use mockall::mock;
use nix::libc::{c_int, off_t};
use num_traits::FromPrimitive;
use std::{
    borrow::{Borrow, BorrowMut},
    fs::OpenOptions,
    io,
    mem::{self, MaybeUninit},
    num::NonZeroU64,
    os::unix::{
        fs::OpenOptionsExt,
        io::{AsRawFd, RawFd}
    },
    path::Path
};
use tokio_file::{AioFut, File, LioFut};

/// FFI definitions that don't belong in libc.  The ioctls can't go in libc
/// because they use Nix's macros.  The structs probably shouldn't go in libc,
/// because they're not really intended to be a stable interface.
#[doc(hidden)]
mod ffi {
    use nix::{ ioctl_readwrite, ioctl_write_ptr, libc::{c_int, off_t} };
    const DISK_IDENT_SIZE: usize = 256;

    #[repr(C)]
    #[doc(hidden)]
    pub union diocgattr_arg_value {
        pub c_str:  [u8; DISK_IDENT_SIZE],
        pub off: off_t,
        pub i: c_int,
        pub c_u16: u16
    }

    #[repr(C)]
    #[doc(hidden)]
    // This should be pub(super), but it must be plain pub instead because Nix's
    // ioctl macros make the ioctl functions `pub`.
    pub struct diocgattr_arg {
        pub name: [u8; 64],
        pub len: c_int,
        pub value: diocgattr_arg_value
    }

    ioctl_readwrite! {
        #[doc(hidden)]
        diocgattr, b'd', 142, diocgattr_arg
    }

    ioctl_write_ptr! {
        /// FreeBSD's catch-all ioctl for hole-punching, TRIM, UNMAP,
        /// Deallocate, and EraseWritePointer
        #[doc(hidden)]
        diocgdelete, b'd', 136, [off_t; 2]
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    /// Vdev UUID, fixed at format time
    uuid:           Uuid,
    /// Number of LBAs per simulated zone
    lbas_per_zone:  LbaT,
    /// Number of LBAs that were present at format time
    lbas:           LbaT,
    /// LBAs in the first zone reserved for storing each spacemap.
    spacemap_space:    LbaT
}

/// `VdevFile`: File-backed implementation of `VdevBlock`
///
/// This is used by the FUSE implementation of BFFFS.  It works with both
/// regular files and device files.
///
/// I/O operations on `VdevFile` happen immediately; they are not scheduled.
///
#[derive(Debug)]
pub struct VdevFile {
    file:           File,
    /// Number of reserved LBAS in first zone for each spacemap
    spacemap_space: LbaT,
    /// Number of LBAs per simulated zone
    lbas_per_zone:  LbaT,
    size:           LbaT,
    uuid:           Uuid,
    /// Does the underlying file or device support delete-like operations?
    candelete:      bool
}

/// Tokio-File requires boxed `DivBufs`, but the upper layers of BFFFS don't.
/// Take care of the mismatch here, by wrapping `DivBuf` in a new struct
struct IoVecContainer(IoVec);
impl Borrow<[u8]> for IoVecContainer {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// Tokio-File requires boxed `DivBufMuts`, but the upper layers of BFFFS don't.
/// Take care of the mismatch here, by wrapping `DivBufMut` in a new struct
struct IoVecMutContainer(IoVecMut);
impl Borrow<[u8]> for IoVecMutContainer {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}
impl BorrowMut<[u8]> for IoVecMutContainer {
    fn borrow_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

impl Vdev for VdevFile {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        if lba >= self.reserved_space() {
            Some((lba / (self.lbas_per_zone as u64)) as ZoneT)
        } else {
            None
        }
    }

    fn optimum_queue_depth(&self) -> u32 {
        // The value `10` is just a total guess.
        10
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn sync_all(&self) -> Box<dyn Future<Item = (), Error = Error>> {
        let fut = self.file.sync_all().unwrap()
            .map(drop)
            .map_err(Error::from);
        Box::new(fut)
    }

    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        if zone == 0 {
            (self.reserved_space(), self.lbas_per_zone)
        } else {
            (u64::from(zone) * self.lbas_per_zone,
             u64::from(zone + 1) * self.lbas_per_zone)
        }
    }

    fn zones(&self) -> ZoneT {
        div_roundup(self.size, self.lbas_per_zone) as ZoneT
    }
}

impl VdevFile {
    /// Size of a simulated zone
    const DEFAULT_LBAS_PER_ZONE: LbaT = 1 << 16;  // 256 MB

    fn candelete(fd: RawFd) -> Result<bool, Error> {
        let mut arg = MaybeUninit::<ffi::diocgattr_arg>::uninit();
        let r = unsafe {
            let p = arg.as_mut_ptr();
            (*p).name[0..16].copy_from_slice(b"GEOM::candelete\0");
            (*p).len = mem::size_of::<c_int>() as c_int;
            ffi::diocgattr(fd, p)
        }.map(|_| unsafe { arg.assume_init().value.i } != 0)
        .map_err(Error::from);
        if r == Err(Error::ENOTTY) {
            // This vdev doesn't support DIOCGATTR, so it must not support
            // DIOCGDELETE either.
            Ok(false)
        } else {
            r
        }
    }

    /// Create a new Vdev, backed by a file
    ///
    /// * `path`:           Pathname for the file.  It may be a device node.
    /// * `lbas_per_zone`:  If specified, this many LBAs will be assigned to
    ///                     simulated zones on devices that don't have native
    ///                     zones.
    pub fn create<P>(path: P, lbas_per_zone: Option<NonZeroU64>)
        -> io::Result<Self>
        where P: AsRef<Path> + 'static
    {
        let f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .map(File::new).unwrap();
        let lpz = match lbas_per_zone {
            None => VdevFile::DEFAULT_LBAS_PER_ZONE,
            Some(x) => x.get()
        };
        let candelete = VdevFile::candelete(f.as_raw_fd()).unwrap();
        let size = f.len().unwrap() / BYTES_PER_LBA as u64;
        let nzones = div_roundup(size, lpz);
        let spacemap_space = spacemap_space(nzones);
        let uuid = Uuid::new_v4();
        Ok(VdevFile{
            file: f,
            spacemap_space,
            lbas_per_zone: lpz,
            size,
            uuid,
            candelete
        })
    }

    /// Asynchronously erase the given zone.
    ///
    /// After this, the zone will be in the empty state.  The data may or may
    /// not be inaccessible, and should not be considered securely erased.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to erase
    pub fn erase_zone(&self, lba: LbaT) -> Box<VdevFut> {
        let fut = if self.candelete {
            // There isn't (yet) a way to asynchronously trim, so use a
            // synchronous ioctl.
            let off = lba as off_t * (BYTES_PER_LBA as off_t);
            let len = self.lbas_per_zone as off_t * BYTES_PER_LBA as off_t;
            let args = [off, len];
            unsafe {
                ffi::diocgdelete(self.file.as_raw_fd(), &args)
            }.map(drop)
            .map_err(Error::from)
        } else {
            Ok(())
        }
        .into_future();
        Box::new(fut)
    }

    /// Asynchronously finish the given zone.
    ///
    /// After this, the zone will be in the Full state and writes will not be
    /// allowed.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to finish
    pub fn finish_zone(&self, _lba: LbaT) -> Box<VdevFut> {
        // ordinary files don't have Zone operations
        Box::new(future::ok::<(), Error>(()))
    }

    /// Open an existing `VdevFile`
    ///
    /// Returns both a new `VdevFile` object, and a `LabelReader` that may be
    /// used to construct other vdevs stacked on top of this one.
    ///
    /// * `path`    Pathname for the file.  It may be a device node.
    pub fn open<P: AsRef<Path>>(path: P)
        -> impl Future<Item=(Self, LabelReader), Error=Error>
    {
        OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
        .map(File::new)
        .into_future()
        .map_err(|e| Error::from_i32(e.raw_os_error().unwrap()).unwrap())
        .and_then(|f| {
            VdevFile::read_label(f, 0)
            .or_else(move |(_e, f)| {
                // Try the second label
                VdevFile::read_label(f, 1)
            }).and_then(move |(mut label_reader, f)| {
                let candelete = VdevFile::candelete(f.as_raw_fd()).unwrap();
                let size = f.len().unwrap() / BYTES_PER_LBA as u64;
                let label: Label = label_reader.deserialize().unwrap();
                assert!(size >= label.lbas, "Vdev has shrunk since creation");
                let vdev = VdevFile {
                    file: f,
                    spacemap_space: label.spacemap_space,
                    lbas_per_zone: label.lbas_per_zone,
                    size: label.lbas,
                    uuid: label.uuid,
                    candelete
                };
                Ok((vdev, label_reader))
            }).map_err(|(e, _f)| e)
        })
    }

    /// Asynchronously open the given zone.
    ///
    /// This should be called on an empty zone before writing to that zone.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to open
    pub fn open_zone(&self, _lba: LbaT) -> Box<VdevFut> {
        // ordinary files don't have Zone operations
        Box::new(future::ok::<(), Error>(()))
    }

    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually read.
    pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut> {
        let container = Box::new(IoVecMutContainer(buf));
        let off = lba * (BYTES_PER_LBA as u64);
        let fut = VdevFileFut(self.file.read_at(container, off).unwrap());
        Box::new(fut)
    }

    /// Read just one of a vdev's labels
    fn read_label(f: File, label: u32)
        -> impl Future<Item=(LabelReader, File), Error=(Error, File)>
    {
        let lba = LabelReader::lba(label);
        let offset = lba * BYTES_PER_LBA as u64;
        // TODO: figure out how to use mem::MaybeUninit with divbuf
        let dbs = DivBufShared::uninitialized(LABEL_SIZE);
        let dbm = dbs.try_mut().unwrap();
        let container = Box::new(IoVecMutContainer(dbm));
        f.read_at(container, offset).unwrap()
        .then(move |r| {
            match r {
                Ok(aio_result) => {
                    drop(aio_result);   // release reference on dbs
                    match LabelReader::from_dbs(dbs) {
                        Ok(lr) => Ok((lr, f)),
                        Err(e) => Err((e, f))
                    }
                },
                Err(e) => Err((Error::from(e), f))
            }
        })
    }

    /// Read one of the spacemaps from disk.
    ///
    /// # Parameters
    /// - `buf`:        Place the still-serialized spacemap here.  `buf` will be
    ///                 resized as needed.
    /// - `idx`:        Index of the spacemap to read.  It should be the same as
    ///                 whichever label is being used.
    pub fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> Box<VdevFut> {
        assert!(LbaT::from(idx) < LABEL_COUNT);
        let lba = u64::from(idx) * self.spacemap_space + 2 * LABEL_LBAS;
        let container = Box::new(IoVecMutContainer(buf));
        let off = lba * (BYTES_PER_LBA as u64);
        let fut = VdevFileFut(self.file.read_at(container, off).unwrap());
        Box::new(fut)
    }

    /// The asynchronous scatter/gather read function.
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`         LBA from which to read
    pub fn readv_at(&self, buf: SGListMut, lba: LbaT) -> Box<VdevFut> {
        let off = lba * (BYTES_PER_LBA as u64);
        let containers = buf.into_iter().map(|iovec| {
            Box::new(IoVecMutContainer(iovec)) as Box<dyn BorrowMut<[u8]>>
        }).collect();
        let fut = VdevFileLioFut(self.file.readv_at(containers, off).unwrap());
        Box::new(fut)
    }

    fn reserved_space(&self) -> LbaT {
        LABEL_COUNT * (LABEL_LBAS as u64 + self.spacemap_space)
    }

    /// Size of a single serialized spacemap, in LBAs, rounded up.
    pub fn spacemap_space(&self) -> LbaT {
        self.spacemap_space
    }

    /// Asynchronously write a contiguous portion of the vdev.
    pub fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        assert!(lba >= self.reserved_space(), "Don't overwrite the labels!");
        let container = Box::new(IoVecContainer(buf));
        Box::new(self.write_at_unchecked(container, lba))
    }

    fn write_at_unchecked(&self, buf: Box<dyn Borrow<[u8]>>, lba: LbaT)
        -> impl Future<Item = (), Error = Error>
    {
        {
            let b: &[u8] = (*buf).borrow();
            debug_assert!(b.len() % BYTES_PER_LBA == 0);
        }
        let off = lba * (BYTES_PER_LBA as u64);
        VdevFileFut(self.file.write_at(buf, off).unwrap())
    }

    /// Asynchronously write this Vdev's label.
    ///
    /// `label_writer` should already contain the serialized labels of every
    /// vdev stacked on top of this one.
    pub fn write_label(&self, mut label_writer: LabelWriter) -> Box<VdevFut> {
        let label = Label {
            uuid: self.uuid,
            spacemap_space: self.spacemap_space,
            lbas_per_zone: self.lbas_per_zone,
            lbas: self.size
        };
        label_writer.serialize(&label).unwrap();
        let lba = label_writer.lba();
        let sglist = label_writer.into_sglist();
        Box::new(self.writev_at(sglist, lba))
    }

    /// Asynchronously write to the Vdev's spacemap area.
    ///
    /// # Parameters
    ///
    /// - `sglist`:     Buffers of data to write
    /// - `idx`:        Index of the spacemap area to write: there are more than
    ///                 one.  It should be the same as whichever label is being
    ///                 written.
    /// - `block`:      LBA-based offset from the start of the spacemap area
    pub fn write_spacemap(&self, buf: SGList, idx: u32, block: LbaT)
        -> Box<VdevFut>
    {
        assert!(LbaT::from(idx) < LABEL_COUNT);
        let lba = block + u64::from(idx) * self.spacemap_space + 2 * LABEL_LBAS;
        let bytes: u64 = buf.iter()
            .map(DivBuf::len)
            .sum::<usize>() as u64;
        debug_assert_eq!(bytes % BYTES_PER_LBA as u64, 0);
        let lbas = bytes / BYTES_PER_LBA as LbaT;
        assert!(lba + lbas <= self.reserved_space());
        let containers = buf.into_iter().map(|iovec| {
            Box::new(IoVecContainer(iovec)) as Box<dyn Borrow<[u8]>>
        }).collect();
        let off = lba * (BYTES_PER_LBA as u64);
        let fut = VdevFileLioFut(self.file.writev_at(containers, off).unwrap());
        Box::new(fut)
    }

    /// The asynchronous scatter/gather write function.
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    pub fn writev_at(&self, buf: SGList, lba: LbaT) -> Box<VdevFut> {
        let off = lba * (BYTES_PER_LBA as u64);
        let containers = buf.into_iter().map(|iovec| {
            Box::new(IoVecContainer(iovec)) as Box<dyn Borrow<[u8]>>
        }).collect();
        let fut = VdevFileLioFut(self.file.writev_at(containers, off).unwrap());
        Box::new(fut)
    }
}

struct VdevFileFut(AioFut);

impl Future for VdevFileFut {
    type Item = ();
    type Error = Error;

    // aio_write and friends will sometimes return an error synchronously (like
    // EAGAIN).  VdevBlock handles those errors synchronously by calling poll()
    // once before spawning the future into the event loop.  But that results in
    // calling poll again after it returns an error, which is incompatible with
    // FuturesExt::{map, map_err}'s implementations.  So we have to define a
    // custom poll method here, with map's and map_err's functionality inlined.
    fn poll(&mut self) -> Poll<(), Error> {
        match self.0.poll() {
            Ok(Async::Ready(_aio_result)) => Ok(Async::Ready(())),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(Error::from(e))
        }
    }
}

struct VdevFileLioFut(LioFut);

impl Future for VdevFileLioFut {
    type Item = ();
    type Error = Error;

    // See comments for VdevFileFut::poll
    fn poll(&mut self) -> Poll<(), Error>{
        match self.0.poll() {
            Ok(Async::Ready(mut lio_result_iter)) => {
                // We must drain the iterator to free the AioCb resources
                lio_result_iter.find(|ref r| r.value.is_err())
                .map(|r| Err(Error::from(r.value.unwrap_err())))
                .unwrap_or(Ok(Async::Ready(())))
            },
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(Error::from(e))
        }
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mock!{
    pub VdevFile {
        fn create<P>(path: P, lbas_per_zone: Option<NonZeroU64>)
            -> io::Result<Self>
            where P: AsRef<Path> + 'static;
        fn erase_zone(&self, lba: LbaT) -> Box<VdevFut>;
        fn finish_zone(&self, lba: LbaT) -> Box<VdevFut>;
        fn open<P>(path: P) -> Box<dyn Future<Item=(Self, LabelReader),
                                              Error=Error>>
            where P: AsRef<Path> + 'static;
        fn open_zone(&self, lba: LbaT) -> Box<VdevFut>;
        fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut>;
        fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> Box<VdevFut>;
        fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> Box<VdevFut>;
        fn spacemap_space(&self) -> LbaT;
        fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
        fn write_label(&self, label_writer: LabelWriter) -> Box<VdevFut>;
        fn write_spacemap(&self, buf: SGList, idx: u32, block: LbaT)
            -> Box<VdevFut>;
        fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
    }
    trait Vdev {
        fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
        fn optimum_queue_depth(&self) -> u32;
        fn size(&self) -> LbaT;
        fn sync_all(&self) -> Box<dyn futures::Future<Item = (),
                                  Error = Error>>;
        fn uuid(&self) -> Uuid;
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
        fn zones(&self) -> ZoneT;
    }
}

#[cfg(test)]
mod t {

mod label {
    use super::super::*;

    // pet kcov
    #[test]
    fn debug() {
        let label = Label{ uuid: Uuid::new_v4(),
            lbas_per_zone: 0,
            lbas: 0,
            spacemap_space: 0
        };
        format!("{:?}", label);
    }
}

}
// LCOV_EXCL_STOP
