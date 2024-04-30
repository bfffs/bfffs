// vim: tw=80

use crate::{
    label::*,
    types::*,
    util::*,
    vdev::*
};
use atomic_enum::atomic_enum;
use cfg_if::cfg_if;
use divbuf::DivBuf;
use futures::{
    Future,
    FutureExt,
    TryFutureExt,
    future,
    task::{Context, Poll}
};
#[cfg(test)] use mockall::mock;
use nix::libc::{c_int, off_t};
use num_traits::FromPrimitive;
use pin_project::pin_project;
use serde_derive::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    collections::BTreeMap,
    fs::OpenOptions,
    io::{self, IoSlice, IoSliceMut},
    mem::{self, MaybeUninit},
    num::NonZeroU64,
    os::unix::{
        fs::OpenOptionsExt,
        io::{AsRawFd, RawFd}
    },
    path::{Path, PathBuf},
    pin::Pin,
    sync::atomic::Ordering
};
use tokio_file::File;
use tokio::task;

/// How does this device deallocate sectors?
#[atomic_enum]
enum EraseMethod {
    None,
    Diocgdelete,
    #[cfg(have_fspacectl)]
    Fspacectl,
    #[cfg(have_fspacectl)]
    MaybeFspacectl,
}

impl AtomicEraseMethod {
    /// Get the initial erase method.
    fn initial(fd: RawFd) -> Result<Self> {
        if VdevFile::candelete(fd)? {
            // The file supports DIOCGDELETE.
            Ok(AtomicEraseMethod::new(EraseMethod::Diocgdelete))
        } else {
            cfg_if! {
                if #[cfg(have_fspacectl)] {
                    // The file does not support DIOCGDELETE.  Optimistically
                    // guess that it supports fspacectl.
                    Ok(AtomicEraseMethod::new(EraseMethod::MaybeFspacectl))
                } else {
                    Ok(AtomicEraseMethod::new(EraseMethod::None))
                }
            }
        }
    }
}

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
        /// Deallocate, and EraseWritePointer of GEOM devices.
        #[doc(hidden)]
        diocgdelete, b'd', 136, [off_t; 2]
    }
}

use ffi::{diocgdelete, diocgattr, diocgattr_arg};

#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    /// Vdev UUID, fixed at format time
    uuid:           Uuid,
    /// Number of LBAs per simulated zone
    lbas_per_zone:  LbaT,
    /// Number of LBAs that were present at format time
    lbas:           LbaT,
    /// LBAs in the first zone reserved for storing each spacemap.
    spacemap_space: LbaT,
}

/// Manage BFFFS-formatted disks that aren't yet part of an imported pool.
#[derive(Default)]
pub struct Manager {
    devices: BTreeMap<Uuid, VdevFile>,
}

impl Manager {
    /// Import a block device that is already known to exist
    pub fn import(&mut self, uuid: Uuid)
        -> impl Future<Output=Result<(VdevFile, LabelReader)>>
    {
        future::ready(self.devices.remove(&uuid).ok_or(Error::ENOENT))
            .and_then(move |vf| async move {
                let mut lr = VdevFile::read_label(&vf.file).await?;
                let label: Label = lr.deserialize().unwrap();
                assert_eq!(uuid, label.uuid);
                Ok((vf, lr))
            })
    }

    /// Taste the device identified by `p` for a BFFFS label.
    ///
    /// If present, retain the device in the `DevManager` for use as a spare or
    /// for building Pools.
    // TODO: add a method for tasting disks in parallel.
    pub async fn taste<P: AsRef<Path>>(&mut self, p: P) -> Result<LabelReader> {
        let (vdev_file, reader) = VdevFile::open(p).await?;
        self.devices.insert(vdev_file.uuid(), vdev_file);
        Ok(reader)
    }
}

/// Return value of [`VdevFile::status`]
#[derive(Clone, Debug)]
pub struct Status {
    pub health: Health,
    pub path: PathBuf,
    pub uuid: Uuid
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
    /// The name used to open this Vdev.  It may change if the pool is exported
    /// and reimported.
    path:           PathBuf,
    size:           LbaT,
    uuid:           Uuid,
    /// How does the underlying file deallocate data?
    erase_method:   AtomicEraseMethod
}

impl Vdev for VdevFile {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        if lba >= self.reserved_space() {
            Some((lba / self.lbas_per_zone) as ZoneT)
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

    fn sync_all(&self) -> BoxVdevFut {
        let fut = self.file.sync_all().unwrap()
            .map_ok(drop)
            .map_err(Error::from);
        Box::pin(fut)
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

    fn candelete(fd: RawFd) -> Result<bool> {
        let mut arg = MaybeUninit::<diocgattr_arg>::uninit();
        let r = unsafe {
            let p = arg.as_mut_ptr();
            (*p).name[0..16].copy_from_slice(b"GEOM::candelete\0");
            (*p).len = mem::size_of::<c_int>() as c_int;
            diocgattr(fd, p)
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
        where P: AsRef<Path>
    {
        let pb = path.as_ref().to_path_buf();
        // NB: Annoyingly, using O_EXLOCK without O_NONBLOCK means that we can
        // block indefinitely.  However, using O_NONBLOCK is worse because it
        // can cause spurious failures, such as when another thread fork()s.
        // That happens frequently in the functional tests.
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT | libc::O_EXLOCK)
            .open(path)
            .map(File::new)?;
        let lpz = match lbas_per_zone {
            None => VdevFile::DEFAULT_LBAS_PER_ZONE,
            Some(x) => x.get()
        };
        let erase_method = AtomicEraseMethod::initial(f.as_raw_fd()).unwrap();
        let size = f.len().unwrap() / BYTES_PER_LBA as u64;
        let nzones = div_roundup(size, lpz);
        let spacemap_space = spacemap_space(nzones);
        let uuid = Uuid::new_v4();
        Ok(VdevFile{
            file: f,
            spacemap_space,
            lbas_per_zone: lpz,
            path: pb,
            size,
            uuid,
            erase_method
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
    // There isn't (yet) a way to asynchronously trim, so use a synchronous
    // method in a blocking_task
    pub fn erase_zone(&self, lba: LbaT) -> BoxVdevFut {
        let fd = self.file.as_raw_fd();
        let off = lba as off_t * (BYTES_PER_LBA as off_t);
        let len = self.lbas_per_zone as off_t * BYTES_PER_LBA as off_t;
        let em = self.erase_method.load(Ordering::Relaxed);
        match em {
            EraseMethod::None => Box::pin(future::ok(())),
            EraseMethod::Diocgdelete => {
                let t = task::spawn_blocking(move || {
                    let args = [off, len];
                    unsafe {
                        diocgdelete(fd, &args)
                    }.map(drop)
                }).map(std::result::Result::unwrap)
                .map_err(Error::from);
                Box::pin(t)
            },
            #[cfg(have_fspacectl)]
            EraseMethod::MaybeFspacectl => {
                use nix::fcntl::fspacectl_all;

                // The first time erasing a zone, do it synchronously so we can
                // change the erase_method.
                match fspacectl_all(fd, off, len) {
                    Err(nix::Error::ENOSYS) =>  {
                        // fspacectl is not supported by the running system
                        self.erase_method.store(EraseMethod::None,
                                                Ordering::Relaxed);
                        Box::pin(future::ok(()))
                    },
                    Err(nix::Error::ENODEV) => {
                        // fspacectl is not supported by this file
                        self.erase_method.store(EraseMethod::None,
                                                Ordering::Relaxed);
                        Box::pin(future::ok(()))
                    },
                    Ok(()) => {
                        // fspacectl is definitely supported
                        self.erase_method.store(EraseMethod::Fspacectl,
                                                Ordering::Relaxed);
                        Box::pin(future::ok(()))
                    },
                    Err(e) => Box::pin(future::err(e.into()))
                }
            },
            #[cfg(have_fspacectl)]
            EraseMethod::Fspacectl => {
                use nix::fcntl::fspacectl_all;

                let t = task::spawn_blocking(move || {
                    fspacectl_all(fd, off, len)
                }).map(std::result::Result::unwrap)
                .map_err(Error::from);
                Box::pin(t)
            }
        }
    }

    /// Asynchronously finish the given zone.
    ///
    /// After this, the zone will be in the Full state and writes will not be
    /// allowed.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to finish
    pub fn finish_zone(&self, _lba: LbaT) -> BoxVdevFut {
        // ordinary files don't have Zone operations
        Box::pin(future::ok(()))
    }

    /// Open an existing `VdevFile`
    ///
    /// Returns both a new `VdevFile` object, and a `LabelReader` that may be
    /// used to construct other vdevs stacked on top of this one.
    ///
    /// * `path`    Pathname for the file.  It may be a device node.
    pub async fn open<P: AsRef<Path>>(path: P)
        -> Result<(Self, LabelReader)>
    {
        let pb = path.as_ref().to_path_buf();
        // NB: Annoyingly, using O_EXLOCK without O_NONBLOCK means that we can
        // block indefinitely.  However, using O_NONBLOCK is worse because it
        // can cause spurious failures, such as when another thread fork()s.
        // That happens frequently in the functional tests.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT | libc::O_EXLOCK)
            .open(path)
            .map(File::new)
            .map_err(|e| Error::from_i32(e.raw_os_error().unwrap()).unwrap());
        match file {
            Ok(f) => {
                match VdevFile::read_label(&f).await {
                    Err(e) => Err(e),
                    Ok(mut label_reader) => {
                        let fd = f.as_raw_fd();
                        let erase_method = AtomicEraseMethod::initial(fd)?;
                        let size = f.len().unwrap() / BYTES_PER_LBA as u64;
                        let label: Label = label_reader.deserialize().unwrap();
                        assert!(size >= label.lbas,
                                "Vdev has shrunk since creation");
                        let vdev = VdevFile {
                            file: f,
                            spacemap_space: label.spacemap_space,
                            lbas_per_zone: label.lbas_per_zone,
                            path: pb,
                            size: label.lbas,
                            uuid: label.uuid,
                            erase_method
                        };
                        Ok((vdev, label_reader))
                    }
                }
            },
            Err(e) => Err(e)
        }
    }

    /// Asynchronously open the given zone.
    ///
    /// This should be called on an empty zone before writing to that zone.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to open
    pub fn open_zone(&self, _lba: LbaT) -> BoxVdevFut {
        // ordinary files don't have Zone operations
        Box::pin(future::ok(()))
    }

    /// The pathname most recently used to open this device.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually read.
    pub fn read_at(&self, mut buf: IoVecMut, lba: LbaT) -> BoxVdevFut {
        // Unlike write_at, the upper layers will never read into a buffer that
        // isn't a multiple of a block size.  DDML::read ensures that.
        debug_assert_eq!(buf.len() % BYTES_PER_LBA, 0);

        let off = lba * (BYTES_PER_LBA as u64);
        let bufaddr: &'static mut [u8] = unsafe {
            // Safe because fut's lifetime will be equal to buf's once we move
            // it into the ReadAt struct.  Also, because buf is already
            // heap-allocated, moving it won't change the data's address.
            mem::transmute::<&mut[u8], &'static mut [u8]>(buf.as_mut())
        };
        let fut = self.file.read_at(&mut *bufaddr, off).unwrap();
        Box::pin(ReadAt { _buf: buf, fut })
    }

    /// Read just one of a vdev's labels
    async fn read_label(f: &File) -> Result<LabelReader>
    {
        let mut r = Err(Error::EDOOFUS);    // Will get overridden

        for label in 0..2 {
            let lba = LabelReader::lba(label);
            let offset = lba * BYTES_PER_LBA as u64;
            // TODO: figure out how to use mem::MaybeUninit with File::read_at
            let mut rbuf = vec![0; LABEL_SIZE];
            match f.read_at(&mut rbuf[..], offset).unwrap().await {
                Ok(_aio_result) => {
                    match LabelReader::new(rbuf) {
                        Ok(lr) => {
                            r = Ok(lr);
                            break
                        }
                        Err(e) => {
                            // If this is the first label, try the second.
                            r = Err(e);
                        }
                    }
                },
                Err(e) => {
                    r = Err(Error::from(e));
                }
            }
        }
        r
    }

    /// Read one of the spacemaps from disk.
    ///
    /// # Parameters
    /// - `buf`:        Place the still-serialized spacemap here.  `buf` will be
    ///                 resized as needed.
    /// * `lba`     LBA to read from
    pub fn read_spacemap(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut
    {
        self.read_at(buf, lba)
    }

    /// The asynchronous scatter/gather read function.
    ///
    /// * `sglist   Scatter-gather list of buffers to receive data
    /// * `lba`     LBA to read from
    #[allow(clippy::transmute_ptr_to_ptr)]  // Clippy false positive
    pub fn readv_at(&self, mut sglist: SGListMut, lba: LbaT) -> BoxVdevFut
    {
        for iovec in sglist.iter() {
            debug_assert_eq!(iovec.len() % BYTES_PER_LBA, 0);
        }
        let off = lba * (BYTES_PER_LBA as u64);
        let mut slices: Box<[IoSliceMut<'static>]> = unsafe {
            // Safe because fut's lifetime will be equal to slices's once we
            // move it into the ReadvAt struct.  Also, because sglist is already
            // heap-allocated, so moving it won't change the data's address.
            sglist.iter_mut()
            .map(|b| {
                let sl = mem::transmute::<&mut [u8], &'static mut[u8]>(&mut b[..]);
                IoSliceMut::new(sl)
            }).collect::<Vec<_>>()
            .into_boxed_slice()
        };
        let bufs: &'static mut [IoSliceMut<'static>] = unsafe {
            // Safe because fut's lifetime will be equal to bufs's once we
            // move it into the ReadvAt struct.  Also, because slies is already
            // heap-allocated, so moving it won't change the data's address.
            mem::transmute::<&mut[IoSliceMut<'static>],
                             &'static mut [IoSliceMut<'static>]>(
                &mut slices
            )
        };
        let fut = self.file.readv_at(bufs, off).unwrap();
        Box::pin(ReadvAt {
            _sglist: sglist,
            _slices: slices,
            fut
        })
    }

    fn reserved_space(&self) -> LbaT {
        LABEL_COUNT * (LABEL_LBAS + self.spacemap_space)
    }

    /// Size of a single serialized spacemap, in LBAs, rounded up.
    pub fn spacemap_space(&self) -> LbaT {
        self.spacemap_space
    }

    pub fn status(&self) -> Status {
        Status {
            health: Health::Online,
            path: self.path.clone(),
            uuid: self.uuid
        }
    }

    /// Asynchronously write a contiguous portion of the vdev.
    pub fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut
    {
        assert!(lba >= self.reserved_space(), "Don't overwrite the labels!");
        debug_assert_eq!(buf.len() % BYTES_PER_LBA, 0);
        self.write_at_unchecked(buf, lba)
    }

    fn write_at_unchecked(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut
    {
        let off = lba * (BYTES_PER_LBA as u64);
        {
            let b: &[u8] = buf.borrow();
            debug_assert!(b.len() % BYTES_PER_LBA == 0);
        }

        // Safe because fut's lifetime is equal to buf's (or rather, it will
        // be once we move it into the WriteAt struct
        let sbuf: &'static [u8] = unsafe {
            mem::transmute::<&[u8], &'static [u8]>( buf.as_ref())
        };
        let fut = self.file.write_at(sbuf, off).unwrap();

        Box::pin(WriteAt { _buf: buf, fut })
    }

    /// Asynchronously write this Vdev's label.
    ///
    /// `label_writer` should already contain the serialized labels of every
    /// vdev stacked on top of this one.
    pub fn write_label(&self, mut label_writer: LabelWriter) -> BoxVdevFut
    {
        let label = Label {
            uuid: self.uuid,
            spacemap_space: self.spacemap_space,
            lbas_per_zone: self.lbas_per_zone,
            lbas: self.size
        };
        label_writer.serialize(&label).unwrap();
        let lba = label_writer.lba();
        let sglist = label_writer.into_sglist();
        let sglist = copy_and_pad_sglist(sglist);
        self.writev_at_unchecked(sglist, lba)
    }

    /// Asynchronously write to the Vdev's spacemap area.
    ///
    /// # Parameters
    ///
    /// - `sglist`:     Buffers of data to write
    /// * `lba`     LBA to write to
    pub fn write_spacemap(&self, sglist: SGList, lba: LbaT)
        -> BoxVdevFut
    {
        let bytes: u64 = sglist.iter()
            .map(DivBuf::len)
            .sum::<usize>() as u64;
        debug_assert_eq!(bytes % BYTES_PER_LBA as u64, 0);
        let lbas = bytes / BYTES_PER_LBA as LbaT;
        assert!(lba + lbas <= self.reserved_space());
        self.writev_at_unchecked(sglist, lba)
    }

    /// The asynchronous scatter/gather write function.
    ///
    /// * `sglist`  Scatter-gather list of buffers to write
    /// * `lba`     LBA to write to
    pub fn writev_at(&self, sglist: SGList, lba: LbaT) -> BoxVdevFut
    {
        assert!(lba >= self.reserved_space(), "Don't overwrite the labels!");
        self.writev_at_unchecked(sglist, lba)
    }

    fn writev_at_unchecked(&self, sglist: SGList, lba: LbaT)
        -> Pin<Box<WritevAt>>
    {
        let off = lba * (BYTES_PER_LBA as u64);

        let slices: Box<[IoSlice<'static>]> =
            sglist.iter()
                .map(|b| {
                    debug_assert_eq!(b.len() % BYTES_PER_LBA, 0);
                    // Safe because fut's lifetime is equal to slices' (or
                    // rather, it will be once we move it into the WriteAt
                    // struct
                    let sb = unsafe {
                        mem::transmute::<&[u8], &'static [u8]>(&b[..])
                    };
                    IoSlice::new(sb)
                }).collect::<Vec<_>>()
                .into_boxed_slice();
        let fut = self.file.writev_at(&slices, off).unwrap();

        Box::pin(WritevAt {
            _sglist: sglist,
            _slices: slices,
            fut
        })
    }
}

#[pin_project]
struct ReadAt {
    // Owns the buffer used by the Future
    _buf: IoVecMut,
    #[pin]
    fut: tokio_file::ReadAt<'static>
}

impl Future for ReadAt {
    type Output = Result<()>;

    // aio_write and friends will sometimes return an error synchronously (like
    // EAGAIN).  VdevBlock handles those errors synchronously by calling poll()
    // once before spawning the future into the event loop.  But that results in
    // calling poll again after it returns an error, which is incompatible with
    // FuturesExt::{map, map_err}'s implementations.  So we have to define a
    // custom poll method here, with map's and map_err's functionality inlined.
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.project().fut.poll(cx) {
            Poll::Ready(Ok(_aio_result)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::from(e))),
            Poll::Pending => Poll::Pending
        }
    }
}

#[pin_project]
struct WriteAt {
    #[pin]
    fut: tokio_file::WriteAt<'static>,
    // Owns the buffer used by the Future
    _buf: IoVec,
}

impl Future for WriteAt {
    type Output = Result<()>;

    // aio_write and friends will sometimes return an error synchronously (like
    // EAGAIN).  VdevBlock handles those errors synchronously by calling poll()
    // once before spawning the future into the event loop.  But that results in
    // calling poll again after it returns an error, which is incompatible with
    // FuturesExt::{map, map_err}'s implementations.  So we have to define a
    // custom poll method here, with map's and map_err's functionality inlined.
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.project().fut.poll(cx) {
            Poll::Ready(Ok(_aio_result)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::from(e))),
            Poll::Pending => Poll::Pending
        }
    }
}

#[pin_project]
struct ReadvAt {
    #[pin]
    fut: tokio_file::ReadvAt<'static>,
    // Owns the pointer array used by the Future
    _slices: Box<[IoSliceMut<'static>]>,
    // Owns the buffers used by the Future
    _sglist: SGListMut,
}

impl Future for ReadvAt {
    type Output = Result<()>;

    // aio_write and friends will sometimes return an error synchronously (like
    // EAGAIN).  VdevBlock handles those errors synchronously by calling poll()
    // once before spawning the future into the event loop.  But that results in
    // calling poll again after it returns an error, which is incompatible with
    // FuturesExt::{map, map_err}'s implementations.  So we have to define a
    // custom poll method here, with map's and map_err's functionality inlined.
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.project().fut.poll(cx) {
            Poll::Ready(Ok(_l)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::from(e))),
            Poll::Pending => Poll::Pending
        }
    }
}

#[pin_project]
struct WritevAt{
    #[pin]
    fut: tokio_file::WritevAt<'static>,
    // Owns the pointer array used by the Future
    _slices: Box<[IoSlice<'static>]>,
    // Owns the buffers used by the Future
    _sglist: SGList,
}

impl Future for WritevAt {
    type Output = Result<()>;

    // aio_write and friends will sometimes return an error synchronously (like
    // EAGAIN).  VdevBlock handles those errors synchronously by calling poll()
    // once before spawning the future into the event loop.  But that results in
    // calling poll again after it returns an error, which is incompatible with
    // FuturesExt::{map, map_err}'s implementations.  So we have to define a
    // custom poll method here, with map's and map_err's functionality inlined.
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.project().fut.poll(cx) {
            Poll::Ready(Ok(_l)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::from(e))),
            Poll::Pending => Poll::Pending
        }
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mock!{
    pub VdevFile {
        #[mockall::concretize]
        pub fn create<P>(path: P, lbas_per_zone: Option<NonZeroU64>)
            -> io::Result<Self>
            where P: AsRef<Path>;
        pub fn erase_zone(&self, lba: LbaT) -> BoxVdevFut;
        pub fn finish_zone(&self, _lba: LbaT) -> BoxVdevFut;
        #[mockall::concretize]
        pub async fn open<P>(path: P) -> Result<(Self, LabelReader)>
            where P: AsRef<Path>;
        pub fn open_zone(&self, _lba: LbaT) -> BoxVdevFut;
        pub fn path(&self) -> &Path;
        pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        pub fn read_spacemap(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> BoxVdevFut;
        pub fn spacemap_space(&self) -> LbaT;
        pub fn status(&self) -> Status;
        pub fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut;
        pub fn write_label(&self, mut label_writer: LabelWriter) -> BoxVdevFut;
        pub fn write_spacemap(&self, buf: SGList, lba: LbaT) -> BoxVdevFut;
        pub fn writev_at(&self, buf: SGList, lba: LbaT) -> BoxVdevFut;
    }
    impl Vdev for VdevFile {
        fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
        fn optimum_queue_depth(&self) -> u32;
        fn size(&self) -> LbaT;
        fn sync_all(&self) -> BoxVdevFut;
        fn uuid(&self) -> Uuid;
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
        fn zones(&self) -> ZoneT;
    }
}

#[cfg(test)]
mod t {
    use super::*;

mod label {
    use super::*;

    // pet kcov
    #[test]
    fn debug() {
        let label = Label{ uuid: Uuid::new_v4(),
            lbas_per_zone: 0,
            lbas: 0,
            spacemap_space: 0
        };
        format!("{label:?}");
    }
}
}
// LCOV_EXCL_STOP
