// vim: tw=80

use common::{*, label::*, vdev::*, vdev_leaf::*};
use divbuf::DivBufShared;
use futures::{Async, Future, Poll, future};
use std::{
    borrow::{Borrow, BorrowMut},
    io,
    num::NonZeroU64,
    path::Path
};
use tokio::reactor::Handle;
use tokio_file::{AioFut, File, LioFut};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    /// Vdev UUID, fixed at format time
    uuid:           Uuid,
    /// Number of LBAs per simulated zone
    lbas_per_zone:  LbaT,
    /// Number of LBAs that were present at format time
    lbas:           LbaT
}

/// `VdevFile`: File-backed implementation of `VdevBlock`
///
/// This is used by the FUSE implementation of BFFFS.  It works with both
/// regular files and device files
///
#[derive(Debug)]
pub struct VdevFile {
    file:   File,
    handle: Handle,
    /// Number of LBAs per simulated zone
    lbas_per_zone:  LbaT,
    size:   LbaT,
    uuid:   Uuid
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
        if lba >= LABEL_LBAS {
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

    fn sync_all(&self) -> Box<Future<Item = (), Error = Error>> {
        let fut = self.file.sync_all().unwrap()
            .map(|_| ())
            .map_err(|e| Error::from(e));
        Box::new(fut)
    }

    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        if zone == 0 {
            (LABEL_LBAS, self.lbas_per_zone)
        } else {
            (u64::from(zone) * self.lbas_per_zone,
             u64::from(zone + 1) * self.lbas_per_zone)
        }
    }

    fn zones(&self) -> ZoneT {
        div_roundup(self.size, self.lbas_per_zone) as ZoneT
    }
}

impl VdevLeafApi for VdevFile {
    fn erase_zone(&self, _lba: LbaT) -> Box<VdevFut> {
        // ordinary files don't have Zone operations
        Box::new(future::ok::<(), Error>(()))
    }

    fn finish_zone(&self, _lba: LbaT) -> Box<VdevFut> {
        // ordinary files don't have Zone operations
        Box::new(future::ok::<(), Error>(()))
    }

    fn open_zone(&self, _lba: LbaT) -> Box<VdevFut> {
        // ordinary files don't have Zone operations
        Box::new(future::ok::<(), Error>(()))
    }

    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut> {
        let container = Box::new(IoVecMutContainer(buf));
        let off = lba * (BYTES_PER_LBA as u64);
        let fut = VdevFileFut(self.file.read_at(container, off).unwrap());
        Box::new(fut)
    }

    fn readv_at(&self, buf: SGListMut, lba: LbaT) -> Box<VdevFut> {
        let off = lba * (BYTES_PER_LBA as u64);
        let containers = buf.into_iter().map(|iovec| {
            Box::new(IoVecMutContainer(iovec)) as Box<BorrowMut<[u8]>>
        }).collect();
        let fut = VdevFileLioFut(self.file.readv_at(containers, off).unwrap());
        Box::new(fut)
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        assert!(lba >= LABEL_LBAS, "Don't overwrite the label!");
        let container = Box::new(IoVecContainer(buf));
        Box::new(self.write_at_unchecked(container, lba))
    }

    fn write_label(&self, mut label_writer: LabelWriter) -> Box<VdevFut> {
        let label = Label {
            uuid: self.uuid,
            lbas_per_zone: self.lbas_per_zone,
            lbas: self.size
        };
        label_writer.serialize(label).unwrap();
        let sglist = label_writer.into_sglist();
        Box::new(self.writev_at(sglist, 0))
    }

    fn writev_at(&self, buf: SGList, lba: LbaT) -> Box<VdevFut> {
        let off = lba * (BYTES_PER_LBA as u64);
        let containers = buf.into_iter().map(|iovec| {
            Box::new(IoVecContainer(iovec)) as Box<Borrow<[u8]>>
        }).collect();
        let fut = VdevFileLioFut(self.file.writev_at(containers, off).unwrap());
        Box::new(fut)
    }
}

impl VdevFile {
    /// Size of a simulated zone
    const DEFAULT_LBAS_PER_ZONE: LbaT = 1 << 16;  // 256 MB

    /// Create a new Vdev, backed by a file
    ///
    /// * `path`:           Pathname for the file.  It may be a device node.
    /// * `lbas_per_zone`:  If specified, this many LBAs will be assigned to
    ///                     simulated zones on devices that don't have native
    ///                     zones.
    pub fn create<P: AsRef<Path>>(path: P, lbas_per_zone: Option<NonZeroU64>)
        -> io::Result<Self>
    {
        let handle = Handle::current();
        let f = File::open(path, handle.clone())?;
        let lpz = match lbas_per_zone {
            None => VdevFile::DEFAULT_LBAS_PER_ZONE,
            Some(x) => x.get()
        };
        let size = f.metadata().unwrap().len() / BYTES_PER_LBA as u64;
        let uuid = Uuid::new_v4();
        Ok(VdevFile{file: f, handle, lbas_per_zone: lpz, size, uuid})
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
        let handle = Handle::current();
        let f = File::open(path, handle.clone()).unwrap();
        let size = f.metadata().unwrap().len() / BYTES_PER_LBA as u64;

        let dbs = DivBufShared::from(vec![0u8; LABEL_SIZE]);
        let dbm = dbs.try_mut().unwrap();
        let container = Box::new(IoVecMutContainer(dbm));
        f.read_at(container, 0).unwrap()
         .map_err(|e| Error::from(e))
         .and_then(move |aio_result| {
            drop(aio_result);   // release reference on dbs
            LabelReader::from_dbs(dbs).and_then(|mut label_reader| {
                let label: Label = label_reader.deserialize().unwrap();
                assert!(size >= label.lbas,
                           "Vdev has shrunk since creation");
                let vdev = VdevFile {
                    file: f,
                    handle,
                    lbas_per_zone: label.lbas_per_zone,
                    size: label.lbas,
                    uuid: label.uuid
                };
                Ok((vdev, label_reader))
            })
        })
    }

    fn write_at_unchecked(&self, buf: Box<Borrow<[u8]>>, lba: LbaT)
        -> impl Future<Item = (), Error = Error>
    {
        let off = lba * (BYTES_PER_LBA as u64);
        VdevFileFut(self.file.write_at(buf, off).unwrap())
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
            Ok(Async::Ready(lio_result)) => {
                // We must drain the iterator to free the AioCb resources
                lio_result.into_iter().map(|_| ()).count();
                Ok(Async::Ready(()))
            },
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(Error::from(e))
        }
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {

mod label {
    use super::super::*;

    // pet kcov
    #[test]
    fn debug() {
        let label = Label{ uuid: Uuid::new_v4(),
            lbas_per_zone: 0,
            lbas: 0
        };
        format!("{:?}", label);
    }
}

}
// LCOV_EXCL_STOP
