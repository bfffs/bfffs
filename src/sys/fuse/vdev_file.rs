// vim: tw=80

use common::{*, label::*, vdev::*, vdev_leaf::*};
use divbuf::DivBufShared;
use futures::{Future, future};
use nix;
use std::{ borrow::{Borrow, BorrowMut}, io, path::Path };
use tokio::reactor::Handle;
use tokio_file::File;
use uuid::Uuid;

// LCOV_EXCL_START
#[derive(Serialize, Deserialize, Debug)]
struct Label {
    /// Vdev UUID, fixed at format time
    uuid:           Uuid,
    /// Number of LBAs per simulated zone
    lbas_per_zone:  LbaT,
    /// Number of LBAs that were present at format time
    lbas:           LbaT
}
// LCOV_EXCL_STOP

/// `VdevFile`: File-backed implementation of `VdevBlock`
///
/// This is used by the FUSE implementation of ArkFS.  It works with both
/// regular files and device files
///
// LCOV_EXCL_START
#[derive(Debug)]
pub struct VdevFile {
    file:   File,
    handle: Handle,
    size:   LbaT,
    uuid:   Uuid
}
// LCOV_EXCL_STOP

/// Tokio-File requires boxed `DivBufs`, but the upper layers of ArkFS don't.
/// Take care of the mismatch here, by wrapping `DivBuf` in a new struct
struct IoVecContainer(IoVec);
impl Borrow<[u8]> for IoVecContainer {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// Tokio-File requires boxed `DivBufMuts`, but the upper layers of ArkFS don't.
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
    fn handle(&self) -> Handle {
        self.handle.clone()
    }

    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        if lba >= LABEL_LBAS {
            Some((lba / (VdevFile::LBAS_PER_ZONE as u64)) as ZoneT)
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

    fn sync_all(&self) -> Box<Future<Item = (), Error = nix::Error>> {
        Box::new(self.file.sync_all().unwrap().map(|_| ()))
    }

    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        if zone == 0 {
            (LABEL_LBAS, VdevFile::LBAS_PER_ZONE)
        } else {
            (u64::from(zone) * VdevFile::LBAS_PER_ZONE,
             u64::from(zone + 1) * VdevFile::LBAS_PER_ZONE)
        }
    }

    fn zones(&self) -> ZoneT {
        div_roundup(self.size, VdevFile::LBAS_PER_ZONE) as ZoneT
    }
}

impl VdevLeafApi for VdevFile {
    fn erase_zone(&self, _lba: LbaT) -> Box<VdevFut> {
        // ordinary files don't have Zone operations
        Box::new(future::ok::<(), nix::Error>(()))
    }

    fn finish_zone(&self, _lba: LbaT) -> Box<VdevFut> {
        // ordinary files don't have Zone operations
        Box::new(future::ok::<(), nix::Error>(()))
    }

    fn open_zone(&self, _lba: LbaT) -> Box<VdevFut> {
        // ordinary files don't have Zone operations
        Box::new(future::ok::<(), nix::Error>(()))
    }

    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut> {
        let container = Box::new(IoVecMutContainer(buf));
        let off = lba * (BYTES_PER_LBA as u64);
        Box::new(self.file.read_at(container, off).unwrap().map(|_| ()))
    }

    fn readv_at(&self, buf: SGListMut, lba: LbaT) -> Box<VdevFut> {
        let off = lba * (BYTES_PER_LBA as u64);
        let containers = buf.into_iter().map(|iovec| {
            Box::new(IoVecMutContainer(iovec)) as Box<BorrowMut<[u8]>>
        }).collect();
        Box::new(self.file.readv_at(containers, off).unwrap().map(|lio_result| {
            // We must drain the iterator to free the AioCb resources
            lio_result.into_iter().map(|_| ()).count();
            ()
        }))
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        assert!(lba >= LABEL_LBAS, "Don't overwrite the label!");
        let container = Box::new(IoVecContainer(buf));
        Box::new(self.write_at_unchecked(container, lba))
    }

    fn write_label(&self, mut label_writer: LabelWriter) -> Box<VdevFut> {
        let label = Label {
            uuid: self.uuid,
            lbas_per_zone: VdevFile::LBAS_PER_ZONE,
            lbas: self.size
        };
        let dbs = label_writer.serialize(label).unwrap();
        let (sglist, keeper) = label_writer.into_sglist();
        Box::new(self.writev_at(sglist, 0).then(move |r| {
            let _ = dbs;
            let _ = keeper;
            r
        }))
    }

    fn writev_at(&self, buf: SGList, lba: LbaT) -> Box<VdevFut> {
        let off = lba * (BYTES_PER_LBA as u64);
        let containers = buf.into_iter().map(|iovec| {
            Box::new(IoVecContainer(iovec)) as Box<Borrow<[u8]>>
        }).collect();
        Box::new(self.file.writev_at(containers, off).unwrap().map(|result| {
            result.into_iter().map(|_| ()).count();
            ()
        }))
    }
}

impl VdevFile {
    /// Size of a simulated zone
    const LBAS_PER_ZONE: LbaT = 1 << 16;  // 256 MB

    /// Create a new Vdev, backed by a file
    ///
    /// * `path`    Pathname for the file.  It may be a device node.
    /// * `h`       Handle to the Tokio reactor that will be used to service
    ///             this vdev.
    pub fn create<P: AsRef<Path>>(path: P, h: Handle) -> io::Result<Self> {
        let f = File::open(path, h.clone())?;
        let size = f.metadata().unwrap().len() / BYTES_PER_LBA as u64;
        let uuid = Uuid::new_v4();
        Ok(VdevFile{file: f, handle: h, size, uuid})
    }

    /// Open an existing `VdevFile`
    ///
    /// Returns both a new `VdevFile` object, and a `LabelReader` that may be
    /// used to construct other vdevs stacked on top of this one.
    ///
    /// * `path`    Pathname for the file.  It may be a device node.
    /// * `h`       Handle to the Tokio reactor that will be used to service
    ///             this vdev.
    pub fn open<P: AsRef<Path>>(path: P, h: Handle)
        -> impl Future<Item=(Self, LabelReader), Error=nix::Error> {

        let f = File::open(path, h.clone()).unwrap();
        let size = f.metadata().unwrap().len() / BYTES_PER_LBA as u64;

        let dbs = DivBufShared::from(vec![0u8; LABEL_SIZE]);
        let dbm = dbs.try_mut().unwrap();
        let container = Box::new(IoVecMutContainer(dbm));
        f.read_at(container, 0).unwrap()
         .and_then(move |aio_result| {
            drop(aio_result);   // release reference on dbs
            LabelReader::from_dbs(dbs).and_then(|mut label_reader| {
                let label: Label = label_reader.deserialize().unwrap();
                assert!(size >= label.lbas,
                           "Vdev has shrunk since creation");
                let vdev = VdevFile {
                    file: f,
                    handle: h,
                    size: label.lbas,
                    uuid: label.uuid
                };
                Ok((vdev, label_reader))
            })
        })
    }

    fn write_at_unchecked(&self, buf: Box<Borrow<[u8]>>,
                          lba: LbaT) -> impl Future<Item = (), Error = nix::Error> {
        let off = lba * (BYTES_PER_LBA as u64);
        self.file.write_at(buf, off).unwrap().map(|_| ())
    }
}
