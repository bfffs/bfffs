// vim: tw=80

use common::*;
use common::vdev::*;
use common::vdev_leaf::*;
use divbuf::DivBufShared;
use futures::{Future, future};
use nix;
use serde;
use serde_cbor;
use std::borrow::{Borrow, BorrowMut};
use std::io;
use std::io::Write;
use std::path::Path;
use tokio::reactor::Handle;
use tokio_file::File;
use uuid::Uuid;

/// The file magic is "ArkFS VdevFile\0\0"
const MAGIC: [u8; 16] = [0x41, 0x72, 0x6b, 0x46, 0x53, 0x20, 0x56, 0x64,
                         0x65, 0x76, 0x46, 0x69, 0x6c, 0x65, 0x00, 0x00];

#[derive(Serialize, Deserialize, Debug)]
struct LabelData {
    /// Vdev UUID, fixed at format time
    uuid:           Uuid,
    /// Number of LBAs per simulated zone
    lbas_per_zone:  LbaT,
    /// Number of LBAs that were present at format time
    lbas:           LbaT
}

#[derive(Serialize, Deserialize, Debug)]
struct Label {
    checksum:   [u8; 8],
    data:       LabelData
}

/// `VdevFile`: File-backed implementation of `VdevBlock`
///
/// This is used by the FUSE implementation of ArkFS.  It works with both
/// regular files and device files
///
#[derive(Debug)]
pub struct VdevFile {
    file:   File,
    handle: Handle,
    size:   LbaT,
    uuid:   Uuid
}

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
        if lba >= VdevFile::LABEL_LBAS {
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
            (VdevFile::LABEL_LBAS,
             u64::from(VdevFile::LABEL_LBAS) * VdevFile::LBAS_PER_ZONE)
        } else {
            (u64::from(zone) * VdevFile::LBAS_PER_ZONE,
             u64::from(zone + 1) * VdevFile::LBAS_PER_ZONE)
        }
    }

    fn zones(&self) -> ZoneT {
        div_roundup(self.size, VdevFile::LBAS_PER_ZONE) as ZoneT
    }
}

impl VdevLeaf for VdevFile {
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
        let off = lba as i64 * (BYTES_PER_LBA as i64);
        Box::new(self.file.read_at(container, off).unwrap().map(|_| ()))
    }

    fn readv_at(&self, buf: SGListMut, lba: LbaT) -> Box<VdevFut> {
        let off = lba as i64 * (BYTES_PER_LBA as i64);
        let containers = buf.into_iter().map(|iovec| {
            Box::new(IoVecMutContainer(iovec)) as Box<BorrowMut<[u8]>>
        }).collect();
        Box::new(self.file.readv_at(containers, off).unwrap().map(|lio_result| {
            // We must drain the iterator to free the AioCb resources
            lio_result.into_iter().map(|_| ()).count();
            ()
        }))
    }

    fn write_at(&mut self, buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        assert!(lba >= VdevFile::LABEL_LBAS, "Don't overwrite the label!");
        let container = Box::new(IoVecContainer(buf));
        self.write_at_unchecked(container, lba)
    }

    fn write_label(&mut self) -> Box<VdevFut> {
        let label_size = VdevFile::LABEL_LBAS as usize * BYTES_PER_LBA;
        let mut buf = Vec::with_capacity(label_size);
        let label = Label {
            checksum: [0, 0, 0, 0, 0, 0, 0, 0], //TODO
            data: LabelData {
                uuid: self.uuid,
                lbas_per_zone: VdevFile::LBAS_PER_ZONE,
                lbas: self.size
            }
        };
        buf.write_all(&MAGIC).unwrap();
        // ser::to_writer_packed would be more compact, but we're committed to
        // burning an LBA anyway
        serde_cbor::ser::to_writer(&mut buf, &label).unwrap();
        buf.resize(label_size, 0);
        self.write_at_unchecked(Box::new(buf), 0)
    }

    fn writev_at(&mut self, buf: SGList, lba: LbaT) -> Box<VdevFut> {
        assert!(lba >= VdevFile::LABEL_LBAS, "Don't overwrite the label!");
        let off = lba as i64 * (BYTES_PER_LBA as i64);
        let containers = buf.into_iter().map(|iovec| {
            Box::new(IoVecContainer(iovec)) as Box<Borrow<[u8]>>
        }).collect();
        Box::new(self.file.writev_at(containers, off).unwrap().map(|result| {
            // We must drain the iterator to free the AioCb resources
            result.into_iter().map(|_| ()).count();
            ()
        }))
    }
}

impl VdevFile {
    /// Size of a simulated zone
    const LBAS_PER_ZONE: LbaT = 1 << 16;  // 256 MB
    const LABEL_LBAS: LbaT = 1;

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
    /// * `path`    Pathname for the file.  It may be a device node.
    /// * `h`       Handle to the Tokio reactor that will be used to service
    ///             this vdev.
    pub fn open<P: AsRef<Path>>(path: P, h: Handle)
        -> Box<Future<Item=Self, Error=nix::Error>> {

        let f = File::open(path, h.clone()).unwrap();
        let size = f.metadata().unwrap().len() / BYTES_PER_LBA as u64;

        let label_size = VdevFile::LABEL_LBAS as usize * BYTES_PER_LBA;
        let dbs = DivBufShared::from(vec![0u8; label_size]);
        let dbm = dbs.try_mut().unwrap();
        let container = Box::new(IoVecMutContainer(dbm));
        Box::new(f.read_at(container, 0).unwrap()
            .and_then(move |aio_result| {
                drop(aio_result);   // release reference on dbs
                let db = dbs.try().unwrap();
                if &db[0..16] != MAGIC {
                    Err(nix::Error::Sys(nix::errno::Errno::EINVAL))
                } else {
                    // TODO: verify checksum
                    let mut deserializer = serde_cbor::Deserializer::from_slice(
                        &db[16..]);
                    let label: Label = serde::Deserialize::deserialize(
                        &mut deserializer).unwrap();
                    assert!(size >= label.data.lbas,
                               "Vdev has shrunk since creation");
                    Ok(VdevFile {
                        file: f,
                        handle: h,
                        size: label.data.lbas,
                        uuid: label.data.uuid
                    })
                }
            }))
    }

    fn write_at_unchecked(&mut self, buf: Box<Borrow<[u8]>>,
                          lba: LbaT) -> Box<VdevFut> {
        let off = lba as i64 * (BYTES_PER_LBA as i64);
        Box::new(self.file.write_at(buf, off).unwrap().map(|_| ()))
    }
}
