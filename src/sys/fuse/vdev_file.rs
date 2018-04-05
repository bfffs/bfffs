// vim: tw=80

use futures::{Future, future};
use nix;
use std::borrow::{Borrow, BorrowMut};
use std::path::Path;
use tokio::reactor::Handle;
use tokio_file::File;

use common::*;
use common::vdev::*;
use common::vdev_leaf::*;


/// `VdevFile`: File-backed implementation of `VdevBlock`
///
/// This is used by the FUSE implementation of ArkFS.  It works with both
/// regular files and device files
///
#[derive(Debug)]
pub struct VdevFile {
    file:   File,
    handle: Handle,
    size:   LbaT
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
        Some((lba / (VdevFile::LBAS_PER_ZONE as u64)) as ZoneT)
    }

    fn optimum_queue_depth(&self) -> u32 {
        // The value `10` is just a total guess.
        10
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        (u64::from(zone) * VdevFile::LBAS_PER_ZONE,
         u64::from(zone + 1) * VdevFile::LBAS_PER_ZONE)
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
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
        Box::new(self.file.read_at(container, off).unwrap().map(|_| ()))
    }

    fn readv_at(&self, buf: SGListMut, lba: LbaT) -> Box<VdevFut> {
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
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
        let container = Box::new(IoVecContainer(buf));
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
        Box::new(self.file.write_at(container, off).unwrap().map(|_| ()))
    }

    fn writev_at(&mut self, buf: SGList, lba: LbaT) -> Box<VdevFut> {
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
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

    /// Open a file for use as a Vdev
    ///
    /// * `path`    Pathname for the file.  It may be a device node.
    /// * `h`       Handle to the Tokio reactor that will be used to service
    ///             this vdev.  
    pub fn open<P: AsRef<Path>>(path: P, h: Handle) -> Self {
        let f = File::open(path, h.clone()).unwrap();
        let size = f.metadata().unwrap().len() / dva::BYTES_PER_LBA as u64;
        VdevFile{file: f, handle: h, size}
    }
}
