// vim: tw=80

use futures::Future;
use nix;
use std::borrow::{Borrow, BorrowMut};
use std::io;
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

/// Tokio-File requires boxed DivBufs, but the upper layers of ArkFS don't.
/// Take care of the mismatch here, but wrapping DivBuf in a new struct
struct IoVecContainer(IoVec);
impl Borrow<[u8]> for IoVecContainer {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// Tokio-File requires boxed DivBufMuts, but the upper layers of ArkFS don't.
/// Take care of the mismatch here, but wrapping DivBufMut in a new struct
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

impl SGVdev for VdevFile {
    fn readv_at(&self, buf: SGListMut, lba: LbaT) -> Box<SGListFut> {
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
        let containers = buf.into_iter().map(|iovec| {
            Box::new(IoVecMutContainer(iovec)) as Box<BorrowMut<[u8]>>
        }).collect();
        Box::new(self.file.readv_at(containers, off).unwrap().map(|r| {
            let v = r.into_iter().map(|x| x.value.unwrap()).sum();
            SGListResult{value: v}
        }).map_err(|e| {
            match e {
                nix::Error::Sys(x) => io::Error::from(x),
                _ => panic!("Unhandled error type")
            }})
        )

    }

    fn writev_at(&mut self, buf: SGList, lba: LbaT) -> Box<SGListFut> {
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
        let containers = buf.into_iter().map(|iovec| {
            Box::new(IoVecContainer(iovec)) as Box<Borrow<[u8]>>
        }).collect();
        Box::new(self.file.writev_at(containers, off).unwrap().map(|r| {
            let v = r.into_iter().map(|x| x.value.unwrap()).sum();
            SGListResult{value: v}
        }).map_err(|e| {
            match e {
                nix::Error::Sys(x) => io::Error::from(x),
                _ => panic!("Unhandled error type")
            }})
        )
    }
}

impl Vdev for VdevFile {
    fn handle(&self) -> Handle {
        self.handle.clone()
    }

    fn lba2zone(&self, lba: LbaT) -> ZoneT {
        (lba / (VdevFile::LBAS_PER_ZONE as u64)) as ZoneT
    }

    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<IoVecFut> {
        let container = Box::new(IoVecMutContainer(buf));
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
        Box::new(self.file.read_at(container, off).unwrap().map(|aio_result| {
            IoVecResult{value: aio_result.value.unwrap()}
        }).map_err(|e| {
            match e {
                nix::Error::Sys(x) => io::Error::from(x),
                _ => panic!("Unhandled error type")
            }
        }))
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn start_of_zone(&self, zone: ZoneT) -> LbaT {
        zone as u64 * VdevFile::LBAS_PER_ZONE
    }

    fn write_at(&mut self, buf: IoVec, lba: LbaT) -> Box<IoVecFut> {
        let container = Box::new(IoVecContainer(buf));
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
        Box::new(self.file.write_at(container, off).unwrap().map(|aio_result| {
            IoVecResult { value: aio_result.value.unwrap() }
        }).map_err(|e| {
            match e {
                nix::Error::Sys(x) => io::Error::from(x),
                _ => panic!("Unhandled error type")
            }})
        )
    }
}

impl VdevLeaf for VdevFile {
}

impl VdevFile {
    /// Size of a simulated zone
    const LBAS_PER_ZONE: LbaT = 1 << 19;  // 256 MB

    /// Open a file for use as a Vdev
    ///
    /// * `path`    Pathname for the file.  It may be a device node.
    /// * `h`       Handle to the Tokio reactor that will be used to service
    ///             this vdev.  
    pub fn open<P: AsRef<Path>>(path: P, h: Handle) -> Self {
        let f = File::open(path, h.clone()).unwrap();
        let size = f.metadata().unwrap().len() / dva::BYTES_PER_LBA as u64;
        VdevFile{file: f, handle: h, size: size}
    }
}
