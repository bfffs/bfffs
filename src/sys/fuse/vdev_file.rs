/// vim: tw=80

use std::io;
use std::path::Path;
use std::rc::Rc;
use tokio_core::reactor::Handle;
use tokio_file::{AioFut, File};

use common::*;
use common::vdev::*;

/// VdevFile: File-backed implementation of VdevBlock
///
/// This is used by the FUSE implementation of ArkFS.  It works with both
/// regular files and device files
///
pub struct VdevFile {
    file:   File,
    size:   LbaT
}

impl Vdev for VdevFile {
    fn lba2zone(&self, lba: LbaT) -> ZoneT {
        lba / self.LBAS_PER_ZONE;
    }

    fn read_at(&self, buf: Rc<Box<[u8]>>,
               lba: LbaT) -> io::Result<AioFut<isize>> {
        self.file.read_at(buf, lba * dva::BYTES_PER_LBA);
    }

    fn start_of_zone(&self, zone: ZoneT) -> LbaT {
        zone * self.LBAS_PER_ZONE;
    }

    fn write_at(&self, buf: Rc<Box<[u8]>>, lba: LbaT) ->
        io::Result<AioFut<isize>> {
        self.file.write_at(buf, lba * dva::BYTES_PER_LBA);
    }

    fn size(&self) -> LbaT {
        self.size
    }
}

impl VdevFile {
    /// Size of a simulated zone
    const LBAS_PER_ZONE: LbaT = 1 << 19;  // 256 MB

    /// Open a file for use as a Vdev
    ///
    /// * `path`    Pathname for the file.  It may be a device node.
    /// * `h`       Handle to the Tokio reactor that will be used to service
    ///             this vdev.  
    fn open<P: AsRef<Path>>(path: P, h: Handle) -> Self {
        let f = File::open(path, h);
        let size = f.metadata().unwrap().len() / dva::BYTES_PER_LBA;
        VdevFile{file: f, size: size}
    }
}

