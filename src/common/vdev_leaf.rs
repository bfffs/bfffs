// vim: tw=80
use common::*;
use common::zoned_device::*;
use std::io;
use tokio_core::reactor::Handle;
use tokio_file::AioFut;

/// The public interface for all leaf Vdevs.  This is a low level thing.  Leaf
/// vdevs are typically files or disks, and this trait is their minimum common
/// interface.  I/O operations on `VdevLeaf` happen immediately; they are not
/// scheduled.
pub trait VdevLeaf : ZonedDevice {
    fn handle(&self) -> Handle;
    fn read_at(&self, buf: IoVec, lba: LbaT) -> io::Result<AioFut<isize>>;
    fn write_at(&self, buf: IoVec, lba: LbaT) -> io::Result<AioFut<isize>>;
}
