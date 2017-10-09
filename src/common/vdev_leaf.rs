// vim: tw=80
use common::vdev::*;

/// The public interface for all leaf Vdevs.  This is a low level thing.  Leaf
/// vdevs are typically files or disks, and this trait is their minimum common
/// interface.  I/O operations on `VdevLeaf` happen immediately; they are not
/// scheduled.
pub trait VdevLeaf : SGVdev {
}
