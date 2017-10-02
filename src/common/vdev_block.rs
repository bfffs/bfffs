// vim: tw=80

use std::rc::Rc;
use tokio_core::reactor::Handle;

use common::*;
use common::block_fut::*;
use common::vdev::*;
use common::zone_scheduler::*;
use common::zoned_device::*;

/// The public interface for all leaf Vdevs.  This is a low level thing.  Leaf
/// vdevs are typically files or disks, and this trait is their minimum common
/// interface.  I/O operations on `VdevLeaf` happen immediately; they are not
/// scheduled.
pub trait VdevLeaf : SGVdev {
}

/// VdevBlock: Virtual Device for basic block device
///
/// This struct contains the functionality that is common between all types of
/// leaf vdev.
pub struct VdevBlock<T> {
    /// Underlying device
    leaf: T,

    /// Stores pending I/O operations and schedules them.  Reference counting it
    /// ensures that there are no in-flight operations for this Vdev when we
    /// tear it down.
    zone_scheduler: Rc<ZoneScheduler>,

    /// Usable size of the vdev, in LBAs
    size:   LbaT
}

impl<T: VdevLeaf> VdevBlock<T> {
    /// Helper function for read and write methods
    fn check_iovec_bounds(&self, lba: LbaT, buf: &IoVec) {
        let buflen = buf.len() as u64;
        let last_lba : LbaT = lba + buflen / (dva::BYTES_PER_LBA as u64);
        assert!(last_lba < self.size as u64)
    }

    /// Helper function for read and write methods
    fn check_sglist_bounds(&self, lba: LbaT, bufs: &SGList) {
        let len : u64 = bufs.iter().fold(0, |accumulator, buf| {
            accumulator + buf.len() as u64
        });
        assert!(lba + len / (dva::BYTES_PER_LBA as u64) < self.size as u64)
    }

    /// Open a VdevBlock
    ///
    /// * `leaf`    An already-open underlying VdevLeaf 
    pub fn open(leaf: T) -> Self {
        let size = leaf.size();
        VdevBlock{ leaf: leaf,
                   zone_scheduler: Rc::new(ZoneScheduler::new()),
                   size: size}
    }

    ///// Helper function that reads a `BlockOp` popped off the scheduler
    //fn read_blockop(&self, block_op: BlockOp) -> io::Result<AioFut<isize>>{
        //let bufs = block_op.bufs;
        //if bufs.len() == 1 {
            //self.leaf.read_at(bufs.first().unwrap().clone(), block_op.lba)
        //} else {
            //self.leaf.readv_at(bufs, block_op.lba)
        //}
    //}

    ///// Helper function that writes a `BlockOp` popped off the scheduler
    //fn write_blockop(&self, block_op: BlockOp) -> io::Result<AioFut<isize>>{
        //let bufs = block_op.bufs;
        //if bufs.len() == 1 {
            //self.leaf.write_at(bufs.first().unwrap().clone(), block_op.lba)
        //} else {
            //self.leaf.writev_at(bufs, block_op.lba)
        //}
    //}
}

impl<T: VdevLeaf> SGVdev for VdevBlock<T> {
    fn readv_at(&self, bufs: SGList, lba: LbaT) -> BlockFut {
        self.check_sglist_bounds(lba, &bufs);
        let block_op = BlockOp::writev_at(bufs, lba);
        BlockFut::new(self.zone_scheduler.clone(), block_op)
    }

    fn writev_at(&self, bufs: SGList, lba: LbaT) -> BlockFut {
        self.check_sglist_bounds(lba, &bufs);
        let block_op = BlockOp::writev_at(bufs, lba);
        BlockFut::new(self.zone_scheduler.clone(), block_op)
    }
}

impl<T: VdevLeaf> Vdev for VdevBlock<T> {
    fn read_at(&self, buf: IoVec, lba: LbaT) -> BlockFut {
        self.check_iovec_bounds(lba, &buf);
        let block_op = BlockOp::read_at(buf, lba);
        BlockFut::new(self.zone_scheduler.clone(), block_op)
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> BlockFut {
        self.check_iovec_bounds(lba, &buf);
        let block_op = BlockOp::write_at(buf, lba);
        BlockFut::new(self.zone_scheduler.clone(), block_op)
    }
}

impl<T: VdevLeaf> ZonedDevice for VdevBlock<T> {
    fn lba2zone(&self, lba: LbaT) -> ZoneT {
        self.leaf.lba2zone(lba)
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn start_of_zone(&self, zone: ZoneT) -> LbaT {
        self.leaf.start_of_zone(zone)
    }

}
