// vim: tw=80

use std::rc::{Rc, Weak};
use tokio_core::reactor::Handle;

use common::*;
use common::vdev::*;
use common::vdev_leaf::*;
use common::zone_scheduler::*;
use common::zoned_device::*;

/// VdevBlock: Virtual Device for basic block device
///
/// This struct contains the functionality that is common between all types of
/// leaf vdev.
pub struct VdevBlock {
    /// Stores pending I/O operations and schedules them.  Reference counting it
    /// ensures that there are no in-flight operations for this Vdev when we
    /// tear it down.
    zone_scheduler: Rc<ZoneScheduler>,

    /// Usable size of the vdev, in LBAs
    size:   LbaT,

    // Needed so we can hand out Rc<Vdev> to `VdevFut`s
    selfref: Weak<VdevBlock>
}

impl VdevBlock {
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
    pub fn open<T: VdevLeaf>(leaf: Box<T>, handle: Handle) -> Self {
        let size = leaf.size();
        VdevBlock{ zone_scheduler: Rc::new(ZoneScheduler::new(leaf, handle)),
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

impl SGVdev for VdevBlock {
    fn readv_at(&self, bufs: SGList, lba: LbaT) -> VdevFut {
        self.check_sglist_bounds(lba, &bufs);
        let block_op = BlockOp::writev_at(bufs, lba);
        selfref = self.selfref.upgrade().unwrap().clone();
        VdevFut::new(selfref, block_op, false)
    }

    fn writev_at(&self, bufs: SGList, lba: LbaT) -> VdevFut {
        self.check_sglist_bounds(lba, &bufs);
        let block_op = BlockOp::writev_at(bufs, lba);
        selfref = self.selfref.upgrade().unwrap().clone();
        VdevFut::new(selfref, block_op, true)
    }
}

impl Vdev for VdevBlock {
    fn read_at(&self, buf: IoVec, lba: LbaT) -> VdevFut {
        self.check_iovec_bounds(lba, &buf);
        let block_op = BlockOp::read_at(buf, lba);
        selfref = self.selfref.upgrade().unwrap().clone();
        VdevFut::new(selfref, block_op, false)
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> VdevFut {
        self.check_iovec_bounds(lba, &buf);
        let block_op = BlockOp::write_at(buf, lba);
        selfref = self.selfref.upgrade().unwrap().clone();
        VdevFut::new(selfref, block_op, true)
    }
}

impl ZonedDevice for VdevBlock {
    fn lba2zone(&self, lba: LbaT) -> ZoneT {
        self.zone_scheduler.leaf.lba2zone(lba)
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn start_of_zone(&self, zone: ZoneT) -> LbaT {
        self.zone_scheduler.leaf.start_of_zone(zone)
    }
}
