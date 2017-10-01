/// vim: tw=80

use std::io;
use std::rc::Rc;
use std::path::Path;
use tokio_core::reactor::Handle;
use tokio_file::{AioFut};

use common::*;
use common::vdev::*;
use common::zone_scheduler::*;

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

    /// Stores pending I/O operations and schedules them
    zone_scheduler: ZoneScheduler,

    /// Usable size of the vdev, in LBAs
    size:   LbaT
}

impl<T: VdevLeaf> VdevBlock<T> {
    /// Helper function for read and write methods
    fn check_io_bounds(&self, lba: LbaT, bufs: &[Rc<Box<[u8]>>]) {
        let len : u64 = bufs.iter().fold(0, |accumulator, buf| {
            accumulator + buf.len() as u64
        });
        assert!(lba + len / (dva::BYTES_PER_LBA as u64) < self.size as u64)
    }

    /// Open a VdevBlock
    ///
    /// * `leaf`    An already-open underlying VdevLeaf 
    fn open(leaf: T) -> Self {
        VdevBlock{ leaf: leaf,
                   zone_scheduler: ZoneScheduler::new(),
                   size:leaf.size()}
    }

    /// Helper function that reads a `BlockOp` popped off the scheduler
    fn read_blockop(&self, block_op: BlockOp) -> io::Result<AioFut<isize>>{
        let bufs = block_op.bufs;
        if bufs.len() == 1 {
            self.leaf.read_at(bufs.first().unwrap().clone(), block_op.lba)
        } else {
            self.leaf.readv_at(&bufs, block_op.lba)
        }
    }

    /// Helper function that writes a `BlockOp` popped off the scheduler
    fn write_blockop(&self, block_op: BlockOp) -> io::Result<AioFut<isize>>{
        let bufs = block_op.bufs;
        if bufs.len() == 1 {
            self.leaf.write_at(bufs.first().unwrap().clone(), block_op.lba)
        } else {
            self.leaf.writev_at(&bufs, block_op.lba)
        }
    }
}

impl<T: VdevLeaf> SGVdev for VdevBlock<T> {
    fn readv_at(&self, bufs: &[Rc<Box<[u8]>>],
                lba: LbaT) -> io::Result<AioFut<isize>> {
        self.check_io_bounds(lba, bufs);
        let (fut, iter) = self.zone_scheduler.readv_at(bufs, lba);
        for block_op in iter {
            self.read_blockop(block_op);
        }
        Ok(fut)
    }

    fn writev_at(&self, bufs: &[Rc<Box<[u8]>>], lba: LbaT) ->
        io::Result<AioFut<isize>> {
        self.check_io_bounds(lba, bufs);
        let (fut, iter) = self.zone_scheduler.writev_at(bufs, lba);
        for block_op in iter {
            self.write_blockop(block_op);
        }
        Ok(fut)
    }
}

impl<T: VdevLeaf> Vdev for VdevBlock<T> {
    fn lba2zone(&self, lba: LbaT) -> ZoneT {
        self.leaf.lba2zone(lba)
    }

    fn read_at(&self, buf: Rc<Box<[u8]>>,
                lba: LbaT) -> io::Result<AioFut<isize>> {
        self.check_io_bounds(lba, &[buf]);
        let (fut, iter) = self.zone_scheduler.read_at(buf, lba);
        for block_op in iter {
            self.read_blockop(block_op);
        }
        Ok(fut)
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn start_of_zone(&self, zone: ZoneT) -> LbaT {
        self.leaf.start_of_zone(zone)
    }

    fn write_at(&self, buf: Rc<Box<[u8]>>, lba: LbaT) ->
        io::Result<AioFut<isize>> {
        self.check_io_bounds(lba, &[buf]);
        let (fut, iter) = self.zone_scheduler.write_at(buf, lba);
        for block_op in iter {
            self.write_blockop(block_op);
        }
        Ok(fut)
    }
}
