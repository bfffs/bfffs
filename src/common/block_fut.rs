// vim: ts=80
use common::zone_scheduler::*;
use std::rc::Rc;

/// Future representing a block operation.  The return type is the amount of
/// data that was actually read/written, or an errno on error
#[must_use = "futures do nothing unless polled"]
pub struct BlockFut {
    zone_scheduler: Rc<ZoneScheduler>,
    block_op: BlockOp
}

impl BlockFut {
    pub fn new(zone_scheduler: Rc<ZoneScheduler>, block_op: BlockOp) -> BlockFut {
        BlockFut {zone_scheduler: zone_scheduler, block_op: block_op}
    }
}
