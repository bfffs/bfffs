// vim: ts=80
use common::zone_scheduler::*;
use futures;
use mio;
use nix;
use std::io;
use std::rc::Rc;
use tokio_core::reactor::{Handle, PollEvented};

struct BlockFutEvented {
    /// Used by `mio::Evented`
    registration: mio::Registration,

    /// Used by the `VdevLeaf` to complete this future
    promise: mio::SetReadiness
}

impl mio::Evented for BlockFutEvented {
    fn register(&self,
                poll: &mio::Poll,
                token: mio::Token,
                interest: mio::Ready,
                opts: mio::PollOpt) -> io::Result<()> {
        self.registration.register(poll, token, interest, opts)
    }

    fn reregister(&self,
                poll: &mio::Poll,
                token: mio::Token,
                interest: mio::Ready,
                opts: mio::PollOpt) -> io::Result<()> {
        self.registration.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &mio::Poll) -> io::Result<()> {
        self.registration.deregister(poll)
    }
}

/// Future representing a block operation.  The return type is the amount of
/// data that was actually read/written, or an errno on error
#[must_use = "futures do nothing unless polled"]
pub struct BlockFut {
    /// Link to the scheduler that should be used for this future.
    zone_scheduler: Rc<ZoneScheduler>,

    /// The associated `BlockOp`.  Whether it's a read or write will be clear
    /// from which `ZoneQueue` it's stored in.
    block_op: BlockOp,

    /// Has this I/O already been scheduled?
    scheduled: bool,

    /// Is this a read or a write operation?
    write: bool,

    // Used by the mio stuff
    io: PollEvented<BlockFutEvented>
}

impl BlockFut {
    pub fn new(zone_scheduler: Rc<ZoneScheduler>,
               block_op: BlockOp,
               write:bool) -> BlockFut {
        let (registration, promise) = mio::Registration::new2();
        let io = BlockFutEvented {registration: registration,
                                  promise: promise};
        let handle = zone_scheduler.handle.clone();
        BlockFut {zone_scheduler: zone_scheduler,
                  block_op: block_op,
                  scheduled: false, write: write,
                  io: PollEvented::new(io, &handle).unwrap()}
    }
}

impl futures::Future for BlockFut {
    type Item = isize;
    type Error = nix::Error;

    fn poll(&mut self) -> futures::Poll<isize, nix::Error> {
        if ! self.scheduled {
            // TODO: schedule it
            self.scheduled = true;
        }
        /// Arbitrary use Readable readiness for this type.  It doesn't matter
        /// what kind of readiness we use, so long as we're consistent.
        let poll_result = self.io.poll_read();
        if poll_result == futures::Async::NotReady {
            return Ok(futures::Async::NotReady);
        }
        Ok(futures::Async::Ready(42))  //TODO: get the real result somehow.
    }
}
