// vim: tw=80

//! The Null Declustering Layout
//!
//! This is a trivial layout that uses a single device.

use common::declust::*;

// LCOV_EXCL_START
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NullRaid {
}
// LCOV_EXCL_STOP

impl NullRaid {
    /// Create a new NullRaid Locator
    ///
    /// # Parameters
    ///
    /// `num_disks`:        Total number of disks in the array
    /// `disks_per_stripe`: Number of disks in each parity group
    /// `redundancy`:       Redundancy level of the RAID array.  This many disks
    ///                     may fail before the data becomes irrecoverable.
    pub fn new(num_disks: i16, disks_per_stripe: i16, redundancy: i16) -> Self {
        assert!(num_disks == 1);
        assert!(disks_per_stripe == 1);
        assert!(redundancy == 0);
        NullRaid{}
    }
}

impl Locator for NullRaid {
    fn clustsize(&self) -> i16 {
        1
    }

    fn datachunks(&self) -> u64 {
        1
    }

    fn depth(&self) -> i16 {
        1
    }

    fn iter(&self, start: ChunkId, end: ChunkId)
        -> Box<Iterator<Item=(ChunkId, Chunkloc)>> {

        let e = match end {
            ChunkId::Data(a) => a,
            ChunkId::Parity(a, _) => a + 1
        };
        Box::new((start.address()..e).map(|a| {
            (ChunkId::Data(a), Chunkloc {disk: 0, offset: a})
        }))
    }

    fn iter_data(&self, start: ChunkId, end: ChunkId)
        -> Box<Iterator<Item=(ChunkId, Chunkloc)>> {

        self.iter(start, end)
    }

    fn loc2id(&self, loc: Chunkloc) -> ChunkId {
        ChunkId::Data(loc.offset)
    }

    fn id2loc(&self, id: ChunkId) -> Chunkloc {
        debug_assert!(id.is_data());
        Chunkloc {disk: 0, offset: id.address()}
    }

    fn parallel_read_count(&self, _consecutive_data_chunks: usize) -> usize {
        1
    }

    fn protection(&self) -> i16 {
        0
    }

    fn stripes(&self) -> u32 {
        1
    }

    fn stripesize(&self) -> i16 {
        1
    }
}
