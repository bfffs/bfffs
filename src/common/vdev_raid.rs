// vim: tw=80

use common::*;
use common::vdev::*;
use common::vdev_block::*;
use common::raid::*;
use common::declust::*;
use tokio_core::reactor::Handle;

#[cfg(test)]
/// Only exists so mockers can replace VdevBlock
pub trait VdevBlockTrait : SGVdev {
}
#[cfg(test)]
pub type VdevBlockLike = Box<VdevBlockTrait>;
#[cfg(not(test))]
pub type VdevBlockLike = VdevBlock;

/// VdevRaid: Virtual Device for the RAID transform
///
/// This Vdev implements the RAID I/O path, for all types of RAID encodings and
/// layout algorithms.
pub struct VdevRaid {
    /// Size of RAID chunks in LBAs
    ///
    /// A chunk, aka stripe unit, is the amount of data that will be written in
    /// a contiguous stretch to one disk before the `VdevRaid` switches to the
    /// next disk
    chunksize: LbaT,

    /// RAID codec
    codec: Codec,

    /// Locator, declustering or otherwise
    locator: Box<Locator>,

    /// Underlying block devices.  Order is important!
    #[cfg(not(test))]
    blockdevs: Box<[VdevBlockLike]>,

    #[cfg(test)]
    blockdevs: Box<[VdevBlockLike]>,
}

impl VdevRaid {
    pub fn new(chunksize: LbaT, codec: Codec, locator: Box<Locator>,
               blockdevs: Box<[VdevBlockLike]>) -> Self {
        for i in 1..blockdevs.len() {
            // All blockdevs must be the same size
            assert_eq!(blockdevs[0].size(), blockdevs[i].size());

            // All blockdevs must have the same zone boundaries
            // XXX this check assumes fixed-size zones
            assert_eq!(blockdevs[0].start_of_zone(1),
                       blockdevs[i].start_of_zone(1));
        }

        VdevRaid { chunksize: chunksize,
                   codec: codec,
                   locator: locator,
                   blockdevs: blockdevs}
    }
}

impl Vdev for VdevRaid {
    fn handle(&self) -> Handle {
        panic!("Unimplemented!  Perhaps handle() should not be part of Trait vdev, because it doesn't make sense for VdevRaid");
    }

    fn lba2zone(&self, lba: LbaT) -> ZoneT {
        let loc = self.locator.id2loc(ChunkId::Data(lba / self.chunksize));
        let disk_lba = loc.offset * self.chunksize;
        self.blockdevs[loc.disk as usize].lba2zone(disk_lba)
    }

    fn read_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        panic!("unimplemented!");
    }

    fn size(&self) -> LbaT {
        let disk_size_in_chunks = self.blockdevs[0].size() / self.chunksize;
        disk_size_in_chunks * self.locator.datachunks() *
            self.chunksize / (self.locator.depth() as LbaT)
    }

    fn start_of_zone(&self, zone: ZoneT) -> LbaT {
        // Zones don't necessarily line up with repetition boundaries.  So we
        // don't know the disk were a given zone begins.  Instead, we'll have to
        // search through every disk to find the one where the zone begins,
        // which will be disk that has the lowest LBA for that disk LBA.

        // All blockdevs must have the same zone map, so we only need to do the
        // start_of_zone call once.
        let disk_lba = self.blockdevs[0].start_of_zone(zone);
        (0..self.blockdevs.len()).map(|i| {
            let disk_chunk = disk_lba / self.chunksize;
            let cid = self.locator.loc2id(Chunkloc::new(i as i16, disk_chunk));
            match cid {
                ChunkId::Data(id) => id * self.chunksize,
                ChunkId::Parity(_, _) => LbaT::max_value()
            }
        }).min().unwrap()
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        panic!("unimplemented!");
    }
}

#[cfg(feature = "mocks")]
#[cfg(test)]
mod t {

use super::*;

mock!{
    MockVdevBlock,
    vdev,
    trait Vdev {
        fn handle(&self) -> Handle;
        fn lba2zone(&self, lba: LbaT) -> ZoneT;
        fn read_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
        fn size(&self) -> LbaT;
        fn start_of_zone(&self, zone: ZoneT) -> LbaT;
        fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
    },
    vdev,
    trait SGVdev  {
        fn readv_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
        fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
    },
    self,
    trait VdevBlockTrait{
    }
}

test_suite! {
    // A small layout that is a multiple of the zone size
    name small;

    use super::super::*;
    use super::MockVdevBlock;
    use super::super::super::prime_s::PrimeS;
    use mockers::Scenario;

    fixture!( mocks() -> (Scenario, VdevRaid) {
            setup(&mut self) {
            let scenario = Scenario::new();
            let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
            for _ in 0..5 {
                let mock = Box::new(scenario.create_mock::<MockVdevBlock>());
                scenario.expect(mock.size_call()
                                    .and_return_clone(262144)
                                    .times(..));  // 256k LBAs
                scenario.expect(mock.start_of_zone_call(1)
                                    .and_return_clone(65536)   // 64k LBAs/zone
                                    .times(..));

                blockdevs.push(mock);
            }

            let n = 5;
            let k = 4;
            let f = 1;

            let codec = Codec::new(k, f);
            let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
            let vdev_raid = VdevRaid::new(16, codec, locator,
                                          blockdevs.into_boxed_slice());
            (scenario, vdev_raid)
        }
    });

    test size0(mocks) {
        assert_eq!(mocks.val.1.size(), 983040);
    }
}

test_suite! {
    // A medium layout that is not a multiple of the zone size
    name medium;

    use super::super::*;
    use super::MockVdevBlock;
    use super::super::super::prime_s::PrimeS;
    use mockers::Scenario;

    fixture!( mocks() -> (Scenario, VdevRaid) {
            setup(&mut self) {
            let scenario = Scenario::new();
            let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
            for _ in 0..7 {
                let mock = Box::new(scenario.create_mock::<MockVdevBlock>());
                scenario.expect(mock.size_call()
                                    .and_return_clone(262144)
                                    .times(..));  // 256k LBAs
                scenario.expect(mock.start_of_zone_call(1)
                                    .and_return_clone(65536)   // 64k LBAs/zone
                                    .times(..));

                blockdevs.push(mock);
            }

            let n = 7;
            let k = 4;
            let f = 1;

            let codec = Codec::new(k, f);
            let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
            let vdev_raid = VdevRaid::new(16, codec, locator,
                                          blockdevs.into_boxed_slice());
            (scenario, vdev_raid)
        }
    });


    // test VdevRaid::size with a layout that's not a multiple of the zone size
    test size1(mocks) {
        assert_eq!(mocks.val.1.size(), 1376256);
    }
}

}
