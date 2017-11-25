// vim: tw=80

use common::*;
use common::dva::*;
use common::vdev::*;
#[cfg(not(test))]
use common::vdev_block::*;
use common::raid::*;
use common::declust::*;
use modulo::Mod;
use std::mem;
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
        assert_eq!(blockdevs.len(), locator.clustsize() as usize,
            "mismatched cluster size");
        assert_eq!(codec.stripesize(), locator.stripesize(),
            "mismatched stripe size");
        assert_eq!(codec.protection(), locator.protection(),
            "mismatched protection level");
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

    fn read_at(&self, _buf: IoVec, _lba: LbaT) -> Box<VdevFut> {
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
        let col_len = self.chunksize as usize * BYTES_PER_LBA as usize;
        let f = self.codec.protection() as usize;
        let m = self.codec.stripesize() as usize - f as usize;
        assert_eq!(buf.len(),
                   col_len * m,
                   "Only single-stripe writes are currently supported");
        assert_eq!(lba.modulo(self.chunksize as u64 * m as u64), 0,
            "Unaligned writes are not yet supported");

        let data = Vec::<IoVec>::with_capacity(m);
        let data_refs = Vec::<*const u8>::with_capacity(m);
        for i in 0..m {
            let b = col_len * m;
            let e = b + col_len;
            let col : Rc<Box<[u8]>> = Rc::new( unsafe {
                mem::transmute(buf[b..e].as_ptr())
            });
            //let col = Rc::new(buf[b..e]);
            data.push(col);
            data_refs.push(col.as_ptr());
        }

        let parity = Vec::<IoVec>::with_capacity(f);
        let parity_refs = Vec::<*mut u8>::with_capacity(f);
        for _ in 0..f {
            let v = Vec::<u8>::with_capacity(col_len);
            //codec::encode will actually fill the column
            unsafe { v.set_len(col_len) };
            let col = Rc::new(v.into_boxed_slice());
            parity.push(col);
            parity_refs.push(col.as_mut_ptr());
        }

        self.codec.encode(col_len, &data_refs, &(parity_refs.into_boxed_slice()));

        // Split buf into m subuffers
        // Allocate f additional buffers
        // encode
        // issue
        panic!("unimplemented!");
    }
}

#[cfg(feature = "mocks")]
#[cfg(test)]
mod t {

use super::*;
use super::super::prime_s::PrimeS;
use super::super::dva::*;
use futures::future;
use mockers::{matchers, Scenario};
use std::io::Error;

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

#[test]
#[should_panic(expected="mismatched cluster size")]
fn mismatched_clustsize() {
        let n = 7;
        let k = 4;
        let f = 1;

        let blockdevs = Vec::<Box<VdevBlockTrait>>::new();
        let codec = Codec::new(k, f);
        let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
        VdevRaid::new(16, codec, locator, blockdevs.into_boxed_slice());
}

#[test]
#[should_panic(expected="mismatched stripe size")]
fn mismatched_stripesize() {
        let n = 7;
        let k = 4;
        let f = 1;

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
        for _ in 0..n {
            let mock = Box::new(s.create_mock::<MockVdevBlock>());
            blockdevs.push(mock);
        }
        let codec = Codec::new(5, f);
        let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
        VdevRaid::new(16, codec, locator, blockdevs.into_boxed_slice());
}

#[test]
#[should_panic(expected="mismatched protection level")]
fn mismatched_protection() {
        let n = 7;
        let k = 4;
        let f = 1;

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
        for _ in 0..n {
            let mock = Box::new(s.create_mock::<MockVdevBlock>());
            blockdevs.push(mock);
        }
        let codec = Codec::new(k, f);
        let locator = Box::new(PrimeS::new(n, k as i16, 2));
        VdevRaid::new(16, codec, locator, blockdevs.into_boxed_slice());
}

test_suite! {
    // A small layout that is a multiple of the zone size
    name small;

    use super::super::*;
    use super::MockVdevBlock;
    use super::super::super::prime_s::PrimeS;
    use mockers::{matchers, Scenario};

    fixture!( mocks() -> (Scenario, VdevRaid) {
            setup(&mut self) {
            let s = Scenario::new();
            let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
            for _ in 0..5 {
                let mock = Box::new(s.create_mock::<MockVdevBlock>());
                s.expect(mock.size_call()
                                    .and_return_clone(262144)
                                    .times(..));  // 256k LBAs
                s.expect(mock.lba2zone_call(matchers::lt(65536))
                                    .and_return_clone(0)
                                    .times(..));
                s.expect(mock.lba2zone_call(matchers::in_range(65536..131072))
                                    .and_return_clone(1)
                                    .times(..));
                s.expect(mock.start_of_zone_call(0)
                                    .and_return_clone(0)
                                    .times(..));
                s.expect(mock.start_of_zone_call(1)
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
            (s, vdev_raid)
        }
    });

    test lba2zone(mocks) {
        assert_eq!(mocks.val.1.lba2zone(0), 0);
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(245759), 0);
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(245760), 1);
    }

    test size(mocks) {
        assert_eq!(mocks.val.1.size(), 983040);
    }

    test start_of_zone(mocks) {
        assert_eq!(mocks.val.1.start_of_zone(0), 0);
        assert_eq!(mocks.val.1.start_of_zone(1), 245760);
    }
}

test_suite! {
    // A medium layout that is not a multiple of the zone size
    name medium;

    use super::super::*;
    use super::MockVdevBlock;
    use super::super::super::prime_s::PrimeS;
    use mockers::{matchers, Scenario};

    fixture!( mocks() -> (Scenario, VdevRaid) {
            setup(&mut self) {
            let s = Scenario::new();
            let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
            for _ in 0..7 {
                let mock = Box::new(s.create_mock::<MockVdevBlock>());
                s.expect(mock.size_call()
                                    .and_return_clone(262144)
                                    .times(..));  // 256k LBAs
                s.expect(mock.lba2zone_call(matchers::lt(65536))
                                    .and_return_clone(0)
                                    .times(..));
                s.expect(mock.lba2zone_call(matchers::in_range(65536..131072))
                                    .and_return_clone(1)
                                    .times(..));
                s.expect(mock.start_of_zone_call(0)
                                    .and_return_clone(0)
                                    .times(..));
                s.expect(mock.start_of_zone_call(1)
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
            (s, vdev_raid)
        }
    });

    test lba2zone(mocks) {
        assert_eq!(mocks.val.1.lba2zone(0), 0);
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(344063), 0);
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(344064), 1);
    }

    test size(mocks) {
        assert_eq!(mocks.val.1.size(), 1376256);
    }

    test start_of_zone(mocks) {
        assert_eq!(mocks.val.1.start_of_zone(0), 0);
        assert_eq!(mocks.val.1.start_of_zone(1), 344064);
    }
}

#[test]
fn write_at_one_stripe() {
        let n = 5;
        let k = 4;
        let f = 1;
        const CHUNKSIZE : LbaT = 2;

        // TODO: verify offset of buffers
        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
        let m0 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m0.size_call().and_return_clone(262144).times(..));
        s.expect(m0.start_of_zone_call(1).and_return_clone(65536).times(..));
        s.expect(m0.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA as usize
        }), matchers::ANY)
            .and_return(Box::new(future::ok::<isize, Error>((0)))));
        blockdevs.push(m0);
        let m1 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m1.size_call().and_return_clone(262144).times(..));
        s.expect(m1.start_of_zone_call(1).and_return_clone(65536).times(..));
        s.expect(m1.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA as usize
        }), matchers::ANY)
            .and_return(Box::new(future::ok::<isize, Error>((0)))));
        blockdevs.push(m1);
        let m2 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m2.size_call().and_return_clone(262144).times(..));
        s.expect(m2.start_of_zone_call(1).and_return_clone(65536).times(..));
        s.expect(m2.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA as usize
        }), matchers::ANY)
            .and_return(Box::new(future::ok::<isize, Error>((0)))));
        blockdevs.push(m2);
        let m3 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m3.size_call().and_return_clone(262144).times(..));
        s.expect(m3.start_of_zone_call(1).and_return_clone(65536).times(..));
        s.expect(m3.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA as usize
        }), matchers::ANY)
            .and_return(Box::new(future::ok::<isize, Error>((0)))));
        blockdevs.push(m3);
        let m4 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m4.size_call().and_return_clone(262144).times(..));
        s.expect(m4.start_of_zone_call(1).and_return_clone(65536).times(..));
        s.expect(m4.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA as usize
        }), matchers::ANY)
            .and_return(Box::new(future::ok::<isize, Error>((0)))));
        blockdevs.push(m4);

        let codec = Codec::new(k, f);
        let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
        let vdev_raid = VdevRaid::new(CHUNKSIZE, codec, locator,
                                      blockdevs.into_boxed_slice());
        let wbuf = Rc::new(vec![0u8; 24576].into_boxed_slice());
        vdev_raid.write_at(wbuf, 0);
}

}
