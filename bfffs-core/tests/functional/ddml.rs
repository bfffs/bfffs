// vim: tw=80
use bfffs_core::{
    cache::*,
    dml::*,
    ddml::*,
    label::LabelWriter,
    TxgT,
    BYTES_PER_LBA,
    LbaT
};
use divbuf::{DivBuf, DivBufShared};
use futures::TryFutureExt;
use pretty_assertions::assert_eq;
use rstest::{fixture, rstest};
use std::{
    fs,
    io::Read,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{Arc, Mutex}
};

#[fixture]
fn ddml() -> DDML {
    let ph = crate::PoolBuilder::new()
        .chunksize(1)
        .build();
    let cache = Cache::with_capacity(1_000_000_000);
    DDML::create(ph.pool, Arc::new(Mutex::new(cache)))
}

/// The DDML can reconstruct a faulted mirrored disk.
#[rstest]
#[test_log::test(tokio::test)]
async fn repair_mirror(
        #[values(
            // non-raid
            (1, 1, 0),
            // Simplest sensible RAID configuration
            (3, 3, 1),
        )]
        raid_config: (usize, i16, i16),
    )
{
    let zone_size = 32;
    let (n, k, f) = raid_config;
    let ph = crate::PoolBuilder::new()
        .mirror_size(2)
        .disks(2 * n)
        .redundancy_level(f)
        .stripe_size(k)
        .fsize(1 << 20)     // 1 MB
        .zone_size(zone_size)
        .build();
    let paths = ph.paths.clone();
    let pool_uuid = ph.pool.uuid();
    let cache = Arc::new(Mutex::new(Cache::with_capacity(1_000_000_000)));
    let ddml = DDML::create(ph.pool, cache.clone());

    // Write initial label to all disks
    let txg = TxgT::from(1);
    ddml.write_label(LabelWriter::new(0), txg).await.unwrap();

    // Fault one disk
    let stat = ddml.status().await;
    let leaf_uuid = stat.clusters[0].mirrors[0].leaves[0].uuid;
    ddml.fault(leaf_uuid).await.unwrap();

    // Write multiple zones' worth of data.  Leave one zone only partially
    // written.
    let txg2 = txg + 1;
    let mut drps = Vec::new();
    for i in 0..(zone_size * 3 + zone_size / 4) {
        let dbs = DivBufShared::from(vec![i as u8; BYTES_PER_LBA]);
        let drp = ddml.put(dbs, Compression::None, txg2).await.unwrap();
        drps.push(drp);
    }

    // Drop the pool and reimport it
    let txg3 = txg2 + 1;
    ddml.flush(0).await.unwrap();
    ddml.write_label(LabelWriter::new(0), txg3).await.unwrap();
    drop(ddml);

    let mut manager = bfffs_core::pool::Manager::default();
    for path in paths.iter() {
        manager.taste(path).await.unwrap();
    }
    let (pool, _) = manager.import(pool_uuid).await.unwrap();

    // Create new DDML
    let ddml = DDML::open(pool, cache);

    // Wait for the mirror repair task to finish
    while ddml.status().await.clusters[0].mirrors[0].rebuilding.is_some() {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Verify by reading all data back
    for (i, drp) in drps.iter().enumerate() {
        let db: Box<DivBuf> = ddml.get::<DivBufShared, DivBuf>(drp).await.unwrap();
        assert_eq!(&db[..], &vec![i as u8; BYTES_PER_LBA][..]);
    }

    // Now fault the OTHER disk and reverify
    let stat = ddml.status().await;
    let other_leaf_uuid = stat.clusters[0].mirrors[0].leaves[1].uuid;
    ddml.fault(other_leaf_uuid).await.unwrap();

    for (i, drp) in drps.iter().enumerate() {
        ddml.evict(drp);
        let db: Box<DivBuf> = ddml.get::<DivBufShared, DivBuf>(drp).await.unwrap();
        assert_eq!(&db[..], &vec![i as u8; BYTES_PER_LBA][..]);
    }
}

#[rstest]
#[tokio::test]
async fn basic(ddml: DDML) {
    let dbs = DivBufShared::from(vec![42u8; 4096]);
    let ddml2 = &ddml;
    ddml.put(dbs, Compression::None, TxgT::from(0))
    .and_then(move |drp| {
        let drp2 = &drp;
        ddml2.get::<DivBufShared, DivBuf>(drp2)
        .map_ok(|db: Box<DivBuf>| {
            assert_eq!(&db[..], &vec![42u8; 4096][..]);
        }).and_then(move |_| {
            ddml2.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
        }).map_ok(|dbs: Box<DivBufShared>| {
            assert_eq!(&dbs.try_const().unwrap()[..],
                       &vec![42u8; 4096][..]);
        }).and_then(move |_| {
            // Even though the record has been removed from cache, it
            // should still be on disk
            ddml2.get::<DivBufShared, DivBuf>(&drp)
        })
    }).map_ok(|db: Box<DivBuf>| {
        assert_eq!(&db[..], &vec![42u8; 4096][..]);
    }).await
    .unwrap();
}

// Round trip some compressible data.  Use the contents of vdev_raid.rs, a
// moderately large and compressible file
#[rstest]
#[tokio::test]
async fn compressible(
    ddml: DDML,
    #[values(
        Compression::None,
        Compression::LZ4(None),
        Compression::Zstd(None)
    )]
    compression: Compression
) {
    let txg = TxgT::from(0);
    let ddml2 = &ddml;
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("src/raid/vdev_raid.rs");
    let mut file = fs::File::open(path).unwrap();
    let mut vdev_raid_contents = Vec::new();
    file.read_to_end(&mut vdev_raid_contents).unwrap();
    let dbs = DivBufShared::from(vdev_raid_contents.clone());
    ddml.put(dbs, compression, txg)
    .and_then(|drp| {
        let drp2 = &drp;
        ddml2.get::<DivBufShared, DivBuf>(drp2)
        .map_ok(|db: Box<DivBuf>| {
            assert_eq!(&db[..], &vdev_raid_contents[..]);
        }).and_then(move |_| {
            ddml2.pop::<DivBufShared, DivBuf>(&drp, txg)
        }).map_ok(|dbs: Box<DivBufShared>| {
            assert_eq!(&dbs.try_const().unwrap()[..],
                       &vdev_raid_contents[..]);
        }).and_then(move |_| {
            // Even though the record has been removed from cache, it
            // should still be on disk
            ddml2.get::<DivBufShared, DivBuf>(&drp)
        }).map_ok(|db: Box<DivBuf>| {
            assert_eq!(&db[..], &vdev_raid_contents[..]);
        })
    }).await
    .unwrap();
}

// Records of less than an LBA should be padded up.
#[rstest]
#[tokio::test]
async fn short(ddml: DDML) {
    let ddml2 = &ddml;
    let dbs = DivBufShared::from(vec![42u8; 1024]);
    ddml.put(dbs, Compression::None, TxgT::from(0))
    .and_then(move |drp| {
        let drp2 = &drp;
        ddml2.get::<DivBufShared, DivBuf>(drp2)
        .map_ok(|db: Box<DivBuf>| {
            assert_eq!(&db[..], &vec![42u8; 1024][..]);
        }).and_then(move |_| {
            ddml2.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
        }).map_ok(|dbs: Box<DivBufShared>| {
            assert_eq!(&dbs.try_const().unwrap()[..],
                       &vec![42u8; 1024][..]);
        }).and_then(move |_| {
            // Even though the record has been removed from cache, it
            // should still be on disk
            ddml2.get::<DivBufShared, DivBuf>(&drp)
        })
    }).map_ok(|db: Box<DivBuf>| {
        assert_eq!(&db[..], &vec![42u8; 1024][..]);
    }).await
    .unwrap();
}

mod integrity {
    use super::*;
    use pretty_assertions::assert_eq;

    use crate::{PoolBuilder, PoolHarness};

    async fn do_test(ph: PoolHarness, ulbas: usize, alignment_lbas: LbaT) {
        let cache = Arc::new(Mutex::new(Cache::with_capacity(1_000_000_000)));
        let ddml = DDML::create(ph.pool, cache);
        let txg = TxgT::from(0);
        let compression = Compression::None;

        // Do a small write, if necessary, to produce desired alignment
        if alignment_lbas > 0 {
            let l = alignment_lbas as usize * BYTES_PER_LBA;
            let dbs = DivBufShared::from(vec![0u8; l]);
            let db = dbs.try_const().unwrap();
            ddml.put_direct(&db, compression, txg).await.unwrap();
        }

        let mut v = Vec::with_capacity(ulbas * BYTES_PER_LBA);
        for lba in 0..ulbas {
            for _ in 0..BYTES_PER_LBA {
                v.push(lba as u8);
            }
        }
        let dbs = DivBufShared::from(v);
        let original = dbs.try_const().unwrap();

        let drp = ddml.put_direct(&original, compression, txg).await.unwrap();
        // Flush any RAID stripe buffer to disk
        ddml.flush(0).await.unwrap();

        // Repeat the experiment with corruption on every disk.
        for bad_disk in ph.paths {
            let mut f = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&bad_disk)
                .unwrap();
            let fsize = f.metadata().unwrap().len();
            let corrupt_buf = vec![0xFFu8; fsize as usize];
            let mut good_buf = vec![0u8; fsize as usize];
            f.read_exact(&mut good_buf[..]).unwrap();

            // Corrupt the data on one disk.
            f.write_all_at(&corrupt_buf[..], 0).unwrap();

            // Now try to read it.
            for _ in 0..ph.disks_per_mirror {
                let recovered = ddml.get::<DivBufShared, DivBuf>(&drp)
                    .await
                    .unwrap();
                assert_eq!(original, *recovered);
                // Evict cache and try again for every mirror member, to ensure
                // that we read from the corrupted mirror child on one attempt.
                ddml.evict(&drp);
            }

            // Restore the disk's original contents before trying again.
            f.write_all_at(&good_buf[..], 0).unwrap();
        }
    }

    /// Corrupted data can be detected.  If redundant information is available,
    /// the correct data can be reconstructed.
    #[rstest]
    #[test_log::test(tokio::test)]
    async fn mirror() {
        let fsize = 131072;
        let ph = PoolBuilder::new()
            .mirror_size(2)
            .disks(2)
            .fsize(fsize)
            .build();
        do_test(ph, 2, 0).await;
    }

    /// How to align the corrupted record that's about to be read
    enum Alignment {
        /// Align it to the beginning of a stripe
        Stripe,
        /// Align one LBA after the beginning of a stripe
        OneLba,
        /// Align one LBA before the beginning of the stripe's 2nd chunk
        AllButOneLba,
        /// Align one Chunk after the beginning of the stripe
        Chunk
    }

    /// How long the corrupted record should be
    enum Length {
        OneLba,
        PartialChunk,
        OneChunk,
        OneStripe,
        ThreeStripes
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn raid(
        #[values(
            // Simplest sensible RAID configuration
            (3, 3, 1),
            // Highly declustered configuratoin
            (11, 3, 1)
        )]
        raid_config: (usize, i16, i16),
        #[values(
            Length::OneLba,
            Length::PartialChunk,
            Length::OneChunk,
            Length::OneStripe,
            Length::ThreeStripes
        )]
        l: Length,
        #[values(
            Alignment::Stripe,
            Alignment::OneLba,
            Alignment::AllButOneLba,
            Alignment::Chunk
        )]
        a: Alignment
    ) {
        let (n, k, f) = raid_config;
        let chunksize = 5;
        let fsize = 131072;
        let ph = PoolBuilder::new()
            .disks(n)
            .chunksize(chunksize)
            .fsize(fsize)
            .redundancy_level(f)
            .stripe_size(k)
            .build();
        let ulbas = match l {
            Length::OneLba => 1,
            Length::PartialChunk if chunksize == 5 => 3,
            Length::OneChunk => chunksize as usize,
            Length::OneStripe => chunksize as usize * (k - f) as usize,
            Length::ThreeStripes => 3 * chunksize as usize * (k - f) as usize,
            _ => unimplemented!()
        };
        let alignment_lbas = match a {
            Alignment::Stripe => 0,
            Alignment::OneLba => 1,
            Alignment::AllButOneLba => chunksize - 1,
            Alignment::Chunk => chunksize,
        };
        do_test(ph, ulbas, alignment_lbas).await;
    }
}
