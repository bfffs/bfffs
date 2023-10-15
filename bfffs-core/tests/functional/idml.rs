// vim: tw=80
mod persistence {
    use bfffs_core::{
        cache::{self, Cache},
        ddml::{self, DDML},
        idml::{self, IDML},
        label::*,
        types::*,
    };
    use futures::TryFutureExt;
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
        path::PathBuf,
        sync::{Arc, Mutex}
    };
    use tempfile::TempDir;

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_IDML_LABEL: [u8; 130] = [
        // Past the Pool::Label, we have an IDML::Label
        // First comes the allocation table
        // Height as 64 bits
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // min_int_fanout as 16 bits
        0x4e, 0x00,
        // max_int_fanout as 16 bits
                    0x38, 0x01,
        // min_leaf_fanout as 16 bits
                                0xe4, 0x04,
        // max_leaf_fanout as 16 bits
                                            0x8d, 0x13,
        // leaf node max size in bytes, as 64-bits
        0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Root node's address as a DRP
        // cluster as 16 bits
        0x00, 0x00,
        // LBA as 64 bits
                    0x0a, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
        // Compression enabled as 8 bits
                    0x00,
        // lsize as 32 bits
                          0x0c, 0x00, 0x00, 0x00,
        // csize as 32 bits
                                                  0x0c,
        0x00, 0x00, 0x00,
        // checksum as 64 bits
                          0x9f, 0xfa, 0xe3, 0x4c, 0xa2,
        0x4d, 0xea, 0x69,
        // Root node's TXG range as a pair of 32-bit numbers
                          0x2a, 0x00, 0x00, 0x00, 0x2b,
        0x00, 0x00, 0x00,
        // Next is the IDML's next RID as 64 bits
                          0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00,
        // Followed by the RIDT, in the same format as the AllocT
        // Height as 64 bits
                          0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00,
        // min_int_fanout as 16 bits
                          0x6d, 0x00,
        // max_int_fanout as 16 bits
                                      0xb1, 0x01,
        // min_leaf_fanout as 16 bits
                                                  0x86,
        0x00,
        // max_leaf_fanout as 16 bits
              0x17, 0x02,
        // leaf node max size in bytes, as 64-bits
                          0x00, 0x00, 0x40, 0x00, 0x00,
        0x00, 0x00, 0x00,
        // Root node's address as a DRP
        // cluster as 16 bits
                          0x00, 0x00,
        // LBA as 64 bits
                                      0x0b, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00,
        // Compression enabled as 8 bits
                                      0x00,
        // lsize as 32 bits
                                            0x0c, 0x00,
        0x00, 0x00,
        // csize as 32 bits
                    0x0c, 0x00, 0x00, 0x00,
        // checksum as 64 bits
                                            0x9f, 0xfa,
        0xe3, 0x4c, 0xa2, 0x4d, 0xea, 0x69,
        // Root node's TXG range as a pair of 32-bit numbers
                                            0x2a, 0x00,
        0x00, 0x00, 0x2b, 0x00, 0x00, 0x00,
        // Label's transaction as 32 bits
                                            0x2a, 0x00,
        0x00, 0x00
    ];

    const POOLNAME: &str = "TestPool";

    #[fixture]
    fn objects() -> (Arc<IDML>, TempDir, Vec<PathBuf>) {
        let ph = crate::PoolBuilder::new()
            .chunksize(1)
            .name(POOLNAME)
            .build();
        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::new(ph.pool, cache.clone()));
        let idml = Arc::new(IDML::create(ddml, cache));
        (idml, ph.tempdir, ph.paths)
    }

    // Testing IDML::open with golden labels is too hard, because we need to
    // store separate golden labels for each VdevLeaf.  Instead, we'll just
    // check that we can open-after-write
    #[rstest]
    #[tokio::test]
    async fn open(objects: (Arc<IDML>, TempDir, Vec<PathBuf>)) {
        let (old_idml, _tempdir, paths) = objects;
        let txg = TxgT::from(42);
        let old_idml2 = old_idml.clone();
        old_idml.advance_transaction(|_| {
            let label_writer = LabelWriter::new(0);
            old_idml2.flush(Some(0), txg)
            .and_then(move |_| {
                old_idml2.write_label(label_writer, txg)
            })
        }).await.unwrap();
        drop(old_idml);

        let cache = cache::Cache::with_capacity(4_194_304);
        let arc_cache = Arc::new(Mutex::new(cache));
        let mut manager = ddml::Manager::default();
        manager.taste(&paths[0]).await.unwrap();
        let uuid = manager.importable_pools()[0].1;
        let (ddml, reader) = manager.import(uuid, arc_cache.clone())
            .await.unwrap();
        let addml = Arc::new(ddml);
        idml::IDML::open(addml, arc_cache, 1<<30, reader);
    }

    #[rstest]
    #[tokio::test]
    async fn write_label(objects: (Arc<IDML>, TempDir, Vec<PathBuf>)) {
        let (idml, _tempdir, paths) = objects;
        let txg = TxgT::from(42);
        let idml2 = idml.clone();
        idml.advance_transaction(move |_| {
            idml2.flush(Some(0), txg)
            .and_then(move |_| {
                let label_writer = LabelWriter::new(0);
                idml2.write_label(label_writer, txg)
            })
        }).await.unwrap();
        let mut f = fs::File::open(&paths[0]).unwrap();
        let mut v = vec![0; 8192];
        // Skip leaf, mirror, raid, cluster, and pool labels
        f.seek(SeekFrom::Start(204)).unwrap();
        f.read_exact(&mut v).unwrap();
        // Uncomment this block to save the binary label for inspection
        /* {
            use std::fs::File;
            use std::io::Write;
            let mut df = File::create("/tmp/label.bin").unwrap();
            df.write_all(&v[..]).unwrap();
        } */
        // Compare against the golden master
        assert_eq!(&v[0..130], &GOLDEN_IDML_LABEL[0..130]);
        // Rest of the buffer should be zero-filled
        assert!(v[130..].iter().all(|&x| x == 0));
    }
}

mod t {
    use bfffs_core::*;
    use bfffs_core::cache::*;
    use bfffs_core::dml::*;
    use bfffs_core::ddml::*;
    use bfffs_core::idml::*;
    use divbuf::DivBufShared;
    use rstest::{fixture, rstest};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    const LBA_PER_ZONE: LbaT = 256;
    const POOLNAME: &str = "TestPool";

    #[fixture]
    fn objects() -> (IDML, TempDir) {
        let ph = crate::PoolBuilder::new()
            .chunksize(1)
            .zone_size(LBA_PER_ZONE)
            .name(POOLNAME)
            .build();
        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::new(ph.pool, cache.clone()));
        let idml = IDML::create(ddml, cache);
        (idml, ph.tempdir)
    }

    // When moving the last record from a zone, the allocator should not reopen
    // the same zone for its destination
    #[rstest]
    #[tokio::test]
    async fn move_last_record(objects: (IDML, TempDir)) {
        let (idml, _tempdir) = objects;
        let idml = Arc::new(idml);
        // Write exactly 1 zone plus an LBA of data, then clean the first
        // zone.  This ensures that when the last record is moved, the
        // second zone will be full and the allocator will need to open a
        // new zone.  It's indepedent of the label size.  At no point should
        // we lose the record's reverse mapping.
        let idml3 = idml.clone();
        for _ in 0..=LBA_PER_ZONE {
            let txg = idml.txg().await;
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            idml.put(dbs, Compression::None, *txg).await.unwrap();
        }
        {
            let txg = idml3.txg().await;
            let cz = idml3.list_closed_zones().await.next().unwrap();
            idml3.clean_zone(cz, *txg).await.unwrap();
        }
        assert!(idml3.check().await.unwrap());
    }
}
