// vim: tw=80
use galvanic_test::test_suite;

test_suite! {
    name persistence;

    use bfffs_core::*;
    use bfffs_core::cache::*;
    use bfffs_core::vdev_block::*;
    use bfffs_core::cluster;
    use bfffs_core::pool::*;
    use bfffs_core::ddml::*;
    use bfffs_core::idml::*;
    use bfffs_core::label::*;
    use bfffs_core::vdev_file::*;
    use futures::TryFutureExt;
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
        num::NonZeroU64,
        path::PathBuf,
        sync::{Arc, Mutex}
    };
    use super::super::basic_runtime;
    use tempfile::{Builder, TempDir};
    use tokio::runtime::Runtime;

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

    const POOLNAME: &str = &"TestPool";

    fixture!( objects() -> (Runtime, Arc<IDML>, TempDir, PathBuf) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir =
                t!(Builder::new().prefix("test_idml_persistence").tempdir());
            let filename = tempdir.path().join("vdev");
            {
                let file = t!(fs::File::create(&filename));
                t!(file.set_len(len));
            }
            let paths = [filename.clone()];
            let rt = basic_runtime();
            let cs = NonZeroU64::new(1);
            let cluster = Pool::create_cluster(cs, 1, None, 0, &paths);
            let clusters = vec![cluster];
            let pool = Pool::create(POOLNAME.to_string(), clusters);
            let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            let idml = Arc::new(IDML::create(ddml, cache));
            (rt, idml, tempdir, filename)
        }
    });

    // Testing IDML::open with golden labels is too hard, because we need to
    // store separate golden labels for each VdevLeaf.  Instead, we'll just
    // check that we can open-after-write
    test open(objects()) {
        let (mut rt, old_idml, _tempdir, path) = objects.val;
        let txg = TxgT::from(42);
        let old_idml2 = old_idml.clone();
        rt.block_on(
            old_idml.advance_transaction(|_| {
                let label_writer = LabelWriter::new(0);
                old_idml2.flush(Some(0), txg)
                .and_then(move |_| {
                    old_idml2.write_label(label_writer, txg)
                })
            })
        ).unwrap();
        drop(old_idml);
        let _idml = rt.block_on(async {
            VdevFile::open(path)
            .and_then(|(leaf, reader)| {
                let block = VdevBlock::new(leaf);
                let (vr, lr) = raid::open(None, vec![(block, reader)]);
                cluster::Cluster::open(vr)
                .map_ok(move |cluster| (cluster, lr))
            }).map_ok(move |(cluster, reader)|{
                let (pool, reader) = Pool::open(None, vec![(cluster, reader)]);
                let cache = cache::Cache::with_capacity(4_194_304);
                let arc_cache = Arc::new(Mutex::new(cache));
                let ddml = Arc::new(ddml::DDML::open(pool, arc_cache.clone()));
                idml::IDML::open(ddml, arc_cache, 1<<30, reader)
            }).await
        }).unwrap();
    }

    test write_label(objects()) {
        let (mut rt, idml, _tempdir, path) = objects.val;
        let txg = TxgT::from(42);
        let idml2 = idml.clone();
        rt.block_on(
            idml.advance_transaction(move |_| {
                idml2.flush(Some(0), txg)
                .and_then(move |_| {
                    let label_writer = LabelWriter::new(0);
                    idml2.write_label(label_writer, txg)
                })
            })
        ).unwrap();
        let mut f = fs::File::open(path).unwrap();
        let mut v = vec![0; 8192];
        // Skip leaf, raid, cluster, and pool labels
        f.seek(SeekFrom::Start(164)).unwrap();
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

test_suite! {
    name t;

    use bfffs_core::*;
    use bfffs_core::cache::*;
    use bfffs_core::pool::*;
    use bfffs_core::ddml::*;
    use bfffs_core::idml::*;
    use divbuf::DivBufShared;
    use futures::{
        FutureExt,
        StreamExt,
        TryFutureExt,
        TryStreamExt,
        stream
    };
    use galvanic_test::*;
    use std::{
        fs,
        num::NonZeroU64,
        sync::{Arc, Mutex}
    };
    use super::super::*;
    use tempfile::{Builder, TempDir};
    use tokio::runtime::Runtime;

    const LBA_PER_ZONE: LbaT = 256;
    const POOLNAME: &str = &"TestPool";

    fixture!( objects() -> (Runtime, IDML, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir =
                t!(Builder::new().prefix("test_idml_persistence").tempdir());
            let filename = tempdir.path().join("vdev");
            {
                let file = t!(fs::File::create(&filename));
                t!(file.set_len(len));
            }
            let paths = [filename];
            let rt = basic_runtime();
            let cs = NonZeroU64::new(1);
            let lpz = NonZeroU64::new(LBA_PER_ZONE);
            let cluster = Pool::create_cluster(cs, 1, lpz, 0, &paths);
            let clusters = vec![cluster];
            let pool = Pool::create(POOLNAME.to_string(), clusters);
            let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            let idml = IDML::create(ddml, cache);
            (rt, idml, tempdir)
        }
    });

    // When moving the last record from a zone, the allocator should not reopen
    // the same zone for its destination
    test move_last_record(objects()) {
        let (mut rt, idml, _tempdir) = objects.val;
        let idml = Arc::new(idml);
        let ok = rt.block_on(async {
            // Write exactly 1 zone plus an LBA of data, then clean the first
            // zone.  This ensures that when the last record is moved, the
            // second zone will be full and the allocator will need to open a
            // new zone.  It's indepedent of the label size.  At no point should
            // we lose the record's reverse mapping.
            let idml3 = idml.clone();
            let idml4 = idml.clone();
            stream::iter(0..=LBA_PER_ZONE)
            .map(Ok)
            .try_for_each(move |_| {
                let idml2 = idml.clone();
                idml.txg()
                .then(move |txg| {
                    let dbs = DivBufShared::from(vec![0u8; 4096]);
                    idml2.put(dbs, Compression::None, *txg)
                }).map_ok(drop)
            }).and_then(move |_| {
                idml3.txg()
                .then(move |txg| {
                    let idml5 = idml3.clone();
                    let cz = idml3.list_closed_zones().next().unwrap();
                    idml5.clean_zone(cz, *txg)
                })
            }).and_then(move |_| {
                idml4.check()
            }).await
        }).unwrap();
        assert!(ok);
    }
}
