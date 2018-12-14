// vim: tw=80
use galvanic_test::*;

test_suite! {
    name persistence;

    use bfffs::common::*;
    use bfffs::common::cache::*;
    use bfffs::common::vdev_block::*;
    use bfffs::common::vdev_raid::*;
    use bfffs::common::cluster;
    use bfffs::common::pool::*;
    use bfffs::common::ddml::*;
    use bfffs::common::idml::*;
    use bfffs::common::label::*;
    use bfffs::sys::vdev_file::*;
    use futures::{Future, future};
    use std::{
        fs,
        io::{Read, Seek, SeekFrom},
        num::NonZeroU64,
        path::PathBuf,
        sync::{Arc, Mutex}
    };
    use tempdir::TempDir;
    use tokio::runtime::current_thread::Runtime;

    // To regenerate this literal, dump the binary label using this command:
    // hexdump -e '8/1 "0x%02x, " " // "' -e '8/1 "%_p" "\n"' /tmp/label.bin
    const GOLDEN_IDML_LABEL: [u8; 228] = [
        // Past the Pool::Label, we have an IDML::Label
        0xa4, 0x66, 0x61, 0x6c, 0x6c, 0x6f, 0x63, 0x74, // .falloct
        0x98, 0x54, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // .T......
        0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 0x00, 0x00, // .....@..
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x2a, 0x00, // ......*.
        0x00, 0x00, 0x18, 0x2b, 0x00, 0x00, 0x00, 0x00, // ...+....
        0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x0c, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, // ........
        0x00, 0x18, 0x9f, 0x18, 0xfa, 0x18, 0xe3, 0x18, // ........
        0x4c, 0x18, 0xa2, 0x18, 0x4d, 0x18, 0xea, 0x18, // L...M...
        0x69, 0x68, 0x6e, 0x65, 0x78, 0x74, 0x5f, 0x72, // ihnext_r
        0x69, 0x64, 0x00, 0x64, 0x72, 0x69, 0x64, 0x74, // id.dridt
        0x98, 0x52, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // .R......
        0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 0x00, 0x00, // .....@..
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x18, 0x2a, 0x00, 0x00, 0x00, // ....*...
        0x18, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // .+......
        0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, // ........
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, // ........
        0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x18, // ........
        0x9f, 0x18, 0xfa, 0x18, 0xe3, 0x18, 0x4c, 0x18, // ......L.
        0xa2, 0x18, 0x4d, 0x18, 0xea, 0x18, 0x69, 0x63, // ..M...ic
        0x74, 0x78, 0x67, 0x18, 0x2a, 0x00, 0x00, 0x00, // txg.*...
        0x00, 0x00, 0x00, 0x00                          // ....
    ];

    const POOLNAME: &str = &"TestPool";

    fixture!( objects() -> (Runtime, Arc<IDML>, TempDir, PathBuf) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(TempDir::new("test_idml_persistence"));
            let filename = tempdir.path().join("vdev");
            {
                let file = t!(fs::File::create(&filename));
                t!(file.set_len(len));
            }
            let paths = [filename.clone()];
            let mut rt = Runtime::new().unwrap();
            let pool = rt.block_on(future::lazy(|| {
                let cs = NonZeroU64::new(1);
                let cluster = Pool::create_cluster(cs, 1, 1, None, 0, &paths);
                let clusters = vec![cluster];
                future::join_all(clusters)
                    .map_err(|_| unreachable!())
                    .and_then(|clusters|
                        Pool::create(POOLNAME.to_string(), clusters)
                    )
            })).unwrap();
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
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
                old_idml2.flush(0, txg)
                .and_then(move |_| {
                    old_idml2.write_label(label_writer, txg)
                })
            })
        ).unwrap();
        drop(old_idml);
        let _idml = rt.block_on(future::lazy(|| {
            VdevFile::open(path)
            .and_then(|(leaf, reader)| {
                    let block = VdevBlock::new(leaf);
                    let (vr, lr) = VdevRaid::open(None, vec![(block, reader)]);
                    cluster::Cluster::open(vr)
                    .map(move |cluster| (cluster, lr))
            }).and_then(move |(cluster, reader)|{
                let proxy = ClusterProxy::new(cluster);
                Pool::open(None, vec![(proxy, reader)])
            }).map(|(pool, reader)| {
                let cache = cache::Cache::with_capacity(1_000_000);
                let arc_cache = Arc::new(Mutex::new(cache));
                let ddml = Arc::new(ddml::DDML::open(pool, arc_cache.clone()));
                idml::IDML::open(ddml, arc_cache, reader)
            })
        })).unwrap();
    }

    test write_label(objects()) {
        let (mut rt, idml, _tempdir, path) = objects.val;
        let txg = TxgT::from(42);
        let idml2 = idml.clone();
        rt.block_on(
            idml.advance_transaction(move |_| {
                idml2.flush(0, txg)
                .and_then(move |_| {
                    let label_writer = LabelWriter::new(0);
                    idml2.write_label(label_writer, txg)
                })
            })
        ).unwrap();
        let mut f = fs::File::open(path).unwrap();
        let mut v = vec![0; 8192];
        // Skip leaf, raid, cluster, and pool labels
        f.seek(SeekFrom::Start(0x117)).unwrap();
        f.read_exact(&mut v).unwrap();
        // Uncomment this block to save the binary label for inspection
        /* {
            use std::fs::File;
            use std::io::Write;
            let mut df = File::create("/tmp/label.bin").unwrap();
            df.write_all(&v[..]).unwrap();
        } */
        // Compare against the golden master, skipping the checksum and UUID
        // fields
        assert_eq!(&v[0..228], &GOLDEN_IDML_LABEL[0..228]);
        // Rest of the buffer should be zero-filled
        assert!(v[228..].iter().all(|&x| x == 0));
    }
}

test_suite! {
    name t;

    use bfffs::*;
    use bfffs::common::*;
    use bfffs::common::cache::*;
    use bfffs::common::pool::*;
    use bfffs::common::ddml::*;
    use bfffs::common::idml::*;
    use divbuf::DivBufShared;
    use futures::{Future, Stream, future, stream};
    use std::{
        fs,
        num::NonZeroU64,
        sync::{Arc, Mutex}
    };
    use tempdir::TempDir;
    use tokio::runtime::current_thread::Runtime;

    const LBA_PER_ZONE: LbaT = 256;
    const POOLNAME: &str = &"TestPool";

    fixture!( objects() -> (Runtime, IDML, TempDir) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(TempDir::new("test_idml_persistence"));
            let filename = tempdir.path().join("vdev");
            {
                let file = t!(fs::File::create(&filename));
                t!(file.set_len(len));
            }
            let paths = [filename.clone()];
            let mut rt = Runtime::new().unwrap();
            let pool = rt.block_on(future::lazy(|| {
                let cs = NonZeroU64::new(1);
                let lpz = NonZeroU64::new(LBA_PER_ZONE);
                let cluster = Pool::create_cluster(cs, 1, 1, lpz, 0, &paths);
                let clusters = vec![cluster];
                future::join_all(clusters)
                    .map_err(|_| unreachable!())
                    .and_then(|clusters|
                        Pool::create(POOLNAME.to_string(), clusters)
                    )
            })).unwrap();
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1_000_000)));
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
        let ok = rt.block_on(future::lazy(|| {
            // Write exactly 1 zone plus an LBA of data, then clean the first
            // zone.  This ensures that when the last record is moved, the
            // second zone will be full and the allocator will need to open a
            // new zone.  It's indepedent of the label size.  At no point should
            // we lose the record's reverse mapping.
            let idml3 = idml.clone();
            let idml4 = idml.clone();
            stream::iter_ok(0..=LBA_PER_ZONE).for_each(move |_| {
                let idml2 = idml.clone();
                idml.txg()
                .map_err(|_| Error::EPIPE)
                .and_then(move |txg| {
                    let dbs = DivBufShared::from(vec![0u8; 4096]);
                    idml2.put(dbs, Compression::None, *txg)
                }).map(drop)
            }).and_then(move |_| {
                idml3.txg()
                .map_err(|_| Error::EPIPE)
                .and_then(move |txg| {
                    let idml5 = idml3.clone();
                    idml3.list_closed_zones()
                    .take(1)
                    .for_each(move  |cz| {
                        boxfut!(idml5.clean_zone(cz, *txg))
                    })
                })
            }).and_then(move |_| {
                idml4.check()
            })
        })).unwrap();
        assert!(ok);
    }
}
