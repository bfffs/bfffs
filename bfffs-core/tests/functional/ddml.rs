// vim: tw=80
use galvanic_test::test_suite;

test_suite! {
    name ddml;

    use bfffs_core::{
        cache::*,
        ddml::*,
        pool::*,
        TxgT
    };
    use divbuf::{DivBuf, DivBufShared};
    use futures::TryFutureExt;
    use galvanic_test::*;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::Read,
        num::NonZeroU64,
        path::Path,
        sync::{Arc, Mutex}
    };
    use tempfile::Builder;
    use super::super::*;
    use tokio::runtime::Runtime;

    fixture!( objects() -> (Runtime, DDML) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(Builder::new().prefix("ddml").tempdir());
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            let rt = basic_runtime();
            let cs = NonZeroU64::new(1);
            let clusters = vec![
                Pool::create_cluster(cs, 1, None, 0, &[filename][..])
            ];
            let pool = Pool::create("TestPool".to_string(), clusters);
            let cache = Cache::with_capacity(1_000_000_000);
            (rt, DDML::new(pool, Arc::new(Mutex::new(cache))))
        }
    });

    test basic(objects) {
        let (mut rt, ddml) = objects.val;
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let ddml2 = &ddml;
        rt.block_on(async {
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
        }).unwrap();
    }

    // Round trip some compressible data.  Use the contents of vdev_raid.rs, a
    // moderately large and compressible file
    test compressible(objects) {
        let txg = TxgT::from(0);
        let (mut rt, ddml) = objects.val;
        let ddml2 = &ddml;
        let mut file = fs::File::open(
                &Path::new("../bfffs-core/src/raid/vdev_raid.rs")
            ).unwrap_or_else(|_|
                fs::File::open(&Path::new("bfffs-core/src/raid/vdev_raid.rs")
            ).unwrap()
        );
        let mut vdev_raid_contents = Vec::new();
        file.read_to_end(&mut vdev_raid_contents).unwrap();
        let dbs = DivBufShared::from(vdev_raid_contents.clone());
        rt.block_on(async {
            ddml.put(dbs, Compression::Zstd(None), txg)
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
        }).unwrap();
    }

    // Records of less than an LBA should be padded up.
    test short(objects) {
        let (mut rt, ddml) = objects.val;
        let ddml2 = &ddml;
        let dbs = DivBufShared::from(vec![42u8; 1024]);
        rt.block_on(async {
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
        }).unwrap();
    }
}
