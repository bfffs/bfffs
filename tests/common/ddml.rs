// vim: tw=80

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name ddml;

    use bfffs::common::{
        cache::*,
        ddml::*,
        pool::*,
        TxgT
    };
    use divbuf::{DivBuf, DivBufShared};
    use futures::{Future, future};
    use std::{
        fs,
        io::Read,
        path::Path,
        sync::{Arc, Mutex}
    };
    use tempdir::TempDir;
    use tokio::runtime::current_thread::Runtime;

    fixture!( objects() -> (Runtime, DDML) {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(TempDir::new("ddml"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            let mut rt = Runtime::new().unwrap();
            let pool = rt.block_on(future::lazy(|| {
                let clusters = vec![
                    Pool::create_cluster(1, 1, 1, None, 0, &[filename][..])
                ];
                future::join_all(clusters)
                    .map_err(|_| unreachable!())
                    .and_then(|clusters|
                        Pool::create("TestPool".to_string(), clusters)
                    )
            })).unwrap();
            let cache = Cache::with_capacity(1_000_000_000);
            (rt, DDML::new(pool, Arc::new(Mutex::new(cache))))
        }
    });

    test basic(objects) {
        let (mut rt, ddml) = objects.val;
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let ddml2 = &ddml;
        rt.block_on(future::lazy(|| {
            ddml.put(dbs, Compression::None, TxgT::from(0))
            .and_then(move |drp| {
                let drp2 = &drp;
                ddml2.get::<DivBufShared, DivBuf>(drp2)
                .map(|db: Box<DivBuf>| {
                    assert_eq!(&db[..], &vec![42u8; 4096][..]);
                }).and_then(move |_| {
                    ddml2.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
                }).map(|dbs: Box<DivBufShared>| {
                    assert_eq!(&dbs.try().unwrap()[..], &vec![42u8; 4096][..]);
                }).and_then(move |_| {
                    // Even though the record has been removed from cache, it
                    // should still be on disk
                    ddml2.get::<DivBufShared, DivBuf>(&drp)
                })
            }).map(|db: Box<DivBuf>| {
                assert_eq!(&db[..], &vec![42u8; 4096][..]);
            })
        })).unwrap();
    }

    // Round trip some compressible data.  Use the contents of vdev_raid.rs, a
    // moderately large and compressible file
    test compressible(objects) {
        let txg = TxgT::from(0);
        let (mut rt, ddml) = objects.val;
        let ddml2 = &ddml;
        let filename = Path::new(file!())
            .parent().unwrap()
            .parent().unwrap()
            .parent().unwrap()
            .join("src/common/vdev_raid.rs");
        let mut file = t!(fs::File::open(&filename));
        let mut vdev_raid_contents = Vec::new();
        file.read_to_end(&mut vdev_raid_contents).unwrap();
        let dbs = DivBufShared::from(vdev_raid_contents.clone());
        rt.block_on(future::lazy(|| {
            ddml.put(dbs, Compression::ZstdL9NoShuffle, txg)
            .and_then(|drp| {
                let drp2 = &drp;
                ddml2.get::<DivBufShared, DivBuf>(drp2)
                .map(|db: Box<DivBuf>| {
                    assert_eq!(&db[..], &vdev_raid_contents[..]);
                }).and_then(move |_| {
                    ddml2.pop::<DivBufShared, DivBuf>(&drp, txg)
                }).map(|dbs: Box<DivBufShared>| {
                    assert_eq!(&dbs.try().unwrap()[..], &vdev_raid_contents[..]);
                }).and_then(move |_| {
                    // Even though the record has been removed from cache, it
                    // should still be on disk
                    ddml2.get::<DivBufShared, DivBuf>(&drp)
                }).map(|db: Box<DivBuf>| {
                    assert_eq!(&db[..], &vdev_raid_contents[..]);
                })
            })
        })).unwrap();
    }

    // Records of less than an LBA should be padded up.
    test short(objects) {
        let (mut rt, ddml) = objects.val;
        let ddml2 = &ddml;
        let dbs = DivBufShared::from(vec![42u8; 1024]);
        rt.block_on(future::lazy(|| {
            ddml.put(dbs, Compression::None, TxgT::from(0))
            .and_then(move |drp| {
                let drp2 = &drp;
                ddml2.get::<DivBufShared, DivBuf>(drp2)
                .map(|db: Box<DivBuf>| {
                    assert_eq!(&db[..], &vec![42u8; 1024][..]);
                }).and_then(move |_| {
                    ddml2.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
                }).map(|dbs: Box<DivBufShared>| {
                    assert_eq!(&dbs.try().unwrap()[..], &vec![42u8; 1024][..]);
                }).and_then(move |_| {
                    // Even though the record has been removed from cache, it
                    // should still be on disk
                    ddml2.get::<DivBufShared, DivBuf>(&drp)
                })
            }).map(|db: Box<DivBuf>| {
                assert_eq!(&db[..], &vec![42u8; 1024][..]);
            })
        })).unwrap();
    }
}
