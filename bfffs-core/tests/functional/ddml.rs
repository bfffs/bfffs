// vim: tw=80
mod ddml {
    use bfffs_core::{
        cache::*,
        dml::*,
        ddml::*,
        TxgT
    };
    use divbuf::{DivBuf, DivBufShared};
    use futures::TryFutureExt;
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use std::{
        fs,
        io::Read,
        path::Path,
        sync::{Arc, Mutex}
    };

    #[fixture]
    fn ddml() -> DDML {
        let (_tempdir, _paths, pool) = crate::PoolBuilder::new()
            .chunksize(1)
            .build();
        let cache = Cache::with_capacity(1_000_000_000);
        DDML::new(pool, Arc::new(Mutex::new(cache)))
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
    async fn compressible(ddml: DDML) {
        let txg = TxgT::from(0);
        let ddml2 = &ddml;
        let mut file = fs::File::open(
                Path::new("../bfffs-core/src/raid/vdev_raid.rs")
            ).unwrap_or_else(|_|
                fs::File::open(Path::new("bfffs-core/src/raid/vdev_raid.rs")
            ).unwrap()
        );
        let mut vdev_raid_contents = Vec::new();
        file.read_to_end(&mut vdev_raid_contents).unwrap();
        let dbs = DivBufShared::from(vdev_raid_contents.clone());
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
}
