// vim: tw=80

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name ddml;

    use arkfs::common::ddml::*;
    use arkfs::common::pool::*;
    use divbuf::DivBufShared;
    use futures::{Future, future};
    use std::fs;
    use tempdir::TempDir;
    use tokio::executor::current_thread;
    use tokio::reactor::Handle;

    fixture!( objects() -> DDML {
        setup(&mut self) {
            let len = 1 << 26;  // 64 MB
            let tempdir = t!(TempDir::new("ddml"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            let pool = Pool::create("TestPool".to_string(),
                vec![
                    Pool::create_cluster(1, 1, 1, 0, &[filename][..],
                                         Handle::default())
                ]
            );
            DDML::new(pool)
        }
    });

    test basic(objects) {
        let ddml: DDML = objects.val;
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let (drp, fut) = ddml.put(dbs, Compression::None);
        current_thread::block_on_all(future::lazy(|| {
            let ddml2 = &ddml;
            let drp2 = &drp;
            fut.and_then(move |_| {
                ddml2.get(drp2)
            }).map(|db| {
                assert_eq!(&db[..], &vec![42u8; 4096][..]);
            }).and_then(|_| {
                ddml.pop(&drp)
            }).map(|dbs| {
                assert_eq!(&dbs.try().unwrap()[..], &vec![42u8; 4096][..]);
            }).and_then(|_| {
                // Even though the record has been removed from cache, it should
                // still be on disk
                ddml.get(&drp)
            }).map(|db| {
                assert_eq!(&db[..], &vec![42u8; 4096][..]);
            })
        })).unwrap();
    }
}
