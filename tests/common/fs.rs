// vim: tw=80

/// Constructs a real filesystem and tests the common FS routines, without mounting
macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

test_suite! {
    name fs;

    use bfffs::{
        common::cache::*,
        common::database::*,
        common::ddml::*,
        common::fs::*,
        common::idml::*,
        common::pool::*,
    };
    use futures::{Future, future};
    use libc;
    use std::{
        fs,
        sync::{Arc, Mutex}
    };
    use tempdir::TempDir;
    use tokio_io_pool::Runtime;

    fixture!( mocks() -> Fs {
        setup(&mut self) {
            let mut rt = Runtime::new();
            let len = 1 << 30;  // 1GB
            let tempdir = t!(TempDir::new("test_fs"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            drop(file);
            let db = rt.block_on(future::lazy(move || {
                Pool::create_cluster(16, 1, 1, None, 0, &[filename])
                .map_err(|_| unreachable!())
                .and_then(|cluster| {
                    Pool::create(String::from("test_fs"), vec![cluster])
                    .map(|pool| {
                        let cache = Arc::new(Mutex::new(Cache::with_capacity(1000)));
                        let ddml = Arc::new(DDML::new(pool, cache.clone()));
                        let idml = IDML::create(ddml, cache);
                        Database::new(Arc::new(idml))
                    })
                })
            })).unwrap();
            let tree_id = rt.block_on(db.new_fs()).unwrap();
            let fs = Fs::new(Arc::new(db), rt, tree_id);
            fs
        }
    });

    /// getattr on the filesystem's root directory
    test getattr(mocks) {
        let inode = mocks.val.getattr(1).unwrap();
        assert_eq!(inode.nlink, 0);
        assert_eq!(inode.flags, 0);
        assert!(inode.atime.sec > 0);
        assert!(inode.mtime.sec > 0);
        assert!(inode.ctime.sec > 0);
        assert!(inode.birthtime.sec > 0);
        assert_eq!(inode.uid, 0);
        assert_eq!(inode.gid, 0);
        assert_eq!(inode.mode & 0o7777, 0o755);
        assert_eq!(inode.mode & libc::S_IFMT, libc::S_IFDIR);
    }

    test readdir(mocks) {
        let mut entries = mocks.val.readdir(1, 0, 0);
        let (dotdot, ofs) = entries.next().unwrap().unwrap();
        assert_eq!(dotdot.d_type, libc::DT_DIR);
        assert_eq!(&dotdot.d_name[0..3], [0x2e, 0x2e, 0x0]); // ".."
        assert_eq!(ofs, 1);
        let (dot, ofs) = entries.next().unwrap().unwrap();
        assert_eq!(dot.d_type, libc::DT_DIR);
        assert_eq!(&dot.d_name[0..2], [0x2e, 0x0]); // "."
        assert_eq!(dot.d_fileno, 1);
        assert_eq!(ofs, 2);
    }

    test statvfs(mocks) {
        let statvfs = mocks.val.statvfs();
        assert_eq!(statvfs.f_blocks, 262144);
    }

}
