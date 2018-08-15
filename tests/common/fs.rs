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
        ffi::OsString,
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
                        Database::create(Arc::new(idml))
                    })
                })
            })).unwrap();
            let tree_id = rt.block_on(db.new_fs()).unwrap();
            let fs = Fs::new(Arc::new(db), rt, tree_id);
            fs
        }
    });

    test create(mocks) {
        let ino = mocks.val.create(1, &OsString::from("x"), 0o644).unwrap();
        assert_eq!(mocks.val.lookup(1, &OsString::from("x")).unwrap(), ino);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.readdir(1, 0, 0);
        let (dirent, _ofs) = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).nth(0)
        .expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_REG);
        assert_eq!(&dirent.d_name[0..2], ['x' as i8, 0x0]); // "x"
        assert_eq!(dirent.d_fileno as u64, ino);

        // The parent dir's link count should've increased
        let parent_attr = mocks.val.getattr(1).unwrap();
        assert_eq!(parent_attr.nlink, 2);
    }

    /// getattr on the filesystem's root directory
    test getattr(mocks) {
        let inode = mocks.val.getattr(1).unwrap();
        assert_eq!(inode.nlink, 1);
        assert_eq!(inode.flags, 0);
        assert!(inode.atime.sec > 0);
        assert!(inode.mtime.sec > 0);
        assert!(inode.ctime.sec > 0);
        assert!(inode.birthtime.sec > 0);
        assert_eq!(inode.uid, 0);
        assert_eq!(inode.gid, 0);
        assert_eq!(inode.mode & 0o7777, 0o755);
        assert_eq!(inode.mode & libc::S_IFMT, libc::S_IFDIR);
        assert_eq!(inode.nlink, 1);
    }

    test mkdir(mocks) {
        let ino = mocks.val.mkdir(1, &OsString::from("x"), 0o755).unwrap();
        assert_eq!(mocks.val.lookup(1, &OsString::from("x")).unwrap(), ino);

        // The new dir should have "." and ".." directory entries
        let mut entries = mocks.val.readdir(ino, 0, 0);
        let (dotdot, _) = entries.next().unwrap().unwrap();
        assert_eq!(dotdot.d_type, libc::DT_DIR);
        assert_eq!(&dotdot.d_name[0..3], [0x2e, 0x2e, 0x0]); // ".."
        assert_eq!(dotdot.d_fileno, 1);
        let (dot, _) = entries.next().unwrap().unwrap();
        assert_eq!(dot.d_type, libc::DT_DIR);
        assert_eq!(&dot.d_name[0..2], [0x2e, 0x0]); // "."
        assert_eq!(dot.d_fileno as u64, ino);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.readdir(1, 0, 0);
        let (dirent, _ofs) = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).nth(0)
        .expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_DIR);
        assert_eq!(&dirent.d_name[0..2], ['x' as i8, 0x0]); // "x"
        assert_eq!(dirent.d_fileno as u64, ino);

        // The parent dir's link count should've increased
        let parent_attr = mocks.val.getattr(1).unwrap();
        assert_eq!(parent_attr.nlink, 2);
    }

    test readdir(mocks) {
        let mut entries = mocks.val.readdir(1, 0, 0);
        let (dotdot, _) = entries.next().unwrap().unwrap();
        assert_eq!(dotdot.d_type, libc::DT_DIR);
        assert_eq!(&dotdot.d_name[0..3], [0x2e, 0x2e, 0x0]); // ".."
        let (dot, _) = entries.next().unwrap().unwrap();
        assert_eq!(dot.d_type, libc::DT_DIR);
        assert_eq!(&dot.d_name[0..2], [0x2e, 0x0]); // "."
        assert_eq!(dot.d_fileno, 1);
    }

    test statvfs(mocks) {
        let statvfs = mocks.val.statvfs();
        assert_eq!(statvfs.f_blocks, 262144);
    }

}
