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
    use rand::{Rng, thread_rng};
    use std::{
        ffi::{CString, CStr, OsString},
        fs,
        os::raw::c_char,
        sync::{Arc, Mutex}
    };
    use tempdir::TempDir;
    use tokio_io_pool::Runtime;

    fixture!( mocks() -> (Fs, Runtime) {
        setup(&mut self) {
            let mut rt = Runtime::new();
            let handle = rt.handle().clone();
            let len = 1 << 30;  // 1GB
            let tempdir = t!(TempDir::new("test_fs"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            drop(file);
            let db = rt.block_on(future::lazy(move || {
                Pool::create_cluster(None, 1, 1, None, 0, &[filename])
                .map_err(|_| unreachable!())
                .and_then(|cluster| {
                    Pool::create(String::from("test_fs"), vec![cluster])
                    .map(|pool| {
                        let cache = Arc::new(
                            Mutex::new(
                                Cache::with_capacity(1_000_000)
                            )
                        );
                        let ddml = Arc::new(DDML::new(pool, cache.clone()));
                        let idml = IDML::create(ddml, cache);
                        Database::create(Arc::new(idml), handle)
                    })
                })
            })).unwrap();
            let tree_id = rt.block_on(db.new_fs()).unwrap();
            let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
            (fs, rt)
        }
    });

    test create(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        assert_eq!(mocks.val.0.lookup(1, &OsString::from("x")).unwrap(), ino);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(1, 0, 0);
        let (dirent, _ofs) = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).nth(0)
        .expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_REG);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(dirent.d_fileno as u64, ino);

        // The parent dir's link count should not have increased
        let parent_attr = mocks.val.0.getattr(1).unwrap();
        assert_eq!(parent_attr.nlink, 1);
    }

    /// getattr on the filesystem's root directory
    test getattr(mocks) {
        let inode = mocks.val.0.getattr(1).unwrap();
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
        let ino = mocks.val.0.mkdir(1, &OsString::from("x"), 0o755).unwrap();
        assert_eq!(mocks.val.0.lookup(1, &OsString::from("x")).unwrap(), ino);

        // The new dir should have "." and ".." directory entries
        let mut entries = mocks.val.0.readdir(ino, 0, 0);
        let (dotdot, _) = entries.next().unwrap().unwrap();
        assert_eq!(dotdot.d_type, libc::DT_DIR);
        let dotdot_name = unsafe{
            CStr::from_ptr(&dotdot.d_name as *const c_char)
        };
        assert_eq!(dotdot_name, CString::new("..").unwrap().as_c_str());
        assert_eq!(dotdot.d_fileno, 1);
        let (dot, _) = entries.next().unwrap().unwrap();
        assert_eq!(dot.d_type, libc::DT_DIR);
        let dot_name = unsafe{
            CStr::from_ptr(&dot.d_name as *const c_char)
        };
        assert_eq!(dot_name, CString::new(".").unwrap().as_c_str());
        assert_eq!(dot.d_fileno as u64, ino);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(1, 0, 0);
        let (dirent, _ofs) = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).nth(0)
        .expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_DIR);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(dirent.d_fileno as u64, ino);

        // The parent dir's link count should've increased
        let parent_attr = mocks.val.0.getattr(1).unwrap();
        assert_eq!(parent_attr.nlink, 2);
    }

    // Read a single BlobExtent record
    test read_blob(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let buf = vec![42u8; 4096];
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        // Sync the filesystem to flush the InlineExtent to a BlobExtent
        mocks.val.0.sync();

        let sglist = mocks.val.0.read(ino, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[..]);
    }

    // A read that's smaller than a record, at both ends
    test read_partial_record(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        let sglist = mocks.val.0.read(ino, 1024, 2048).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[1024..3072]);
    }

    // A read that's split across two records
    test read_two_recs(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 0, &buf[0..4096], 0);
        assert_eq!(Ok(4096), r);
        let r = mocks.val.0.write(ino, 4096, &buf[4096..8192], 0);
        assert_eq!(Ok(4096), r);

        let sglist = mocks.val.0.read(ino, 0, 8192).unwrap();
        let db0 = &sglist[0];
        assert_eq!(&db0[..], &buf[0..4096]);
        let db1 = &sglist[1];
        assert_eq!(&db1[..], &buf[4096..8192]);
    }

    test readdir(mocks) {
        let mut entries = mocks.val.0.readdir(1, 0, 0);
        let (dotdot, _) = entries.next().unwrap().unwrap();
        assert_eq!(dotdot.d_type, libc::DT_DIR);
        let dotdot_name = unsafe{
            CStr::from_ptr(&dotdot.d_name as *const c_char)
        };
        assert_eq!(dotdot_name, CString::new("..").unwrap().as_c_str());
        let (dot, _) = entries.next().unwrap().unwrap();
        assert_eq!(dot.d_type, libc::DT_DIR);
        let dot_name = unsafe{
            CStr::from_ptr(&dot.d_name as *const c_char)
        };
        assert_eq!(dot_name, CString::new(".").unwrap().as_c_str());
        assert_eq!(dot.d_fileno, 1);
    }

    test rmdir(mocks) {
        let dirname = OsString::from("x");
        let ino = mocks.val.0.mkdir(1, &dirname, 0o755).unwrap();
        mocks.val.0.rmdir(1, &dirname).unwrap();

        // Make sure it's gone
        assert_eq!(mocks.val.0.getattr(ino).unwrap_err(), libc::ENOENT);
        assert!(mocks.val.0.readdir(1, 0, 0)
            .filter(|r| {
                let dirent = r.unwrap().0;
                dirent.d_name[0] == 'x' as i8
            }).nth(0).is_none());

        // Make sure the parent dir's refcount dropped
        let inode = mocks.val.0.getattr(1).unwrap();
        assert_eq!(inode.nlink, 1);
    }

    test rmdir_enoent(mocks) {
        let dirname = OsString::from("x");
        assert_eq!(mocks.val.0.rmdir(1, &dirname).unwrap_err(), libc::ENOENT);
    }

    test rmdir_enotempty(mocks) {
        let dirname = OsString::from("x");
        let ino = mocks.val.0.mkdir(1, &dirname, 0o755).unwrap();
        mocks.val.0.mkdir(ino, &dirname, 0o755).unwrap();
        assert_eq!(mocks.val.0.rmdir(1, &dirname).unwrap_err(), libc::ENOTEMPTY);
    }

    test statvfs(mocks) {
        let statvfs = mocks.val.0.statvfs();
        assert_eq!(statvfs.f_blocks, 262144);
    }

    // A very simple single record write to an empty file
    test write(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let buf = vec![42u8; 4096];
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        // Check the file size
        let inode = mocks.val.0.getattr(ino).unwrap();
        assert_eq!(inode.size, 4096);

        let sglist = mocks.val.0.read(ino, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[..]);
    }

    // A partial single record write appended to the file's end
    test write_append(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let mut buf0 = vec![0u8; 1024];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 0, &buf0[..], 0);
        assert_eq!(Ok(1024), r);

        let sglist = mocks.val.0.read(ino, 0, 1024).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf0[..]);
    }

    // A partial single record write that needs RMW on both ends
    test write_partial_record(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let mut buf0 = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 0, &buf0[..], 0);
        assert_eq!(Ok(4096), r);
        let buf1 = vec![0u8; 2048];
        let r = mocks.val.0.write(ino, 512, &buf1[..], 0);
        assert_eq!(Ok(2048), r);

        let sglist = mocks.val.0.read(ino, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[0..512], &buf0[0..512]);
        assert_eq!(&db[512..2560], &buf1[..]);
        assert_eq!(&db[2560..], &buf0[2560..]);
    }

    // write can RMW BlobExtents
    test write_partial_blob_record(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let mut buf0 = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 0, &buf0[..], 0);
        assert_eq!(Ok(4096), r);

        // Sync the fs to flush the InlineExtent to a BlobExtent
        mocks.val.0.sync();

        let buf1 = vec![0u8; 2048];
        let r = mocks.val.0.write(ino, 512, &buf1[..], 0);
        assert_eq!(Ok(2048), r);

        let sglist = mocks.val.0.read(ino, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[0..512], &buf0[0..512]);
        assert_eq!(&db[512..2560], &buf1[..]);
        assert_eq!(&db[2560..], &buf0[2560..]);
    }

    // A write to an empty file that's split across two records
    test write_two_recs(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(8192), r);

        // Check the file size
        let inode = mocks.val.0.getattr(ino).unwrap();
        assert_eq!(inode.size, 8192);

        let sglist = mocks.val.0.read(ino, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[0..4096]);
        let sglist = mocks.val.0.read(ino, 4096, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[4096..8192]);
    }

}
