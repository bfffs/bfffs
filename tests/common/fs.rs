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
    use time::Timespec;
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
        assert_eq!(u64::from(dirent.d_fileno), ino);

        // The parent dir's link count should not have increased
        let parent_attr = mocks.val.0.getattr(1).unwrap();
        assert_eq!(parent_attr.nlink, 1);
    }

    // Dumps an FS tree, with enough data to create IntNodes
    test dump(mocks) {
        let ino_w = mocks.val.0.mkdir(1, &OsString::from("w"), 0o755).unwrap();
        let ino_x = mocks.val.0.mkdir(1, &OsString::from("x"), 0o755).unwrap();
        let ino_y = mocks.val.0.mkdir(1, &OsString::from("y"), 0o755).unwrap();
        let ino_z = mocks.val.0.mkdir(1, &OsString::from("z"), 0o755).unwrap();
        // Zero out the timestamp strings
        let attr = SetAttr {
            mode: None,
            uid: None,
            gid: None,
            size: None,
            atime: Some(Timespec{sec: 0, nsec: 0}),
            mtime: Some(Timespec{sec: 0, nsec: 0}),
            ctime: Some(Timespec{sec: 0, nsec: 0}),
            birthtime: Some(Timespec{sec: 0, nsec: 0}),
            flags: None,
        };
        mocks.val.0.setattr(1, attr).unwrap();
        mocks.val.0.setattr(ino_w, attr).unwrap();
        mocks.val.0.setattr(ino_x, attr).unwrap();
        mocks.val.0.setattr(ino_y, attr).unwrap();
        mocks.val.0.setattr(ino_z, attr).unwrap();
        mocks.val.0.sync();

        let mut buf = Vec::with_capacity(1024);
        mocks.val.0.dump(&mut buf);
        let fs_tree = String::from_utf8(buf).unwrap();
        assert_eq!(fs_tree,
r#"---
height: 2
min_fanout: 4
max_fanout: 16
_max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 1
  ptr:
    Addr: 2
---
0:
  Leaf:
    items:
      18474578933339636253:
        DirEntry:
          ino: 5
          dtype: 4
          name:
            Unix:
              - 122
      18478388752068107043:
        DirEntry:
          ino: 1
          dtype: 4
          name:
            Unix:
              - 46
              - 46
      18482701415731367003:
        DirEntry:
          ino: 3
          dtype: 4
          name:
            Unix:
              - 120
      18486768971900222402:
        DirEntry:
          ino: 2
          dtype: 4
          name:
            Unix:
              - 119
      18490468108375165510:
        DirEntry:
          ino: 1
          dtype: 4
          name:
            Unix:
              - 46
      18501635435641183657:
        DirEntry:
          ino: 4
          dtype: 4
          name:
            Unix:
              - 121
      18518801667747479552:
        Inode:
          size: 0
          nlink: 5
          flags: 0
          atime:
            sec: 0
            nsec: 0
          mtime:
            sec: 0
            nsec: 0
          ctime:
            sec: 0
            nsec: 0
          birthtime:
            sec: 0
            nsec: 0
          uid: 0
          gid: 0
          mode: 16877
      36925132825777658659:
        DirEntry:
          ino: 1
          dtype: 4
          name:
            Unix:
              - 46
              - 46
      36937212182084717126:
        DirEntry:
          ino: 2
          dtype: 4
          name:
            Unix:
              - 46
1:
  Leaf:
    items:
      36965545741457031168:
        Inode:
          size: 0
          nlink: 2
          flags: 0
          atime:
            sec: 0
            nsec: 0
          mtime:
            sec: 0
            nsec: 0
          ctime:
            sec: 0
            nsec: 0
          birthtime:
            sec: 0
            nsec: 0
          uid: 0
          gid: 0
          mode: 16877
      55371876899487210275:
        DirEntry:
          ino: 1
          dtype: 4
          name:
            Unix:
              - 46
              - 46
      55383956255794268742:
        DirEntry:
          ino: 3
          dtype: 4
          name:
            Unix:
              - 46
      55412289815166582784:
        Inode:
          size: 0
          nlink: 2
          flags: 0
          atime:
            sec: 0
            nsec: 0
          mtime:
            sec: 0
            nsec: 0
          ctime:
            sec: 0
            nsec: 0
          birthtime:
            sec: 0
            nsec: 0
          uid: 0
          gid: 0
          mode: 16877
      73818620973196761891:
        DirEntry:
          ino: 1
          dtype: 4
          name:
            Unix:
              - 46
              - 46
      73830700329503820358:
        DirEntry:
          ino: 4
          dtype: 4
          name:
            Unix:
              - 46
      73859033888876134400:
        Inode:
          size: 0
          nlink: 2
          flags: 0
          atime:
            sec: 0
            nsec: 0
          mtime:
            sec: 0
            nsec: 0
          ctime:
            sec: 0
            nsec: 0
          birthtime:
            sec: 0
            nsec: 0
          uid: 0
          gid: 0
          mode: 16877
      92265365046906313507:
        DirEntry:
          ino: 1
          dtype: 4
          name:
            Unix:
              - 46
              - 46
      92277444403213371974:
        DirEntry:
          ino: 5
          dtype: 4
          name:
            Unix:
              - 46
      92305777962585686016:
        Inode:
          size: 0
          nlink: 2
          flags: 0
          atime:
            sec: 0
            nsec: 0
          mtime:
            sec: 0
            nsec: 0
          ctime:
            sec: 0
            nsec: 0
          birthtime:
            sec: 0
            nsec: 0
          uid: 0
          gid: 0
          mode: 16877
---
2:
  Int:
    children:
      - key: 0
        txgs:
          start: 0
          end: 1
        ptr:
          Addr: 0
      - key: 36965545741457031168
        txgs:
          start: 0
          end: 1
        ptr:
          Addr: 1
"#);
    }

    /// getattr on the filesystem's root directory
    test getattr(mocks) {
        let attr = mocks.val.0.getattr(1).unwrap();
        assert_eq!(attr.nlink, 1);
        assert_eq!(attr.flags, 0);
        assert!(attr.atime.sec > 0);
        assert!(attr.mtime.sec > 0);
        assert!(attr.ctime.sec > 0);
        assert!(attr.birthtime.sec > 0);
        assert_eq!(attr.uid, 0);
        assert_eq!(attr.gid, 0);
        assert_eq!(attr.mode & 0o7777, 0o755);
        assert_eq!(attr.mode & libc::S_IFMT, libc::S_IFDIR);
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
        assert_eq!(u64::from(dot.d_fileno), ino);

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
        assert_eq!(u64::from(dirent.d_fileno), ino);

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

    test read_empty_file(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let sglist = mocks.val.0.read(ino, 0, 1024).unwrap();
        assert!(sglist.is_empty());
    }

    test read_empty_file_past_start(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let sglist = mocks.val.0.read(ino, 2048, 2048).unwrap();
        assert!(sglist.is_empty());
    }

    // Read a hole within a sparse file
    test read_hole(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 4096, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        let sglist = mocks.val.0.read(ino, 0, 4096).unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);
    }

    // Read a record within a sparse file that is partially occupied by an
    // inline extent
    test read_partial_hole(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let mut buf = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(2048), r);
        let r = mocks.val.0.write(ino, 4096, &buf[..], 0);
        assert_eq!(Ok(2048), r);

        // The file should now have a hole from offset 2048 to 4096
        let sglist = mocks.val.0.read(ino, 2048, 2048).unwrap();
        let db = &sglist[0];
        let expected = [0u8; 2048];
        assert_eq!(&db[..], &expected[..]);
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

    test read_past_eof(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let mut buf = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(2048), r);

        let sglist = mocks.val.0.read(ino, 2048, 1024).unwrap();
        assert!(sglist.is_empty());
    }

    // Read past EOF, in an entirely different record
    test read_well_past_eof(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644).unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        let sglist = mocks.val.0.read(ino, 1 << 30, 4096).unwrap();
        assert!(sglist.is_empty());
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

    // It's allowed for the client of Fs::readdir to drop the iterator without
    // reading all entries.  The FUSE module does that when it runs out of space
    // in the kernel-provided buffer.
    // Just check that nothing panics.
    test readdir_partial(mocks) {
        let mut entries = mocks.val.0.readdir(1, 0, 0);
        let _ = entries.next().unwrap().unwrap();
    }

    #[cfg_attr(feature = "cargo-clippy",
               allow(clippy::block_in_if_condition_stmt))]
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

    #[should_panic]
    test rmdir_enotdir(mocks) {
        let filename = OsString::from("x");
        mocks.val.0.create(1, &filename, 0o644).unwrap();
        mocks.val.0.rmdir(1, &filename).unwrap();
    }

    test rmdir_enotempty(mocks) {
        let dirname = OsString::from("x");
        let ino = mocks.val.0.mkdir(1, &dirname, 0o755).unwrap();
        mocks.val.0.mkdir(ino, &dirname, 0o755).unwrap();
        assert_eq!(mocks.val.0.rmdir(1, &dirname).unwrap_err(), libc::ENOTEMPTY);
    }

    test setattr(mocks) {
        let filename = OsString::from("x");
        let ino = mocks.val.0.create(1, &filename, 0o644).unwrap();
        let mode = 0o1357;
        let uid = 12345;
        let gid = 54321;
        let size = 9999;
        let atime = Timespec {sec: 1, nsec: 2};
        let mtime = Timespec {sec: 3, nsec: 4};
        let ctime = Timespec {sec: 5, nsec: 6};
        let birthtime = Timespec {sec: 7, nsec: 8};
        let flags = 1;
        // TODO: define UF_NODUMP in libc and use that here.
        //let flags = libc::UF_NODUMP;

        let assert = |attr: GetAttr| {
            assert_eq!(attr.nlink, 1); // Shouldn't have been changed
            assert_eq!(attr.flags, flags);
            assert_eq!(attr.atime, atime);
            assert_eq!(attr.mtime, mtime);
            assert_eq!(attr.ctime, ctime);
            assert_eq!(attr.birthtime, birthtime);
            assert_eq!(attr.uid, uid);
            assert_eq!(attr.gid, gid);
            assert_eq!(attr.mode & 0o7777, mode);
            assert_eq!(attr.size, size);
            assert_eq!(attr.mode & libc::S_IFMT, libc::S_IFREG);
        };

        let attr = SetAttr {
            mode: Some(mode),
            uid: Some(uid),
            gid: Some(gid),
            size: Some(size),
            atime: Some(atime),
            mtime: Some(mtime),
            ctime: Some(ctime),
            birthtime: Some(birthtime),
            flags: Some(flags)
        };
        mocks.val.0.setattr(ino, attr).unwrap();
        let attr = mocks.val.0.getattr(ino).unwrap();
        assert(attr);

        // Now test using setattr to update nothing
        let attr = SetAttr {
            mode: None,
            uid: None,
            gid: None,
            size: None,
            atime: None,
            mtime: None,
            ctime: None,
            birthtime: None,
            flags: None,
        };
        mocks.val.0.setattr(ino, attr).unwrap();
        let attr = mocks.val.0.getattr(ino).unwrap();
        assert(attr);
    }

    test statvfs(mocks) {
        let statvfs = mocks.val.0.statvfs();
        assert_eq!(statvfs.f_blocks, 262_144);
    }

    test unlink(mocks) {
        let filename = OsString::from("x");
        let ino = mocks.val.0.create(1, &filename, 0o644).unwrap();
        let r = mocks.val.0.unlink(1, &filename);
        assert_eq!(Ok(()), r);

        // Check that the inode is gone
        let inode = mocks.val.0.getattr(ino);
        assert_eq!(Err(libc::ENOENT), inode, "Inode was not removed");

        // The parent dir should not have an "x" directory entry
        let entries = mocks.val.0.readdir(1, 0, 0);
        let x_de = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).nth(0);
        assert!(x_de.is_none(), "Directory entry was not removed");
    }

    test unlink_enoent(mocks) {
        let filename = OsString::from("x");
        let e = mocks.val.0.unlink(1, &filename).unwrap_err();
        assert_eq!(e, libc::ENOENT);
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

test_suite! {
    name torture;

    use bfffs::{
        common::*,
        common::cache::*,
        common::database::*,
        common::ddml::*,
        common::fs::*,
        common::idml::*,
        common::pool::*,
    };
    use env_logger;
    use futures::{Future, future};
    use rand::{
        Rng,
        RngCore,
        SeedableRng,
        distributions::{Distribution, WeightedIndex},
        thread_rng
    };
    use rand_xorshift::XorShiftRng;
    use std::{
        ffi::OsString,
        fs,
        num::NonZeroU64,
        sync::{Arc, Mutex, Once},
        time::{Duration, Instant},
    };
    use tempdir::TempDir;
    use tokio_io_pool::Runtime;

    #[derive(Clone, Copy, Debug)]
    pub enum Op {
        Clean,
        /// Should be `Sync`, but that word is reserved
        SyncAll,
        RmEnoent,
        Ls,
        Rmdir,
        Mkdir,
        Rm,
        Touch,
        Write,
        Read
    }

    struct TortureTest {
        db: Option<Arc<Database>>,
        dirs: Vec<(u64, u64)>,
        fs: Fs,
        files: Vec<(u64, u64)>,
        rng: XorShiftRng,
        rt: Option<Runtime>,
        w: Vec<(Op, f64)>,
        wi: WeightedIndex<f64>
    }

    impl TortureTest {
        fn check(&mut self) {
            let db = self.db.as_ref().unwrap();
            let rt = self.rt.as_mut().unwrap();
            assert!(rt.block_on(db.check()).unwrap());
        }

        fn clean(&mut self) {
            info!("clean");
            self.db.as_ref().unwrap().clean().wait().unwrap();
            self.check();
        }

        fn mkdir(&mut self) {
            let num: u64 = self.rng.gen();
            let fname = format!("{:x}", num);
            info!("mkdir {}", fname);
            let ino = self.fs.mkdir(1, &OsString::from(&fname), 0o755).unwrap();
            self.dirs.push((num, ino));
        }

        fn ls(&mut self) {
            let idx = self.rng.gen_range(0, self.dirs.len() + 1);
            let (fname, ino) = if idx == self.dirs.len() {
                ("/".to_owned(), 1)
            } else {
                let spec = self.dirs[idx];
                (format!("{:x}", spec.0), spec.1)
            };
            let c = self.fs.readdir(ino, 0, 0).count();
            info!("ls {}: {} entries", fname, c);
        }

        fn new(db: Arc<Database>, fs: Fs, rng: XorShiftRng, rt: Runtime,
               w: Option<Vec<(Op, f64)>>) -> Self
        {
            let w = w.unwrap_or(vec![
                (Op::Clean, 0.001),
                (Op::SyncAll, 0.003),
                (Op::RmEnoent, 1.0),
                (Op::Ls, 5.0),
                (Op::Rmdir, 5.0),
                (Op::Mkdir, 6.0),
                (Op::Rm, 15.0),
                (Op::Touch, 18.0),
                (Op::Write, 25.0),
                (Op::Read, 25.0)
            ]);
            let wi = WeightedIndex::new(w.iter().map(|item| item.1)).unwrap();
            TortureTest{db: Some(db), dirs: Vec::new(), files: Vec::new(), fs,
                        rng, rt: Some(rt), w, wi}
        }

        fn read(&mut self) {
            if !self.files.is_empty() {
                // Pick a random file to read from
                let idx = self.rng.gen_range(0, self.files.len());
                let ino = self.files[idx].1;
                // Pick a random offset within the first 8KB
                let ofs = 2048 * self.rng.gen_range(0, 4);
                info!("read {:x} at offset {}", self.files[idx].0, ofs);
                let r = self.fs.read(ino, ofs, 2048);
                // TODO: check buffer contents
                assert!(r.is_ok());
            }
        }

        fn rm_enoent(&mut self) {
            // Generate a random name that corresponds to no real file, but
            // could be sorted anywhere amongst them.
            let num: u64 = self.rng.gen();
            let fname = format!("{:x}_x", num);
            info!("rm {}", fname);
            assert_eq!(self.fs.unlink(1, &OsString::from(&fname)),
                       Err(Error::ENOENT.into()));
        }

        fn rm(&mut self) {
            if !self.files.is_empty() {
                let idx = self.rng.gen_range(0, self.files.len());
                let fname = format!("{:x}", self.files.remove(idx).0);
                info!("rm {}", fname);
                self.fs.unlink(1, &OsString::from(&fname)).unwrap();
            }
        }

        fn rmdir(&mut self) {
            if !self.dirs.is_empty() {
                let idx = self.rng.gen_range(0, self.dirs.len());
                let fname = format!("{:x}", self.dirs.remove(idx).0);
                info!("rmdir {}", fname);
                self.fs.rmdir(1, &OsString::from(&fname)).unwrap();
            }
        }

        fn shutdown(mut self) {
            drop(self.fs);
            let mut db = Arc::try_unwrap(self.db.take().unwrap())
                .ok().expect("Arc::try_unwrap");
            let mut rt = self.rt.take().unwrap();
            rt.block_on(db.shutdown()).unwrap();
            rt.shutdown_on_idle();
        }

        fn step(&mut self) {
            match self.w[self.wi.sample(&mut self.rng)].0.clone() {
                Op::Clean => self.clean(),
                Op::Ls => self.ls(),
                Op::Mkdir => self.mkdir(),
                Op::Read => self.read(),
                Op::Rm => self.rm(),
                Op::Rmdir => self.rmdir(),
                Op::RmEnoent => self.rm_enoent(),
                Op::SyncAll => self.sync(),
                Op::Touch => self.touch(),
                Op::Write => self.write(),
                //x => println!("{:?}", x)
            }
        }

        fn sync(&mut self) {
            info!("sync");
            self.fs.sync();
        }

        fn touch(&mut self) {
            // The BTree is basically a flat namespace, so there's little test
            // coverage to be gained by testing a hierarchical directory
            // structure.  Instead, we'll stick all files in the root directory,
            // which has inode 1.
            let num: u64 = self.rng.gen();
            let fname = format!("{:x}", num);
            info!("Touch {}", fname);
            let ino = self.fs.create(1, &OsString::from(&fname), 0o644).unwrap();
            self.files.push((num, ino));
        }

        /// Write to a file.
        ///
        /// Writes just 2KB.  This may create inline or on-disk extents.  It may
        /// RMW on-disk extents.  The purpose is to exercise the tree, not large
        /// I/O.
        fn write(&mut self) {
            if !self.files.is_empty() {
                // Pick a random file to write to
                let idx = self.rng.gen_range(0, self.files.len());
                let ino = self.files[idx].1;
                // Pick a random offset within the first 8KB
                let piece: u64 = self.rng.gen_range(0, 4);
                let ofs = 2048 * piece;
                // Use a predictable fill value
                let fill = (ino.wrapping_mul(piece) % u8::max_value as u64)
                    as u8;
                let buf = [fill; 2048];
                //self.rng.fill_bytes(&mut buf);
                info!("write {:x} at offset {}", self.files[idx].0, ofs);
                let r = self.fs.write(ino, ofs, &buf[..], 0);
                assert!(r.is_ok());
            }
        }
    }

    fixture!( mocks(seed: Option<[u8; 16]>, freqs: Option<Vec<(Op, f64)>>,
                    zone_size: u64) -> (TortureTest)
    {
        setup(&mut self) {
            static ENV_LOGGER: Once = Once::new();
            ENV_LOGGER.call_once(|| {
                env_logger::init();
            });

            let mut rt = Runtime::new();
            let handle = rt.handle().clone();
            let len = 1 << 30;  // 1GB
            let tempdir = t!(TempDir::new("test_fs"));
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            drop(file);
            let zone_size = NonZeroU64::new(*self.zone_size);
            let db = rt.block_on(future::lazy(move || {
                Pool::create_cluster(None, 1, 1, zone_size, 0, &[filename])
                .map_err(|_| unreachable!())
                .and_then(|cluster| {
                    Pool::create(String::from("test_fs"), vec![cluster])
                    .map(|pool| {
                        let cache = Arc::new(
                            Mutex::new(
                                Cache::with_capacity(32_000_000)
                            )
                        );
                        let ddml = Arc::new(DDML::new(pool, cache.clone()));
                        let idml = IDML::create(ddml, cache);
                        Arc::new(Database::create(Arc::new(idml), handle))
                    })
                })
            })).unwrap();
            let tree_id = rt.block_on(db.new_fs()).unwrap();
            let fs = Fs::new(db.clone(), rt.handle().clone(), tree_id);
            let mut seed = self.seed.unwrap_or_else(|| {
                let mut seed = [0u8; 16];
                let mut seeder = thread_rng();
                seeder.fill_bytes(&mut seed);
                seed
            });
            println!("Using seed {:?}", &seed);
            // Use XorShiftRng because it's deterministic and seedable
            let rng = XorShiftRng::from_seed(seed);

            TortureTest::new(db, fs, rng, rt, self.freqs.clone())
        }
    });

    fn do_test(mut torture_test: TortureTest, duration: Option<Duration>) {
        // Random torture test.  At each step check the trees and also do one of:
        // *) Clean zones
        // *) Sync
        // *) Remove a nonexisting regular file
        // *) Remove an existing file
        // *) Create a new regular file
        // *) Remove an empty directory
        // *) Create a directory
        // *) List a directory
        // *) Write to a regular file
        // *) Read from a regular file
        let duration = duration.unwrap_or(Duration::from_secs(60));
        let start = Instant::now();
        while start.elapsed() < duration {
            torture_test.step()
        }
        torture_test.shutdown();
    }

    /// Randomly execute a long series of filesystem operations.
    #[ignore = "Slow"]
    test random(mocks((None, None, 512))) {
        do_test(mocks.val, None);
    }

    /// Randomly execute a series of filesystem operations, designed expecially
    /// to stress the cleaner.
    #[ignore = "Slow"]
    test random_clean_zone(mocks((
        None,
        Some(vec![
            (Op::Clean, 0.01),
            (Op::SyncAll, 0.03),
            (Op::Mkdir, 10.0),
            (Op::Touch, 10.0),
        ]),
        512)))
    {
        do_test(mocks.val, Some(Duration::from_secs(10)));
    }
}
