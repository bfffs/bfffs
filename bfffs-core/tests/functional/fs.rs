// vim: tw=80
// Constructs a real filesystem and tests the common FS routines, without
// mounting
mod fs {
    use bfffs_core::{
        ZERO_REGION_LEN,
        cache::*,
        database::*,
        ddml::*,
        fs::*,
        idml::*,
        pool::*,
        property::*
    };
    use futures::TryStreamExt;
    use rand::{Rng, thread_rng};
    use rstest::rstest;
    use std::{
        collections::HashSet,
        ffi::{CString, CStr, OsString, OsStr},
        fs,
        os::raw::c_char,
        os::unix::ffi::OsStrExt,
        slice,
        sync::{Arc, Mutex}
    };
    use tempfile::Builder;

    type Harness = (Fs, Arc<Mutex<Cache>>, Arc<Database>);

    async fn harness(props: Vec<Property>) -> Harness {
        let len = 1 << 30;  // 1GB
        let tempdir = t!(Builder::new().prefix("test_fs").tempdir());
        let filename = tempdir.path().join("vdev");
        let file = t!(fs::File::create(&filename));
        t!(file.set_len(len));
        drop(file);
        let cache = Arc::new(Mutex::new(Cache::with_capacity(1_000_000)));
        let cache2 = cache.clone();
        let cluster = Pool::create_cluster(None, 1, None, 0, &[filename]);
        let pool = Pool::create(String::from("test_fs"), vec![cluster]);
        let ddml = Arc::new(DDML::new(pool, cache2.clone()));
        let idml = IDML::create(ddml, cache2);
        let db = Arc::new(Database::create(Arc::new(idml)));
        let tree_id = db.create_fs(None, "").await.unwrap();
        let fs = Fs::new(db.clone(), tree_id).await;
        for prop in props.into_iter() {
            fs.set_prop(prop).await.unwrap();
        }
        (fs, cache, db)
    }

    /// Use a small recordsize for most tests, because it's faster to test
    /// conditions that require multiple records.
    async fn harness4k() -> Harness {
        harness(vec![Property::RecordSize(12)]).await
    }

    async fn harness8k() -> Harness {
        harness(vec![Property::RecordSize(13)]).await
    }

    fn assert_dirents_collide(name0: &OsStr, name1: &OsStr) {
        use bfffs_core::fs_tree::ObjKey;

        let objkey0 = ObjKey::dir_entry(name0);
        let objkey1 = ObjKey::dir_entry(name1);
        // If this assertion fails, then the on-disk format has changed.  If
        // that was intentional, then generate new has collisions by running
        // examples/hash_collision.rs.
        assert_eq!(objkey0.offset(), objkey1.offset());
    }

    fn assert_extattrs_collide(ns0: ExtAttrNamespace, name0: &OsStr,
                               ns1: ExtAttrNamespace, name1: &OsStr)
    {
        use bfffs_core::fs_tree::ObjKey;

        let objkey0 = ObjKey::extattr(ns0, name0);
        let objkey1 = ObjKey::extattr(ns1, name1);
        // If this assertion fails, then the on-disk format has changed.  If
        // that was intentional, then generate new has collisions by running
        // examples/hash_collision.rs.
        assert_eq!(objkey0.offset(), objkey1.offset());
    }

    /// Assert that some combination of timestamps have changed since they were
    /// last cleared.
    async fn assert_ts_changed(ds: &Fs, fd: &FileData, atime: bool, mtime: bool,
                               ctime: bool, birthtime: bool)
    {
        let attr = ds.getattr(fd).await.unwrap();
        let ts0 = Timespec{sec: 0, nsec: 0};
        assert!(atime ^ (attr.atime == ts0));
        assert!(mtime ^ (attr.mtime == ts0));
        assert!(ctime ^ (attr.ctime == ts0));
        assert!(birthtime ^ (attr.birthtime == ts0));
    }

    async fn clear_timestamps(ds: &Fs, fd: &FileData) {
        let attr = SetAttr {
            perm: None,
            uid: None,
            gid: None,
            size: None,
            atime: Some(Timespec{sec: 0, nsec: 0}),
            mtime: Some(Timespec{sec: 0, nsec: 0}),
            ctime: Some(Timespec{sec: 0, nsec: 0}),
            birthtime: Some(Timespec{sec: 0, nsec: 0}),
            flags: None,
        };
        ds.setattr(fd, attr).await.unwrap();
    }

    /// Helper method to read the full contents of a directory
    async fn readdir_all(fs: &Fs, fd: &FileData, offs: i64)
        -> Vec<(libc::dirent, i64)>
    {
        fs.readdir(fd, offs)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn create() {
        let (fs, _cache, _db) = harness4k().await;
        let uid = 12345;
        let gid = 54321;
        let name = OsStr::from_bytes(b"x");
        let root = fs.root();
        let fd0 = fs.create(&root, name, 0o644, uid, gid).await.unwrap();
        let fd1 = fs.lookup(None, &root, name).await.unwrap();
        assert_eq!(fd1.ino(), fd0.ino());

        // The parent dir should have an "x" directory entry
        let (dirent, _ofs) = readdir_all(&fs, &root, 0).await
            .into_iter()
            .find(|(dirent, _ofs)| {
                dirent.d_name[0] == 'x' as i8
            }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_REG);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(u64::from(dirent.d_fileno), fd0.ino());

        // The parent dir's link count should not have increased
        let parent_attr = fs.getattr(&root).await.unwrap();
        assert_eq!(parent_attr.nlink, 1);

        // Check the new file's attributes
        let attr = fs.getattr(&fd1).await.unwrap();
        assert_eq!(attr.ino, fd1.ino());
        assert_eq!(attr.size, 0);
        assert_eq!(attr.bytes, 0);
        assert_ne!(attr.atime.sec, 0);
        assert_eq!(attr.atime, attr.mtime);
        assert_eq!(attr.atime, attr.ctime);
        assert_eq!(attr.atime, attr.birthtime);
        assert_eq!(attr.mode.0, libc::S_IFREG | 0o644);
        assert_eq!(attr.nlink, 1);
        assert_eq!(attr.uid, uid);
        assert_eq!(attr.gid, gid);
        assert_eq!(attr.blksize, 4096);
        assert_eq!(attr.flags, 0);
    }

    /// Creating a file that already exists should panic.  It is the
    /// responsibility of the VFS to prevent this error when you call
    /// open(_, O_CREAT)
    #[should_panic]
    #[tokio::test]
    async fn create_eexist() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let _fd = fs.create(&root, &filename, 0o644, 0, 0).await
            .unwrap();
        fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
    }

    /// Create should update the parent dir's timestamps
    #[tokio::test]
    async fn create_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        clear_timestamps(&fs, &root).await;

        fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await.unwrap();
        assert_ts_changed(&fs, &root, false, true, true, false).await;
    }

    /// Deallocate a whole extent
    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn deallocate_whole_extent(#[case] blobs: bool) {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        clear_timestamps(&fs, &fd).await;
        if blobs {
            fs.sync().await;        // Flush it to a BlobExtent
        }

        assert!(fs.deallocate(&fd, 0, 4096).await.is_ok());

        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.bytes, 4096);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&fs, &fd, false, true, true, false).await;

        // Finally, read the deallocated record.  It should be a hole
        let sglist = fs.read(&fd, 0, 4096).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);
    }

    /// Deallocating a hole is a no-op
    #[tokio::test]
    async fn deallocate_hole() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let attr = SetAttr {
            size: Some(8192),
            .. Default::default()
        };
        fs.setattr(&fd, attr).await.unwrap();
        clear_timestamps(&fs, &fd).await;

        // Deallocate the hole
        assert!(fs.deallocate(&fd, 0, 4096).await.is_ok());

        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.bytes, 0);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&fs, &fd, false, true, true, false).await;
    }

    /// Deallocate multiple extents, some blob and some inline
    #[tokio::test]
    async fn deallocate_multiple_extents() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        fs.sync().await;        // Flush them to BlobExtents
        // And write some inline extents, too
        let r = fs.write(&fd, 8192, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        clear_timestamps(&fs, &fd).await;

        assert!(fs.deallocate(&fd, 0, 16384).await.is_ok());

        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.bytes, 0);
        assert_eq!(attr.size, 16384);
        assert_ts_changed(&fs, &fd, false, true, true, false).await;

        // Finally, read the deallocated area.  It should be a hole.
        let sglist = fs.read(&fd, 0, 16384).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 16384];
        assert_eq!(&db[..], &expected[..]);
    }

    /// Deallocate the left part of an extent
    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn deallocate_left_half_of_extent(#[case] blobs: bool) {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        clear_timestamps(&fs, &fd).await;
        if blobs {
            fs.sync().await;        // Flush it to a BlobExtent
        }

        assert!(fs.deallocate(&fd, 0, 6144).await.is_ok());
        let attr = fs.getattr(&fd).await.unwrap();
        // The partially deallocated extent still takes up space
        assert_eq!(attr.bytes, 4096);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&fs, &fd, false, true, true, false).await;

        // Finally, read the partially deallocated record.  It should have a
        // hole in the middle.
        let sglist = fs.read(&fd, 0, 8192).await.unwrap();
        let zbuf = [0u8; 4096];
        assert_eq!(&sglist[0][..], &zbuf[..]);
        assert_eq!(&sglist[1][..2048], &zbuf[..2048]);
        assert_eq!(&sglist[1][2048..], &buf[6144..]);
    }

    /// Deallocate the left part of a record which is already a hole
    #[tokio::test]
    async fn deallocate_left_half_of_hole() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let attr = SetAttr {
            size: Some(8192),
            .. Default::default()
        };
        fs.setattr(&fd, attr).await.unwrap();
        clear_timestamps(&fs, &fd).await;

        assert!(fs.deallocate(&fd, 0, 6144).await.is_ok());
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.bytes, 0);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&fs, &fd, false, true, true, false).await;

        // Finally, read the partially deallocated record.
        let sglist = fs.read(&fd, 0, 8192).await.unwrap();
        let zbuf = [0u8; 8192];
        assert_eq!(&sglist[0][..], &zbuf[..]);
    }

    /// Deallocate the middle of an extent
    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn deallocate_middle_of_extent(#[case] blobs: bool) {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);
        clear_timestamps(&fs, &fd).await;
        if blobs {
            fs.sync().await;        // Flush it to a BlobExtent
        }

        assert!(fs.deallocate(&fd, 1024, 2048).await.is_ok());
        let attr = fs.getattr(&fd).await.unwrap();
        // The partially deallocated extent still takes up space
        assert_eq!(attr.bytes, 4096);
        assert_eq!(attr.size, 4096);
        assert_ts_changed(&fs, &fd, false, true, true, false).await;

        // Finally, read the partially deallocated record.  It should have a
        // hole in the middle.
        let sglist = fs.read(&fd, 0, 4096).await.unwrap();
        let db = &sglist[0];
        let zbuf = [0u8; 2048];
        assert_eq!(&db[0..1024], &buf[0..1024]);
        assert_eq!(&db[1024..3072], &zbuf[0..2048]);
        assert_eq!(&db[3072..4096], &buf[3072..4096]);
    }

    /// Deallocate the right part of an extent
    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn deallocate_right_half_of_extent(#[case] blobs: bool) {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        clear_timestamps(&fs, &fd).await;
        if blobs {
            fs.sync().await;        // Flush it to a BlobExtent
        }

        assert!(fs.deallocate(&fd, 2048, 6144).await.is_ok());
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.bytes, 2048);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&fs, &fd, false, true, true, false).await;

        // Finally, read the partially deallocated record.  It should have a
        // hole in the middle.
        let sglist = fs.read(&fd, 0, 8192).await.unwrap();
        let zbuf = [0u8; 6144];
        assert_eq!(&sglist[0][..2048], &buf[..2048]);
        assert_eq!(&sglist[1][..], &zbuf[..]);
    }

    /// Deallocate the right part of a record which happens to be a hole
    #[tokio::test]
    async fn deallocate_right_hole() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let attr = SetAttr {
            size: Some(8192),
            .. Default::default()
        };
        fs.setattr(&fd, attr).await.unwrap();
        clear_timestamps(&fs, &fd).await;

        assert!(fs.deallocate(&fd, 2048, 6144).await.is_ok());
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.bytes, 0);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&fs, &fd, false, true, true, false).await;

        // Finally, read the partially deallocated record.
        let sglist = fs.read(&fd, 0, 8192).await.unwrap();
        let zbuf = [0u8; 8192];
        assert_eq!(&sglist[0][..], &zbuf[..]);
    }

    #[tokio::test]
    async fn deallocate_parts_of_two_records() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        clear_timestamps(&fs, &fd).await;

        assert!(fs.deallocate(&fd, 3072, 2048).await.is_ok());
        let attr = fs.getattr(&fd).await.unwrap();
        // The partially deallocated extents still takes some space on the right
        // record, but not on the left.
        assert_eq!(attr.bytes, 7168);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&fs, &fd, false, true, true, false).await;

        // Finally, read the partially deallocated records.  They should have a
        // hole in the middle.
        let sglist = fs.read(&fd, 0, 8192).await.unwrap();
        {
            // These assertions are implementation-specific.  The sglist shape
            // could change.
            assert_eq!(3, sglist.len());
            assert_eq!(3072, sglist[0].len());
            assert_eq!(1024, sglist[1].len());
            assert_eq!(4096, sglist[2].len());
        }
        let zbuf = [0u8; 1024];
        assert_eq!(&sglist[0][..3072], &buf[..3072]);
        assert_eq!(&sglist[1][..], &zbuf[..]);
        assert_eq!(&sglist[2][..1024], &zbuf[..]);
        assert_eq!(&sglist[2][1024..], &buf[5120..]);
    }

    /// Deallocating past EoF is a no-op
    #[tokio::test]
    async fn deallocate_past_eof() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        clear_timestamps(&fs, &fd).await;

        // Deallocate past EoF
        assert!(fs.deallocate(&fd, 0, 4096).await.is_ok());
        assert_ts_changed(&fs, &fd, false, false, false, false).await;
    }

    #[tokio::test]
    async fn deleteextattr() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, ns, &name, &value[..]).await.unwrap();
        fs.deleteextattr(&fd, ns, &name).await.unwrap();
        assert_eq!(fs.getextattr(&fd, ns, &name).await.unwrap_err(),
            libc::ENOATTR);
    }

    /// deleteextattr with a hash collision.
    #[tokio::test]
    async fn deleteextattr_collision() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();

        // First try deleting the attributes in order
        fs.setextattr(&fd, ns0, &name0, &value0[..]).await.unwrap();
        fs.setextattr(&fd, ns1, &name1, &value1[..]).await.unwrap();
        fs.deleteextattr(&fd, ns0, &name0).await.unwrap();
        assert!(fs.getextattr(&fd, ns0, &name0).await.is_err());
        assert!(fs.getextattr(&fd, ns1, &name1).await.is_ok());
        fs.deleteextattr(&fd, ns1, &name1).await.unwrap();
        assert!(fs.getextattr(&fd, ns0, &name0).await.is_err());
        assert!(fs.getextattr(&fd, ns1, &name1).await.is_err());

        // Repeat, this time out-of-order
        fs.setextattr(&fd, ns0, &name0, &value0[..]).await.unwrap();
        fs.setextattr(&fd, ns1, &name1, &value1[..]).await.unwrap();
        fs.deleteextattr(&fd, ns1, &name1).await.unwrap();
        assert!(fs.getextattr(&fd, ns0, &name0).await.is_ok());
        assert!(fs.getextattr(&fd, ns1, &name1).await.is_err());
        fs.deleteextattr(&fd, ns0, &name0).await.unwrap();
        assert!(fs.getextattr(&fd, ns0, &name0).await.is_err());
        assert!(fs.getextattr(&fd, ns1, &name1).await.is_err());
    }

    /// deleteextattr of a nonexistent attribute that hash-collides with an
    /// existing one.
    #[tokio::test]
    async fn deleteextattr_collision_enoattr() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];

        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();

        fs.setextattr(&fd, ns0, &name0, &value0[..]).await.unwrap();

        assert_eq!(fs.deleteextattr(&fd, ns1, &name1).await,
                   Err(libc::ENOATTR));
        assert!(fs.getextattr(&fd, ns0, &name0).await.is_ok());
    }

    #[tokio::test]
    async fn deleteextattr_enoattr() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let ns = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        assert_eq!(fs.deleteextattr(&fd, ns, &name).await,
                   Err(libc::ENOATTR));
    }

    /// rmextattr(2) should not modify any timestamps
    #[tokio::test]
    async fn deleteextattr_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, ns, &name, &value[..]).await.unwrap();
        clear_timestamps(&fs, &fd).await;

        fs.deleteextattr(&fd, ns, &name).await.unwrap();
        assert_ts_changed(&fs, &fd, false, false, false, false).await;
    }

    // Dumps a nearly empty FS tree.  All of the real work is done in
    // Tree::dump, so the bulk of testing is in the tree tests.
    #[tokio::test]
    async fn dump_fs() {
        let (fs, _cache, _db) = harness(vec![]).await;
        let mut buf = Vec::with_capacity(1024);
        let root = fs.root();
        // Sync before clearing timestamps to improve determinism; the timed
        // flusher may or may not have already flushed the tree.
        fs.sync().await;
        // Clear timestamps to make the dump output deterministic
        clear_timestamps(&fs, &root).await;
        fs.sync().await;
        fs.dump_fs(&mut buf).await.unwrap();

        let fs_tree = String::from_utf8(buf).unwrap();
        let expected = r#"---
limits:
  min_int_fanout: 91
  max_int_fanout: 364
  min_leaf_fanout: 576
  max_leaf_fanout: 2302
  _max_size: 4194304
root:
  height: 1
  elem:
    key: 0-0-00000000000000
    txgs:
      start: 1
      end: 2
    ptr:
      Addr: 3
...
---
3:
  Leaf:
    credit: 0
    items:
      1-0-706caad497db23:
        DirEntry:
          ino: 1
          dtype: 4
          name: ".."
      1-0-9b56c722640a46:
        DirEntry:
          ino: 1
          dtype: 4
          name: "."
      1-1-00000000000000:
        Inode:
          size: 0
          bytes: 0
          nlink: 1
          flags: 0
          atime: "1970-01-01T00:00:00Z"
          mtime: "1970-01-01T00:00:00Z"
          ctime: "1970-01-01T00:00:00Z"
          birthtime: "1970-01-01T00:00:00Z"
          uid: 0
          gid: 0
          perm: 493
          file_type: Dir
"#;
        pretty_assertions::assert_eq!(expected, fs_tree);
    }

    #[tokio::test]
    async fn get_prop_default() {
        let (fs, _cache, _db) = harness4k().await;

        let (val, source) = fs.get_prop(PropertyName::Atime)
            .await
            .unwrap();
        assert_eq!(val, Property::default_value(PropertyName::Atime));
        assert_eq!(source, PropertySource::Default);
    }

    /// getattr on the filesystem's root directory
    #[tokio::test]
    async fn getattr() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let attr = fs.getattr(&root).await.unwrap();
        assert_eq!(attr.nlink, 1);
        assert_eq!(attr.blksize, 4096);
        assert_eq!(attr.flags, 0);
        assert!(attr.atime.sec > 0);
        assert!(attr.mtime.sec > 0);
        assert!(attr.ctime.sec > 0);
        assert!(attr.birthtime.sec > 0);
        assert_eq!(attr.uid, 0);
        assert_eq!(attr.gid, 0);
        assert_eq!(attr.mode.perm(), 0o755);
        assert_eq!(attr.mode.file_type(), libc::S_IFDIR);
    }

    /// Regular files' st_blksize should equal the record size
    #[tokio::test]
    async fn getattr_4k() {
        let (fs, _cache, _db) = harness4k().await;
        let name = OsStr::from_bytes(b"x");
        let root = fs.root();
        let fd = fs.create(&root, name, 0o644, 0, 0).await.unwrap();
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.blksize, 4096);
    }

    /// Regular files' st_blksize should equal the record size
    #[tokio::test]
    async fn getattr_8k() {
        let (fs, _cache, _db) = harness8k().await;
        let name = OsStr::from_bytes(b"y");
        let root = fs.root();
        let fd = fs.create(&root, name, 0o644, 0, 0).await.unwrap();
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.blksize, 8192);
    }

    // Get an extended attribute.  Very short extended attributes will remain
    // inline forever.
    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn getextattr(#[case] on_disk: bool) {
        let (fs, cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let namespace = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, namespace, &name, &value[..]).await.unwrap();

        if on_disk {
            // Sync the filesystem to store the InlineExtent on disk
            fs.sync().await;

            // Drop cache
            cache.lock().unwrap().drop_cache();
        }

        assert_eq!(fs.getextattrlen(&fd, namespace, &name).await.unwrap(),
                   3);
        let v = fs.getextattr(&fd, namespace, &name).await.unwrap();
        assert_eq!(&v[..], &value);
    }

    /// Read a large extattr as a blob
    #[tokio::test]
    async fn getextattr_blob() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = vec![42u8; 4096];
        let namespace = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, namespace, &name, &value[..]).await.unwrap();

        // Sync the filesystem to flush the InlineExtent to a BlobExtent
        fs.sync().await;

        assert_eq!(fs.getextattrlen(&fd, namespace, &name).await.unwrap(),
                   4096);
        let v = fs.getextattr(&fd, namespace, &name).await.unwrap();
        assert_eq!(&v[..], &value[..]);
    }

    /// A collision between a blob extattr and an inline one.  Get the blob
    /// extattr.
    #[tokio::test]
    async fn getextattr_blob_collision() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = vec![42u8; 4096];

        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();

        fs.setextattr(&fd, ns0, &name0, &value0[..]).await.unwrap();
        fs.setextattr(&fd, ns1, &name1, &value1[..]).await.unwrap();
        fs.sync().await; // Flush the large xattr into a blob
        assert_eq!(fs.getextattrlen(&fd, ns1, &name1).await.unwrap(), 4096);
        let v1 = fs.getextattr(&fd, ns1, &name1).await.unwrap();
        assert_eq!(&v1[..], &value1[..]);
    }

    /// setextattr and getextattr with a hash collision.
    #[tokio::test]
    async fn getextattr_collision() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();

        fs.setextattr(&fd, ns0, &name0, &value0[..]).await.unwrap();
        fs.setextattr(&fd, ns1, &name1, &value1[..]).await.unwrap();
        assert_eq!(fs.getextattrlen(&fd, ns0, &name0).await.unwrap(), 3);
        let v0 = fs.getextattr(&fd, ns0, &name0).await.unwrap();
        assert_eq!(&v0[..], &value0);
        assert_eq!(fs.getextattrlen(&fd, ns1, &name1).await.unwrap(), 4);
        let v1 = fs.getextattr(&fd, ns1, &name1).await.unwrap();
        assert_eq!(&v1[..], &value1);
    }

    // The same attribute name exists in two namespaces
    #[tokio::test]
    async fn getextattr_dual_namespaces() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value1 = [1u8, 2, 3];
        let value2 = [4u8, 5, 6, 7];
        let ns1 = ExtAttrNamespace::User;
        let ns2 = ExtAttrNamespace::System;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, ns1, &name, &value1[..]).await.unwrap();
        fs.setextattr(&fd, ns2, &name, &value2[..]).await.unwrap();

        assert_eq!(fs.getextattrlen(&fd, ns1, &name).await.unwrap(), 3);
        let v1 = fs.getextattr(&fd, ns1, &name).await.unwrap();
        assert_eq!(&v1[..], &value1);

        assert_eq!(fs.getextattrlen(&fd, ns2, &name).await.unwrap(), 4);
        let v2 = fs.getextattr(&fd, ns2, &name).await.unwrap();
        assert_eq!(&v2[..], &value2);
    }

    // The file exists, but its extended attribute does not
    #[tokio::test]
    async fn getextattr_enoattr() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let namespace = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        assert_eq!(fs.getextattrlen(&fd, namespace, &name).await,
                   Err(libc::ENOATTR));
        assert_eq!(fs.getextattr(&fd, namespace, &name).await,
                   Err(libc::ENOATTR));
    }

    // The file does not exist.  Fortunately, VOP_GETEXTATTR(9) does not require
    // us to distinguish this from the ENOATTR case.
    #[tokio::test]
    async fn getextattr_enoent() {
        let (fs, _cache, _db) = harness4k().await;
        let name = OsString::from("foo");
        let namespace = ExtAttrNamespace::User;
        let fd = FileData::new_for_tests(Some(1), 9999);
        assert_eq!(fs.getextattrlen(&fd, namespace, &name).await,
                   Err(libc::ENOATTR));
        assert_eq!(fs.getextattr(&fd, namespace, &name).await,
                   Err(libc::ENOATTR));
    }

    /// getextattr(2) should not modify any timestamps
    #[tokio::test]
    async fn getextattr_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let namespace = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, namespace, &name, &value[..]).await.unwrap();
        clear_timestamps(&fs, &fd).await;

        fs.getextattr(&fd, namespace, &name).await.unwrap();
        assert_ts_changed(&fs, &fd, false, false, false, false).await;
    }

    // Lookup a directory by its inode number without knowing its parent
    #[tokio::test]
    async fn ilookup_dir() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let fd0 = fs.mkdir(&root, &filename, 0o755, 0, 0).await.unwrap();
        let ino = fd0.ino();
        fs.inactive(fd0).await;

        let fd1 = fs.ilookup(ino).await.unwrap();
        assert_eq!(fd1.ino(), ino);
        assert_eq!(fd1.lookup_count, 1);
        assert_eq!(fd1.parent(), Some(root.ino()));
    }

    // Try and fail to lookup a file by its inode nubmer
    #[tokio::test]
    async fn ilookup_enoent() {
        let (fs, _cache, _db) = harness4k().await;
        let ino = 123456789;

        let e = fs.ilookup(ino).await.unwrap_err();
        assert_eq!(e, libc::ENOENT);
    }

    // Lookup a regular file by its inode number without knowing its parent
    #[tokio::test]
    async fn ilookup_file() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let fd0 = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        let ino = fd0.ino();
        fs.inactive(fd0).await;

        let fd1 = fs.ilookup(ino).await.unwrap();
        assert_eq!(fd1.ino(), ino);
        assert_eq!(fd1.lookup_count, 1);
        assert_eq!(fd1.parent(), None);
    }

    #[tokio::test]
    async fn link() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let fd = fs.create(&root, &src, 0o644, 0, 0).await.unwrap();
        fs.link(&root, &fd, &dst).await.unwrap();

        // The target's link count should've increased
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.nlink, 2);

        // The parent should have a new directory entry
        assert_eq!(fs.lookup(None, &root, &dst).await.unwrap().ino(),
            fd.ino());
    }

    /// link(2) should update the inode's ctime
    #[tokio::test]
    async fn link_ctime() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let fd = fs.create(&root, &src, 0o644, 0, 0).await.unwrap();
        clear_timestamps(&fs, &fd).await;
        fs.link(&root, &fd, &dst).await.unwrap();
        assert_ts_changed(&fs, &fd, false, false, true, false).await;
    }

    ///link(2) should update the parent's mtime and ctime
    #[tokio::test]
    async fn link_parent_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let fd = fs.create(&root, &src, 0o644, 0, 0).await.unwrap();
        clear_timestamps(&fs, &root).await;

        fs.link(&root, &fd, &dst).await.unwrap();
        assert_ts_changed(&fs, &root, false, true, true, false).await;
    }


    /// Helper for FreeBSD-style VFS
    ///
    /// In due course this should move into the FreeBSD implementation of
    /// `vop_listextattr`, and the test should move into that file, too.
    fn listextattr_lenf(ns: ExtAttrNamespace)
        -> impl Fn(&ExtAttr) -> u32 + Send + 'static
    {
        move |extattr: &ExtAttr| {
            if ns == extattr.namespace() {
                let name = extattr.name();
                assert!(name.len() as u32 <= u32::from(u8::max_value()));
                1 + name.as_bytes().len() as u32
            } else {
                0
            }
        }
    }

    /// Helper for FreeBSD-style VFS
    ///
    /// In due course this should move into the FreeBSD implementation of
    /// `vop_listextattr`, and the test should move into that file, too.
    fn listextattr_lsf(ns: ExtAttrNamespace)
        -> impl Fn(&mut Vec<u8>, &ExtAttr) + Send + 'static
    {
        move |buf: &mut Vec<u8>, extattr: &ExtAttr| {
            if ns == extattr.namespace() {
                assert!(extattr.name().len() <= u8::max_value() as usize);
                buf.push(extattr.name().len() as u8);
                buf.extend_from_slice(extattr.name().as_bytes());
            }
        }
    }

    #[tokio::test]
    async fn listextattr() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let name2 = OsString::from("bar");
        let ns = ExtAttrNamespace::User;
        let value = [0u8, 1, 2];
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, ns, &name1, &value[..]).await.unwrap();
        fs.setextattr(&fd, ns, &name2, &value[..]).await.unwrap();

        // expected has the form of <length as u8><value as [u8]>...
        // values are _not_ null terminated.
        // There is no requirement on the order of names
        let expected = b"\x03bar\x03foo";

        let lenf = self::listextattr_lenf(ns);
        let lsf = self::listextattr_lsf(ns);
        assert_eq!(fs.listextattrlen(&fd, lenf).await.unwrap(), 8);
        assert_eq!(&fs.listextattr(&fd, 64, lsf).await.unwrap()[..],
                   &expected[..]);
    }

    /// setextattr and listextattr with a cross-namespace hash collision.
    #[tokio::test]
    async fn listextattr_collision_separate_namespaces() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();

        fs.setextattr(&fd, ns0, &name0, &value0[..]).await.unwrap();
        fs.setextattr(&fd, ns1, &name1, &value1[..]).await.unwrap();

        let expected0 = b"\x0aBWCdLQkApB";
        let lenf0 = self::listextattr_lenf(ns0);
        let lsf0 = self::listextattr_lsf(ns0);
        assert_eq!(fs.listextattrlen(&fd, lenf0).await.unwrap(), 11);
        assert_eq!(&fs.listextattr(&fd, 64, lsf0).await.unwrap()[..],
                   &expected0[..]);

        let expected1 = b"\x0aD6tLLI4mys";
        let lenf1 = self::listextattr_lenf(ns1);
        let lsf1 = self::listextattr_lsf(ns1);
        assert_eq!(fs.listextattrlen(&fd, lenf1).await.unwrap(), 11);
        assert_eq!(&fs.listextattr(&fd, 64, lsf1).await.unwrap()[..],
                   &expected1[..]);
    }

    #[tokio::test]
    async fn listextattr_dual_namespaces() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let name2 = OsString::from("bean");
        let ns1 = ExtAttrNamespace::User;
        let ns2 = ExtAttrNamespace::System;
        let value1 = [0u8, 1, 2];
        let value2 = [3u8, 4, 5];

        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, ns1, &name1, &value1[..]).await.unwrap();
        fs.setextattr(&fd, ns2, &name2, &value2[..]).await.unwrap();

        // Test queries for a single namespace
        let lenf = self::listextattr_lenf(ns1);
        let lsf = self::listextattr_lsf(ns1);
        assert_eq!(fs.listextattrlen(&fd, lenf).await, Ok(4));
        assert_eq!(&fs.listextattr(&fd, 64, lsf).await.unwrap()[..],
                   &b"\x03foo"[..]);
        let lenf = self::listextattr_lenf(ns2);
        let lsf = self::listextattr_lsf(ns2);
        assert_eq!(fs.listextattrlen(&fd, lenf).await, Ok(5));
        assert_eq!(&fs.listextattr(&fd, 64, lsf).await.unwrap()[..],
                   &b"\x04bean"[..]);
    }

    #[tokio::test]
    async fn listextattr_empty() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        let lenf = self::listextattr_lenf(ExtAttrNamespace::User);
        let lsf = self::listextattr_lsf(ExtAttrNamespace::User);
        assert_eq!(fs.listextattrlen(&fd, lenf).await, Ok(0));
        assert!(fs.listextattr(&fd, 64, lsf).await.unwrap().is_empty());
    }

    /// Lookup of a directory entry that has a hash collision
    #[tokio::test]
    async fn lookup_collision() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = fs.create(&root, &filename0, 0o644, 0, 0).await.unwrap();
        let fd1 = fs.create(&root, &filename1, 0o644, 0, 0).await.unwrap();

        assert_eq!(fs.lookup(None, &root, &filename0).await.unwrap().ino(),
            fd0.ino());
        assert_eq!(fs.lookup(None, &root, &filename1).await.unwrap().ino(),
            fd1.ino());
    }

    #[tokio::test]
    async fn lookup_dot() {
        let (fs, _cache, _db) = harness4k().await;
        let name0 = OsStr::from_bytes(b"x");
        let dotname = OsStr::from_bytes(b".");

        let root = fs.root();
        let fd0 = fs.mkdir(&root, name0, 0o755, 0, 0).await.unwrap();

        let fd1 = fs.lookup(Some(&root), &fd0, dotname).await.unwrap();
        assert_eq!(fd1.ino(), fd0.ino());
        assert_eq!(fd1.parent(), Some(root.ino()));
    }

    #[tokio::test]
    async fn lookup_dotdot() {
        let (fs, _cache, _db) = harness4k().await;
        let name0 = OsStr::from_bytes(b"x");
        let name1 = OsStr::from_bytes(b"y");
        let dotdotname = OsStr::from_bytes(b"..");

        let root = fs.root();
        let fd0 = fs.mkdir(&root, name0, 0o755, 0, 0).await.unwrap();
        let fd1 = fs.mkdir(&fd0, name1, 0o755, 0, 0).await.unwrap();

        let fd2 = fs.lookup(Some(&fd0), &fd1, dotdotname).await.unwrap();
        assert_eq!(fd2.ino(), fd0.ino());
        assert_eq!(fd2.parent(), Some(root.ino()));
    }

    #[tokio::test]
    async fn lookup_enoent() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("nonexistent");
        assert_eq!(fs.lookup(None, &root, &filename).await.unwrap_err(),
            libc::ENOENT);
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn lseek(#[case] blobs: bool) {
        use SeekWhence::{Data, Hole};
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();

        // Create a file like this:
        // |      |======|      |======|>
        // | hole | data | hole | data |EOF
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        let buf = vec![42u8; 4096];
        fs.write(&fd, 4096, &buf[..], 0).await.unwrap();
        fs.write(&fd, 12288, &buf[..], 0).await.unwrap();
        if blobs {
            // Sync the filesystem to flush the InlineExtents to BlobExtents
            fs.sync().await;
        }

        // SeekData past EOF
        assert_eq!(Err(libc::ENXIO), fs.lseek(&fd, 16485, Data).await);
        // SeekHole past EOF
        assert_eq!(Err(libc::ENXIO), fs.lseek(&fd, 16485, Hole).await);
        // SeekHole with no hole until EOF
        assert_eq!(Ok(16384), fs.lseek(&fd, 12288, Hole).await);
        // SeekData at start of data
        assert_eq!(Ok(4096), fs.lseek(&fd, 4096, Data).await);
        // SeekHole at start of hole
        assert_eq!(Ok(8192), fs.lseek(&fd, 8192, Hole).await);
        // SeekData in middle of data
        assert_eq!(Ok(6144), fs.lseek(&fd, 6144, Data).await);
        // SeekHole in middle of hole
        assert_eq!(Ok(2048), fs.lseek(&fd, 2048, Hole).await);
        // SeekData before some data
        assert_eq!(Ok(4096), fs.lseek(&fd, 2048, Data).await);
        // SeekHole before a hole
        assert_eq!(Ok(8192), fs.lseek(&fd, 6144, Hole).await);
    }

    // SeekData should return ENXIO if there is no data until EOF
    #[tokio::test]
    async fn lseek_data_before_eof() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();

        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        let attr = SetAttr {
            size: Some(4096),
            .. Default::default()
        };
        fs.setattr(&fd, attr).await.unwrap();

        let r = fs.lseek(&fd, 0, SeekWhence::Data).await;
        assert_eq!(r, Err(libc::ENXIO));
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn lseek_last_hole(#[case] blobs: bool) {
        use SeekWhence::Hole;
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();

        // Create a file like this:
        // |======|      |>
        // | data | hole |EOF
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        let buf = vec![42u8; 4096];
        fs.write(&fd, 0, &buf[..], 0).await.unwrap();
        let attr = SetAttr {
            size: Some(8192),
            .. Default::default()
        };
        fs.setattr(&fd, attr).await.unwrap();
        if blobs {
            // Sync the filesystem to flush the InlineExtents to BlobExtents
            fs.sync().await;
        }

        // SeekHole prior to the last hole in the file
        assert_eq!(Ok(4096), fs.lseek(&fd, 0, Hole).await);
    }

    /// The file ends with a data extent followed by a hole in the same record
    #[tokio::test]
    async fn lseek_partial_hole_at_end() {
        use SeekWhence::Hole;
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();

        // Create a file like this, all in one record:
        // |======|      |>
        // | data | hole |EOF
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        let buf = vec![42u8; 2048];
        fs.write(&fd, 0, &buf[..], 0).await.unwrap();
        let attr = SetAttr {
            size: Some(4096),
            .. Default::default()
        };
        fs.setattr(&fd, attr).await.unwrap();

        // SeekHole in the middle of the final hole should return EOF
        assert_eq!(Ok(4096), fs.lseek(&fd, 3072, Hole).await);
        // SeekData in the middle of the final hole should return ENXIO
        let r = fs.lseek(&fd, 3072, SeekWhence::Data).await;
        assert_eq!(r, Err(libc::ENXIO));
    }

    #[tokio::test]
    async fn mkdir() {
        let (fs, _cache, _db) = harness4k().await;
        let uid = 12345;
        let gid = 54321;
        let name = OsStr::from_bytes(b"x");
        let root = fs.root();
        let fd = fs.mkdir(&root, name, 0o755, uid, gid).await
        .unwrap();
        let fd1 = fs.lookup(None, &root, name).await.unwrap();
        assert_eq!(fd1.ino(), fd.ino());

        // The new dir should have "." and ".." directory entries
        let entries = readdir_all(&fs, &fd, 0).await;
        let (dotdot, _) = entries[0];
        assert_eq!(dotdot.d_type, libc::DT_DIR);
        let dotdot_name = unsafe{
            CStr::from_ptr(&dotdot.d_name as *const c_char)
        };
        assert_eq!(dotdot_name, CString::new("..").unwrap().as_c_str());
        assert_eq!(u64::from(dotdot.d_fileno), root.ino());
        let (dot, _) = entries[1];
        assert_eq!(dot.d_type, libc::DT_DIR);
        let dot_name = unsafe{
            CStr::from_ptr(&dot.d_name as *const c_char)
        };
        assert_eq!(dot_name, CString::new(".").unwrap().as_c_str());
        assert_eq!(u64::from(dot.d_fileno), fd.ino());

        // The parent dir should have an "x" directory entry
        let entries = readdir_all(&fs, &root, 0).await;
        let (dirent, _ofs) = entries
        .into_iter()
        .find(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_DIR);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());

        // The parent dir's link count should've increased
        let parent_attr = fs.getattr(&root).await.unwrap();
        assert_eq!(parent_attr.nlink, 2);

        // Check the new directory's attributes
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.ino, fd.ino());
        assert_eq!(attr.size, 0);
        assert_ne!(attr.atime.sec, 0);
        assert_eq!(attr.atime, attr.mtime);
        assert_eq!(attr.atime, attr.ctime);
        assert_eq!(attr.atime, attr.birthtime);
        assert_eq!(attr.mode.0, libc::S_IFDIR | 0o755);
        assert_eq!(attr.nlink, 2);
        assert_eq!(attr.uid, uid);
        assert_eq!(attr.gid, gid);
        assert_eq!(attr.blksize, 4096);
        assert_eq!(attr.flags, 0);
    }

    /// mkdir creates two directories whose names have a hash collision
    // Note that it's practically impossible to find a collision for a specific
    // name, like "." or "..", so those cases won't have test coverage
    #[tokio::test]
    async fn mkdir_collision() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = fs.mkdir(&root, &filename0, 0o755, 0, 0).await.unwrap();
        let fd1 = fs.mkdir(&root, &filename1, 0o755, 0, 0).await.unwrap();

        assert_eq!(fs.lookup(None, &root, &filename0).await.unwrap().ino(),
            fd0.ino());
        assert_eq!(fs.lookup(None, &root, &filename1).await.unwrap().ino(),
            fd1.ino());
    }

    /// mkdir(2) should update the parent dir's timestamps
    #[tokio::test]
    async fn mkdir_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        clear_timestamps(&fs, &root).await;

        fs.mkdir(&root, &OsString::from("x"), 0o755, 0, 0).await.unwrap();
        assert_ts_changed(&fs, &root, false, true, true, false).await;
    }

    #[tokio::test]
    async fn mkchar() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.mkchar(&root, &OsString::from("x"), 0o644, 0, 0, 42).await
        .unwrap();
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.mode.0, libc::S_IFCHR | 0o644);
        assert_eq!(attr.rdev, 42);
        assert_eq!(attr.ino, fd.ino());
        assert_eq!(attr.size, 0);
        assert_eq!(attr.bytes, 0);
        assert_ne!(attr.atime.sec, 0);
        assert_eq!(attr.atime, attr.mtime);
        assert_eq!(attr.atime, attr.ctime);
        assert_eq!(attr.atime, attr.birthtime);
        assert_eq!(attr.nlink, 1);
        assert_eq!(attr.uid, 0);
        assert_eq!(attr.gid, 0);
        assert_eq!(attr.blksize, 4096);
        assert_eq!(attr.flags, 0);

        // The parent dir should have an "x" directory entry
        let entries = readdir_all(&fs, &root, 0).await;
        let (dirent, _ofs) = entries
        .into_iter()
        .find(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_CHR);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());
    }

    /// mknod(2) should update the parent dir's timestamps
    #[tokio::test]
    async fn mkchar_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        clear_timestamps(&fs, &root).await;

        fs.mkchar(&root, &OsString::from("x"), 0o644, 0, 0, 42).await.unwrap();
        assert_ts_changed(&fs, &root, false, true, true, false).await;
    }

    #[tokio::test]
    async fn mkblock() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.mkblock(&root, &OsString::from("x"), 0o644, 0, 0, 42).await
        .unwrap();
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.mode.0, libc::S_IFBLK | 0o644);
        assert_eq!(attr.rdev, 42);
        assert_eq!(attr.ino, fd.ino());
        assert_eq!(attr.size, 0);
        assert_eq!(attr.bytes, 0);
        assert_ne!(attr.atime.sec, 0);
        assert_eq!(attr.atime, attr.mtime);
        assert_eq!(attr.atime, attr.ctime);
        assert_eq!(attr.atime, attr.birthtime);
        assert_eq!(attr.nlink, 1);
        assert_eq!(attr.uid, 0);
        assert_eq!(attr.gid, 0);
        assert_eq!(attr.blksize, 4096);
        assert_eq!(attr.flags, 0);

        // The parent dir should have an "x" directory entry
        let entries = readdir_all(&fs, &root, 0).await;
        let (dirent, _ofs) = entries
        .into_iter()
        .find(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_BLK);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());
    }

    /// mknod(2) should update the parent dir's timestamps
    #[tokio::test]
    async fn mkblock_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        clear_timestamps(&fs, &root).await;

        fs.mkblock(&root, &OsString::from("x"), 0o644, 0, 0, 42).await.unwrap();
        assert_ts_changed(&fs, &root, false, true, true, false).await;
    }

    #[tokio::test]
    async fn mkfifo() {
        let (fs, _cache, _db) = harness4k().await;
        let uid = 12345;
        let gid = 54321;
        let root = fs.root();
        let fd = fs.mkfifo(&root, &OsString::from("x"), 0o644, uid, gid).await
        .unwrap();
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.mode.0, libc::S_IFIFO | 0o644);
        assert_eq!(attr.ino, fd.ino());
        assert_eq!(attr.size, 0);
        assert_eq!(attr.bytes, 0);
        assert_ne!(attr.atime.sec, 0);
        assert_eq!(attr.atime, attr.mtime);
        assert_eq!(attr.atime, attr.ctime);
        assert_eq!(attr.atime, attr.birthtime);
        assert_eq!(attr.nlink, 1);
        assert_eq!(attr.uid, uid);
        assert_eq!(attr.gid, gid);
        assert_eq!(attr.blksize, 4096);
        assert_eq!(attr.flags, 0);

        // The parent dir should have an "x" directory entry
        let entries = readdir_all(&fs, &root, 0).await;
        let (dirent, _ofs) = entries
        .into_iter()
        .find(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_FIFO);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());
    }

    /// mkfifo(2) should update the parent dir's timestamps
    #[tokio::test]
    async fn mkfifo_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        clear_timestamps(&fs, &root).await;

        fs.mkfifo(&root, &OsString::from("x"), 0o644, 0, 0).await.unwrap();
        assert_ts_changed(&fs, &root, false, true, true, false).await;
    }

    #[tokio::test]
    async fn mksock() {
        let (fs, _cache, _db) = harness4k().await;
        let uid = 12345;
        let gid = 54321;
        let root = fs.root();
        let fd = fs.mksock(&root, &OsString::from("x"), 0o644, uid, gid).await
        .unwrap();
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.mode.0, libc::S_IFSOCK | 0o644);
        assert_eq!(attr.ino, fd.ino());
        assert_eq!(attr.size, 0);
        assert_eq!(attr.bytes, 0);
        assert_ne!(attr.atime.sec, 0);
        assert_eq!(attr.atime, attr.mtime);
        assert_eq!(attr.atime, attr.ctime);
        assert_eq!(attr.atime, attr.birthtime);
        assert_eq!(attr.nlink, 1);
        assert_eq!(attr.uid, uid);
        assert_eq!(attr.gid, gid);
        assert_eq!(attr.blksize, 4096);
        assert_eq!(attr.flags, 0);

        // The parent dir should have an "x" directory entry
        let entries = readdir_all(&fs, &root, 0).await;
        let (dirent, _ofs) = entries
        .into_iter()
        .find(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_SOCK);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());
    }

    /// mksock(2) should update the parent dir's timestamps
    #[tokio::test]
    async fn mksock_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        clear_timestamps(&fs, &root).await;

        fs.mkfifo(&root, &OsString::from("x"), 0o644, 0, 0).await.unwrap();
        assert_ts_changed(&fs, &root, false, true, true, false).await;
    }

    // If the file system was unmounted uncleanly and has open but deleted
    // files, they should be deleted during mount
    #[cfg(debug_assertions)]
    #[tokio::test]
    async fn mount_with_open_but_deleted_files() {
        let (fs, _cache, db) = harness4k().await;
        let root = fs.root();

        // First create a file, open it, and unlink it, but don't close it
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        let ino = fd.ino();
        let r = fs.unlink(&root, Some(&fd), &filename).await;
        fs.sync().await;
        assert_eq!(Ok(()), r);

        // Unmount, without closing the file
        drop(fs);

        // Mount again
        let tree_id = db.lookup_fs("").await.unwrap().1.unwrap();
        let fs = Fs::new(db, tree_id).await;

        // Try to open the file again.
        // Wait up to 2 seconds for the inode to be deleted
        let mut r = Err(0);
        for _ in 0..20 {
            r = fs.igetattr(ino).await;
            if r.is_err() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert_eq!(Err(libc::ENOENT), r);
    }

    // Read a hole that's bigger than the zero region
    #[tokio::test]
    async fn read_big_hole() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let holesize = 2 * ZERO_REGION_LEN;
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, holesize as u64, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        let sglist = fs.read(&fd, 0, holesize).await.unwrap();
        let expected = vec![0u8; ZERO_REGION_LEN];
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &expected[..]);
        assert_eq!(&sglist[1][..], &expected[..]);
    }

    // Read a single BlobExtent record
    #[tokio::test]
    async fn read_blob() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let buf = vec![42u8; 4096];
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        // Sync the filesystem to flush the InlineExtent to a BlobExtent
        fs.sync().await;

        let sglist = fs.read(&fd, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[..]);
    }

    #[tokio::test]
    async fn read_empty_file() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let sglist = fs.read(&fd, 0, 1024).await.unwrap();
        assert!(sglist.is_empty());
    }

    #[tokio::test]
    async fn read_empty_file_past_start() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let sglist = fs.read(&fd, 2048, 2048).await.unwrap();
        assert!(sglist.is_empty());
    }

    // Read a hole within a sparse file
    #[tokio::test]
    async fn read_hole() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 4096, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        let sglist = fs.read(&fd, 0, 4096).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);
    }

    // Read a hole in between two adjacent records.
    #[tokio::test]
    async fn read_partial_hole_between_recs() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(2048), r);
        let r = fs.write(&fd, 4096, &buf[..], 0).await;
        assert_eq!(Ok(2048), r);

        // The file should now have a hole from offset 2048 to 4096
        let sglist = fs.read(&fd, 3072, 1024).await.unwrap();
        let db = &sglist[0];
        let zbuf = [0u8; 2048];
        assert_eq!(&db[..], &zbuf[..1024]);

        // It should also be possible to read data and hole in one operation
        let sglist = fs.read(&fd, 0, 4096).await.unwrap();
        assert_eq!(&sglist[0][..], &buf[..]);
        assert_eq!(&sglist[1][..], &zbuf[..]);
    }

    // Read a chunk of a file that includes a partial hole at the beginning and
    // data at the end.
    #[tokio::test]
    async fn read_partial_hole_trailing_edge() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(2048), r);
        let r = fs.write(&fd, 4096, &buf[..], 0).await;
        assert_eq!(Ok(2048), r);

        // The file should now have a hole from offset 2048 to 4096
        let sglist = fs.read(&fd, 3072, 2048).await.unwrap();
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &[0u8; 1024][..]);
        assert_eq!(&sglist[1][..], &buf[0..1024]);
    }

    // A read that's smaller than a record, at both ends
    #[tokio::test]
    async fn read_partial_record() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        let sglist = fs.read(&fd, 1024, 2048).await.unwrap();
        let db = &sglist[0];
        assert_eq!(db.len(), 2048);
        assert_eq!(&db[..], &buf[1024..3072]);
    }

    #[tokio::test]
    async fn read_past_eof() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(2048), r);

        let sglist = fs.read(&fd, 2048, 1024).await.unwrap();
        assert!(sglist.is_empty());
    }

    /// A read that spans 3 records, where the middle record is a hole
    #[tokio::test]
    async fn read_spans_hole() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        assert_eq!(4096, fs.write(&fd, 0, &buf[..], 0).await.unwrap());
        assert_eq!(4096, fs.write(&fd, 8192, &buf[..], 0).await.unwrap());

        let sglist = fs.read(&fd, 0, 12288).await.unwrap();
        assert_eq!(sglist.len(), 3);
        assert_eq!(&sglist[0][..], &buf[..]);
        assert_eq!(&sglist[1][..], &[0u8; 4096][..]);
        assert_eq!(&sglist[2][..], &buf[..]);
    }

    /// read(2) should update the file's atime
    #[tokio::test]
    async fn read_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let buf = vec![42u8; 4096];
        fs.write(&fd, 0, &buf[..], 0).await.unwrap();
        clear_timestamps(&fs, &fd).await;

        fs.read(&fd, 0, 4096).await.unwrap();
        assert_ts_changed(&fs, &fd, true, false, false, false).await;
    }

    // When atime is disabled, reading a file should not update its atime.
    #[tokio::test]
    async fn read_timestamps_no_atime() {
        let (fs, _cache, _db) = harness(vec![Property::Atime(false)]).await;
        let root = fs.root();

        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
            .unwrap();
        let buf = vec![42u8; 4096];
        fs.write(&fd, 0, &buf[..], 0).await.unwrap();
        clear_timestamps(&fs, &fd).await;

        fs.read(&fd, 0, 4096).await.unwrap();
        assert_ts_changed(&fs, &fd, false, false, false, false).await;
    }

    // A read that's split across two records
    #[tokio::test]
    async fn read_two_recs() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[0..4096], 0).await;
        assert_eq!(Ok(4096), r);
        let r = fs.write(&fd, 4096, &buf[4096..8192], 0).await;
        assert_eq!(Ok(4096), r);

        let sglist = fs.read(&fd, 0, 8192).await.unwrap();
        assert_eq!(2, sglist.len(), "Read didn't span multiple records");
        let db0 = &sglist[0];
        assert_eq!(&db0[..], &buf[0..4096]);
        let db1 = &sglist[1];
        assert_eq!(&db1[..], &buf[4096..8192]);
    }

    // Read past EOF, in an entirely different record
    #[tokio::test]
    async fn read_well_past_eof() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        let sglist = fs.read(&fd, 1 << 30, 4096).await.unwrap();
        assert!(sglist.is_empty());
    }

    #[tokio::test]
    async fn readdir() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let entries = readdir_all(&fs, &root, 0).await;
        let (dotdot, _) = entries[0];
        assert_eq!(dotdot.d_type, libc::DT_DIR);
        let dotdot_name = unsafe{
            CStr::from_ptr(&dotdot.d_name as *const c_char)
        };
        assert_eq!(dotdot_name, CString::new("..").unwrap().as_c_str());
        let (dot, _) = entries[1];
        assert_eq!(dot.d_type, libc::DT_DIR);
        let dot_name = unsafe{
            CStr::from_ptr(&dot.d_name as *const c_char)
        };
        assert_eq!(dot_name, CString::new(".").unwrap().as_c_str());
        assert_eq!(u64::from(dot.d_fileno), root.ino());
    }

    /// Readdir beginning at the offset of the last dirent.  The NFS server will
    /// do this sometimes.
    #[tokio::test]
    async fn readdir_eof() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let entries = readdir_all(&fs, &root, 0).await;
        // Should be two entries, "." and ".."
        assert_eq!(2, entries.len());
        let ofs = entries[1].1;
        let entries2 = readdir_all(&fs, &root, ofs).await;
        // Nothing should be returned
        assert!(entries2.is_empty());
    }

    // Readdir of a directory with a hash collision
    #[tokio::test]
    async fn readdir_collision() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);

        fs.create(&root, &filename0, 0o644, 0, 0).await.unwrap();
        fs.create(&root, &filename1, 0o644, 0, 0).await.unwrap();

        // There's no requirement for the order of readdir's output.
        let mut expected = HashSet::new();
        expected.insert(OsString::from("."));
        expected.insert(OsString::from(".."));
        expected.insert(filename0);
        expected.insert(filename1);
        let entries = readdir_all(&fs, &root, 0).await;
        for (entry, _) in entries.into_iter() {
            let nameptr = entry.d_name.as_ptr() as *const u8;
            let namelen = usize::from(entry.d_namlen);
            let name_s = unsafe{slice::from_raw_parts(nameptr, namelen)};
            let name = OsStr::from_bytes(name_s);
            assert!(expected.remove(name));
        }
        assert!(expected.is_empty());
    }

    // Readdir of a directory with a hash collision, and the two colliding files
    // straddle the boundary of the client's buffer.  The client must call
    // readdir again with the provided offset, and it must see neither duplicate
    // nor missing entries
    #[tokio::test]
    async fn readdir_collision_at_offset() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);

        let _fd0 = fs.create(&root, &filename0, 0o644, 0, 0).await.unwrap();
        let fd1 = fs.create(&root, &filename1, 0o644, 0, 0).await.unwrap();

        // There's no requirement for the order of readdir's output, but
        // filename1 happens to come first.
        let mut stream0 = Box::pin(fs.readdir(&root, 0));
        let (result0, offset0) = stream0.try_next().await.unwrap().unwrap();
        assert_eq!(u64::from(result0.d_fileno), fd1.ino());

        // Now interrupt the stream, and resume with the supplied offset.
        let mut expected = HashSet::new();
        expected.insert(OsString::from("."));
        expected.insert(OsString::from(".."));
        expected.insert(filename0);
        drop(stream0);
        let entries = readdir_all(&fs, &root, offset0).await;
        for (entry, _) in entries.into_iter() {
            let nameptr = entry.d_name.as_ptr() as *const u8;
            let namelen = usize::from(entry.d_namlen);
            let name_s = unsafe{slice::from_raw_parts(nameptr, namelen)};
            let name = OsStr::from_bytes(name_s);
            assert!(expected.remove(name));
        }
        assert!(expected.is_empty());
    }

    // Remove a file in a colliding hash bucket, then resume readdir at that
    // point.  No remaining files should be skipped.
    // NB: results may be different for a 3-way hash collision, but I can't yet
    // generate any 3-way collisions to test with.
    #[tokio::test]
    async fn readdir_rm_during_stream_at_collision() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);

        let _fd0 = fs.create(&root, &filename0, 0o644, 0, 0).await.unwrap();
        let fd1 = fs.create(&root, &filename1, 0o644, 0, 0).await.unwrap();

        // There's no requirement for the order of readdir's output, but
        // filename1 happens to come first.
        let mut stream0 = Box::pin(fs.readdir(&root, 0));
        let (result0, offset0) = stream0.try_next().await.unwrap().unwrap();
        assert_eq!(u64::from(result0.d_fileno), fd1.ino());

        // Now interrupt the stream, remove the first has bucket entry, and
        // resume with the supplied offset.
        let r = fs.unlink(&root, Some(&fd1), &filename1).await;
        assert_eq!(Ok(()), r);
        let mut expected = HashSet::new();
        expected.insert(OsString::from("."));
        expected.insert(OsString::from(".."));
        expected.insert(filename0);
        drop(stream0);
        let entries = readdir_all(&fs, &root, offset0).await;
        for (entry, _) in entries.into_iter() {
            let nameptr = entry.d_name.as_ptr() as *const u8;
            let namelen = usize::from(entry.d_namlen);
            let name_s = unsafe{slice::from_raw_parts(nameptr, namelen)};
            let name = OsStr::from_bytes(name_s);
            assert!(expected.remove(name));
        }
        assert!(expected.is_empty());
    }

    // It's allowed for the client of Fs::readdir to drop the stream without
    // reading all entries.  The FUSE module does that when it runs out of space
    // in the kernel-provided buffer.
    // Just check that nothing panics.
    #[tokio::test]
    async fn readdir_partial() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let mut stream = Box::pin(fs.readdir(&root, 0));
        let _ = stream.try_next().await.unwrap().unwrap();
    }

    /// readdir(2) should not update any timestamps
    #[tokio::test]
    async fn readdir_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        clear_timestamps(&fs, &root).await;

        let _entries = readdir_all(&fs, &root, 0).await;
        assert_ts_changed(&fs, &root, false, false, false, false).await;
    }

    #[tokio::test]
    async fn readlink() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let fd = fs.symlink(&root, &srcname, 0o642, 0, 0, &dstname).await
        .unwrap();
        let output = fs.readlink(&fd).await.unwrap();
        assert_eq!(dstname, output);
    }

    // Calling readlink on a non-symlink should return EINVAL
    #[tokio::test]
    async fn readlink_einval() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let output = fs.readlink(&root).await;
        assert_eq!(libc::EINVAL, output.unwrap_err());
    }

    #[tokio::test]
    async fn readlink_enoent() {
        let (fs, _cache, _db) = harness4k().await;
        let fd = FileData::new_for_tests(Some(1), 1000);
        let output = fs.readlink(&fd).await;
        assert_eq!(libc::ENOENT, output.unwrap_err());
    }

    /// readlink(2) should not update any timestamps
    #[tokio::test]
    async fn readlink_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let fd = fs.symlink(&root, &srcname, 0o642, 0, 0, &dstname).await
        .unwrap();
        clear_timestamps(&fs, &fd).await;

        let output = fs.readlink(&fd).await.unwrap();
        assert_eq!(dstname, output);
        assert_ts_changed(&fs, &fd, false, false, false, false).await;
    }

    // Rename a file that has a hash collision in both the source and
    // destination directories
    #[tokio::test]
    async fn rename_collision() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("F0jS2Tptj7");
        let src_c = OsString::from("PLe01T116a");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("Gg1AG3wll2");
        let dst_c = OsString::from("FDCIlvDxYn");
        let dstdir = OsString::from("dstdir");
        assert_dirents_collide(&src, &src_c);
        assert_dirents_collide(&dst, &dst_c);

        let srcdir_fd = fs.mkdir(&root, &srcdir, 0o755, 0, 0).await.unwrap();
        let dstdir_fd = fs.mkdir(&root, &dstdir, 0o755, 0, 0).await.unwrap();
        let src_c_fd = fs.create(&srcdir_fd, &src_c, 0o644, 0, 0).await
        .unwrap();
        let src_c_ino = src_c_fd.ino();
        let dst_c_fd = fs.create(&dstdir_fd, &dst_c, 0o644, 0, 0).await
        .unwrap();
        let dst_c_ino = dst_c_fd.ino();
        let src_fd = fs.create(&srcdir_fd, &src, 0o644, 0, 0).await.unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = fs.create(&dstdir_fd, &dst, 0o644, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let dst_ino = dst_fd.ino();

        assert_eq!(src_fd.ino(),
            fs.rename(&srcdir_fd, &src_fd, &src, &dstdir_fd,
                Some(dst_fd.ino()), &dst).await
            .unwrap()
        );

        fs.inactive(src_fd).await;
        assert_eq!(src_ino,
            fs.lookup(Some(&root), &dstdir_fd, &dst).await.unwrap().ino()
        );
        let r = fs.lookup(Some(&root), &srcdir_fd, &src).await;
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let srcdir_inode = fs.getattr(&srcdir_fd).await.unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = fs.getattr(&dstdir_fd).await.unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(dst_ino).await, Err(libc::ENOENT));
        }

        // Finally, make sure we didn't upset the colliding files
        fs.inactive(src_c_fd).await;
        fs.inactive(dst_c_fd).await;
        let src_c_fd1 = fs.lookup(Some(&root), &srcdir_fd, &src_c).await;
        assert_eq!(src_c_fd1.unwrap().ino(), src_c_ino);
        let dst_c_fd1 = fs.lookup(Some(&root), &dstdir_fd, &dst_c).await;
        assert_eq!(dst_c_fd1.unwrap().ino(), dst_c_ino);
    }

    // Rename a directory.  The target is also a directory
    #[tokio::test]
    async fn rename_dir_to_dir() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = fs.mkdir(&root, &srcdir, 0o755, 0, 0).await
        .unwrap();
        let dstdir_fd = fs.mkdir(&root, &dstdir, 0o755, 0, 0).await
        .unwrap();
        let src_fd = fs.mkdir(&srcdir_fd, &src, 0o755, 0, 0).await
        .unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = fs.mkdir(&dstdir_fd, &dst, 0o755, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let dst_ino = dst_fd.ino();

        assert_eq!(src_fd.ino(),
            fs.rename(&srcdir_fd, &src_fd, &src, &dstdir_fd,
                Some(dst_fd.ino()), &dst).await.unwrap()
        );

        fs.inactive(src_fd).await;
        let dst_fd1 = fs.lookup(Some(&root), &dstdir_fd, &dst).await;
        assert_eq!(dst_fd1.unwrap().ino(), src_ino);
        let r = fs.lookup(Some(&root), &srcdir_fd, &src).await;
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let srcdir_inode = fs.getattr(&srcdir_fd).await.unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = fs.getattr(&dstdir_fd).await.unwrap();
        assert_eq!(dstdir_inode.nlink, 3);
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(dst_ino).await, Err(libc::ENOENT));
        }
    }

    #[tokio::test]
    async fn rename_dir_to_dir_same_parent() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let parent = OsString::from("parent");
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let parent_fd = fs.mkdir(&root, &parent, 0o755, 0, 0).await.unwrap();
        let src_fd = fs.mkdir(&parent_fd, &src, 0o755, 0, 0).await.unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = fs.mkdir(&parent_fd, &dst, 0o755, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let dst_ino = dst_fd.ino();

        assert_eq!(src_fd.ino(),
            fs.rename(&parent_fd, &src_fd, &src, &parent_fd,
                Some(dst_fd.ino()), &dst).await
            .unwrap()
        );

        fs.inactive(src_fd).await;
        let dst_fd1 = fs.lookup(Some(&root), &parent_fd, &dst).await;
        assert_eq!(dst_fd1.unwrap().ino(), src_ino);
        let r = fs.lookup(Some(&root), &parent_fd, &src).await;
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let parent_inode = fs.getattr(&parent_fd).await.unwrap();
        assert_eq!(parent_inode.nlink, 3);
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(dst_ino).await, Err(libc::ENOENT));
        }
    }

    // Rename a directory.  The target is also a directory that isn't empty.
    // Nothing should change.
    #[tokio::test]
    async fn rename_dir_to_nonemptydir() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let dstf = OsString::from("dstf");
        let srcdir_fd = fs.mkdir(&root, &srcdir, 0o755, 0, 0).await
        .unwrap();
        let dstdir_fd = fs.mkdir(&root, &dstdir, 0o755, 0, 0).await
        .unwrap();
        let src_fd = fs.mkdir(&srcdir_fd, &src, 0o755, 0, 0).await
        .unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = fs.mkdir(&dstdir_fd, &dst, 0o755, 0, 0).await
        .unwrap();
        let dst_ino = dst_fd.ino();
        let dstf_fd = fs.create(&dst_fd, &dstf, 0o644, 0, 0).await
        .unwrap();

        let r = fs.rename(&srcdir_fd, &src_fd, &src,
            &dstdir_fd, Some(dst_fd.ino()), &dst).await;
        assert_eq!(r, Err(libc::ENOTEMPTY));

        fs.inactive(src_fd).await;
        assert_eq!(src_ino,
            fs.lookup(Some(&root), &srcdir_fd, &src).await.unwrap().ino()
        );
        let dst_fd1 = fs.lookup(Some(&root), &dstdir_fd, &dst).await
        .unwrap();
        assert_eq!(dst_fd1.ino(), dst_ino);
        let dstf_fd1 = fs.lookup(Some(&dstdir_fd), &dst_fd1, &dstf).await;
        assert_eq!(dstf_fd1.unwrap().ino(), dstf_fd.ino());
    }

    // Rename a directory.  The target name does not exist
    #[tokio::test]
    async fn rename_dir_to_nothing() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let dotdotname = OsStr::from_bytes(b"..");
        let srcdir_fd = fs.mkdir(&root, &srcdir, 0o755, 0, 0).await
        .unwrap();
        let dstdir_fd = fs.mkdir(&root, &dstdir, 0o755, 0, 0).await
        .unwrap();
        let mut fd = fs.mkdir(&srcdir_fd, &src, 0o755, 0, 0).await.unwrap();
        let src_ino = fd.ino();

        assert_eq!(fd.ino(),
            fs.rename(&srcdir_fd, &fd, &src, &dstdir_fd,
                None, &dst).await
            .unwrap()
        );
        fd.reparent(dstdir_fd.ino());

        // Check that the moved directory's parent is correct in memory
        assert_eq!(dstdir_fd.ino(), fd.parent().unwrap());
        let dotdot_fd = fs.lookup(Some(&dstdir_fd), &fd, dotdotname).await
            .unwrap();
        assert_eq!(dotdot_fd.ino(), dstdir_fd.ino());
        assert_eq!(dotdot_fd.parent(), Some(root.ino()));

        fs.inactive(fd).await;

        // Check that the moved directory's parent is correct on disk
        let fd = fs.ilookup(src_ino).await.unwrap();
        assert_eq!(fd.parent(), Some(dstdir_fd.ino()));
        fs.inactive(fd).await;

        // Check that the moved directory is visible by its new, not old, name
        let dst_fd = fs.lookup(Some(&root), &dstdir_fd, &dst).await.unwrap();
        assert_eq!(dst_fd.ino(), src_ino);
        let r = fs.lookup(Some(&root), &srcdir_fd, &src).await;
        assert_eq!(r.unwrap_err(), libc::ENOENT);

        // Check parents' link counts
        let srcdir_attr = fs.getattr(&srcdir_fd).await.unwrap();
        assert_eq!(srcdir_attr.nlink, 2);
        let dstdir_attr = fs.getattr(&dstdir_fd).await.unwrap();
        assert_eq!(dstdir_attr.nlink, 3);
    }

    // Attempting to rename "." should return EINVAL
    #[tokio::test]
    async fn rename_dot() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let dot = OsStr::from_bytes(b".");
        let srcdir = OsStr::from_bytes(b"srcdir");
        let dst = OsStr::from_bytes(b"dst");
        let srcdir_fd = fs.mkdir(&root, srcdir, 0o755, 0, 0).await
        .unwrap();

        let r = fs.rename(&srcdir_fd, &srcdir_fd, dot, &srcdir_fd,
                None, dst).await;
        assert_eq!(Err(libc::EINVAL), r);
    }

    // Attempting to rename ".." should return EINVAL
    #[tokio::test]
    async fn rename_dotdot() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let dotdot = OsStr::from_bytes(b"..");
        let srcdir = OsStr::from_bytes(b"srcdir");
        let dst = OsStr::from_bytes(b"dst");
        let srcdir_fd = fs.mkdir(&root, srcdir, 0o755, 0, 0).await
        .unwrap();

        let r = fs.rename(&srcdir_fd, &root, dotdot, &srcdir_fd,
                None, dst).await;
        assert_eq!(Err(libc::EINVAL), r);
    }

    // Rename a non-directory to a multiply-linked file.  The destination
    // directory entry should be removed, but not the inode.
    #[tokio::test]
    async fn rename_nondir_to_hardlink() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let lnk = OsString::from("lnk");
        let src_fd = fs.create(&root, &src, 0o644, 0, 0).await.unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = fs.create(&root, &dst, 0o644, 0, 0).await.unwrap();
        let dst_ino = dst_fd.ino();
        fs.link(&root, &dst_fd, &lnk).await.unwrap();
        clear_timestamps(&fs, &dst_fd).await;

        assert_eq!(src_fd.ino(),
            fs.rename(&root, &src_fd, &src, &root, Some(dst_fd.ino()),
                &dst).await
            .unwrap()
        );

        fs.inactive(src_fd).await;
        assert_eq!(fs.lookup(None, &root, &dst).await.unwrap().ino(),
            src_ino);
        assert_eq!(fs.lookup(None, &root, &src).await.unwrap_err(),
            libc::ENOENT);
        let lnk_fd = fs.lookup(None, &root, &lnk).await.unwrap();
        assert_eq!(lnk_fd.ino(), dst_ino);
        let lnk_attr = fs.getattr(&lnk_fd).await.unwrap();
        assert_eq!(lnk_attr.nlink, 1);
        assert_ts_changed(&fs, &lnk_fd, false, false, true, false).await;
    }

    // Rename a non-directory.  The target is also a non-directory
    #[tokio::test]
    async fn rename_nondir_to_nondir() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = fs.mkdir(&root, &srcdir, 0o755, 0, 0).await.unwrap();
        let dstdir_fd = fs.mkdir(&root, &dstdir, 0o755, 0, 0).await.unwrap();
        let src_fd = fs.create(&srcdir_fd, &src, 0o644, 0, 0).await.unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = fs.create(&dstdir_fd, &dst, 0o644, 0, 0).await.unwrap();
        let dst_ino = dst_fd.ino();

        assert_eq!(src_fd.ino(),
            fs.rename(&srcdir_fd, &src_fd, &src, &dstdir_fd,
                Some(dst_ino), &dst).await.unwrap()
        );

        fs.inactive(src_fd).await;
        let dst_fd1 = fs.lookup(Some(&root), &dstdir_fd, &dst).await;
        assert_eq!(dst_fd1.unwrap().ino(), src_ino);
        let r = fs.lookup(Some(&root), &srcdir_fd, &src).await;
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let srcdir_inode = fs.getattr(&srcdir_fd).await.unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = fs.getattr(&dstdir_fd).await.unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(dst_ino).await, Err(libc::ENOENT));
        }
    }

    // Rename a non-directory.  The target name does not exist
    #[tokio::test]
    async fn rename_nondir_to_nothing() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = fs.mkdir(&root, &srcdir, 0o755, 0, 0).await.unwrap();
        let dstdir_fd = fs.mkdir(&root, &dstdir, 0o755, 0, 0).await.unwrap();
        let fd = fs.create(&srcdir_fd, &src, 0o644, 0, 0).await.unwrap();
        let src_ino = fd.ino();

        assert_eq!(fd.ino(),
            fs.rename(&srcdir_fd, &fd, &src, &dstdir_fd, None, &dst).await
            .unwrap()
        );

        fs.inactive(fd).await;
        assert_eq!(src_ino,
            fs.lookup(Some(&root), &dstdir_fd, &dst).await.unwrap().ino()
        );
        assert_eq!(libc::ENOENT,
            fs.lookup(Some(&root), &srcdir_fd, &src).await.unwrap_err()
        );
        let srcdir_inode = fs.getattr(&srcdir_fd).await.unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = fs.getattr(&dstdir_fd).await.unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
    }

    // Rename a regular file to a symlink.  Make sure the target is a regular
    // file afterwards
    #[tokio::test]
    async fn rename_reg_to_symlink() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let linktarget = OsString::from("nonexistent");
        let srcdir_fd = fs.mkdir(&root, &srcdir, 0o755, 0, 0).await.unwrap();
        let dstdir_fd = fs.mkdir(&root, &dstdir, 0o755, 0, 0).await.unwrap();
        let src_fd = fs.create(&srcdir_fd, &src, 0o644, 0, 0).await.unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = fs.symlink(&dstdir_fd, &dst, 0o642, 0, 0, &linktarget)
            .await
            .unwrap();
        #[cfg(debug_assertions)] let dst_ino = dst_fd.ino();

        assert_eq!(src_fd.ino(),
            fs.rename(&srcdir_fd, &src_fd, &src, &dstdir_fd,
                Some(dst_fd.ino()), &dst).await.unwrap()
        );

        fs.inactive(src_fd).await;
        assert_eq!(src_ino,
            fs.lookup(Some(&root), &dstdir_fd, &dst).await.unwrap().ino()
        );
        assert_eq!(libc::ENOENT,
            fs.lookup(Some(&root), &srcdir_fd, &src).await.unwrap_err()
        );
        let srcdir_inode = fs.getattr(&srcdir_fd).await.unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = fs.getattr(&dstdir_fd).await.unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
        let entries = readdir_all(&fs, &dstdir_fd, 0).await;
        let (de, _) = entries
            .into_iter()
            .find(|(dirent, _)| u64::from(dirent.d_fileno) == src_ino )
            .unwrap();
        assert_eq!(de.d_type, libc::DT_REG);
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(dst_ino).await, Err(libc::ENOENT));
        }
    }

    // Rename a file with extended attributes.
    #[tokio::test]
    async fn rename_reg_with_extattrs() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;

        let fd = fs.create(&root, &src, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, ns, &name, &value[..]).await.unwrap();

        assert_eq!(fd.ino(),
            fs.rename(&root, &fd, &src, &root, None, &dst).await
            .unwrap()
        );

        fs.inactive(fd).await;
        let new_fd = fs.lookup(None, &root, &dst).await.unwrap();
        let v = fs.getextattr(&new_fd, ns, &name).await.unwrap();
        assert_eq!(&v[..], &value);
    }

    // rename updates a file's parent directories' ctime and mtime
    #[tokio::test]
    async fn rename_parent_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = fs.mkdir(&root, &srcdir, 0o755, 0, 0).await
        .unwrap();
        let dstdir_fd = fs.mkdir(&root, &dstdir, 0o755, 0, 0).await
        .unwrap();
        let fd = fs.create(&srcdir_fd, &src, 0o644, 0, 0).await
        .unwrap();
        clear_timestamps(&fs, &srcdir_fd).await;
        clear_timestamps(&fs, &dstdir_fd).await;
        clear_timestamps(&fs, &fd).await;

        assert_eq!(fd.ino(),
            fs.rename(&srcdir_fd, &fd, &src, &dstdir_fd, None, &dst).await
            .unwrap()
        );

        // Timestamps should've been updated for parent directories, but not for
        // the file itself
        assert_ts_changed(&fs, &srcdir_fd, false, true, true, false).await;
        assert_ts_changed(&fs, &dstdir_fd, false, true, true, false).await;
        assert_ts_changed(&fs, &fd, false, false, false, false).await;
    }

    #[allow(clippy::blocks_in_if_conditions)]
    #[tokio::test]
    async fn rmdir() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let dirname = OsString::from("x");
        let fd = fs.mkdir(&root, &dirname, 0o755, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let ino = fd.ino();
        fs.rmdir(&root, &dirname).await.unwrap();

        // Make sure it's gone
        assert_eq!(fs.getattr(&fd).await.unwrap_err(), libc::ENOENT);
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(ino).await, Err(libc::ENOENT));
        }
        assert!(!readdir_all(&fs, &root, 0).await
            .into_iter()
            .any(|(dirent, _)| dirent.d_name[0] == 'x' as i8));

        // Make sure the parent dir's refcount dropped
        let inode = fs.getattr(&root).await.unwrap();
        assert_eq!(inode.nlink, 1);
    }

    /// Remove a directory whose name has a hash collision
    #[tokio::test]
    async fn rmdir_collision() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = fs.mkdir(&root, &filename0, 0o755, 0, 0).await.unwrap();
        let _fd1 = fs.mkdir(&root, &filename1, 0o755, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let ino1 = _fd1.ino();
        fs.rmdir(&root, &filename1).await.unwrap();

        assert_eq!(fs.lookup(None, &root, &filename0).await.unwrap().ino(),
            fd0.ino());
        assert_eq!(fs.lookup(None, &root, &filename1).await.unwrap_err(),
            libc::ENOENT);
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(ino1).await, Err(libc::ENOENT));
        }
    }

    #[tokio::test]
    async fn rmdir_enoent() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let dirname = OsString::from("x");
        assert_eq!(fs.rmdir(&root, &dirname).await.unwrap_err(),
            libc::ENOENT);
    }

    #[should_panic]
    #[tokio::test]
    async fn rmdir_enotdir() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        fs.create(&root, &filename, 0o644, 0, 0).await
            .unwrap();
        fs.rmdir(&root, &filename).await.unwrap();
    }

    #[tokio::test]
    async fn rmdir_enotempty() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let dirname = OsString::from("x");
        let fd = fs.mkdir(&root, &dirname, 0o755, 0, 0).await
        .unwrap();
        fs.mkdir(&fd, &dirname, 0o755, 0, 0).await
        .unwrap();
        assert_eq!(fs.rmdir(&root, &dirname).await.unwrap_err(),
            libc::ENOTEMPTY);
    }

    /// Try to remove a directory that isn't empty, and that has a hash
    /// collision with another file or directory
    #[tokio::test]
    async fn rmdir_enotempty_collision() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename0 = OsString::from("basedir");
        let filename1 = OsString::from("HsxUh682JQ");
        let filename2 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename1, &filename2);
        let fd0 = fs.mkdir(&root, &filename0, 0o755, 0, 0).await.unwrap();
        let _fd1 = fs.mkdir(&fd0, &filename1, 0o755, 0, 0).await.unwrap();
        let _fd2 = fs.mkdir(&fd0, &filename2, 0o755, 0, 0).await.unwrap();
        assert_eq!(fs.rmdir(&root, &filename0).await.unwrap_err(),
         libc::ENOTEMPTY);
    }

    /// Remove a directory with an extended attribute
    #[tokio::test]
    async fn rmdir_extattr() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let dirname = OsString::from("x");
        let xname = OsString::from("foo");
        let xvalue1 = [0u8, 1, 2];
        let ns = ExtAttrNamespace::User;
        let fd = fs.mkdir(&root, &dirname, 0o755, 0, 0).await
        .unwrap();
        fs.setextattr(&fd, ns, &xname, &xvalue1[..]).await.unwrap();
        fs.rmdir(&root, &dirname).await.unwrap();

        // Make sure the xattr is gone.  As I read things, POSIX allows us to
        // return either ENOATTR or ENOENT in this case.
        assert_eq!(fs.getextattr(&fd, ns, &xname).await.unwrap_err(),
                   libc::ENOATTR);
    }

    /// Removing a directory should update its parent's timestamps
    #[tokio::test]
    async fn rmdir_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let dirname = OsString::from("x");
        fs.mkdir(&root, &dirname, 0o755, 0, 0).await.unwrap();
        clear_timestamps(&fs, &root).await;

        fs.rmdir(&root, &dirname).await.unwrap();

        // Timestamps should've been updated
        assert_ts_changed(&fs, &root, false, true, true, false).await;
    }

    #[tokio::test]
    async fn set_prop() {
        let (fs, _cache, _db) = harness4k().await;
        fs.set_prop(Property::Atime(false)).await.unwrap();

        // Read the property back
        let (val, source) = fs.get_prop(PropertyName::Atime)
            .await
            .unwrap();
        assert_eq!(val, Property::Atime(false));
        assert_eq!(source, PropertySource::Local);

        // Check that atime is truly disabled
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
            .unwrap();
        let buf = vec![42u8; 4096];
        fs.write(&fd, 0, &buf[..], 0).await.unwrap();
        clear_timestamps(&fs, &fd).await;

        fs.read(&fd, 0, 4096).await.unwrap();
        assert_ts_changed(&fs, &fd, false, false, false, false).await;
    }

    #[tokio::test]
    async fn setattr() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await
        .unwrap();
        let perm = 0o1357;
        let uid = 12345;
        let gid = 54321;
        let size = 9999;
        let atime = Timespec {sec: 1, nsec: 2};
        let mtime = Timespec {sec: 3, nsec: 4};
        let ctime = Timespec {sec: 5, nsec: 6};
        let birthtime = Timespec {sec: 7, nsec: 8};
        let flags = libc::UF_NODUMP;

        let assert = |attr: GetAttr| {
            assert_eq!(attr.nlink, 1); // Shouldn't have been changed
            assert_eq!(attr.flags, flags);
            assert_eq!(attr.atime, atime);
            assert_eq!(attr.mtime, mtime);
            assert_eq!(attr.ctime, ctime);
            assert_eq!(attr.birthtime, birthtime);
            assert_eq!(attr.uid, uid);
            assert_eq!(attr.gid, gid);
            assert_eq!(attr.mode.file_type(), libc::S_IFREG);
            assert_eq!(attr.mode.perm(), perm);
            assert_eq!(attr.size, size);
        };

        let attr = SetAttr {
            perm: Some(perm),
            uid: Some(uid),
            gid: Some(gid),
            size: Some(size),
            atime: Some(atime),
            mtime: Some(mtime),
            ctime: Some(ctime),
            birthtime: Some(birthtime),
            flags: Some(flags)
        };
        fs.setattr(&fd, attr).await.unwrap();
        let attr = fs.getattr(&fd).await.unwrap();
        assert(attr);

        // Now test using setattr to update nothing
        let attr = SetAttr {
            perm: None,
            uid: None,
            gid: None,
            size: None,
            atime: None,
            mtime: None,
            // ctime will get updated to "now" if we don't explicitly set it
            ctime: Some(ctime),
            birthtime: None,
            flags: None,
        };
        fs.setattr(&fd, attr).await.unwrap();
        let attr = fs.getattr(&fd).await.unwrap();
        assert(attr);
    }

    // setattr updates a file's ctime and mtime
    #[tokio::test]
    async fn setattr_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        clear_timestamps(&fs, &fd).await;

        let attr = SetAttr {
            perm: None,
            uid: None,
            gid: None,
            size: None,
            atime: None,
            mtime: None,
            ctime: None,
            birthtime: None,
            flags: None,
        };
        fs.setattr(&fd, attr).await.unwrap();

        // Timestamps should've been updated
        assert_ts_changed(&fs, &fd, false, false, true, false).await;
    }

    // truncating a file should delete data past the truncation point
    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn setattr_truncate(#[case] blobs: bool) {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        // First write two records
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let buf = vec![42u8; 8192];
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);

        // Then truncate one of them.
        let mut attr = SetAttr {
            size: Some(4096),
            .. Default::default()
        };
        fs.setattr(&fd, attr).await.unwrap();

        if blobs {
            fs.sync().await;        // Flush it to a BlobExtent
        }

        // Now extend the file past the truncated record
        attr.size = Some(8192);
        fs.setattr(&fd, attr).await.unwrap();

        // Finally, read the truncated record.  It should be a hole
        let sglist = fs.read(&fd, 4096, 4096).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);

        // blocks used should only include the non-truncated records
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(4096, attr.bytes);
    }

    // Like setattr_truncate, but everything happens within a single record
    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn setattr_truncate_partial_record(#[case] blobs: bool) {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        // First write one record
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let buf = vec![42u8; 4096];
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        // Then truncate it.
        let mut attr = SetAttr {
            size: Some(1000),
            .. Default::default()
        };
        fs.setattr(&fd, attr).await.unwrap();

        if blobs {
            fs.sync().await;        // Flush it to a BlobExtent
        }

        // Now extend the file past the truncated record
        attr.size = Some(4000);
        fs.setattr(&fd, attr).await.unwrap();

        // Finally, read from the truncated area.  It should be a hole
        let sglist = fs.read(&fd, 2000, 1000).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 1000];
        assert_eq!(&db[..], &expected[..]);
    }

    // Like setattr_truncate, but there is a hole at the end of the file
    #[tokio::test]
    async fn setattr_truncate_partial_hole() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        // First create a sparse file
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut attr = SetAttr {
            size: Some(8192),
            .. Default::default()
        };
        fs.setattr(&fd, attr).await.unwrap();

        // Then truncate the file partway down
        attr.size = Some(6144);
        fs.setattr(&fd, attr).await.unwrap();

        // Now extend the file past the truncated record
        attr.size = Some(8192);
        fs.setattr(&fd, attr).await.unwrap();

        // Finally, read the truncated record.  It should be a hole
        let sglist = fs.read(&fd, 4096, 4096).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);
    }

    // truncating a file should update the mtime
    #[tokio::test]
    async fn setattr_truncate_updates_mtime() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        // Create a file
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        clear_timestamps(&fs, &fd).await;

        // Then truncate the file
        let attr = SetAttr {
            size: Some(4096),
            .. Default::default()
        };
        fs.setattr(&fd, attr).await.unwrap();

        // mtime should've changed
        assert_ts_changed(&fs, &fd, false, true, true, false).await;
    }

    /// Set an blob extended attribute
    #[tokio::test]
    async fn setextattr_blob() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = vec![42u8; 4096];
        let ns = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, ns, &name, &value[..]).await.unwrap();
        fs.sync().await;
        let v = fs.getextattr(&fd, ns, &name).await.unwrap();
        assert_eq!(&v[..], &value);
        // extended attributes should not contribute to stat.st_blocks
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.bytes, 0);
    }

    /// Set an inline extended attribute.  Short attributes will never be
    /// flushed to blobs.
    #[tokio::test]
    async fn setextattr_inline() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [0u8, 1, 2];
        let ns = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, ns, &name, &value[..]).await.unwrap();
        let v = fs.getextattr(&fd, ns, &name).await.unwrap();
        assert_eq!(&v[..], &value);
        // extended attributes should not contribute to stat.st_blocks
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.bytes, 0);
    }

    /// Overwrite an existing extended attribute
    #[tokio::test]
    async fn setextattr_overwrite() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value1 = [0u8, 1, 2];
        let value2 = [3u8, 4, 5, 6];
        let ns = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, ns, &name, &value1[..]).await.unwrap();
        fs.setextattr(&fd, ns, &name, &value2[..]).await.unwrap();
        let v = fs.getextattr(&fd, ns, &name).await.unwrap();
        assert_eq!(&v[..], &value2);
    }

    /// Overwrite an existing extended attribute that hash-collided with a
    /// different xattr
    #[tokio::test]
    async fn setextattr_collision_overwrite() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];
        let value1a = [4u8, 7, 8, 9, 10];

        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, ns0, &name0, &value0[..]).await.unwrap();
        fs.setextattr(&fd, ns1, &name1, &value1[..]).await.unwrap();
        fs.setextattr(&fd, ns1, &name1, &value1a[..]).await.unwrap();
        let v0 = fs.getextattr(&fd, ns0, &name0).await.unwrap();
        assert_eq!(&v0[..], &value0);
        let v1 = fs.getextattr(&fd, ns1, &name1).await.unwrap();
        assert_eq!(&v1[..], &value1a);
    }

    /// setextattr(2) should not update any timestamps
    #[tokio::test]
    async fn setextattr_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [0u8, 1, 2];
        let ns = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        clear_timestamps(&fs, &fd).await;

        fs.setextattr(&fd, ns, &name, &value[..]).await.unwrap();
        assert_ts_changed(&fs, &fd, false, false, false, false).await;
    }

    /// The file already has a blob extattr.  Set another extattr and flush them
    /// both.
    #[tokio::test]
    async fn setextattr_overwrite_blob() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let value1 = vec![42u8; 4096];
        let name2 = OsString::from("bar");
        let value2 = [3u8, 4, 5, 6];
        let ns = ExtAttrNamespace::User;
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.setextattr(&fd, ns, &name1, &value1[..]).await.unwrap();
        fs.sync().await; // Create a blob ExtAttr
        fs.setextattr(&fd, ns, &name2, &value2[..]).await.unwrap();
        fs.sync().await; // Achieve coverage of BlobExtAttr::flush

        let v = fs.getextattr(&fd, ns, &name1).await.unwrap();
        assert_eq!(&v[..], &value1[..]);
    }

    #[tokio::test]
    async fn statvfs() {
        let (fs, _cache, _db) = harness4k().await;
        let statvfs = fs.statvfs().await.unwrap();
        assert_eq!(statvfs.f_blocks, 262_144);
        assert_eq!(statvfs.f_bsize, 4096);
        assert_eq!(statvfs.f_frsize, 4096);
    }

    #[tokio::test]
    async fn statvfs_8k() {
        let (fs, _cache, _db) = harness8k().await;
        let statvfs = fs.statvfs().await.unwrap();
        assert_eq!(statvfs.f_blocks, 262_144);
        assert_eq!(statvfs.f_bsize, 8192);
        assert_eq!(statvfs.f_frsize, 4096);
    }

    #[tokio::test]
    async fn symlink() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let uid = 12345;
        let gid = 54321;
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let fd = fs.symlink(&root, &srcname, 0o642, uid, gid, &dstname).await
        .unwrap();
        assert_eq!(fd.ino(),
            fs.lookup(None, &root, &srcname).await.unwrap().ino()
        );

        // The parent dir should have an "src" symlink entry
        let entries = readdir_all(&fs, &root, 0).await;
        let (dirent, _ofs) = entries
        .into_iter()
        .find(|(dirent, _ofs)| {
            dirent.d_name[0] == 's' as i8
        }).expect("'s' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_LNK);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name.to_str().unwrap(), srcname.to_str().unwrap());
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());

        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.mode.0, libc::S_IFLNK | 0o642);
        assert_eq!(attr.ino, fd.ino());
        assert_eq!(attr.size, 0);
        assert_eq!(attr.bytes, 0);
        assert_ne!(attr.atime.sec, 0);
        assert_eq!(attr.atime, attr.mtime);
        assert_eq!(attr.atime, attr.ctime);
        assert_eq!(attr.atime, attr.birthtime);
        assert_eq!(attr.nlink, 1);
        assert_eq!(attr.uid, uid);
        assert_eq!(attr.gid, gid);
        assert_eq!(attr.blksize, 4096);
        assert_eq!(attr.flags, 0);
    }

    /// symlink should update the parent dir's timestamps
    #[tokio::test]
    async fn symlink_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        clear_timestamps(&fs, &root).await;

        fs.symlink(&root, &srcname, 0o642, 0, 0, &dstname).await.unwrap();
        assert_ts_changed(&fs, &root, false, true, true, false).await;
    }

    #[tokio::test]
    async fn unlink() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let ino = fd.ino();
        let r = fs.unlink(&root, Some(&fd), &filename).await;
        assert_eq!(Ok(()), r);
        fs.inactive(fd).await;

        // Check that the directory entry is gone
        let r = fs.lookup(None, &root, &filename).await;
        assert_eq!(libc::ENOENT, r.unwrap_err(), "Dirent was not removed");
        // Check that the inode is gone
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(ino).await, Err(libc::ENOENT));
        }

        // The parent dir should not have an "x" directory entry
        let entries = readdir_all(&fs, &root, 0).await;
        let x_de = entries
        .into_iter()
        .find(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        });
        assert!(x_de.is_none(), "Directory entry was not removed");
    }

    // Access an opened but deleted file
    #[tokio::test]
    async fn unlink_but_opened() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        let r = fs.unlink(&root, Some(&fd), &filename).await;
        assert_eq!(Ok(()), r);

        let attr = fs.getattr(&fd).await.expect("Inode deleted too soon");
        assert_eq!(0, attr.nlink);

        fs.inactive(fd).await;
    }

    // Access an open file that was deleted during a previous TXG
    #[tokio::test]
    async fn unlink_but_opened_across_txg() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        let r = fs.unlink(&root, Some(&fd), &filename).await;
        assert_eq!(Ok(()), r);

        fs.sync().await;

        let attr = fs.getattr(&fd).await.expect("Inode deleted too soon");
        assert_eq!(0, attr.nlink);

        fs.inactive(fd).await;
    }

    // Unlink a file that has a name collision with another file in the same
    // directory.
    #[tokio::test]
    async fn unlink_collision() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = fs.create(&root, &filename0, 0o644, 0, 0).await.unwrap();
        let fd1 = fs.create(&root, &filename1, 0o644, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let ino1 = fd1.ino();

        fs.unlink(&root, Some(&fd1), &filename1).await.unwrap();
        fs.inactive(fd1).await;

        assert_eq!(fs.lookup(None, &root, &filename0).await.unwrap().ino(),
            fd0.ino());
        assert_eq!(fs.lookup(None, &root, &filename1).await.unwrap_err(),
            libc::ENOENT);
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(ino1).await, Err(libc::ENOENT));
        }
    }

    // When unlinking a multiply linked file, its ctime should be updated
    #[tokio::test]
    async fn unlink_ctime() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let name1 = OsString::from("name1");
        let name2 = OsString::from("name2");
        let fd = fs.create(&root, &name1, 0o644, 0, 0).await.unwrap();
        fs.link(&root, &fd, &name2).await.unwrap();
        clear_timestamps(&fs, &fd).await;

        fs.unlink(&root, Some(&fd), &name2).await.unwrap();
        assert_ts_changed(&fs, &fd, false, false, true, false).await;
    }

    #[tokio::test]
    async fn unlink_enoent() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        fs.unlink(&root, Some(&fd), &filename).await.unwrap();
        let e = fs.unlink(&root, Some(&fd), &filename).await.unwrap_err();
        assert_eq!(e, libc::ENOENT);
    }

    // When unlinking a hardlink, the file should not be removed until its link
    // count reaches zero.
    #[tokio::test]
    async fn unlink_hardlink() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let name1 = OsString::from("name1");
        let name2 = OsString::from("name2");
        let fd = fs.create(&root, &name1, 0o644, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let ino = fd.ino();
        fs.link(&root, &fd, &name2).await.unwrap();

        fs.unlink(&root, Some(&fd), &name1).await.unwrap();
        // File should still exist, now with link count 1.
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.nlink, 1);
        assert_eq!(fs.lookup(None, &root, &name1).await.unwrap_err(),
            libc::ENOENT);

        // Even if we drop the file data, the inode should not be deleted,
        // because it has nlink 1
        fs.inactive(fd).await;
        let fd = fs.lookup(None, &root, &name2).await.unwrap();
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.nlink, 1);

        // A second unlink should remove the file
        fs.unlink(&root, Some(&fd), &name2).await.unwrap();
        fs.inactive(fd).await;

        // File should actually be gone now
        assert_eq!(fs.lookup(None, &root, &name1).await.unwrap_err(),
            libc::ENOENT);
        assert_eq!(fs.lookup(None, &root, &name2).await.unwrap_err(),
            libc::ENOENT);
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(ino).await, Err(libc::ENOENT));
        }
    }

    // Unlink should work on inactive vnodes
    #[tokio::test]
    async fn unlink_inactive() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let ino = fd.ino();
        fs.inactive(fd).await;
        let r = fs.unlink(&root, None, &filename).await;
        assert_eq!(Ok(()), r);

        // Check that the directory entry is gone
        let r = fs.lookup(None, &root, &filename).await;
        assert_eq!(libc::ENOENT, r.expect_err("Inode was not removed"));
        // Check that the inode is gone
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(ino).await, Err(libc::ENOENT));
        }

        // The parent dir should not have an "x" directory entry
        let entries = readdir_all(&fs, &root, 0).await;
        let x_de = entries
        .into_iter()
        .find(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        });
        assert!(x_de.is_none(), "Directory entry was not removed");
    }

    /// unlink(2) should update the parent dir's timestamps
    #[tokio::test]
    async fn unlink_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        clear_timestamps(&fs, &root).await;

        fs.unlink(&root, Some(&fd), &filename).await.unwrap();
        fs.inactive(fd).await;
        assert_ts_changed(&fs, &root, false, true, true, false).await;
    }

    /// Unlink a file with blobs on disk
    #[tokio::test]
    async fn unlink_with_blobs() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let ino = fd.ino();
        let buf = vec![42u8; 4096];
        for i in 0..1024 {
            assert_eq!(Ok(4096), fs.write(&fd, 4096 * i, &buf[..], 0).await);
        }

        fs.sync().await;

        let r = fs.unlink(&root, Some(&fd), &filename).await;
        assert_eq!(Ok(()), r);
        fs.inactive(fd).await;

        // Check that the directory entry is gone
        let r = fs.lookup(None, &root, &filename).await;
        assert_eq!(libc::ENOENT, r.unwrap_err(), "Dirent was not removed");
        // Check that the inode is gone
        #[cfg(debug_assertions)]
        {
            assert_eq!(fs.igetattr(ino).await, Err(libc::ENOENT));
        }

        // The parent dir should not have an "x" directory entry
        let entries = readdir_all(&fs, &root, 0).await;
        let x_de = entries
        .into_iter()
        .find(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        });
        assert!(x_de.is_none(), "Directory entry was not removed");
    }

    // A very simple single record write to an empty file
    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn write(#[case] blobs: bool) {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let buf = vec![42u8; 4096];
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);
        if blobs {
            fs.sync().await;        // Flush it to a BlobExtent
        }

        // Check the file attributes
        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.size, 4096);
        assert_eq!(attr.bytes, 4096);

        let sglist = fs.read(&fd, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[..]);
    }

    // A partial single record write appended to the file's end
    #[tokio::test]
    async fn write_append() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf0 = vec![0u8; 1024];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf0[..], 0).await;
        assert_eq!(Ok(1024), r);

        let sglist = fs.read(&fd, 0, 1024).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf0[..]);
    }

    // A partial record write appended to a partial record at file's end
    #[tokio::test]
    async fn write_append_to_partial_record() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf0 = vec![0u8; 1024];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let mut buf1 = vec![0u8; 1024];
        let mut rng = thread_rng();
        for x in &mut buf1 {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf0[..], 0).await;
        assert_eq!(Ok(1024), r);
        let r = fs.write(&fd, 1024, &buf1[..], 0).await;
        assert_eq!(Ok(1024), r);

        let attr = fs.getattr(&fd).await.unwrap();
        assert_eq!(attr.size, 2048);
        assert_eq!(attr.bytes, 2048);

        let sglist = fs.read(&fd, 0, 2048).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[0..1024], &buf0[..]);
        assert_eq!(&db[1024..2048], &buf1[..]);
    }

    // Partially fill a hole that's at neither the beginning nor the end of the
    // file
    #[tokio::test]
    async fn write_partial_hole() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let attr = SetAttr {
            size: Some(4096 * 4),
            .. Default::default()
        };
        fs.setattr(&fd, attr).await.unwrap();

        let mut buf0 = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 9216, &buf0[..], 0).await;
        assert_eq!(Ok(2048), r);

        let sglist = fs.read(&fd, 9216, 2048).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf0[..]);
    }

    // A partial single record write that needs RMW on both ends
    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn write_partial_record(#[case] blobs: bool) {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf0 = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf0[..], 0).await;
        assert_eq!(Ok(4096), r);

        if blobs {
            fs.sync().await;        // Flush it to a BlobExtent
        }

        let buf1 = vec![0u8; 2048];
        let r = fs.write(&fd, 512, &buf1[..], 0).await;
        assert_eq!(Ok(2048), r);

        let sglist = fs.read(&fd, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[0..512], &buf0[0..512]);
        assert_eq!(&db[512..2560], &buf1[..]);
        assert_eq!(&db[2560..], &buf0[2560..]);
    }

    // write updates a file's ctime and mtime
    #[tokio::test]
    async fn write_timestamps() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        clear_timestamps(&fs, &fd).await;

        let buf = vec![42u8; 4096];
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        // Timestamps should've been updated
        assert_ts_changed(&fs, &fd, false, true, true, false).await;
    }

    // A write to an empty file that's split across two records
    #[tokio::test]
    async fn write_two_recs() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);

        // Check the file size
        let inode = fs.getattr(&fd).await.unwrap();
        assert_eq!(inode.size, 8192);

        let sglist = fs.read(&fd, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[0..4096]);
        let sglist = fs.read(&fd, 4096, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[4096..8192]);
    }

    // A write to an empty file that's split across three records
    #[tokio::test]
    async fn write_three_recs() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 12288];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(12288), r);

        // Check the file size
        let inode = fs.getattr(&fd).await.unwrap();
        assert_eq!(inode.size, 12288);

        let sglist = fs.read(&fd, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[0..4096]);
        let sglist = fs.read(&fd, 4096, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[4096..8192]);
        let sglist = fs.read(&fd, 8192, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[8192..12288]);
    }

    // Write one hold record and a partial one to an initially empty file.
    #[tokio::test]
    async fn write_one_and_a_half_records() {
        let (fs, _cache, _db) = harness4k().await;
        let root = fs.root();
        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let mut buf = vec![0u8; 6144];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = fs.write(&fd, 0, &buf[..], 0).await;
        assert_eq!(Ok(6144), r);

        // Check the file size
        let inode = fs.getattr(&fd).await.unwrap();
        assert_eq!(inode.size, 6144);

        let sglist = fs.read(&fd, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[0..4096]);
        let sglist = fs.read(&fd, 4096, 2048).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[4096..6144]);
    }
}

mod torture {
    use bfffs_core::{
        *,
        cache::*,
        database::*,
        ddml::*,
        fs::*,
        idml::*,
        pool::*,
    };
    use futures::{future, StreamExt};
    use tracing::*;
    use pretty_assertions::assert_eq;
    use rand::{
        Rng,
        RngCore,
        SeedableRng,
        distributions::{Distribution, WeightedIndex},
        thread_rng
    };
    use rand_xorshift::XorShiftRng;
    use rstest::rstest;
    use std::{
        ffi::OsString,
        fs,
        num::NonZeroU64,
        sync::{Arc, Mutex, Once},
        time::{Duration, Instant},
    };
    use tempfile::Builder;
    use tokio::runtime::Runtime;
    use tracing_subscriber::EnvFilter;

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
        dirs: Vec<(u64, FileData)>,
        fs: Fs,
        files: Vec<(u64, FileData)>,
        rng: XorShiftRng,
        root: FileData,
        rt: Option<Runtime>,
        w: Vec<(Op, f64)>,
        wi: WeightedIndex<f64>
    }

    impl TortureTest {
        fn check(&mut self) {
            let db = self.db.as_ref().unwrap();
            let rt = self.rt.as_ref().unwrap();
            assert!(rt.block_on(db.check()).unwrap());
        }

        fn clean(&mut self) {
            info!("clean");
            let db = self.db.as_ref().unwrap();
            let rt = self.rt.as_ref().unwrap();
            rt.block_on( async {
                db.clean()
                .await
            }).unwrap();
            self.check();
        }

        fn mkdir(&mut self) {
            let num: u64 = self.rng.gen();
            let fname = format!("{:x}", num);
            info!("mkdir {}", fname);
            let fd = self.rt.as_ref().unwrap().block_on(async {
                self.fs.mkdir(&self.root, &OsString::from(&fname), 0o755, 0, 0)
                    .await
            }).unwrap();
            self.dirs.push((num, fd));
        }

        fn ls(&mut self) {
            let idx = self.rng.gen_range(0..self.dirs.len() + 1);
            let (fname, fd) = if idx == self.dirs.len() {
                ("/".to_owned(), &self.root)
            } else {
                let spec = &self.dirs[idx];
                (format!("{:x}", spec.0), &spec.1)
            };
            let mut c = 0;
            self.rt.as_ref().unwrap().block_on(async {
                self.fs.readdir(fd, 0)
                    .for_each(|_| {
                        c += 1;
                        future::ready(())
                    }).await
            });
            info!("ls {}: {} entries", fname, c);
        }

        fn new(db: Arc<Database>, fs: Fs, rng: XorShiftRng, rt: Runtime,
               w: Option<Vec<(Op, f64)>>) -> Self
        {
            let w = w.unwrap_or_else(|| vec![
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
            let root = fs.root();
            TortureTest{db: Some(db), dirs: Vec::new(), files: Vec::new(), fs,
                        rng, root, rt: Some(rt), w, wi}
        }

        fn read(&mut self) {
            if !self.files.is_empty() {
                // Pick a random file to read from
                let idx = self.rng.gen_range(0..self.files.len());
                let fd = &self.files[idx].1;
                // Pick a random offset within the first 8KB
                let ofs = 2048 * self.rng.gen_range(0..4);
                info!("read {:x} at offset {}", self.files[idx].0, ofs);
                self.rt.as_ref().unwrap().block_on(async {
                    let r = self.fs.read(fd, ofs, 2048).await;
                    // TODO: check buffer contents
                    assert!(r.is_ok());
                })
            }
        }

        fn rm_enoent(&mut self) {
            // Generate a random name that corresponds to no real file, but
            // could be sorted anywhere amongst them.
            let num: u64 = self.rng.gen();
            let fname = format!("{:x}_x", num);
            let fd = FileData::new_for_tests(Some(1), num);
            info!("rm {}", fname);
            let r = self.rt.as_ref().unwrap().block_on(async {
                self.fs.unlink(&self.root, Some(&fd), &OsString::from(&fname))
                    .await
            });
            assert_eq!(r, Err(Error::ENOENT.into()));
        }

        fn rm(&mut self) {
            if !self.files.is_empty() {
                let idx = self.rng.gen_range(0..self.files.len());
                let (basename, fd) = self.files.remove(idx);
                let fname = format!("{:x}", basename);
                info!("rm {}", fname);
                self.rt.as_ref().unwrap().block_on(async {
                    self.fs.unlink(&self.root, Some(&fd),
                        &OsString::from(&fname)).await
                }).unwrap();
            }
        }

        fn rmdir(&mut self) {
            if !self.dirs.is_empty() {
                let idx = self.rng.gen_range(0..self.dirs.len());
                let fname = format!("{:x}", self.dirs.remove(idx).0);
                info!("rmdir {}", fname);
                self.rt.as_ref().unwrap().block_on(async {
                    self.fs.rmdir(&self.root, &OsString::from(&fname)).await
                }).unwrap();
            }
        }

        fn shutdown(mut self) {
            let rt = self.rt.take().unwrap();
            rt.block_on(async {
                self.fs.inactive(self.root).await
            });
            drop(self.fs);
            let db = Arc::try_unwrap(self.db.take().unwrap())
                .ok().expect("Arc::try_unwrap");
            rt.block_on(db.shutdown());
        }

        fn step(&mut self) {
            match self.w[self.wi.sample(&mut self.rng)].0 {
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
            }
        }

        fn sync(&mut self) {
            info!("sync");
            self.rt.as_ref().unwrap().block_on(async {
                self.fs.sync().await;
            });
        }

        fn touch(&mut self) {
            // The BTree is basically a flat namespace, so there's little test
            // coverage to be gained by testing a hierarchical directory
            // structure.  Instead, we'll stick all files in the root directory,
            let num: u64 = self.rng.gen();
            let fname = format!("{:x}", num);
            info!("Touch {}", fname);
            let fd = self.rt.as_ref().unwrap().block_on(async {
                self.fs.create(&self.root, &OsString::from(&fname), 0o644, 0, 0)
                    .await
            }).unwrap();
            self.files.push((num, fd));
        }

        /// Write to a file.
        ///
        /// Writes just 2KB.  This may create inline or on-disk extents.  It may
        /// RMW on-disk extents.  The purpose is to exercise the tree, not large
        /// I/O.
        fn write(&mut self) {
            if !self.files.is_empty() {
                // Pick a random file to write to
                let idx = self.rng.gen_range(0..self.files.len());
                let fd = &self.files[idx].1;
                // Pick a random offset within the first 8KB
                let piece: u64 = self.rng.gen_range(0..4);
                let ofs = 2048 * piece;
                // Use a predictable fill value
                let fill = (fd.ino().wrapping_mul(piece) %
                            u64::from(u8::max_value()))
                    as u8;
                let buf = [fill; 2048];
                info!("write {:x} at offset {}", self.files[idx].0, ofs);
                self.rt.as_ref().unwrap().block_on(async {
                    let r = self.fs.write(fd, ofs, &buf[..], 0).await;
                    assert!(r.is_ok());
                })
            }
        }
    }

    fn torture_test(seed: Option<[u8; 16]>, freqs: Option<Vec<(Op, f64)>>,
                    zone_size: u64) -> TortureTest
    {
        static TRACINGSUBSCRIBER: Once = Once::new();
        TRACINGSUBSCRIBER.call_once(|| {
            tracing_subscriber::fmt()
                .pretty()
                .with_env_filter(EnvFilter::from_default_env())
                .init();
        });

        let rt = Runtime::new().unwrap();
        let len = 1 << 30;  // 1GB
        let tempdir = t!(Builder::new().prefix("test_fs").tempdir());
        let filename = tempdir.path().join("vdev");
        let file = t!(fs::File::create(&filename));
        t!(file.set_len(len));
        drop(file);
        let zone_size = NonZeroU64::new(zone_size);
        let cluster = Pool::create_cluster(None, 1, zone_size, 0,
                                           &[filename]);
        let pool = Pool::create(String::from("test_fs"), vec![cluster]);
        let cache = Arc::new(
            Mutex::new(
                Cache::with_capacity(32_000_000)
            )
        );
        let ddml = Arc::new(DDML::new(pool, cache.clone()));
        let idml = IDML::create(ddml, cache);
        let db = Arc::new(Database::create(Arc::new(idml)));
        let (tree_id, db) = rt.block_on(async move {
            let tree_id = db.create_fs(None, "").await.unwrap();
            (tree_id, db)
        });
        let fs = rt.block_on(async {
            Fs::new(db.clone(), tree_id).await
        });
        let seed = seed.unwrap_or_else(|| {
            let mut seed = [0u8; 16];
            let mut seeder = thread_rng();
            seeder.fill_bytes(&mut seed);
            seed
        });
        println!("Using seed {:?}", &seed);
        // Use XorShiftRng because it's deterministic and seedable
        let rng = XorShiftRng::from_seed(seed);

        TortureTest::new(db, fs, rng, rt, freqs)
    }

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
        let duration = duration.unwrap_or_else(|| Duration::from_secs(60));
        let start = Instant::now();
        while start.elapsed() < duration {
            torture_test.step()
        }
        torture_test.shutdown();
    }

    /// Randomly execute a long series of filesystem operations.
    #[rstest]
    #[case(torture_test(None, None, 512))]
    #[ignore = "Slow"]
    fn random(#[case] torture_test: TortureTest) {
        do_test(torture_test, None);
    }

    /// Randomly execute a series of filesystem operations, designed expecially
    /// to stress the cleaner.
    #[rstest]
    #[case(torture_test(
        None,
        Some(vec![
            (Op::Clean, 0.01),
            (Op::SyncAll, 0.03),
            (Op::Mkdir, 10.0),
            (Op::Touch, 10.0),
        ]),
        512
    ))]
    #[ignore = "Slow"]
    fn random_clean_zone(#[case] torture_test: TortureTest) {
        do_test(torture_test, Some(Duration::from_secs(10)));
    }
}
