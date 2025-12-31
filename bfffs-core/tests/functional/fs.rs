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
        property::*
    };
    use futures::TryStreamExt;
    use rand::{Rng, thread_rng};
    use rstest::rstest;
    use std::{
        collections::HashSet,
        ffi::{CStr, OsString, OsStr},
        os::raw::c_char,
        os::unix::ffi::OsStrExt,
        slice,
        sync::{Arc, Mutex}
    };

    struct Harness {
        fs: Fs,
        cache: Arc<Mutex<Cache>>,
        db: Arc<Database>
    }

    async fn harness(props: Vec<Property>) -> Harness {
        let ph = crate::PoolBuilder::new()
            .build();
        let cache = Arc::new(Mutex::new(Cache::with_capacity(16_000_000)));
        let cache2 = cache.clone();
        let ddml = Arc::new(DDML::new(ph.pool, cache2.clone()));
        let idml = IDML::create(ddml, cache2);
        let db = Arc::new(Database::create(Arc::new(idml)));
        let tree_id = db.create_fs(None, "").await.unwrap();
        let fs = Fs::new(db.clone(), tree_id).await;
        for prop in props.into_iter() {
            fs.set_prop(prop).await.unwrap();
        }
        Harness{fs, cache, db}
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
    async fn readdir_all(fs: &Fs, fd: &FileData, offs: i64) -> Vec<libc::dirent>
    {
        fs.readdir(fd, offs)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn create(#[future] h: Harness) {
        let uid = 12345;
        let gid = 54321;
        let name = OsStr::from_bytes(b"x");
        let root = h.fs.root();
        let rooth = root.handle();
        let fd0 = h.fs.create(&rooth, name, 0o644, uid, gid).await.unwrap();
        let fd1 = h.fs.lookup(None, &rooth, name).await.unwrap();
        assert_eq!(fd1.ino(), fd0.ino());

        // The parent dir should have an "x" directory entry
        let dirent = readdir_all(&h.fs, &rooth, 0).await
            .into_iter()
            .find(|dirent| {
                dirent.d_name[0] == 'x' as i8
            }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_REG);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, c"x");
        assert_eq!(dirent.d_fileno, fd0.ino());

        // The parent dir's link count should not have increased
        let parent_attr = h.fs.getattr(&rooth).await.unwrap();
        assert_eq!(parent_attr.nlink, 1);

        // Check the new file's attributes
        let attr = h.fs.getattr(&fd1.handle()).await.unwrap();
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
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn create_eexist(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let _fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await
            .unwrap();
        h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
    }

    /// Create should update the parent dir's timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn create_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        clear_timestamps(&h.fs, &rooth).await;

        h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await.unwrap();
        assert_ts_changed(&h.fs, &rooth, false, true, true, false).await;
    }

    /// Deallocate a whole extent
    // Need atime off to fs.read() doesn't dirty the tree and change the
    // f_bfree value.
    #[rstest]
    #[case(harness(vec![Property::RecordSize(12), Property::Atime(false)]), false)]
    #[case(harness(vec![Property::RecordSize(12), Property::Atime(false)]), true)]
    #[tokio::test]
    #[awt]
    async fn deallocate_whole_extent(
        #[future] #[case] h: Harness,
        #[case] blobs: bool
    ) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        clear_timestamps(&h.fs, &fdh).await;
        if blobs {
            h.fs.sync().await;        // Flush it to a BlobExtent
        }
        let stat1 = h.fs.statvfs().await.unwrap();

        assert!(h.fs.deallocate(&fdh, 0, 4096).await.is_ok());

        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.bytes, 4096);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;

        // Finally, read the deallocated record.  It should be a hole
        let sglist = h.fs.read(&fdh, 0, 4096).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);

        // And its storage should be freed, if it has been synced to disk
        if blobs {
            h.fs.sync().await;
            let stat2 = h.fs.statvfs().await.unwrap();
            let freed_bytes = ((stat2.f_bfree - stat1.f_bfree) * 4096) as usize;
            let expected = 4096;
            assert_eq!(expected, freed_bytes);
        }
    }

    /// Deallocating a hole is a no-op
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deallocate_hole(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let attr = SetAttr {
            size: Some(8192),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();
        clear_timestamps(&h.fs, &fdh).await;

        // Deallocate the hole
        assert!(h.fs.deallocate(&fdh, 0, 4096).await.is_ok());

        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.bytes, 0);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;
    }

    /// Deallocate multiple extents, some blob and some inline
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deallocate_multiple_extents(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        h.fs.sync().await;        // Flush them to BlobExtents
        // And write some inline extents, too
        let r = h.fs.write(&fdh, 8192, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        clear_timestamps(&h.fs, &fdh).await;

        assert!(h.fs.deallocate(&fdh, 0, 16384).await.is_ok());

        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.bytes, 0);
        assert_eq!(attr.size, 16384);
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;

        // Finally, read the deallocated area.  It should be a hole.
        let sglist = h.fs.read(&fdh, 0, 16384).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 16384];
        assert_eq!(&db[..], &expected[..]);
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deallocate_entire_partial_extent_from_left(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 6144];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        assert_eq!(Ok(6144), h.fs.write(&fdh, 0, &buf[..], 0).await);
        clear_timestamps(&h.fs, &fdh).await;

        assert!(h.fs.deallocate(&fdh, 4096, 2048).await.is_ok());

        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.bytes, 4096);
        assert_eq!(attr.size, 6144);
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;
        // Finally, verify that the deallocated space is zeroes.
        let sglist = h.fs.read(&fdh, 0, 6144).await.unwrap();
        let zbuf = [0u8; 2048];
        assert_eq!(sglist[0].len(), 4096);
        assert_eq!(&sglist[0][..], &buf[0..4096]);
        assert_eq!(&sglist[1][..], &zbuf[..]);
    }

    /// Deallocate the left part of an extent
    #[rstest]
    #[case(harness4k(), false)]
    #[case(harness4k(), true)]
    #[tokio::test]
    #[awt]
    async fn deallocate_left_half_of_extent(
        #[future] #[case] h: Harness,
        #[case] blobs: bool
    ) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        clear_timestamps(&h.fs, &fdh).await;
        if blobs {
            h.fs.sync().await;        // Flush it to a BlobExtent
        }

        assert!(h.fs.deallocate(&fdh, 0, 6144).await.is_ok());
        let attr = h.fs.getattr(&fdh).await.unwrap();
        // The partially deallocated extent still takes up space
        assert_eq!(attr.bytes, 4096);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;

        // Finally, read the partially deallocated record.  It should have a
        // hole in the middle.
        let sglist = h.fs.read(&fdh, 0, 8192).await.unwrap();
        let zbuf = [0u8; 4096];
        assert_eq!(&sglist[0][..], &zbuf[..]);
        assert_eq!(&sglist[1][..2048], &zbuf[..2048]);
        assert_eq!(&sglist[1][2048..], &buf[6144..]);
    }

    /// Deallocate the left part of a record which is already a hole
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deallocate_left_half_of_hole(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let attr = SetAttr {
            size: Some(8192),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();
        clear_timestamps(&h.fs, &fdh).await;

        assert!(h.fs.deallocate(&fdh, 0, 6144).await.is_ok());
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.bytes, 0);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;

        // Finally, read the partially deallocated record.
        let sglist = h.fs.read(&fdh, 0, 8192).await.unwrap();
        let zbuf = [0u8; 8192];
        assert_eq!(&sglist[0][..], &zbuf[..]);
    }

    /// Deallocate the middle of an extent
    #[rstest]
    #[case(harness4k(), false)]
    #[case(harness4k(), true)]
    #[tokio::test]
    #[awt]
    async fn deallocate_middle_of_extent(
        #[future] #[case] h: Harness,
        #[case] blobs: bool
    ) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);
        clear_timestamps(&h.fs, &fdh).await;
        if blobs {
            h.fs.sync().await;        // Flush it to a BlobExtent
        }

        assert!(h.fs.deallocate(&fdh, 1024, 2048).await.is_ok());
        let attr = h.fs.getattr(&fdh).await.unwrap();
        // The partially deallocated extent still takes up space
        assert_eq!(attr.bytes, 4096);
        assert_eq!(attr.size, 4096);
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;

        // Finally, read the partially deallocated record.  It should have a
        // hole in the middle.
        let sglist = h.fs.read(&fdh, 0, 4096).await.unwrap();
        let db = &sglist[0];
        let zbuf = [0u8; 2048];
        assert_eq!(&db[0..1024], &buf[0..1024]);
        assert_eq!(&db[1024..3072], &zbuf[0..2048]);
        assert_eq!(&db[3072..4096], &buf[3072..4096]);
    }

    /// Deallocate space from a partial hole: an extent that only has data in
    /// the bottom portion.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deallocate_partial_hole_from_start_of_extent(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 1024];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        assert_eq!(Ok(1024), h.fs.write(&fdh, 0, &buf[..], 0).await);
        let attr = SetAttr {
            size: Some(4096),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();
        clear_timestamps(&h.fs, &fdh).await;

        assert!(h.fs.deallocate(&fdh, 0, 2048).await.is_ok());

        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.bytes, 0);
        assert_eq!(attr.size, 4096);
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;
        // Finally, read the deallocated record.  It should be zeros
        let sglist = h.fs.read(&fdh, 0, 4096).await.unwrap();
        let zbuf = [0u8; 4096];
        assert_eq!(sglist[0].len(), 4096);
        assert_eq!(&sglist[0][..], &zbuf[..]);
    }

    /// Deallocate the right part of an extent
    #[rstest]
    #[case(harness4k(), false)]
    #[case(harness4k(), true)]
    #[tokio::test]
    #[awt]
    async fn deallocate_right_half_of_extent(
        #[future] #[case] h: Harness,
        #[case] blobs: bool)
    {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        clear_timestamps(&h.fs, &fdh).await;
        if blobs {
            h.fs.sync().await;        // Flush it to a BlobExtent
        }

        assert!(h.fs.deallocate(&fdh, 2048, 6144).await.is_ok());
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.bytes, 2048);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;

        // Finally, read the partially deallocated record.  It should have a
        // hole in the middle.
        let sglist = h.fs.read(&fdh, 0, 8192).await.unwrap();
        let zbuf = [0u8; 6144];
        assert_eq!(&sglist[0][..2048], &buf[..2048]);
        assert_eq!(&sglist[1][..], &zbuf[..]);
    }

    /// Deallocate the right part of a record which happens to be a hole
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deallocate_right_hole(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let attr = SetAttr {
            size: Some(8192),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();
        clear_timestamps(&h.fs, &fdh).await;

        assert!(h.fs.deallocate(&fdh, 2048, 6144).await.is_ok());
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.bytes, 0);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;

        // Finally, read the partially deallocated record.
        let sglist = h.fs.read(&fdh, 0, 8192).await.unwrap();
        let zbuf = [0u8; 8192];
        assert_eq!(&sglist[0][..], &zbuf[..]);
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deallocate_parts_of_two_records(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);
        clear_timestamps(&h.fs, &fdh).await;

        assert!(h.fs.deallocate(&fdh, 3072, 2048).await.is_ok());
        let attr = h.fs.getattr(&fdh).await.unwrap();
        // The partially deallocated extents still takes some space on the right
        // record, but not on the left.
        assert_eq!(attr.bytes, 7168);
        assert_eq!(attr.size, 8192);
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;

        // Finally, read the partially deallocated records.  They should have a
        // hole in the middle.
        let sglist = h.fs.read(&fdh, 0, 8192).await.unwrap();
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
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deallocate_past_eof(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        clear_timestamps(&h.fs, &fdh).await;

        // Deallocate past EoF
        assert!(h.fs.deallocate(&fdh, 0, 4096).await.is_ok());
        assert_ts_changed(&h.fs, &fdh, false, false, false, false).await;
    }

    /// Deallocate a whole extent and portions of two others
    /// This is a regression test for an insufficient credit bug that could
    /// only be triggered with 128kB record sizes or above.
    /// Found by FSX.
    #[tokio::test]
    async fn deallocate_whole_and_partial() {
        let exp = 17u8;
        let rs = 1usize << exp;
        let props = vec![Property::RecordSize(exp), Property::Atime(false)];
        let h = harness(props).await;
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let hdl = fd.handle();

        let buf = vec![0u8; rs * 3];
        assert_eq!(Ok(rs as u32 * 3), h.fs.write(&hdl, 0, &buf[..], 0).await);
        assert!(h.fs.deallocate(&hdl, rs as u64 / 2, rs as u64 * 2).await.is_ok());
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deleteextattr(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &name, &value[..]).await.unwrap();
        h.fs.deleteextattr(&fdh, ns, &name).await.unwrap();
        assert_eq!(h.fs.getextattr(&fdh, ns, &name).await.unwrap_err(),
            libc::ENOATTR);
    }

    /// Delete a blob extattr
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deleteextattr_blob(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = vec![42u8; 131072];
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &name, &value[..]).await.unwrap();
        h.fs.sync().await;
        let stat1 = h.fs.statvfs().await.unwrap();

        h.fs.deleteextattr(&fdh, ns, &name).await.unwrap();

        // The extattr should be gone
        assert_eq!(h.fs.getextattr(&fdh, ns, &name).await.unwrap_err(),
            libc::ENOATTR);

        // And its storage should be freed
        h.fs.sync().await;
        let stat2 = h.fs.statvfs().await.unwrap();
        let freed_bytes = ((stat2.f_bfree - stat1.f_bfree) * 4096) as usize;
        let expected = value.len();
        assert_eq!(expected, freed_bytes);
    }

    /// deleteextattr with a hash collision.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deleteextattr_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();

        // First try deleting the attributes in order
        h.fs.setextattr(&fdh, ns0, &name0, &value0[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns1, &name1, &value1[..]).await.unwrap();
        h.fs.deleteextattr(&fdh, ns0, &name0).await.unwrap();
        assert!(h.fs.getextattr(&fdh, ns0, &name0).await.is_err());
        assert!(h.fs.getextattr(&fdh, ns1, &name1).await.is_ok());
        h.fs.deleteextattr(&fdh, ns1, &name1).await.unwrap();
        assert!(h.fs.getextattr(&fdh, ns0, &name0).await.is_err());
        assert!(h.fs.getextattr(&fdh, ns1, &name1).await.is_err());

        // Repeat, this time out-of-order
        h.fs.setextattr(&fdh, ns0, &name0, &value0[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns1, &name1, &value1[..]).await.unwrap();
        h.fs.deleteextattr(&fdh, ns1, &name1).await.unwrap();
        assert!(h.fs.getextattr(&fdh, ns0, &name0).await.is_ok());
        assert!(h.fs.getextattr(&fdh, ns1, &name1).await.is_err());
        h.fs.deleteextattr(&fdh, ns0, &name0).await.unwrap();
        assert!(h.fs.getextattr(&fdh, ns0, &name0).await.is_err());
        assert!(h.fs.getextattr(&fdh, ns1, &name1).await.is_err());
    }

    /// deleteextattr of a nonexistent attribute that hash-collides with an
    /// existing one.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deleteextattr_collision_enoattr(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];

        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();

        h.fs.setextattr(&fdh, ns0, &name0, &value0[..]).await.unwrap();

        assert_eq!(h.fs.deleteextattr(&fdh, ns1, &name1).await,
                   Err(libc::ENOATTR));
        assert!(h.fs.getextattr(&fdh, ns0, &name0).await.is_ok());
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deleteextattr_enoattr(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        assert_eq!(h.fs.deleteextattr(&fdh, ns, &name).await,
                   Err(libc::ENOATTR));
    }

    /// rmextattr(2) should not modify any timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn deleteextattr_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &name, &value[..]).await.unwrap();
        clear_timestamps(&h.fs, &fdh).await;

        h.fs.deleteextattr(&fdh, ns, &name).await.unwrap();
        assert_ts_changed(&h.fs, &fdh, false, false, false, false).await;
    }

    /// Destroying a file system should free its contents
    // This is really a test of Database::destroy_fs, but it needs to be in this
    // file so we can populate the file system.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn destroy_fs(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let buf = vec![42u8; 4096];
        let ns = ExtAttrNamespace::User;
        let xname = OsString::from("foo");
        let xvalue = vec![42u8; 4096];
        h.fs.sync().await;

        let stat1 = h.db.stat().await;

        // Create a file with blob extents and extended attrs
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        assert_eq!(Ok(4096), h.fs.write(&fdh, 0, &buf[..], 0).await);
        h.fs.setextattr(&fdh, ns, &xname, &xvalue[..]).await.unwrap();
        h.fs.sync().await;

        let stat2 = h.db.stat().await;
        assert_eq!(stat2.used - stat1.used, 2);

        drop(h.fs);
        let tree_id = h.db.lookup_fs("").await.unwrap().1.unwrap();
        h.db.destroy_fs(None, tree_id, "").await.unwrap();

        // The fs tree and both blobs should've been deallocated.  Other
        // metadata stuff might've been deallocated too, in the forest or idml.
        let stat3 = h.db.stat().await;
        assert!(stat3.used < stat1.used);
    }

    // Dumps a nearly empty FS tree.  All of the real work is done in
    // Tree::dump, so the bulk of testing is in the tree tests.
    #[rstest(h, case(harness(vec![])))]
    #[tokio::test]
    #[awt]
    async fn dump_fs(#[future] h: Harness) {
        let mut buf = Vec::with_capacity(1024);
        let root = h.fs.root();
        let rooth = root.handle();
        // Sync before clearing timestamps to improve determinism; the timed
        // flusher may or may not have already flushed the tree.
        h.fs.sync().await;
        // Clear timestamps to make the dump output deterministic
        clear_timestamps(&h.fs, &rooth).await;
        h.fs.sync().await;
        h.fs.dump_fs(&mut buf).await.unwrap();

        let fs_tree = String::from_utf8(buf).unwrap();
        let expected =
r#"limits:
  min_int_fanout: 103
  max_int_fanout: 410
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
    ptr: !Addr 3
---
3: !Leaf
  credit: 0
  items:
    1-0-706caad497db23: !DirEntry
      ino: 1
      dtype: 4
      name: ..
    1-0-9b56c722640a46: !DirEntry
      ino: 1
      dtype: 4
      name: .
    1-1-00000000000000: !Inode
      size: 0
      bytes: 0
      nlink: 1
      flags: 0
      atime: 1970-01-01T00:00:00Z
      mtime: 1970-01-01T00:00:00Z
      ctime: 1970-01-01T00:00:00Z
      birthtime: 1970-01-01T00:00:00Z
      uid: 0
      gid: 0
      perm: 493
      file_type: Dir
"#;
        pretty_assertions::assert_eq!(expected, fs_tree);
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn get_prop_default(#[future] h: Harness) {

        let (val, source) = h.fs.get_prop(PropertyName::Atime)
            .await
            .unwrap();
        assert_eq!(val, Property::default_value(PropertyName::Atime));
        assert_eq!(source, PropertySource::Default);
    }

    /// getattr on the filesystem's root directory
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn getattr(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let attr = h.fs.getattr(&rooth).await.unwrap();
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
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn getattr_4k(#[future] h: Harness) {
        let name = OsStr::from_bytes(b"x");
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, name, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.blksize, 4096);
    }

    /// Regular files' st_blksize should equal the record size
    #[rstest(h, case(harness8k()))]
    #[tokio::test]
    #[awt]
    async fn getattr_8k(#[future] h: Harness) {
        let name = OsStr::from_bytes(b"y");
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, name, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.blksize, 8192);
    }

    // Get an extended attribute.  Very short extended attributes will remain
    // inline forever.
    #[rstest]
    #[case(harness4k(), false)]
    #[case(harness4k(), true)]
    #[tokio::test]
    #[awt]
    async fn getextattr(#[future] #[case] h: Harness, #[case] on_disk: bool) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let namespace = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, namespace, &name, &value[..]).await.unwrap();

        if on_disk {
            // Sync the filesystem to store the InlineExtent on disk
            h.fs.sync().await;

            // Drop cache
            h.cache.lock().unwrap().drop_cache();
        }

        assert_eq!(h.fs.getextattrlen(&fdh, namespace, &name).await.unwrap(),
                   3);
        let v = h.fs.getextattr(&fdh, namespace, &name).await.unwrap();
        assert_eq!(&v[..], &value);
    }

    /// Read a large extattr as a blob
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn getextattr_blob(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = vec![42u8; 4096];
        let namespace = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, namespace, &name, &value[..]).await.unwrap();

        // Sync the filesystem to flush the InlineExtent to a BlobExtent
        h.fs.sync().await;

        assert_eq!(h.fs.getextattrlen(&fdh, namespace, &name).await.unwrap(),
                   4096);
        let v = h.fs.getextattr(&fdh, namespace, &name).await.unwrap();
        assert_eq!(&v[..], &value[..]);
    }

    /// A collision between a blob extattr and an inline one.  Get the blob
    /// extattr.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn getextattr_blob_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = vec![42u8; 4096];

        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();

        h.fs.setextattr(&fdh, ns0, &name0, &value0[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns1, &name1, &value1[..]).await.unwrap();
        h.fs.sync().await; // Flush the large xattr into a blob
        assert_eq!(h.fs.getextattrlen(&fdh, ns1, &name1).await.unwrap(), 4096);
        let v1 = h.fs.getextattr(&fdh, ns1, &name1).await.unwrap();
        assert_eq!(&v1[..], &value1[..]);
    }

    /// setextattr and getextattr with a hash collision.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn getextattr_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();

        h.fs.setextattr(&fdh, ns0, &name0, &value0[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns1, &name1, &value1[..]).await.unwrap();
        assert_eq!(h.fs.getextattrlen(&fdh, ns0, &name0).await.unwrap(), 3);
        let v0 = h.fs.getextattr(&fdh, ns0, &name0).await.unwrap();
        assert_eq!(&v0[..], &value0);
        assert_eq!(h.fs.getextattrlen(&fdh, ns1, &name1).await.unwrap(), 4);
        let v1 = h.fs.getextattr(&fdh, ns1, &name1).await.unwrap();
        assert_eq!(&v1[..], &value1);
    }

    // The same attribute name exists in two namespaces
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn getextattr_dual_namespaces(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value1 = [1u8, 2, 3];
        let value2 = [4u8, 5, 6, 7];
        let ns1 = ExtAttrNamespace::User;
        let ns2 = ExtAttrNamespace::System;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns1, &name, &value1[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns2, &name, &value2[..]).await.unwrap();

        assert_eq!(h.fs.getextattrlen(&fdh, ns1, &name).await.unwrap(), 3);
        let v1 = h.fs.getextattr(&fdh, ns1, &name).await.unwrap();
        assert_eq!(&v1[..], &value1);

        assert_eq!(h.fs.getextattrlen(&fdh, ns2, &name).await.unwrap(), 4);
        let v2 = h.fs.getextattr(&fdh, ns2, &name).await.unwrap();
        assert_eq!(&v2[..], &value2);
    }

    // The file exists, but its extended attribute does not
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn getextattr_enoattr(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let namespace = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        assert_eq!(h.fs.getextattrlen(&fdh, namespace, &name).await,
                   Err(libc::ENOATTR));
        assert_eq!(h.fs.getextattr(&fdh, namespace, &name).await,
                   Err(libc::ENOATTR));
    }

    // The file does not exist.  Fortunately, VOP_GETEXTATTR(9) does not require
    // us to distinguish this from the ENOATTR case.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn getextattr_enoent(#[future] h: Harness) {
        let name = OsString::from("foo");
        let namespace = ExtAttrNamespace::User;
        let fd = FileDataMut::new_for_tests(Some(1), 9999);
        let fdh = fd.handle();
        assert_eq!(h.fs.getextattrlen(&fdh, namespace, &name).await,
                   Err(libc::ENOATTR));
        assert_eq!(h.fs.getextattr(&fdh, namespace, &name).await,
                   Err(libc::ENOATTR));
    }

    /// getextattr(2) should not modify any timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn getextattr_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let namespace = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, namespace, &name, &value[..]).await.unwrap();
        clear_timestamps(&h.fs, &fdh).await;

        h.fs.getextattr(&fdh, namespace, &name).await.unwrap();
        assert_ts_changed(&h.fs, &fdh, false, false, false, false).await;
    }

    // Lookup a directory by its inode number without knowing its parent
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn ilookup_dir(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let fd0 = h.fs.mkdir(&rooth, &filename, 0o755, 0, 0).await.unwrap();
        let ino = fd0.ino();
        h.fs.inactive(fd0).await;

        let fd1 = h.fs.ilookup(ino).await.unwrap();
        assert_eq!(fd1.ino(), ino);
        assert_eq!(fd1.lookup_count, 1);
        assert_eq!(fd1.parent(), Some(root.ino()));
    }

    // Try and fail to lookup a file by its inode nubmer
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn ilookup_enoent(#[future] h: Harness) {
        let ino = 123456789;

        let e = h.fs.ilookup(ino).await.unwrap_err();
        assert_eq!(e, libc::ENOENT);
    }

    // Lookup a regular file by its inode number without knowing its parent
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn ilookup_file(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let fd0 = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let ino = fd0.ino();
        h.fs.inactive(fd0).await;

        let fd1 = h.fs.ilookup(ino).await.unwrap();
        assert_eq!(fd1.ino(), ino);
        assert_eq!(fd1.lookup_count, 1);
        assert_eq!(fd1.parent(), None);
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn link(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let fd = h.fs.create(&rooth, &src, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.link(&rooth, &fdh, &dst).await.unwrap();

        // The target's link count should've increased
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.nlink, 2);

        // The parent should have a new directory entry
        assert_eq!(h.fs.lookup(None, &rooth, &dst).await.unwrap().ino(),
            fd.ino());
    }

    /// link(2) should update the inode's ctime
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn link_ctime(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let fd = h.fs.create(&rooth, &src, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        clear_timestamps(&h.fs, &fdh).await;
        h.fs.link(&rooth, &fdh, &dst).await.unwrap();
        assert_ts_changed(&h.fs, &fdh, false, false, true, false).await;
    }

    ///link(2) should update the parent's mtime and ctime
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn link_parent_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let fd = h.fs.create(&rooth, &src, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        clear_timestamps(&h.fs, &rooth).await;

        h.fs.link(&rooth, &fdh, &dst).await.unwrap();
        assert_ts_changed(&h.fs, &rooth, false, true, true, false).await;
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
                assert!(name.len() as u32 <= u32::from(u8::MAX));
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
                assert!(extattr.name().len() <= u8::MAX as usize);
                buf.push(extattr.name().len() as u8);
                buf.extend_from_slice(extattr.name().as_bytes());
            }
        }
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn listextattr(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let name2 = OsString::from("bar");
        let ns = ExtAttrNamespace::User;
        let value = [0u8, 1, 2];
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &name1, &value[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns, &name2, &value[..]).await.unwrap();

        // expected has the form of <length as u8><value as [u8]>...
        // values are _not_ null terminated.
        // There is no requirement on the order of names
        let expected = b"\x03bar\x03foo";

        let lenf = self::listextattr_lenf(ns);
        let lsf = self::listextattr_lsf(ns);
        assert_eq!(h.fs.listextattrlen(&fdh, lenf).await.unwrap(), 8);
        assert_eq!(&h.fs.listextattr(&fdh, 64, lsf).await.unwrap()[..],
                   &expected[..]);
    }

    /// setextattr and listextattr with a cross-namespace hash collision.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn listextattr_collision_separate_namespaces(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();

        h.fs.setextattr(&fdh, ns0, &name0, &value0[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns1, &name1, &value1[..]).await.unwrap();

        let expected0 = b"\x0aBWCdLQkApB";
        let lenf0 = self::listextattr_lenf(ns0);
        let lsf0 = self::listextattr_lsf(ns0);
        assert_eq!(h.fs.listextattrlen(&fdh, lenf0).await.unwrap(), 11);
        assert_eq!(&h.fs.listextattr(&fdh, 64, lsf0).await.unwrap()[..],
                   &expected0[..]);

        let expected1 = b"\x0aD6tLLI4mys";
        let lenf1 = self::listextattr_lenf(ns1);
        let lsf1 = self::listextattr_lsf(ns1);
        assert_eq!(h.fs.listextattrlen(&fdh, lenf1).await.unwrap(), 11);
        assert_eq!(&h.fs.listextattr(&fdh, 64, lsf1).await.unwrap()[..],
                   &expected1[..]);
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn listextattr_dual_namespaces(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let name2 = OsString::from("bean");
        let ns1 = ExtAttrNamespace::User;
        let ns2 = ExtAttrNamespace::System;
        let value1 = [0u8, 1, 2];
        let value2 = [3u8, 4, 5];

        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns1, &name1, &value1[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns2, &name2, &value2[..]).await.unwrap();

        // Test queries for a single namespace
        let lenf = self::listextattr_lenf(ns1);
        let lsf = self::listextattr_lsf(ns1);
        assert_eq!(h.fs.listextattrlen(&fdh, lenf).await, Ok(4));
        assert_eq!(&h.fs.listextattr(&fdh, 64, lsf).await.unwrap()[..],
                   &b"\x03foo"[..]);
        let lenf = self::listextattr_lenf(ns2);
        let lsf = self::listextattr_lsf(ns2);
        assert_eq!(h.fs.listextattrlen(&fdh, lenf).await, Ok(5));
        assert_eq!(&h.fs.listextattr(&fdh, 64, lsf).await.unwrap()[..],
                   &b"\x04bean"[..]);
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn listextattr_empty(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let lenf = self::listextattr_lenf(ExtAttrNamespace::User);
        let lsf = self::listextattr_lsf(ExtAttrNamespace::User);
        assert_eq!(h.fs.listextattrlen(&fdh, lenf).await, Ok(0));
        assert!(h.fs.listextattr(&fdh, 64, lsf).await.unwrap().is_empty());
    }

    /// Lookup of a directory entry that has a hash collision
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn lookup_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = h.fs.create(&rooth, &filename0, 0o644, 0, 0).await.unwrap();
        let fd1 = h.fs.create(&rooth, &filename1, 0o644, 0, 0).await.unwrap();

        assert_eq!(h.fs.lookup(None, &rooth, &filename0).await.unwrap().ino(),
            fd0.ino());
        assert_eq!(h.fs.lookup(None, &rooth, &filename1).await.unwrap().ino(),
            fd1.ino());
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn lookup_dot(#[future] h: Harness) {
        let name0 = OsStr::from_bytes(b"x");
        let dotname = OsStr::from_bytes(b".");

        let root = h.fs.root();
        let rooth = root.handle();
        let fd0 = h.fs.mkdir(&rooth, name0, 0o755, 0, 0).await.unwrap();

        let fd1 = h.fs.lookup(Some(&rooth), &fd0.handle(), dotname).await.unwrap();
        assert_eq!(fd1.ino(), fd0.ino());
        assert_eq!(fd1.parent(), Some(root.ino()));
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn lookup_dotdot(#[future] h: Harness) {
        let name0 = OsStr::from_bytes(b"x");
        let name1 = OsStr::from_bytes(b"y");
        let dotdotname = OsStr::from_bytes(b"..");

        let root = h.fs.root();
        let rooth = root.handle();
        let fd0 = h.fs.mkdir(&rooth, name0, 0o755, 0, 0).await.unwrap();
        let fd1 = h.fs.mkdir(&fd0.handle(), name1, 0o755, 0, 0).await.unwrap();

        let fd2 = h.fs.lookup(Some(&fd0.handle()), &fd1.handle(), dotdotname).await.unwrap();
        assert_eq!(fd2.ino(), fd0.ino());
        assert_eq!(fd2.parent(), Some(root.ino()));
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn lookup_enoent(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("nonexistent");
        assert_eq!(h.fs.lookup(None, &rooth, &filename).await.unwrap_err(),
            libc::ENOENT);
    }

    #[rstest]
    #[case(harness4k(), false)]
    #[case(harness4k(), true)]
    #[tokio::test]
    #[awt]
    async fn lseek(#[future] #[case] h: Harness, #[case] blobs: bool) {
        use SeekWhence::{Data, Hole};
        let root = h.fs.root();
        let rooth = root.handle();

        // Create a file like this:
        // |      |======|      |======|>
        // | hole | data | hole | data |EOF
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let buf = vec![42u8; 4096];
        h.fs.write(&fdh, 4096, &buf[..], 0).await.unwrap();
        h.fs.write(&fdh, 12288, &buf[..], 0).await.unwrap();
        if blobs {
            // Sync the filesystem to flush the InlineExtents to BlobExtents
            h.fs.sync().await;
        }

        // SeekData past EOF
        assert_eq!(Err(libc::ENXIO), h.fs.lseek(&fdh, 16485, Data).await);
        // SeekHole past EOF
        assert_eq!(Err(libc::ENXIO), h.fs.lseek(&fdh, 16485, Hole).await);
        // SeekHole with no hole until EOF
        assert_eq!(Ok(16384), h.fs.lseek(&fdh, 12288, Hole).await);
        // SeekData at start of data
        assert_eq!(Ok(4096), h.fs.lseek(&fdh, 4096, Data).await);
        // SeekHole at start of hole
        assert_eq!(Ok(8192), h.fs.lseek(&fdh, 8192, Hole).await);
        // SeekData in middle of data
        assert_eq!(Ok(6144), h.fs.lseek(&fdh, 6144, Data).await);
        // SeekHole in middle of hole
        assert_eq!(Ok(2048), h.fs.lseek(&fdh, 2048, Hole).await);
        // SeekData before some data
        assert_eq!(Ok(4096), h.fs.lseek(&fdh, 2048, Data).await);
        // SeekHole before a hole
        assert_eq!(Ok(8192), h.fs.lseek(&fdh, 6144, Hole).await);
    }

    // SeekData should return ENXIO if there is no data until EOF
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn lseek_data_before_eof(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();

        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let attr = SetAttr {
            size: Some(4096),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();

        let r = h.fs.lseek(&fdh, 0, SeekWhence::Data).await;
        assert_eq!(r, Err(libc::ENXIO));
    }

    #[rstest]
    #[case(harness4k(), false)]
    #[case(harness4k(), true)]
    #[tokio::test]
    #[awt]
    async fn lseek_last_hole(#[future] #[case] h: Harness, #[case] blobs: bool)
    {
        use SeekWhence::Hole;
        let root = h.fs.root();
        let rooth = root.handle();

        // Create a file like this:
        // |======|      |>
        // | data | hole |EOF
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let buf = vec![42u8; 4096];
        h.fs.write(&fdh, 0, &buf[..], 0).await.unwrap();
        let attr = SetAttr {
            size: Some(8192),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();
        if blobs {
            // Sync the filesystem to flush the InlineExtents to BlobExtents
            h.fs.sync().await;
        }

        // SeekHole prior to the last hole in the file
        assert_eq!(Ok(4096), h.fs.lseek(&fdh, 0, Hole).await);
    }

    /// The file ends with a data extent followed by a hole in the same record
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn lseek_partial_hole_at_end(#[future] h: Harness) {
        use SeekWhence::Hole;
        let root = h.fs.root();
        let rooth = root.handle();

        // Create a file like this, all in one record:
        // |======|      |>
        // | data | hole |EOF
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let buf = vec![42u8; 2048];
        h.fs.write(&fdh, 0, &buf[..], 0).await.unwrap();
        let attr = SetAttr {
            size: Some(4096),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();

        // SeekHole in the middle of the final hole should return EOF
        assert_eq!(Ok(4096), h.fs.lseek(&fdh, 3072, Hole).await);
        // SeekData in the middle of the final hole should return ENXIO
        let r = h.fs.lseek(&fdh, 3072, SeekWhence::Data).await;
        assert_eq!(r, Err(libc::ENXIO));
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mkdir(#[future] h: Harness) {
        let uid = 12345;
        let gid = 54321;
        let name = OsStr::from_bytes(b"x");
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.mkdir(&rooth, name, 0o755, uid, gid).await
        .unwrap();
        let fdh = fd.handle();
        let fd1 = h.fs.lookup(None, &rooth, name).await.unwrap();
        assert_eq!(fd1.ino(), fd.ino());

        // The new dir should have "." and ".." directory entries
        let entries = readdir_all(&h.fs, &fdh, 0).await;
        let dotdot = entries[0];
        assert_eq!(dotdot.d_type, libc::DT_DIR);
        let dotdot_name = unsafe{
            CStr::from_ptr(&dotdot.d_name as *const c_char)
        };
        assert_eq!(dotdot_name, c"..");
        assert_eq!(dotdot.d_fileno, root.ino());
        let dot = entries[1];
        assert_eq!(dot.d_type, libc::DT_DIR);
        let dot_name = unsafe{
            CStr::from_ptr(&dot.d_name as *const c_char)
        };
        assert_eq!(dot_name, c".");
        assert_eq!(dot.d_fileno, fd.ino());

        // The parent dir should have an "x" directory entry
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        let dirent = entries
        .into_iter()
        .find(|dirent| {
            dirent.d_name[0] == 'x' as i8
        }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_DIR);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, c"x");
        assert_eq!(dirent.d_fileno, fd.ino());

        // The parent dir's link count should've increased
        let parent_attr = h.fs.getattr(&rooth).await.unwrap();
        assert_eq!(parent_attr.nlink, 2);

        // Check the new directory's attributes
        let attr = h.fs.getattr(&fdh).await.unwrap();
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
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mkdir_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = h.fs.mkdir(&rooth, &filename0, 0o755, 0, 0).await.unwrap();
        let fd1 = h.fs.mkdir(&rooth, &filename1, 0o755, 0, 0).await.unwrap();

        assert_eq!(h.fs.lookup(None, &rooth, &filename0).await.unwrap().ino(),
            fd0.ino());
        assert_eq!(h.fs.lookup(None, &rooth, &filename1).await.unwrap().ino(),
            fd1.ino());
    }

    /// mkdir(2) should update the parent dir's timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mkdir_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        clear_timestamps(&h.fs, &rooth).await;

        h.fs.mkdir(&rooth, &OsString::from("x"), 0o755, 0, 0).await.unwrap();
        assert_ts_changed(&h.fs, &rooth, false, true, true, false).await;
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mkchar(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.mkchar(&rooth, &OsString::from("x"), 0o644, 0, 0, 42).await
        .unwrap();
        let fdh = fd.handle();
        let attr = h.fs.getattr(&fdh).await.unwrap();
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
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        let dirent = entries
        .into_iter()
        .find(|dirent| {
            dirent.d_name[0] == 'x' as i8
        }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_CHR);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, c"x");
        assert_eq!(dirent.d_fileno, fd.ino());
    }

    /// mknod(2) should update the parent dir's timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mkchar_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        clear_timestamps(&h.fs, &rooth).await;

        h.fs.mkchar(&rooth, &OsString::from("x"), 0o644, 0, 0, 42).await.unwrap();
        assert_ts_changed(&h.fs, &rooth, false, true, true, false).await;
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mkblock(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.mkblock(&rooth, &OsString::from("x"), 0o644, 0, 0, 42).await
        .unwrap();
        let fdh = fd.handle();
        let attr = h.fs.getattr(&fdh).await.unwrap();
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
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        let dirent = entries
        .into_iter()
        .find(|dirent| {
            dirent.d_name[0] == 'x' as i8
        }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_BLK);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, c"x");
        assert_eq!(dirent.d_fileno, fd.ino());
    }

    /// mknod(2) should update the parent dir's timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mkblock_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        clear_timestamps(&h.fs, &rooth).await;

        h.fs.mkblock(&rooth, &OsString::from("x"), 0o644, 0, 0, 42).await.unwrap();
        assert_ts_changed(&h.fs, &rooth, false, true, true, false).await;
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mkfifo(#[future] h: Harness) {
        let uid = 12345;
        let gid = 54321;
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.mkfifo(&rooth, &OsString::from("x"), 0o644, uid, gid).await
        .unwrap();
        let fdh = fd.handle();
        let attr = h.fs.getattr(&fdh).await.unwrap();
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
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        let dirent = entries
        .into_iter()
        .find(|dirent| {
            dirent.d_name[0] == 'x' as i8
        }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_FIFO);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, c"x");
        assert_eq!(dirent.d_fileno, fd.ino());
    }

    /// mkfifo(2) should update the parent dir's timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mkfifo_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        clear_timestamps(&h.fs, &rooth).await;

        h.fs.mkfifo(&rooth, &OsString::from("x"), 0o644, 0, 0).await.unwrap();
        assert_ts_changed(&h.fs, &rooth, false, true, true, false).await;
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mksock(#[future] h: Harness) {
        let uid = 12345;
        let gid = 54321;
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.mksock(&rooth, &OsString::from("x"), 0o644, uid, gid).await
        .unwrap();
        let fdh = fd.handle();
        let attr = h.fs.getattr(&fdh).await.unwrap();
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
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        let dirent = entries
        .into_iter()
        .find(|dirent| {
            dirent.d_name[0] == 'x' as i8
        }).expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_SOCK);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, c"x");
        assert_eq!(dirent.d_fileno, fd.ino());
    }

    /// mksock(2) should update the parent dir's timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mksock_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        clear_timestamps(&h.fs, &rooth).await;

        h.fs.mkfifo(&rooth, &OsString::from("x"), 0o644, 0, 0).await.unwrap();
        assert_ts_changed(&h.fs, &rooth, false, true, true, false).await;
    }

    // If the file system was unmounted uncleanly and has open but deleted
    // files, they should be deleted during mount
    #[cfg(debug_assertions)]
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn mount_with_open_but_deleted_files(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();

        // First create a file, open it, and unlink it, but don't close it
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let ino = fd.ino();
        let r = h.fs.unlink(&rooth, Some(&fdh), &filename).await;
        h.fs.sync().await;
        assert_eq!(Ok(()), r);

        // Unmount, without closing the file
        drop(h.fs);

        // Mount again
        let tree_id = h.db.lookup_fs("").await.unwrap().1.unwrap();
        let fs = Fs::new(h.db, tree_id).await;

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
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_big_hole(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let holesize = 2 * ZERO_REGION_LEN;
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, holesize as u64, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        let sglist = h.fs.read(&fdh, 0, holesize).await.unwrap();
        let expected = vec![0u8; ZERO_REGION_LEN];
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &expected[..]);
        assert_eq!(&sglist[1][..], &expected[..]);
    }

    // Read a single BlobExtent record
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_blob(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let buf = vec![42u8; 4096];
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        // Sync the filesystem to flush the InlineExtent to a BlobExtent
        h.fs.sync().await;

        let sglist = h.fs.read(&fdh, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[..]);
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_empty_file(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let sglist = h.fs.read(&fdh, 0, 1024).await.unwrap();
        assert!(sglist.is_empty());
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_empty_file_past_start(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let sglist = h.fs.read(&fdh, 2048, 2048).await.unwrap();
        assert!(sglist.is_empty());
    }

    // Read a hole within a sparse file
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_hole(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 4096, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        let sglist = h.fs.read(&fdh, 0, 4096).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);
    }

    // Read a hole in between two adjacent records.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_partial_hole_between_recs(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(2048), r);
        let r = h.fs.write(&fdh, 4096, &buf[..], 0).await;
        assert_eq!(Ok(2048), r);

        // The file should now have a hole from offset 2048 to 4096
        let sglist = h.fs.read(&fdh, 3072, 1024).await.unwrap();
        let db = &sglist[0];
        let zbuf = [0u8; 2048];
        assert_eq!(&db[..], &zbuf[..1024]);

        // It should also be possible to read data and hole in one operation
        let sglist = h.fs.read(&fdh, 0, 4096).await.unwrap();
        assert_eq!(&sglist[0][..], &buf[..]);
        assert_eq!(&sglist[1][..], &zbuf[..]);
    }

    // Read a chunk of a file that includes a partial hole at the beginning and
    // data at the end.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_partial_hole_trailing_edge(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(2048), r);
        let r = h.fs.write(&fdh, 4096, &buf[..], 0).await;
        assert_eq!(Ok(2048), r);

        // The file should now have a hole from offset 2048 to 4096
        let sglist = h.fs.read(&fdh, 3072, 2048).await.unwrap();
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &[0u8; 1024][..]);
        assert_eq!(&sglist[1][..], &buf[0..1024]);
    }

    // A read that's smaller than a record, at both ends
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_partial_record(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        let sglist = h.fs.read(&fdh, 1024, 2048).await.unwrap();
        let db = &sglist[0];
        assert_eq!(db.len(), 2048);
        assert_eq!(&db[..], &buf[1024..3072]);
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_past_eof(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(2048), r);

        let sglist = h.fs.read(&fdh, 2048, 1024).await.unwrap();
        assert!(sglist.is_empty());
    }

    /// A read that spans 3 records, where the middle record is a hole
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_spans_hole(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        assert_eq!(4096, h.fs.write(&fdh, 0, &buf[..], 0).await.unwrap());
        assert_eq!(4096, h.fs.write(&fdh, 8192, &buf[..], 0).await.unwrap());

        let sglist = h.fs.read(&fdh, 0, 12288).await.unwrap();
        assert_eq!(sglist.len(), 3);
        assert_eq!(&sglist[0][..], &buf[..]);
        assert_eq!(&sglist[1][..], &[0u8; 4096][..]);
        assert_eq!(&sglist[2][..], &buf[..]);
    }

    /// read(2) should update the file's atime
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let buf = vec![42u8; 4096];
        h.fs.write(&fdh, 0, &buf[..], 0).await.unwrap();
        clear_timestamps(&h.fs, &fdh).await;

        h.fs.read(&fdh, 0, 4096).await.unwrap();
        assert_ts_changed(&h.fs, &fdh, true, false, false, false).await;
    }

    // When atime is disabled, reading a file should not update its atime.
    #[rstest(h, case(harness(vec![Property::Atime(false)])))]
    #[tokio::test]
    #[awt]
    async fn read_timestamps_no_atime(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();

        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
            .unwrap();
        let fdh = fd.handle();
        let buf = vec![42u8; 4096];
        h.fs.write(&fdh, 0, &buf[..], 0).await.unwrap();
        clear_timestamps(&h.fs, &fdh).await;

        h.fs.read(&fdh, 0, 4096).await.unwrap();
        assert_ts_changed(&h.fs, &fdh, false, false, false, false).await;
    }

    // A read that's split across two records
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_two_recs(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[0..4096], 0).await;
        assert_eq!(Ok(4096), r);
        let r = h.fs.write(&fdh, 4096, &buf[4096..8192], 0).await;
        assert_eq!(Ok(4096), r);

        let sglist = h.fs.read(&fdh, 0, 8192).await.unwrap();
        assert_eq!(2, sglist.len(), "Read didn't span multiple records");
        let db0 = &sglist[0];
        assert_eq!(&db0[..], &buf[0..4096]);
        let db1 = &sglist[1];
        assert_eq!(&db1[..], &buf[4096..8192]);
    }

    // Read past EOF, in an entirely different record
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn read_well_past_eof(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        let sglist = h.fs.read(&fdh, 1 << 30, 4096).await.unwrap();
        assert!(sglist.is_empty());
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn readdir(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        let dotdot = entries[0];
        assert_eq!(dotdot.d_type, libc::DT_DIR);
        let dotdot_name = unsafe{
            CStr::from_ptr(&dotdot.d_name as *const c_char)
        };
        assert_eq!(dotdot_name, c"..");
        assert_eq!(dotdot.d_namlen, 2);
        let dot = entries[1];
        assert_eq!(dot.d_type, libc::DT_DIR);
        let dot_name = unsafe{
            CStr::from_ptr(&dot.d_name as *const c_char)
        };
        assert_eq!(dot_name, c".");
        assert_eq!(dot.d_namlen, 1);
        assert_eq!(dot.d_fileno, root.ino());
    }

    /// Readdir beginning at the offset of the last dirent.  The NFS server will
    /// do this sometimes.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn readdir_eof(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        // Should be two entries, "." and ".."
        assert_eq!(2, entries.len());
        let ofs = entries[1].d_off;
        let entries2 = readdir_all(&h.fs, &rooth, ofs).await;
        // Nothing should be returned
        assert!(entries2.is_empty());
    }

    // Readdir of a directory with a hash collision
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn readdir_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);

        h.fs.create(&rooth, &filename0, 0o644, 0, 0).await.unwrap();
        h.fs.create(&rooth, &filename1, 0o644, 0, 0).await.unwrap();

        // There's no requirement for the order of readdir's output.
        let mut expected = HashSet::new();
        expected.insert(OsString::from("."));
        expected.insert(OsString::from(".."));
        expected.insert(filename0);
        expected.insert(filename1);
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        for entry in entries.into_iter() {
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
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn readdir_collision_at_offset(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);

        let _fd0 = h.fs.create(&rooth, &filename0, 0o644, 0, 0).await.unwrap();
        let fd1 = h.fs.create(&rooth, &filename1, 0o644, 0, 0).await.unwrap();

        // There's no requirement for the order of readdir's output, but
        // filename1 happens to come first.
        let mut stream0 = Box::pin(h.fs.readdir(&rooth, 0));
        let result0 = stream0.try_next().await.unwrap().unwrap();
        assert_eq!(result0.d_fileno, fd1.ino());

        // Now interrupt the stream, and resume with the supplied offset.
        let mut expected = HashSet::new();
        expected.insert(OsString::from("."));
        expected.insert(OsString::from(".."));
        expected.insert(filename0);
        drop(stream0);
        let entries = readdir_all(&h.fs, &rooth, result0.d_off).await;
        for entry in entries.into_iter() {
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
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn readdir_rm_during_stream_at_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);

        let _fd0 = h.fs.create(&rooth, &filename0, 0o644, 0, 0).await.unwrap();
        let fd1 = h.fs.create(&rooth, &filename1, 0o644, 0, 0).await.unwrap();

        // There's no requirement for the order of readdir's output, but
        // filename1 happens to come first.
        let mut stream0 = Box::pin(h.fs.readdir(&rooth, 0));
        let result0 = stream0.try_next().await.unwrap().unwrap();
        assert_eq!(result0.d_fileno, fd1.ino());

        // Now interrupt the stream, remove the first has bucket entry, and
        // resume with the supplied offset.
        let r = h.fs.unlink(&rooth, Some(&fd1.handle()), &filename1).await;
        assert_eq!(Ok(()), r);
        let mut expected = HashSet::new();
        expected.insert(OsString::from("."));
        expected.insert(OsString::from(".."));
        expected.insert(filename0);
        drop(stream0);
        let entries = readdir_all(&h.fs, &rooth, result0.d_off).await;
        for entry in entries.into_iter() {
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
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn readdir_partial(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let mut stream = Box::pin(h.fs.readdir(&rooth, 0));
        let _ = stream.try_next().await.unwrap().unwrap();
    }

    /// readdir(2) should not update any timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn readdir_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        clear_timestamps(&h.fs, &rooth).await;

        let _entries = readdir_all(&h.fs, &rooth, 0).await;
        assert_ts_changed(&h.fs, &rooth, false, false, false, false).await;
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn readlink(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let fd = h.fs.symlink(&rooth, &srcname, 0o642, 0, 0, &dstname).await
        .unwrap();
        let fdh = fd.handle();
        let output = h.fs.readlink(&fdh).await.unwrap();
        assert_eq!(dstname, output);
    }

    // Calling readlink on a non-symlink should return EINVAL
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn readlink_einval(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let output = h.fs.readlink(&rooth).await;
        assert_eq!(libc::EINVAL, output.unwrap_err());
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn readlink_enoent(#[future] h: Harness) {
        let fd = FileDataMut::new_for_tests(Some(1), 1000);
        let fdh = fd.handle();
        let output = h.fs.readlink(&fdh).await;
        assert_eq!(libc::ENOENT, output.unwrap_err());
    }

    /// readlink(2) should not update any timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn readlink_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let fd = h.fs.symlink(&rooth, &srcname, 0o642, 0, 0, &dstname).await
        .unwrap();
        let fdh = fd.handle();
        clear_timestamps(&h.fs, &fdh).await;

        let output = h.fs.readlink(&fdh).await.unwrap();
        assert_eq!(dstname, output);
        assert_ts_changed(&h.fs, &fdh, false, false, false, false).await;
    }

    // Rename a file that has a hash collision in both the source and
    // destination directories
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("F0jS2Tptj7");
        let src_c = OsString::from("PLe01T116a");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("Gg1AG3wll2");
        let dst_c = OsString::from("FDCIlvDxYn");
        let dstdir = OsString::from("dstdir");
        assert_dirents_collide(&src, &src_c);
        assert_dirents_collide(&dst, &dst_c);

        let srcdir_fd = h.fs.mkdir(&rooth, &srcdir, 0o755, 0, 0).await.unwrap();
        let dstdir_fd = h.fs.mkdir(&rooth, &dstdir, 0o755, 0, 0).await.unwrap();
        let srcdir_fdh = srcdir_fd.handle();
        let dstdir_fdh = dstdir_fd.handle();
        let src_c_fd = h.fs.create(&srcdir_fdh, &src_c, 0o644, 0, 0).await
        .unwrap();
        let src_c_ino = src_c_fd.ino();
        let dst_c_fd = h.fs.create(&dstdir_fdh, &dst_c, 0o644, 0, 0).await
        .unwrap();
        let dst_c_ino = dst_c_fd.ino();
        let src_fd = h.fs.create(&srcdir_fdh, &src, 0o644, 0, 0).await.unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = h.fs.create(&dstdir_fdh, &dst, 0o644, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let dst_ino = dst_fd.ino();

        assert_eq!(src_fd.ino(),
            h.fs.rename(&srcdir_fdh, &src_fd.handle(), &src, &dstdir_fdh,
                Some(dst_fd.ino()), &dst).await
            .unwrap()
        );

        h.fs.inactive(src_fd).await;
        assert_eq!(src_ino,
            h.fs.lookup(Some(&rooth), &dstdir_fdh, &dst).await.unwrap().ino()
        );
        let r = h.fs.lookup(Some(&rooth), &srcdir_fdh, &src).await;
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let srcdir_inode = h.fs.getattr(&srcdir_fdh).await.unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = h.fs.getattr(&dstdir_fdh).await.unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(dst_ino).await, Err(libc::ENOENT));
        }

        // Finally, make sure we didn't upset the colliding files
        h.fs.inactive(src_c_fd).await;
        h.fs.inactive(dst_c_fd).await;
        let src_c_fd1 = h.fs.lookup(Some(&rooth), &srcdir_fdh, &src_c).await;
        assert_eq!(src_c_fd1.unwrap().ino(), src_c_ino);
        let dst_c_fd1 = h.fs.lookup(Some(&rooth), &dstdir_fdh, &dst_c).await;
        assert_eq!(dst_c_fd1.unwrap().ino(), dst_c_ino);
    }

    // Rename a directory.  The target is also a directory
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_dir_to_dir(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = h.fs.mkdir(&rooth, &srcdir, 0o755, 0, 0).await
        .unwrap();
        let dstdir_fd = h.fs.mkdir(&rooth, &dstdir, 0o755, 0, 0).await
        .unwrap();
        let srcdir_fdh = srcdir_fd.handle();
        let dstdir_fdh = dstdir_fd.handle();
        let src_fd = h.fs.mkdir(&srcdir_fdh, &src, 0o755, 0, 0).await
        .unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = h.fs.mkdir(&dstdir_fdh, &dst, 0o755, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let dst_ino = dst_fd.ino();

        assert_eq!(src_fd.ino(),
            h.fs.rename(&srcdir_fdh, &src_fd.handle(), &src, &dstdir_fdh,
                Some(dst_fd.ino()), &dst).await.unwrap()
        );

        h.fs.inactive(src_fd).await;
        let dst_fd1 = h.fs.lookup(Some(&rooth), &dstdir_fdh, &dst).await;
        assert_eq!(dst_fd1.unwrap().ino(), src_ino);
        let r = h.fs.lookup(Some(&rooth), &srcdir_fdh, &src).await;
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let srcdir_inode = h.fs.getattr(&srcdir_fdh).await.unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = h.fs.getattr(&dstdir_fdh).await.unwrap();
        assert_eq!(dstdir_inode.nlink, 3);
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(dst_ino).await, Err(libc::ENOENT));
        }
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_dir_to_dir_same_parent(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let parent = OsString::from("parent");
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let parent_fd = h.fs.mkdir(&rooth, &parent, 0o755, 0, 0).await.unwrap();
        let parent_fdh = parent_fd.handle();
        let src_fd = h.fs.mkdir(&parent_fdh, &src, 0o755, 0, 0).await.unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = h.fs.mkdir(&parent_fdh, &dst, 0o755, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let dst_ino = dst_fd.ino();

        assert_eq!(src_fd.ino(),
            h.fs.rename(&parent_fdh, &src_fd.handle(), &src, &parent_fdh,
                Some(dst_fd.ino()), &dst).await
            .unwrap()
        );

        h.fs.inactive(src_fd).await;
        let dst_fd1 = h.fs.lookup(Some(&rooth), &parent_fdh, &dst).await;
        assert_eq!(dst_fd1.unwrap().ino(), src_ino);
        let r = h.fs.lookup(Some(&rooth), &parent_fdh, &src).await;
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let parent_inode = h.fs.getattr(&parent_fdh).await.unwrap();
        assert_eq!(parent_inode.nlink, 3);
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(dst_ino).await, Err(libc::ENOENT));
        }
    }

    // Rename a directory.  The target is also a directory that isn't empty.
    // Nothing should change.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_dir_to_nonemptydir(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let dstf = OsString::from("dstf");
        let srcdir_fd = h.fs.mkdir(&rooth, &srcdir, 0o755, 0, 0).await
        .unwrap();
        let dstdir_fd = h.fs.mkdir(&rooth, &dstdir, 0o755, 0, 0).await
        .unwrap();
        let srcdir_fdh = srcdir_fd.handle();
        let dstdir_fdh = dstdir_fd.handle();
        let src_fd = h.fs.mkdir(&srcdir_fdh, &src, 0o755, 0, 0).await
        .unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = h.fs.mkdir(&dstdir_fdh, &dst, 0o755, 0, 0).await
        .unwrap();
        let dst_ino = dst_fd.ino();
        let dstf_fd = h.fs.create(&dst_fd.handle(), &dstf, 0o644, 0, 0).await
        .unwrap();

        let r = h.fs.rename(&srcdir_fdh, &src_fd.handle(), &src,
            &dstdir_fdh, Some(dst_fd.ino()), &dst).await;
        assert_eq!(r, Err(libc::ENOTEMPTY));

        h.fs.inactive(src_fd).await;
        assert_eq!(src_ino,
            h.fs.lookup(Some(&rooth), &srcdir_fdh, &src).await.unwrap().ino()
        );
        let dst_fd1 = h.fs.lookup(Some(&rooth), &dstdir_fdh, &dst).await
        .unwrap();
        assert_eq!(dst_fd1.ino(), dst_ino);
        let dstf_fd1 = h.fs.lookup(Some(&dstdir_fdh), &dst_fd1.handle(), &dstf)
            .await;
        assert_eq!(dstf_fd1.unwrap().ino(), dstf_fd.ino());
    }

    // Rename a directory.  The target name does not exist
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_dir_to_nothing(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let dotdotname = OsStr::from_bytes(b"..");
        let srcdir_fd = h.fs.mkdir(&rooth, &srcdir, 0o755, 0, 0).await
        .unwrap();
        let dstdir_fd = h.fs.mkdir(&rooth, &dstdir, 0o755, 0, 0).await
        .unwrap();
        let srcdir_fdh = srcdir_fd.handle();
        let dstdir_fdh = dstdir_fd.handle();
        let mut fd = h.fs.mkdir(&srcdir_fdh, &src, 0o755, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let src_ino = fd.ino();

        assert_eq!(fd.ino(),
            h.fs.rename(&srcdir_fdh, &fdh, &src, &dstdir_fdh,
                None, &dst).await
            .unwrap()
        );
        fd.reparent(dstdir_fd.ino());

        // Check that the moved directory's parent is correct in memory
        assert_eq!(dstdir_fd.ino(), fd.parent().unwrap());
        let dotdot_fd = h.fs.lookup(Some(&dstdir_fdh), &fdh, dotdotname).await
            .unwrap();
        assert_eq!(dotdot_fd.ino(), dstdir_fd.ino());
        assert_eq!(dotdot_fd.parent(), Some(root.ino()));

        h.fs.inactive(fd).await;

        // Check that the moved directory's parent is correct on disk
        let fd = h.fs.ilookup(src_ino).await.unwrap();
        assert_eq!(fd.parent(), Some(dstdir_fd.ino()));
        h.fs.inactive(fd).await;

        // Check that the moved directory is visible by its new, not old, name
        let dst_fd = h.fs.lookup(Some(&rooth), &dstdir_fdh, &dst).await.unwrap();
        assert_eq!(dst_fd.ino(), src_ino);
        let r = h.fs.lookup(Some(&rooth), &srcdir_fdh, &src).await;
        assert_eq!(r.unwrap_err(), libc::ENOENT);

        // Check parents' link counts
        let srcdir_attr = h.fs.getattr(&srcdir_fdh).await.unwrap();
        assert_eq!(srcdir_attr.nlink, 2);
        let dstdir_attr = h.fs.getattr(&dstdir_fdh).await.unwrap();
        assert_eq!(dstdir_attr.nlink, 3);
    }

    // Attempting to rename "." should return EINVAL
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_dot(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let dot = OsStr::from_bytes(b".");
        let srcdir = OsStr::from_bytes(b"srcdir");
        let dst = OsStr::from_bytes(b"dst");
        let srcdir_fd = h.fs.mkdir(&rooth, srcdir, 0o755, 0, 0).await
        .unwrap();
        let srcdir_fdh = srcdir_fd.handle();

        let r = h.fs.rename(&srcdir_fdh, &srcdir_fdh, dot, &srcdir_fdh,
                None, dst).await;
        assert_eq!(Err(libc::EINVAL), r);
    }

    // Attempting to rename ".." should return EINVAL
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_dotdot(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let dotdot = OsStr::from_bytes(b"..");
        let srcdir = OsStr::from_bytes(b"srcdir");
        let dst = OsStr::from_bytes(b"dst");
        let srcdir_fd = h.fs.mkdir(&rooth, srcdir, 0o755, 0, 0).await
        .unwrap();
        let srcdir_fdh = srcdir_fd.handle();

        let r = h.fs.rename(&srcdir_fdh, &rooth, dotdot, &srcdir_fdh,
                None, dst).await;
        assert_eq!(Err(libc::EINVAL), r);
    }

    // Rename a non-directory to a multiply-linked file.  The destination
    // directory entry should be removed, but not the inode.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_nondir_to_hardlink(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let lnk = OsString::from("lnk");
        let src_fd = h.fs.create(&rooth, &src, 0o644, 0, 0).await.unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = h.fs.create(&rooth, &dst, 0o644, 0, 0).await.unwrap();
        let dst_ino = dst_fd.ino();
        h.fs.link(&rooth, &dst_fd.handle(), &lnk).await.unwrap();
        clear_timestamps(&h.fs, &dst_fd.handle()).await;

        assert_eq!(src_fd.ino(),
            h.fs.rename(&rooth, &src_fd.handle(), &src, &rooth, Some(dst_fd.ino()),
                &dst).await
            .unwrap()
        );

        h.fs.inactive(src_fd).await;
        assert_eq!(h.fs.lookup(None, &rooth, &dst).await.unwrap().ino(),
            src_ino);
        assert_eq!(h.fs.lookup(None, &rooth, &src).await.unwrap_err(),
            libc::ENOENT);
        let lnk_fd = h.fs.lookup(None, &rooth, &lnk).await.unwrap();
        assert_eq!(lnk_fd.ino(), dst_ino);
        let lnk_fdh = lnk_fd.handle();
        let lnk_attr = h.fs.getattr(&lnk_fdh).await.unwrap();
        assert_eq!(lnk_attr.nlink, 1);
        assert_ts_changed(&h.fs, &lnk_fdh, false, false, true, false).await;
    }

    // Rename a non-directory.  The target is also a non-directory
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_nondir_to_nondir(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = h.fs.mkdir(&rooth, &srcdir, 0o755, 0, 0).await.unwrap();
        let dstdir_fd = h.fs.mkdir(&rooth, &dstdir, 0o755, 0, 0).await.unwrap();
        let srcdir_fdh = srcdir_fd.handle();
        let dstdir_fdh = dstdir_fd.handle();
        let src_fd = h.fs.create(&srcdir_fdh, &src, 0o644, 0, 0).await.unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = h.fs.create(&dstdir_fdh, &dst, 0o644, 0, 0).await.unwrap();
        let dst_ino = dst_fd.ino();

        assert_eq!(src_fd.ino(),
            h.fs.rename(&srcdir_fdh, &src_fd.handle(), &src, &dstdir_fdh,
                Some(dst_ino), &dst).await.unwrap()
        );

        h.fs.inactive(src_fd).await;
        let dst_fd1 = h.fs.lookup(Some(&rooth), &dstdir_fdh, &dst).await;
        assert_eq!(dst_fd1.unwrap().ino(), src_ino);
        let r = h.fs.lookup(Some(&rooth), &srcdir_fdh, &src).await;
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let srcdir_inode = h.fs.getattr(&srcdir_fdh).await.unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = h.fs.getattr(&dstdir_fdh).await.unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(dst_ino).await, Err(libc::ENOENT));
        }
    }

    // Rename a non-directory.  The target name does not exist
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_nondir_to_nothing(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = h.fs.mkdir(&rooth, &srcdir, 0o755, 0, 0).await.unwrap();
        let dstdir_fd = h.fs.mkdir(&rooth, &dstdir, 0o755, 0, 0).await.unwrap();
        let srcdir_fdh = srcdir_fd.handle();
        let dstdir_fdh = dstdir_fd.handle();
        let fd = h.fs.create(&srcdir_fdh, &src, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let src_ino = fd.ino();

        assert_eq!(fd.ino(),
            h.fs.rename(&srcdir_fdh, &fdh, &src, &dstdir_fdh, None, &dst).await
            .unwrap()
        );

        h.fs.inactive(fd).await;
        assert_eq!(src_ino,
            h.fs.lookup(Some(&rooth), &dstdir_fdh, &dst).await.unwrap().ino()
        );
        assert_eq!(libc::ENOENT,
            h.fs.lookup(Some(&rooth), &srcdir_fdh, &src).await.unwrap_err()
        );
        let srcdir_inode = h.fs.getattr(&srcdir_fdh).await.unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = h.fs.getattr(&dstdir_fdh).await.unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
    }

    // Rename a regular file to a symlink.  Make sure the target is a regular
    // file afterwards
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_reg_to_symlink(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let linktarget = OsString::from("nonexistent");
        let srcdir_fd = h.fs.mkdir(&rooth, &srcdir, 0o755, 0, 0).await.unwrap();
        let dstdir_fd = h.fs.mkdir(&rooth, &dstdir, 0o755, 0, 0).await.unwrap();
        let srcdir_fdh = srcdir_fd.handle();
        let dstdir_fdh = dstdir_fd.handle();
        let src_fd = h.fs.create(&srcdir_fdh, &src, 0o644, 0, 0).await.unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = h.fs.symlink(&dstdir_fdh, &dst, 0o642, 0, 0, &linktarget)
            .await
            .unwrap();
        #[cfg(debug_assertions)] let dst_ino = dst_fd.ino();

        assert_eq!(src_fd.ino(),
            h.fs.rename(&srcdir_fdh, &src_fd.handle(), &src, &dstdir_fdh,
                Some(dst_fd.ino()), &dst).await.unwrap()
        );

        h.fs.inactive(src_fd).await;
        assert_eq!(src_ino,
            h.fs.lookup(Some(&rooth), &dstdir_fdh, &dst).await.unwrap().ino()
        );
        assert_eq!(libc::ENOENT,
            h.fs.lookup(Some(&rooth), &srcdir_fdh, &src).await.unwrap_err()
        );
        let srcdir_inode = h.fs.getattr(&srcdir_fdh).await.unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = h.fs.getattr(&dstdir_fdh).await.unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
        let entries = readdir_all(&h.fs, &dstdir_fdh, 0).await;
        let de = entries
            .into_iter()
            .find(|dirent| dirent.d_fileno == src_ino )
            .unwrap();
        assert_eq!(de.d_type, libc::DT_REG);
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(dst_ino).await, Err(libc::ENOENT));
        }
    }

    // Rename a file with extended attributes.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_reg_with_extattrs(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;

        let fd = h.fs.create(&rooth, &src, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &name, &value[..]).await.unwrap();

        assert_eq!(fd.ino(),
            h.fs.rename(&rooth, &fdh, &src, &rooth, None, &dst).await
            .unwrap()
        );

        h.fs.inactive(fd).await;
        let new_fd = h.fs.lookup(None, &rooth, &dst).await.unwrap();
        let v = h.fs.getextattr(&new_fd.handle(), ns, &name).await.unwrap();
        assert_eq!(&v[..], &value);
    }

    // rename updates a file's parent directories' ctime and mtime
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rename_parent_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = h.fs.mkdir(&rooth, &srcdir, 0o755, 0, 0).await
        .unwrap();
        let dstdir_fd = h.fs.mkdir(&rooth, &dstdir, 0o755, 0, 0).await
        .unwrap();
        let srcdir_fdh = srcdir_fd.handle();
        let dstdir_fdh = dstdir_fd.handle();
        let fd = h.fs.create(&srcdir_fdh, &src, 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        clear_timestamps(&h.fs, &srcdir_fdh).await;
        clear_timestamps(&h.fs, &dstdir_fdh).await;
        clear_timestamps(&h.fs, &fdh).await;

        assert_eq!(fd.ino(),
            h.fs.rename(&srcdir_fdh, &fdh, &src, &dstdir_fdh, None, &dst).await
            .unwrap()
        );

        // Timestamps should've been updated for parent directories, but not for
        // the file itself
        assert_ts_changed(&h.fs, &srcdir_fdh, false, true, true, false).await;
        assert_ts_changed(&h.fs, &dstdir_fdh, false, true, true, false).await;
        assert_ts_changed(&h.fs, &fdh, false, false, false, false).await;
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rmdir(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let dirname = OsString::from("x");
        let fd = h.fs.mkdir(&rooth, &dirname, 0o755, 0, 0).await.unwrap();
        let fdh = fd.handle();
        #[cfg(debug_assertions)] let ino = fd.ino();
        h.fs.rmdir(&rooth, &dirname).await.unwrap();

        // Make sure it's gone
        assert_eq!(h.fs.getattr(&fdh).await.unwrap_err(), libc::ENOENT);
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(ino).await, Err(libc::ENOENT));
        }
        assert!(!readdir_all(&h.fs, &rooth, 0).await
            .into_iter()
            .any(|dirent| dirent.d_name[0] == 'x' as i8));

        // Make sure the parent dir's refcount dropped
        let inode = h.fs.getattr(&rooth).await.unwrap();
        assert_eq!(inode.nlink, 1);
    }

    /// Remove a directory whose name has a hash collision
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rmdir_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = h.fs.mkdir(&rooth, &filename0, 0o755, 0, 0).await.unwrap();
        let _fd1 = h.fs.mkdir(&rooth, &filename1, 0o755, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let ino1 = _fd1.ino();
        h.fs.rmdir(&rooth, &filename1).await.unwrap();

        assert_eq!(h.fs.lookup(None, &rooth, &filename0).await.unwrap().ino(),
            fd0.ino());
        assert_eq!(h.fs.lookup(None, &rooth, &filename1).await.unwrap_err(),
            libc::ENOENT);
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(ino1).await, Err(libc::ENOENT));
        }
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rmdir_enoent(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let dirname = OsString::from("x");
        assert_eq!(h.fs.rmdir(&rooth, &dirname).await.unwrap_err(),
            libc::ENOENT);
    }

    #[should_panic]
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rmdir_enotdir(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        h.fs.create(&rooth, &filename, 0o644, 0, 0).await
            .unwrap();
        h.fs.rmdir(&rooth, &filename).await.unwrap();
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rmdir_enotempty(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let dirname = OsString::from("x");
        let fd = h.fs.mkdir(&rooth, &dirname, 0o755, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        h.fs.mkdir(&fdh, &dirname, 0o755, 0, 0).await
        .unwrap();
        assert_eq!(h.fs.rmdir(&rooth, &dirname).await.unwrap_err(),
            libc::ENOTEMPTY);
    }

    /// Try to remove a directory that isn't empty, and that has a hash
    /// collision with another file or directory
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rmdir_enotempty_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename0 = OsString::from("basedir");
        let filename1 = OsString::from("HsxUh682JQ");
        let filename2 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename1, &filename2);
        let fd0 = h.fs.mkdir(&rooth, &filename0, 0o755, 0, 0).await.unwrap();
        let _fd1 = h.fs.mkdir(&fd0.handle(), &filename1, 0o755, 0, 0).await.unwrap();
        let _fd2 = h.fs.mkdir(&fd0.handle(), &filename2, 0o755, 0, 0).await.unwrap();
        assert_eq!(h.fs.rmdir(&rooth, &filename0).await.unwrap_err(),
         libc::ENOTEMPTY);
    }

    /// Remove a directory with an extended attribute
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rmdir_extattr(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let dirname = OsString::from("x");
        let xname = OsString::from("foo");
        let xvalue1 = [0u8, 1, 2];
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.mkdir(&rooth, &dirname, 0o755, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &xname, &xvalue1[..]).await.unwrap();
        h.fs.rmdir(&rooth, &dirname).await.unwrap();

        // Make sure the xattr is gone.  As I read things, POSIX allows us to
        // return either ENOATTR or ENOENT in this case.
        assert_eq!(h.fs.getextattr(&fdh, ns, &xname).await.unwrap_err(),
                   libc::ENOATTR);
    }

    /// Remove a directory with a blob extended attribute
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rmdir_blob_extattr(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let dirname = OsString::from("x");
        let xname = OsString::from("foo");
        let xvalue = vec![42u8; 4096];
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.mkdir(&rooth, &dirname, 0o755, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &xname, &xvalue[..]).await.unwrap();
        h.fs.sync().await;
        h.fs.rmdir(&rooth, &dirname).await.unwrap();

        // Make sure the xattr is gone.  As I read things, POSIX allows us to
        // return either ENOATTR or ENOENT in this case.
        assert_eq!(h.fs.getextattr(&fdh, ns, &xname).await.unwrap_err(),
                   libc::ENOATTR);
    }

    /// Removing a directory should update its parent's timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn rmdir_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let dirname = OsString::from("x");
        h.fs.mkdir(&rooth, &dirname, 0o755, 0, 0).await.unwrap();
        clear_timestamps(&h.fs, &rooth).await;

        h.fs.rmdir(&rooth, &dirname).await.unwrap();

        // Timestamps should've been updated
        assert_ts_changed(&h.fs, &rooth, false, true, true, false).await;
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn set_prop(#[future] h: Harness) {
        h.fs.set_prop(Property::Atime(false)).await.unwrap();

        // Read the property back
        let (val, source) = h.fs.get_prop(PropertyName::Atime)
            .await
            .unwrap();
        assert_eq!(val, Property::Atime(false));
        assert_eq!(source, PropertySource::LOCAL);

        // Check that atime is truly disabled
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
            .unwrap();
        let fdh = fd.handle();
        let buf = vec![42u8; 4096];
        h.fs.write(&fdh, 0, &buf[..], 0).await.unwrap();
        clear_timestamps(&h.fs, &fdh).await;

        h.fs.read(&fdh, 0, 4096).await.unwrap();
        assert_ts_changed(&h.fs, &fdh, false, false, false, false).await;
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setattr(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let perm = 0o1357;
        let uid = 12345;
        let gid = 54321;
        let size = 9999;
        let atime = Timespec {sec: 1, nsec: 2};
        let mtime = Timespec {sec: 3, nsec: 4};
        let ctime = Timespec {sec: 5, nsec: 6};
        let birthtime = Timespec {sec: 7, nsec: 8};
        #[allow(clippy::useless_conversion)]    // Not useless on i686
        let flags = u64::from(libc::UF_NODUMP);

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
        h.fs.setattr(&fdh, attr).await.unwrap();
        let attr = h.fs.getattr(&fdh).await.unwrap();
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
        h.fs.setattr(&fdh, attr).await.unwrap();
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert(attr);
    }

    // setattr updates a file's ctime and mtime
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setattr_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        clear_timestamps(&h.fs, &fdh).await;

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
        h.fs.setattr(&fdh, attr).await.unwrap();

        // Timestamps should've been updated
        assert_ts_changed(&h.fs, &fdh, false, false, true, false).await;
    }

    // truncating a file should delete data past the truncation point
    #[rstest]
    #[case(harness4k(), false)]
    #[case(harness4k(), true)]
    #[tokio::test]
    #[awt]
    async fn setattr_truncate(#[future] #[case] h: Harness, #[case] blobs: bool)
    {
        let root = h.fs.root();
        let rooth = root.handle();
        // First write two records
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let buf = vec![42u8; 8192];
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);

        // Then truncate one of them.
        let mut attr = SetAttr {
            size: Some(4096),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();

        if blobs {
            h.fs.sync().await;        // Flush it to a BlobExtent
        }

        // Now extend the file past the truncated record
        attr.size = Some(8192);
        h.fs.setattr(&fdh, attr).await.unwrap();

        // Finally, read the truncated record.  It should be a hole
        let sglist = h.fs.read(&fdh, 4096, 4096).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);

        // blocks used should only include the non-truncated records
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(4096, attr.bytes);
    }

    // Like setattr_truncate, but everything happens within a single record
    #[rstest]
    #[case(harness4k(), false)]
    #[case(harness4k(), true)]
    #[tokio::test]
    #[awt]
    async fn setattr_truncate_partial_record(
        #[future] #[case] h: Harness,
        #[case] blobs: bool)
    {
        let root = h.fs.root();
        let rooth = root.handle();
        // First write one record
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let buf = vec![42u8; 4096];
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        // Then truncate it.
        let mut attr = SetAttr {
            size: Some(1000),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();

        if blobs {
            h.fs.sync().await;        // Flush it to a BlobExtent
        }

        // Now extend the file past the truncated record
        attr.size = Some(4000);
        h.fs.setattr(&fdh, attr).await.unwrap();

        // Finally, read from the truncated area.  It should be a hole
        let sglist = h.fs.read(&fdh, 2000, 1000).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 1000];
        assert_eq!(&db[..], &expected[..]);
    }

    // Like setattr_truncate, but there is a hole at the end of the file
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setattr_truncate_partial_hole(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        // First create a sparse file
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut attr = SetAttr {
            size: Some(8192),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();

        // Then truncate the file partway down
        attr.size = Some(6144);
        h.fs.setattr(&fdh, attr).await.unwrap();

        // Now extend the file past the truncated record
        attr.size = Some(8192);
        h.fs.setattr(&fdh, attr).await.unwrap();

        // Finally, read the truncated record.  It should be a hole
        let sglist = h.fs.read(&fdh, 4096, 4096).await.unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);
    }

    // truncating a file should update the mtime
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setattr_truncate_updates_mtime(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        // Create a file
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        clear_timestamps(&h.fs, &fdh).await;

        // Then truncate the file
        let attr = SetAttr {
            size: Some(4096),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();

        // mtime should've changed
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;
    }

    /// Truncate a record that is already sparse.  The truncation begins past
    /// the end of the record's data.
    // Found by "fsx -WR -S12"
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setattr_truncate_sparse_record(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        // Create a file
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let wbuf = vec![42u8; 2048];
        h.fs.write(&fdh, 0, &wbuf[..], 0).await.unwrap();
        // Extend the file past the end of the first record
        let attr = SetAttr { size: Some(8192), .. Default::default() };
        h.fs.setattr(&fdh, attr).await.unwrap();
        // Now truncate the file beginning in the first record, but past the end
        // of the data
        let attr = SetAttr { size: Some(3072), .. Default::default() };
        h.fs.setattr(&fdh, attr).await.unwrap();
    }

    /// Set an blob extended attribute
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setextattr_blob(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = vec![42u8; 4096];
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &name, &value[..]).await.unwrap();
        h.fs.sync().await;
        let v = h.fs.getextattr(&fdh, ns, &name).await.unwrap();
        assert_eq!(&v[..], &value);
        // extended attributes should not contribute to stat.st_blocks
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.bytes, 0);
    }

    /// Set an inline extended attribute.  Short attributes will never be
    /// flushed to blobs.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setextattr_inline(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [0u8, 1, 2];
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &name, &value[..]).await.unwrap();
        let v = h.fs.getextattr(&fdh, ns, &name).await.unwrap();
        assert_eq!(&v[..], &value);
        // extended attributes should not contribute to stat.st_blocks
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.bytes, 0);
    }

    /// Overwrite an existing extended attribute
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setextattr_overwrite(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value1 = [0u8, 1, 2];
        let value2 = [3u8, 4, 5, 6];
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &name, &value1[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns, &name, &value2[..]).await.unwrap();
        let v = h.fs.getextattr(&fdh, ns, &name).await.unwrap();
        assert_eq!(&v[..], &value2);
    }

    /// Overwrite an existing extended attribute that hash-collided with a
    /// different xattr
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setextattr_collision_overwrite(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];
        let value1a = [4u8, 7, 8, 9, 10];

        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns0, &name0, &value0[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns1, &name1, &value1[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns1, &name1, &value1a[..]).await.unwrap();
        let v0 = h.fs.getextattr(&fdh, ns0, &name0).await.unwrap();
        assert_eq!(&v0[..], &value0);
        let v1 = h.fs.getextattr(&fdh, ns1, &name1).await.unwrap();
        assert_eq!(&v1[..], &value1a);
    }

    /// setextattr(2) should not update any timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setextattr_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [0u8, 1, 2];
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        clear_timestamps(&h.fs, &fdh).await;

        h.fs.setextattr(&fdh, ns, &name, &value[..]).await.unwrap();
        assert_ts_changed(&h.fs, &fdh, false, false, false, false).await;
    }

    /// The file already has a blob extattr.  Set another extattr and flush them
    /// both.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setextattr_with_blob(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let value1 = vec![42u8; 4096];
        let name2 = OsString::from("bar");
        let value2 = [3u8, 4, 5, 6];
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &name1, &value1[..]).await.unwrap();
        h.fs.sync().await; // Create a blob ExtAttr

        h.fs.setextattr(&fdh, ns, &name2, &value2[..]).await.unwrap();
        h.fs.sync().await; // Achieve coverage of BlobExtAttr::flush

        // Both attributes should be present
        let v1 = h.fs.getextattr(&fdh, ns, &name1).await.unwrap();
        assert_eq!(&v1[..], &value1[..]);
        let v2 = h.fs.getextattr(&fdh, ns, &name2).await.unwrap();
        assert_eq!(&v2[..], &value2[..]);
    }

    /// The file already has a blob extattr.  Overwrite it with a new one.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn setextattr_overwrite_blob(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value1 = vec![42u8; 65536];
        let value2 = [43u8; 65536];
        let ns = ExtAttrNamespace::User;
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &name, &value1[..]).await.unwrap();
        h.fs.sync().await; // Create a blob ExtAttr
        let stat1 = h.fs.statvfs().await.unwrap();

        h.fs.setextattr(&fdh, ns, &name, &value2[..]).await.unwrap();
        h.fs.sync().await; // Flush the new blob ExtAttr

        // Only the new attribute should be present
        let v = h.fs.getextattr(&fdh, ns, &name).await.unwrap();
        assert_eq!(&v[..], &value2[..]);

        // The overall amount of storage used should change but little
        h.fs.sync().await;
        let stat2 = h.fs.statvfs().await.unwrap();
        assert_eq!(stat1.f_bfree, stat2.f_bfree);
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn statvfs(#[future] h: Harness) {
        let statvfs = h.fs.statvfs().await.unwrap();
        assert_eq!(statvfs.f_blocks, 262_144);
        assert_eq!(statvfs.f_bsize, 4096);
        assert_eq!(statvfs.f_frsize, 4096);
    }

    #[rstest(h, case(harness8k()))]
    #[tokio::test]
    #[awt]
    async fn statvfs_8k(#[future] h: Harness) {
        let statvfs = h.fs.statvfs().await.unwrap();
        assert_eq!(statvfs.f_blocks, 262_144);
        assert_eq!(statvfs.f_bsize, 8192);
        assert_eq!(statvfs.f_frsize, 4096);
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn symlink(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let uid = 12345;
        let gid = 54321;
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let fd = h.fs.symlink(&rooth, &srcname, 0o642, uid, gid, &dstname).await
        .unwrap();
        let fdh = fd.handle();
        assert_eq!(fd.ino(),
            h.fs.lookup(None, &rooth, &srcname).await.unwrap().ino()
        );

        // The parent dir should have an "src" symlink entry
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        let dirent = entries
        .into_iter()
        .find(|dirent| {
            dirent.d_name[0] == 's' as i8
        }).expect("'s' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_LNK);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name.to_str().unwrap(), srcname.to_str().unwrap());
        assert_eq!(dirent.d_fileno, fd.ino());

        let attr = h.fs.getattr(&fdh).await.unwrap();
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
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn symlink_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        clear_timestamps(&h.fs, &rooth).await;

        h.fs.symlink(&rooth, &srcname, 0o642, 0, 0, &dstname).await.unwrap();
        assert_ts_changed(&h.fs, &rooth, false, true, true, false).await;
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        #[cfg(debug_assertions)] let ino = fd.ino();
        let r = h.fs.unlink(&rooth, Some(&fdh), &filename).await;
        assert_eq!(Ok(()), r);
        h.fs.inactive(fd).await;

        // Check that the directory entry is gone
        let r = h.fs.lookup(None, &rooth, &filename).await;
        assert_eq!(libc::ENOENT, r.unwrap_err(), "Dirent was not removed");
        // Check that the inode is gone
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(ino).await, Err(libc::ENOENT));
        }

        // The parent dir should not have an "x" directory entry
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        let x_de = entries
        .into_iter()
        .find(|dirent| {
            dirent.d_name[0] == 'x' as i8
        });
        assert!(x_de.is_none(), "Directory entry was not removed");
    }

    // Access an opened but deleted file
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink_but_opened(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let r = h.fs.unlink(&rooth, Some(&fdh), &filename).await;
        assert_eq!(Ok(()), r);

        let attr = h.fs.getattr(&fdh).await.expect("Inode deleted too soon");
        assert_eq!(0, attr.nlink);

        h.fs.inactive(fd).await;
    }

    // Access an open file that was deleted during a previous TXG
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink_but_opened_across_txg(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        let r = h.fs.unlink(&rooth, Some(&fdh), &filename).await;
        assert_eq!(Ok(()), r);

        h.fs.sync().await;

        let attr = h.fs.getattr(&fdh).await.expect("Inode deleted too soon");
        assert_eq!(0, attr.nlink);

        h.fs.inactive(fd).await;
    }

    // Unlink a file that has a name collision with another file in the same
    // directory.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = h.fs.create(&rooth, &filename0, 0o644, 0, 0).await.unwrap();
        let fd1 = h.fs.create(&rooth, &filename1, 0o644, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let ino1 = fd1.ino();

        h.fs.unlink(&rooth, Some(&fd1.handle()), &filename1).await.unwrap();
        h.fs.inactive(fd1).await;

        assert_eq!(h.fs.lookup(None, &rooth, &filename0).await.unwrap().ino(),
            fd0.ino());
        assert_eq!(h.fs.lookup(None, &rooth, &filename1).await.unwrap_err(),
            libc::ENOENT);
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(ino1).await, Err(libc::ENOENT));
        }
    }

    // When unlinking a multiply linked file, its ctime should be updated
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink_ctime(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let name1 = OsString::from("name1");
        let name2 = OsString::from("name2");
        let fd = h.fs.create(&rooth, &name1, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.link(&rooth, &fdh, &name2).await.unwrap();
        clear_timestamps(&h.fs, &fdh).await;

        h.fs.unlink(&rooth, Some(&fdh), &name2).await.unwrap();
        assert_ts_changed(&h.fs, &fdh, false, false, true, false).await;
    }

    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink_enoent(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.unlink(&rooth, Some(&fdh), &filename).await.unwrap();
        let e = h.fs.unlink(&rooth, Some(&fdh), &filename).await.unwrap_err();
        assert_eq!(e, libc::ENOENT);
    }

    // When unlinking a hardlink, the file should not be removed until its link
    // count reaches zero.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink_hardlink(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let name1 = OsString::from("name1");
        let name2 = OsString::from("name2");
        let fd = h.fs.create(&rooth, &name1, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        #[cfg(debug_assertions)] let ino = fd.ino();
        h.fs.link(&rooth, &fdh, &name2).await.unwrap();

        h.fs.unlink(&rooth, Some(&fdh), &name1).await.unwrap();
        // File should still exist, now with link count 1.
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.nlink, 1);
        assert_eq!(h.fs.lookup(None, &rooth, &name1).await.unwrap_err(),
            libc::ENOENT);

        // Even if we drop the file data, the inode should not be deleted,
        // because it has nlink 1
        h.fs.inactive(fd).await;
        let fd = h.fs.lookup(None, &rooth, &name2).await.unwrap();
        let fdh = fd.handle();
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.nlink, 1);

        // A second unlink should remove the file
        h.fs.unlink(&rooth, Some(&fdh), &name2).await.unwrap();
        h.fs.inactive(fd).await;

        // File should actually be gone now
        assert_eq!(h.fs.lookup(None, &rooth, &name1).await.unwrap_err(),
            libc::ENOENT);
        assert_eq!(h.fs.lookup(None, &rooth, &name2).await.unwrap_err(),
            libc::ENOENT);
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(ino).await, Err(libc::ENOENT));
        }
    }

    // Unlink should work on inactive vnodes
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink_inactive(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        #[cfg(debug_assertions)] let ino = fd.ino();
        h.fs.inactive(fd).await;
        let r = h.fs.unlink(&rooth, None, &filename).await;
        assert_eq!(Ok(()), r);

        // Check that the directory entry is gone
        let r = h.fs.lookup(None, &rooth, &filename).await;
        assert_eq!(libc::ENOENT, r.expect_err("Inode was not removed"));
        // Check that the inode is gone
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(ino).await, Err(libc::ENOENT));
        }

        // The parent dir should not have an "x" directory entry
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        let x_de = entries
        .into_iter()
        .find(|dirent| {
            dirent.d_name[0] == 'x' as i8
        });
        assert!(x_de.is_none(), "Directory entry was not removed");
    }

    /// unlink(2) should update the parent dir's timestamps
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        clear_timestamps(&h.fs, &rooth).await;

        h.fs.unlink(&rooth, Some(&fdh), &filename).await.unwrap();
        h.fs.inactive(fd).await;
        assert_ts_changed(&h.fs, &rooth, false, true, true, false).await;
    }

    /// Unlink a file with blobs on disk
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink_with_blobs(#[future] h: Harness) {
        // Must write a lot of data to ensure the Tree is of depth 2
        const NBLOCKS: usize = 1024;
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        #[cfg(debug_assertions)] let ino = fd.ino();
        let buf = vec![42u8; 4096];
        for i in 0..NBLOCKS {
            assert_eq!(Ok(4096), h.fs.write(&fdh, 4096 * i as u64, &buf[..], 0)
                       .await);
        }

        h.fs.sync().await;
        let stat1 = h.fs.statvfs().await.unwrap();

        let r = h.fs.unlink(&rooth, Some(&fdh), &filename).await;
        assert_eq!(Ok(()), r);
        h.fs.inactive(fd).await;

        // Check that the directory entry is gone
        let r = h.fs.lookup(None, &rooth, &filename).await;
        assert_eq!(libc::ENOENT, r.unwrap_err(), "Dirent was not removed");
        // Check that the inode is gone
        #[cfg(debug_assertions)]
        {
            assert_eq!(h.fs.igetattr(ino).await, Err(libc::ENOENT));
        }

        // The parent dir should not have an "x" directory entry
        let entries = readdir_all(&h.fs, &rooth, 0).await;
        let x_de = entries
        .into_iter()
        .find(|dirent| {
            dirent.d_name[0] == 'x' as i8
        });
        assert!(x_de.is_none(), "Directory entry was not removed");

        // The blobs' storage should have been freed
        let stat2 = h.fs.statvfs().await.unwrap();
        let freed_bytes = ((stat2.f_bfree - stat1.f_bfree) * 4096) as usize;
        let expected = NBLOCKS * buf.len();
        // <= because space for Tree nodes got deleted too
        assert!(expected <= freed_bytes);
    }

    /// Unlink a file with blob extended attributes on disk
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink_with_blob_extattr(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let ns = ExtAttrNamespace::User;
        let xname = OsString::from("foo");
        let xvalue = vec![42u8; 4096];
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();
        h.fs.setextattr(&fdh, ns, &xname, &xvalue[..]).await.unwrap();

        h.fs.sync().await;

        let r = h.fs.unlink(&rooth, Some(&fdh), &filename).await;
        assert_eq!(Ok(()), r);
        h.fs.inactive(fd).await;

        // Make sure the xattr is gone.  As I read things, POSIX allows us to
        // return either ENOATTR or ENOENT in this case.
        assert_eq!(h.fs.getextattr(&fdh, ns, &xname).await.unwrap_err(),
                   libc::ENOATTR);
    }

    /// Unlink a file with multiple hash-colliding blob extended attributess on
    /// disk.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn unlink_with_blob_extattr_collision(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [1u8; 4096];
        let value1 = [2u8; 4096];
        let fd = h.fs.create(&rooth, &filename, 0o644, 0, 0).await.unwrap();
        let fdh = fd.handle();

        h.fs.setextattr(&fdh, ns0, &name0, &value0[..]).await.unwrap();
        h.fs.setextattr(&fdh, ns1, &name1, &value1[..]).await.unwrap();
        h.fs.sync().await;

        let r = h.fs.unlink(&rooth, Some(&fdh), &filename).await;
        assert_eq!(Ok(()), r);
        h.fs.inactive(fd).await;

        // Make sure the xattrs are gone.  As I read things, POSIX allows us to
        // return either ENOATTR or ENOENT in this case.
        assert_eq!(h.fs.getextattr(&fdh, ns0, &name0).await.unwrap_err(),
                   libc::ENOATTR);
        assert_eq!(h.fs.getextattr(&fdh, ns1, &name1).await.unwrap_err(),
                   libc::ENOATTR);
    }

    // A very simple single record write to an empty file
    #[rstest]
    #[case(harness4k(), false)]
    #[case(harness4k(), true)]
    #[tokio::test]
    #[awt]
    async fn write(#[future] #[case] h: Harness, #[case] blobs: bool) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let buf = vec![42u8; 4096];
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);
        if blobs {
            h.fs.sync().await;        // Flush it to a BlobExtent
        }

        // Check the file attributes
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.size, 4096);
        assert_eq!(attr.bytes, 4096);

        let sglist = h.fs.read(&fdh, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[..]);
    }

    // Overwrite a single record in an otherwise empty file
    #[rstest]
    #[case(harness(vec![Property::RecordSize(12), Property::Atime(false)]), false)]
    #[case(harness(vec![Property::RecordSize(12), Property::Atime(false)]), true)]
    #[tokio::test]
    #[awt]
    async fn write_overwrite(#[future] #[case] h: Harness, #[case] blobs: bool)
    {
        const BSIZE: usize = 4096;
        // Need atime off to fs.read() doesn't dirty the tree and change the
        // f_bfree value.
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let buf1 = vec![42u8; BSIZE];
        let buf2 = vec![43u8; BSIZE];

        let r = h.fs.write(&fdh, 0, &buf1[..], 0).await;
        assert_eq!(Ok(BSIZE as u32), r);
        if blobs {
            h.fs.sync().await;        // Flush it to a BlobExtent
        }
        let stat1 = h.fs.statvfs().await.unwrap();

        let r = h.fs.write(&fdh, 0, &buf2[..], 0).await;
        assert_eq!(Ok(BSIZE as u32), r);
        if blobs {
            h.fs.sync().await;        // Flush it to a BlobExtent
        }

        // Check the file attributes
        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.size, BSIZE as u64);
        assert_eq!(attr.bytes, BSIZE as u64);

        let sglist = h.fs.read(&fdh, 0, BSIZE).await.unwrap();
        assert_eq!(&sglist[0][..], &buf2[..]);

        // The overall amount of storage used should be unchanged
        let stat2 = h.fs.statvfs().await.unwrap();
        assert_eq!(stat1.f_bfree, stat2.f_bfree);
    }


    // A partial single record write appended to the file's end
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn write_append(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf0 = vec![0u8; 1024];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf0[..], 0).await;
        assert_eq!(Ok(1024), r);

        let sglist = h.fs.read(&fdh, 0, 1024).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf0[..]);
    }

    // A partial record write appended to a partial record at file's end
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn write_append_to_partial_record(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
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
        let r = h.fs.write(&fdh, 0, &buf0[..], 0).await;
        assert_eq!(Ok(1024), r);
        let r = h.fs.write(&fdh, 1024, &buf1[..], 0).await;
        assert_eq!(Ok(1024), r);

        let attr = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(attr.size, 2048);
        assert_eq!(attr.bytes, 2048);

        let sglist = h.fs.read(&fdh, 0, 2048).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[0..1024], &buf0[..]);
        assert_eq!(&db[1024..2048], &buf1[..]);
    }

    // Partially fill a hole that's at neither the beginning nor the end of the
    // file
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn write_partial_hole(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let attr = SetAttr {
            size: Some(4096 * 4),
            .. Default::default()
        };
        h.fs.setattr(&fdh, attr).await.unwrap();

        let mut buf0 = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 9216, &buf0[..], 0).await;
        assert_eq!(Ok(2048), r);

        let sglist = h.fs.read(&fdh, 9216, 2048).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf0[..]);
    }

    // A partial single record write that needs RMW on both ends
    #[rstest]
    #[case(harness4k(), false)]
    #[case(harness4k(), true)]
    #[tokio::test]
    #[awt]
    async fn write_partial_record(
        #[future] #[case] h: Harness,
        #[case] blobs: bool)
    {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf0 = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf0[..], 0).await;
        assert_eq!(Ok(4096), r);

        if blobs {
            h.fs.sync().await;        // Flush it to a BlobExtent
        }

        let buf1 = vec![0u8; 2048];
        let r = h.fs.write(&fdh, 512, &buf1[..], 0).await;
        assert_eq!(Ok(2048), r);

        let sglist = h.fs.read(&fdh, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[0..512], &buf0[0..512]);
        assert_eq!(&db[512..2560], &buf1[..]);
        assert_eq!(&db[2560..], &buf0[2560..]);
    }

    // write updates a file's ctime and mtime
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn write_timestamps(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        clear_timestamps(&h.fs, &fdh).await;

        let buf = vec![42u8; 4096];
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(4096), r);

        // Timestamps should've been updated
        assert_ts_changed(&h.fs, &fdh, false, true, true, false).await;
    }

    // A write to an empty file that's split across two records
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn write_two_recs(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(8192), r);

        // Check the file size
        let inode = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(inode.size, 8192);

        let sglist = h.fs.read(&fdh, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[0..4096]);
        let sglist = h.fs.read(&fdh, 4096, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[4096..8192]);
    }

    // A write to an empty file that's split across three records
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn write_three_recs(#[future] h: Harness) {
        let root = h.fs.root();
        let rooth = root.handle();
        let fd = h.fs.create(&rooth, &OsString::from("x"), 0o644, 0, 0).await
        .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 12288];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(12288), r);

        // Check the file size
        let inode = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(inode.size, 12288);

        let sglist = h.fs.read(&fdh, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[0..4096]);
        let sglist = h.fs.read(&fdh, 4096, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[4096..8192]);
        let sglist = h.fs.read(&fdh, 8192, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[8192..12288]);
    }

    // Write one hold record and a partial one to an initially empty file.
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn write_one_and_a_half_records(#[future] h: Harness) {
        let root = h.fs.root();
        let fd = h.fs.create(&root.handle(), &OsString::from("x"), 0o644, 0, 0)
            .await
            .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 6144];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = h.fs.write(&fdh, 0, &buf[..], 0).await;
        assert_eq!(Ok(6144), r);

        // Check the file size
        let inode = h.fs.getattr(&fdh).await.unwrap();
        assert_eq!(inode.size, 6144);

        let sglist = h.fs.read(&fdh, 0, 4096).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[0..4096]);
        let sglist = h.fs.read(&fdh, 4096, 2048).await.unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[4096..6144]);
    }

    /// regression test for an insufficient credit bug.  Triggered by
    /// "cp -a /usr/include /testpool/include"
    #[rstest(h, case(harness4k()))]
    #[tokio::test]
    #[awt]
    async fn write_one_and_two_halves_records(#[future] h: Harness) {
        //let rs = 4096;
        let rse = h.fs.get_prop(PropertyName::RecordSize)
            .await
            .unwrap()
            .0
            .as_u8();
        let rs = (1 << rse) as u64;
        let nextents = 2048;    // Must be close to max_leaf_fanout

        let root = h.fs.root();
        let fd = h.fs.create(&root.handle(), &OsString::from("x"), 0o644, 0, 0)
            .await
            .unwrap();
        let fdh = fd.handle();
        let mut buf = vec![0u8; 1024]; // Largest extent that will remain inline
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        // Fill up the file with InlineExtents
        for i in 0..nextents {
            let r = h.fs.write(&fdh, i * rs, &buf[..], 0)
                .await;
            assert_eq!(Ok(1024), r);
        }
        h.fs.sync().await;

        // Now rewrite somewhere in the middle record, which will require
        // acrediting a very large leaf node
        let r = h.fs.write(&fdh, nextents / 2 * rs, &buf[..], 0).await;
        assert_eq!(Ok(1024), r);
    }
}
