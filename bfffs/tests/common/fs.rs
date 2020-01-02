// vim: tw=80
use galvanic_test::test_suite;

// Constructs a real filesystem and tests the common FS routines, without
// mounting
test_suite! {
    name fs;

    use bfffs::{
        common::{RID, ZERO_REGION_LEN},
        common::cache::*,
        common::database::*,
        common::ddml::*,
        common::fs::*,
        common::idml::*,
        common::pool::*,
        common::property::*
    };
    use futures::{Future, future};
    use galvanic_test::*;
    use libc;
    use pretty_assertions::assert_eq;
    use rand::{Rng, thread_rng};
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
    use time::Timespec;
    use tokio_io_pool::Runtime;

    fixture!( mocks(props: Vec<Property>)
              -> (Fs, Runtime, Arc<Mutex<Cache>>, Arc<Database>, TreeID)
    {
        params { vec![Vec::new()].into_iter() }

        setup(&mut self) {
            let mut rt = Runtime::new();
            let handle = rt.handle().clone();
            let len = 1 << 30;  // 1GB
            let tempdir = t!(Builder::new().prefix("test_fs").tempdir());
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            drop(file);
            let cache = Arc::new(Mutex::new(Cache::with_capacity(1_000_000)));
            let cache2 = cache.clone();
            let db = rt.block_on(future::lazy(move || {
                Pool::create_cluster(None, 1, None, 0, &[filename])
                .map_err(|_| unreachable!())
                .and_then(|cluster| {
                    Pool::create(String::from("test_fs"), vec![cluster])
                    .map(|pool| {
                        let ddml = Arc::new(DDML::new(pool, cache2.clone()));
                        let idml = IDML::create(ddml, cache2);
                        Arc::new(Database::create(Arc::new(idml), handle))
                    })
                })
            })).unwrap();
            let handle = rt.handle().clone();
            let props = self.props.clone();
            let db2 = db.clone();
            let (fs, tree_id) = rt.block_on(future::lazy(move || {
                db2.new_fs(props)
                .map(move |tree_id| {
                    (Fs::new(db2, handle, tree_id), tree_id)
                })
            })).unwrap();
            (fs, rt, cache, db, tree_id)
        }
    });

    fn assert_dirents_collide(name0: &OsStr, name1: &OsStr) {
        use bfffs::common::fs_tree::ObjKey;

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
        use bfffs::common::fs_tree::ObjKey;

        let objkey0 = ObjKey::extattr(ns0, name0);
        let objkey1 = ObjKey::extattr(ns1, name1);
        // If this assertion fails, then the on-disk format has changed.  If
        // that was intentional, then generate new has collisions by running
        // examples/hash_collision.rs.
        assert_eq!(objkey0.offset(), objkey1.offset());
    }

    /// Assert that some combination of timestamps have changed since they were
    /// last cleared.
    fn assert_ts_changed(ds: &Fs, fd: &FileData, atime: bool, mtime: bool,
                         ctime: bool, birthtime: bool)
    {
        let attr = ds.getattr(fd).unwrap();
        let ts0 = Timespec{sec: 0, nsec: 0};
        assert!(atime ^ (attr.atime == ts0));
        assert!(mtime ^ (attr.mtime == ts0));
        assert!(ctime ^ (attr.ctime == ts0));
        assert!(birthtime ^ (attr.birthtime == ts0));
    }

    fn clear_timestamps(ds: &Fs, fd: &FileData) {
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
        ds.setattr(fd, attr).unwrap();
    }

    test create(mocks) {
        let name = OsStr::from_bytes(b"x");
        let root = mocks.val.0.root();
        let fd0 = mocks.val.0.create(&root, name, 0o644, 0, 0).unwrap();
        let fd1 = mocks.val.0.lookup(None, &root, name).unwrap();
        assert_eq!(fd1.ino(), fd0.ino());

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(&root, 0);
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
        assert_eq!(u64::from(dirent.d_fileno), fd0.ino());

        // The parent dir's link count should not have increased
        let parent_attr = mocks.val.0.getattr(&root).unwrap();
        assert_eq!(parent_attr.nlink, 1);
    }

    /// Creating a file that already exists should panic.  It is the
    /// responsibility of the VFS to prevent this error when you call
    /// open(_, O_CREAT)
    #[should_panic]
    test create_eexist(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let _fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
    }

    /// Create should update the parent dir's timestamps
    test create_timestamps(mocks) {
        let root = mocks.val.0.root();
        clear_timestamps(&mocks.val.0, &root);

        mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0).unwrap();
        assert_ts_changed(&mocks.val.0, &root, false, true, true, false);
    }

    test deleteextattr(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, ns, &name, &value[..]).unwrap();
        mocks.val.0.deleteextattr(&fd, ns, &name).unwrap();
        assert_eq!(mocks.val.0.getextattr(&fd, ns, &name).unwrap_err(),
            libc::ENOATTR);
    }

    /// deleteextattr with a hash collision.
    test deleteextattr_collision(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();

        // First try deleting the attributes in order
        mocks.val.0.setextattr(&fd, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(&fd, ns1, &name1, &value1[..]).unwrap();
        mocks.val.0.deleteextattr(&fd, ns0, &name0).unwrap();
        assert!(mocks.val.0.getextattr(&fd, ns0, &name0).is_err());
        assert!(mocks.val.0.getextattr(&fd, ns1, &name1).is_ok());
        mocks.val.0.deleteextattr(&fd, ns1, &name1).unwrap();
        assert!(mocks.val.0.getextattr(&fd, ns0, &name0).is_err());
        assert!(mocks.val.0.getextattr(&fd, ns1, &name1).is_err());

        // Repeat, this time out-of-order
        mocks.val.0.setextattr(&fd, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(&fd, ns1, &name1, &value1[..]).unwrap();
        mocks.val.0.deleteextattr(&fd, ns1, &name1).unwrap();
        assert!(mocks.val.0.getextattr(&fd, ns0, &name0).is_ok());
        assert!(mocks.val.0.getextattr(&fd, ns1, &name1).is_err());
        mocks.val.0.deleteextattr(&fd, ns0, &name0).unwrap();
        assert!(mocks.val.0.getextattr(&fd, ns0, &name0).is_err());
        assert!(mocks.val.0.getextattr(&fd, ns1, &name1).is_err());
    }

    /// deleteextattr of a nonexistent attribute that hash-collides with an
    /// existing one.
    test deleteextattr_collision_enoattr(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];

        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();

        mocks.val.0.setextattr(&fd, ns0, &name0, &value0[..]).unwrap();

        assert_eq!(mocks.val.0.deleteextattr(&fd, ns1, &name1),
                   Err(libc::ENOATTR));
        assert!(mocks.val.0.getextattr(&fd, ns0, &name0).is_ok());
    }

    test deleteextattr_enoattr(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let ns = ExtAttrNamespace::User;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        assert_eq!(mocks.val.0.deleteextattr(&fd, ns, &name),
                   Err(libc::ENOATTR));
    }

    /// rmextattr(2) should not modify any timestamps
    test deleteextattr_timestamps(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, ns, &name, &value[..]).unwrap();
        clear_timestamps(&mocks.val.0, &&fd);

        mocks.val.0.deleteextattr(&fd, ns, &name).unwrap();
        assert_ts_changed(&mocks.val.0, &fd, false, false, false, false);
    }

    // Dumps a nearly FS tree.  All of the real work is done in Tree::dump, so
    // the bulk of testing is in the tree tests.
    test dump(mocks) {
        let root = mocks.val.0.root();
        clear_timestamps(&mocks.val.0, &root);
        mocks.val.0.sync();

        let mut buf = Vec::with_capacity(1024);
        mocks.val.0.dump(&mut buf).unwrap();
        let fs_tree = String::from_utf8(buf).unwrap();
        let expected = r#"---
height: 1
limits:
  min_int_fanout: 91
  max_int_fanout: 364
  min_leaf_fanout: 576
  max_leaf_fanout: 2302
  _max_size: 4194304
root:
  key: 0
  txgs:
    start: 0
    end: 1
  ptr:
    Addr: 0
---
0:
  Leaf:
    items:
      18478388752068107043:
        DirEntry:
          ino: 1
          dtype: 4
          name:
            Unix:
              - 46
              - 46
      18490468108375165510:
        DirEntry:
          ino: 1
          dtype: 4
          name:
            Unix:
              - 46
      18518801667747479552:
        Inode:
          size: 0
          nlink: 1
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
          perm: 493
          file_type: Dir
"#;
        assert_eq!(expected, fs_tree);
    }

    /// getattr on the filesystem's root directory
    test getattr(mocks) {
        let root = mocks.val.0.root();
        let attr = mocks.val.0.getattr(&root).unwrap();
        assert_eq!(attr.nlink, 1);
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

    test getextattr(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let namespace = ExtAttrNamespace::User;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, namespace, &name, &value[..]).unwrap();
        assert_eq!(mocks.val.0.getextattrlen(&fd, namespace, &name).unwrap(),
                   3);
        let v = mocks.val.0.getextattr(&fd, namespace, &name).unwrap();
        assert_eq!(&v[..], &value);
    }

    /// Read a large extattr as a blob
    test getextattr_blob(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = vec![42u8; 4096];
        let namespace = ExtAttrNamespace::User;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, namespace, &name, &value[..]).unwrap();

        // Sync the filesystem to flush the InlineExtent to a BlobExtent
        mocks.val.0.sync();

        assert_eq!(mocks.val.0.getextattrlen(&fd, namespace, &name).unwrap(),
                   4096);
        let v = mocks.val.0.getextattr(&fd, namespace, &name).unwrap();
        assert_eq!(&v[..], &value[..]);
    }

    /// A collision between a blob extattr and an inline one.  Get the blob
    /// extattr.
    test getextattr_blob_collision(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = vec![42u8; 4096];

        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();

        mocks.val.0.setextattr(&fd, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(&fd, ns1, &name1, &value1[..]).unwrap();
        mocks.val.0.sync(); // Flush the large xattr into a blob
        assert_eq!(mocks.val.0.getextattrlen(&fd, ns1, &name1).unwrap(), 4096);
        let v1 = mocks.val.0.getextattr(&fd, ns1, &name1).unwrap();
        assert_eq!(&v1[..], &value1[..]);
    }

    /// setextattr and getextattr with a hash collision.
    test getextattr_collision(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();

        mocks.val.0.setextattr(&fd, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(&fd, ns1, &name1, &value1[..]).unwrap();
        assert_eq!(mocks.val.0.getextattrlen(&fd, ns0, &name0).unwrap(), 3);
        let v0 = mocks.val.0.getextattr(&fd, ns0, &name0).unwrap();
        assert_eq!(&v0[..], &value0);
        assert_eq!(mocks.val.0.getextattrlen(&fd, ns1, &name1).unwrap(), 4);
        let v1 = mocks.val.0.getextattr(&fd, ns1, &name1).unwrap();
        assert_eq!(&v1[..], &value1);
    }

    // The same attribute name exists in two namespaces
    test getextattr_dual_namespaces(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value1 = [1u8, 2, 3];
        let value2 = [4u8, 5, 6, 7];
        let ns1 = ExtAttrNamespace::User;
        let ns2 = ExtAttrNamespace::System;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, ns1, &name, &value1[..]).unwrap();
        mocks.val.0.setextattr(&fd, ns2, &name, &value2[..]).unwrap();

        assert_eq!(mocks.val.0.getextattrlen(&fd, ns1, &name).unwrap(), 3);
        let v1 = mocks.val.0.getextattr(&fd, ns1, &name).unwrap();
        assert_eq!(&v1[..], &value1);

        assert_eq!(mocks.val.0.getextattrlen(&fd, ns2, &name).unwrap(), 4);
        let v2 = mocks.val.0.getextattr(&fd, ns2, &name).unwrap();
        assert_eq!(&v2[..], &value2);
    }

    // The file exists, but its extended attribute does not
    test getextattr_enoattr(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let namespace = ExtAttrNamespace::User;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        assert_eq!(mocks.val.0.getextattrlen(&fd, namespace, &name),
                   Err(libc::ENOATTR));
        assert_eq!(mocks.val.0.getextattr(&fd, namespace, &name),
                   Err(libc::ENOATTR));
    }

    // The file does not exist.  Fortunately, VOP_GETEXTATTR(9) does not require
    // us to distinguish this from the ENOATTR case.
    test getextattr_enoent(mocks) {
        let name = OsString::from("foo");
        let namespace = ExtAttrNamespace::User;
        let fd = FileData::new_for_tests(Some(1), 9999);
        assert_eq!(mocks.val.0.getextattrlen(&fd, namespace, &name),
                   Err(libc::ENOATTR));
        assert_eq!(mocks.val.0.getextattr(&fd, namespace, &name),
                   Err(libc::ENOATTR));
    }

    /// Read an InlineExtAttr from disk
    test getextattr_inline(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = vec![0, 1, 2, 3, 4];
        let namespace = ExtAttrNamespace::User;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, namespace, &name, &value[..]).unwrap();

        // Sync the filesystem to store the InlineExtent on disk
        mocks.val.0.sync();

        // Drop cache
        mocks.val.2.lock().unwrap().drop_cache();

        // Read the extattr from disk
        assert_eq!(mocks.val.0.getextattrlen(&fd, namespace, &name).unwrap(),
                   5);
        let v = mocks.val.0.getextattr(&fd, namespace, &name).unwrap();
        assert_eq!(&v[..], &value[..]);
    }

    /// getextattr(2) should not modify any timestamps
    test getextattr_timestamps(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let namespace = ExtAttrNamespace::User;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, namespace, &name, &value[..]).unwrap();
        clear_timestamps(&mocks.val.0, &fd);

        mocks.val.0.getextattr(&fd, namespace, &name).unwrap();
        assert_ts_changed(&mocks.val.0, &fd, false, false, false, false);
    }

    test link(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let fd = mocks.val.0.create(&root, &src, 0o644, 0, 0).unwrap();
        mocks.val.0.link(&root, &fd, &dst).unwrap();

        // The target's link count should've increased
        let attr = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(attr.nlink, 2);

        // The parent should have a new directory entry
        assert_eq!(mocks.val.0.lookup(None, &root, &dst).unwrap().ino(),
            fd.ino());
    }

    /// link(2) should update the inode's ctime
    test link_ctime(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let fd = mocks.val.0.create(&root, &src, 0o644, 0, 0).unwrap();
        clear_timestamps(&mocks.val.0, &fd);
        mocks.val.0.link(&root, &fd, &dst).unwrap();
        assert_ts_changed(&mocks.val.0, &fd, false, false, true, false);
    }

    ///link(2) should update the parent's mtime and ctime
    test link_parent_timestamps(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let fd = mocks.val.0.create(&root, &src, 0o644, 0, 0).unwrap();
        clear_timestamps(&mocks.val.0, &root);

        mocks.val.0.link(&root, &fd, &dst).unwrap();
        assert_ts_changed(&mocks.val.0, &root, false, true, true, false);
    }


    /// Helper for FreeBSD-style VFS
    ///
    /// In due course this should move into the FreeBSD implementation of
    /// `vop_listextattr`, and the test should move into that file, too.
    fn listextattr_lenf(ns: ExtAttrNamespace)
        -> impl Fn(&ExtAttr<RID>) -> u32 + Send + 'static
    {
        move |extattr: &ExtAttr<RID>| {
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
        -> impl Fn(&mut Vec<u8>, &ExtAttr<RID>) + Send + 'static
    {
        move |buf: &mut Vec<u8>, extattr: &ExtAttr<RID>| {
            if ns == extattr.namespace() {
                assert!(extattr.name().len() <= u8::max_value() as usize);
                buf.push(extattr.name().len() as u8);
                buf.extend_from_slice(extattr.name().as_bytes());
            }
        }
    }

    test listextattr(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let name2 = OsString::from("bar");
        let ns = ExtAttrNamespace::User;
        let value = [0u8, 1, 2];
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, ns, &name1, &value[..]).unwrap();
        mocks.val.0.setextattr(&fd, ns, &name2, &value[..]).unwrap();

        // expected has the form of <length as u8><value as [u8]>...
        // values are _not_ null terminated.
        // There is no requirement on the order of names
        let expected = b"\x03bar\x03foo";

        let lenf = self::listextattr_lenf(ns);
        let lsf = self::listextattr_lsf(ns);
        assert_eq!(mocks.val.0.listextattrlen(&fd, lenf).unwrap(), 8);
        assert_eq!(&mocks.val.0.listextattr(&fd, 64, lsf).unwrap()[..],
                   &expected[..]);
    }

    /// setextattr and listextattr with a cross-namespace hash collision.
    test listextattr_collision_separate_namespaces(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();

        mocks.val.0.setextattr(&fd, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(&fd, ns1, &name1, &value1[..]).unwrap();

        let expected0 = b"\x0aBWCdLQkApB";
        let lenf0 = self::listextattr_lenf(ns0);
        let lsf0 = self::listextattr_lsf(ns0);
        assert_eq!(mocks.val.0.listextattrlen(&fd, lenf0).unwrap(), 11);
        assert_eq!(&mocks.val.0.listextattr(&fd, 64, lsf0).unwrap()[..],
                   &expected0[..]);

        let expected1 = b"\x0aD6tLLI4mys";
        let lenf1 = self::listextattr_lenf(ns1);
        let lsf1 = self::listextattr_lsf(ns1);
        assert_eq!(mocks.val.0.listextattrlen(&fd, lenf1).unwrap(), 11);
        assert_eq!(&mocks.val.0.listextattr(&fd, 64, lsf1).unwrap()[..],
                   &expected1[..]);
    }

    test listextattr_dual_namespaces(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let name2 = OsString::from("bean");
        let ns1 = ExtAttrNamespace::User;
        let ns2 = ExtAttrNamespace::System;
        let value1 = [0u8, 1, 2];
        let value2 = [3u8, 4, 5];

        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, ns1, &name1, &value1[..]).unwrap();
        mocks.val.0.setextattr(&fd, ns2, &name2, &value2[..]).unwrap();

        // Test queries for a single namespace
        let lenf = self::listextattr_lenf(ns1);
        let lsf = self::listextattr_lsf(ns1);
        assert_eq!(mocks.val.0.listextattrlen(&fd, lenf), Ok(4));
        assert_eq!(&mocks.val.0.listextattr(&fd, 64, lsf).unwrap()[..],
                   &b"\x03foo"[..]);
        let lenf = self::listextattr_lenf(ns2);
        let lsf = self::listextattr_lsf(ns2);
        assert_eq!(mocks.val.0.listextattrlen(&fd, lenf), Ok(5));
        assert_eq!(&mocks.val.0.listextattr(&fd, 64, lsf).unwrap()[..],
                   &b"\x04bean"[..]);
    }

    test listextattr_empty(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        let lenf = self::listextattr_lenf(ExtAttrNamespace::User);
        let lsf = self::listextattr_lsf(ExtAttrNamespace::User);
        assert_eq!(mocks.val.0.listextattrlen(&fd, lenf), Ok(0));
        assert!(mocks.val.0.listextattr(&fd, 64, lsf).unwrap().is_empty());
    }

    /// Lookup of a directory entry that has a hash collision
    test lookup_collision(mocks) {
        let root = mocks.val.0.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = mocks.val.0.create(&root, &filename0, 0o644, 0, 0).unwrap();
        let fd1 = mocks.val.0.create(&root, &filename1, 0o644, 0, 0).unwrap();

        assert_eq!(mocks.val.0.lookup(None, &root, &filename0).unwrap().ino(),
            fd0.ino());
        assert_eq!(mocks.val.0.lookup(None, &root, &filename1).unwrap().ino(),
            fd1.ino());
    }

    test lookup_dot(mocks) {
        let name0 = OsStr::from_bytes(b"x");
        let dotname = OsStr::from_bytes(b".");

        let root = mocks.val.0.root();
        let fd0 = mocks.val.0.mkdir(&root, name0, 0o755, 0, 0).unwrap();

        let fd1 = mocks.val.0.lookup(Some(&root), &fd0, dotname).unwrap();
        assert_eq!(fd1.ino(), fd0.ino());
        assert_eq!(fd1.parent(), Some(root.ino()));
    }

    test lookup_dotdot(mocks) {
        let name0 = OsStr::from_bytes(b"x");
        let name1 = OsStr::from_bytes(b"y");
        let dotdotname = OsStr::from_bytes(b"..");

        let root = mocks.val.0.root();
        let fd0 = mocks.val.0.mkdir(&root, name0, 0o755, 0, 0).unwrap();
        let fd1 = mocks.val.0.mkdir(&fd0, name1, 0o755, 0, 0).unwrap();

        let fd2 = mocks.val.0.lookup(Some(&fd0), &fd1, dotdotname).unwrap();
        assert_eq!(fd2.ino(), fd0.ino());
        assert_eq!(fd2.parent(), Some(root.ino()));
    }

    test lookup_enoent(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("nonexistent");
        assert_eq!(mocks.val.0.lookup(None, &root, &filename).unwrap_err(),
            libc::ENOENT);
    }

    test mkdir(mocks) {
        let name = OsStr::from_bytes(b"x");
        let root = mocks.val.0.root();
        let fd = mocks.val.0.mkdir(&root, name, 0o755, 0, 0)
        .unwrap();
        let fd1 = mocks.val.0.lookup(None, &root, name).unwrap();
        assert_eq!(fd1.ino(), fd.ino());

        // The new dir should have "." and ".." directory entries
        let mut entries = mocks.val.0.readdir(&fd, 0);
        let (dotdot, _) = entries.next().unwrap().unwrap();
        assert_eq!(dotdot.d_type, libc::DT_DIR);
        let dotdot_name = unsafe{
            CStr::from_ptr(&dotdot.d_name as *const c_char)
        };
        assert_eq!(dotdot_name, CString::new("..").unwrap().as_c_str());
        assert_eq!(u64::from(dotdot.d_fileno), root.ino());
        let (dot, _) = entries.next().unwrap().unwrap();
        assert_eq!(dot.d_type, libc::DT_DIR);
        let dot_name = unsafe{
            CStr::from_ptr(&dot.d_name as *const c_char)
        };
        assert_eq!(dot_name, CString::new(".").unwrap().as_c_str());
        assert_eq!(u64::from(dot.d_fileno), fd.ino());

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(&root, 0);
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
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());

        // The parent dir's link count should've increased
        let parent_attr = mocks.val.0.getattr(&root).unwrap();
        assert_eq!(parent_attr.nlink, 2);
    }

    /// mkdir creates two directories whose names have a hash collision
    // Note that it's practically impossible to find a collision for a specific
    // name, like "." or "..", so those cases won't have test coverage
    test mkdir_collision(mocks) {
        let root = mocks.val.0.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = mocks.val.0.mkdir(&root, &filename0, 0o755, 0, 0).unwrap();
        let fd1 = mocks.val.0.mkdir(&root, &filename1, 0o755, 0, 0).unwrap();

        assert_eq!(mocks.val.0.lookup(None, &root, &filename0).unwrap().ino(),
            fd0.ino());
        assert_eq!(mocks.val.0.lookup(None, &root, &filename1).unwrap().ino(),
            fd1.ino());
    }

    /// mkdir(2) should update the parent dir's timestamps
    test mkdir_timestamps(mocks) {
        let root = mocks.val.0.root();
        clear_timestamps(&mocks.val.0, &root);

        mocks.val.0.mkdir(&root, &OsString::from("x"), 0o755, 0, 0).unwrap();
        assert_ts_changed(&mocks.val.0, &root, false, true, true, false);
    }

    test mkchar(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.mkchar(&root, &OsString::from("x"), 0o644, 0, 0, 42)
        .unwrap();
        let attr = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(attr.mode.0, libc::S_IFCHR | 0o644);
        assert_eq!(attr.rdev, 42);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(&root, 0);
        let (dirent, _ofs) = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).nth(0)
        .expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_CHR);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());
    }

    /// mknod(2) should update the parent dir's timestamps
    test mkchar_timestamps(mocks) {
        let root = mocks.val.0.root();
        clear_timestamps(&mocks.val.0, &root);

        mocks.val.0.mkchar(&root, &OsString::from("x"), 0o644, 0, 0, 42).unwrap();
        assert_ts_changed(&mocks.val.0, &root, false, true, true, false);
    }

    test mkblock(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.mkblock(&root, &OsString::from("x"), 0o644, 0, 0, 42)
        .unwrap();
        let attr = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(attr.mode.0, libc::S_IFBLK | 0o644);
        assert_eq!(attr.rdev, 42);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(&root, 0);
        let (dirent, _ofs) = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).nth(0)
        .expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_BLK);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());
    }

    /// mknod(2) should update the parent dir's timestamps
    test mkblock_timestamps(mocks) {
        let root = mocks.val.0.root();
        clear_timestamps(&mocks.val.0, &root);

        mocks.val.0.mkblock(&root, &OsString::from("x"), 0o644, 0, 0, 42).unwrap();
        assert_ts_changed(&mocks.val.0, &root, false, true, true, false);
    }

    test mkfifo(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.mkfifo(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let attr = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(attr.mode.0, libc::S_IFIFO | 0o644);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(&root, 0);
        let (dirent, _ofs) = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).nth(0)
        .expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_FIFO);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());
    }

    /// mkfifo(2) should update the parent dir's timestamps
    test mkfifo_timestamps(mocks) {
        let root = mocks.val.0.root();
        clear_timestamps(&mocks.val.0, &root);

        mocks.val.0.mkfifo(&root, &OsString::from("x"), 0o644, 0, 0).unwrap();
        assert_ts_changed(&mocks.val.0, &root, false, true, true, false);
    }

    test mksock(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.mksock(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let attr = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(attr.mode.0, libc::S_IFSOCK | 0o644);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(&root, 0);
        let (dirent, _ofs) = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).nth(0)
        .expect("'x' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_SOCK);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name, CString::new("x").unwrap().as_c_str());
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());
    }

    /// mksock(2) should update the parent dir's timestamps
    test mksock_timestamps(mocks) {
        let root = mocks.val.0.root();
        clear_timestamps(&mocks.val.0, &root);

        mocks.val.0.mkfifo(&root, &OsString::from("x"), 0o644, 0, 0).unwrap();
        assert_ts_changed(&mocks.val.0, &root, false, true, true, false);
    }

    // If the file system was unmounted uncleanly and has open but deleted
    // files, they should be deleted during mount
    test mount_with_open_but_deleted_files(mocks) {
        let (fs, rt, _cache, db, tree_id) = mocks.val;
        let root = fs.root();

        // First create a file, open it, and unlink it, but don't close it
        let filename = OsString::from("x");
        let fd = fs.create(&root, &filename, 0o644, 0, 0).unwrap();
        let r = fs.unlink(&root, Some(&fd), &filename);
        fs.sync();
        assert_eq!(Ok(()), r);

        // Unmount, without closing the file
        drop(fs);

        // Mount again
        let handle = rt.handle().clone();
        let fs = Fs::new(db, handle, tree_id);

        // Try to open the file again.
        // XXX It's not legal to reuse a FileData structure, but it happens to
        // work, and it's the only way to attempt to open an unlinked inode!
        // Wait up to 2 seconds for the inode to be deleted
        let mut r = Err(0);
        for _ in 0..20 {
            r = fs.getattr(&fd);
            if r.is_err() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert_eq!(Err(libc::ENOENT), r);
    }

    // Read a hole that's bigger than the zero region
    test read_big_hole(mocks) {
        let root = mocks.val.0.root();
        let holesize = 2 * ZERO_REGION_LEN;
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, holesize as u64, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        let sglist = mocks.val.0.read(&fd, 0, holesize).unwrap();
        let expected = vec![0u8; ZERO_REGION_LEN];
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &expected[..]);
        assert_eq!(&sglist[1][..], &expected[..]);
    }

    // Read a single BlobExtent record
    test read_blob(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let buf = vec![42u8; 4096];
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        // Sync the filesystem to flush the InlineExtent to a BlobExtent
        mocks.val.0.sync();

        let sglist = mocks.val.0.read(&fd, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[..]);
    }

    test read_empty_file(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let sglist = mocks.val.0.read(&fd, 0, 1024).unwrap();
        assert!(sglist.is_empty());
    }

    test read_empty_file_past_start(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let sglist = mocks.val.0.read(&fd, 2048, 2048).unwrap();
        assert!(sglist.is_empty());
    }

    // Read a hole within a sparse file
    test read_hole(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 4096, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        let sglist = mocks.val.0.read(&fd, 0, 4096).unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);
    }

    // Read a record within a sparse file that is partially occupied by an
    // inline extent
    test read_partial_hole(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(2048), r);
        let r = mocks.val.0.write(&fd, 4096, &buf[..], 0);
        assert_eq!(Ok(2048), r);

        // The file should now have a hole from offset 2048 to 4096
        let sglist = mocks.val.0.read(&fd, 3072, 1024).unwrap();
        let db = &sglist[0];
        let expected = [0u8; 1024];
        assert_eq!(&db[..], &expected[..]);
    }

    // Read a chunk of a file that includes a partial hole at the beginning and
    // data at the end.
    test read_partial_hole_trailing_edge(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(2048), r);
        let r = mocks.val.0.write(&fd, 4096, &buf[..], 0);
        assert_eq!(Ok(2048), r);

        // The file should now have a hole from offset 2048 to 4096
        let sglist = mocks.val.0.read(&fd, 3072, 2048).unwrap();
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &[0u8; 1024][..]);
        assert_eq!(&sglist[1][..], &buf[0..1024]);
    }

    // A read that's smaller than a record, at both ends
    test read_partial_record(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        let sglist = mocks.val.0.read(&fd, 1024, 2048).unwrap();
        let db = &sglist[0];
        assert_eq!(db.len(), 2048);
        assert_eq!(&db[..], &buf[1024..3072]);
    }

    test read_past_eof(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(2048), r);

        let sglist = mocks.val.0.read(&fd, 2048, 1024).unwrap();
        assert!(sglist.is_empty());
    }

    /// A read that spans 3 records, where the middle record is a hole
    test read_spans_hole(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        assert_eq!(4096, mocks.val.0.write(&fd, 0, &buf[..], 0).unwrap());
        assert_eq!(4096, mocks.val.0.write(&fd, 8192, &buf[..], 0).unwrap());

        let sglist = mocks.val.0.read(&fd, 0, 12288).unwrap();
        assert_eq!(sglist.len(), 3);
        assert_eq!(&sglist[0][..], &buf[..]);
        assert_eq!(&sglist[1][..], &[0u8; 4096][..]);
        assert_eq!(&sglist[2][..], &buf[..]);
    }

    /// read(2) should update the file's atime
    test read_timestamps(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let buf = vec![42u8; 4096];
        mocks.val.0.write(&fd, 0, &buf[..], 0).unwrap();
        clear_timestamps(&mocks.val.0, &fd);

        mocks.val.0.read(&fd, 0, 4096).unwrap();
        assert_ts_changed(&mocks.val.0, &fd, true, false, false, false);
    }

    // When atime is disabled, reading a file should not update its atime.
    test read_timestamps_no_atime(mocks(vec![Property::Atime(false)])) {
        let (fs, _rt, _cache, _db, _tree_id) = mocks.val;
        let root = fs.root();

        let fd = fs.create(&root, &OsString::from("x"), 0o644, 0, 0).unwrap();
        let buf = vec![42u8; 4096];
        fs.write(&fd, 0, &buf[..], 0).unwrap();
        clear_timestamps(&fs, &fd);

        fs.read(&fd, 0, 4096).unwrap();
        assert_ts_changed(&fs, &fd, false, false, false, false);
    }

    // A read that's split across two records
    test read_two_recs(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf[0..4096], 0);
        assert_eq!(Ok(4096), r);
        let r = mocks.val.0.write(&fd, 4096, &buf[4096..8192], 0);
        assert_eq!(Ok(4096), r);

        let sglist = mocks.val.0.read(&fd, 0, 8192).unwrap();
        let db0 = &sglist[0];
        assert_eq!(&db0[..], &buf[0..4096]);
        let db1 = &sglist[1];
        assert_eq!(&db1[..], &buf[4096..8192]);
    }

    // Read past EOF, in an entirely different record
    test read_well_past_eof(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        let sglist = mocks.val.0.read(&fd, 1 << 30, 4096).unwrap();
        assert!(sglist.is_empty());
    }

    test readdir(mocks) {
        let root = mocks.val.0.root();
        let mut entries = mocks.val.0.readdir(&root, 0);
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
        assert_eq!(u64::from(dot.d_fileno), root.ino());
    }

    // Readdir of a directory with a hash collision
    test readdir_collision(mocks) {
        let root = mocks.val.0.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);

        mocks.val.0.create(&root, &filename0, 0o644, 0, 0).unwrap();
        mocks.val.0.create(&root, &filename1, 0o644, 0, 0).unwrap();

        // There's no requirement for the order of readdir's output.
        let mut expected = HashSet::new();
        expected.insert(OsString::from("."));
        expected.insert(OsString::from(".."));
        expected.insert(filename0.clone());
        expected.insert(filename1.clone());
        for result in mocks.val.0.readdir(&root, 0) {
            let entry = result.unwrap().0;
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
    test readdir_collision_at_offset(mocks) {
        let root = mocks.val.0.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);

        let fd0 = mocks.val.0.create(&root, &filename0, 0o644, 0, 0).unwrap();
        let _fd1 = mocks.val.0.create(&root, &filename1, 0o644, 0, 0).unwrap();

        // There's no requirement for the order of readdir's output, but
        // filename0 happens to come first.
        let mut stream0 = mocks.val.0.readdir(&root, 0);
        let (result0, offset0) = stream0.next().unwrap().unwrap();
        assert_eq!(u64::from(result0.d_fileno), fd0.ino());

        // Now interrupt the stream, and resume with the supplied offset.
        let mut expected = HashSet::new();
        expected.insert(OsString::from("."));
        expected.insert(OsString::from(".."));
        expected.insert(filename1.clone());
        drop(stream0);
        let stream1 = mocks.val.0.readdir(&root, offset0);
        for result in stream1 {
            let entry = result.unwrap().0;
            let nameptr = entry.d_name.as_ptr() as *const u8;
            let namelen = usize::from(entry.d_namlen);
            let name_s = unsafe{slice::from_raw_parts(nameptr, namelen)};
            let name = OsStr::from_bytes(name_s);
            assert!(expected.remove(name));
        }
        assert!(expected.is_empty());
    }

    // It's allowed for the client of Fs::readdir to drop the iterator without
    // reading all entries.  The FUSE module does that when it runs out of space
    // in the kernel-provided buffer.
    // Just check that nothing panics.
    test readdir_partial(mocks) {
        let root = mocks.val.0.root();
        let mut entries = mocks.val.0.readdir(&root, 0);
        let _ = entries.next().unwrap().unwrap();
    }

    /// readdir(2) should not update any timestamps
    test readdir_timestamps(mocks) {
        let root = mocks.val.0.root();
        clear_timestamps(&mocks.val.0, &root);

        let mut entries = mocks.val.0.readdir(&root, 0);
        entries.next().unwrap().unwrap();
        entries.next().unwrap().unwrap();
        assert_ts_changed(&mocks.val.0, &root, false, false, false, false);
    }

    test readlink(mocks) {
        let root = mocks.val.0.root();
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let fd = mocks.val.0.symlink(&root, &srcname, 0o642, 0, 0, &dstname)
        .unwrap();
        assert_eq!(dstname, mocks.val.0.readlink(&fd).unwrap());
    }

    // Calling readlink on a non-symlink should return EINVAL
    test readlink_einval(mocks) {
        let root = mocks.val.0.root();
        assert_eq!(libc::EINVAL, mocks.val.0.readlink(&root).unwrap_err());
    }

    test readlink_enoent(mocks) {
        let fd = FileData::new_for_tests(Some(1), 1000);
        assert_eq!(libc::ENOENT, mocks.val.0.readlink(&fd).unwrap_err());
    }

    /// readlink(2) should not update any timestamps
    test readlink_timestamps(mocks) {
        let root = mocks.val.0.root();
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let fd = mocks.val.0.symlink(&root, &srcname, 0o642, 0, 0, &dstname)
        .unwrap();
        clear_timestamps(&mocks.val.0, &fd);

        assert_eq!(dstname, mocks.val.0.readlink(&fd).unwrap());
        assert_ts_changed(&mocks.val.0, &fd, false, false, false, false);
    }

    // Rename a file that has a hash collision in both the source and
    // destination directories
    test rename_collision(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("F0jS2Tptj7");
        let src_c = OsString::from("PLe01T116a");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("Gg1AG3wll2");
        let dst_c = OsString::from("FDCIlvDxYn");
        let dstdir = OsString::from("dstdir");
        assert_dirents_collide(&src, &src_c);
        assert_dirents_collide(&dst, &dst_c);

        let srcdir_fd = mocks.val.0.mkdir(&root, &srcdir, 0o755, 0, 0).unwrap();
        let dstdir_fd = mocks.val.0.mkdir(&root, &dstdir, 0o755, 0, 0).unwrap();
        let src_c_fd = mocks.val.0.create(&srcdir_fd, &src_c, 0o644, 0, 0)
        .unwrap();
        let src_c_ino = src_c_fd.ino();
        let dst_c_fd = mocks.val.0.create(&dstdir_fd, &dst_c, 0o644, 0, 0)
        .unwrap();
        let dst_c_ino = dst_c_fd.ino();
        let src_fd = mocks.val.0.create(&srcdir_fd, &src, 0o644, 0, 0).unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = mocks.val.0.create(&dstdir_fd, &dst, 0o644, 0, 0).unwrap();

        assert_eq!(src_fd.ino(),
            mocks.val.0.rename(&srcdir_fd, &src_fd, &src, &dstdir_fd,
                Some(dst_fd.ino()), &dst).unwrap()
        );

        mocks.val.0.inactive(src_fd);
        assert_eq!(src_ino,
            mocks.val.0.lookup(Some(&root), &dstdir_fd, &dst).unwrap().ino()
        );
        let r = mocks.val.0.lookup(Some(&root), &srcdir_fd, &src);
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let srcdir_inode = mocks.val.0.getattr(&srcdir_fd).unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = mocks.val.0.getattr(&dstdir_fd).unwrap();
        assert_eq!(dstdir_inode.nlink, 2);

        // Finally, make sure we didn't upset the colliding files
        mocks.val.0.inactive(src_c_fd);
        mocks.val.0.inactive(dst_c_fd);
        let src_c_fd1 = mocks.val.0.lookup(Some(&root), &srcdir_fd, &src_c);
        assert_eq!(src_c_fd1.unwrap().ino(), src_c_ino);
        let dst_c_fd1 = mocks.val.0.lookup(Some(&root), &dstdir_fd, &dst_c);
        assert_eq!(dst_c_fd1.unwrap().ino(), dst_c_ino);
    }

    // Rename a directory.  The target is also a directory
    test rename_dir_to_dir(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = mocks.val.0.mkdir(&root, &srcdir, 0o755, 0, 0)
        .unwrap();
        let dstdir_fd = mocks.val.0.mkdir(&root, &dstdir, 0o755, 0, 0)
        .unwrap();
        let src_fd = mocks.val.0.mkdir(&srcdir_fd, &src, 0o755, 0, 0)
        .unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = mocks.val.0.mkdir(&dstdir_fd, &dst, 0o755, 0, 0)
        .unwrap();

        assert_eq!(src_fd.ino(),
            mocks.val.0.rename(&srcdir_fd, &src_fd, &src, &dstdir_fd,
                Some(dst_fd.ino()), &dst).unwrap()
        );

        mocks.val.0.inactive(src_fd);
        let dst_fd1 = mocks.val.0.lookup(Some(&root), &dstdir_fd, &dst);
        assert_eq!(dst_fd1.unwrap().ino(), src_ino);
        let r = mocks.val.0.lookup(Some(&root), &srcdir_fd, &src);
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let srcdir_inode = mocks.val.0.getattr(&srcdir_fd).unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = mocks.val.0.getattr(&dstdir_fd).unwrap();
        assert_eq!(dstdir_inode.nlink, 3);
    }

    test rename_dir_to_dir_same_parent(mocks) {
        let root = mocks.val.0.root();
        let parent = OsString::from("parent");
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let parent_fd = mocks.val.0.mkdir(&root, &parent, 0o755, 0, 0).unwrap();
        let src_fd = mocks.val.0.mkdir(&parent_fd, &src, 0o755, 0, 0).unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = mocks.val.0.mkdir(&parent_fd, &dst, 0o755, 0, 0).unwrap();

        assert_eq!(src_fd.ino(),
            mocks.val.0.rename(&parent_fd, &src_fd, &src, &parent_fd,
                Some(dst_fd.ino()), &dst).unwrap()
        );

        mocks.val.0.inactive(src_fd);
        let dst_fd1 = mocks.val.0.lookup(Some(&root), &parent_fd, &dst);
        assert_eq!(dst_fd1.unwrap().ino(), src_ino);
        let r = mocks.val.0.lookup(Some(&root), &parent_fd, &src);
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let parent_inode = mocks.val.0.getattr(&parent_fd).unwrap();
        assert_eq!(parent_inode.nlink, 3);
    }

    // Rename a directory.  The target is also a directory that isn't empty.
    // Nothing should change.
    test rename_dir_to_nonemptydir(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let dstf = OsString::from("dstf");
        let srcdir_fd = mocks.val.0.mkdir(&root, &srcdir, 0o755, 0, 0)
        .unwrap();
        let dstdir_fd = mocks.val.0.mkdir(&root, &dstdir, 0o755, 0, 0)
        .unwrap();
        let src_fd = mocks.val.0.mkdir(&srcdir_fd, &src, 0o755, 0, 0)
        .unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = mocks.val.0.mkdir(&dstdir_fd, &dst, 0o755, 0, 0)
        .unwrap();
        let dst_ino = dst_fd.ino();
        let dstf_fd = mocks.val.0.create(&dst_fd, &dstf, 0o644, 0, 0)
        .unwrap();

        let r = mocks.val.0.rename(&srcdir_fd, &src_fd, &src,
            &dstdir_fd, Some(dst_fd.ino()), &dst);
        assert_eq!(r, Err(libc::ENOTEMPTY));

        mocks.val.0.inactive(src_fd);
        assert_eq!(src_ino,
            mocks.val.0.lookup(Some(&root), &srcdir_fd, &src).unwrap().ino()
        );
        let dst_fd1 = mocks.val.0.lookup(Some(&root), &dstdir_fd, &dst)
        .unwrap();
        assert_eq!(dst_fd1.ino(), dst_ino);
        let dstf_fd1 = mocks.val.0.lookup(Some(&dstdir_fd), &dst_fd1, &dstf);
        assert_eq!(dstf_fd1.unwrap().ino(), dstf_fd.ino());
    }

    // Rename a directory.  The target name does not exist
    test rename_dir_to_nothing(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = mocks.val.0.mkdir(&root, &srcdir, 0o755, 0, 0)
        .unwrap();
        let dstdir_fd = mocks.val.0.mkdir(&root, &dstdir, 0o755, 0, 0)
        .unwrap();
        let fd = mocks.val.0.mkdir(&srcdir_fd, &src, 0o755, 0, 0).unwrap();
        let src_ino = fd.ino();

        assert_eq!(fd.ino(),
            mocks.val.0.rename(&srcdir_fd, &fd, &src, &dstdir_fd,
                None, &dst).unwrap()
        );

        mocks.val.0.inactive(fd);
        let dst_fd = mocks.val.0.lookup(Some(&root), &dstdir_fd, &dst).unwrap();
        assert_eq!(dst_fd.ino(), src_ino);
        let r = mocks.val.0.lookup(Some(&root), &srcdir_fd, &src);
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let srcdir_attr = mocks.val.0.getattr(&srcdir_fd).unwrap();
        assert_eq!(srcdir_attr.nlink, 2);
        let dstdir_attr = mocks.val.0.getattr(&dstdir_fd).unwrap();
        assert_eq!(dstdir_attr.nlink, 3);
    }

    // Attempting to rename "." should return EINVAL
    test rename_dot(mocks) {
        let root = mocks.val.0.root();
        let dot = OsStr::from_bytes(b".");
        let srcdir = OsStr::from_bytes(b"srcdir");
        let dst = OsStr::from_bytes(b"dst");
        let srcdir_fd = mocks.val.0.mkdir(&root, &srcdir, 0o755, 0, 0)
        .unwrap();

        let r = mocks.val.0.rename(&srcdir_fd, &srcdir_fd, &dot, &srcdir_fd,
                None, &dst);
        assert_eq!(Err(libc::EINVAL), r);
    }

    // Attempting to rename ".." should return EINVAL
    test rename_dotdot(mocks) {
        let root = mocks.val.0.root();
        let dotdot = OsStr::from_bytes(b"..");
        let srcdir = OsStr::from_bytes(b"srcdir");
        let dst = OsStr::from_bytes(b"dst");
        let srcdir_fd = mocks.val.0.mkdir(&root, &srcdir, 0o755, 0, 0)
        .unwrap();

        let r = mocks.val.0.rename(&srcdir_fd, &root, &dotdot, &srcdir_fd,
                None, &dst);
        assert_eq!(Err(libc::EINVAL), r);
    }

    // Rename a non-directory to a multiply-linked file.  The destination
    // directory entry should be removed, but not the inode.
    test rename_nondir_to_hardlink(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let lnk = OsString::from("lnk");
        let src_fd = mocks.val.0.create(&root, &src, 0o644, 0, 0).unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = mocks.val.0.create(&root, &dst, 0o644, 0, 0).unwrap();
        let dst_ino = dst_fd.ino();
        mocks.val.0.link(&root, &dst_fd, &lnk).unwrap();
        clear_timestamps(&mocks.val.0, &dst_fd);

        assert_eq!(src_fd.ino(),
            mocks.val.0.rename(&root, &src_fd, &src, &root, Some(dst_fd.ino()),
                &dst).unwrap()
        );

        mocks.val.0.inactive(src_fd);
        assert_eq!(mocks.val.0.lookup(None, &root, &dst).unwrap().ino(),
            src_ino);
        assert_eq!(mocks.val.0.lookup(None, &root, &src).unwrap_err(),
            libc::ENOENT);
        let lnk_fd = mocks.val.0.lookup(None, &root, &lnk).unwrap();
        assert_eq!(lnk_fd.ino(), dst_ino);
        let lnk_attr = mocks.val.0.getattr(&lnk_fd).unwrap();
        assert_eq!(lnk_attr.nlink, 1);
        assert_ts_changed(&mocks.val.0, &lnk_fd, false, false, true, false);
    }

    // Rename a non-directory.  The target is also a non-directory
    test rename_nondir_to_nondir(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = mocks.val.0.mkdir(&root, &srcdir, 0o755, 0, 0).unwrap();
        let dstdir_fd = mocks.val.0.mkdir(&root, &dstdir, 0o755, 0, 0).unwrap();
        let src_fd = mocks.val.0.create(&srcdir_fd, &src, 0o644, 0, 0).unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = mocks.val.0.create(&dstdir_fd, &dst, 0o644, 0, 0).unwrap();

        assert_eq!(src_fd.ino(),
            mocks.val.0.rename(&srcdir_fd, &src_fd, &src, &dstdir_fd,
                Some(dst_fd.ino()), &dst).unwrap()
        );

        mocks.val.0.inactive(src_fd);
        let dst_fd1 = mocks.val.0.lookup(Some(&root), &dstdir_fd, &dst);
        assert_eq!(dst_fd1.unwrap().ino(), src_ino);
        let r = mocks.val.0.lookup(Some(&root), &srcdir_fd, &src);
        assert_eq!(r.unwrap_err(), libc::ENOENT);
        let srcdir_inode = mocks.val.0.getattr(&srcdir_fd).unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = mocks.val.0.getattr(&dstdir_fd).unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
    }

    // Rename a non-directory.  The target name does not exist
    test rename_nondir_to_nothing(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = mocks.val.0.mkdir(&root, &srcdir, 0o755, 0, 0).unwrap();
        let dstdir_fd = mocks.val.0.mkdir(&root, &dstdir, 0o755, 0, 0).unwrap();
        let fd = mocks.val.0.create(&srcdir_fd, &src, 0o644, 0, 0).unwrap();
        let src_ino = fd.ino();

        assert_eq!(fd.ino(),
            mocks.val.0.rename(&srcdir_fd, &fd, &src, &dstdir_fd, None,
                &dst)
            .unwrap()
        );

        mocks.val.0.inactive(fd);
        assert_eq!(src_ino,
            mocks.val.0.lookup(Some(&root), &dstdir_fd, &dst).unwrap().ino()
        );
        assert_eq!(libc::ENOENT,
            mocks.val.0.lookup(Some(&root), &srcdir_fd, &src).unwrap_err()
        );
        let srcdir_inode = mocks.val.0.getattr(&srcdir_fd).unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = mocks.val.0.getattr(&dstdir_fd).unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
    }

    // Rename a regular file to a symlink.  Make sure the target is a regular
    // file afterwards
    test rename_reg_to_symlink(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let linktarget = OsString::from("nonexistent");
        let srcdir_fd = mocks.val.0.mkdir(&root, &srcdir, 0o755, 0, 0).unwrap();
        let dstdir_fd = mocks.val.0.mkdir(&root, &dstdir, 0o755, 0, 0).unwrap();
        let src_fd = mocks.val.0.create(&srcdir_fd, &src, 0o644, 0, 0).unwrap();
        let src_ino = src_fd.ino();
        let dst_fd = mocks.val.0.symlink(&dstdir_fd, &dst, 0o642, 0, 0,
                                          &linktarget)
        .unwrap();

        assert_eq!(src_fd.ino(),
            mocks.val.0.rename(&srcdir_fd, &src_fd, &src, &dstdir_fd,
                Some(dst_fd.ino()), &dst).unwrap()
        );

        mocks.val.0.inactive(src_fd);
        assert_eq!(src_ino,
            mocks.val.0.lookup(Some(&root), &dstdir_fd, &dst).unwrap().ino()
        );
        assert_eq!(libc::ENOENT,
            mocks.val.0.lookup(Some(&root), &srcdir_fd, &src).unwrap_err()
        );
        let srcdir_inode = mocks.val.0.getattr(&srcdir_fd).unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = mocks.val.0.getattr(&dstdir_fd).unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
        let (de, _) = mocks.val.0.readdir(&dstdir_fd, 0)
            .filter(|r| {
                let dirent = r.unwrap().0;
                u64::from(dirent.d_fileno) == src_ino
            }).nth(0).unwrap().unwrap();
        assert_eq!(de.d_type, libc::DT_REG);
    }

    // Rename a file with extended attributes.
    test rename_reg_with_extattrs(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;

        let fd = mocks.val.0.create(&root, &src, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, ns, &name, &value[..]).unwrap();

        assert_eq!(fd.ino(),
            mocks.val.0.rename(&root, &fd, &src, &root, None, &dst)
            .unwrap()
        );

        mocks.val.0.inactive(fd);
        let new_fd = mocks.val.0.lookup(None, &root, &dst).unwrap();
        let v = mocks.val.0.getextattr(&new_fd, ns, &name).unwrap();
        assert_eq!(&v[..], &value);
    }

    // rename updates a file's parent directories' ctime and mtime
    test rename_parent_timestamps(mocks) {
        let root = mocks.val.0.root();
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_fd = mocks.val.0.mkdir(&root, &srcdir, 0o755, 0, 0)
        .unwrap();
        let dstdir_fd = mocks.val.0.mkdir(&root, &dstdir, 0o755, 0, 0)
        .unwrap();
        let fd = mocks.val.0.create(&srcdir_fd, &src, 0o644, 0, 0)
        .unwrap();
        clear_timestamps(&mocks.val.0, &srcdir_fd);
        clear_timestamps(&mocks.val.0, &dstdir_fd);
        clear_timestamps(&mocks.val.0, &fd);

        assert_eq!(fd.ino(),
            mocks.val.0.rename(&srcdir_fd, &fd, &src, &dstdir_fd, None,
                &dst).unwrap()
        );

        // Timestamps should've been updated for parent directories, but not for
        // the file itself
        assert_ts_changed(&mocks.val.0, &srcdir_fd, false, true, true, false);
        assert_ts_changed(&mocks.val.0, &dstdir_fd, false, true, true, false);
        assert_ts_changed(&mocks.val.0, &fd, false, false, false, false);
    }

    #[allow(clippy::block_in_if_condition_stmt)]
    test rmdir(mocks) {
        let root = mocks.val.0.root();
        let dirname = OsString::from("x");
        let fd = mocks.val.0.mkdir(&root, &dirname, 0o755, 0, 0)
        .unwrap();
        mocks.val.0.rmdir(&root, &dirname).unwrap();

        // Make sure it's gone
        assert_eq!(mocks.val.0.getattr(&fd).unwrap_err(), libc::ENOENT);
        assert!(mocks.val.0.readdir(&root, 0)
            .filter(|r| {
                let dirent = r.unwrap().0;
                dirent.d_name[0] == 'x' as i8
            }).nth(0).is_none());

        // Make sure the parent dir's refcount dropped
        let inode = mocks.val.0.getattr(&root).unwrap();
        assert_eq!(inode.nlink, 1);
    }

    /// Remove a directory whose name has a hash collision
    test rmdir_collision(mocks) {
        let root = mocks.val.0.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = mocks.val.0.mkdir(&root, &filename0, 0o755, 0, 0).unwrap();
        let _fd1 = mocks.val.0.mkdir(&root, &filename1, 0o755, 0, 0).unwrap();
        mocks.val.0.rmdir(&root, &filename1).unwrap();

        assert_eq!(mocks.val.0.lookup(None, &root, &filename0).unwrap().ino(),
            fd0.ino());
        assert_eq!(mocks.val.0.lookup(None, &root, &filename1).unwrap_err(),
            libc::ENOENT);
    }

    test rmdir_enoent(mocks) {
        let root = mocks.val.0.root();
        let dirname = OsString::from("x");
        assert_eq!(mocks.val.0.rmdir(&root, &dirname).unwrap_err(),
            libc::ENOENT);
    }

    #[should_panic]
    test rmdir_enotdir(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        mocks.val.0.create(&root, &filename, 0o644, 0, 0)
            .unwrap();
        mocks.val.0.rmdir(&root, &filename).unwrap();
    }

    test rmdir_enotempty(mocks) {
        let root = mocks.val.0.root();
        let dirname = OsString::from("x");
        let fd = mocks.val.0.mkdir(&root, &dirname, 0o755, 0, 0)
        .unwrap();
        mocks.val.0.mkdir(&fd, &dirname, 0o755, 0, 0)
        .unwrap();
        assert_eq!(mocks.val.0.rmdir(&root, &dirname).unwrap_err(),
            libc::ENOTEMPTY);
    }

    /// Try to remove a directory that isn't empty, and that has a hash
    /// collision with another file or directory
    test rmdir_enotempty_collision(mocks) {
        let root = mocks.val.0.root();
        let filename0 = OsString::from("basedir");
        let filename1 = OsString::from("HsxUh682JQ");
        let filename2 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename1, &filename2);
        let fd0 = mocks.val.0.mkdir(&root, &filename0, 0o755, 0, 0).unwrap();
        let _fd1 = mocks.val.0.mkdir(&fd0, &filename1, 0o755, 0, 0).unwrap();
        let _fd2 = mocks.val.0.mkdir(&fd0, &filename2, 0o755, 0, 0).unwrap();
        assert_eq!(mocks.val.0.rmdir(&root, &filename0).unwrap_err(),
         libc::ENOTEMPTY);
    }

    /// Remove a directory with an extended attribute
    test rmdir_extattr(mocks) {
        let root = mocks.val.0.root();
        let dirname = OsString::from("x");
        let xname = OsString::from("foo");
        let xvalue1 = [0u8, 1, 2];
        let ns = ExtAttrNamespace::User;
        let fd = mocks.val.0.mkdir(&root, &dirname, 0o755, 0, 0)
        .unwrap();
        mocks.val.0.setextattr(&fd, ns, &xname, &xvalue1[..]).unwrap();
        mocks.val.0.rmdir(&root, &dirname).unwrap();

        // Make sure the xattr is gone.  As I read things, POSIX allows us to
        // return either ENOATTR or ENOENT in this case.
        assert_eq!(mocks.val.0.getextattr(&fd, ns, &xname).unwrap_err(),
                   libc::ENOATTR);
    }

    /// Removing a directory should update its parent's timestamps
    test rmdir_timestamps(mocks) {
        let root = mocks.val.0.root();
        let dirname = OsString::from("x");
        mocks.val.0.mkdir(&root, &dirname, 0o755, 0, 0).unwrap();
        clear_timestamps(&mocks.val.0, &root);

        mocks.val.0.rmdir(&root, &dirname).unwrap();

        // Timestamps should've been updated
        assert_ts_changed(&mocks.val.0, &root, false, true, true, false);
    }

    test setattr(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0)
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
        mocks.val.0.setattr(&fd, attr).unwrap();
        let attr = mocks.val.0.getattr(&fd).unwrap();
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
        mocks.val.0.setattr(&fd, attr).unwrap();
        let attr = mocks.val.0.getattr(&fd).unwrap();
        assert(attr);
    }

    // setattr updates a file's ctime and mtime
    test setattr_timestamps(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        clear_timestamps(&mocks.val.0, &fd);

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
        mocks.val.0.setattr(&fd, attr).unwrap();

        // Timestamps should've been updated
        assert_ts_changed(&mocks.val.0, &fd, false, false, true, false);
    }

    // truncating a file should delete data past the truncation point
    test setattr_truncate(mocks) {
        let root = mocks.val.0.root();
        // First write two records
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let buf = vec![42u8; 8192];
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(8192), r);

        // Then truncate one of them.
        let mut attr = SetAttr::default();
        attr.size = Some(4096);
        mocks.val.0.setattr(&fd, attr).unwrap();

        // Now extend the file past the truncated record
        attr.size = Some(8192);
        mocks.val.0.setattr(&fd, attr).unwrap();

        // Finally, read the truncated record.  It should be a hole
        let sglist = mocks.val.0.read(&fd, 4096, 4096).unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);
    }

    // Like setattr_truncate, but everything happens within a single record
    test setattr_truncate_partial_records(mocks) {
        let root = mocks.val.0.root();
        // First write one record
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let buf = vec![42u8; 4096];
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        // Then truncate it.
        let mut attr = SetAttr::default();
        attr.size = Some(1000);
        mocks.val.0.setattr(&fd, attr).unwrap();

        // Now extend the file past the truncated record
        attr.size = Some(4000);
        mocks.val.0.setattr(&fd, attr).unwrap();

        // Finally, read from the truncated area.  It should be a hole
        let sglist = mocks.val.0.read(&fd, 2000, 1000).unwrap();
        let db = &sglist[0];
        let expected = [0u8; 1000];
        assert_eq!(&db[..], &expected[..]);
    }

    // Like setattr_truncate_partial_record, but the record is a Blob
    test setattr_truncate_partial_blob_record(mocks) {
        let root = mocks.val.0.root();
        // First write one record
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let buf = vec![42u8; 4096];
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);
        mocks.val.0.sync(); // Create a blob record

        // Then truncate it.
        let mut attr = SetAttr::default();
        attr.size = Some(1000);
        mocks.val.0.setattr(&fd, attr).unwrap();

        // Now extend the file past the truncated record
        attr.size = Some(4000);
        mocks.val.0.setattr(&fd, attr).unwrap();

        // Finally, read from the truncated area.  It should be a hole
        let sglist = mocks.val.0.read(&fd, 2000, 1000).unwrap();
        let db = &sglist[0];
        let expected = [0u8; 1000];
        assert_eq!(&db[..], &expected[..]);
    }

    // truncating a file should update the mtime
    test setattr_truncate_updates_mtime(mocks) {
        let root = mocks.val.0.root();
        // Create a file
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        clear_timestamps(&mocks.val.0, &fd);

        // Then truncate the file
        let mut attr = SetAttr::default();
        attr.size = Some(4096);
        mocks.val.0.setattr(&fd, attr).unwrap();

        // mtime should've changed
        assert_ts_changed(&mocks.val.0, &fd, false, true, true, false);
    }

    /// Overwrite an existing extended attribute
    test setextattr_overwrite(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value1 = [0u8, 1, 2];
        let value2 = [3u8, 4, 5, 6];
        let ns = ExtAttrNamespace::User;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, ns, &name, &value1[..]).unwrap();
        mocks.val.0.setextattr(&fd, ns, &name, &value2[..]).unwrap();
        let v = mocks.val.0.getextattr(&fd, ns, &name).unwrap();
        assert_eq!(&v[..], &value2);
    }

    /// Overwrite an existing extended attribute that hash-collided with a
    /// different xattr
    test setextattr_collision_overwrite(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];
        let value1a = [4u8, 7, 8, 9, 10];

        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(&fd, ns1, &name1, &value1[..]).unwrap();
        mocks.val.0.setextattr(&fd, ns1, &name1, &value1a[..]).unwrap();
        let v0 = mocks.val.0.getextattr(&fd, ns0, &name0).unwrap();
        assert_eq!(&v0[..], &value0);
        let v1 = mocks.val.0.getextattr(&fd, ns1, &name1).unwrap();
        assert_eq!(&v1[..], &value1a);
    }

    /// setextattr(2) should not update any timestamps
    test setextattr_timestamps(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [0u8, 1, 2];
        let ns = ExtAttrNamespace::User;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        clear_timestamps(&mocks.val.0, &fd);

        mocks.val.0.setextattr(&fd, ns, &name, &value[..]).unwrap();
        assert_ts_changed(&mocks.val.0, &fd, false, false, false, false);
    }

    /// The file already has a blob extattr.  Set another extattr and flush them
    /// both.
    test setextattr_with_blob(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let value1 = vec![42u8; 4096];
        let name2 = OsString::from("bar");
        let value2 = [3u8, 4, 5, 6];
        let ns = ExtAttrNamespace::User;
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(&fd, ns, &name1, &value1[..]).unwrap();
        mocks.val.0.sync(); // Create a blob ExtAttr
        mocks.val.0.setextattr(&fd, ns, &name2, &value2[..]).unwrap();
        mocks.val.0.sync(); // Achieve coverage of BlobExtAttr::flush

        let v = mocks.val.0.getextattr(&fd, ns, &name1).unwrap();
        assert_eq!(&v[..], &value1[..]);
    }

    test statvfs(mocks) {
        let statvfs = mocks.val.0.statvfs().unwrap();
        assert_eq!(statvfs.f_blocks, 262_144);
        assert_eq!(statvfs.f_bsize, 4096);
        assert_eq!(statvfs.f_frsize, 4096);
    }

    test statvfs_8k(mocks(vec![Property::RecordSize(13)])) {
        let statvfs = mocks.val.0.statvfs().unwrap();
        assert_eq!(statvfs.f_blocks, 262_144);
        assert_eq!(statvfs.f_bsize, 8192);
        assert_eq!(statvfs.f_frsize, 4096);
    }

    test symlink(mocks) {
        let root = mocks.val.0.root();
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let fd = mocks.val.0.symlink(&root, &srcname, 0o642, 0, 0, &dstname)
        .unwrap();
        assert_eq!(fd.ino(),
            mocks.val.0.lookup(None, &root, &srcname).unwrap().ino()
        );

        // The parent dir should have an "src" symlink entry
        let entries = mocks.val.0.readdir(&root, 0);
        let (dirent, _ofs) = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 's' as i8
        }).nth(0)
        .expect("'s' directory entry not found");
        assert_eq!(dirent.d_type, libc::DT_LNK);
        let dirent_name = unsafe{
            CStr::from_ptr(&dirent.d_name as *const c_char)
        };
        assert_eq!(dirent_name.to_str().unwrap(), srcname.to_str().unwrap());
        assert_eq!(u64::from(dirent.d_fileno), fd.ino());

        let attr = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(attr.mode.0, libc::S_IFLNK | 0o642);
    }

    /// symlink should update the parent dir's timestamps
    test symlink_timestamps(mocks) {
        let root = mocks.val.0.root();
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        clear_timestamps(&mocks.val.0, &root);

        mocks.val.0.symlink(&root, &srcname, 0o642, 0, 0, &dstname).unwrap();
        assert_ts_changed(&mocks.val.0, &root, false, true, true, false);
    }

    test unlink(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0)
        .unwrap();
        let r = mocks.val.0.unlink(&root, Some(&fd), &filename);
        assert_eq!(Ok(()), r);
        mocks.val.0.inactive(fd);

        // Check that the inode is gone
        let r = mocks.val.0.lookup(None, &root, &filename);
        assert_eq!(libc::ENOENT, r.unwrap_err(), "Dirent was not removed");

        // The parent dir should not have an "x" directory entry
        let entries = mocks.val.0.readdir(&root, 0);
        let x_de = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).nth(0);
        assert!(x_de.is_none(), "Directory entry was not removed");
    }

    // Access an opened but deleted file
    test unlink_but_opened(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        let r = mocks.val.0.unlink(&root, Some(&fd), &filename);
        assert_eq!(Ok(()), r);

        let attr = mocks.val.0.getattr(&fd).expect("Inode deleted too soon");
        assert_eq!(0, attr.nlink);

        mocks.val.0.inactive(fd);
    }

    // Access an open file that was deleted during a previous TXG
    test unlink_but_opened_across_txg(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        let r = mocks.val.0.unlink(&root, Some(&fd), &filename);
        assert_eq!(Ok(()), r);

        mocks.val.0.sync();

        let attr = mocks.val.0.getattr(&fd).expect("Inode deleted too soon");
        assert_eq!(0, attr.nlink);

        mocks.val.0.inactive(fd);
    }

    // Unlink a file that has a name collision with another file in the same
    // directory.
    test unlink_collision(mocks) {
        let root = mocks.val.0.root();
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let fd0 = mocks.val.0.create(&root, &filename0, 0o644, 0, 0).unwrap();
        let fd1 = mocks.val.0.create(&root, &filename1, 0o644, 0, 0).unwrap();

        mocks.val.0.unlink(&root, Some(&fd1), &filename1).unwrap();
        mocks.val.0.inactive(fd1);

        assert_eq!(mocks.val.0.lookup(None, &root, &filename0).unwrap().ino(),
            fd0.ino());
        assert_eq!(mocks.val.0.lookup(None, &root, &filename1).unwrap_err(),
            libc::ENOENT);
    }

    // When unlinking a multiply linked file, its ctime should be updated
    test unlink_ctime(mocks) {
        let root = mocks.val.0.root();
        let name1 = OsString::from("name1");
        let name2 = OsString::from("name2");
        let fd = mocks.val.0.create(&root, &name1, 0o644, 0, 0).unwrap();
        dbg!(&fd);
        mocks.val.0.link(&root, &fd, &name2).unwrap();
        clear_timestamps(&mocks.val.0, &fd);

        mocks.val.0.unlink(&root, Some(&fd), &name2).unwrap();
        assert_ts_changed(&mocks.val.0, &fd, false, false, true, false);
    }

    test unlink_enoent(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.unlink(&root, Some(&fd), &filename).unwrap();
        let e = mocks.val.0.unlink(&root, Some(&fd), &filename).unwrap_err();
        assert_eq!(e, libc::ENOENT);
    }

    // When unlinking a hardlink, the file should not be removed until its link
    // count reaches zero.
    test unlink_hardlink(mocks) {
        let root = mocks.val.0.root();
        let name1 = OsString::from("name1");
        let name2 = OsString::from("name2");
        let fd = mocks.val.0.create(&root, &name1, 0o644, 0, 0).unwrap();
        mocks.val.0.link(&root, &fd, &name2).unwrap();

        mocks.val.0.unlink(&root, Some(&fd), &name1).unwrap();
        // File should still exist, now with link count 1.
        let attr = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(attr.nlink, 1);
        assert_eq!(mocks.val.0.lookup(None, &root, &name1).unwrap_err(),
            libc::ENOENT);

        // Even if we drop the file data, the inode should not be deleted,
        // because it has nlink 1
        mocks.val.0.inactive(fd);
        let fd = mocks.val.0.lookup(None, &root, &name2).unwrap();
        let attr = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(attr.nlink, 1);

        // A second unlink should remove the file
        mocks.val.0.unlink(&root, Some(&fd), &name2).unwrap();
        mocks.val.0.inactive(fd);

        // File should actually be gone now
        assert_eq!(mocks.val.0.lookup(None, &root, &name1).unwrap_err(),
            libc::ENOENT);
        assert_eq!(mocks.val.0.lookup(None, &root, &name2).unwrap_err(),
            libc::ENOENT);
    }

    // Unlink should work on inactive vnodes
    test unlink_inactive(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.inactive(fd);
        let r = mocks.val.0.unlink(&root, None, &filename);
        assert_eq!(Ok(()), r);

        // Check that the inode is gone
        let r = mocks.val.0.lookup(None, &root, &filename);
        assert_eq!(libc::ENOENT, r.expect_err("Inode was not removed"));

        // The parent dir should not have an "x" directory entry
        let entries = mocks.val.0.readdir(&root, 0);
        let x_de = entries
        .map(|r| r.unwrap())
        .filter(|(dirent, _ofs)| {
            dirent.d_name[0] == 'x' as i8
        }).nth(0);
        assert!(x_de.is_none(), "Directory entry was not removed");
    }

    /// unlink(2) should update the parent dir's timestamps
    test unlink_timestamps(mocks) {
        let root = mocks.val.0.root();
        let filename = OsString::from("x");
        let fd = mocks.val.0.create(&root, &filename, 0o644, 0, 0).unwrap();
        clear_timestamps(&mocks.val.0, &root);

        mocks.val.0.unlink(&root, Some(&fd), &filename).unwrap();
        mocks.val.0.inactive(fd);
        assert_ts_changed(&mocks.val.0, &root, false, true, true, false);
    }

    // A very simple single record write to an empty file
    test write(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let buf = vec![42u8; 4096];
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        // Check the file size
        let attr = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(attr.size, 4096);

        let sglist = mocks.val.0.read(&fd, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[..]);
    }

    // A partial single record write appended to the file's end
    test write_append(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf0 = vec![0u8; 1024];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf0[..], 0);
        assert_eq!(Ok(1024), r);

        let sglist = mocks.val.0.read(&fd, 0, 1024).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf0[..]);
    }

    // A partial record write appended to a partial record at file's end
    test write_append_to_partial_record(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
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
        let r = mocks.val.0.write(&fd, 0, &buf0[..], 0);
        assert_eq!(Ok(1024), r);
        let r = mocks.val.0.write(&fd, 1024, &buf1[..], 0);
        assert_eq!(Ok(1024), r);

        let sglist = mocks.val.0.read(&fd, 0, 2048).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[0..1024], &buf0[..]);
        assert_eq!(&db[1024..2048], &buf1[..]);
    }

    // write can RMW BlobExtents
    test write_partial_blob_record(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf0 = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf0[..], 0);
        assert_eq!(Ok(4096), r);

        // Sync the fs to flush the InlineExtent to a BlobExtent
        mocks.val.0.sync();

        let buf1 = vec![0u8; 2048];
        let r = mocks.val.0.write(&fd, 512, &buf1[..], 0);
        assert_eq!(Ok(2048), r);

        let sglist = mocks.val.0.read(&fd, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[0..512], &buf0[0..512]);
        assert_eq!(&db[512..2560], &buf1[..]);
        assert_eq!(&db[2560..], &buf0[2560..]);
    }

    // Partially fill a hole that's at neither the beginning nor the end of the
    // file
    test write_partial_hole(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut attr = SetAttr::default();
        attr.size = Some(4096 * 4);
        mocks.val.0.setattr(&fd, attr).unwrap();

        let mut buf0 = vec![0u8; 2048];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 9216, &buf0[..], 0);
        assert_eq!(Ok(2048), r);

        let sglist = mocks.val.0.read(&fd, 9216, 2048).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf0[..]);
    }

    // A partial single record write that needs RMW on both ends
    test write_partial_record(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf0 = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf0 {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf0[..], 0);
        assert_eq!(Ok(4096), r);
        let buf1 = vec![0u8; 2048];
        let r = mocks.val.0.write(&fd, 512, &buf1[..], 0);
        assert_eq!(Ok(2048), r);

        let sglist = mocks.val.0.read(&fd, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[0..512], &buf0[0..512]);
        assert_eq!(&db[512..2560], &buf1[..]);
        assert_eq!(&db[2560..], &buf0[2560..]);
    }

    // write updates a file's ctime and mtime
    test write_timestamps(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        clear_timestamps(&mocks.val.0, &fd);

        let buf = vec![42u8; 4096];
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        // Timestamps should've been updated
        assert_ts_changed(&mocks.val.0, &fd, false, true, true, false);
    }

    // A write to an empty file that's split across two records
    test write_two_recs(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 8192];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(8192), r);

        // Check the file size
        let inode = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(inode.size, 8192);

        let sglist = mocks.val.0.read(&fd, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[0..4096]);
        let sglist = mocks.val.0.read(&fd, 4096, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[4096..8192]);
    }

    // A write to an empty file that's split across three records
    test write_three_recs(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 12288];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(12288), r);

        // Check the file size
        let inode = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(inode.size, 12288);

        let sglist = mocks.val.0.read(&fd, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[0..4096]);
        let sglist = mocks.val.0.read(&fd, 4096, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[4096..8192]);
        let sglist = mocks.val.0.read(&fd, 8192, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[8192..12288]);
    }

    // Write one hold record and a partial one to an initially empty file.
    test write_one_and_a_half_records(mocks) {
        let root = mocks.val.0.root();
        let fd = mocks.val.0.create(&root, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 6144];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(&fd, 0, &buf[..], 0);
        assert_eq!(Ok(6144), r);

        // Check the file size
        let inode = mocks.val.0.getattr(&fd).unwrap();
        assert_eq!(inode.size, 6144);

        let sglist = mocks.val.0.read(&fd, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[0..4096]);
        let sglist = mocks.val.0.read(&fd, 4096, 2048).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[4096..6144]);
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
    use galvanic_test::*;
    use log::*;
    use pretty_assertions::assert_eq;
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
    use tempfile::Builder;
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
            let fd = self.fs.mkdir(&self.root, &OsString::from(&fname), 0o755,
                0, 0)
            .unwrap();
            self.dirs.push((num, fd));
        }

        fn ls(&mut self) {
            let idx = self.rng.gen_range(0, self.dirs.len() + 1);
            let (fname, fd) = if idx == self.dirs.len() {
                ("/".to_owned(), &self.root)
            } else {
                let spec = &self.dirs[idx];
                (format!("{:x}", spec.0), &spec.1)
            };
            let c = self.fs.readdir(&fd, 0).count();
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
                let idx = self.rng.gen_range(0, self.files.len());
                let fd = &self.files[idx].1;
                // Pick a random offset within the first 8KB
                let ofs = 2048 * self.rng.gen_range(0, 4);
                info!("read {:x} at offset {}", self.files[idx].0, ofs);
                let r = self.fs.read(&fd, ofs, 2048);
                // TODO: check buffer contents
                assert!(r.is_ok());
            }
        }

        fn rm_enoent(&mut self) {
            // Generate a random name that corresponds to no real file, but
            // could be sorted anywhere amongst them.
            let num: u64 = self.rng.gen();
            let fname = format!("{:x}_x", num);
            let fd = FileData::new_for_tests(Some(1), num);
            info!("rm {}", fname);
            assert_eq!(self.fs.unlink(&self.root, Some(&fd),
                                      &OsString::from(&fname)),
                       Err(Error::ENOENT.into()));
        }

        fn rm(&mut self) {
            if !self.files.is_empty() {
                let idx = self.rng.gen_range(0, self.files.len());
                let (basename, fd) = self.files.remove(idx);
                let fname = format!("{:x}", basename);
                info!("rm {}", fname);
                self.fs.unlink(&self.root, Some(&fd), &OsString::from(&fname))
                    .unwrap();
            }
        }

        fn rmdir(&mut self) {
            if !self.dirs.is_empty() {
                let idx = self.rng.gen_range(0, self.dirs.len());
                let fname = format!("{:x}", self.dirs.remove(idx).0);
                info!("rmdir {}", fname);
                self.fs.rmdir(&self.root, &OsString::from(&fname)).unwrap();
            }
        }

        fn shutdown(mut self) {
            self.fs.inactive(self.root);
            drop(self.fs);
            let mut db = Arc::try_unwrap(self.db.take().unwrap())
                .ok().expect("Arc::try_unwrap");
            let mut rt = self.rt.take().unwrap();
            rt.block_on(db.shutdown()).unwrap();
            rt.shutdown_on_idle();
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
            self.fs.sync();
        }

        fn touch(&mut self) {
            // The BTree is basically a flat namespace, so there's little test
            // coverage to be gained by testing a hierarchical directory
            // structure.  Instead, we'll stick all files in the root directory,
            let num: u64 = self.rng.gen();
            let fname = format!("{:x}", num);
            info!("Touch {}", fname);
            let fd = self.fs.create(&self.root, &OsString::from(&fname), 0o644,
                0, 0)
            .unwrap();
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
                let idx = self.rng.gen_range(0, self.files.len());
                let fd = &self.files[idx].1;
                // Pick a random offset within the first 8KB
                let piece: u64 = self.rng.gen_range(0, 4);
                let ofs = 2048 * piece;
                // Use a predictable fill value
                let fill = (fd.ino().wrapping_mul(piece) %
                            u64::from(u8::max_value()))
                    as u8;
                let buf = [fill; 2048];
                info!("write {:x} at offset {}", self.files[idx].0, ofs);
                let r = self.fs.write(&fd, ofs, &buf[..], 0);
                assert!(r.is_ok());
            }
        }
    }

    fixture!( mocks(seed: Option<[u8; 16]>, freqs: Option<Vec<(Op, f64)>>,
                    zone_size: u64) -> TortureTest
    {
        setup(&mut self) {
            static ENV_LOGGER: Once = Once::new();
            ENV_LOGGER.call_once(|| {
                env_logger::init();
            });

            let mut rt = Runtime::new();
            let handle = rt.handle().clone();
            let len = 1 << 30;  // 1GB
            let tempdir = t!(Builder::new().prefix("test_fs").tempdir());
            let filename = tempdir.path().join("vdev");
            let file = t!(fs::File::create(&filename));
            t!(file.set_len(len));
            drop(file);
            let zone_size = NonZeroU64::new(*self.zone_size);
            let db = rt.block_on(future::lazy(move || {
                Pool::create_cluster(None, 1, zone_size, 0, &[filename])
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
            let handle = rt.handle().clone();
            let (db, fs) = rt.block_on(future::lazy(move || {
                db.new_fs(Vec::new())
                .map(move |tree_id| {
                    let fs = Fs::new(db.clone(), handle, tree_id);
                    (db, fs)
                })
            })).unwrap();
            let seed = self.seed.unwrap_or_else(|| {
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
