// vim: tw=80
use galvanic_test::*;
use log::*;

mod fs_dump_output;

/// Constructs a real filesystem and tests the common FS routines, without
/// mounting
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
    use tempdir::TempDir;
    use time::Timespec;
    use tokio_io_pool::Runtime;

    fixture!( mocks(props: Vec<Property>) -> (Fs, Runtime) {
        params { vec![Vec::new()].into_iter() }

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
                        Arc::new(Database::create(Arc::new(idml), handle))
                    })
                })
            })).unwrap();
            let handle = rt.handle().clone();
            let props = self.props.clone();
            let fs = rt.block_on(future::lazy(move || {
                db.new_fs(props)
                .map(move |tree_id| {
                    Fs::new(db.clone(), handle, tree_id)
                })
            })).unwrap();
            (fs, rt)
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
    fn assert_ts_changed(ds: &Fs, ino: u64, atime: bool, mtime: bool,
                         ctime: bool, birthtime: bool)
    {
        let attr = ds.getattr(ino).unwrap();
        let ts0 = Timespec{sec: 0, nsec: 0};
        assert!(atime ^ (attr.atime == ts0));
        assert!(mtime ^ (attr.mtime == ts0));
        assert!(ctime ^ (attr.ctime == ts0));
        assert!(birthtime ^ (attr.birthtime == ts0));
    }

    fn clear_timestamps(ds: &Fs, ino: u64) {
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
        ds.setattr(ino, attr).unwrap();
    }

    test create(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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

    /// Creating a file that already exists should panic.  It is the
    /// responsibility of the VFS to prevent this error when you call
    /// open(_, O_CREAT)
    #[should_panic]
    test create_eexist(mocks) {
        let filename = OsString::from("x");
        let _ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
    }

    /// Create should update the parent dir's timestamps
    test create_timestamps(mocks) {
        clear_timestamps(&mocks.val.0, 1);

        mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0).unwrap();
        assert_ts_changed(&mocks.val.0, 1, false, true, true, false);
    }

    test deleteextattr(mocks) {
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, ns, &name, &value[..]).unwrap();
        mocks.val.0.deleteextattr(ino, ns, &name).unwrap();
        assert_eq!(mocks.val.0.getextattr(ino, ns, &name), Err(libc::ENOATTR));
    }

    /// deleteextattr with a hash collision.
    test deleteextattr_collision(mocks) {
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();

        // First try deleting the attributes in order
        mocks.val.0.setextattr(ino, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(ino, ns1, &name1, &value1[..]).unwrap();
        mocks.val.0.deleteextattr(ino, ns0, &name0).unwrap();
        assert!(mocks.val.0.getextattr(ino, ns0, &name0).is_err());
        assert!(mocks.val.0.getextattr(ino, ns1, &name1).is_ok());
        mocks.val.0.deleteextattr(ino, ns1, &name1).unwrap();
        assert!(mocks.val.0.getextattr(ino, ns0, &name0).is_err());
        assert!(mocks.val.0.getextattr(ino, ns1, &name1).is_err());

        // Repeat, this time out-of-order
        mocks.val.0.setextattr(ino, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(ino, ns1, &name1, &value1[..]).unwrap();
        mocks.val.0.deleteextattr(ino, ns1, &name1).unwrap();
        assert!(mocks.val.0.getextattr(ino, ns0, &name0).is_ok());
        assert!(mocks.val.0.getextattr(ino, ns1, &name1).is_err());
        mocks.val.0.deleteextattr(ino, ns0, &name0).unwrap();
        assert!(mocks.val.0.getextattr(ino, ns0, &name0).is_err());
        assert!(mocks.val.0.getextattr(ino, ns1, &name1).is_err());
    }

    /// deleteextattr of a nonexistent attribute that hash-collides with an
    /// existing one.
    test deleteextattr_collision_enoattr(mocks) {
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];

        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();

        mocks.val.0.setextattr(ino, ns0, &name0, &value0[..]).unwrap();

        assert_eq!(mocks.val.0.deleteextattr(ino, ns1, &name1),
                   Err(libc::ENOATTR));
        assert!(mocks.val.0.getextattr(ino, ns0, &name0).is_ok());
    }

    test deleteextattr_enoattr(mocks) {
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let ns = ExtAttrNamespace::User;
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        assert_eq!(mocks.val.0.deleteextattr(ino, ns, &name),
                   Err(libc::ENOATTR));
    }

    /// rmextattr(2) should not modify any timestamps
    test deleteextattr_timestamps(mocks) {
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, ns, &name, &value[..]).unwrap();
        clear_timestamps(&mocks.val.0, ino);

        mocks.val.0.deleteextattr(ino, ns, &name).unwrap();
        assert_ts_changed(&mocks.val.0, ino, false, false, false, false);
    }

    // Dumps an FS tree, with enough data to create IntNodes
    test dump(mocks) {
        // First create enough directories to split the root LeafNode
        let inos = (0..31).map(|i| {
            let uid = 2 * i + 1;
            let gid = 2 * i + 2;
            let filename = OsString::from(format!("{}", i));
            mocks.val.0.mkdir(1, &filename, 0o755, uid, gid).unwrap()
        }).collect::<Vec<_>>();

        // Clear the timestamps so the dump will be reproducible
        for ino in inos.iter() {
            clear_timestamps(&mocks.val.0, *ino);
        }

        // Then delete some directories to reduce the size of the dump.  But
        // don't delete so many that the LeafNodes merge
        for i in 0..8 {
            let filename = OsString::from(format!("{}", i));
            mocks.val.0.rmdir(1, &filename).unwrap()
        }

        // Clear the root's timestamp
        clear_timestamps(&mocks.val.0, 1);
        mocks.val.0.sync();

        let mut buf = Vec::with_capacity(1024);
        mocks.val.0.dump(&mut buf).unwrap();
        let fs_tree = String::from_utf8(buf).unwrap();
        // Use std::assert_eq! instead of pretty_assertions::assert_eq because
        // the latter is too slow on a failure when the string is this large.
        std::assert_eq!(fs_dump_output::EXPECTED, fs_tree);
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
        assert_eq!(attr.mode.perm(), 0o755);
        assert_eq!(attr.mode.file_type(), libc::S_IFDIR);
    }

    test getextattr(mocks) {
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let namespace = ExtAttrNamespace::User;
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, namespace, &name, &value[..]).unwrap();
        assert_eq!(mocks.val.0.getextattrlen(ino, namespace, &name).unwrap(),
                   3);
        let v = mocks.val.0.getextattr(ino, namespace, &name).unwrap();
        assert_eq!(&v[..], &value);
    }

    /// Read a large extattr as a blob
    test getextattr_blob(mocks) {
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = vec![42u8; 4096];
        let namespace = ExtAttrNamespace::User;
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, namespace, &name, &value[..]).unwrap();

        // Sync the filesystem to flush the InlineExtent to a BlobExtent
        mocks.val.0.sync();

        assert_eq!(mocks.val.0.getextattrlen(ino, namespace, &name).unwrap(),
                   4096);
        let v = mocks.val.0.getextattr(ino, namespace, &name).unwrap();
        assert_eq!(&v[..], &value[..]);
    }

    /// A collision between a blob extattr and an inline one.  Get the blob
    /// extattr.
    test getextattr_blob_collision(mocks) {
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = vec![42u8; 4096];

        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();

        mocks.val.0.setextattr(ino, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(ino, ns1, &name1, &value1[..]).unwrap();
        mocks.val.0.sync(); // Flush the large xattr into a blob
        assert_eq!(mocks.val.0.getextattrlen(ino, ns1, &name1).unwrap(), 4096);
        let v1 = mocks.val.0.getextattr(ino, ns1, &name1).unwrap();
        assert_eq!(&v1[..], &value1[..]);
    }

    /// setextattr and getextattr with a hash collision.
    test getextattr_collision(mocks) {
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();

        mocks.val.0.setextattr(ino, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(ino, ns1, &name1, &value1[..]).unwrap();
        assert_eq!(mocks.val.0.getextattrlen(ino, ns0, &name0).unwrap(), 3);
        let v0 = mocks.val.0.getextattr(ino, ns0, &name0).unwrap();
        assert_eq!(&v0[..], &value0);
        assert_eq!(mocks.val.0.getextattrlen(ino, ns1, &name1).unwrap(), 4);
        let v1 = mocks.val.0.getextattr(ino, ns1, &name1).unwrap();
        assert_eq!(&v1[..], &value1);
    }

    // The same attribute name exists in two namespaces
    test getextattr_dual_namespaces(mocks) {
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value1 = [1u8, 2, 3];
        let value2 = [4u8, 5, 6, 7];
        let ns1 = ExtAttrNamespace::User;
        let ns2 = ExtAttrNamespace::System;
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, ns1, &name, &value1[..]).unwrap();
        mocks.val.0.setextattr(ino, ns2, &name, &value2[..]).unwrap();

        assert_eq!(mocks.val.0.getextattrlen(ino, ns1, &name).unwrap(), 3);
        let v1 = mocks.val.0.getextattr(ino, ns1, &name).unwrap();
        assert_eq!(&v1[..], &value1);

        assert_eq!(mocks.val.0.getextattrlen(ino, ns2, &name).unwrap(), 4);
        let v2 = mocks.val.0.getextattr(ino, ns2, &name).unwrap();
        assert_eq!(&v2[..], &value2);
    }

    // The file exists, but its extended attribute does not
    test getextattr_enoattr(mocks) {
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let namespace = ExtAttrNamespace::User;
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        assert_eq!(mocks.val.0.getextattrlen(ino, namespace, &name),
                   Err(libc::ENOATTR));
        assert_eq!(mocks.val.0.getextattr(ino, namespace, &name),
                   Err(libc::ENOATTR));
    }

    // The file does not exist.  Fortunately, VOP_GETEXTATTR(9) does not require
    // us to distinguish this from the ENOATTR case.
    test getextattr_enoent(mocks) {
        let name = OsString::from("foo");
        let namespace = ExtAttrNamespace::User;
        let ino = 9999;
        assert_eq!(mocks.val.0.getextattrlen(ino, namespace, &name),
                   Err(libc::ENOATTR));
        assert_eq!(mocks.val.0.getextattr(ino, namespace, &name),
                   Err(libc::ENOATTR));
    }

    /// getextattr(2) should not modify any timestamps
    test getextattr_timestamps(mocks) {
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let namespace = ExtAttrNamespace::User;
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, namespace, &name, &value[..]).unwrap();
        clear_timestamps(&mocks.val.0, ino);

        mocks.val.0.getextattr(ino, namespace, &name).unwrap();
        assert_ts_changed(&mocks.val.0, ino, false, false, false, false);
    }

    test link(mocks) {
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let ino = mocks.val.0.create(1, &src, 0o644, 0, 0).unwrap();
        let l_ino = mocks.val.0.link(1, ino, &dst).unwrap();
        assert_eq!(ino, l_ino);

        // The target's link count should've increased
        let attr = mocks.val.0.getattr(ino).unwrap();
        assert_eq!(attr.nlink, 2);

        // The parent should have a new directory entry
        assert_eq!(mocks.val.0.lookup(1, &dst).unwrap(), ino);
    }

    ///link(2) should update the parent's mtime and ctime
    test link_timestamps(mocks) {
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let ino = mocks.val.0.create(1, &src, 0o644, 0, 0).unwrap();
        clear_timestamps(&mocks.val.0, 1);

        mocks.val.0.link(1, ino, &dst).unwrap();
        assert_ts_changed(&mocks.val.0, 1, false, true, true, false);
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
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let name2 = OsString::from("bar");
        let ns = ExtAttrNamespace::User;
        let value = [0u8, 1, 2];
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, ns, &name1, &value[..]).unwrap();
        mocks.val.0.setextattr(ino, ns, &name2, &value[..]).unwrap();

        // expected has the form of <length as u8><value as [u8]>...
        // values are _not_ null terminated.
        // There is no requirement on the order of names
        let expected = b"\x03bar\x03foo";

        let lenf = self::listextattr_lenf(ns);
        let lsf = self::listextattr_lsf(ns);
        assert_eq!(mocks.val.0.listextattrlen(ino, lenf).unwrap(), 8);
        assert_eq!(&mocks.val.0.listextattr(ino, 64, lsf).unwrap()[..],
                   &expected[..]);
    }

    /// setextattr and listextattr with a cross-namespace hash collision.
    test listextattr_collision_separate_namespaces(mocks) {
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];

        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();

        mocks.val.0.setextattr(ino, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(ino, ns1, &name1, &value1[..]).unwrap();

        let expected0 = b"\x0aBWCdLQkApB";
        let lenf0 = self::listextattr_lenf(ns0);
        let lsf0 = self::listextattr_lsf(ns0);
        assert_eq!(mocks.val.0.listextattrlen(ino, lenf0).unwrap(), 11);
        assert_eq!(&mocks.val.0.listextattr(ino, 64, lsf0).unwrap()[..],
                   &expected0[..]);

        let expected1 = b"\x0aD6tLLI4mys";
        let lenf1 = self::listextattr_lenf(ns1);
        let lsf1 = self::listextattr_lsf(ns1);
        assert_eq!(mocks.val.0.listextattrlen(ino, lenf1).unwrap(), 11);
        assert_eq!(&mocks.val.0.listextattr(ino, 64, lsf1).unwrap()[..],
                   &expected1[..]);
    }

    test listextattr_dual_namespaces(mocks) {
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let name2 = OsString::from("bean");
        let ns1 = ExtAttrNamespace::User;
        let ns2 = ExtAttrNamespace::System;
        let value1 = [0u8, 1, 2];
        let value2 = [3u8, 4, 5];

        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, ns1, &name1, &value1[..]).unwrap();
        mocks.val.0.setextattr(ino, ns2, &name2, &value2[..]).unwrap();

        // Test queries for a single namespace
        let lenf = self::listextattr_lenf(ns1);
        let lsf = self::listextattr_lsf(ns1);
        assert_eq!(mocks.val.0.listextattrlen(ino, lenf), Ok(4));
        assert_eq!(&mocks.val.0.listextattr(ino, 64, lsf).unwrap()[..],
                   &b"\x03foo"[..]);
        let lenf = self::listextattr_lenf(ns2);
        let lsf = self::listextattr_lsf(ns2);
        assert_eq!(mocks.val.0.listextattrlen(ino, lenf), Ok(5));
        assert_eq!(&mocks.val.0.listextattr(ino, 64, lsf).unwrap()[..],
                   &b"\x04bean"[..]);
    }

    test listextattr_empty(mocks) {
        let filename = OsString::from("x");
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        let lenf = self::listextattr_lenf(ExtAttrNamespace::User);
        let lsf = self::listextattr_lsf(ExtAttrNamespace::User);
        assert_eq!(mocks.val.0.listextattrlen(ino, lenf), Ok(0));
        assert!(mocks.val.0.listextattr(ino, 64, lsf).unwrap().is_empty());
    }

    /// Lookup of a directory entry that has a hash collision
    test lookup_collision(mocks) {
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let ino0 = mocks.val.0.create(1, &filename0, 0o644, 0, 0).unwrap();
        let ino1 = mocks.val.0.create(1, &filename1, 0o644, 0, 0).unwrap();

        assert_eq!(mocks.val.0.lookup(1, &filename0), Ok(ino0));
        assert_eq!(mocks.val.0.lookup(1, &filename1), Ok(ino1));
    }

    test lookup_enoent(mocks) {
        let filename = OsString::from("nonexistent");
        assert_eq!(mocks.val.0.lookup(1, &filename), Err(libc::ENOENT));
    }

    test mkdir(mocks) {
        let ino = mocks.val.0.mkdir(1, &OsString::from("x"), 0o755, 0, 0)
        .unwrap();
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

    /// mkdir creates two directories whose names have a hash collision
    // Note that it's practically impossible to find a collision for a specific
    // name, like "." or "..", so those cases won't have test coverage
    test mkdir_collision(mocks) {
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let ino0 = mocks.val.0.mkdir(1, &filename0, 0o755, 0, 0).unwrap();
        let ino1 = mocks.val.0.mkdir(1, &filename1, 0o755, 0, 0).unwrap();

        assert_eq!(mocks.val.0.lookup(1, &filename0), Ok(ino0));
        assert_eq!(mocks.val.0.lookup(1, &filename1), Ok(ino1));
    }

    /// mkdir(2) should update the parent dir's timestamps
    test mkdir_timestamps(mocks) {
        clear_timestamps(&mocks.val.0, 1);

        mocks.val.0.mkdir(1, &OsString::from("x"), 0o755, 0, 0).unwrap();
        assert_ts_changed(&mocks.val.0, 1, false, true, true, false);
    }

    test mkchar(mocks) {
        let ino = mocks.val.0.mkchar(1, &OsString::from("x"), 0o644, 0, 0, 42)
        .unwrap();
        let attr = mocks.val.0.getattr(ino).unwrap();
        assert_eq!(attr.mode.0, libc::S_IFCHR | 0o644);
        assert_eq!(attr.rdev, 42);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(1, 0, 0);
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
        assert_eq!(u64::from(dirent.d_fileno), ino);
    }

    /// mknod(2) should update the parent dir's timestamps
    test mkchar_timestamps(mocks) {
        clear_timestamps(&mocks.val.0, 1);

        mocks.val.0.mkchar(1, &OsString::from("x"), 0o644, 0, 0, 42).unwrap();
        assert_ts_changed(&mocks.val.0, 1, false, true, true, false);
    }

    test mkblock(mocks) {
        let ino = mocks.val.0.mkblock(1, &OsString::from("x"), 0o644, 0, 0, 42)
        .unwrap();
        let attr = mocks.val.0.getattr(ino).unwrap();
        assert_eq!(attr.mode.0, libc::S_IFBLK | 0o644);
        assert_eq!(attr.rdev, 42);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(1, 0, 0);
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
        assert_eq!(u64::from(dirent.d_fileno), ino);
    }

    /// mknod(2) should update the parent dir's timestamps
    test mkblock_timestamps(mocks) {
        clear_timestamps(&mocks.val.0, 1);

        mocks.val.0.mkblock(1, &OsString::from("x"), 0o644, 0, 0, 42).unwrap();
        assert_ts_changed(&mocks.val.0, 1, false, true, true, false);
    }

    test mkfifo(mocks) {
        let ino = mocks.val.0.mkfifo(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let attr = mocks.val.0.getattr(ino).unwrap();
        assert_eq!(attr.mode.0, libc::S_IFIFO | 0o644);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(1, 0, 0);
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
        assert_eq!(u64::from(dirent.d_fileno), ino);
    }

    /// mkfifo(2) should update the parent dir's timestamps
    test mkfifo_timestamps(mocks) {
        clear_timestamps(&mocks.val.0, 1);

        mocks.val.0.mkfifo(1, &OsString::from("x"), 0o644, 0, 0).unwrap();
        assert_ts_changed(&mocks.val.0, 1, false, true, true, false);
    }

    test mksock(mocks) {
        let ino = mocks.val.0.mksock(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let attr = mocks.val.0.getattr(ino).unwrap();
        assert_eq!(attr.mode.0, libc::S_IFSOCK | 0o644);

        // The parent dir should have an "x" directory entry
        let entries = mocks.val.0.readdir(1, 0, 0);
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
        assert_eq!(u64::from(dirent.d_fileno), ino);
    }

    /// mksock(2) should update the parent dir's timestamps
    test mksock_timestamps(mocks) {
        clear_timestamps(&mocks.val.0, 1);

        mocks.val.0.mkfifo(1, &OsString::from("x"), 0o644, 0, 0).unwrap();
        assert_ts_changed(&mocks.val.0, 1, false, true, true, false);
    }

    // Read a hole that's bigger than the zero region
    test read_big_hole(mocks) {
        let holesize = 2 * ZERO_REGION_LEN;
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, holesize as u64, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        let sglist = mocks.val.0.read(ino, 0, holesize).unwrap();
        let expected = vec![0u8; ZERO_REGION_LEN];
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &expected[..]);
        assert_eq!(&sglist[1][..], &expected[..]);
    }

    // Read a single BlobExtent record
    test read_blob(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let sglist = mocks.val.0.read(ino, 0, 1024).unwrap();
        assert!(sglist.is_empty());
    }

    test read_empty_file_past_start(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let sglist = mocks.val.0.read(ino, 2048, 2048).unwrap();
        assert!(sglist.is_empty());
    }

    // Read a hole within a sparse file
    test read_hole(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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
        let sglist = mocks.val.0.read(ino, 3072, 1024).unwrap();
        let db = &sglist[0];
        let expected = [0u8; 1024];
        assert_eq!(&db[..], &expected[..]);
    }

    // Read a chunk of a file that includes a partial hole at the beginning and
    // data at the end.
    test read_partial_hole_trailing_edge(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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
        let sglist = mocks.val.0.read(ino, 3072, 2048).unwrap();
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &[0u8; 1024][..]);
        assert_eq!(&sglist[1][..], &buf[0..1024]);
    }

    // A read that's smaller than a record, at both ends
    test read_partial_record(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        let sglist = mocks.val.0.read(ino, 1024, 2048).unwrap();
        let db = &sglist[0];
        assert_eq!(db.len(), 2048);
        assert_eq!(&db[..], &buf[1024..3072]);
    }

    test read_past_eof(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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

    /// A read that spans 3 records, where the middle record is a hole
    test read_spans_hole(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 4096];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        assert_eq!(4096, mocks.val.0.write(ino, 0, &buf[..], 0).unwrap());
        assert_eq!(4096, mocks.val.0.write(ino, 8192, &buf[..], 0).unwrap());

        let sglist = mocks.val.0.read(ino, 0, 12288).unwrap();
        assert_eq!(sglist.len(), 3);
        assert_eq!(&sglist[0][..], &buf[..]);
        assert_eq!(&sglist[1][..], &[0u8; 4096][..]);
        assert_eq!(&sglist[2][..], &buf[..]);
    }

    /// read(2) should update the file's atime
    test read_timestamps(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let buf = vec![42u8; 4096];
        mocks.val.0.write(ino, 0, &buf[..], 0).unwrap();
        clear_timestamps(&mocks.val.0, ino);

        mocks.val.0.read(ino, 0, 4096).unwrap();
        assert_ts_changed(&mocks.val.0, ino, true, false, false, false);
    }

    // When atime is disabled, reading a file should not update its atime.
    test read_timestamps_no_atime(mocks(vec![Property::Atime(false)])) {
        let (fs, _rt) = mocks.val;

        let ino = fs.create(1, &OsString::from("x"), 0o644, 0, 0).unwrap();
        let buf = vec![42u8; 4096];
        fs.write(ino, 0, &buf[..], 0).unwrap();
        clear_timestamps(&fs, ino);

        fs.read(ino, 0, 4096).unwrap();
        assert_ts_changed(&fs, ino, false, false, false, false);
    }

    // A read that's split across two records
    test read_two_recs(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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

    // Read past EOF, in an entirely different record
    test read_well_past_eof(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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

    // Readdir of a directory with a hash collision
    test readdir_collision(mocks) {
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);

        mocks.val.0.create(1, &filename0, 0o644, 0, 0).unwrap();
        mocks.val.0.create(1, &filename1, 0o644, 0, 0).unwrap();

        // There's no requirement for the order of readdir's output.
        let mut expected = HashSet::new();
        expected.insert(OsString::from("."));
        expected.insert(OsString::from(".."));
        expected.insert(filename0.clone());
        expected.insert(filename1.clone());
        for result in mocks.val.0.readdir(1, 0, 0) {
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
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);

        let ino0 = mocks.val.0.create(1, &filename0, 0o644, 0, 0).unwrap();
        let _ino1 = mocks.val.0.create(1, &filename1, 0o644, 0, 0).unwrap();

        // There's no requirement for the order of readdir's output, but
        // filename0 happens to come first.
        let mut stream0 = mocks.val.0.readdir(1, 0, 0);
        let (result0, offset0) = stream0.next().unwrap().unwrap();
        assert_eq!(u64::from(result0.d_fileno), ino0);

        // Now interrupt the stream, and resume with the supplied offset.
        let mut expected = HashSet::new();
        expected.insert(OsString::from("."));
        expected.insert(OsString::from(".."));
        expected.insert(filename1.clone());
        drop(stream0);
        let stream1 = mocks.val.0.readdir(1, 0, offset0);
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
        let mut entries = mocks.val.0.readdir(1, 0, 0);
        let _ = entries.next().unwrap().unwrap();
    }

    /// readdir(2) should not update any timestamps
    test readdir_timestamps(mocks) {
        clear_timestamps(&mocks.val.0, 1);

        let mut entries = mocks.val.0.readdir(1, 0, 0);
        entries.next().unwrap().unwrap();
        entries.next().unwrap().unwrap();
        assert_ts_changed(&mocks.val.0, 1, false, false, false, false);
    }

    test readlink(mocks) {
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let ino = mocks.val.0.symlink(1, &srcname, 0o642, 0, 0, &dstname)
        .unwrap();
        assert_eq!(dstname, mocks.val.0.readlink(ino).unwrap());
    }

    // Calling readlink on a non-symlink should return EINVAL
    test readlink_einval(mocks) {
        assert_eq!(Err(libc::EINVAL), mocks.val.0.readlink(1));
    }

    test readlink_enoent(mocks) {
        assert_eq!(Err(libc::ENOENT), mocks.val.0.readlink(1000));
    }

    /// readlink(2) should not update any timestamps
    test readlink_timestamps(mocks) {
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let ino = mocks.val.0.symlink(1, &srcname, 0o642, 0, 0, &dstname)
        .unwrap();
        clear_timestamps(&mocks.val.0, ino);

        assert_eq!(dstname, mocks.val.0.readlink(ino).unwrap());
        assert_ts_changed(&mocks.val.0, ino, false, false, false, false);
    }

    // Rename a file that has a hash collision in both the source and
    // destination directories
    test rename_collision(mocks) {
        let src = OsString::from("F0jS2Tptj7");
        let src_c = OsString::from("PLe01T116a");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("Gg1AG3wll2");
        let dst_c = OsString::from("FDCIlvDxYn");
        let dstdir = OsString::from("dstdir");
        assert_dirents_collide(&src, &src_c);
        assert_dirents_collide(&dst, &dst_c);

        let srcdir_ino = mocks.val.0.mkdir(1, &srcdir, 0o755, 0, 0).unwrap();
        let dstdir_ino = mocks.val.0.mkdir(1, &dstdir, 0o755, 0, 0).unwrap();
        let src_c_ino = mocks.val.0.create(srcdir_ino, &src_c, 0o644, 0, 0)
        .unwrap();
        let dst_c_ino = mocks.val.0.create(dstdir_ino, &dst_c, 0o644, 0, 0)
        .unwrap();
        let src_ino = mocks.val.0.create(srcdir_ino, &src, 0o644, 0, 0)
        .unwrap();
        let dst_ino = mocks.val.0.create(dstdir_ino, &dst, 0o644, 0, 0)
        .unwrap();

        mocks.val.0.rename(srcdir_ino, &src, dstdir_ino, &dst).unwrap();

        assert_eq!(mocks.val.0.lookup(dstdir_ino, &dst), Ok(src_ino));
        assert_eq!(mocks.val.0.lookup(srcdir_ino, &src), Err(libc::ENOENT));
        let srcdir_inode = mocks.val.0.getattr(srcdir_ino).unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = mocks.val.0.getattr(dstdir_ino).unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
        assert_eq!(mocks.val.0.getattr(dst_ino), Err(libc::ENOENT));

        // Finally, make sure we didn't upset the colliding files
        assert_eq!(mocks.val.0.lookup(srcdir_ino, &src_c), Ok(src_c_ino));
        assert_eq!(mocks.val.0.lookup(dstdir_ino, &dst_c), Ok(dst_c_ino));
    }

    // Rename a directory.  The target is also a directory
    test rename_dir_to_dir(mocks) {
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_ino = mocks.val.0.mkdir(1, &srcdir, 0o755, 0, 0)
        .unwrap();
        let dstdir_ino = mocks.val.0.mkdir(1, &dstdir, 0o755, 0, 0)
        .unwrap();
        let src_ino = mocks.val.0.mkdir(srcdir_ino, &src, 0o755, 0, 0)
        .unwrap();
        let dst_ino = mocks.val.0.mkdir(dstdir_ino, &dst, 0o755, 0, 0)
        .unwrap();

        mocks.val.0.rename(srcdir_ino, &src, dstdir_ino, &dst).unwrap();

        assert_eq!(mocks.val.0.lookup(dstdir_ino, &dst), Ok(src_ino));
        assert_eq!(mocks.val.0.lookup(srcdir_ino, &src), Err(libc::ENOENT));
        let srcdir_inode = mocks.val.0.getattr(srcdir_ino).unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = mocks.val.0.getattr(dstdir_ino).unwrap();
        assert_eq!(dstdir_inode.nlink, 3);
        assert_eq!(mocks.val.0.getattr(dst_ino), Err(libc::ENOENT));
    }

    // Rename a directory.  The target is also a directory that isn't empty.
    // Nothing should change.
    test rename_dir_to_nonemptydir(mocks) {
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let dstf = OsString::from("dstf");
        let srcdir_ino = mocks.val.0.mkdir(1, &srcdir, 0o755, 0, 0)
        .unwrap();
        let dstdir_ino = mocks.val.0.mkdir(1, &dstdir, 0o755, 0, 0)
        .unwrap();
        let src_ino = mocks.val.0.mkdir(srcdir_ino, &src, 0o755, 0, 0)
        .unwrap();
        let dst_ino = mocks.val.0.mkdir(dstdir_ino, &dst, 0o755, 0, 0)
        .unwrap();
        let dstf_ino = mocks.val.0.create(dst_ino, &dstf, 0o644, 0, 0)
        .unwrap();

        assert_eq!(mocks.val.0.rename(srcdir_ino, &src, dstdir_ino, &dst),
                   Err(libc::ENOTEMPTY));

        assert_eq!(mocks.val.0.lookup(srcdir_ino, &src), Ok(src_ino));
        assert_eq!(mocks.val.0.lookup(dstdir_ino, &dst), Ok(dst_ino));
        assert_eq!(mocks.val.0.lookup(dst_ino, &dstf), Ok(dstf_ino));
    }

    // Rename a directory.  The target name does not exist
    test rename_dir_to_nothing(mocks) {
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_ino = mocks.val.0.mkdir(1, &srcdir, 0o755, 0, 0)
        .unwrap();
        let dstdir_ino = mocks.val.0.mkdir(1, &dstdir, 0o755, 0, 0)
        .unwrap();
        let ino = mocks.val.0.mkdir(srcdir_ino, &src, 0o755, 0, 0)
        .unwrap();

        mocks.val.0.rename(srcdir_ino, &src, dstdir_ino, &dst).unwrap();

        assert_eq!(mocks.val.0.lookup(dstdir_ino, &dst), Ok(ino));
        assert_eq!(mocks.val.0.lookup(srcdir_ino, &src), Err(libc::ENOENT));
        let srcdir_attr = mocks.val.0.getattr(srcdir_ino).unwrap();
        assert_eq!(srcdir_attr.nlink, 2);
        let dstdir_attr = mocks.val.0.getattr(dstdir_ino).unwrap();
        assert_eq!(dstdir_attr.nlink, 3);
    }

    // Rename a non-directory to a multiply-linked file.  The destination
    // directory entry should be removed, but not the inode.
    test rename_nondir_to_hardlink(mocks) {
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let lnk = OsString::from("lnk");
        let src_ino = mocks.val.0.create(1, &src, 0o644, 0, 0)
        .unwrap();
        let dst_ino = mocks.val.0.create(1, &dst, 0o644, 0, 0)
        .unwrap();
        let lnk_ino = mocks.val.0.link(1, dst_ino, &lnk).unwrap();
        assert_eq!(dst_ino, lnk_ino);

        mocks.val.0.rename(1, &src, 1, &dst).unwrap();

        assert_eq!(mocks.val.0.lookup(1, &dst), Ok(src_ino));
        assert_eq!(mocks.val.0.lookup(1, &src), Err(libc::ENOENT));
        assert_eq!(mocks.val.0.lookup(1, &lnk), Ok(lnk_ino));
        let lnk_attr = mocks.val.0.getattr(lnk_ino).unwrap();
        assert_eq!(lnk_attr.nlink, 1);
    }

    // Rename a non-directory.  The target is also a non-directory
    test rename_nondir_to_nondir(mocks) {
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_ino = mocks.val.0.mkdir(1, &srcdir, 0o755, 0, 0)
        .unwrap();
        let dstdir_ino = mocks.val.0.mkdir(1, &dstdir, 0o755, 0, 0)
        .unwrap();
        let src_ino = mocks.val.0.create(srcdir_ino, &src, 0o644, 0, 0)
        .unwrap();
        let dst_ino = mocks.val.0.create(dstdir_ino, &dst, 0o644, 0, 0)
        .unwrap();

        mocks.val.0.rename(srcdir_ino, &src, dstdir_ino, &dst).unwrap();

        assert_eq!(mocks.val.0.lookup(dstdir_ino, &dst), Ok(src_ino));
        assert_eq!(mocks.val.0.lookup(srcdir_ino, &src), Err(libc::ENOENT));
        let srcdir_inode = mocks.val.0.getattr(srcdir_ino).unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = mocks.val.0.getattr(dstdir_ino).unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
        assert_eq!(mocks.val.0.getattr(dst_ino), Err(libc::ENOENT));
    }

    // Rename a non-directory.  The target name does not exist
    test rename_nondir_to_nothing(mocks) {
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_ino = mocks.val.0.mkdir(1, &srcdir, 0o755, 0, 0)
        .unwrap();
        let dstdir_ino = mocks.val.0.mkdir(1, &dstdir, 0o755, 0, 0)
        .unwrap();
        let ino = mocks.val.0.create(srcdir_ino, &src, 0o644, 0, 0)
        .unwrap();

        mocks.val.0.rename(srcdir_ino, &src, dstdir_ino, &dst).unwrap();

        assert_eq!(mocks.val.0.lookup(dstdir_ino, &dst), Ok(ino));
        assert_eq!(mocks.val.0.lookup(srcdir_ino, &src), Err(libc::ENOENT));
        let srcdir_inode = mocks.val.0.getattr(srcdir_ino).unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = mocks.val.0.getattr(dstdir_ino).unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
    }

    // Rename a regular file to a symlink.  Make sure the target is a symlink
    // afterwards
    test rename_reg_to_symlink(mocks) {
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let linktarget = OsString::from("nonexistent");
        let srcdir_ino = mocks.val.0.mkdir(1, &srcdir, 0o755, 0, 0)
        .unwrap();
        let dstdir_ino = mocks.val.0.mkdir(1, &dstdir, 0o755, 0, 0)
        .unwrap();
        let src_ino = mocks.val.0.create(srcdir_ino, &src, 0o644, 0, 0)
        .unwrap();
        let dst_ino = mocks.val.0.symlink(dstdir_ino, &dst, 0o642, 0, 0,
                                          &linktarget)
        .unwrap();

        mocks.val.0.rename(srcdir_ino, &src, dstdir_ino, &dst).unwrap();

        assert_eq!(mocks.val.0.lookup(dstdir_ino, &dst), Ok(src_ino));
        assert_eq!(mocks.val.0.lookup(srcdir_ino, &src), Err(libc::ENOENT));
        let srcdir_inode = mocks.val.0.getattr(srcdir_ino).unwrap();
        assert_eq!(srcdir_inode.nlink, 2);
        let dstdir_inode = mocks.val.0.getattr(dstdir_ino).unwrap();
        assert_eq!(dstdir_inode.nlink, 2);
        assert_eq!(mocks.val.0.getattr(dst_ino), Err(libc::ENOENT));
        let (de, _) = mocks.val.0.readdir(dstdir_ino, 0, 0)
            .filter(|r| {
                let dirent = r.unwrap().0;
                u64::from(dirent.d_fileno) == src_ino
            }).nth(0).unwrap().unwrap();
        assert_eq!(de.d_type, libc::DT_REG);
    }

    // Rename a file with extended attributes.
    test rename_reg_with_extattrs(mocks) {
        let src = OsString::from("src");
        let dst = OsString::from("dst");
        let name = OsString::from("foo");
        let value = [1u8, 2, 3];
        let ns = ExtAttrNamespace::User;

        let ino = mocks.val.0.create(1, &src, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, ns, &name, &value[..]).unwrap();

        mocks.val.0.rename(1, &src, 1, &dst).unwrap();

        let new_ino = mocks.val.0.lookup(1, &dst).unwrap();
        let v = mocks.val.0.getextattr(new_ino, ns, &name).unwrap();
        assert_eq!(&v[..], &value);
    }

    // rename updates a file's parent directories' ctime and mtime
    test rename_timestamps(mocks) {
        let src = OsString::from("src");
        let srcdir = OsString::from("srcdir");
        let dst = OsString::from("dst");
        let dstdir = OsString::from("dstdir");
        let srcdir_ino = mocks.val.0.mkdir(1, &srcdir, 0o755, 0, 0)
        .unwrap();
        let dstdir_ino = mocks.val.0.mkdir(1, &dstdir, 0o755, 0, 0)
        .unwrap();
        let ino = mocks.val.0.create(srcdir_ino, &src, 0o644, 0, 0)
        .unwrap();
        clear_timestamps(&mocks.val.0, srcdir_ino);
        clear_timestamps(&mocks.val.0, dstdir_ino);
        clear_timestamps(&mocks.val.0, ino);

        mocks.val.0.rename(srcdir_ino, &src, dstdir_ino, &dst).unwrap();

        // Timestamps should've been updated for parent directories, but not for
        // the file itself
        assert_ts_changed(&mocks.val.0, srcdir_ino, false, true, true, false);
        assert_ts_changed(&mocks.val.0, dstdir_ino, false, true, true, false);
        assert_ts_changed(&mocks.val.0, ino, false, false, false, false);
    }

    #[allow(clippy::block_in_if_condition_stmt)]
    test rmdir(mocks) {
        let dirname = OsString::from("x");
        let ino = mocks.val.0.mkdir(1, &dirname, 0o755, 0, 0)
        .unwrap();
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

    /// Remove a directory whose name has a hash collision
    test rmdir_collision(mocks) {
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let ino0 = mocks.val.0.mkdir(1, &filename0, 0o755, 0, 0).unwrap();
        let _ino1 = mocks.val.0.mkdir(1, &filename1, 0o755, 0, 0).unwrap();
        mocks.val.0.rmdir(1, &filename1).unwrap();

        assert_eq!(mocks.val.0.lookup(1, &filename0), Ok(ino0));
        assert_eq!(mocks.val.0.lookup(1, &filename1), Err(libc::ENOENT));
    }

    test rmdir_enoent(mocks) {
        let dirname = OsString::from("x");
        assert_eq!(mocks.val.0.rmdir(1, &dirname).unwrap_err(), libc::ENOENT);
    }

    #[should_panic]
    test rmdir_enotdir(mocks) {
        let filename = OsString::from("x");
        mocks.val.0.create(1, &filename, 0o644, 0, 0)
            .unwrap();
        mocks.val.0.rmdir(1, &filename).unwrap();
    }

    test rmdir_enotempty(mocks) {
        let dirname = OsString::from("x");
        let ino = mocks.val.0.mkdir(1, &dirname, 0o755, 0, 0)
        .unwrap();
        mocks.val.0.mkdir(ino, &dirname, 0o755, 0, 0)
        .unwrap();
        assert_eq!(mocks.val.0.rmdir(1, &dirname).unwrap_err(), libc::ENOTEMPTY);
    }

    /// Try to remove a directory that isn't empty, and that has a hash
    /// collision with another file or directory
    test rmdir_enotempty_collision(mocks) {
        let filename0 = OsString::from("basedir");
        let filename1 = OsString::from("HsxUh682JQ");
        let filename2 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename1, &filename2);
        let ino0 = mocks.val.0.mkdir(1, &filename0, 0o755, 0, 0).unwrap();
        let _ino1 = mocks.val.0.mkdir(ino0, &filename1, 0o755, 0, 0).unwrap();
        let _ino2 = mocks.val.0.mkdir(ino0, &filename2, 0o755, 0, 0).unwrap();
        assert_eq!(mocks.val.0.rmdir(1, &filename0), Err(libc::ENOTEMPTY));
    }

    /// Remove a directory with an extended attribute
    test rmdir_extattr(mocks) {
        let dirname = OsString::from("x");
        let xname = OsString::from("foo");
        let xvalue1 = [0u8, 1, 2];
        let ns = ExtAttrNamespace::User;
        let ino = mocks.val.0.mkdir(1, &dirname, 0o755, 0, 0)
        .unwrap();
        mocks.val.0.setextattr(ino, ns, &xname, &xvalue1[..]).unwrap();
        mocks.val.0.rmdir(1, &dirname).unwrap();

        // Make sure the xattr is gone.  As I read things, POSIX allows us to
        // return either ENOATTR or ENOENT in this case.
        assert_eq!(mocks.val.0.getextattr(ino, ns, &xname).unwrap_err(),
                   libc::ENOATTR);
    }

    /// Removing a directory should update its parent's timestamps
    test rmdir_timestamps(mocks) {
        let dirname = OsString::from("x");
        mocks.val.0.mkdir(1, &dirname, 0o755, 0, 0).unwrap();
        clear_timestamps(&mocks.val.0, 1);

        mocks.val.0.rmdir(1, &dirname).unwrap();

        // Timestamps should've been updated
        assert_ts_changed(&mocks.val.0, 1, false, true, true, false);
    }

    test setattr(mocks) {
        let filename = OsString::from("x");
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0)
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
        mocks.val.0.setattr(ino, attr).unwrap();
        let attr = mocks.val.0.getattr(ino).unwrap();
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
        mocks.val.0.setattr(ino, attr).unwrap();
        let attr = mocks.val.0.getattr(ino).unwrap();
        assert(attr);
    }

    // setattr updates a file's ctime and mtime
    test setattr_timestamps(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        clear_timestamps(&mocks.val.0, ino);

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
        mocks.val.0.setattr(ino, attr).unwrap();

        // Timestamps should've been updated
        assert_ts_changed(&mocks.val.0, ino, false, false, true, false);
    }

    // truncating a file should delete data past the truncation point
    test setattr_truncate(mocks) {
        // First write two records
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let buf = vec![42u8; 8192];
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(8192), r);

        // Then truncate one of them.
        let mut attr = SetAttr::default();
        attr.size = Some(4096);
        mocks.val.0.setattr(ino, attr).unwrap();

        // Now extend the file past the truncated record
        attr.size = Some(8192);
        mocks.val.0.setattr(ino, attr).unwrap();

        // Finally, read the truncated record.  It should be a hole
        let sglist = mocks.val.0.read(ino, 4096, 4096).unwrap();
        let db = &sglist[0];
        let expected = [0u8; 4096];
        assert_eq!(&db[..], &expected[..]);
    }

    // Like setattr_truncate, but everything happens within a single record
    test setattr_truncate_partial_records(mocks) {
        // First write one records
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let buf = vec![42u8; 4096];
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        // Then truncate one of them.
        let mut attr = SetAttr::default();
        attr.size = Some(1000);
        mocks.val.0.setattr(ino, attr).unwrap();

        // Now extend the file past the truncated record
        attr.size = Some(4000);
        mocks.val.0.setattr(ino, attr).unwrap();

        // Finally, read from the truncated area.  It should be a hole
        let sglist = mocks.val.0.read(ino, 2000, 1000).unwrap();
        let db = &sglist[0];
        let expected = [0u8; 1000];
        assert_eq!(&db[..], &expected[..]);
    }

    /// Overwrite an existing extended attribute
    test setextattr_overwrite(mocks) {
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value1 = [0u8, 1, 2];
        let value2 = [3u8, 4, 5, 6];
        let ns = ExtAttrNamespace::User;
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, ns, &name, &value1[..]).unwrap();
        mocks.val.0.setextattr(ino, ns, &name, &value2[..]).unwrap();
        let v = mocks.val.0.getextattr(ino, ns, &name).unwrap();
        assert_eq!(&v[..], &value2);
    }

    /// Overwrite an existing extended attribute that hash-collided with a
    /// different xattr
    test setextattr_collision_overwrite(mocks) {
        let filename = OsString::from("x");
        let ns0 = ExtAttrNamespace::User;
        let ns1 = ExtAttrNamespace::System;
        let name0 = OsString::from("BWCdLQkApB");
        let name1 = OsString::from("D6tLLI4mys");
        assert_extattrs_collide(ns0, &name0, ns1, &name1);
        let value0 = [0u8, 1, 2];
        let value1 = [3u8, 4, 5, 6];
        let value1a = [4u8, 7, 8, 9, 10];

        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, ns0, &name0, &value0[..]).unwrap();
        mocks.val.0.setextattr(ino, ns1, &name1, &value1[..]).unwrap();
        mocks.val.0.setextattr(ino, ns1, &name1, &value1a[..]).unwrap();
        let v0 = mocks.val.0.getextattr(ino, ns0, &name0).unwrap();
        assert_eq!(&v0[..], &value0);
        let v1 = mocks.val.0.getextattr(ino, ns1, &name1).unwrap();
        assert_eq!(&v1[..], &value1a);
    }

    /// setextattr(2) should not update any timestamps
    test setextattr_timestamps(mocks) {
        let filename = OsString::from("x");
        let name = OsString::from("foo");
        let value = [0u8, 1, 2];
        let ns = ExtAttrNamespace::User;
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        clear_timestamps(&mocks.val.0, ino);

        mocks.val.0.setextattr(ino, ns, &name, &value[..]).unwrap();
        assert_ts_changed(&mocks.val.0, ino, false, false, false, false);
    }

    /// The file already has a blob extattr.  Set another extattr and flush them
    /// both.
    test setextattr_with_blob(mocks) {
        let filename = OsString::from("x");
        let name1 = OsString::from("foo");
        let value1 = vec![42u8; 4096];
        let name2 = OsString::from("bar");
        let value2 = [3u8, 4, 5, 6];
        let ns = ExtAttrNamespace::User;
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        mocks.val.0.setextattr(ino, ns, &name1, &value1[..]).unwrap();
        mocks.val.0.sync(); // Create a blob ExtAttr
        mocks.val.0.setextattr(ino, ns, &name2, &value2[..]).unwrap();
        mocks.val.0.sync(); // Achieve coverage of BlobExtAttr::flush

        let v = mocks.val.0.getextattr(ino, ns, &name1).unwrap();
        assert_eq!(&v[..], &value1[..]);
    }

    test statvfs(mocks) {
        let statvfs = mocks.val.0.statvfs();
        assert_eq!(statvfs.f_blocks, 262_144);
        assert_eq!(statvfs.f_bsize, 4096);
        assert_eq!(statvfs.f_frsize, 4096);
    }

    test statvfs_8k(mocks(vec![Property::RecordSize(13)])) {
        let statvfs = mocks.val.0.statvfs();
        assert_eq!(statvfs.f_blocks, 262_144);
        assert_eq!(statvfs.f_bsize, 8192);
        assert_eq!(statvfs.f_frsize, 4096);
    }

    test symlink(mocks) {
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        let ino = mocks.val.0.symlink(1, &srcname, 0o642, 0, 0, &dstname)
        .unwrap();
        assert_eq!(mocks.val.0.lookup(1, &srcname).unwrap(), ino);

        // The parent dir should have an "src" symlink entry
        let entries = mocks.val.0.readdir(1, 0, 0);
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
        assert_eq!(u64::from(dirent.d_fileno), ino);

        let attr = mocks.val.0.getattr(ino).unwrap();
        assert_eq!(attr.mode.0, libc::S_IFLNK | 0o642);
    }

    /// symlink should update the parent dir's timestamps
    test symlink_timestamps(mocks) {
        let dstname = OsString::from("dst");
        let srcname = OsString::from("src");
        clear_timestamps(&mocks.val.0, 1);

        mocks.val.0.symlink(1, &srcname, 0o642, 0, 0, &dstname).unwrap();
        assert_ts_changed(&mocks.val.0, 1, false, true, true, false);
    }

    test unlink(mocks) {
        let filename = OsString::from("x");
        let ino = mocks.val.0.create(1, &filename, 0o644, 0, 0)
        .unwrap();
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

    // Unlink a file that has a name collision with another file in the same
    // directory.
    test unlink_collision(mocks) {
        let filename0 = OsString::from("HsxUh682JQ");
        let filename1 = OsString::from("4FatHJ8I6H");
        assert_dirents_collide(&filename0, &filename1);
        let ino0 = mocks.val.0.create(1, &filename0, 0o644, 0, 0).unwrap();
        let _ino1 = mocks.val.0.create(1, &filename1, 0o644, 0, 0).unwrap();

        mocks.val.0.unlink(1, &filename1).unwrap();

        assert_eq!(mocks.val.0.lookup(1, &filename0), Ok(ino0));
        assert_eq!(mocks.val.0.lookup(1, &filename1), Err(libc::ENOENT));
    }

    test unlink_enoent(mocks) {
        let filename = OsString::from("x");
        let e = mocks.val.0.unlink(1, &filename).unwrap_err();
        assert_eq!(e, libc::ENOENT);
    }

    // When unlinking a hardlink, the file should not be removed until its link
    // count reaches zero.
    test unlink_hardlink(mocks) {
        let name1 = OsString::from("name1");
        let name2 = OsString::from("name2");
        let ino = mocks.val.0.create(1, &name1, 0o644, 0, 0)
        .unwrap();
        assert_eq!(mocks.val.0.link(1, ino, &name2).unwrap(), ino);

        mocks.val.0.unlink(1, &name1).unwrap();
        // File should still exist, now with link count 1.
        let attr = mocks.val.0.getattr(ino).unwrap();
        assert_eq!(attr.nlink, 1);
        assert_eq!(mocks.val.0.lookup(1, &name1), Err(libc::ENOENT));

        mocks.val.0.unlink(1, &name2).unwrap();
        // File should actually be gone now
        assert_eq!(mocks.val.0.lookup(1, &name1), Err(libc::ENOENT));
        assert_eq!(mocks.val.0.lookup(1, &name2), Err(libc::ENOENT));
        assert_eq!(mocks.val.0.getattr(ino), Err(libc::ENOENT));
    }

    /// unlink(2) should update the parent dir's timestamps
    test unlink_timestamps(mocks) {
        let filename = OsString::from("x");
        mocks.val.0.create(1, &filename, 0o644, 0, 0).unwrap();
        clear_timestamps(&mocks.val.0, 1);

        mocks.val.0.unlink(1, &filename).unwrap();
        assert_ts_changed(&mocks.val.0, 1, false, true, true, false);
    }

    // A very simple single record write to an empty file
    test write(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let buf = vec![42u8; 4096];
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        // Check the file size
        let attr = mocks.val.0.getattr(ino).unwrap();
        assert_eq!(attr.size, 4096);

        let sglist = mocks.val.0.read(ino, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[..]);
    }

    // A partial single record write appended to the file's end
    test write_append(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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

    // write can RMW BlobExtents
    test write_partial_blob_record(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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

    // A partial single record write that needs RMW on both ends
    test write_partial_record(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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

    // write updates a file's ctime and mtime
    test write_timestamps(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        clear_timestamps(&mocks.val.0, ino);

        let buf = vec![42u8; 4096];
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(4096), r);

        // Timestamps should've been updated
        assert_ts_changed(&mocks.val.0, ino, false, true, true, false);
    }

    // A write to an empty file that's split across two records
    test write_two_recs(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
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

    // Write one hold record and a partial one to an initially empty file.
    test write_one_and_a_half_records(mocks) {
        let ino = mocks.val.0.create(1, &OsString::from("x"), 0o644, 0, 0)
        .unwrap();
        let mut buf = vec![0u8; 6144];
        let mut rng = thread_rng();
        for x in &mut buf {
            *x = rng.gen();
        }
        let r = mocks.val.0.write(ino, 0, &buf[..], 0);
        assert_eq!(Ok(6144), r);

        // Check the file size
        let inode = mocks.val.0.getattr(ino).unwrap();
        assert_eq!(inode.size, 6144);

        let sglist = mocks.val.0.read(ino, 0, 4096).unwrap();
        let db = &sglist[0];
        assert_eq!(&db[..], &buf[0..4096]);
        let sglist = mocks.val.0.read(ino, 4096, 2048).unwrap();
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
            let ino = self.fs.mkdir(1, &OsString::from(&fname), 0o755, 0, 0)
            .unwrap();
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
            // which has inode 1.
            let num: u64 = self.rng.gen();
            let fname = format!("{:x}", num);
            info!("Touch {}", fname);
            let ino = self.fs.create(1, &OsString::from(&fname), 0o644, 0, 0)
            .unwrap();
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
                let fill = (ino.wrapping_mul(piece) %
                            u64::from(u8::max_value()))
                    as u8;
                let buf = [fill; 2048];
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
