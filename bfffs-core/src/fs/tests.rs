//! FS unit tests

// LCOV_EXCL_START
// TODO: add unit tests to assert that Fs::write borrows the correct amount
// of credit
use super::*;
use crate::{
    dataset::RangeQuery,
    tree::{Key, Value}
};
use futures::task::Poll;
use mockall::{Sequence, predicate::*};
use pretty_assertions::assert_eq;
use std::{
    borrow::Borrow,
    ffi::OsString
};

fn read_write_filesystem() -> ReadWriteFilesystem {
    ReadWriteFilesystem::default()
}

async fn setup() -> Database {
    let mut rwds = read_write_filesystem();
    rwds.expect_range()
        .once()
        .with(eq(FSKey::dying_inode_range()))
        .returning(move |_| {
            mock_range_query(Vec::new())
        });
    let mut db = Database::default();
    db.expect_create_fs()
        .once()
        .returning(|_, _: &'static str| Ok(TreeID(0)));
    db.expect_fsread_inner()
        .times(3)
        .returning(move |_| {
            let mut rods = ReadOnlyFilesystem::default();
            rods.expect_get()
                .with(eq(FSKey::new(PROPERTY_OBJECT,
                                    ObjKey::Property(PropertyName::Atime))))
                .returning(|_| future::ok(None).boxed());
            rods.expect_get()
                .with(eq(FSKey::new(PROPERTY_OBJECT,
                                    ObjKey::Property(PropertyName::RecordSize))))
                .returning(|_| future::ok(None).boxed());
            rods.expect_last_key()
                .returning(|| {
                    let root_inode_key = FSKey::new(1, ObjKey::Inode);
                    future::ok(Some(root_inode_key)).boxed()
                });
            rods
        });
    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| rwds);
    db.expect_lookup_parent()
        .with(eq(TreeID(0)))
        .returning(|_| future::ok(None).boxed());
    db.expect_lookup_fs()
        .with(eq(""))
        .returning(|_| future::ok((None, Some(TreeID(0)))).boxed());
    db.create_fs(None, "").await.unwrap();
    db
}

/// Helper that creates a mock RangeQuery from the vec of items that it should
/// return
fn mock_range_query<K, T, V>(items: Vec<(K, V)>) -> RangeQuery<K, T, V>
    where K: Key + Borrow<T>,
          T: Debug + Ord + Clone + Send,
          V: Value
{
    let mut rq = RangeQuery::new();
    let mut seq = Sequence::new();
    for item in items.into_iter() {
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_once(|_| Poll::Ready(Some(Ok(item))));
    }
    rq.expect_poll_next()
        .once()
        .in_sequence(&mut seq)
        .return_once(|_| Poll::Ready(None));
    rq
}

#[tokio::test]
async fn create() {
    let mut db = setup().await;
    let mut ds = read_write_filesystem();
    let root_ino = 1;
    let ino = 2;
    let filename = OsString::from("x");
    let filename2 = filename.clone();
    let old_ts = Timespec::new(0, 0);
    ds.expect_get()
        .once()
        .with(eq(FSKey::new(root_ino, ObjKey::Inode)))
        .returning(move |_| {
            let inode = Inode {
                size: 0,
                bytes: 0,
                nlink: 2,
                flags: 0,
                atime: old_ts,
                mtime: old_ts,
                ctime: old_ts,
                birthtime: old_ts,
                uid: 0,
                gid: 0,
                file_type: FileType::Dir,
                perm: 0o755,
            };
            future::ok(Some(FSValue::Inode(inode))).boxed()
        });
    ds.expect_insert()
        .once()
        .withf(|key, value| {
            key.is_inode() &&
            value.as_inode().unwrap().size == 0 &&
            value.as_inode().unwrap().nlink == 1 &&
            value.as_inode().unwrap().file_type == FileType::Reg(17) &&
            value.as_inode().unwrap().perm == 0o644 &&
            value.as_inode().unwrap().uid == 123 &&
            value.as_inode().unwrap().gid == 456
        }).returning(|_, _| {
            future::ok(None).boxed()
        });
    ds.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_direntry() &&
            value.as_direntry().unwrap().dtype == libc::DT_REG &&
            value.as_direntry().unwrap().name == filename2 &&
            value.as_direntry().unwrap().ino == ino
        }).returning(|_, _| {
            future::ok(None).boxed()
        });
    ds.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_inode() &&
            value.as_inode().unwrap().file_type == FileType::Dir &&
            value.as_inode().unwrap().atime == old_ts &&
            value.as_inode().unwrap().mtime != old_ts &&
            value.as_inode().unwrap().ctime != old_ts &&
            value.as_inode().unwrap().birthtime == old_ts
        }).returning(|_, _| {
            future::ok(None).boxed()
        });
    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);

    let fs = Fs::new(Arc::new(db), TreeID(0)).await;
    let fd = fs.create(&fs.root().handle(), &filename, 0o644, 123, 456).await
        .unwrap();
    assert_eq!(ino, fd.ino);
}

/// Create experiences a hash collision when adding the new directory entry
#[tokio::test]
async fn create_hash_collision() {
    let mut db = setup().await;
    let mut ds = read_write_filesystem();
    let root_ino = 1;
    let ino = 2;
    let other_ino = 100;
    let filename = OsString::from("x");
    let filename2 = filename.clone();
    let filename3 = filename.clone();
    let filename4 = filename.clone();
    let other_filename = OsString::from("y");
    let other_filename2 = other_filename.clone();
    let old_ts = Timespec::new(0, 0);
    ds.expect_get()
        .once()
        .with(eq(FSKey::new(root_ino, ObjKey::Inode)))
        .returning(move |_| {
            let inode = Inode {
                size: 0,
                bytes: 0,
                nlink: 2,
                flags: 0,
                atime: old_ts,
                mtime: old_ts,
                ctime: old_ts,
                birthtime: old_ts,
                uid: 0,
                gid: 0,
                file_type: FileType::Dir,
                perm: 0o755,
            };
            future::ok(Some(FSValue::Inode(inode))).boxed()
        });
    ds.expect_insert()
        .once()
        .withf(|key, _value| {
            key.is_inode()
        }).returning(|_, _| {
            future::ok(None).boxed()
        });
    ds.expect_insert()
        .once()
        .withf(|key, _value| {
            key.is_direntry()
        }).returning(move |_, _| {
            // Return a different directory entry
            let name = other_filename2.clone();
            let dirent = Dirent{ino: other_ino, dtype: libc::DT_REG, name};
            let v = FSValue::DirEntry(dirent);
            future::ok(Some(v)).boxed()
        });
    ds.expect_get()
        .once()
        .withf(move |args: &FSKey| {
            args.is_direntry()
        }).returning(move |_| {
            // Return the dirent that we just inserted
            let name = filename2.clone();
            let dirent = Dirent{ino, dtype: libc::DT_REG, name};
            let v = FSValue::DirEntry(dirent);
            future::ok(Some(v)).boxed()
        });
    ds.expect_insert()
        .once()
        .withf(move |key, value| {
            // Check that we're inserting a bucket with both direntries.  The
            // order doesn't matter.
            if let Some(dirents) = value.as_direntries() {
                key.is_direntry() &&
                dirents[0].dtype == libc::DT_REG &&
                dirents[0].name == other_filename &&
                dirents[0].ino == other_ino &&
                dirents[1].dtype == libc::DT_REG &&
                dirents[1].name == filename3 &&
                dirents[1].ino == ino
            } else {
                false
            }
        }).returning(move |_, _| {
            // Return the dirent that we just inserted
            let name = filename4.clone();
            let dirent = Dirent{ino, dtype: libc::DT_REG, name};
            let v = FSValue::DirEntry(dirent);
            future::ok(Some(v)).boxed()
        });
    ds.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_inode() &&
            value.as_inode().unwrap().file_type == FileType::Dir &&
            value.as_inode().unwrap().atime == old_ts &&
            value.as_inode().unwrap().mtime != old_ts &&
            value.as_inode().unwrap().ctime != old_ts &&
            value.as_inode().unwrap().birthtime == old_ts
        }).returning(|_, _| {
            future::ok(None).boxed()
        });

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), TreeID(0)).await;

    let fd = fs.create(&fs.root().handle(), &filename, 0o644, 123, 456).await
        .unwrap();
    assert_eq!(ino, fd.ino);
}

// Pet kcov
#[test]
fn debug_getattr() {
    let attr = GetAttr {
        ino: 1,
        size: 4096,
        bytes: 4096,
        atime: Timespec::new(1, 2),
        mtime: Timespec::new(3, 4),
        ctime: Timespec::new(5, 6),
        birthtime: Timespec::new(7, 8),
        mode: Mode(libc::S_IFREG | 0o644),
        nlink: 1,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        blksize: 131072,
        flags: 0,
    };
    let s = format!("{:?}", attr);
    assert_eq!("GetAttr { ino: 1, size: 4096, bytes: 4096, atime: Timespec { sec: 1, nsec: 2 }, mtime: Timespec { sec: 3, nsec: 4 }, ctime: Timespec { sec: 5, nsec: 6 }, birthtime: Timespec { sec: 7, nsec: 8 }, mode: Mode { .0: 33188, perm: 420 }, nlink: 1, uid: 1000, gid: 1000, rdev: 0, blksize: 131072, flags: 0 }", s);
}

// Pet kcov
#[test]
fn eq_getattr() {
    let attr = GetAttr {
        ino: 1,
        size: 4096,
        bytes: 4096,
        atime: Timespec::new(1, 2),
        mtime: Timespec::new(3, 4),
        ctime: Timespec::new(5, 6),
        birthtime: Timespec::new(7, 8),
        mode: Mode(libc::S_IFREG | 0o644),
        nlink: 1,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        blksize: 65536,
        flags: 0,
    };
    let attr2 = attr;
    assert_eq!(attr2, attr);
}

// Pet kcov
#[test]
fn debug_setattr() {
    let attr = SetAttr {
        size: None,
        atime: None,
        mtime: None,
        ctime: None,
        birthtime: None,
        perm: None,
        uid: None,
        gid: None,
        flags: None,
    };
    let s = format!("{:?}", attr);
    assert_eq!("SetAttr { size: None, atime: None, mtime: None, ctime: None, birthtime: None, perm: None, uid: None, gid: None, flags: None }", s);
}

/// A 3-way hash collision of extended attributes.  deleteextattr removes one of
/// them.
#[tokio::test]
async fn deleteextattr_3way_collision() {
    let mut db = setup().await;
    let mut ds = read_write_filesystem();
    let ino = 1;
    // Three attributes share a bucket.  The test will delete name2
    let name0 = OsString::from("foo");
    let name0a = name0.clone();
    let value0 = OsString::from("foov");
    let name1 = OsString::from("bar");
    let name1a = name1.clone();
    let value1 = OsString::from("barf");
    let name2 = OsString::from("baz");
    let name2a = name2.clone();
    let value2 = OsString::from("zoobo");
    let namespace = ExtAttrNamespace::User;

    ds.expect_remove()
    .once()
    .returning(move |_| {
        // Return the three values
        let dbs0 = Arc::new(DivBufShared::from(Vec::from(value0.as_bytes())));
        let extent0 = InlineExtent::new(dbs0);
        let name = name0a.clone();
        let iea0 = InlineExtAttr{namespace, name, extent: extent0};
        let extattr0 = ExtAttr::Inline(iea0);
        let dbs1 = Arc::new(DivBufShared::from(Vec::from(value1.as_bytes())));
        let extent1 = InlineExtent::new(dbs1);
        let name = name1a.clone();
        let iea1 = InlineExtAttr{namespace, name, extent: extent1};
        let extattr1 = ExtAttr::Inline(iea1);
        let dbs2 = Arc::new(DivBufShared::from(Vec::from(value2.as_bytes())));
        let extent2 = InlineExtent::new(dbs2);
        let name = name2a.clone();
        let iea2 = InlineExtAttr{namespace, name, extent: extent2};
        let extattr2 = ExtAttr::Inline(iea2);
        let v = FSValue::ExtAttrs(vec![extattr0, extattr1, extattr2]);
        future::ok(Some(v)).boxed()
    });
    ds.expect_insert()
    .once()
    .withf(move |_key, value| {
        // name0 and name1 should be reinserted
        let extattrs = value.as_extattrs().unwrap();
        let ie0 = extattrs[0].as_inline().unwrap();
        let ie1 = extattrs[1].as_inline().unwrap();
        ie0.name == name0 && ie1.name == name1 && extattrs.len() == 2
    }).returning(|_, _| {
        future::ok(None).boxed()
    });

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), TreeID(0)).await;
    let fd = FileDataMut::new(Some(1), ino);
    let r = fs.deleteextattr(&fd.handle(), namespace, &name2).await;
    assert_eq!(Ok(()), r);
}

/// A 3-way hash collision of extended attributes.  Two are stored in one key,
/// and deleteextattr tries to delete a third that hashes to the same key, but
/// isn't stored there.
#[tokio::test]
async fn deleteextattr_3way_collision_enoattr() {
    let mut db = setup().await;
    let mut ds = read_write_filesystem();
    let ino = 1;
    // name0 and name1 are stored.  The test tries to delete name2
    let name0 = OsString::from("foo");
    let name0a = name0.clone();
    let value0 = OsString::from("foov");
    let name1 = OsString::from("bar");
    let name1a = name1.clone();
    let value1 = OsString::from("barf");
    let name2 = OsString::from("baz");
    let namespace = ExtAttrNamespace::User;

    ds.expect_remove()
    .once()
    .returning(move |_| {
        // Return the first two values
        let dbs0 = Arc::new(DivBufShared::from(Vec::from(value0.as_bytes())));
        let extent0 = InlineExtent::new(dbs0);
        let name = name0a.clone();
        let iea0 = InlineExtAttr{namespace, name, extent: extent0};
        let extattr0 = ExtAttr::Inline(iea0);
        let dbs1 = Arc::new(DivBufShared::from(Vec::from(value1.as_bytes())));
        let extent1 = InlineExtent::new(dbs1);
        let name = name1a.clone();
        let iea1 = InlineExtAttr{namespace, name, extent: extent1};
        let extattr1 = ExtAttr::Inline(iea1);
        let v = FSValue::ExtAttrs(vec![extattr0, extattr1]);
        future::ok(Some(v)).boxed()
    });
    ds.expect_insert()
    .once()
    .withf(move |_key, value| {
        // name0 and name1 should be reinserted
        let extattrs = value.as_extattrs().unwrap();
        let ie0 = extattrs[0].as_inline().unwrap();
        let ie1 = extattrs[1].as_inline().unwrap();
        ie0.name == name0 && ie1.name == name1 && extattrs.len() == 2
    }).returning(|_, _| {
        future::ok(None).boxed()
    });

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), TreeID(0)).await;
    let fd = FileDataMut::new(Some(1), ino);
    let r = fs.deleteextattr(&fd.handle(), namespace, &name2).await;
    assert_eq!(Err(libc::ENOATTR), r);
}

#[tokio::test]
async fn fsync() {
    let ino = 42;

    let mut db = setup().await;
    db.expect_sync_transaction()
        .once()
        .returning(|| future::ok(()).boxed());
    let fs = Fs::new(Arc::new(db), TreeID(0)).await;

    let fd = FileDataMut::new(Some(1), ino);
    assert!(fs.fsync(&fd.handle()).await.is_ok());
}

/// Reading the source returns EIO.  Don't delete the dest
#[tokio::test]
async fn rename_eio() {
    let mut db = setup().await;
    let mut ds = read_write_filesystem();
    let srcname = OsString::from("x");
    let dstname = OsString::from("y");
    let src_ino = 3;
    let dst_ino = 4;
    let dst_dirent = Dirent {
        ino: dst_ino,
        dtype: libc::DT_REG,
        name: dstname.clone()
    };
    ds.expect_get()
        .once()
        .with(eq(FSKey::new(1, ObjKey::dir_entry(&dstname))))
        .returning(move |_| {
            let v = FSValue::DirEntry(dst_dirent.clone());
            future::ok(Some(v)).boxed()
        });
    ds.expect_remove()
        .once()
        .with(
            eq(FSKey::new(1, ObjKey::dir_entry(&srcname)))
        ).returning(move |_| {
            future::err(Error::EIO).boxed()
        });

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);

    let fs = Fs::new(Arc::new(db), TreeID(0)).await;
    let root = fs.root();
    let fd = FileDataMut::new_for_tests(Some(root.ino), src_ino);
    let r = fs.rename(&root.handle(), &fd.handle(), &srcname, &root.handle(),
        Some(dst_ino), &dstname)
        .await;
    assert_eq!(Err(libc::EIO), r);
}

/// Basic setextattr test, that does not rely on any other extattr
/// functionality.
#[tokio::test]
async fn setextattr() {
    let mut db = setup().await;
    let mut ds = read_write_filesystem();
    let root_ino = 1;
    let name = OsString::from("foo");
    let name2 = name.clone();
    let value = OsString::from("bar");
    let value2 = value.clone();
    let namespace = ExtAttrNamespace::User;

    ds.expect_insert()
    .once()
    .withf(move |key, value| {
        let extattr = value.as_extattr().unwrap();
        let ie = extattr.as_inline().unwrap();
        key.is_extattr() &&
        key.objtype() == 3 &&
        key.object() == root_ino &&
        ie.namespace == namespace &&
        ie.name == name2 &&
        &ie.extent.buf.try_const().unwrap()[..] == value2.as_bytes()
    }).returning(|_, _| {
        future::ok(None).boxed()
    });

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), TreeID(0)).await;
    let r = fs.setextattr(&fs.root().handle(), namespace, &name,
        value.as_bytes())
        .await;
    assert_eq!(Ok(()), r);
}

/// setextattr with a 3-way hash collision.  It's hard to programmatically
/// generate 3-way hash collisions, so we simulate them using mocks
#[tokio::test]
async fn setextattr_3way_collision() {
    let mut db = setup().await;
    let mut ds = read_write_filesystem();
    let root_ino = 1;
    // name0 and name1 are already set
    let name0 = OsString::from("foo");
    let name0a = name0.clone();
    let value0 = OsString::from("foov");
    let name1 = OsString::from("bar");
    let name1a = name1.clone();
    let value1 = OsString::from("barf");
    // The test will set name3, and its hash will collide with name0
    let name2 = OsString::from("baz");
    let name2a = name2.clone();
    let name2b = name2.clone();
    let name2c = name2.clone();
    let name2d = name2.clone();
    let value2 = OsString::from("zoobo");
    let value2a = value2.clone();
    let value2b = value2.clone();
    let namespace = ExtAttrNamespace::User;

    ds.expect_insert()
    .once()
    .withf(move |_key, value| {
        if let Some(extattr) = value.as_extattr() {
            let ie = extattr.as_inline().unwrap();
            ie.name == name2a
        } else {
            false
        }
    }).returning(move |_, _| {
        // Return the previous two values
        let dbs0 = Arc::new(DivBufShared::from(Vec::from(value0.as_bytes())));
        let extent0 = InlineExtent::new(dbs0);
        let name = name0a.clone();
        let iea0 = InlineExtAttr{namespace, name, extent: extent0};
        let extattr0 = ExtAttr::Inline(iea0);
        let dbs1 = Arc::new(DivBufShared::from(Vec::from(value1.as_bytes())));
        let extent1 = InlineExtent::new(dbs1);
        let name = name1a.clone();
        let iea1 = InlineExtAttr{namespace, name, extent: extent1};
        let extattr1 = ExtAttr::Inline(iea1);
        let v = FSValue::ExtAttrs(vec![extattr0, extattr1]);
        future::ok(Some(v)).boxed()
    });
    ds.expect_get()
    .once()
    .withf(move |arg: &FSKey| {
        arg.is_extattr() &&
        arg.objtype() == 3 &&
        arg.object() == root_ino
    }).returning(move |_| {
        // Return the newly inserted value2
        let dbs2 = Arc::new(DivBufShared::from(Vec::from(value2a.as_bytes())));
        let extent2 = InlineExtent::new(dbs2);
        let name = name2b.clone();
        let iea2 = InlineExtAttr{namespace, name, extent: extent2};
        let extattr2 = ExtAttr::Inline(iea2);
        let v = FSValue::ExtAttr(extattr2);
        future::ok(Some(v)).boxed()
    });
    ds.expect_insert()
    .once()
    .withf(move |_key, value| {
        let extattrs = value.as_extattrs().unwrap();
        let ie0 = extattrs[0].as_inline().unwrap();
        let ie1 = extattrs[1].as_inline().unwrap();
        let ie2 = extattrs[2].as_inline().unwrap();
        ie0.name == name0 && ie1.name == name1 && ie2.name == name2c
    }).returning(move |_, _| {
        // Return a copy of the recently inserted value2
        let dbs2 = Arc::new(DivBufShared::from(Vec::from(value2b.as_bytes())));
        let extent2 = InlineExtent::new(dbs2);
        let name = name2d.clone();
        let iea2 = InlineExtAttr{namespace, name, extent: extent2};
        let extattr2 = ExtAttr::Inline(iea2);
        let v = FSValue::ExtAttr(extattr2);
        future::ok(Some(v)).boxed()
    });

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), TreeID(0)).await;
    let r = fs.setextattr(&fs.root().handle(), namespace, &name2,
        value2.as_bytes())
        .await;
    assert_eq!(Ok(()), r);
}

#[tokio::test]
async fn set_prop() {
    let mut db = setup().await;
    let mut ds0 = read_write_filesystem();
    let objkey = ObjKey::Property(PropertyName::Atime);
    ds0.expect_insert()
        .once()
        .with(eq(FSKey::new(PROPERTY_OBJECT, objkey)),
              eq(FSValue::Property(Property::Atime(false)))
        )
        .returning(|_, _| {
            future::ok(None).boxed()
        });
    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds0);
    let fs = Fs::new(Arc::new(db), TreeID(0)).await;
    fs.set_prop(Property::Atime(false)).await.unwrap();
}

#[tokio::test]
async fn sync() {
    let mut db = setup().await;
    db.expect_sync_transaction()
        .once()
        .returning(|| future::ok(()).boxed());
    let fs = Fs::new(Arc::new(db), TreeID(0)).await;

    fs.sync().await;
}
