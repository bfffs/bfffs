// vim: tw=80
use std::mem;

use bfffs_core::fs::{FileData, GetAttr, Mode};
use futures::FutureExt;
use mockall::{predicate, Sequence};

use super::*;

fn assert_cached(fusefs: &FuseFs, parent_ino: u64, name: &OsStr, ino: u64) {
    assert!(fusefs.files.lock().unwrap().contains_key(&ino));
    let key = (parent_ino, name.to_owned());
    assert_eq!(Some(&ino), fusefs.names.lock().unwrap().get(&key));
}

fn assert_not_cached(
    fusefs: &FuseFs,
    parent_ino: u64,
    name: &OsStr,
    ino: Option<u64>,
) {
    if let Some(i) = ino {
        assert!(!fusefs.files.lock().unwrap().contains_key(&i));
    }
    let key = (parent_ino, name.to_owned());
    assert!(!fusefs.names.lock().unwrap().contains_key(&key));
}

fn make_mock_fs<F>(f: F) -> FuseFs
where
    F: FnOnce(&mut Fs),
{
    let mut mock_fs = Fs::default();
    mock_fs
        .expect_root()
        .returning(|| FileDataMut::new_for_tests(None, 1));
    f(&mut mock_fs);
    FuseFs::from(Arc::new(mock_fs))
}

mod create {
    use super::*;

    const FLAGS: u32 = (libc::O_CREAT | libc::O_RDWR) as u32;

    #[test]
    fn enotdir() {
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;

        let request = Request {
            uid: 12345,
            gid: 12345,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_create()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::always(),
                    predicate::always(),
                )
                .returning(|_, _, _, _, _| Err(libc::ENOTDIR));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .create(request, parent, name, mode.into(), FLAGS)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::ENOTDIR.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    // The file gets created successfully, but fetching its attributes fails
    #[test]
    fn getattr_eio() {
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let ino = 43;

        let request = Request {
            uid: 12345,
            gid: 12345,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_create()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::always(),
                    predicate::always(),
                )
                .returning(move |_, _, _, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .return_const(Err(libc::EIO));
            mock_fs
                .expect_inactive()
                .withf(move |fd| fd.ino() == ino)
                .times(1)
                .return_const(());
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));

        let reply = fusefs
            .create(request, parent, name, mode.into(), FLAGS)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
        assert_not_cached(&fusefs, parent, name, Some(ino));
    }

    #[test]
    fn ok() {
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request {
            uid,
            gid,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_create()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::always(),
                    predicate::always(),
                )
                .returning(move |_, _, _, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .return_const(Ok(GetAttr {
                    ino,
                    size: 0,
                    bytes: 0,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFREG),
                    nlink: 1,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 131072,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .create(request, parent, name, mode.into(), FLAGS)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_cached(&fusefs, parent, name, ino);
    }
}

mod removexattr {
    use super::*;

    #[test]
    fn enoattr() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_deleteextattr()
                .times(1)
                .withf(
                    move |fd: &FileData,
                          ns: &ExtAttrNamespace,
                          name: &OsStr| {
                        fd.ino() == ino &&
                            *ns == ExtAttrNamespace::System &&
                            name == OsStr::from_bytes(b"md5")
                    },
                )
                .returning(move |_, _, _| Err(libc::ENOATTR));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .removexattr(request, ino, packed_name)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::ENOATTR.into()));
    }

    #[test]
    fn ok() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_deleteextattr()
                .times(1)
                .withf(
                    move |fd: &FileData,
                          ns: &ExtAttrNamespace,
                          name: &OsStr| {
                        fd.ino() == ino &&
                            *ns == ExtAttrNamespace::System &&
                            name == OsStr::from_bytes(b"md5")
                    },
                )
                .returning(move |_, _, _| Ok(()));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .removexattr(request, ino, packed_name)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Ok(()));
    }
}

mod forget {
    use super::*;

    #[test]
    fn one() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"foo.txt");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_lookup()
                .times(1)
                .with(
                    predicate::always(),
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                )
                .returning(move |_, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .times(1)
                .return_const(Ok(GetAttr {
                    ino,
                    size,
                    bytes: size,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFREG),
                    nlink: 1,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                }));
            mock_fs
                .expect_inactive()
                .withf(move |fd| fd.ino() == ino)
                .times(1)
                .return_const(());
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .lookup(request, parent, name)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        // forget does not return a Result
        fusefs.forget(request, ino, 1).now_or_never().unwrap();
        assert!(!fusefs.files.lock().unwrap().contains_key(&ino))
    }
}

mod fsync {
    use super::*;

    #[test]
    fn eio() {
        let ino = 42;
        let fh = 0xdeadbeef;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_fsync()
                .times(1)
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .return_const(Err(libc::EIO));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .fsync(request, ino, fh, false)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
    }

    #[test]
    fn ok() {
        let ino = 42;
        let fh = 0xdeadbeef;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_fsync()
                .times(1)
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .return_const(Ok(()));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .fsync(request, ino, fh, false)
            .now_or_never()
            .unwrap();
        assert!(reply.is_ok());
    }
}

mod getattr {
    use super::*;

    #[test]
    fn enoent() {
        let ino = 42;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_getattr()
                .times(1)
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .return_const(Err(libc::ENOENT));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .getattr(request, ino, None, 0)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::ENOENT.into()));
    }

    #[test]
    fn ok() {
        let ino = 42;
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .times(1)
                .return_const(Ok(GetAttr {
                    ino,
                    size,
                    bytes: size,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFREG),
                    nlink: 1,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 8192,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .getattr(request, ino, None, 0)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, size);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 0);
        assert_eq!(reply.attr.blksize, 8192);
    }
}

mod getxattr {
    use divbuf::DivBufShared;

    use super::*;

    #[test]
    fn length_enoattr() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 0;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_getextattrlen()
                .times(1)
                .withf(
                    move |fd: &FileData,
                          ns: &ExtAttrNamespace,
                          name: &OsStr| {
                        fd.ino() == ino &&
                            *ns == ExtAttrNamespace::System &&
                            name == OsStr::from_bytes(b"md5")
                    },
                )
                .returning(move |_, _, _| Err(libc::ENOATTR));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .getxattr(request, ino, packed_name, wantsize)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::ENOATTR.into()));
    }

    #[test]
    fn length_ok() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 0;
        let size = 16;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_getextattrlen()
                .times(1)
                .withf(
                    move |fd: &FileData,
                          ns: &ExtAttrNamespace,
                          name: &OsStr| {
                        fd.ino() == ino &&
                            *ns == ExtAttrNamespace::System &&
                            name == OsStr::from_bytes(b"md5")
                    },
                )
                .returning(move |_, _, _| Ok(size));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .getxattr(request, ino, packed_name, wantsize)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply, ReplyXAttr::Size(size));
    }

    #[test]
    fn value_enoattr() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 80;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_getextattr()
                .times(1)
                .withf(
                    move |fd: &FileData,
                          ns: &ExtAttrNamespace,
                          name: &OsStr| {
                        fd.ino() == ino &&
                            *ns == ExtAttrNamespace::System &&
                            name == OsStr::from_bytes(b"md5")
                    },
                )
                .return_const(Err(libc::ENOATTR));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .getxattr(request, ino, packed_name, wantsize)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::ENOATTR.into()));
    }

    // The FUSE protocol requires a file system to return ERANGE if the
    // attribute's value can't fit in the size requested by the client.
    // That's contrary to how FreeBSD's getextattr(2) works and contrary to how
    // BFFFS's Fs::getextattr works.  It's also hard to trigger during normal
    // use, because the kernel first asks for the size of the attribute.  So
    // during normal use, this error can only be the result of a race.
    #[test]
    fn value_erange() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let wantsize = 16;
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_getextattr()
                .times(1)
                .withf(
                    move |fd: &FileData,
                          ns: &ExtAttrNamespace,
                          name: &OsStr| {
                        fd.ino() == ino &&
                            *ns == ExtAttrNamespace::System &&
                            name == OsStr::from_bytes(b"md5")
                    },
                )
                .returning(move |_, _, _| {
                    let dbs = DivBufShared::from(&v[..]);
                    Ok(dbs.try_const().unwrap())
                });
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .getxattr(request, ino, packed_name, wantsize)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::ERANGE.into()));
    }

    #[test]
    fn value_ok() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"user.md5");
        let wantsize = 80;
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_getextattr()
                .times(1)
                .withf(
                    move |fd: &FileData,
                          ns: &ExtAttrNamespace,
                          name: &OsStr| {
                        fd.ino() == ino &&
                            *ns == ExtAttrNamespace::User &&
                            name == OsStr::from_bytes(b"md5")
                    },
                )
                .returning(move |_, _, _| {
                    let dbs = DivBufShared::from(&v[..]);
                    Ok(dbs.try_const().unwrap())
                });
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .getxattr(request, ino, packed_name, wantsize)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply, ReplyXAttr::Data(Bytes::copy_from_slice(v)));
    }
}

mod link {
    use super::*;

    // POSIX stupidly requires link(2) to return EPERM for directories.  EISDIR
    // would've been a better choice
    #[test]
    fn eperm() {
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let ino = 43;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_link()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(name),
                )
                .return_const(Err(libc::EPERM));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .link(request, ino, parent, name)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    // link completes successfully, but the subsequent getattr does not
    #[test]
    fn getattr_eio() {
        let mut seq = Sequence::new();
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let ino = 43;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_link()
                .times(1)
                .in_sequence(&mut seq)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(name),
                )
                .return_const(Ok(()));
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .times(1)
                .in_sequence(&mut seq)
                .return_const(Err(libc::EIO));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .link(request, ino, parent, name)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    #[test]
    fn ok() {
        let mut seq = Sequence::new();
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let ino = 43;
        let size = 1024;
        let mode = 0o644;
        let uid = 123;
        let gid = 456;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_link()
                .times(1)
                .in_sequence(&mut seq)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(name),
                )
                .return_const(Ok(()));
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .times(1)
                .in_sequence(&mut seq)
                .return_const(Ok(GetAttr {
                    ino,
                    size,
                    bytes: size,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFREG),
                    nlink: 1,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 16384,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .link(request, ino, parent, name)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.blksize, 16384);
        assert_eq!(reply.attr.rdev, 0);
        assert_cached(&fusefs, parent, name, ino);
    }
}

mod listxattr {
    use bfffs_core::fs_tree::{InlineExtAttr, InlineExtent};

    use super::*;

    #[test]
    fn length_eperm() {
        let ino = 42;
        let wantsize = 0;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_listextattrlen()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::always(),
                )
                .returning(|_ino, _f| Err(libc::EPERM));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .listxattr(request, ino, wantsize)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
    }

    #[test]
    fn length_ok() {
        let ino = 42;
        let wantsize = 0;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_listextattrlen()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::always(),
                )
                .returning(|_ino, f| {
                    Ok(f(&ExtAttr::Inline(InlineExtAttr {
                        namespace: ExtAttrNamespace::System,
                        name:      OsString::from("md5"),
                        extent:    InlineExtent::default(),
                    })) + f(&ExtAttr::Inline(InlineExtAttr {
                        namespace: ExtAttrNamespace::User,
                        name:      OsString::from("icon"),
                        extent:    InlineExtent::default(),
                    })))
                });
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .listxattr(request, ino, wantsize)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply, ReplyXAttr::Size(21));
    }

    #[test]
    fn list_eperm() {
        let ino = 42;
        let wantsize = 1024;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_listextattr()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(wantsize),
                    predicate::always(),
                )
                .returning(|_ino, _size, _f| Err(libc::EPERM));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .listxattr(request, ino, wantsize)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
    }

    // The list of attributes doesn't fit in the space requested.  This is most
    // likely due to a race; an attribute was added after the kernel requested
    // the size of the attribute list.
    #[test]
    fn list_erange() {
        let ino = 42;
        let wantsize = 10;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_listextattr()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(wantsize),
                    predicate::always(),
                )
                .returning(|_ino, wantsize, f| {
                    let mut buf = Vec::with_capacity(wantsize as usize);
                    let md5 = ExtAttr::Inline(InlineExtAttr {
                        namespace: ExtAttrNamespace::System,
                        name:      OsString::from("md5"),
                        extent:    InlineExtent::default(),
                    });
                    let icon = ExtAttr::Inline(InlineExtAttr {
                        namespace: ExtAttrNamespace::User,
                        name:      OsString::from("icon"),
                        extent:    InlineExtent::default(),
                    });
                    f(&mut buf, &md5);
                    f(&mut buf, &icon);
                    Ok(buf)
                });
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .listxattr(request, ino, wantsize)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::ERANGE.into()));
    }

    #[test]
    fn list_ok() {
        let ino = 42;
        let wantsize = 1024;
        let expected = b"system.md5\0user.icon\0";

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_listextattr()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(wantsize),
                    predicate::always(),
                )
                .returning(|_ino, wantsize, f| {
                    let mut buf = Vec::with_capacity(wantsize as usize);
                    let md5 = ExtAttr::Inline(InlineExtAttr {
                        namespace: ExtAttrNamespace::System,
                        name:      OsString::from("md5"),
                        extent:    InlineExtent::default(),
                    });
                    let icon = ExtAttr::Inline(InlineExtAttr {
                        namespace: ExtAttrNamespace::User,
                        name:      OsString::from("icon"),
                        extent:    InlineExtent::default(),
                    });
                    f(&mut buf, &md5);
                    f(&mut buf, &icon);
                    Ok(buf)
                });
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .listxattr(request, ino, wantsize)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply, ReplyXAttr::Data(Bytes::copy_from_slice(expected)));
    }
}

mod lookup {
    use super::*;

    #[test]
    fn cached() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"foo.txt");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .times(1)
                .return_const(Ok(GetAttr {
                    ino,
                    size,
                    bytes: size,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFREG),
                    nlink: 1,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 32768,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(None, parent));
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(Some(1), ino));
        fusefs
            .names
            .lock()
            .unwrap()
            .insert((parent, name.to_owned()), ino);
        let reply = fusefs
            .lookup(request, parent, name)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, size);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 0);
        assert_eq!(reply.attr.blksize, 32768);
    }

    /// Looking up "." increments the directory's lookup count
    #[test]
    fn dot() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"emptydir");
        let dot = OsStr::from_bytes(b".");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o755;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .times(1)
                .return_const(Ok(GetAttr {
                    ino,
                    size: 2,
                    bytes: 0,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFDIR),
                    nlink: 2,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 0,
                    flags: 0,
                }));
            mock_fs
                .expect_lookup()
                .times(1)
                .with(
                    predicate::always(),
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(dot),
                )
                .returning(move |_, _, _| {
                    Ok(FileDataMut::new_for_tests(Some(1), ino))
                });
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(None, parent));
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(Some(1), ino));
        fusefs
            .names
            .lock()
            .unwrap()
            .insert((parent, name.to_owned()), ino);

        let reply = fusefs
            .lookup(request, ino, dot)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.nlink, 2);
        let lookup_count =
            fusefs.files.lock().unwrap().get(&ino).unwrap().lookup_count;
        assert_eq!(lookup_count, 2);
    }

    /// The NFS server will sometimes lookup "." before doing a lookup for the
    /// directory itself.
    #[test]
    fn dot_uncached() {
        let parent = 42;
        let ino = 43;
        //let name = OsStr::from_bytes(b"emptydir");
        let dot = OsStr::from_bytes(b".");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o755;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .times(1)
                .return_const(Ok(GetAttr {
                    ino,
                    size: 2,
                    bytes: 0,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFDIR),
                    nlink: 2,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 0,
                    flags: 0,
                }));
            mock_fs
                .expect_ilookup()
                .times(1)
                .with(predicate::eq(ino))
                .returning(move |_| {
                    Ok(FileDataMut::new_for_tests(Some(1), ino))
                });
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(None, parent));

        let reply = fusefs
            .lookup(request, ino, dot)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.nlink, 2);
        let lookup_count =
            fusefs.files.lock().unwrap().get(&ino).unwrap().lookup_count;
        assert_eq!(lookup_count, 1);
    }

    /// Looking up ".." increments the parent's lookup count
    #[test]
    fn dotdot() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"emptydir");
        let dotdot = OsStr::from_bytes(b"..");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o755;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| {
                    fd.ino() == parent
                }))
                .times(1)
                .return_const(Ok(GetAttr {
                    ino: parent,
                    size: 3,
                    bytes: 0,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFDIR),
                    nlink: 3,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 0,
                    flags: 0,
                }));
            mock_fs
                .expect_lookup()
                .times(1)
                .withf(move |gfd, fd, name| {
                    gfd.unwrap().ino() == parent &&
                        fd.ino() == ino &&
                        name == dotdot
                })
                .returning(move |_, _, _| {
                    Ok(FileDataMut::new_for_tests(Some(1), parent))
                });
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(None, parent));
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(Some(parent), ino));
        fusefs
            .names
            .lock()
            .unwrap()
            .insert((parent, name.to_owned()), ino);

        let reply = fusefs
            .lookup(request, ino, dotdot)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, parent);
        assert_eq!(reply.attr.nlink, 3);
        let lookup_count = fusefs
            .files
            .lock()
            .unwrap()
            .get(&parent)
            .unwrap()
            .lookup_count;
        assert_eq!(lookup_count, 2);
    }

    #[test]
    fn enoent() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo.txt");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_lookup()
                .times(1)
                .with(
                    predicate::always(),
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                )
                .returning(|_, _, _| Err(libc::ENOENT));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(None, parent));
        let reply =
            fusefs.lookup(request, parent, name).now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ENOENT.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    // Lookup both names of a hard-linked file
    #[test]
    fn hardlink() {
        let parent = 42;
        let ino = 43;
        let name0 = OsStr::from_bytes(b"foo.txt");
        let name1 = OsStr::from_bytes(b"bar.txt");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_lookup()
                .times(1)
                .with(
                    predicate::always(),
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name0),
                )
                .returning(move |_, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_lookup()
                .times(1)
                .with(
                    predicate::always(),
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name1),
                )
                .returning(move |_, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .times(2)
                .return_const(Ok(GetAttr {
                    ino,
                    size,
                    bytes: size,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFREG),
                    nlink: 2,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));

        let reply0 = fusefs
            .lookup(request, parent, name0)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply0.attr.ino, ino);
        assert_eq!(reply0.attr.nlink, 2);
        assert_cached(&fusefs, parent, name0, ino);
        let reply1 = fusefs
            .lookup(request, parent, name1)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply1.attr.ino, ino);
        assert_eq!(reply1.attr.nlink, 2);
        assert_cached(&fusefs, parent, name1, ino);
    }

    // The file's name is cached, but its FileData is not
    #[test]
    fn name_cached() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"foo.txt");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_lookup()
                .times(1)
                .with(
                    predicate::always(),
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                )
                .returning(move |_, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .times(1)
                .return_const(Ok(GetAttr {
                    ino,
                    size,
                    bytes: size,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFREG),
                    nlink: 1,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        fusefs
            .names
            .lock()
            .unwrap()
            .insert((parent, name.to_owned()), ino);
        let reply = fusefs
            .lookup(request, parent, name)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, size);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 0);
        assert_eq!(reply.attr.blksize, 4096);
        assert_cached(&fusefs, parent, name, ino);
    }

    // The file's name is cached, but its FileData is not, and getattr returns
    // an error.  We must not leak the FileData.
    #[test]
    fn name_cached_getattr_io() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"foo.txt");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_lookup()
                .times(1)
                .with(
                    predicate::always(),
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                )
                .returning(move |_, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .times(1)
                .return_const(Err(libc::EIO));
            mock_fs
                .expect_inactive()
                .withf(move |fd| fd.ino() == ino)
                .return_const(());
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        fusefs
            .names
            .lock()
            .unwrap()
            .insert((parent, name.to_owned()), ino);
        let reply =
            fusefs.lookup(request, parent, name).now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
        assert!(!fusefs.files.lock().unwrap().contains_key(&ino));
    }

    #[test]
    fn ok() {
        let parent = 42;
        let ino = 43;
        let name = OsStr::from_bytes(b"foo.txt");
        let uid = 12345u32;
        let gid = 54321u32;
        let mode = 0o644;
        let size = 1024;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_lookup()
                .times(1)
                .with(
                    predicate::always(),
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                )
                .returning(move |_, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .times(1)
                .return_const(Ok(GetAttr {
                    ino,
                    size,
                    bytes: size,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFREG),
                    nlink: 1,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .lookup(request, parent, name)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, size);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 0);
        assert_eq!(reply.attr.blksize, 4096);
        assert_cached(&fusefs, parent, name, ino);
    }
}

mod lseek {
    use super::*;

    #[test]
    fn hole() {
        let ino = 42;
        let fh = 0xdeadbeef;
        let ofs = 2048;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_lseek()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(ofs),
                    predicate::eq(SeekWhence::Hole),
                )
                .returning(|_ino, _ofs, _whence| Ok(4096));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let whence = libc::SEEK_HOLE as u32;
        let reply = fusefs
            .lseek(request, ino, fh, ofs, whence)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.offset, 4096);
    }

    #[test]
    fn invalid_whence() {
        let ino = 42;
        let fh = 0xdeadbeef;
        let ofs = 2048;

        let request = Request::default();

        let fusefs = make_mock_fs(|_| ());

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let whence = libc::SEEK_CUR as u32;
        let reply = fusefs
            .lseek(request, ino, fh, ofs, whence)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(fuse3::Errno::from(libc::EINVAL)));
    }
}

mod mkdir {
    use super::*;

    #[test]
    fn eperm() {
        let mode: u16 = 0o755;
        let name = OsStr::from_bytes(b"foo.txt");
        let parent = 42;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request {
            uid,
            gid,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_mkdir()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::eq(uid),
                    predicate::eq(gid),
                )
                .returning(|_, _, _, _, _| Err(libc::EPERM));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .mkdir(request, parent, name, (libc::S_IFDIR | mode).into(), 0)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    // The mkdir succeeds, but fetching attributes afterwards fails
    #[test]
    fn getattr_eio() {
        let mode: u16 = 0o755;
        let name = OsStr::from_bytes(b"foo");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request {
            uid,
            gid,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_mkdir()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::eq(uid),
                    predicate::eq(gid),
                )
                .returning(move |_, _, _, _, _| {
                    Ok(FileDataMut::new_for_tests(Some(parent), ino))
                });
            mock_fs
                .expect_getattr()
                .times(1)
                .return_const(Err(libc::EIO));
            mock_fs
                .expect_inactive()
                .withf(move |fd| fd.ino() == ino)
                .times(1)
                .return_const(());
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .mkdir(request, parent, name, (libc::S_IFDIR | mode).into(), 0)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    #[test]
    fn ok() {
        let mode: u16 = 0o755;
        let name = OsStr::from_bytes(b"foo");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request {
            uid,
            gid,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_mkdir()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::eq(uid),
                    predicate::eq(gid),
                )
                .returning(move |_, _, _, _, _| {
                    Ok(FileDataMut::new_for_tests(Some(parent), ino))
                });
            mock_fs.expect_getattr().times(1).return_const(Ok(GetAttr {
                ino,
                size: 0,
                bytes: 0,
                atime: Timespec { sec: 0, nsec: 0 },
                mtime: Timespec { sec: 0, nsec: 0 },
                ctime: Timespec { sec: 0, nsec: 0 },
                birthtime: Timespec { sec: 0, nsec: 0 },
                mode: Mode(mode | libc::S_IFDIR),
                nlink: 1,
                uid,
                gid,
                blksize: 4096,
                rdev: 0,
                flags: 0,
            }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .mkdir(request, parent, name, (libc::S_IFDIR | mode).into(), 0)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::Directory);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 0);
        assert_eq!(reply.attr.blksize, 4096);
        assert_cached(&fusefs, parent, name, ino);
    }
}

mod mknod {
    use super::*;

    #[test]
    fn blk() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;
        let rdev = 69;

        let request = Request {
            uid,
            gid,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_mkblock()
                .times(1)
                .in_sequence(&mut seq)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::eq(uid),
                    predicate::eq(gid),
                    predicate::eq(rdev),
                )
                .returning(move |_, _, _, _, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .times(1)
                .in_sequence(&mut seq)
                .return_const(Ok(GetAttr {
                    ino,
                    size: 0,
                    bytes: 0,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFBLK),
                    nlink: 1,
                    uid,
                    gid,
                    rdev,
                    blksize: 4096,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .mknod(request, parent, name, (libc::S_IFBLK | mode).into(), rdev)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::BlockDevice);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 69);
        assert_cached(&fusefs, parent, name, ino);
    }

    #[test]
    fn char() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;
        let rdev = 69;

        let request = Request {
            uid,
            gid,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_mkchar()
                .times(1)
                .in_sequence(&mut seq)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::eq(uid),
                    predicate::eq(gid),
                    predicate::eq(rdev),
                )
                .returning(move |_, _, _, _, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .times(1)
                .in_sequence(&mut seq)
                .return_const(Ok(GetAttr {
                    ino,
                    size: 0,
                    bytes: 0,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFCHR),
                    nlink: 1,
                    uid,
                    gid,
                    rdev,
                    blksize: 4096,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .mknod(request, parent, name, (libc::S_IFCHR | mode).into(), rdev)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::CharDevice);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.rdev, 69);
        assert_cached(&fusefs, parent, name, ino);
    }

    #[test]
    fn eperm() {
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.pipe");
        let parent = 42;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request {
            uid,
            gid,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_mkfifo()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::eq(uid),
                    predicate::eq(gid),
                )
                .returning(|_, _, _, _, _| Err(libc::EPERM));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .mknod(request, parent, name, (libc::S_IFIFO | mode).into(), 0)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    #[test]
    fn fifo() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.pipe");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request {
            uid,
            gid,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_mkfifo()
                .times(1)
                .in_sequence(&mut seq)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::eq(uid),
                    predicate::eq(gid),
                )
                .returning(move |_, _, _, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .times(1)
                .in_sequence(&mut seq)
                .return_const(Ok(GetAttr {
                    ino,
                    size: 0,
                    bytes: 0,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFIFO),
                    nlink: 1,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .mknod(request, parent, name, (libc::S_IFIFO | mode).into(), 0)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::NamedPipe);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_cached(&fusefs, parent, name, ino);
    }

    #[test]
    fn sock() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        let name = OsStr::from_bytes(b"foo.sock");
        let parent = 42;
        let ino = 43;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request {
            uid,
            gid,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_mksock()
                .times(1)
                .in_sequence(&mut seq)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::eq(uid),
                    predicate::eq(gid),
                )
                .returning(move |_, _, _, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .times(1)
                .in_sequence(&mut seq)
                .return_const(Ok(GetAttr {
                    ino,
                    size: 0,
                    bytes: 0,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFSOCK),
                    nlink: 1,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .mknod(request, parent, name, (libc::S_IFSOCK | mode).into(), 0)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::Socket);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_cached(&fusefs, parent, name, ino);
    }
}

mod read {
    use bfffs_core::SGList;
    use divbuf::*;

    use super::*;

    #[test]
    fn eio() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 0;
        let len = 1024;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_read()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(ofs),
                    predicate::eq(len as usize),
                )
                .return_const(Err(libc::EIO));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .read(request, ino, fh, ofs, len)
            .now_or_never()
            .unwrap()
            .err()
            .unwrap();
        assert_eq!(reply, libc::EIO.into());
    }

    // A Read past eof should return nothing
    #[test]
    fn eof() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 2048;
        let len = 1024;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_read()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(ofs),
                    predicate::eq(len as usize),
                )
                .return_const(Ok(SGList::new()));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .read(request, ino, fh, ofs, len)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert!(reply.data.is_empty());
    }

    // A read of one block or fewer
    #[test]
    fn small() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 2048;
        let len = 1024;
        const DATA: &[u8] = &[0u8, 1, 2, 3, 4, 5];

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_read()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(ofs),
                    predicate::eq(len as usize),
                )
                .returning(|_ino, _ofs, _len| {
                    let dbs = DivBufShared::from(DATA);
                    let db = dbs.try_const().unwrap();
                    Ok(vec![db])
                });
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .read(request, ino, fh, ofs, len)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(&reply.data[..], DATA);
    }

    // A large read from multiple blocks will use a scatter-gather list
    #[test]
    fn large() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 2048;
        let len = 1024;
        const DATA0: &[u8] = &[0u8, 1, 2, 3, 4, 5];
        const DATA1: &[u8] = &[6u8, 7, 8, 9, 10, 11];

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_read()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(ofs),
                    predicate::eq(len as usize),
                )
                .returning(|_ino, _ofs, _len| {
                    let dbs0 = DivBufShared::from(DATA0);
                    let db0 = dbs0.try_const().unwrap();
                    let dbs1 = DivBufShared::from(DATA1);
                    let db1 = dbs1.try_const().unwrap();
                    Ok(vec![db0, db1])
                });
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .read(request, ino, fh, ofs, len)
            .now_or_never()
            .unwrap()
            .unwrap();
        // fuse3 doesn't work with scatter-gather reads; we have to
        // copy the buffers into one
        // https://github.com/Sherlock-Holo/fuse3/issues/13
        assert_eq!(&reply.data[0..6], DATA0);
        assert_eq!(&reply.data[6..12], DATA1);
    }
}

mod readdir {
    use futures::stream;

    use super::*;

    /// A directory containing one file of every file type recognized by
    /// rust-fuse
    // libc's ino type could be either u32 or u64, depending on which
    // version of freebsd we're targeting.
    #[allow(clippy::useless_conversion)]
    #[test]
    fn all_file_types() {
        let fh = 0xdeadbeef;
        // libc's ino type could be either u32 or u64, depending on which
        // version of freebsd we're targeting.
        let ofs = 0;
        let mut dotname = [0; 256];
        dotname[0] = '.' as libc::c_char;
        let dot_ino = 42u32;
        let dot_ofs = 0;
        let mut regname = [0; 256];
        regname[0] = 'r' as libc::c_char;
        let reg_ino = 43u32;
        let reg_ofs = 1;
        let mut charname = [0; 256];
        charname[0] = 'c' as libc::c_char;
        let char_ino = 43u32;
        let char_ofs = 2;
        let mut blockname = [0; 256];
        blockname[0] = 'b' as libc::c_char;
        let block_ino = 43u32;
        let block_ofs = 3;
        let mut pipename = [0; 256];
        pipename[0] = 'p' as libc::c_char;
        let pipe_ino = 43u32;
        let pipe_ofs = 4;
        let mut symlinkname = [0; 256];
        symlinkname[0] = 'l' as libc::c_char;
        let symlink_ino = 43u32;
        let symlink_ofs = 5;
        let mut sockname = [0; 256];
        sockname[0] = 's' as libc::c_char;
        let sock_ino = 43u32;
        let sock_ofs = 6;
        let contents = vec![
            Ok((
                libc::dirent {
                    d_fileno: dot_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type:   libc::DT_DIR,
                    d_name:   dotname,
                    d_namlen: 1,
                },
                dot_ofs,
            )),
            Ok((
                libc::dirent {
                    d_fileno: reg_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type:   libc::DT_REG,
                    d_name:   regname,
                    d_namlen: 1,
                },
                reg_ofs,
            )),
            Ok((
                libc::dirent {
                    d_fileno: char_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type:   libc::DT_CHR,
                    d_name:   charname,
                    d_namlen: 1,
                },
                char_ofs,
            )),
            Ok((
                libc::dirent {
                    d_fileno: block_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type:   libc::DT_BLK,
                    d_name:   blockname,
                    d_namlen: 1,
                },
                block_ofs,
            )),
            Ok((
                libc::dirent {
                    d_fileno: pipe_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type:   libc::DT_FIFO,
                    d_name:   pipename,
                    d_namlen: 1,
                },
                pipe_ofs,
            )),
            Ok((
                libc::dirent {
                    d_fileno: symlink_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type:   libc::DT_LNK,
                    d_name:   symlinkname,
                    d_namlen: 1,
                },
                symlink_ofs,
            )),
            Ok((
                libc::dirent {
                    d_fileno: sock_ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type:   libc::DT_SOCK,
                    d_name:   sockname,
                    d_namlen: 1,
                },
                sock_ofs,
            )),
        ];

        let request = Request::default();

        let fusefs = make_mock_fs(move |mock_fs| {
            mock_fs
                .expect_readdir()
                .times(1)
                .with(
                    predicate::function(move |h: &FileData| {
                        u64::from(dot_ino) == h.ino()
                    }),
                    predicate::eq(ofs),
                )
                .return_once(move |_, _| {
                    Box::pin(stream::iter(contents.into_iter()))
                });
        });

        fusefs.files.lock().unwrap().insert(
            dot_ino.into(),
            FileDataMut::new_for_tests(Some(1), dot_ino.into()),
        );
        #[rustfmt::skip]
        let reply = fusefs
            .readdir(request, dot_ino.into(), fh, ofs)
            .now_or_never()
            .unwrap()                   // Result<_>
            .unwrap()                   // ReplyDirectory
            .entries                    // Stream
            .try_collect::<Vec<_>>()    // Future<Result<Vec<_>>>
            .now_or_never()             // Option<Result<Vec<_>>>
            .unwrap()                   // Result<Vec<_>>
            .unwrap()                   // Vec<_>>
            ;
        assert_eq!(reply[0].inode, u64::from(dot_ino));
        assert_eq!(reply[0].kind, FileType::Directory);
        assert_eq!(reply[0].name, OsStr::from_bytes(b"."));
        assert_eq!(reply[1].inode, u64::from(reg_ino));
        assert_eq!(reply[1].kind, FileType::RegularFile);
        assert_eq!(reply[1].name, OsStr::from_bytes(b"r"));
        assert_eq!(reply[2].inode, u64::from(char_ino));
        assert_eq!(reply[2].kind, FileType::CharDevice);
        assert_eq!(reply[2].name, OsStr::from_bytes(b"c"));
        assert_eq!(reply[3].inode, u64::from(block_ino));
        assert_eq!(reply[3].kind, FileType::BlockDevice);
        assert_eq!(reply[3].name, OsStr::from_bytes(b"b"));
        assert_eq!(reply[4].inode, u64::from(pipe_ino));
        assert_eq!(reply[4].kind, FileType::NamedPipe);
        assert_eq!(reply[4].name, OsStr::from_bytes(b"p"));
        assert_eq!(reply[5].inode, u64::from(symlink_ino));
        assert_eq!(reply[5].kind, FileType::Symlink);
        assert_eq!(reply[5].name, OsStr::from_bytes(b"l"));
        assert_eq!(reply[6].inode, u64::from(sock_ino));
        assert_eq!(reply[6].kind, FileType::Socket);
        assert_eq!(reply[6].name, OsStr::from_bytes(b"s"));
        assert_eq!(reply.len(), 7);
    }

    #[test]
    fn eio() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 0;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_readdir()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(ofs),
                )
                .returning(|_, _| {
                    Box::pin(stream::iter(vec![Err(libc::EIO)].into_iter()))
                });
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(Some(1), ino));
        #[rustfmt::skip]
        let reply = fusefs
            .readdir(request, ino, fh, ofs)
            .now_or_never()
            .unwrap()                   // Result<_>
            .unwrap()                   // ReplyDirectory
            .entries                    // Stream
            .try_collect::<Vec<_>>()    // Future<Result<Vec<_>>>
            .now_or_never()             // Option<Result<Vec<_>>>
            .unwrap()                   // Result<Vec<_>>
        ;
        assert_eq!(reply, Err(libc::EIO.into()));
    }

    /// A directory containing nothing but "." and ".."
    // libc's ino type could be either u32 or u64, depending on which
    // version of freebsd we're targeting.
    #[allow(clippy::useless_conversion)]
    #[test]
    fn empty() {
        let fh = 0xdeadbeef;
        let ino = 42u32;
        let parent = 41u32;
        let ofs = 0;
        let mut dotname = [0; 256];
        dotname[0] = '.' as libc::c_char;
        let mut dotdotname = [0; 256];
        dotdotname[0] = '.' as libc::c_char;
        dotdotname[1] = '.' as libc::c_char;
        let contents = vec![
            Ok((
                libc::dirent {
                    d_fileno: ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type:   libc::DT_DIR,
                    d_name:   dotname,
                    d_namlen: 1,
                },
                0,
            )),
            Ok((
                libc::dirent {
                    d_fileno: parent.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type:   libc::DT_DIR,
                    d_name:   dotdotname,
                    d_namlen: 2,
                },
                1,
            )),
        ];

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_readdir()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        u64::from(ino) == fd.ino()
                    }),
                    predicate::eq(ofs),
                )
                .return_once(move |_, _| {
                    Box::pin(stream::iter(contents.into_iter()))
                });
        });

        fusefs.files.lock().unwrap().insert(
            ino.into(),
            FileDataMut::new_for_tests(Some(1), ino.into()),
        );
        #[rustfmt::skip]
        let reply = fusefs
            .readdir(request, ino.into(), fh, ofs)
            .now_or_never()
            .unwrap()                   // Result<_>
            .unwrap()                   // ReplyDirectory
            .entries                    // Stream
            .try_collect::<Vec<_>>()    // Future<Result<Vec<_>>>
            .now_or_never()             // Option
            .unwrap()                   // Result<Vec<_>>
            .unwrap()                   // Vec<_>>
            ;
        assert_eq!(reply[0].inode, u64::from(ino));
        assert_eq!(reply[0].kind, FileType::Directory);
        assert_eq!(reply[0].name, OsStr::from_bytes(b"."));
        assert_eq!(reply[1].inode, u64::from(parent));
        assert_eq!(reply[1].kind, FileType::Directory);
        assert_eq!(reply[1].name, OsStr::from_bytes(b".."));
        assert_eq!(reply.len(), 2);
    }

    /// If fuse3's internal buffer runs out of space, it will terminate early
    /// and drop the stream.  Nothing bad should happen.
    // libc's ino type could be either u32 or u64, depending on which
    // version of freebsd we're targeting.
    #[allow(clippy::useless_conversion)]
    #[test]
    fn out_of_space() {
        let fh = 0xdeadbeef;
        let ino = 42u32;
        let parent = 41u32;
        let ofs = 0;
        let mut dotname = [0; 256];
        dotname[0] = '.' as libc::c_char;
        let mut dotdotname = [0; 256];
        dotdotname[0] = '.' as libc::c_char;
        dotdotname[1] = '.' as libc::c_char;
        let contents = vec![
            Ok((
                libc::dirent {
                    d_fileno: ino.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type:   libc::DT_DIR,
                    d_name:   dotname,
                    d_namlen: 1,
                },
                0,
            )),
            Ok((
                libc::dirent {
                    d_fileno: parent.into(),
                    d_reclen: mem::size_of::<libc::dirent>() as u16,
                    d_type:   libc::DT_DIR,
                    d_name:   dotdotname,
                    d_namlen: 2,
                },
                1,
            )),
        ];

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_readdir()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        u64::from(ino) == fd.ino()
                    }),
                    predicate::eq(ofs),
                )
                .return_once(move |_, _| {
                    Box::pin(stream::iter(contents.into_iter()))
                });
        });

        fusefs.files.lock().unwrap().insert(
            ino.into(),
            FileDataMut::new_for_tests(Some(1), ino.into()),
        );
        let mut entries = fusefs
            .readdir(request, ino.into(), fh, ofs)
            .now_or_never()
            .unwrap() // Result<_>
            .unwrap() // ReplyDirectory
            .entries; // Stream
        let first_dirent =
            entries.try_next().now_or_never().unwrap().unwrap().unwrap();
        assert_eq!(first_dirent.inode, u64::from(ino));
        assert_eq!(first_dirent.kind, FileType::Directory);
        assert_eq!(first_dirent.name, OsStr::from_bytes(b"."));
        drop(entries);
    }
}

mod readlink {
    use super::*;

    #[test]
    fn enoent() {
        let ino = 42;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_readlink()
                .times(1)
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .return_const(Err(libc::ENOENT));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .readlink(request, ino)
            .now_or_never()
            .unwrap()
            .err()
            .unwrap();
        assert_eq!(reply, libc::ENOENT.into());
    }

    #[test]
    fn ok() {
        let ino = 42;
        let name = OsStr::from_bytes(b"some_file.txt");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_readlink()
                .times(1)
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .return_const(Ok(name.to_owned()));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .readlink(request, ino)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.data, Bytes::copy_from_slice(name.as_bytes()))
    }
}

mod rename {
    use super::*;

    // Rename a directory
    #[test]
    fn dir() {
        let parent = 42;
        let newparent = 43;
        let ino = 44;
        let name = OsStr::from_bytes(b"foo");
        let newname = OsStr::from_bytes(b"bar");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_rename()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(name),
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == newparent
                    }),
                    predicate::eq(None),
                    predicate::eq(newname),
                )
                .return_once(move |_, _, _, _, _, _| Ok(ino));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(newparent, FileDataMut::new_for_tests(Some(1), newparent));
        fusefs
            .names
            .lock()
            .unwrap()
            .insert((parent, name.to_owned()), ino);
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(Some(parent), ino));
        let reply = fusefs
            .rename(request, parent, name, newparent, newname)
            .now_or_never()
            .unwrap();
        assert!(reply.is_ok());
        assert_not_cached(&fusefs, parent, name, None);
        assert_eq!(
            Some(newparent),
            fusefs.files.lock().unwrap().get(&ino).unwrap().parent()
        );
    }

    // Rename fails because the src is a directory but the dst is not.
    #[test]
    fn enotdir() {
        let parent = 42;
        let newparent = 43;
        let ino = 44;
        let dst_ino = 45;
        let name = OsStr::from_bytes(b"foo");
        let newname = OsStr::from_bytes(b"bar");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_rename()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(name),
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == newparent
                    }),
                    predicate::eq(Some(dst_ino)),
                    predicate::eq(newname),
                )
                .return_const(Err(libc::ENOTDIR));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(newparent, FileDataMut::new_for_tests(None, newparent));
        fusefs
            .names
            .lock()
            .unwrap()
            .insert((parent, name.to_owned()), ino);
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(Some(parent), ino));
        fusefs.files.lock().unwrap().insert(
            dst_ino,
            FileDataMut::new_for_tests(Some(newparent), dst_ino),
        );
        fusefs
            .names
            .lock()
            .unwrap()
            .insert((newparent, newname.to_owned()), dst_ino);
        let reply = fusefs
            .rename(request, parent, name, newparent, newname)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::ENOTDIR.into()));
        assert_cached(&fusefs, newparent, newname, dst_ino);
        assert_cached(&fusefs, parent, name, ino);
    }

    // It should not be possible to create directory loops
    #[test]
    fn dirloop() {
        let parent = 42;
        let child = 43;
        let name = OsStr::from_bytes(b"parent");

        let request = Request::default();

        let fusefs = make_mock_fs(|_| ());

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(child, FileDataMut::new_for_tests(Some(parent), child));
        fusefs
            .names
            .lock()
            .unwrap()
            .insert((1, name.to_owned()), parent);
        let reply = fusefs
            .rename(request, 1, name, child, name)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EINVAL.into()));
        assert_not_cached(&fusefs, child, name, None);
        assert_cached(&fusefs, 1, name, parent);
        assert_eq!(
            Some(1),
            fusefs.files.lock().unwrap().get(&parent).unwrap().parent()
        );
    }

    // It should not be possible to create directory loops
    #[test]
    fn dirloop_grandchild() {
        let grandparent = 41;
        let parent = 42;
        let child = 43;
        let name = OsStr::from_bytes(b"grandparent");

        let request = Request::default();

        let fusefs = make_mock_fs(|_| ());

        fusefs.files.lock().unwrap().insert(
            grandparent,
            FileDataMut::new_for_tests(Some(1), grandparent),
        );
        fusefs.files.lock().unwrap().insert(
            parent,
            FileDataMut::new_for_tests(Some(grandparent), parent),
        );
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(child, FileDataMut::new_for_tests(Some(parent), child));
        fusefs
            .names
            .lock()
            .unwrap()
            .insert((1, name.to_owned()), grandparent);
        let reply = fusefs
            .rename(request, 1, name, child, name)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EINVAL.into()));
        assert_not_cached(&fusefs, child, name, None);
        assert_cached(&fusefs, 1, name, grandparent);
        assert_eq!(
            Some(1),
            fusefs
                .files
                .lock()
                .unwrap()
                .get(&grandparent)
                .unwrap()
                .parent()
        );
    }

    // Rename a regular file
    #[test]
    fn regular_file() {
        let parent = 42;
        let newparent = 43;
        let ino = 44;
        let name = OsStr::from_bytes(b"foo");
        let newname = OsStr::from_bytes(b"bar");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_rename()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(name),
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == newparent
                    }),
                    predicate::eq(None),
                    predicate::eq(newname),
                )
                .return_const(Ok(ino));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(newparent, FileDataMut::new_for_tests(Some(1), newparent));
        fusefs
            .names
            .lock()
            .unwrap()
            .insert((parent, name.to_owned()), ino);
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .rename(request, parent, name, newparent, newname)
            .now_or_never()
            .unwrap();
        assert!(reply.is_ok());
        assert_not_cached(&fusefs, parent, name, None);
        assert_eq!(
            None,
            fusefs.files.lock().unwrap().get(&ino).unwrap().parent()
        );
    }
}

mod rmdir {
    use super::*;

    #[test]
    fn enotdir() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_rmdir()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                )
                .returning(move |_, _| Err(libc::ENOTDIR));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs.rmdir(request, parent, name).now_or_never().unwrap();
        assert_eq!(reply, Err(libc::ENOTDIR.into()));
    }

    #[test]
    fn ok() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_rmdir()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                )
                .returning(move |_, _| Ok(()));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs.rmdir(request, parent, name).now_or_never().unwrap();
        assert!(reply.is_ok());
        assert_not_cached(&fusefs, parent, name, None);
    }
}

mod setattr {
    use super::*;

    #[test]
    fn eperm() {
        let mode: u16 = 0o644;
        let ino = 42;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_setattr()
                .times(1)
                .withf(move |fd, attr| {
                    fd.ino() == ino &&
                        attr.size.is_none() &&
                        attr.atime.is_none() &&
                        attr.mtime.is_none() &&
                        attr.ctime.is_none() &&
                        attr.birthtime.is_none() &&
                        attr.perm == Some(mode) &&
                        attr.uid.is_none() &&
                        attr.gid.is_none() &&
                        attr.flags.is_none()
                })
                .return_const(Err(libc::EPERM));
        });

        let attr = SetAttr {
            mode: Some(mode),
            ..Default::default()
        };
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .setattr(request, ino, None, attr)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EPERM.into()));
    }

    #[test]
    fn chmod() {
        let mut seq = Sequence::new();
        let mode: u16 = 0o644;
        let ino = 42;
        let size = 500;
        let uid = 12345u32;
        let gid = 54321u32;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_setattr()
                .times(1)
                .in_sequence(&mut seq)
                .withf(move |fd, attr| {
                    fd.ino() == ino &&
                        attr.size.is_none() &&
                        attr.atime.is_none() &&
                        attr.mtime.is_none() &&
                        attr.ctime.is_none() &&
                        attr.birthtime.is_none() &&
                        attr.perm == Some(mode) &&
                        attr.uid.is_none() &&
                        attr.gid.is_none() &&
                        attr.flags.is_none()
                })
                .return_const(Ok(()));
            mock_fs
                .expect_getattr()
                .times(1)
                .in_sequence(&mut seq)
                .return_const(Ok(GetAttr {
                    ino,
                    size,
                    bytes: size,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFREG),
                    nlink: 1,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 16384,
                    flags: 0,
                }));
        });

        let attr = SetAttr {
            mode: Some(mode),
            ..Default::default()
        };
        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .setattr(request, ino, None, attr)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, size);
        assert_eq!(reply.attr.kind, FileType::RegularFile);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
        assert_eq!(reply.attr.blksize, 16384);
    }
}

mod setxattr {
    use super::*;

    #[test]
    fn value_erofs() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_setextattr()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(ExtAttrNamespace::System),
                    predicate::eq(OsStr::from_bytes(b"md5")),
                    predicate::eq(&v[..]),
                )
                .return_const(Err(libc::EROFS));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .setxattr(request, ino, packed_name, v, 0, 0)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EROFS.into()));
    }

    #[test]
    fn value_ok() {
        let ino = 42;
        let packed_name = OsStr::from_bytes(b"system.md5");
        let v = b"ed7e85e23a86d29980a6de32b082fd5b";

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_setextattr()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(ExtAttrNamespace::System),
                    predicate::eq(OsStr::from_bytes(b"md5")),
                    predicate::eq(&v[..]),
                )
                .return_const(Ok(()));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .setxattr(request, ino, packed_name, v, 0, 0)
            .now_or_never()
            .unwrap();
        assert!(reply.is_ok());
    }
}

mod statfs {
    use super::*;

    #[test]
    fn eio() {
        let ino = 42;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_statvfs()
                .times(1)
                .return_const(Err(libc::EIO));
        });

        let reply = fusefs.statfs(request, ino).now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EIO.into()));
    }

    #[test]
    fn ok() {
        let ino = 42;

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_statvfs()
                .times(1)
                .return_const(Ok(libc::statvfs {
                    f_bavail:  300000,
                    f_bfree:   200000,
                    f_blocks:  100000,
                    f_favail:  30000,
                    f_ffree:   20000,
                    f_files:   10000,
                    f_bsize:   4096,
                    f_flag:    0,
                    f_frsize:  512,
                    f_fsid:    0,
                    f_namemax: 1000,
                }));
        });

        let reply =
            fusefs.statfs(request, ino).now_or_never().unwrap().unwrap();
        assert_eq!(reply.blocks, 100000);
        assert_eq!(reply.bfree, 200000);
        assert_eq!(reply.bavail, 300000);
        assert_eq!(reply.files, 10000);
        assert_eq!(reply.ffree, 20000);
        assert_eq!(reply.bsize, 4096);
        assert_eq!(reply.namelen, 1000);
        assert_eq!(reply.frsize, 512);
    }
}

mod symlink {
    use super::*;

    #[test]
    fn eloop() {
        let name = OsStr::from_bytes(b"foo");
        let mode: u16 = 0o755;
        let parent = 42;

        let request = Request {
            uid: 12345,
            gid: 12345,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_symlink()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::always(),
                    predicate::always(),
                    predicate::eq(name),
                )
                .returning(|_, _, _, _, _, _| Err(libc::ELOOP));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .symlink(request, parent, name, name)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::ELOOP.into()));
        assert_not_cached(&fusefs, parent, name, None);
    }

    #[test]
    fn ok() {
        let name = OsStr::from_bytes(b"foo");
        let mode: u16 = 0o755;
        let parent = 42;
        let ino = 43;
        let uid = 12345;
        let gid = 54321;

        let request = Request {
            uid,
            gid,
            ..Default::default()
        };

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_symlink()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::eq(name),
                    predicate::eq(mode),
                    predicate::always(),
                    predicate::always(),
                    predicate::eq(name),
                )
                .returning(move |_, _, _, _, _, _| {
                    Ok(FileDataMut::new_for_tests(None, ino))
                });
            mock_fs
                .expect_getattr()
                .with(predicate::function(move |fd: &FileData| fd.ino() == ino))
                .return_const(Ok(GetAttr {
                    ino,
                    size: 0,
                    bytes: 0,
                    atime: Timespec { sec: 0, nsec: 0 },
                    mtime: Timespec { sec: 0, nsec: 0 },
                    ctime: Timespec { sec: 0, nsec: 0 },
                    birthtime: Timespec { sec: 0, nsec: 0 },
                    mode: Mode(mode | libc::S_IFLNK),
                    nlink: 1,
                    uid,
                    gid,
                    blksize: 4096,
                    rdev: 0,
                    flags: 0,
                }));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply = fusefs
            .symlink(request, parent, name, name)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.attr.ino, ino);
        assert_eq!(reply.attr.size, 0);
        assert_eq!(reply.attr.kind, FileType::Symlink);
        assert_eq!(reply.attr.perm, mode);
        assert_eq!(reply.attr.nlink, 1);
        assert_eq!(reply.attr.uid, uid);
        assert_eq!(reply.attr.gid, gid);
    }
}

mod unlink {
    use super::*;

    #[test]
    fn eisdir() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_unlink()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::always(),
                    predicate::eq(name),
                )
                .returning(move |_, _, _| Err(libc::EISDIR));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply =
            fusefs.unlink(request, parent, name).now_or_never().unwrap();
        assert_eq!(reply, Err(libc::EISDIR.into()));
    }

    #[test]
    fn ok() {
        let parent = 42;
        let name = OsStr::from_bytes(b"foo");

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_unlink()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| {
                        fd.ino() == parent
                    }),
                    predicate::always(),
                    predicate::eq(name),
                )
                .returning(move |_, _, _| Ok(()));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(parent, FileDataMut::new_for_tests(Some(1), parent));
        let reply =
            fusefs.unlink(request, parent, name).now_or_never().unwrap();
        assert!(reply.is_ok());
        assert_not_cached(&fusefs, parent, name, None);
    }
}

mod write {
    use super::*;

    #[test]
    fn erofs() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 0;
        const DATA: &[u8] = &[0u8, 1, 2, 3, 4, 5];

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_write()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(ofs),
                    predicate::eq(DATA),
                    predicate::always(),
                )
                .return_const(Err(libc::EROFS));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .write(request, ino, fh, ofs, DATA, 0)
            .now_or_never()
            .unwrap();
        assert_eq!(reply, Err(libc::EROFS.into()));
    }

    // A read of one block or fewer
    #[test]
    fn ok() {
        let fh = 0xdeadbeef;
        let ino = 42;
        let ofs = 2048;
        const DATA: &[u8] = &[0u8, 1, 2, 3, 4, 5];

        let request = Request::default();

        let fusefs = make_mock_fs(|mock_fs| {
            mock_fs
                .expect_write()
                .times(1)
                .with(
                    predicate::function(move |fd: &FileData| fd.ino() == ino),
                    predicate::eq(ofs),
                    predicate::eq(DATA),
                    predicate::always(),
                )
                .return_const(Ok(DATA.len() as u32));
        });

        fusefs
            .files
            .lock()
            .unwrap()
            .insert(ino, FileDataMut::new_for_tests(None, ino));
        let reply = fusefs
            .write(request, ino, fh, ofs, DATA, 0)
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.written, DATA.len() as u32);
    }
}
