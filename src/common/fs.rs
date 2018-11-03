// vim: tw=80
//! Common VFS implementation

use atomic::*;
use bitfield::*;
use crate::common::*;
#[cfg(not(test))] use crate::common::database::*;
#[cfg(test)] use crate::common::database_mock::DatabaseMock as Database;
#[cfg(test)] use crate::common::database_mock::ReadWriteFilesystem;
use crate::common::database::TreeID;
use crate::common::fs_tree::*;
use divbuf::{DivBufShared, DivBuf};
use futures::{
    Future,
    IntoFuture,
    Sink,
    Stream,
    future,
    stream,
    sync::{mpsc, oneshot}
};
use libc;
use std::{
    cmp,
    ffi::{OsStr, OsString},
    io,
    mem,
    os::unix::ffi::OsStrExt,
    sync::Arc,
};
use time;
use tokio_io_pool;

pub use self::fs_tree::ExtAttr;
pub use self::fs_tree::ExtAttrNamespace;

const RECORDSIZE: usize = 4 * 1024;    // Default record size 4KB

/// Generic Filesystem layer.
///
/// Bridges the synchronous with Tokio domains, and the system-independent with
/// system-dependent filesystem interfaces.
pub struct Fs {
    db: Arc<Database>,
    next_object: Atomic<u64>,
    handle: tokio_io_pool::Handle,
    tree: TreeID,
}

bitfield! {
    /// File mode, including permissions and file type
    #[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
    pub struct Mode(u16);
    impl Debug;
    pub perm, _: 11, 0;
}
impl Mode {
    // Access the `file_type` field without shifting, so we can compare it to
    // the libc::S_IF* constants.
    pub fn file_type(self) -> u16 {
        self.0 & libc::S_IFMT
    }
}

/// File attributes, as returned by `getattr`
#[derive(Debug, PartialEq)]
pub struct GetAttr {
    pub ino:        u64,
    /// File size in bytes
    pub size:       u64,
    /// File size in blocks
    pub blocks:     u64,
    /// access time
    pub atime:      time::Timespec,
    /// modification time
    pub mtime:      time::Timespec,
    /// change time
    pub ctime:      time::Timespec,
    /// birth time
    pub birthtime:  time::Timespec,
    /// File mode as returned by stat(2)
    pub mode: Mode,
    /// Link count
    pub nlink:      u64,
    /// user id
    pub uid:        u32,
    /// Group id
    pub gid:        u32,
    /// Device number, for device nodes only
    pub rdev:       u32,
    /// File flags
    pub flags:      u64,
}

/// File attributes, as set by `setattr`
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SetAttr {
    /// File size in bytes
    pub size:       Option<u64>,
    /// access time
    pub atime:      Option<time::Timespec>,
    /// modification time
    pub mtime:      Option<time::Timespec>,
    /// change time
    pub ctime:      Option<time::Timespec>,
    /// birth time
    pub birthtime:  Option<time::Timespec>,
    /// File permissions
    pub perm:       Option<u16>,
    /// user id
    pub uid:        Option<u32>,
    /// Group id
    pub gid:        Option<u32>,
    /// File flags
    pub flags:      Option<u64>,
}

/// Arguments for Fs::do_create
struct CreateArgs
{
    parent: u64,
    file_type: FileType,
    flags: u64,
    name: OsString,
    perm: u16,
    uid: u32,
    gid: u32,
    nlink: u64,
    // NB: this could be a Box<FnOnce> after bug 28796 is fixed
    // https://github.com/rust-lang/rust/issues/28796
    cb: Box<Fn(&Arc<ReadWriteFilesystem>, u64)
        -> Box<Future<Item=(), Error=Error> + Send + 'static> + Send >
}

impl CreateArgs {
    pub fn callback<F>(mut self, f: F) -> Self
        where F: Fn(&Arc<ReadWriteFilesystem>, u64)
        -> Box<Future<Item=(), Error=Error> + Send + 'static> + Send + 'static
    {
        self.cb = Box::new(f);
        self
    }

    fn default_cb(_: &Arc<ReadWriteFilesystem>, _: u64)
        -> Box<Future<Item=(), Error=Error> + Send + 'static>
    {
        Box::new(Ok(()).into_future())
    }

    // Enable once chflags(2) support comes in
    //pub fn flags(mut self, flags: u64) -> Self {
        //self.flags = flags;
        //self
    //}

    pub fn new(parent: u64, name: &OsStr, perm: u16, uid: u32,
               gid: u32, file_type: FileType) -> Self
    {
        let cb = Box::new(CreateArgs::default_cb);
        CreateArgs{parent, flags: 0, name: name.to_owned(), perm,
                   file_type, uid, gid, nlink: 1, cb}
    }

    pub fn nlink(mut self, nlink: u64) -> Self {
        self.nlink = nlink;
        self
    }
}

impl Fs {
    /// Delete an extended attribute
    pub fn deleteextattr(&self, ino: u64, ns: ExtAttrNamespace, name: &OsStr)
        -> Result<(), i32>
    {
        let (tx, rx) = oneshot::channel();
        let objkey = ObjKey::extattr(ns, &name);
        let name = name.to_owned();
        let key = FSKey::new(ino, objkey);
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                dataset.remove(key)
                .and_then(move |r| {
                    match r {
                        Some(FSValue::ExtAttr(ref old)) if old.namespace() == ns
                            && old.name() == name =>
                        {
                            // This is the xattr we're looking for
                            Box::new(Ok(()).into_future())
                                as Box<Future<Item=(), Error=Error> + Send>
                        },
                        Some(FSValue::ExtAttr(old)) =>
                        {
                            // Hash collision.  Put it back, and return ENOATTR
                            let value = FSValue::ExtAttr(old);
                            let fut = dataset.insert(key, value)
                            .and_then(|_| Err(Error::ENOATTR).into_future());
                            Box::new(fut)
                                as Box<Future<Item=(), Error=Error> + Send>
                        },
                        Some(FSValue::ExtAttrs(mut old)) => {
                            // There was previously a hash collision.
                            if let Some(i) = old.iter().position(|x| {
                                x.namespace() == ns && x.name() == name
                            }) {
                                old.swap_remove(i);
                                let v = if old.len() == 1 {
                                    // A 2-way collision; remove one
                                    FSValue::ExtAttr(old.pop().unwrap())
                                } else {
                                    // A 3 (or more) way collision; remove one
                                    FSValue::ExtAttrs(old)
                                };
                                let fut = dataset.insert(key, v)
                                .map(|_| ());
                                Box::new(fut)
                                    as Box<Future<Item=(), Error=Error> + Send>
                            } else {
                                // A 3 (or more) way hash collision between the
                                // accessed attribute and at least two different
                                // ones.
                                let v = FSValue::ExtAttrs(old);
                                let fut = dataset.insert(key, v)
                                .and_then(|_| Err(Error::ENOATTR).into_future());
                                Box::new(fut)
                                    as Box<Future<Item=(), Error=Error> + Send>
                            }
                        },
                        None => {
                            Box::new(Err(Error::ENOATTR).into_future())
                                as Box<Future<Item=(), Error=Error> + Send>
                        },
                        x => panic!("Unexpected value {:?} for key {:?}",
                                    x, key)
                    }
                })
            }).map_err(|e| e.into())
            .then(|r| {
                tx.send(r).unwrap();
                Ok(()).into_future()
            })
        ).unwrap();
        rx.wait().unwrap()
    }

    fn do_create(&self, args: CreateArgs)
        -> Result<u64, i32>
    {
        let (tx, rx) = oneshot::channel();
        let ino = self.next_object();
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        let parent_dirent_objkey = ObjKey::dir_entry(&args.name);
        let parent_dirent = Dirent {
            ino,
            dtype: args.file_type.dtype(),
            name:   args.name
        };
        let parent_dirent_key = FSKey::new(args.parent, parent_dirent_objkey);
        let parent_dirent_value = FSValue::DirEntry(parent_dirent);

        let now = time::get_time();
        let inode = Inode {
            size: 0,
            nlink: args.nlink,
            flags: args.flags,
            atime: now,
            mtime: now,
            ctime: now,
            birthtime: now,
            uid: args.uid,
            gid: args.gid,
            perm: args.perm,
            file_type: args.file_type
        };
        let inode_value = FSValue::Inode(inode);

        let cb = args.cb;
        self.handle.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                let extra_fut = cb(&dataset, ino);
                dataset.insert(inode_key, inode_value).join3(
                    dataset.insert(parent_dirent_key, parent_dirent_value),
                    extra_fut
                ).map(move |_| tx.send(Ok(ino)).unwrap())
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    /// Actually remove a directory, after all checks have passed
    fn do_rmdir(dataset: Arc<ReadWriteFilesystem>, parent: u64, ino: u64,
                dec_nlink: bool)
        -> impl Future<Item=(), Error=Error> + Send
    {
        // Outline:
        // 1) range_delete its key range
        // 2) Decrement the parent dir's link count

        // 1) range_delete its key range
        let ino_fut = dataset.range_delete(FSKey::obj_range(ino));

        // 2) Decrement the parent dir's link count
        let parent_ino_key = FSKey::new(parent, ObjKey::Inode);
        let nlink_fut = if dec_nlink {
            let fut = dataset.get(parent_ino_key)
            .and_then(move |r| {
                let mut value = r.unwrap();
                value.as_mut_inode().unwrap().nlink -= 1;
                dataset.insert(parent_ino_key, value)
                .map(|_| ())
            });
            Box::new(fut)
                as Box<Future<Item=_, Error=_> + Send>
        } else {
            Box::new(Ok(()).into_future())
                as Box<Future<Item=_, Error=_> + Send>
        };
        ino_fut.join(nlink_fut)
        .map(|_| ())
    }

    /// Unlink a file whose inode number is known and whose directory entry is
    /// already deleted.  Returns the new link count.
    fn do_unlink(dataset: Arc<ReadWriteFilesystem>, ino: u64)
        -> impl Future<Item=u64, Error=Error> + Send
    {
        // 1) Lookup the inode
        let key = FSKey::new(ino, ObjKey::Inode);
        dataset.get(key)
        .map(move |r| {
            match r {
                Some(v) => {
                    v.as_inode().unwrap().clone()
                },
                None => {
                    panic!("Orphan directory entry")
                },
            }
        }).and_then(move |mut iv| {
            if iv.nlink > 1 {
                // 2a) Decrement the link count
                iv.nlink -= 1;
                let nlink = iv.nlink;
                let fut = dataset.insert(key, FSValue::Inode(iv))
                .map(move |_| nlink);
                Box::new(fut) as Box<Future<Item=_, Error=_> + Send>
            } else {
                let dataset2 = dataset.clone();
                // 2b) delete its blob extents and blob extended attributes
                let extent_stream = dataset.range(FSKey::extent_range(ino))
                .filter_map(move |(_k, v)| {
                    if let Extent::Blob(be) = v.as_extent().unwrap()
                    {
                        Some(be.rid)
                    } else {
                        None
                    }
                });
                let extattr_stream = dataset.range(FSKey::extattr_range(ino))
                .filter_map(move |(_k, v)| {
                    // TODO: handle hash collisions.
                    match v {
                        FSValue::ExtAttr(ExtAttr::Blob(be)) => {
                            let s = Box::new(stream::once(Ok(be.extent.rid)))
                                as Box<Stream<Item=RID, Error=Error> + Send>;
                            Some(s)
                        },
                        FSValue::ExtAttrs(r) => {
                            let rids = r.iter().filter_map(|v| {
                                if let ExtAttr::Blob(be) = v {
                                    Some(be.extent.rid)
                                } else {
                                    None
                                }
                            }).collect::<Vec<_>>();
                            let s = Box::new(stream::iter_ok(rids))
                                as Box<Stream<Item=RID, Error=Error> + Send>;
                            Some(s)
                        }
                        _ => None
                    }
                }).flatten();
                let fut = extent_stream.chain(extattr_stream)
                .for_each(move |rid| dataset2.delete_blob(rid))
                .and_then(move |_| {
                    // 2c) range_delete its key range
                    dataset.range_delete(FSKey::obj_range(ino))
                }).map(|_| 0u64);
                Box::new(fut) as Box<Future<Item=_, Error=_> + Send>
            }
        })
    }

    pub fn new(database: Arc<Database>, handle: tokio_io_pool::Handle,
               tree: TreeID) -> Self
    {
        let (tx, rx) = oneshot::channel::<Option<FSKey>>();
        handle.spawn(
            database.fsread(tree, |dataset| {
                dataset.last_key()
                    .map(move |k| tx.send(k).unwrap())
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        let last_key = rx.wait().unwrap();
        let next_object = Atomic::new(last_key.unwrap().object() + 1);
        Fs{db: database, next_object, handle, tree}
    }

    fn next_object(&self) -> u64 {
        self.next_object.fetch_add(1, Ordering::Relaxed)
    }
}

impl Fs {
    pub fn create(&self, parent: u64, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> Result<u64, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Reg);
        self.do_create(create_args)
    }

    /// Dump a YAMLized representation of the filesystem's Tree to a plain
    /// `std::fs::File`.
    pub fn dump(&self, f: &mut io::Write) {
        self.db.dump(f, self.tree)
    }

    pub fn getattr(&self, ino: u64) -> Result<GetAttr, i32> {
        let (tx, rx) = oneshot::channel();
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                let key = FSKey::new(ino, ObjKey::Inode);
                dataset.get(key)
                .then(move |r| {
                    match r {
                        Ok(Some(v)) => {
                            let inode = v.as_inode().unwrap();
                            let rdev = match inode.file_type {
                                FileType::Char(x) | FileType::Block(x) => x,
                                _ => 0
                            };
                            let attr = GetAttr {
                                ino,
                                size: inode.size,
                                blocks: 0,
                                atime: inode.atime,
                                mtime: inode.mtime,
                                ctime: inode.ctime,
                                birthtime: inode.birthtime,
                                mode: Mode(inode.file_type.mode() | inode.perm),
                                nlink: inode.nlink,
                                uid: inode.uid,
                                gid: inode.gid,
                                rdev,
                                flags: inode.flags,
                            };
                            tx.send(Ok(attr))
                        },
                        Ok(None) => {
                            tx.send(Err(Error::ENOENT.into()))
                        },
                        Err(e) => {
                            tx.send(Err(e.into()))
                        }
                    }.unwrap();
                    future::ok::<(), Error>(())
                })
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    /// Retrieve the value of an extended attribute
    pub fn getextattr(&self, ino: u64, ns: ExtAttrNamespace, name: &OsStr)
        -> Result<DivBuf, i32>
    {
        let owned_name = name.to_owned();
        let (tx, rx) = oneshot::channel();
        let objkey = ObjKey::extattr(ns, &name);
        let key = FSKey::new(ino, objkey);
        type MyFut = Box<Future<Item=DivBuf, Error=Error> + Send>;
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                dataset.get(key)
                .then(move |r| {
                    match r {
                        Ok(Some(FSValue::ExtAttr(ref xattr)))
                            if xattr.namespace() == ns &&
                               xattr.name() == owned_name =>
                        {
                            // Found the right xattr
                            let fut = match xattr {
                                ExtAttr::Inline(iea) => {
                                    let buf = iea.extent.buf.try().unwrap();
                                    Box::new(Ok(buf).into_future()) as MyFut
                                },
                                ExtAttr::Blob(bea) => {
                                    let bfut = dataset.get_blob(bea.extent.rid)
                                        .map(|bdb| *bdb);
                                    Box::new(bfut) as MyFut
                                }
                            };
                            Box::new(fut) as MyFut
                        },
                        Ok(Some(FSValue::ExtAttrs(ref xattrs))) => {
                            // A bucket of multiple xattrs
                            assert!(xattrs.len() > 1);
                            if let Some(xattr) = xattrs.iter().find(|x| {
                                x.namespace() == ns && x.name() == owned_name
                            }) {
                                // Found the right one
                                let fut = match xattr {
                                    ExtAttr::Inline(iea) => {
                                        let buf = iea.extent.buf.try().unwrap();
                                        Box::new(Ok(buf).into_future()) as MyFut
                                    },
                                    ExtAttr::Blob(bea) => {
                                        let rid = bea.extent.rid;
                                        let bfut = dataset.get_blob(rid)
                                        .map(|bdb| *bdb);
                                        Box::new(bfut) as MyFut
                                    }
                                };
                                Box::new(fut) as MyFut
                            } else {
                                Box::new(Err(Error::ENOATTR).into_future())
                                    as MyFut
                            }
                        }
                        Err(e) => {
                            Box::new(Err(e).into_future()) as MyFut
                        }
                        _ => {
                            Box::new(Err(Error::ENOATTR).into_future()) as MyFut
                        },
                    }
                })
            }).then(move |r| {
                match r {
                    Ok(buf) => tx.send(Ok(buf)),
                    Err(e) => tx.send(Err(e.into()))
                }.unwrap();
                Ok(()).into_future()
            })
        ).unwrap();
        rx.wait().unwrap()
    }

    /// Retrieve the length of the value of an extended attribute
    pub fn getextattrlen(&self, ino: u64, ns: ExtAttrNamespace, name: &OsStr)
        -> Result<u32, i32>
    {
        let owned_name = name.to_owned();
        let (tx, rx) = oneshot::channel();
        let objkey = ObjKey::extattr(ns, &name);
        let key = FSKey::new(ino, objkey);
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                dataset.get(key)
                .then(move |r| {
                    let result = match r {
                        Ok(Some(FSValue::ExtAttr(ref xattr)))
                            if xattr.namespace() == ns &&
                               xattr.name() == owned_name =>
                        {
                            // Found the right xattr
                            let len = match xattr {
                                ExtAttr::Inline(iea) => {
                                    iea.extent.buf.len() as u32
                                },
                                ExtAttr::Blob(bea) => {
                                    bea.extent.lsize
                                }
                            };
                            Ok(len)
                        },
                        Ok(Some(FSValue::ExtAttrs(ref xattrs))) => {
                            // A bucket of multiple xattrs
                            assert!(xattrs.len() > 1);
                            if let Some(xattr) = xattrs.iter().find(|x| {
                                x.namespace() == ns && x.name() == owned_name
                            }) {
                                // Found the right one
                                let len = match xattr {
                                    ExtAttr::Inline(iea) => {
                                        iea.extent.buf.len() as u32
                                    },
                                    ExtAttr::Blob(bea) => {
                                        bea.extent.lsize
                                    }
                                };
                                Ok(len)
                            } else {
                                Err(Error::ENOATTR.into())
                            }
                        }
                        Err(e) => {
                            Err(e.into())
                        }
                        _ => {
                            Err(Error::ENOATTR.into())
                        },
                    };
                    tx.send(result).unwrap();
                    future::ok::<(), Error>(())
                })
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    /// Create a hardlink from `ino` to `parent/name`.
    pub fn link(&self, parent: u64, ino: u64, name: &OsStr) -> Result<u64, i32>
    {
        // Outline:
        // * Increase the target's link count
        // * Add the new directory entry
        let (tx, rx) = oneshot::channel();
        let name = name.to_owned();
        self.handle.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let inode_key = FSKey::new(ino, ObjKey::Inode);
                ds.get(inode_key)
                .and_then(move |r| {
                    let mut iv = r.unwrap().as_mut_inode().unwrap().clone();
                    iv.nlink += 1;
                    let dtype = iv.file_type.dtype();
                    // FUSE is single-threaded, so we don't have to worry that
                    // the target gets deleted before we increase its link
                    // count.  The real VFS will provide a held vnode rather than an
                    // inode.  So in neither case is there a race here.
                    let ifut = ds.insert(inode_key, FSValue::Inode(iv));

                    let dirent_objkey = ObjKey::dir_entry(&name);
                    let dirent = Dirent { ino, dtype, name };
                    let dirent_key = FSKey::new(parent, dirent_objkey);
                    let dirent_value = FSValue::DirEntry(dirent);
                    let dfut = ds.insert(dirent_key, dirent_value);

                    ifut.join(dfut)
                }).map(move |_| tx.send(Ok(ino)).unwrap())
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn lookup(&self, parent: u64, name: &OsStr) -> Result<u64, i32>
    {
        let (tx, rx) = oneshot::channel();
        let objkey = ObjKey::dir_entry(name);
        let owned_name = name.to_owned();
        let key = FSKey::new(parent, objkey);
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                dataset.get(key)
                .then(move |r| {
                    match r {
                        Ok(Some(v)) => {
                            // Verify that the direntry contains the right name
                            // TODO: deal with hash collisions
                            let de = v.as_direntry().unwrap();
                            assert_eq!(de.name, owned_name);
                            tx.send(Ok(de.ino))
                        },
                        Ok(None) => {
                            tx.send(Err(Error::ENOENT.into()))
                        },
                        Err(e) => {
                            tx.send(Err(e.into()))
                        }
                    }.unwrap();
                    future::ok::<(), Error>(())
                })
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    /// Retrieve a packed list of extended attribute names.
    ///
    /// # Parameters
    ///
    /// - `size`:       The expected length of list that will be returned
    /// - `f`:          A function that filters each extended attribute
    ///                 and packs it into an expandable buffer
    ///
    /// # Returns
    ///
    /// A buffer containing all extended attributes' names packed by `f`.
    pub fn listextattr<F>(&self, ino: u64, size: u32, f: F)
        -> Result<Vec<u8>, i32>
        where F: Fn(&mut Vec<u8>, &ExtAttr<RID>) + Send + 'static
    {
        let (tx, rx) = oneshot::channel();
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                let buf = Vec::with_capacity(size as usize);
                dataset.range(FSKey::extattr_range(ino))
                .fold(buf, move |mut buf, (k, v)| {
                    match v {
                        FSValue::ExtAttr(xattr) => f(&mut buf, &xattr),
                        FSValue::ExtAttrs(v) => {
                            for xattr in v {
                                f(&mut buf, &xattr);
                            }
                        },
                        _ => panic!("Unexpected value {:?} for key {:?}", v, k)
                    }
                    future::ok::<Vec<u8>, Error>(buf)
                }).map(move |buf| tx.send(Ok(buf)).unwrap())
            }).map_err(|e: Error| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    /// Like [`listextattr`](#method.listextattr), but it returns the length of
    /// the buffer that `listextattr` would return.
    ///
    /// # Parameters
    ///
    /// - `f`:          A function that filters each extended attribute
    ///                 and calculates its size as it would be packed by
    ///                 `listextattr`.
    ///
    /// # Returns
    ///
    /// The length of buffer that would be required for `listextattr`.
    pub fn listextattrlen<F>(&self, ino: u64, f: F) -> Result<u32, i32>
        where F: Fn(&ExtAttr<RID>) -> u32 + Send + 'static
    {
        let (tx, rx) = oneshot::channel();
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                dataset.range(FSKey::extattr_range(ino))
                .fold(0u32, move |mut len, (k, v)| {
                    len += match v {
                        FSValue::ExtAttr(xattr) => f(&xattr),
                        FSValue::ExtAttrs(v) => {
                            v.iter().map(|xattr| f(xattr)).sum()
                        },
                        _ => panic!("Unexpected value {:?} for key {:?}", v, k)
                    };
                    future::ok::<u32, Error>(len)
                }).map(move |l| tx.send(Ok(l)).unwrap())
            }).map_err(|e: Error| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn mkdir(&self, parent: u64, name: &OsStr, perm: u16, uid: u32,
                 gid: u32) -> Result<u64, i32>
    {
        let nlink = 2;  // One for the parent dir, and one for "."

        let f = move |dataset: &Arc<ReadWriteFilesystem>, ino| {
            let dot_dirent = Dirent {
                ino,
                dtype: libc::DT_DIR,
                name:  OsString::from(".")
            };
            let dot_dirent_objkey = ObjKey::dir_entry(OsStr::new("."));
            let dot_dirent_key = FSKey::new(ino, dot_dirent_objkey);
            let dot_dirent_value = FSValue::DirEntry(dot_dirent);

            let dotdot_dirent = Dirent {
                ino: parent,
                dtype: libc::DT_DIR,
                name:  OsString::from("..")
            };
            let dotdot_dirent_objkey = ObjKey::dir_entry(OsStr::new(".."));
            let dotdot_dirent_key = FSKey::new(ino, dotdot_dirent_objkey);
            let dotdot_dirent_value = FSValue::DirEntry(dotdot_dirent);

            let parent_inode_key = FSKey::new(parent, ObjKey::Inode);
            let dataset2 = dataset.clone();
            let nlink_fut = dataset.get(parent_inode_key)
                .and_then(move |r| {
                    let mut value = r.unwrap();
                    value.as_mut_inode().unwrap().nlink += 1;
                    dataset2.insert(parent_inode_key, value)
                });

            let fut = dataset.insert(dot_dirent_key, dot_dirent_value).join3(
                dataset.insert(dotdot_dirent_key, dotdot_dirent_value),
                nlink_fut)
            .map(|_| ());
            Box::new(fut) as Box<Future<Item=(), Error=Error> + Send>
        };

        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Dir)
        .nlink(nlink)
        .callback(f);

        self.do_create(create_args)
    }

    /// Make a block device
    pub fn mkblock(&self, parent: u64, name: &OsStr, perm: u16, uid: u32,
                   gid: u32, rdev: u32) -> Result<u64, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Block(rdev));
        self.do_create(create_args)
    }

    /// Make a character device
    pub fn mkchar(&self, parent: u64, name: &OsStr, perm: u16, uid: u32,
                  gid: u32, rdev: u32) -> Result<u64, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Char(rdev));
        self.do_create(create_args)
    }

    pub fn mkfifo(&self, parent: u64, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> Result<u64, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Fifo);
        self.do_create(create_args)
    }

    pub fn mksock(&self, parent: u64, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> Result<u64, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Socket);
        self.do_create(create_args)
    }

    /// Check that a directory is safe to delete
    fn ok_to_rmdir(ds: &ReadWriteFilesystem, ino: u64, parent: u64,
                   name: OsString)
        -> impl Future<Item=(), Error=Error> + Send
    {
        ds.range(FSKey::obj_range(ino))
        .fold(false, |found_inode, (_, v)| {
            if let Some(dirent) = v.as_direntry() {
                if dirent.name != OsStr::new(".") &&
                   dirent.name != OsStr::new("..") {
                    Err(Error::ENOTEMPTY).into_future()
                } else {
                    Ok(found_inode).into_future()
                }
            } else if let Some(inode) = v.as_inode() {
                // TODO: check permissions, file flags, etc
                // The VFS should've already checked that inode is a
                // directory.
                assert_eq!(inode.file_type, FileType::Dir,
                           "rmdir of a non-directory");
                assert_eq!(inode.nlink, 2,
                    "Hard links to directories are forbidden.  nlink={}",
                    inode.nlink);
                Ok(true).into_future()
            } else {
                // Probably an extended attribute or something.
                Ok(found_inode).into_future()
            }
        }).map(move |found_inode| {
            assert!(found_inode,
                concat!("Inode {} not found, but parent ",
                        "direntry {}:{:?} exists!"),
                ino, parent, name);
        })
    }

    pub fn read(&self, ino: u64, offset: u64, mut size: usize)
        -> Result<SGList, i32>
    {
        let (tx, rx) = oneshot::channel();
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        self.handle.spawn(
            self.db.fsread(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                // First lookup the inode to get the file size
                dataset.get(inode_key)
                .and_then(move |value| {
                    let fsize = value.unwrap().as_inode().unwrap().size;
                    // Truncate read to file size
                    size = size.min((fsize.saturating_sub(offset)) as usize);
                    let rs = RECORDSIZE as u64;
                    let nrecs = if size == 0 {
                        0
                    } else {
                        div_roundup(offset + size as u64, rs) - (offset / rs)
                    };
                    let baseoffset = offset - (offset % rs);
                    let futs = (0..nrecs).map(|rec| {
                        let dataset2 = dataset.clone();
                        let offs = baseoffset + rec * rs;
                        debug_assert!(fsize > offs);
                        let k = FSKey::new(ino, ObjKey::Extent(offs));
                        // Lookup the extent
                        dataset2.get(k)
                        .and_then(move |v| {
                            if let Some(item) = v {
                                match item.as_extent().unwrap() {
                                    Extent::Inline(ile) => {
                                        let buf = ile.buf.try().unwrap();
                                        Box::new(Ok(buf).into_future())
                                            as Box<Future<Item=_,
                                                          Error=_> + Send>
                                    },
                                    Extent::Blob(be) => {
                                        let bfut = dataset2.get_blob(be.rid)
                                            .map(|bdb| *bdb);
                                        Box::new(bfut)
                                            as Box<Future<Item=_,
                                                          Error=_> + Send>
                                    }
                                }
                            } else {
                                // No extent found; it's a hole
                                let db = if ZERO_REGION_LEN <= RECORDSIZE {
                                    ZERO_REGION.try().unwrap()
                                } else {
                                    let v = vec![0u8; RECORDSIZE];
                                    let dbs = DivBufShared::from(v);
                                    dbs.try().unwrap()
                                };
                                Box::new(Ok(db).into_future())
                                    as Box<Future<Item=DivBuf,
                                                  Error=Error> + Send>
                            }
                        }).map(move |mut db| {
                            if rec == 0 {
                                // Trim the beginning
                                db.split_to((offset - baseoffset) as usize);
                            }
                            if rec == nrecs - 1 {
                                // Trim the end
                                let end = RECORDSIZE
                                    - ((nrecs * rs) as usize - size);
                                if db.len() >= end {
                                    db.split_off(RECORDSIZE
                                        - ((nrecs * rs) as usize - size));
                                    db
                                } else {
                                    // A partial hole.  We got some data,
                                    // but not enough.  Copy to a new buffer
                                    let mut v = vec![0u8; end];
                                    v[0..db.len()].copy_from_slice(&db[..]);
                                    DivBufShared::from(v).try().unwrap()
                                }
                            } else {
                                db
                            }
                        })
                    }).collect::<Vec<_>>();
                    future::join_all(futs)
                    .map(|sglist| {
                        tx.send(Ok(sglist)).unwrap();
                    })
                })
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    // TODO: instead of the full size struct libc::dirent, use a variable size
    // structure in the mpsc channel
    pub fn readdir(&self, ino: u64, _fh: u64, soffs: i64)
        -> impl Iterator<Item=Result<(libc::dirent, i64), i32>>
    {
        // Big enough to fill a 4KB page with full-size dirents
        let chansize: usize = 14;
        let dirent_size = mem::size_of::<libc::dirent>() as u16;

        let (tx, rx) = mpsc::channel(chansize);
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                // NB: the next two lines can be replaced by
                // u64::try_from(soffs) once that feature is stabilized
                // https://github.com/rust-lang/rust/issues/33417
                assert!(soffs >= 0);
                let offs = soffs as u64;
                let fut = dataset.range(FSKey::dirent_range(ino, offs))
                    .fold(tx, move |tx, (k, v)| {
                        let dirent = v.as_direntry().unwrap();
                        let namlen = dirent.name.as_bytes().len();
                        let mut reply = libc::dirent {
                            d_fileno: dirent.ino as u32,
                            d_reclen: dirent_size,
                            d_type: dirent.dtype,
                            d_namlen: namlen as u8,
                            d_name: unsafe{mem::zeroed()}
                        };
                        // libc::dirent uses "char" when it should be using
                        // "unsigned char", so we need an unsafe conversion
                        let p = dirent.name.as_bytes()
                            as *const [u8] as *const [i8];
                        reply.d_name[0..namlen].copy_from_slice(unsafe{&*p});
                        tx.send(Ok((reply, (k.offset() + 1) as i64)))
                            .map_err(|_| Error::EPIPE)
                    }).map(|_| ());
                Box::new(fut) as Box<Future<Item=(), Error=Error> + Send>
            }).map_err(|e| {
                // An EPIPE here means that the caller dropped the iterator
                // without reading all entries, probably because it found the
                // entry that it wants.  Swallow the error.
                if e != Error::EPIPE {
                    panic!("{:?}", e)
                }
            })
        ).unwrap();
        rx.wait().map(|r| r.unwrap())
    }

    pub fn readlink(&self, ino: u64) -> Result<OsString, i32> {
        let (tx, rx) = oneshot::channel();
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                let key = FSKey::new(ino, ObjKey::Inode);
                dataset.get(key)
                .then(move |r| {
                    match r {
                        Ok(Some(v)) => {
                            let inode = v.as_inode().unwrap();
                            if let FileType::Link(ref path) = inode.file_type {
                                tx.send(Ok(path.clone()))
                            } else {
                                tx.send(Err(Error::EINVAL.into()))
                            }
                        },
                        Ok(None) => {
                            tx.send(Err(Error::ENOENT.into()))
                        },
                        Err(e) => {
                            tx.send(Err(e.into()))
                        }
                    }.unwrap();
                    future::ok::<(), Error>(())
                })
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn rename(&self, parent: u64, name: &OsStr,
        newparent: u64, newname: &OsStr) -> Result<(), i32>
    {
        // Outline:
        // 0)  Check conditions
        // 1)  Remove the source dirent
        // 2)  Insert the dst dirent
        // 3a) If source was a directory decrement parent's nlink
        // 3b) If source was a directory and target did not exist, increment
        //     newparent's nlink
        // 3ci) If dst existed and is not a directory, decrement its link count
        // 3cii) If dst existed and is a directory, remove it
        let (tx, rx) = oneshot::channel();
        let src_objkey = ObjKey::dir_entry(&name);
        let dst_objkey = ObjKey::dir_entry(&newname);
        let owned_newname = newname.to_owned();
        let owned_newname2 = owned_newname.clone();
        let samedir = parent == newparent;
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                let ds = Arc::new(dataset);
                let ds4 = ds.clone();
                let ds5 = ds.clone();
                let dst_de_key = FSKey::new(newparent, dst_objkey);
                // 0) Check conditions
                ds.get(dst_de_key)
                .and_then(move |r| {
                    if let Some(v) = r {
                        let dirent = v.as_direntry().unwrap();
                        // Is it not a directory?
                        if dirent.dtype != libc::DT_DIR {
                            Box::new(Ok(()).into_future())
                                as Box<Future<Item=(), Error=Error> + Send>
                        } else {
                            // Is it a nonempty directory?
                            Box::new(Fs::ok_to_rmdir(&ds4, dirent.ino,
                                                     newparent, owned_newname))
                                as Box<Future<Item=(), Error=Error> + Send>
                        }
                    } else {
                        // Destination doesn't exist.  No problem!
                        Box::new(Ok(()).into_future())
                            as Box<Future<Item=(), Error=Error> + Send>
                    }
                }).and_then(move |_| {
                    // 1) Remove the source directory entry
                    let src_de_key = FSKey::new(parent, src_objkey);
                    ds5.remove(src_de_key)
                }).and_then(move |r| {
                    if let Some(v) = r {
                        let dirent = v.as_direntry().unwrap();
                        Ok(dirent.clone()).into_future()
                    } else {
                        Err(Error::ENOENT).into_future()
                    }
                }).and_then(move |mut dirent| {
                    // 2) Insert the new directory entry
                    let isdir = dirent.dtype == libc::DT_DIR;
                    dirent.name = owned_newname2;
                    let de_value = FSValue::DirEntry(dirent);
                    ds.insert(dst_de_key, de_value)
                    .map(move |r| (r, ds, isdir))
                }).and_then(move |(r, ds, isdir)| {
                    let p_nlink_fut = if isdir && !samedir {
                        // 3a) Decrement parent dir's link count
                        let ds2 = ds.clone();
                        let parent_ino_key = FSKey::new(parent, ObjKey::Inode);
                        let fut = ds.get(parent_ino_key)
                        .and_then(move |r| {
                            let mut value = r.unwrap();
                            value.as_mut_inode().unwrap().nlink -= 1;
                            ds2.insert(parent_ino_key, value)
                            .map(|_| ())
                        });
                        Box::new(fut)
                            as Box<Future<Item=(), Error=Error> + Send>
                    } else {
                        Box::new(Ok(()).into_future())
                            as Box<Future<Item=(), Error=Error> + Send>
                    };
                    let np_nlink_fut = if isdir && !samedir && r.is_none() {
                        // 3b) Increment new parent dir's link count
                        let ds3 = ds.clone();
                        let newparent_ino_key = FSKey::new(newparent,
                                                           ObjKey::Inode);
                        let fut = ds.get(newparent_ino_key)
                        .and_then(move |r| {
                            let mut value = r.unwrap();
                            value.as_mut_inode().unwrap().nlink += 1;
                            ds3.insert(newparent_ino_key, value)
                            .map(|_| ())
                        });
                        Box::new(fut)
                            as Box<Future<Item=(), Error=Error> + Send>
                    } else {
                        Box::new(Ok(()).into_future())
                            as Box<Future<Item=(), Error=Error> + Send>
                    };
                    let unlink_fut = if let Some(v) = r {
                        let dst_ino = v.as_direntry().unwrap().ino;
                        // 3ci) Decrement old dst's link count
                        if isdir {
                            let fut = Fs::do_rmdir(ds, newparent, dst_ino,
                                                   false);
                            Box::new(fut)
                                as Box<Future<Item=(), Error=Error> + Send>
                        } else {
                            let fut = Fs::do_unlink(ds.clone(), dst_ino)
                            .map(|_| ());
                            Box::new(fut)
                                as Box<Future<Item=(), Error=Error> + Send>
                        }
                    } else {
                        Box::new(Ok(()).into_future())
                            as Box<Future<Item=(), Error=Error> + Send>
                    };
                    unlink_fut.join3(p_nlink_fut, np_nlink_fut)
                }).then(move |r| {
                    match r {
                        Ok(_) => tx.send(Ok(())),
                        Err(e) => tx.send(Err(e.into()))
                    }.expect("FS::rename: send failed");
                    Ok(()).into_future()
                })
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn rmdir(&self, parent: u64, name: &OsStr) -> Result<(), i32> {
        // Outline:
        // 1) Lookup the directory
        // 2) Check that the directory is empty
        // 3) Remove its parent's directory entry
        // 4) Actually remove it
        let (tx, rx) = oneshot::channel();
        let owned_name = name.to_os_string();
        let objkey = ObjKey::dir_entry(&owned_name);
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                let ds = Arc::new(dataset);
                let ds2 = ds.clone();
                // 1) Lookup the directory
                let key = FSKey::new(parent, objkey);
                ds.get(key)
                .and_then(|r| {
                    match r {
                        Some(v) => {
                            let de = v.as_direntry().unwrap();
                            Ok(de.ino).into_future()
                        },
                        None => Err(Error::ENOENT).into_future()
                    }
                }).and_then(move |ino| {
                    // 2) Check that the directory is empty
                    Fs::ok_to_rmdir(&ds, ino, parent, owned_name)
                    .map(move |_| ino)
                }).and_then(move |ino| {
                    // 3) Remove the parent dir's dir_entry
                    let de_key = FSKey::new(parent, objkey);
                    let dirent_fut = ds2.remove(de_key);

                    // 4) Actually remove the directory
                    let dfut = Fs::do_rmdir(ds2, parent, ino, true);
                    dirent_fut.join(dfut)
                }).then(move |r| {
                    match r {
                        Ok(_) => tx.send(Ok(())),
                        Err(e) => tx.send(Err(e.into()))
                    }.expect("FS::rmdir: send failed");
                    Ok(()).into_future()
                })
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn setattr(&self, ino: u64, attr: SetAttr) -> Result<(), i32> {
        let (tx, rx) = oneshot::channel();
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        self.handle.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                dataset.get(inode_key)
                .and_then(move |r| {
                    let mut iv = r.unwrap().as_mut_inode().unwrap().clone();
                    iv.perm = attr.perm.unwrap_or(iv.perm);
                    iv.uid = attr.uid.unwrap_or(iv.uid);
                    iv.gid = attr.gid.unwrap_or(iv.gid);
                    iv.size = attr.size.unwrap_or(iv.size);
                    iv.atime = attr.atime.unwrap_or(iv.atime);
                    iv.mtime = attr.mtime.unwrap_or(iv.mtime);
                    iv.ctime = attr.ctime.unwrap_or(iv.ctime);
                    iv.birthtime = attr.birthtime.unwrap_or(iv.birthtime);
                    iv.flags = attr.flags.unwrap_or(iv.flags);
                    iv.uid = attr.uid.unwrap_or(iv.uid);
                    dataset.insert(inode_key, FSValue::Inode(iv))
                }).map(move |_| tx.send(Ok(())).unwrap())
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn setextattr(&self, ino: u64, ns: ExtAttrNamespace,
                      name: &OsStr, data: &[u8]) -> Result<(), i32>
    {
        let (tx, rx) = oneshot::channel();
        let objkey = ObjKey::extattr(ns, &name);
        let key = FSKey::new(ino, objkey);
        // Data copy
        let buf = Arc::new(DivBufShared::from(Vec::from(&data[..])));
        let extent = InlineExtent::new(buf);
        let extattr = ExtAttr::Inline(InlineExtAttr {
            namespace: ns,
            name: name.to_owned(),
            extent
        });
        let name2 = name.to_owned();
        let value = FSValue::ExtAttr(extattr);
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                dataset.insert(key, value)
                .and_then(move |r| {
                    match r {
                        Some(FSValue::ExtAttr(old)) => {
                            if old.namespace() == ns && old.name() == name2 {
                                // We're overwriting an existing xattr
                                Box::new(future::ok::<(), Error>(()))
                                    as Box<Future<Item=(), Error=Error> + Send>
                            } else {
                                // We had a hash collision setting an unrelated
                                // xattr. Get the old value back, and pack them
                                // together.
                                let fut = dataset.get(key)
                                .and_then(move |r| {
                                    let v = r.unwrap();
                                    let extattr = v.into_extattr().unwrap();
                                    let extattrs = vec![old, extattr];
                                    let value = FSValue::ExtAttrs(extattrs);
                                    dataset.insert(key, value)
                                    .map(|_| ())
                                });
                                Box::new(fut)
                                    as Box<Future<Item=(), Error=Error> + Send>
                            }
                        },
                        Some(FSValue::ExtAttrs(mut old)) => {
                            // There was previously a hash collision.  Get the
                            // new value back, then pack them together.
                            let fut = dataset.get(key)
                            .and_then(move |r| {
                                let v = r.unwrap();
                                let extattr = v.into_extattr().unwrap();
                                if let Some(i) = old.iter().position(|x| {
                                    x.namespace() == ns && x.name() == name2
                                }) {
                                    // A 2-way hash collision, overwriting one
                                    // value.  Replace the old value.
                                    old[i] = extattr;
                                } else {
                                    // A three (or more)-way hash collision.
                                    // Append the new xattr
                                    old.push(extattr);
                                }
                                dataset.insert(key, FSValue::ExtAttrs(old))
                                .map(|_| ())
                            });
                            Box::new(fut)
                                as Box<Future<Item=(), Error=Error> + Send>
                        },
                        None => {
                            // No previous value
                            Box::new(future::ok::<(), Error>(()))
                                as Box<Future<Item=(), Error=Error> + Send>
                        },
                        x => panic!("Unexpected value {:?} for key {:?}", x,
                                    key)
                    }
                }).map(move |_| tx.send(Ok(())).unwrap())
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn statvfs(&self) -> libc::statvfs {
        let (tx, rx) = oneshot::channel::<libc::statvfs>();
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                let blocks = dataset.size();
                let allocated = dataset.allocated();
                let r = libc::statvfs {
                    f_bavail: blocks - allocated,
                    f_bfree: blocks - allocated,
                    f_blocks: blocks,
                    f_favail: u64::max_value(),
                    f_ffree: u64::max_value(),
                    f_files: u64::max_value(),
                    f_bsize: 4096,
                    f_flag: 0,
                    f_frsize: 4096,
                    f_fsid: 0,
                    f_namemax: 255,
                };
                tx.send(r).ok().expect("Fs::statvfs: send failed");
                Ok(()).into_future()
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    /// Create a symlink from `name` to `link`.  Returns the link's inode on
    /// success, or an errno on failure.
    pub fn symlink(&self, parent: u64, name: &OsStr, perm: u16, uid: u32,
                   gid: u32, link: &OsStr) -> Result<u64, i32>
    {
        let file_type = FileType::Link(link.to_os_string());
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          file_type);
        self.do_create(create_args)
    }

    pub fn sync(&self) {
        let (tx, rx) = oneshot::channel::<()>();
        self.handle.spawn(
            self.db.sync_transaction()
            .map_err(|e| panic!("{:?}", e))
            .map(|_| tx.send(()).unwrap())
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn unlink(&self, parent: u64, name: &OsStr) -> Result<(), i32> {
        // Outline:
        // 1) Lookup and remove the directory entry
        // 2) Unlink the Inode
        let (tx, rx) = oneshot::channel();
        let owned_name = name.to_os_string();
        let dekey = ObjKey::dir_entry(&owned_name);
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                // 1) Lookup and remove the directory entry
                let key = FSKey::new(parent, dekey);
                dataset.remove(key)
                .and_then(|r| {
                    match r {
                        Some(v) => {
                            let de = v.as_direntry().unwrap();
                            Ok(de.ino).into_future()
                        },
                        None => Err(Error::ENOENT).into_future()
                    }
                }).and_then(move |ino|  {
                    // 2) Unlink the inode
                    Fs::do_unlink(Arc::new(dataset), ino)
                }).then(move |r| {
                    match r {
                        Ok(_) => tx.send(Ok(())),
                        Err(e) => tx.send(Err(e.into()))
                    }.expect("FS::unlink: send failed");
                    Ok(()).into_future()
                })
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn write(&mut self, ino: u64, offset: u64, data: &[u8], _flags: u32)
        -> Result<u32, i32>
    {
        // Outline:
        // 1) Split the I/O into discrete records
        // 2) For each record
        //   a) If it's complete, write it to the tree as an InlineExtent
        //   b) If it's a partial record, try to read the old one from the tree
        //     i) If sucessful, RMW it
        //     ii) If not, pad the beginning of the record with zeros.  Pad the
        //         end if the Inode indicates that the file size requires it.
        //         Then write it as an InlineExtent
        //  3) Set file length
        let (tx, rx) = oneshot::channel();
        let rs = RECORDSIZE as u64;

        let datalen = data.len();
        let nrecs = (div_roundup(offset + datalen as u64, rs)
                     - (offset / rs)) as usize;
        let reclen1 = cmp::min(datalen, (rs - (offset % rs)) as usize);
        let sglist = (0..nrecs).map(|rec| {
            let range = if rec == 0 {
                0..reclen1
            } else {
                (reclen1 + (rec - 1) * RECORDSIZE)..(reclen1 + rec * RECORDSIZE)
            };
            // Data copy
            let v = Vec::from(&data[range]);
            Arc::new(DivBufShared::from(v))
        }).collect::<Vec<_>>();

        self.handle.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let inode_key = FSKey::new(ino, ObjKey::Inode);
                let dataset = Arc::new(ds);
                let dataset2 = dataset.clone();
                dataset.get(inode_key)
                .and_then(move |r| {
                    let mut inode_value = r.unwrap();
                    let filesize = inode_value.as_inode().unwrap().size;
                    let data_futs = sglist.into_iter()
                        .enumerate()
                        .map(|(i, dbs)| {
                        let ds3 = dataset.clone();
                        Fs::write_record(ino, filesize, offset, i, dbs, ds3)
                    }).collect::<Vec<_>>();
                    future::join_all(data_futs).and_then(move |_| {
                        let new_size = cmp::max(filesize,
                                                offset + datalen as u64);
                        inode_value.as_mut_inode().unwrap().size = new_size;
                        dataset2.insert(inode_key, inode_value)
                    })
                }).map(move |_| tx.send(Ok(datalen as u32)).unwrap())
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    // Subroutine of write
    #[inline]
    fn write_record(ino: u64, filesize: u64, offset: u64, i: usize,
                    dbs: Arc<DivBufShared>, dataset: Arc<ReadWriteFilesystem>)
        -> Box<Future<Item=Option<FSValue<RID>>, Error=Error> + Send>
    {
        let rs = RECORDSIZE as u64;
        let baseoffset = offset - (offset % rs);
        let offs = baseoffset + i as u64 * rs;
        let k = FSKey::new(ino, ObjKey::Extent(offs));
        if dbs.len() < RECORDSIZE {
            // We must read-modify-write
            let dataset4 = dataset.clone();
            let fut = dataset.get(k)
            .and_then(move |r| {
                match r {
                    None => {
                        // Either a hole, or beyond EOF
                        let hsize = cmp::min(filesize, i as u64 * rs) as usize;
                        let v = vec![0u8; hsize];
                        let r = Box::new(DivBufShared::from(v).try().unwrap());
                        Box::new(Ok(r).into_future())
                            as Box<Future<Item=_, Error=_> + Send>
                    },
                    Some(FSValue::InlineExtent(ile)) => {
                        let r = Box::new(ile.buf.try().unwrap());
                        Box::new(Ok(r).into_future())
                            as Box<Future<Item=_, Error=_> + Send>
                    },
                    Some(FSValue::BlobExtent(be)) => {
                        Box::new(dataset4.get_blob(be.rid))
                            as Box<Future<Item=_, Error=_> + Send>
                    },
                    x => panic!("Unexpected value {:?} for key {:?}",
                                x, k)
                }.and_then(move |db: Box<DivBuf>| {
                    // Data copy, because we can't modify cache
                    let mut base = Vec::from(&db[..]);
                    let overlay = dbs.try().unwrap();
                    let l = overlay.len() as u64;
                    if i == 0 {
                        let s = (offset - baseoffset) as usize;
                        let e = (offset - baseoffset + l) as usize;
                        let r = s..e;
                        if e > base.len() {
                            // We must be appending
                            base.resize(e, 0);
                        }
                        base[r].copy_from_slice(&overlay[..]);
                    } else {
                        base[0..l as usize].copy_from_slice(&overlay[..]);
                    }
                    let new_dbs = Arc::new(
                        DivBufShared::from(base)
                    );
                    let extent = InlineExtent::new(new_dbs);
                    let new_v = FSValue::InlineExtent(extent);
                    dataset.insert(k, new_v)
                })
            });
            Box::new(fut)
                as Box<Future<Item=_, Error=_> + Send>
        } else {
            let v = FSValue::InlineExtent(InlineExtent::new(dbs));
            Box::new(dataset.insert(k, v))
                as Box<Future<Item=_, Error=_> + Send>
        }
    }
}

// LCOV_EXCL_START
#[cfg(test)]
#[cfg(feature = "mocks")]
mod t {

use super::*;
use crate::common::tree::MinValue;
#[cfg(test)] use crate::common::database_mock::ReadOnlyFilesystem;
use futures::stream;
use simulacrum::*;

fn setup() -> (tokio_io_pool::Runtime, Database, TreeID) {
    let mut rt = tokio_io_pool::Builder::default()
        .pool_size(1)
        .build()
        .unwrap();
    let mut ds = ReadOnlyFilesystem::new();
    ds.expect_last_key()
        .called_once()
        .returning(|_| Box::new(Ok(Some(FSKey::min_value())).into_future()));
    let mut opt_ds = Some(ds);
    let mut db = Database::new();
    db.expect_new_fs()
        .called_once()
        .returning(|_| Box::new(Ok(TreeID::Fs(0)).into_future()));
    db.expect_fsread()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let tree_id = rt.block_on(db.new_fs()).unwrap();
    (rt, db, tree_id)
}

#[test]
fn create() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
    // For the unit tests, we skip creating the "/" directory
    let ino = 1;
    let filename = OsString::from("x");
    let filename2 = filename.clone();
    ds.expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_inode() &&
            args.1.as_inode().unwrap().size == 0 &&
            args.1.as_inode().unwrap().nlink == 1 &&
            args.1.as_inode().unwrap().file_type == FileType::Reg &&
            args.1.as_inode().unwrap().perm == 0o644 &&
            args.1.as_inode().unwrap().uid == 123 &&
            args.1.as_inode().unwrap().gid == 456
        })).returning(|_| Box::new(Ok(None).into_future()));
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_direntry() &&
            args.1.as_direntry().unwrap().dtype == libc::DT_REG &&
            args.1.as_direntry().unwrap().name == filename2 &&
            args.1.as_direntry().unwrap().ino == ino
        })).returning(|_| Box::new(Ok(None).into_future()));
    let mut opt_ds = Some(ds);
    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);

    assert_eq!(ino, fs.create(1, &filename, 0o644, 123, 456).unwrap());
}

// Pet kcov
#[test]
fn debug_getattr() {
    let attr = GetAttr {
        ino: 1,
        size: 4096,
        blocks: 1,
        atime: time::Timespec::new(1, 2),
        mtime: time::Timespec::new(3, 4),
        ctime: time::Timespec::new(5, 6),
        birthtime: time::Timespec::new(7, 8),
        mode: Mode(libc::S_IFREG | 0o644),
        nlink: 1,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        flags: 0,
    };
    let s = format!("{:?}", attr);
    assert_eq!("GetAttr { ino: 1, size: 4096, blocks: 1, atime: Timespec { sec: 1, nsec: 2 }, mtime: Timespec { sec: 3, nsec: 4 }, ctime: Timespec { sec: 5, nsec: 6 }, birthtime: Timespec { sec: 7, nsec: 8 }, mode: Mode { .0: 33188, perm: 420 }, nlink: 1, uid: 1000, gid: 1000, rdev: 0, flags: 0 }", s);
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
#[test]
fn deleteextattr_3way_collision() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
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
    .called_once()
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
        Box::new(Ok(Some(v)).into_future())
            as Box<Future<Item=Option<FSValue<RID>>, Error=Error> + Send>
    });
    ds.then().expect_insert()
    .called_once()
    .with(passes(move |args: &(FSKey, FSValue<RID>)| {
        // name0 and name1 should be reinserted
        let extattrs = args.1.as_extattrs().unwrap();
        let ie0 = extattrs[0].as_inline().unwrap();
        let ie1 = extattrs[1].as_inline().unwrap();
        ie0.name == name0 && ie1.name == name1 && extattrs.len() == 2
    })).returning(|_| {
        Box::new(Ok(None).into_future())
    });

    let mut opt_ds = Some(ds);
    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.deleteextattr(ino, namespace, &name2);
    assert_eq!(Ok(()), r);
}

/// A 3-way hash collision of extended attributes.  Two are stored in one key,
/// and deleteextattr tries to delete a third that hashes to the same key, but
/// isn't stored there.
#[test]
fn deleteextattr_3way_collision_enoattr() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
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
    .called_once()
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
        Box::new(Ok(Some(v)).into_future())
            as Box<Future<Item=Option<FSValue<RID>>, Error=Error> + Send>
    });
    ds.then().expect_insert()
    .called_once()
    .with(passes(move |args: &(FSKey, FSValue<RID>)| {
        // name0 and name1 should be reinserted
        let extattrs = args.1.as_extattrs().unwrap();
        let ie0 = extattrs[0].as_inline().unwrap();
        let ie1 = extattrs[1].as_inline().unwrap();
        ie0.name == name0 && ie1.name == name1 && extattrs.len() == 2
    })).returning(|_| {
        Box::new(Ok(None).into_future())
    });

    let mut opt_ds = Some(ds);
    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.deleteextattr(ino, namespace, &name2);
    assert_eq!(Err(libc::ENOATTR), r);
}

/// Reading the source returns EIO.  Don't delete the dest
#[test]
fn rename_eio() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
    let srcname = OsString::from("x");
    let dstname = OsString::from("y");
    let dst_dirent = Dirent {
        ino: 3,
        dtype: libc::DT_REG,
        name: dstname.clone()
    };
    ds.expect_get()
        .called_once()
        .with(FSKey::new(1, ObjKey::dir_entry(&dstname)))
        .returning(move |_| {
            let v = FSValue::DirEntry(dst_dirent.clone());
            Box::new(Ok(Some(v)).into_future())
        });
    ds.then().expect_remove()
        .called_once()
        .with(FSKey::new(1, ObjKey::dir_entry(&srcname)))
        .returning(move |_| {
            Box::new(Err(Error::EIO).into_future())
        });

    let mut opt_ds = Some(ds);
    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());

    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.rename(1, &srcname, 1, &dstname);
    assert_eq!(Err(libc::EIO), r);
}

/// Rename a file within the same directory.  The parent directory's inode
/// should not be modified.
#[test]
fn rename_samedir() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
    let srcname = OsString::from("x");
    let dstname = OsString::from("y");
    let src_dirent = Dirent {
        ino: 2,
        dtype: libc::DT_REG,
        name: srcname.clone()
    };
    let new_dirent = Dirent {
        ino: 2,
        dtype: libc::DT_REG,
        name: dstname.clone()
    };
    let new_de_value = FSValue::DirEntry(new_dirent);
    ds.expect_get()
        .called_once()
        .with(FSKey::new(1, ObjKey::dir_entry(&dstname)))
        .returning(move |_| {
            Box::new(Ok(None).into_future())
        });
    ds.then().expect_remove()
        .called_once()
        .with(FSKey::new(1, ObjKey::dir_entry(&srcname)))
        .returning(move |_| {
            let v = FSValue::DirEntry(src_dirent.clone());
            Box::new(Ok(Some(v)).into_future())
        });
    ds.then().expect_insert()
        .called_once()
        .with((FSKey::new(1, ObjKey::dir_entry(&dstname)), new_de_value))
        .returning(move |_| {
            Box::new(Ok(None).into_future())
        });

    let mut opt_ds = Some(ds);
    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());

    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.rename(1, &srcname, 1, &dstname);
    assert_eq!(Ok(()), r);
}

/// Basic setextattr test, that does not rely on any other extattr
/// functionality.
#[test]
fn setextattr() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
    let ino = 1;
    let name = OsString::from("foo");
    let name2 = name.clone();
    let value = OsString::from("bar");
    let value2 = value.clone();
    let namespace = ExtAttrNamespace::User;

    ds.expect_insert()
    .called_once()
    .with(passes(move |args: &(FSKey, FSValue<RID>)| {
        let extattr = args.1.as_extattr().unwrap();
        let ie = extattr.as_inline().unwrap();
        args.0.is_extattr() &&
        args.0.objtype() == 3 &&
        args.0.object() == ino &&
        ie.namespace == namespace &&
        ie.name == name2 &&
        &ie.extent.buf.try().unwrap()[..] == value2.as_bytes()
    })).returning(|_| Box::new(Ok(None).into_future()));

    let mut opt_ds = Some(ds);
    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.setextattr(ino, namespace, &name, value.as_bytes());
    assert_eq!(Ok(()), r);
}

/// setextattr with a 3-way hash collision.  It's hard to programmatically
/// generate 3-way hash collisions, so we simulate them using mocks
#[test]
fn setextattr_3way_collision() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
    let ino = 1;
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
    .called_once()
    .with(passes(move |args: &(FSKey, FSValue<RID>)| {
        let extattr = args.1.as_extattr().unwrap();
        let ie = extattr.as_inline().unwrap();
        ie.name == name2a
    })).returning(move |_| {
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
        Box::new(Ok(Some(v)).into_future())
            as Box<Future<Item=Option<FSValue<RID>>, Error=Error> + Send>
    });
    ds.then().expect_get()
    .called_once()
    .with(passes(move |arg: &FSKey| {
        arg.is_extattr() &&
        arg.objtype() == 3 &&
        arg.object() == ino
    })).returning(move |_| {
        // Return the newly inserted value2
        let dbs2 = Arc::new(DivBufShared::from(Vec::from(value2a.as_bytes())));
        let extent2 = InlineExtent::new(dbs2);
        let name = name2b.clone();
        let iea2 = InlineExtAttr{namespace, name, extent: extent2};
        let extattr2 = ExtAttr::Inline(iea2);
        let v = FSValue::ExtAttr(extattr2);
        Box::new(Ok(Some(v)).into_future())
            as Box<Future<Item=Option<FSValue<RID>>, Error=Error> + Send>
    });
    ds.then().expect_insert()
    .called_once()
    .with(passes(move |args: &(FSKey, FSValue<RID>)| {
        let extattrs = args.1.as_extattrs().unwrap();
        let ie0 = extattrs[0].as_inline().unwrap();
        let ie1 = extattrs[1].as_inline().unwrap();
        let ie2 = extattrs[2].as_inline().unwrap();
        ie0.name == name0 && ie1.name == name1 && ie2.name == name2c
    })).returning(move |_| {
        // Return a copy of the recently inserted value2
        let dbs2 = Arc::new(DivBufShared::from(Vec::from(value2b.as_bytes())));
        let extent2 = InlineExtent::new(dbs2);
        let name = name2d.clone();
        let iea2 = InlineExtAttr{namespace, name, extent: extent2};
        let extattr2 = ExtAttr::Inline(iea2);
        let v = FSValue::ExtAttr(extattr2);
        Box::new(Ok(Some(v)).into_future())
            as Box<Future<Item=Option<FSValue<RID>>, Error=Error> + Send>
    });

    let mut opt_ds = Some(ds);
    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.setextattr(ino, namespace, &name2, value2.as_bytes());
    assert_eq!(Ok(()), r);
}

#[test]
fn sync() {
    let (rt, mut db, tree_id) = setup();
    db.expect_sync_transaction()
        .called_once()
        .returning(|_| Box::new(Ok(()).into_future()));
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);

    fs.sync();
}

#[test]
fn unlink() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
    let parent_ino = 1;
    let ino = 2;
    let blob_rid = RID(99999);
    let filename = OsString::from("x");
    let filename2 = filename.clone();

    ds.expect_remove()
        .called_once()
        .with(FSKey::new(parent_ino, ObjKey::dir_entry(&filename)))
        .returning(move |_| {
            let dirent = Dirent {
                ino,
                dtype: libc::DT_REG,
                name: filename2.clone()
            };
            let v = Some(FSValue::DirEntry(dirent));
            Box::new(Ok(v).into_future())
        });
    ds.expect_get()
        .called_once()
        .with(FSKey::new(ino, ObjKey::Inode))
        .returning(move |_| {
            let now = time::get_time();
            let inode = Inode {
                size: 4098,
                nlink: 1,
                flags: 0,
                atime: now,
                mtime: now,
                ctime: now,
                birthtime: now,
                uid: 0,
                gid: 0,
                file_type: FileType::Reg,
                perm: 0o644,
            };
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });

    ds.then().expect_range()
        .called_once()
        .with(FSKey::extent_range(ino))
        .returning(move |_| {
            // Return one blob extent and one embedded extent
            let k0 = FSKey::new(ino, ObjKey::Extent(0));
            let be0 = BlobExtent{lsize: 4096, rid: blob_rid};
            let v0 = FSValue::BlobExtent(be0);
            let k1 = FSKey::new(ino, ObjKey::Extent(4096));
            let dbs0 = Arc::new(DivBufShared::from(vec![0u8; 1]));
            let v1 = FSValue::InlineExtent(InlineExtent::new(dbs0));
            let extents = vec![(k0, v0), (k1, v1)];
            Box::new(stream::iter_ok(extents))
        });
    ds.then().expect_range()
        .called_once()
        .with(FSKey::extattr_range(ino))
        .returning(move |_| {
            Box::new(stream::iter_ok(Vec::new()))
        });
    ds.then().expect_delete_blob()
        .called_once()
        .with(passes(move |rid: &RID| blob_rid == *rid))
        .returning(|_| Box::new(Ok(()).into_future()));
    ds.then().expect_range_delete()
        .called_once()
        .with(FSKey::obj_range(ino))
        .returning(|_| Box::new(Ok(()).into_future()));
    let mut opt_ds = Some(ds);

    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.unlink(1, &filename);
    assert_eq!(Ok(()), r);
}

// Unlink of a multiply linked file
#[test]
fn unlink_hardlink() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
    let parent_ino = 1;
    let ino = 2;
    let filename = OsString::from("x");
    let filename2 = filename.clone();

    ds.expect_remove()
        .called_once()
        .with(FSKey::new(parent_ino, ObjKey::dir_entry(&filename)))
        .returning(move |_| {
            let dirent = Dirent {
                ino,
                dtype: libc::DT_REG,
                name: filename2.clone()
            };
            let v = Some(FSValue::DirEntry(dirent));
            Box::new(Ok(v).into_future())
        });
    ds.expect_get()
        .called_once()
        .with(FSKey::new(ino, ObjKey::Inode))
        .returning(move |_| {
            let now = time::get_time();
            let inode = Inode {
                size: 4098,
                nlink: 2,
                flags: 0,
                atime: now,
                mtime: now,
                ctime: now,
                birthtime: now,
                uid: 0,
                gid: 0,
                file_type: FileType::Reg,
                perm: 0o644,
            };
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_inode() &&
            args.1.as_inode().unwrap().nlink == 1
        })).returning(|_| Box::new(Ok(None).into_future()));

    let mut opt_ds = Some(ds);

    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.unlink(1, &filename);
    assert_eq!(Ok(()), r);
}

// Unlink a file with extended attributes, and don't forget to free them too!
#[test]
fn unlink_with_blob_extattr() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
    let parent_ino = 1;
    let ino = 2;
    let blob_rid = RID(99999);
    let xattr_blob_rid = RID(88888);
    let filename = OsString::from("x");
    let filename2 = filename.clone();

    ds.expect_remove()
        .called_once()
        .with(FSKey::new(parent_ino, ObjKey::dir_entry(&filename)))
        .returning(move |_| {
            let dirent = Dirent {
                ino,
                dtype: libc::DT_REG,
                name: filename2.clone()
            };
            let v = Some(FSValue::DirEntry(dirent));
            Box::new(Ok(v).into_future())
        });
    ds.expect_get()
        .called_once()
        .with(FSKey::new(ino, ObjKey::Inode))
        .returning(move |_| {
            let now = time::get_time();
            let inode = Inode {
                size: 4098,
                nlink: 1,
                flags: 0,
                atime: now,
                mtime: now,
                ctime: now,
                birthtime: now,
                uid: 0,
                gid: 0,
                file_type: FileType::Reg,
                perm: 0o644,
            };
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });

    ds.then().expect_range()
        .called_once()
        .with(FSKey::extent_range(ino))
        .returning(move |_| {
            // Return one blob extent and one embedded extent
            let k0 = FSKey::new(ino, ObjKey::Extent(0));
            let be0 = BlobExtent{lsize: 4096, rid: blob_rid};
            let v0 = FSValue::BlobExtent(be0);
            let k1 = FSKey::new(ino, ObjKey::Extent(4096));
            let dbs0 = Arc::new(DivBufShared::from(vec![0u8; 1]));
            let v1 = FSValue::InlineExtent(InlineExtent::new(dbs0));
            let extents = vec![(k0, v0), (k1, v1)];
            Box::new(stream::iter_ok(extents))
        });
    // NB: there is no requirement that extents be deleted before extattrs, but
    // Simulacrum forces us to choose, since you can't set two Simulacrum mocks
    // for the same method in the same era.
    ds.then().expect_range()
        .called_once()
        .with(FSKey::extattr_range(ino))
        .returning(move |_| {
            // Return one blob extattr and one inline extattr
            let namespace = ExtAttrNamespace::User;
            let name0 = OsString::from("foo");
            let k0 = FSKey::new(ino, ObjKey::extattr(namespace, &name0));
            let extent0 = BlobExtent{lsize: 4096, rid: xattr_blob_rid};
            let be = BlobExtAttr{namespace, name: name0, extent: extent0};
            let v0 = FSValue::ExtAttr(ExtAttr::Blob(be));

            let name1 = OsString::from("bar");
            let k1 = FSKey::new(ino, ObjKey::extattr(namespace, &name1));
            let dbs1 = Arc::new(DivBufShared::from(vec![0u8; 1]));
            let extent1 = InlineExtent::new(dbs1);
            let ie = InlineExtAttr{namespace, name: name1, extent: extent1};
            let v1 = FSValue::ExtAttr(ExtAttr::Inline(ie));
            let extents = vec![(k0, v0), (k1, v1)];
            Box::new(stream::iter_ok(extents))
        });
    ds.expect_delete_blob()
        .called_once()
        .with(passes(move |rid: &RID| blob_rid == *rid))
        .returning(|_| Box::new(Ok(()).into_future()));
    ds.then().expect_delete_blob()
        .called_once()
        .with(passes(move |rid: &RID| xattr_blob_rid == *rid))
        .returning(|_| Box::new(Ok(()).into_future()));
    ds.then().expect_range_delete()
        .called_once()
        .with(FSKey::obj_range(ino))
        .returning(|_| Box::new(Ok(()).into_future()));
    let mut opt_ds = Some(ds);

    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.unlink(1, &filename);
    assert_eq!(Ok(()), r);
}

// Unlink a file with two extended attributes that hashed to the same bucket.
// One is a blob, and must be freed
#[test]
fn unlink_with_extattr_hash_collision() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
    let parent_ino = 1;
    let ino = 2;
    let blob_rid = RID(99999);
    let xattr_blob_rid = RID(88888);
    let filename = OsString::from("x");
    let filename2 = filename.clone();

    ds.expect_remove()
        .called_once()
        .with(FSKey::new(parent_ino, ObjKey::dir_entry(&filename)))
        .returning(move |_| {
            let dirent = Dirent {
                ino,
                dtype: libc::DT_REG,
                name: filename2.clone()
            };
            let v = Some(FSValue::DirEntry(dirent));
            Box::new(Ok(v).into_future())
        });
    ds.expect_get()
        .called_once()
        .with(FSKey::new(ino, ObjKey::Inode))
        .returning(move |_| {
            let now = time::get_time();
            let inode = Inode {
                size: 0,
                nlink: 1,
                flags: 0,
                atime: now,
                mtime: now,
                ctime: now,
                birthtime: now,
                uid: 0,
                gid: 0,
                file_type: FileType::Reg,
                perm: 0o644,
            };
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });
    ds.then().expect_range()
        .called_once()
        .with(FSKey::extent_range(ino))
        .returning(move |_| {
            // The file is empty
            let extents = vec![];
            Box::new(stream::iter_ok(extents))
        });
    ds.then().expect_range()
        .called_once()
        .with(FSKey::extattr_range(ino))
        .returning(move |_| {
            // Return one blob extattr and one inline extattr, in the same
            // bucket
            let namespace = ExtAttrNamespace::User;
            let name0 = OsString::from("foo");
            let k0 = FSKey::new(ino, ObjKey::extattr(namespace, &name0));
            let extent0 = BlobExtent{lsize: 4096, rid: xattr_blob_rid};
            let be = BlobExtAttr{namespace, name: name0, extent: extent0};
            let extattr0 = ExtAttr::Blob(be);

            let name1 = OsString::from("bar");
            let dbs1 = Arc::new(DivBufShared::from(vec![0u8; 1]));
            let extent1 = InlineExtent::new(dbs1);
            let ie = InlineExtAttr{namespace, name: name1, extent: extent1};
            let extattr1 = ExtAttr::Inline(ie);
            let v = FSValue::ExtAttrs(vec![extattr0, extattr1]);
            let extents = vec![(k0, v)];
            Box::new(stream::iter_ok(extents))
        });
    ds.then().expect_delete_blob()
        .called_once()
        .with(passes(move |rid: &RID| xattr_blob_rid == *rid))
        .returning(|_| Box::new(Ok(()).into_future()));
    ds.then().expect_range_delete()
        .called_once()
        .with(FSKey::obj_range(ino))
        .returning(|_| Box::new(Ok(()).into_future()));

    let mut opt_ds = Some(ds);
    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.unlink(1, &filename);
    assert_eq!(Ok(()), r);
}


}
