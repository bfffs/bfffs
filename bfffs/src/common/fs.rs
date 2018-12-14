// vim: tw=80
//! Common VFS implementation

use atomic::*;
use bitfield::*;
use crate::common::*;
#[cfg(not(test))] use crate::common::database::*;
#[cfg(test)] use crate::common::database_mock::DatabaseMock as Database;
#[cfg(test)] use crate::common::database_mock::ReadOnlyFilesystem;
#[cfg(test)] use crate::common::database_mock::ReadWriteFilesystem;
use crate::common::{
    database::TreeID,
    fs_tree::*,
    property::*
};
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

/// Operations used for data that is stored in in-BTree hash tables
mod htable {
    use super::*;

    /// Argument type for `htable::get`.
    // A more obvious approach would be to create a ReadDataset trait that is
    // implemented by both filesystem types.  However, that would require extra
    // allocations, since trait methods can't return `impl trait`.
    pub(super) enum ReadFilesystem<'a> {
        ReadOnly(&'a ReadOnlyFilesystem),
        ReadWrite(&'a ReadWriteFilesystem)
    }

    impl ReadFilesystem<'_> {
        fn get(&self, k: FSKey)
            -> Box<Future<Item=Option<FSValue<RID>>, Error=Error> + Send>
        {
            match self {
                ReadFilesystem::ReadOnly(ds) => Box::new(ds.get(k)),
                ReadFilesystem::ReadWrite(ds) => Box::new(ds.get(k))
            }
        }
    }

    /// Get an item from an in-BTree hash table
    pub(super) fn get<T>(dataset: &ReadFilesystem, key: FSKey,
                         aux: T::Aux, name: OsString)
        -> impl Future<Item=T, Error=Error> + Send
        where T: HTItem
    {
        type MyFut<T> = Box<Future<Item=T, Error=Error> + Send>;
        dataset.get(key)
        .then(move |r| {
            match r {
                Ok(fsvalue) => {
                    match T::from_table(fsvalue) {
                        HTValue::Single(old) => {
                            if old.same(aux, &name) {
                                // Found the right item
                                Box::new(Ok(old).into_future()) as MyFut<T>
                            } else {
                                // Hash collision
                                Box::new(Err(T::ENOTFOUND).into_future())
                                    as MyFut<T>
                            }
                        },
                        HTValue::Bucket(old) => {
                            assert!(old.len() > 1);
                            if let Some(v) = old.into_iter().find(|x| {
                                x.same(aux, &name)
                            }) {
                                // Found the right one
                                Box::new(Ok(v).into_future()) as MyFut<T>
                            } else {
                                // A 3 (or more) way hash collision.  The
                                // item we're looking up isn't found.
                                Box::new(Err(T::ENOTFOUND).into_future())
                                    as MyFut<T>
                            }
                        },
                        HTValue::None => {
                            Box::new(Err(T::ENOTFOUND).into_future())
                                as MyFut<T>
                        },
                        HTValue::Other(x) =>
                            panic!("Unexpected value {:?} for key {:?}", x, key)
                    }
                },
                Err(e) => {
                    Box::new(Err(e).into_future()) as MyFut<T>
                }
            }
        })
    }

    /// Insert an item that is stored in a BTree hash table
    pub(super) fn insert<D, T>(dataset: D, key: FSKey, value: T, name: OsString)
        -> impl Future<Item=Option<T>, Error=Error> + Send
        where D: AsRef<ReadWriteFilesystem> + Send + 'static,
              T: HTItem
    {
        type MyFut<T> = Box<Future<Item=Option<T>, Error=Error> + Send>;
        let aux = value.aux();
        let fsvalue = value.into_fsvalue();
        dataset.as_ref().insert(key, fsvalue)
        .and_then(move |r| {
            match T::from_table(r) {
                HTValue::Single(old) => {
                    if old.same(aux, &name) {
                        // We're overwriting an existing item
                        Box::new(future::ok::<Option<T>, Error>(Some(old)))
                            as MyFut<T>
                    } else {
                        // We had a hash collision setting an unrelated
                        // item. Get the old value back, and pack them together.
                        let fut = dataset.as_ref().get(key)
                        .and_then(move |r| {
                            let v = r.unwrap();
                            let new = T::from_fsvalue(v).unwrap();
                            let values = vec![old, new];
                            let fsvalue = T::into_bucket(values);
                            dataset.as_ref().insert(key, fsvalue)
                            .map(|_| None)
                        });
                        Box::new(fut) as MyFut<T>
                    }
                },
                HTValue::Bucket(mut old) => {
                    // There was previously a hash collision.  Get the new value
                    // back, then pack them together.
                    let fut = dataset.as_ref().get(key)
                    .and_then(move |r| {
                        let v = r.unwrap();
                        let new = T::from_fsvalue(v).unwrap();
                        let r = if let Some(i) = old.iter().position(|x| {
                            x.same(aux, &name)
                        }) {
                            // A 2-way hash collision, overwriting one value.
                            // Replace the old value.
                            Some(mem::replace(&mut old[i], new))
                        } else {
                            // A three (or more)-way hash collision.  Append the
                            // new item
                            old.push(new);
                            None
                        };
                        dataset.as_ref().insert(key, T::into_bucket(old))
                        .map(|_| r)
                    });
                    Box::new(fut) as MyFut<T>
                },
                HTValue::Other(x) => {
                    panic!("Unexpected value {:?} for key {:?}", x, key)
                },
                HTValue::None => {
                    Box::new(future::ok::<Option<T>, Error>(None)) as MyFut<T>
                }
            }
        })
    }

    /// Remove an item that is stored in a BTree hash table
    ///
    /// Return the old item, if any.
    pub(super) fn remove<D, T>(dataset: D, key: FSKey, aux: T::Aux,
                               name: OsString)
        -> impl Future<Item=T, Error=Error> + Send
        where D: AsRef<ReadWriteFilesystem> + Send + 'static,
              T: HTItem
    {
        dataset.as_ref().remove(key)
        .and_then(move |r| {
            match T::from_table(r) {
                HTValue::Single(old) =>
                {
                    if old.same(aux, name) {
                        // This is the item we're looking for
                        Box::new(Ok(old).into_future())
                            as Box<Future<Item=T, Error=Error> + Send>
                    } else {
                        // Hash collision.  Put it back, and return not found
                        let value = old.into_fsvalue();
                        let fut = dataset.as_ref().insert(key, value)
                        .and_then(|_| Err(T::ENOTFOUND).into_future());
                        Box::new(fut)
                            as Box<Future<Item=T, Error=Error> + Send>
                    }
                },
                HTValue::Bucket(mut old) => {
                    // There was previously a hash collision.
                    if let Some(i) = old.iter().position(|x| {
                        x.same(aux, &name)
                    }) {
                        // Desired item was found
                        let r = old.swap_remove(i);
                        let v = if old.len() == 1 {
                            // A 2-way collision; remove one
                            old.pop().unwrap().into_fsvalue()
                        } else {
                            // A 3 (or more) way collision; remove one
                            T::into_bucket(old)
                        };
                        let fut = dataset.as_ref().insert(key, v)
                        .map(move |_| r);
                        Box::new(fut)
                            as Box<Future<Item=T, Error=Error> + Send>
                    } else {
                        // A 3 (or more) way hash collision between the
                        // accessed item and at least two different ones.
                        let v = T::into_bucket(old);
                        let fut = dataset.as_ref().insert(key, v)
                        .and_then(|_| Err(T::ENOTFOUND).into_future());
                        Box::new(fut)
                            as Box<Future<Item=T, Error=Error> + Send>
                    }
                },
                HTValue::None => {
                    Box::new(Err(T::ENOTFOUND).into_future())
                        as Box<Future<Item=T, Error=Error> + Send>
                },
                HTValue::Other(x) =>
                    panic!("Unexpected value {:?} for key {:?}", x, key)
            }
        })
    }
}

/// Generic Filesystem layer.
///
/// Bridges the synchronous with Tokio domains, and the system-independent with
/// system-dependent filesystem interfaces.
pub struct Fs {
    db: Arc<Database>,
    next_object: Atomic<u64>,
    handle: tokio_io_pool::Handle,
    tree: TreeID,

    // These options may only be changed when the filesystem is mounting or
    // remounting the filesystem.
    /// Update files' atimes when reading?
    _atime: bool,
    /// Record size for new files, in bytes
    _record_size: usize
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
#[derive(Clone, Copy, Debug, Default, PartialEq)]
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
    cb: Box<Fn(&Arc<ReadWriteFilesystem>, u64, u64)
        -> Box<Future<Item=(), Error=Error> + Send + 'static> + Send >
}

impl CreateArgs {
    pub fn callback<F>(mut self, f: F) -> Self
        where F: Fn(&Arc<ReadWriteFilesystem>, u64, u64)
        -> Box<Future<Item=(), Error=Error> + Send + 'static> + Send + 'static
    {
        self.cb = Box::new(f);
        self
    }

    fn default_cb(_: &Arc<ReadWriteFilesystem>, _: u64, _: u64)
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
                htable::remove::<ReadWriteFilesystem, ExtAttr<RID>>(dataset,
                    key, ns, name)
            }).map(drop)
            .map_err(Error::into)
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
        let name2 = args.name.clone();
        let parent_dirent = Dirent {
            ino,
            dtype: args.file_type.dtype(),
            name:   args.name
        };
        let parent_dirent_key = FSKey::new(args.parent, parent_dirent_objkey);

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
        let parent_ino = args.parent;
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                let ds = Arc::new(dataset);
                let extra_fut = cb(&ds, parent_ino, ino);
                ds.insert(inode_key, inode_value).join3(
                    htable::insert(ds, parent_dirent_key, parent_dirent, name2),
                    extra_fut
                ).map(move |(inode_r, dirent_r, _)| {
                    assert!(dirent_r.is_none(),
                    "Create of an existing file.  The VFS should prevent this");
                    assert!(inode_r.is_none(),
                    "Inode double-create detected, ino={}", ino);
                    tx.send(Ok(ino)).unwrap()
                })
            }).map_err(Error::unhandled)
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
        // 2) Decrement the parent dir's link count and update its timestamps

        // 1) range_delete its key range
        let dataset2 = dataset.clone();
        let dataset3 = dataset.clone();
        let ino_fut = Fs::list_extattr_rids(&*dataset, ino)
        .for_each(move |rid| {
            dataset2.delete_blob(rid)
        }).and_then(move |_| dataset3.range_delete(FSKey::obj_range(ino)));

        // 2) Update the parent dir's link count and timestamps
        let parent_ino_key = FSKey::new(parent, ObjKey::Inode);
        let nlink_fut = if dec_nlink {
            let fut = dataset.get(parent_ino_key)
            .and_then(move |r| {
                let mut value = r.unwrap();
                {
                    let inode = value.as_mut_inode().unwrap();
                    let now = time::get_time();
                    inode.mtime = now;
                    inode.ctime = now;
                    inode.nlink -= 1;
                }
                dataset.insert(parent_ino_key, value)
                .map(drop)
            });
            Box::new(fut)
                as Box<Future<Item=_, Error=_> + Send>
        } else {
            Box::new(Ok(()).into_future())
                as Box<Future<Item=_, Error=_> + Send>
        };
        ino_fut.join(nlink_fut)
        .map(drop)
    }

    fn do_setattr(dataset: Arc<ReadWriteFilesystem>, ino: u64, attr: SetAttr)
        -> impl Future<Item=(), Error=Error> + Send
    {
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        dataset.get(inode_key)
        .and_then(move |r| {
            let mut iv = r.unwrap().as_mut_inode().unwrap().clone();
            iv.perm = attr.perm.unwrap_or(iv.perm);
            iv.uid = attr.uid.unwrap_or(iv.uid);
            iv.gid = attr.gid.unwrap_or(iv.gid);
            let old_size = iv.size;
            let new_size = attr.size.unwrap_or(iv.size);
            iv.size = new_size;
            iv.atime = attr.atime.unwrap_or(iv.atime);
            iv.mtime = attr.mtime.unwrap_or(iv.mtime);
            iv.ctime = attr.ctime.unwrap_or(iv.ctime);
            iv.birthtime = attr.birthtime.unwrap_or(iv.birthtime);
            iv.flags = attr.flags.unwrap_or(iv.flags);

            let truncate_fut = if new_size < old_size {
                assert!(iv.file_type.dtype() == libc::DT_REG);
                Box::new(Fs::do_truncate(dataset.clone(), ino, new_size))
                    as Box<Future<Item=(), Error=Error> + Send>
            } else {
                let fut = Ok(()).into_future();
                Box::new(fut) as Box<Future<Item=(), Error=Error> + Send>
            };

            dataset.insert(inode_key, FSValue::Inode(iv))
            .join(truncate_fut)
            .map(drop)
        })
    }

    /// Remove all of a file's data past `size`.  This routine does _not_ update
    /// the Inode
    fn do_truncate(dataset: Arc<ReadWriteFilesystem>, ino: u64, size: u64)
        -> impl Future<Item=(), Error=Error> + Send
    {
        // Delete data past the truncation point
        let dataset2 = dataset.clone();
        let dataset3 = dataset.clone();
        let full_fut = dataset.range(FSKey::extent_range(ino, size..))
        .filter_map(move |(_k, v)| {
            if let Extent::Blob(be) = v.as_extent().unwrap()
            {
                Some(be.rid)
            } else {
                None
            }
        }).for_each(move |rid| dataset2.delete_blob(rid))
        .and_then(move |_| {
            dataset3.range_delete(FSKey::extent_range(ino, size..))
        });
        let partial_fut = if size % RECORDSIZE as u64 > 0 {
            let ofs = size - size % RECORDSIZE as u64;
            let len = (size - ofs) as usize;
            let k = FSKey::new(ino, ObjKey::Extent(ofs));
            let fut = dataset.get(k)
            .and_then(move |v| {
                match v {
                    None => {
                        // It's a hole; nothing to do
                        Box::new(Ok(()).into_future())
                            as Box<Future<Item=(), Error=Error> + Send>
                    },
                    Some(FSValue::InlineExtent(ile)) => {
                        let mut b = ile.buf.try_mut().unwrap();
                        b.try_truncate(len).unwrap();
                        let extent = InlineExtent::new(ile.buf);
                        let v = FSValue::InlineExtent(extent);
                        Box::new(dataset.insert(k, v).map(drop))
                            as Box<Future<Item=(), Error=Error> + Send>
                    },
                    Some(FSValue::BlobExtent(be)) => {
                        let fut = dataset.get_blob(be.rid)
                        .and_then(move |db: Box<DivBuf>| {
                            // TODO: eliminate this data copy by adding
                            // a pop_blob method
                            let dbs = DivBufShared::from(&db[0..len]);
                            let adbs = Arc::new(dbs);
                            let extent = InlineExtent::new(adbs);
                            let v = FSValue::InlineExtent(extent);
                            dataset.insert(k, v)
                            .map(drop)
                        });
                        Box::new(fut)
                            as Box<Future<Item=(), Error=Error> + Send>
                    },
                    x => panic!("Unexpectec value {:?} for key {:?}",
                                x, k)
                }
            });
            Box::new(fut) as Box<Future<Item=(), Error=Error> + Send>
        } else {
            let fut = Ok(()).into_future();
            Box::new(fut) as Box<Future<Item=(), Error=Error> + Send>
        };
        full_fut.join(partial_fut).map(drop)
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
                let extent_stream = dataset.range(FSKey::extent_range(ino, ..))
                .filter_map(move |(_k, v)| {
                    if let Extent::Blob(be) = v.as_extent().unwrap()
                    {
                        Some(be.rid)
                    } else {
                        None
                    }
                });
                let extattr_stream = Fs::list_extattr_rids(&*dataset, ino);
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
        let (tx, rx) = oneshot::channel();
        let db2 = database.clone();
        handle.spawn(
            database.fsread(tree, move |dataset| {
                dataset.last_key()
                .join3(db2.get_prop(tree, PropertyName::Atime),
                       db2.get_prop(tree, PropertyName::RecordSize))
                .map(move |r| tx.send(r).unwrap())
            }).map_err(Error::unhandled)
        ).unwrap();
        let (last_key, (atimep, _), (recsizep, _)) = rx.wait().unwrap();
        let next_object = Atomic::new(last_key.unwrap().object() + 1);
        let _atime = atimep.as_bool();
        let _record_size = 1 << recsizep.as_u8();
        Fs{db: database, next_object, handle, tree, _atime, _record_size}
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
                                          FileType::Reg)
        .callback(Fs::create_ts_callback);
        self.do_create(create_args)
    }

    fn create_ts_callback(dataset: &Arc<ReadWriteFilesystem>, parent: u64,
                          _ino: u64)
        -> Box<Future<Item=(), Error=Error> + Send>
    {
        let now = time::get_time();
        let mut attr = SetAttr::default();
        attr.ctime = Some(now);
        attr.mtime = Some(now);
        Box::new(Fs::do_setattr(dataset.clone(), parent, attr))
            as Box<Future<Item=(), Error=Error> + Send>
    }

    /// Dump a YAMLized representation of the filesystem's Tree to a plain
    /// `std::fs::File`.
    pub fn dump(&self, f: &mut io::Write) -> Result<(), i32> {
        self.db.dump(f, self.tree)
        .map_err(|e| e.into())
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
            }).map_err(Error::unhandled)
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
        type MyFut = Box<Future<Item=Box<DivBuf>, Error=Error> + Send>;
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                htable::get(&htable::ReadFilesystem::ReadOnly(&dataset), key,
                            ns, owned_name)
                .and_then(move |extattr| {
                    match extattr {
                        ExtAttr::Inline(iea) => {
                            let buf = Box::new(iea.extent.buf.try_const()
                                               .unwrap());
                            Box::new(Ok(buf).into_future()) as MyFut
                        },
                        ExtAttr::Blob(bea) => {
                            let bfut = dataset.get_blob(bea.extent.rid);
                            Box::new(bfut) as MyFut
                        }
                    }
                })
            }).then(|r| {
                match r {
                    Ok(buf) => tx.send(Ok(*buf)),
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
            }).map_err(Error::unhandled)
        ).unwrap();
        rx.wait().unwrap()
    }

    /// Create a hardlink from `ino` to `parent/name`.
    pub fn link(&self, parent: u64, ino: u64, name: &OsStr) -> Result<u64, i32>
    {
        // Outline:
        // * Increase the target's link count
        // * Add the new directory entry
        // * Update the parent's mtime and ctime
        let (tx, rx) = oneshot::channel();
        let name = name.to_owned();
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                let ds = Arc::new(dataset);
                let inode_key = FSKey::new(ino, ObjKey::Inode);
                ds.get(inode_key)
                .and_then(move |r| {
                    let mut iv = r.unwrap().as_mut_inode().unwrap().clone();
                    iv.nlink += 1;
                    let dtype = iv.file_type.dtype();
                    // FUSE is single-threaded, so we don't have to worry that
                    // the target gets deleted before we increase its link
                    // count.  The real VFS will provide a held vnode rather
                    // than an inode.  So in neither case is there a race here.
                    let ifut = ds.insert(inode_key, FSValue::Inode(iv));

                    let dirent_objkey = ObjKey::dir_entry(&name);
                    let dirent = Dirent { ino, dtype, name };
                    let dirent_key = FSKey::new(parent, dirent_objkey);
                    let dirent_value = FSValue::DirEntry(dirent);
                    let dfut = ds.insert(dirent_key, dirent_value);

                    let now = time::get_time();
                    let mut attr = SetAttr::default();
                    attr.ctime = Some(now);
                    attr.mtime = Some(now);
                    let ts_fut = Fs::do_setattr(ds, parent, attr);

                    ifut.join3(dfut, ts_fut)
                }).map(move |_| tx.send(Ok(ino)).unwrap())
            }).map_err(Error::unhandled)
        ).unwrap();
        rx.wait().unwrap()
    }

    /// List the RID of every blob extattr for a file
    fn list_extattr_rids(dataset: &ReadWriteFilesystem, ino: u64)
        -> impl Stream<Item=RID, Error=Error>
    {
        dataset.range(FSKey::extattr_range(ino))
        .filter_map(move |(k, v)| {
            match v {
                FSValue::ExtAttr(ExtAttr::Inline(_)) => None,
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
                x => panic!("Unexpected value {:?} for key {:?}", x, k)
            }
        }).flatten()
    }

    pub fn lookup(&self, parent: u64, name: &OsStr) -> Result<u64, i32>
    {
        let (tx, rx) = oneshot::channel();
        let objkey = ObjKey::dir_entry(name);
        let owned_name = name.to_owned();
        let key = FSKey::new(parent, objkey);
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                let rfs = htable::ReadFilesystem::ReadOnly(&dataset);
                htable::get::<Dirent>(&rfs, key, 0, owned_name)
                .then(move |r| {
                    match r {
                        Ok(de) => tx.send(Ok(de.ino)),
                        Err(e) => tx.send(Err(e.into()))
                    }.unwrap();
                    future::ok::<(), Error>(())
                })
            }).map_err(Error::unhandled)
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
                            v.iter().map(&f).sum()
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

        let f = move |dataset: &Arc<ReadWriteFilesystem>, _parent, ino| {
            let dot_dirent = Dirent {
                ino,
                dtype: libc::DT_DIR,
                name:  OsString::from(".")
            };
            let dot_filename = OsString::from(".");
            let dot_dirent_objkey = ObjKey::dir_entry(&dot_filename);
            let dot_dirent_key = FSKey::new(ino, dot_dirent_objkey);

            let dotdot_dirent = Dirent {
                ino: parent,
                dtype: libc::DT_DIR,
                name:  OsString::from("..")
            };
            let dotdot_filename = OsString::from("..");
            let dotdot_dirent_objkey = ObjKey::dir_entry(&dotdot_filename);
            let dotdot_dirent_key = FSKey::new(ino, dotdot_dirent_objkey);

            let parent_inode_key = FSKey::new(parent, ObjKey::Inode);
            let dataset2 = dataset.clone();
            let dataset3 = dataset.clone();
            let dataset4 = dataset.clone();
            let nlink_fut = dataset.get(parent_inode_key)
                .and_then(move |r| {
                    let mut value = r.unwrap();
                    {
                        let inode = value.as_mut_inode().unwrap();
                        inode.nlink += 1;
                        let now = time::get_time();
                        inode.mtime = now;
                        inode.ctime = now;
                    }
                    dataset2.insert(parent_inode_key, value)
                });

            let dot_fut = htable::insert(dataset3, dot_dirent_key,
                dot_dirent, dot_filename);
            let dotdot_fut = htable::insert(dataset4, dotdot_dirent_key,
                dotdot_dirent, dotdot_filename);
            let fut = dot_fut.join3(dotdot_fut, nlink_fut)
            .map(drop);
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
                                          FileType::Block(rdev))
        .callback(Fs::create_ts_callback);
        self.do_create(create_args)
    }

    /// Make a character device
    pub fn mkchar(&self, parent: u64, name: &OsStr, perm: u16, uid: u32,
                  gid: u32, rdev: u32) -> Result<u64, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Char(rdev))
        .callback(Fs::create_ts_callback);
        self.do_create(create_args)
    }

    pub fn mkfifo(&self, parent: u64, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> Result<u64, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Fifo)
        .callback(Fs::create_ts_callback);
        self.do_create(create_args)
    }

    pub fn mksock(&self, parent: u64, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> Result<u64, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Socket)
        .callback(Fs::create_ts_callback);
        self.do_create(create_args)
    }

    /// Check that a directory is safe to delete
    fn ok_to_rmdir(ds: &ReadWriteFilesystem, ino: u64, parent: u64,
                   name: OsString)
        -> impl Future<Item=(), Error=Error> + Send
    {
        ds.range(FSKey::obj_range(ino))
        .fold(false, |found_inode, (_, v)| {
            match v {
                FSValue::DirEntry(dirent) => {
                    if dirent.name != OsStr::new(".") &&
                       dirent.name != OsStr::new("..") {
                        Err(Error::ENOTEMPTY).into_future()
                    } else {
                        Ok(found_inode).into_future()
                    }
                },
                FSValue::DirEntries(_) => {
                    // It is known that "." and ".." do not have a hash
                    // collision (otherwise it would be impossible to ever rmdir
                    // anything).  So the mere presence of this value type
                    // indicates ENOTEMPTY.  We don't need to check the
                    // contents.
                    Err(Error::ENOTEMPTY).into_future()
                },
                FSValue::Inode(inode) => {
                    // TODO: check permissions, file flags, etc
                    // The VFS should've already checked that inode is a
                    // directory.
                    assert_eq!(inode.file_type, FileType::Dir,
                               "rmdir of a non-directory");
                    assert_eq!(inode.nlink, 2,
                        "Hard links to directories are forbidden.  nlink={}",
                        inode.nlink);
                    Ok(true).into_future()
                },
                FSValue::ExtAttr(_) | FSValue::ExtAttrs(_) => {
                    // It's fine to remove a directory with extended attributes.
                    Ok(found_inode).into_future()
                },
                FSValue::InlineExtent(_) | FSValue::BlobExtent(_) => {
                    panic!("Directories should not have extents")
                },
                FSValue::Property(_) => {
                    panic!("Directories should not have properties")
                },

            }
        }).map(move |found_inode| {
            assert!(found_inode,
                concat!("Inode {} not found, but parent ",
                        "direntry {}:{:?} exists!"),
                ino, parent, name);
        })
    }

    pub fn read(&self, ino: u64, offset: u64, size: usize)
        -> Result<SGList, i32>
    {
        // Populate a hole region in an sglist.
        let fill_hole = |sglist: &mut SGList, p: &mut u64, l: usize| {
            let l = cmp::min(l, ZERO_REGION_LEN);
            let zb = ZERO_REGION.try_const().unwrap().slice_to(l);
            *p += zb.len() as u64;
            sglist.push(zb);
        };

        let mut size64 = size as u64;
        let (tx, rx) = oneshot::channel();
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        self.handle.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                // First lookup the inode to get the file size
                dataset.get(inode_key)
                .and_then(move |r| {
                    let dataset2 = dataset.clone();
                    let mut value = r.unwrap();
                    {
                        let inode = value.as_mut_inode().unwrap();
                        let now = time::get_time();
                        // TODO: only update if self.atime
                        inode.atime = now;
                    }
                    let fsize = value.as_inode().unwrap().size;
                    // Truncate read to file size
                    size64 = size64.min(fsize.saturating_sub(offset));
                    let rs = RECORDSIZE as u64;
                    let baseoffset = offset - (offset % rs);

                    let end = offset + size64;
                    let erange = FSKey::extent_range(ino, baseoffset..end);
                    let initial = (Vec::<IoVec>::new(), offset, 0usize);
                    type T = (u64, DivBuf);
                    let data_fut = dataset.range(erange)
                    .and_then(move |(k, v)| {
                        let ofs = k.offset();
                        match v.as_extent().unwrap() {
                            Extent::Inline(ile) => {
                                let buf = ile.buf.try_const().unwrap();
                                Box::new(Ok((ofs, buf)).into_future())
                                    as Box<Future<Item=T, Error=Error> + Send>
                            },
                            Extent::Blob(be) => {
                                let bfut = dataset.get_blob(be.rid)
                                .map(move |bbuf| (ofs, *bbuf));
                                Box::new(bfut)
                                    as Box<Future<Item=T, Error=Error> + Send>
                            }
                        }
                    }).fold(initial, move |acc, (ofs, mut db)| {
                        let (mut sglist, mut p, rec) = acc;
                        if ofs < p {
                            // Trim the beginning of the buffer, if this is the
                            // first record.  Trim the end, too, if it's the
                            // last.
                            assert_eq!(rec, 0);
                            let s = p - ofs;
                            let e = cmp::min(db.len() as u64, p + size64 - ofs);
                            if e > s {
                                let tb = db.slice(s as usize, e as usize);
                                p += tb.len() as u64;
                                sglist.push(tb);
                            }
                        } else {
                            // Fill in any hole
                            while ofs > p {
                                let l = (ofs - p) as usize;
                                fill_hole(&mut sglist, &mut p, l);
                            }
                            // Trim the end of the buffer.
                            if ofs + db.len() as u64 > offset + size64 {
                                db.split_off((offset + size64 - ofs) as usize);
                            }
                            p += db.len() as u64;
                            sglist.push(db);
                        }
                        future::ok::<(SGList, u64, usize), Error>
                            ((sglist, p, rec + 1))
                    }).map(move |(mut sglist, mut p, _rec)| {
                        // Fill in any hole at the end.
                        while p - offset < size64 {
                            let l = (size64 - (p - offset)) as usize;
                            fill_hole(&mut sglist, &mut p, l);
                        }
                        sglist
                    });
                    let atime_fut = dataset2.insert(inode_key, value);
                    data_fut.join(atime_fut)
                    .map(|(sglist, _)| {
                        tx.send(Ok(sglist)).unwrap();
                    })
                })
            }).map_err(Error::unhandled)
        ).unwrap();
        rx.wait().unwrap()
    }

    // TODO: instead of the full size struct libc::dirent, use a variable size
    // structure in the mpsc channel
    pub fn readdir(&self, ino: u64, _fh: u64, soffs: i64)
        -> impl Iterator<Item=Result<(libc::dirent, i64), i32>>
    {
        type T = Result<(libc::dirent, i64), i32>;
        type LoopFut = Box<Future<Item=mpsc::Sender<T>, Error=Error> + Send>;

        bitfield! {
            struct Cursor(u64);
            // Offset of the BTree key of the next dirent
            u64; offset, _: 63, 8;
            // Index within the bucket, if any, of the next dirent
            u8; bucket_idx, _: 7, 0;
        }
        impl Cursor {
            fn new(offset: u64, bucket_idx: usize) -> Self {
                debug_assert!(bucket_idx <= u8::max_value() as usize,
                    "A directory has a > 256-way hash collision?");
                Cursor((offset << 8) | bucket_idx as u64)
            }
        }
        impl From<i64> for Cursor {
            fn from(t: i64) -> Self {
                Cursor(t as u64)
            }
        }

        const DIRENT_SIZE: usize = mem::size_of::<libc::dirent>();
        // Big enough to fill a 4KB page with full-size dirents
        const CHANSIZE: usize = 4096 / DIRENT_SIZE;

        let send = move |tx: mpsc::Sender<T>, offs: Cursor, dirent: &Dirent| {
            let namlen = dirent.name.as_bytes().len();
            let mut reply = libc::dirent {
                d_fileno: dirent.ino as u32,
                d_reclen: DIRENT_SIZE as u16,
                d_type: dirent.dtype,
                d_namlen: namlen as u8,
                d_name: unsafe{mem::zeroed()}
            };
            // libc::dirent uses "char" when it should be using "unsigned char",
            // so we need an unsafe conversion
            let p = dirent.name.as_bytes() as *const [u8] as *const [i8];
            reply.d_name[0..namlen].copy_from_slice(unsafe{&*p});
            tx.send(Ok((reply, offs.0 as i64)))
            .map_err(|_| Error::EPIPE)
        };

        let (tx, rx) = mpsc::channel(CHANSIZE);
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                let cursor = Cursor::from(soffs);
                let offs = cursor.offset();
                let bucket_idx = cursor.bucket_idx();
                let fut = dataset.range(FSKey::dirent_range(ino, offs))
                    .fold(tx, move |tx, (k, v)| {
                        match v {
                            FSValue::DirEntry(dirent) => {
                                let curs = Cursor::new(k.offset() + 1, 0);
                                Box::new(send(tx, curs, &dirent)) as LoopFut
                            },
                            FSValue::DirEntries(bucket) => {
                                let bucket_size = bucket.len();
                                let fut = stream::iter_ok(
                                    bucket.into_iter()
                                    .skip(bucket_idx as usize)
                                    .enumerate()
                                )
                                .fold(tx, move |tx, (i, dirent)| {
                                    let idx = i + bucket_idx as usize;
                                    let curs = if idx < bucket_size - 1 {
                                        Cursor::new(k.offset(), idx + 1)
                                    } else {
                                        Cursor::new(k.offset() + 1, 0)
                                    };
                                    send(tx, curs, &dirent)
                                });
                                Box::new(fut) as LoopFut
                            }
                            x => panic!("Unexpected value {:?} for key {:?}",
                                        x, k)
                        }
                    }).map(drop);
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
        rx.wait().map(Result::unwrap)
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
            }).map_err(Error::unhandled)
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
        // 3a) Update old parent's attributes
        // 3b) Update new parent's attributes, unless it's the same as old
        //     parent.
        // 3ci) If dst existed and is not a directory, decrement its link count
        // 3cii) If dst existed and is a directory, remove it
        let (tx, rx) = oneshot::channel();
        let src_objkey = ObjKey::dir_entry(&name);
        let owned_name = name.to_owned();
        let owned_name2 = owned_name.clone();
        let dst_objkey = ObjKey::dir_entry(&newname);
        let owned_newname = newname.to_owned();
        let owned_newname2 = owned_newname.clone();
        let owned_newname3 = owned_newname.clone();
        let samedir = parent == newparent;
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                let ds = Arc::new(dataset);
                let ds4 = ds.clone();
                let ds5 = ds.clone();
                let ds6 = ds.clone();
                let dst_de_key = FSKey::new(newparent, dst_objkey);
                // 0) Check conditions
                htable::get(&htable::ReadFilesystem::ReadWrite(ds.as_ref()),
                            dst_de_key, 0, owned_newname)
                .then(move |r: Result<Dirent, Error>| {
                    match r {
                        Ok(dirent) => {
                            if dirent.dtype != libc::DT_DIR {
                                // Overwriting non-directories is allowed
                                Box::new(Ok(()).into_future())
                                    as Box<Future<Item=(), Error=Error> + Send>
                            } else {
                                // Is it a nonempty directory?
                                Box::new(Fs::ok_to_rmdir(&ds4, dirent.ino,
                                    newparent, owned_newname3))
                                    as Box<Future<Item=(), Error=Error> + Send>
                            }
                        },
                        Err(Error::ENOENT) => {
                            // Destination doesn't exist.  No problem!
                            Box::new(Ok(()).into_future())
                                as Box<Future<Item=(), Error=Error> + Send>
                        },
                        Err(e) => {
                            // Other errors should propagate upwards
                            Box::new(Err(e).into_future())
                                as Box<Future<Item=(), Error=Error> + Send>
                        }
                    }
                }).and_then(move |_| {
                    // 1) Remove the source directory entry
                    let src_de_key = FSKey::new(parent, src_objkey);
                    htable::remove::<Arc<ReadWriteFilesystem>, Dirent>
                        (ds5.clone(), src_de_key, 0, owned_name2)
                }).and_then(move |mut dirent| {
                    // 2) Insert the new directory entry
                    let isdir = dirent.dtype == libc::DT_DIR;
                    dirent.name = owned_newname2.clone();
                    htable::insert(ds6, dst_de_key, dirent, owned_newname2)
                    .map(move |r| {
                        let old_dst_ino = r.map(|dirent| dirent.ino);
                        (old_dst_ino, isdir)
                    })
                }).and_then(move |(old_dst_ino, isdir)| {
                    // 3a) Decrement parent dir's link count
                    let ds2 = ds.clone();
                    let parent_ino_key = FSKey::new(parent, ObjKey::Inode);
                    let p_nlink_fut = ds.get(parent_ino_key)
                    .and_then(move |r| {
                        let mut value = r.unwrap();
                        {
                            let inode = value.as_mut_inode().unwrap();
                            let now = time::get_time();
                            inode.mtime = now;
                            inode.ctime = now;
                            if isdir && !samedir {
                                inode.nlink -= 1;
                            }
                        }
                        ds2.insert(parent_ino_key, value)
                        .map(drop)
                    });
                    let np_nlink_fut =
                    if !samedir {
                        // 3b) Increment new parent dir's link count
                        let ds3 = ds.clone();
                        let newparent_ino_key = FSKey::new(newparent,
                                                           ObjKey::Inode);
                        let fut = ds.get(newparent_ino_key)
                        .and_then(move |r| {
                            let mut value = r.unwrap();
                            {
                                let inode = value.as_mut_inode().unwrap();
                                let now = time::get_time();
                                inode.mtime = now;
                                inode.ctime = now;
                                if isdir && old_dst_ino.is_none() {
                                    inode.nlink += 1;
                                }
                            }
                            ds3.insert(newparent_ino_key, value)
                            .map(drop)
                        });
                        Box::new(fut)
                            as Box<Future<Item=(), Error=Error> + Send>
                    } else {
                        Box::new(Ok(()).into_future())
                            as Box<Future<Item=(), Error=Error> + Send>
                    };
                    let unlink_fut = if let Some(v) = old_dst_ino {
                        // 3ci) Decrement old dst's link count
                        if isdir {
                            let fut = Fs::do_rmdir(ds, newparent, v, false);
                            Box::new(fut)
                                as Box<Future<Item=(), Error=Error> + Send>
                        } else {
                            let fut = Fs::do_unlink(ds.clone(), v)
                            .map(drop);
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
            }).map_err(Error::unhandled)
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
        let owned_name2 = owned_name.clone();
        let owned_name3 = owned_name.clone();
        let objkey = ObjKey::dir_entry(&owned_name);
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                let ds = Arc::new(dataset);
                let ds2 = ds.clone();
                // 1) Lookup the directory
                let key = FSKey::new(parent, objkey);
                htable::get(&htable::ReadFilesystem::ReadWrite(&ds), key, 0,
                            owned_name3)
                .and_then(move |de: Dirent| {
                    let ino = de.ino;
                    // 2) Check that the directory is empty
                    Fs::ok_to_rmdir(&ds, ino, parent, owned_name2)
                    .map(move |_| ino)
                }).and_then(move |ino| {
                    // 3) Remove the parent dir's dir_entry
                    let de_key = FSKey::new(parent, objkey);
                    let dirent_fut = htable::remove::<Arc<ReadWriteFilesystem>,
                                                      Dirent>
                        (ds2.clone(), de_key, 0, owned_name);

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
            }).map_err(Error::unhandled)
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn setattr(&self, ino: u64, mut attr: SetAttr) -> Result<(), i32> {
        let (tx, rx) = oneshot::channel();
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                let ds = Arc::new(dataset);
                if attr.ctime.is_none() {
                    let now = time::get_time();
                    if attr.ctime.is_none() {
                        attr.ctime = Some(now);
                    }
                }
                Fs::do_setattr(ds, ino, attr)
                .map(move |_| tx.send(Ok(())).unwrap())
            }).map_err(Error::unhandled)
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
        let owned_name = name.to_owned();
        let extattr = ExtAttr::Inline(InlineExtAttr {
            namespace: ns,
            name: owned_name.clone(),
            extent
        });
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                htable::insert(dataset, key, extattr, owned_name)
                .map(move |_| tx.send(Ok(())).unwrap())
            }).map_err(Error::unhandled)
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
            }).map_err(Error::unhandled)
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
                                          file_type)
        .callback(Fs::create_ts_callback);
        self.do_create(create_args)
    }

    pub fn sync(&self) {
        let (tx, rx) = oneshot::channel::<()>();
        self.handle.spawn(
            self.db.sync_transaction()
            .map_err(Error::unhandled)
            .map(|_| tx.send(()).unwrap())
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn unlink(&self, parent: u64, name: &OsStr) -> Result<(), i32> {
        // Outline:
        // 1) Lookup and remove the directory entry
        // 2) Unlink the Inode
        // 3) Update parent's mtime and ctime
        let (tx, rx) = oneshot::channel();
        let owned_name = name.to_os_string();
        let dekey = ObjKey::dir_entry(&owned_name);
        self.handle.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                // 1) Lookup and remove the directory entry
                let key = FSKey::new(parent, dekey);
                htable::remove::<Arc<ReadWriteFilesystem>, Dirent>
                    (dataset.clone(), key, 0, owned_name)
                .and_then(move |dirent|  {
                    // 2) Unlink the inode
                    let unlink_fut = Fs::do_unlink(dataset.clone(), dirent.ino);
                    let now = time::get_time();
                    let mut attr = SetAttr::default();
                    attr.ctime = Some(now);
                    attr.mtime = Some(now);
                    let ts_fut = Fs::do_setattr(dataset, parent, attr);
                    unlink_fut.join(ts_fut)
                }).then(move |r| {
                    match r {
                        Ok(_) => tx.send(Ok(())),
                        Err(e) => tx.send(Err(e.into()))
                    }.expect("FS::unlink: send failed");
                    Ok(()).into_future()
                })
            }).map_err(Error::unhandled)
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn write(&self, ino: u64, offset: u64, data: &[u8], _flags: u32)
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
            } else if rec == nrecs - 1 {
                (reclen1 + (rec - 1) * RECORDSIZE)..datalen
            } else {
                (reclen1 + (rec - 1) * RECORDSIZE)..(reclen1 + rec * RECORDSIZE)
            };
            // Data copy
            let v = Vec::from(&data[range]);
            Arc::new(DivBufShared::from(v))
        }).collect::<Vec<_>>();

        self.handle.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                let inode_key = FSKey::new(ino, ObjKey::Inode);
                dataset.get(inode_key)
                .and_then(move |r| {
                    let mut value = r.unwrap();
                    let filesize = value.as_inode().unwrap().size;
                    let data_futs = sglist.into_iter()
                        .enumerate()
                        .map(|(i, dbs)| {
                        let ds3 = dataset.clone();
                        Fs::write_record(ino, filesize, offset, i, dbs, ds3)
                    }).collect::<Vec<_>>();
                    let new_size = cmp::max(filesize, offset + datalen as u64);
                    {
                        let inode = value.as_mut_inode().unwrap();
                        inode.size = new_size;
                        let now = time::get_time();
                        inode.mtime = now;
                        inode.ctime = now;
                    }
                    let ino_fut = dataset.insert(inode_key, value);
                    future::join_all(data_futs).join(ino_fut)
                }).map(move |_| tx.send(Ok(datalen as u32)).unwrap())
            }).map_err(Error::unhandled)
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
                        let r = Box::new(DivBufShared::from(v).try_const()
                                         .unwrap());
                        Box::new(Ok(r).into_future())
                            as Box<Future<Item=_, Error=_> + Send>
                    },
                    Some(FSValue::InlineExtent(ile)) => {
                        let r = Box::new(ile.buf.try_const().unwrap());
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
                    let overlay = dbs.try_const().unwrap();
                    let l = overlay.len();
                    let r = if i == 0 {
                        let s = (offset - baseoffset) as usize;
                        let e = s + l;
                        s..e
                    } else {
                        0..l
                    };
                    if r.end > base.len() {
                        // We must be appending
                        base.resize(r.end, 0);
                    }
                    base[r].copy_from_slice(&overlay[..]);
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
    let mut db = Database::default();
    db.expect_new_fs()
        .called_once()
        .returning(|_| Box::new(Ok(TreeID::Fs(0)).into_future()));
    db.expect_fsread()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    db.expect_get_prop()
        .called_times(2)
        .returning(|(_tree_id, propname)| {
            let prop = Property::default_value(propname);
            let source = PropertySource::Default;
            Box::new(Ok((prop, source)).into_future())
        });
    let tree_id = rt.block_on(db.new_fs(Vec::new())).unwrap();
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
    let old_ts = time::Timespec::new(0, 0);
    ds.expect_get()
        .called_once()
        .with(FSKey::new(1, ObjKey::Inode))
        .returning(move |_| {
            let inode = Inode {
                size: 0,
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
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });
    ds.then().expect_insert()
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
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_inode() &&
            args.1.as_inode().unwrap().file_type == FileType::Dir &&
            args.1.as_inode().unwrap().atime == old_ts &&
            args.1.as_inode().unwrap().mtime != old_ts &&
            args.1.as_inode().unwrap().ctime != old_ts &&
            args.1.as_inode().unwrap().birthtime == old_ts
        })).returning(|_| Box::new(Ok(None).into_future()));
    let mut opt_ds = Some(ds);
    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);

    assert_eq!(ino, fs.create(1, &filename, 0o644, 123, 456).unwrap());
}

/// Create experiences a hash collision when adding the new directory entry
#[test]
fn create_hash_collision() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
    // For the unit tests, we skip creating the "/" directory
    let ino = 1;
    let other_ino = 100;
    let filename = OsString::from("x");
    let filename2 = filename.clone();
    let filename3 = filename.clone();
    let filename4 = filename.clone();
    let other_filename = OsString::from("y");
    let other_filename2 = other_filename.clone();
    let old_ts = time::Timespec::new(0, 0);
    ds.expect_get()
        .called_once()
        .with(FSKey::new(1, ObjKey::Inode))
        .returning(move |_| {
            let inode = Inode {
                size: 0,
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
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_inode()
        })).returning(|_| Box::new(Ok(None).into_future()));
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_direntry()
        })).returning(move |_| {
            // Return a different directory entry
            let name = other_filename2.clone();
            let dirent = Dirent{ino: other_ino, dtype: libc::DT_REG, name};
            let v = FSValue::DirEntry(dirent);
            Box::new(Ok(Some(v)).into_future())
        });
    ds.then().expect_get()
        .called_once()
        .with(passes(move |args: &FSKey| {
            args.is_direntry()
        })).returning(move |_| {
            // Return the dirent that we just inserted
            let name = filename2.clone();
            let dirent = Dirent{ino: 1, dtype: libc::DT_REG, name};
            let v = FSValue::DirEntry(dirent);
            Box::new(Ok(Some(v)).into_future())
        });
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            // Check that we're inserting a bucket with both direntries.  The
            // order doesn't matter.
            let dirents = args.1.as_direntries().unwrap();
            args.0.is_direntry() &&
            dirents[0].dtype == libc::DT_REG &&
            dirents[0].name == other_filename &&
            dirents[0].ino == other_ino &&
            dirents[1].dtype == libc::DT_REG &&
            dirents[1].name == filename3 &&
            dirents[1].ino == ino
        })).returning(move |_| {
            // Return the dirent that we just inserted
            let name = filename4.clone();
            let dirent = Dirent{ino: 1, dtype: libc::DT_REG, name};
            let v = FSValue::DirEntry(dirent);
            Box::new(Ok(Some(v)).into_future())
        });
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_inode() &&
            args.1.as_inode().unwrap().file_type == FileType::Dir &&
            args.1.as_inode().unwrap().atime == old_ts &&
            args.1.as_inode().unwrap().mtime != old_ts &&
            args.1.as_inode().unwrap().ctime != old_ts &&
            args.1.as_inode().unwrap().birthtime == old_ts
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

// Rmdir a directory with extended attributes, and don't forget to free them
// too!
#[test]
fn rmdir_with_blob_extattr() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::new();
    let parent_ino = 1;
    let ino = 2;
    let xattr_blob_rid = RID(88888);
    let filename = OsString::from("x");
    let filename2 = filename.clone();
    let filename3 = filename.clone();

    // First it must do a lookup
    ds.expect_get()
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
    // This part comes from ok_to_rmdir
    ds.then().expect_range()
        .called_once()
        .with(FSKey::obj_range(ino))
        .returning(move |_| {
            // Return one blob extattr, one inline extattr, one inode, and two
            // directory entries for "." and ".."
            let dotdot_name = OsString::from("..");
            let k0 = FSKey::new(ino, ObjKey::dir_entry(&dotdot_name));
            let dotdot_dirent = Dirent {
                ino: parent_ino,
                dtype: libc::DT_DIR,
                name:  dotdot_name
            };
            let v0 = FSValue::DirEntry(dotdot_dirent);

            let dot_name = OsString::from(".");
            let k1 = FSKey::new(ino, ObjKey::dir_entry(&dot_name));
            let dot_dirent = Dirent {
                ino,
                dtype: libc::DT_DIR,
                name:  dot_name
            };
            let v1 = FSValue::DirEntry(dot_dirent);

            let now = time::get_time();
            let inode = Inode {
                size: 0,
                nlink: 2,
                flags: 0,
                atime: now,
                mtime: now,
                ctime: now,
                birthtime: now,
                uid: 0,
                gid: 0,
                file_type: FileType::Dir,
                perm: 0o755,
            };
            let k2 = FSKey::new(ino, ObjKey::Inode);
            let v2 = FSValue::Inode(inode);

            let namespace = ExtAttrNamespace::User;
            let name0 = OsString::from("foo");
            let k3 = FSKey::new(ino, ObjKey::extattr(namespace, &name0));
            let extent0 = BlobExtent{lsize: 4096, rid: xattr_blob_rid};
            let be = BlobExtAttr{namespace, name: name0, extent: extent0};
            let v3 = FSValue::ExtAttr(ExtAttr::Blob(be));

            let name1 = OsString::from("bar");
            let k4 = FSKey::new(ino, ObjKey::extattr(namespace, &name1));
            let dbs1 = Arc::new(DivBufShared::from(vec![0u8; 1]));
            let extent1 = InlineExtent::new(dbs1);
            let ie = InlineExtAttr{namespace, name: name1, extent: extent1};
            let v4 = FSValue::ExtAttr(ExtAttr::Inline(ie));
            let items = vec![(k0, v0), (k1, v1), (k2, v2), (k3, v3), (k4, v4)];
            Box::new(stream::iter_ok(items))
        });
    ds.then().expect_remove()
        .called_once()
        .with(FSKey::new(parent_ino, ObjKey::dir_entry(&filename)))
        .returning(move |_| {
            let dirent = Dirent {
                ino,
                dtype: libc::DT_REG,
                name: filename3.clone()
            };
            let v = Some(FSValue::DirEntry(dirent));
            Box::new(Ok(v).into_future())
        });
    ds.expect_range_delete()
        .called_once()
        .with(FSKey::obj_range(ino))
        .returning(|_| {
            Box::new(Ok(()).into_future())
        });
    ds.expect_range()
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
        .with(passes(move |rid: &RID| xattr_blob_rid == *rid))
        .returning(|_| {
            Box::new(Ok(()).into_future())
         });
    ds.expect_get()
        .called_once()
        .with(FSKey::new(parent_ino, ObjKey::Inode))
        .returning(|_| {
            let now = time::get_time();
            let inode = Inode {
                size: 0,
                nlink: 3,
                flags: 0,
                atime: now,
                mtime: now,
                ctime: now,
                birthtime: now,
                uid: 0,
                gid: 0,
                file_type: FileType::Dir,
                perm: 0o755,
            };
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });
    ds.expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0 == FSKey::new(parent_ino, ObjKey::Inode) &&
            args.1.as_inode().unwrap().nlink == 2
        })).returning(|_| {
            let now = time::get_time();
            let inode = Inode {
                size: 0,
                nlink: 2,
                flags: 0,
                atime: now,
                mtime: now,
                ctime: now,
                birthtime: now,
                uid: 0,
                gid: 0,
                file_type: FileType::Dir,
                perm: 0o755,
            };
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });

    let mut opt_ds = Some(ds);
    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.rmdir(1, &filename);
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
        &ie.extent.buf.try_const().unwrap()[..] == value2.as_bytes()
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
    ds.then().expect_get()
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

    let old_ts = time::Timespec::new(0, 0);
    ds.then().expect_get()
        .called_once()
        .with(FSKey::new(1, ObjKey::Inode))
        .returning(move |_| {
            let inode = Inode {
                size: 0,
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
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });
    ds.then().expect_range()
        .called_once()
        .with(FSKey::extent_range(ino, ..))
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
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_inode() &&
            args.1.as_inode().unwrap().file_type == FileType::Dir &&
            args.1.as_inode().unwrap().atime == old_ts &&
            args.1.as_inode().unwrap().mtime != old_ts &&
            args.1.as_inode().unwrap().ctime != old_ts &&
            args.1.as_inode().unwrap().birthtime == old_ts
        })).returning(|_| Box::new(Ok(None).into_future()));
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
    let old_ts = time::Timespec::new(0, 0);
    ds.then().expect_get()
        .called_once()
        .with(FSKey::new(1, ObjKey::Inode))
        .returning(move |_| {
            let inode = Inode {
                size: 0,
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
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_inode() &&
            args.1.as_inode().unwrap().nlink == 1
        })).returning(|_| Box::new(Ok(None).into_future()));
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_inode() &&
            args.1.as_inode().unwrap().file_type == FileType::Dir &&
            args.1.as_inode().unwrap().atime == old_ts &&
            args.1.as_inode().unwrap().mtime != old_ts &&
            args.1.as_inode().unwrap().ctime != old_ts &&
            args.1.as_inode().unwrap().birthtime == old_ts
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
    let old_ts = time::Timespec::new(0, 0);
    ds.then().expect_get()
        .called_once()
        .with(FSKey::new(1, ObjKey::Inode))
        .returning(move |_| {
            let inode = Inode {
                size: 0,
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
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });

    ds.then().expect_range()
        .called_once()
        .with(FSKey::extent_range(ino, ..))
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
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_inode() &&
            args.1.as_inode().unwrap().file_type == FileType::Dir &&
            args.1.as_inode().unwrap().atime == old_ts &&
            args.1.as_inode().unwrap().mtime != old_ts &&
            args.1.as_inode().unwrap().ctime != old_ts &&
            args.1.as_inode().unwrap().birthtime == old_ts
        })).returning(|_| Box::new(Ok(None).into_future()));
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
    let old_ts = time::Timespec::new(0, 0);
    ds.then().expect_get()
        .called_once()
        .with(FSKey::new(1, ObjKey::Inode))
        .returning(move |_| {
            let inode = Inode {
                size: 0,
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
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });
    ds.then().expect_range()
        .called_once()
        .with(FSKey::extent_range(ino, ..))
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
    ds.then().expect_insert()
        .called_once()
        .with(passes(move |args: &(FSKey, FSValue<RID>)| {
            args.0.is_inode() &&
            args.1.as_inode().unwrap().file_type == FileType::Dir &&
            args.1.as_inode().unwrap().atime == old_ts &&
            args.1.as_inode().unwrap().mtime != old_ts &&
            args.1.as_inode().unwrap().ctime != old_ts &&
            args.1.as_inode().unwrap().birthtime == old_ts
        })).returning(|_| Box::new(Ok(None).into_future()));

    let mut opt_ds = Some(ds);
    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.unlink(1, &filename);
    assert_eq!(Ok(()), r);
}


}
