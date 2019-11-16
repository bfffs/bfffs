// vim: tw=80
//! Common VFS implementation

use bitfield::*;
use crate::{
    *,
    common::{
        *,
        database::*,
        dataset::ReadDataset,
        fs_tree::*,
        property::*
    }
};
use divbuf::{DivBufShared, DivBuf};
use futures::{
    Future,
    IntoFuture,
    Sink,
    Stream,
    future,
    stream,
    sync::{
        mpsc,
        oneshot
    }
};
use libc;
use std::{
    cmp,
    ffi::{OsStr, OsString},
    mem,
    os::unix::ffi::OsStrExt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    }
};
#[cfg(not(test))] use std::io;
use time;
use tokio_io_pool;

pub use self::fs_tree::ExtAttr;
pub use self::fs_tree::ExtAttrNamespace;

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
            -> Box<dyn Future<Item=Option<FSValue<RID>>, Error=Error> + Send>
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
        type MyFut<T> = Box<dyn Future<Item=T, Error=Error> + Send>;
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
        type MyFut<T> = Box<dyn Future<Item=Option<T>, Error=Error> + Send>;
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
                            let new = T::try_from(v).unwrap();
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
                        let new = T::try_from(v).unwrap();
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
                        boxfut!(Ok(old).into_future())
                    } else {
                        // Hash collision.  Put it back, and return not found
                        let value = old.into_fsvalue();
                        let fut = dataset.as_ref().insert(key, value)
                        .and_then(|_| Err(T::ENOTFOUND).into_future());
                        boxfut!(fut)
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
                        boxfut!(fut)
                    } else {
                        // A 3 (or more) way hash collision between the
                        // accessed item and at least two different ones.
                        let v = T::into_bucket(old);
                        let fut = dataset.as_ref().insert(key, v)
                        .and_then(|_| Err(T::ENOTFOUND).into_future());
                        boxfut!(fut)
                    }
                },
                HTValue::None => {
                    boxfut!(Err(T::ENOTFOUND).into_future())
                },
                HTValue::Other(x) =>
                    panic!("Unexpected value {:?} for key {:?}", x, key)
            }
        })
    }
}

/// BFFF's version of `struct uio`.  For userland implementations, this is just
/// a wrapper around a slice.  For kernelland implementations, it will probably
/// actually wrap `struct uio`.
// The purpose of Uio is to move data copies out of Fs::write.  Kernel clients
// will need to copyin(9) at some point, and this is where they'll do it.
pub struct Uio {
    data: &'static [u8]
}

impl Uio {
    /// Divide this `uio` up into some kind of scatter/gather list.
    ///
    /// # Safety
    ///
    /// `Uio` does not track the lifetime of the buffer from which it was
    /// created.  The caller must ensure that that buffer is still valid when
    /// `into_chunks` is called.
    //
    /// Perhaps with async/await, bfffs::common::fs::Fs::write could be made
    /// safe with non-'static references.
    unsafe fn into_chunks<F, T>(self, offset0: usize, rs: usize, f: F) -> Vec<T>
        where F: Fn(Vec<u8>) -> T
    {
        let nrecs = (div_roundup(offset0 + self.len(), rs)
                     - (offset0 / rs)) as usize;
        let reclen1 = cmp::min(self.len(), rs - offset0);
        (0..nrecs).map(|rec| {
            let range = if rec == 0 {
                0..reclen1
            } else if rec == nrecs - 1 {
                (reclen1 + (rec - 1) * rs)..self.len()
            } else {
                (reclen1 + (rec - 1) * rs)..(reclen1 + rec * rs)
            };
            // Data copy
            let v = Vec::from(&self.data[range]);
            f(v)
        }).collect::<Vec<T>>()
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

impl<'a> From<&'a [u8]> for Uio {
    /// Create a new `Uio`.  The caller must ensure that the `Uio` drops before
    /// the `sl` reference becomes invalid.
    fn from(sl: &'a [u8]) -> Self {
        let data = unsafe {std::mem::transmute::<&'a [u8], &'static [u8]>(sl) };
        Uio { data }
    }
}

/// Information about an in-use file
///
/// Basically, this is the stuff that would go in a vnode's v_data field
#[derive(Debug)]
pub struct FileData {
    ino: u64,
    pub lookup_count: u64,
    /// This file's parent's inode number.  Only valid for directories that
    /// aren't the root directory.
    parent: Option<u64>
}

impl FileData {
    pub fn ino(&self) -> u64
    {
        self.ino
    }

    /// Create a new `FileData`
    fn new(parent: Option<u64>, ino: u64) -> FileData {
        FileData{ino, lookup_count: 1, parent}
    }

    /// Create a new `FileData` for use in tests outside of this module
    pub fn new_for_tests(parent: Option<u64>, ino: u64) -> Self {
        FileData::new(parent, ino)
    }

    pub fn parent(&self) -> Option<u64> {
        self.parent
    }

    pub fn reparent(&mut self, parent: u64) {
        if self.parent.is_some() {
            self.parent = Some(parent);
        } else {
            // Probably a non-directory, which doesn't store its parent.
        }
    }
}

/// Generic Filesystem layer.
///
/// Bridges the synchronous with Tokio domains, and the system-independent with
/// system-dependent filesystem interfaces.
pub struct Fs {
    db: Arc<Database>,
    next_object: AtomicU64,
    handle: tokio_io_pool::Handle,
    tree: TreeID,

    // These options may only be changed when the filesystem is mounting or
    // remounting the filesystem.
    /// Update files' atimes when reading?
    atime: bool,
    /// Record size for new files, in bytes, log base 2.
    record_size: u8
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
#[derive(Clone, Copy, Debug, PartialEq)]
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

/// Private trait bound for functions that can be used as callbacks for
/// Fs::create
type CreateCallback = fn(&Arc<ReadWriteFilesystem>, u64, u64)
        -> Box<dyn Future<Item=(), Error=Error> + Send + 'static>;

/// Arguments for Fs::do_create
struct CreateArgs<'a>
{
    parent: &'a FileData,
    file_type: FileType,
    flags: u64,
    name: OsString,
    perm: u16,
    uid: u32,
    gid: u32,
    nlink: u64,
    cb: CreateCallback
}

impl<'a> CreateArgs<'a> {
    const DEFAULT_CB: CreateCallback = Fs::create_ts_callback;

    pub fn callback(mut self, f: CreateCallback) -> Self
    {
        self.cb = f;
        self
    }

    // Enable once chflags(2) support comes in
    //pub fn flags(mut self, flags: u64) -> Self {
        //self.flags = flags;
        //self
    //}

    pub fn new(parent: &'a FileData, name: &OsStr, perm: u16, uid: u32,
               gid: u32, file_type: FileType) -> Self
    {
        let cb = Self::DEFAULT_CB;
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
    pub fn deleteextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
                         name: &OsStr)
        -> Result<(), i32>
    {
        let (tx, rx) = oneshot::channel();
        let objkey = ObjKey::extattr(ns, &name);
        let name = name.to_owned();
        let key = FSKey::new(fd.ino, objkey);
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
        -> Result<FileData, i32>
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
        let parent_dirent_key = FSKey::new(args.parent.ino,
                                           parent_dirent_objkey);

        let cb = args.cb;
        let parent_ino = args.parent.ino;
        let fd_parent = if args.file_type == FileType::Dir {
            Some(parent_ino)
        } else {
            // Non-directories may be multiply-linked, so the FileData can't
            // accurately store the parent ino.
            None
        };
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
                    let fd = FileData::new(fd_parent, ino);
                    tx.send(Ok(fd)).unwrap()
                })
            }).map_err(Error::unhandled)
        ).unwrap();
        rx.wait().unwrap()
    }

    // Actually delete an inode, which must already be unlinked
    fn do_delete_inode(ds: Arc<ReadWriteFilesystem>, ino: u64)
        -> impl Future<Item=(), Error=Error>
    {
        let ds2 = ds.clone();
        // delete its blob extents and blob extended attributes
        let extent_stream = ds.range(FSKey::extent_range(ino, ..))
        .filter_map(move |(_k, v)| {
            if let Extent::Blob(be) = v.as_extent().unwrap()
            {
                Some(be.rid)
            } else {
                None
            }
        });
        let extattr_stream = Fs::list_extattr_rids(&*ds, ino);
        extent_stream.chain(extattr_stream)
        .for_each(move |rid| ds2.delete_blob(rid))
        .and_then(move |_| {
            // Finally, range_delete its key range, including inode,
            // inline extents, and inline extattrs
            ds.range_delete(FSKey::obj_range(ino))
        })
    }

    // Remove the inode if this was its last reference
    fn do_inactive(ds: Arc<ReadWriteFilesystem>, ino: u64)
        -> impl Future<Item=(), Error=Error>
    {
        let dikey = FSKey::new(0, ObjKey::dying_inode(ino));
        ds.get(dikey)
        .and_then(move |di| {
            match di {
                None => boxfut!(Ok(()).into_future()),
                Some(di2) => {
                    assert_eq!(ino, di2.as_dying_inode().unwrap().ino());
                    boxfut!(Fs::do_delete_inode(ds, ino))
                }
            }
        })
    }

    /// Asynchronously read from a file.
    fn do_read<DS>(dataset: DS, ino: u64, fsize: u64, rs: u64, offset: u64,
                   size: usize)
        -> impl Future<Item=SGList, Error=Error>
        where DS: ReadDataset<FSKey, FSValue<RID>>
    {
        // Populate a hole region in an sglist.
        let fill_hole = |sglist: &mut SGList, p: &mut u64, l: usize| {
            let l = cmp::min(l, ZERO_REGION_LEN);
            let zb = ZERO_REGION.try_const().unwrap().slice_to(l);
            *p += zb.len() as u64;
            sglist.push(zb);
        };

        let mut size64 = size as u64;
        size64 = size64.min(fsize.saturating_sub(offset));
        let baseoffset = offset - (offset % rs);

        let end = offset + size64;
        let erange = FSKey::extent_range(ino, baseoffset..end);
        let initial = (Vec::<IoVec>::new(), offset, 0usize);
        type T = (u64, DivBuf);
        dataset.range(erange)
        .and_then(move |(k, v)| {
            let ofs = k.offset();
            match v.as_extent().unwrap() {
                Extent::Inline(ile) => {
                    let buf = ile.buf.try_const().unwrap();
                    boxfut!(Ok((ofs, buf)).into_future(), T, Error)
                },
                Extent::Blob(be) => {
                    let bfut = dataset.get_blob(be.rid)
                    .map(move |bbuf| (ofs, *bbuf));
                    boxfut!(bfut, T, Error)
                }
            }
        }).fold(initial, move |acc, (ofs, mut db)| {
            let (mut sglist, mut p, rec) = acc;
            if ofs < p {
                // Trim the beginning of the buffer, if this is the first
                // record.  Trim the end, too, if it's the last.
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
            future::ok::<(SGList, u64, usize), Error>((sglist, p, rec + 1))
        }).map(move |(mut sglist, mut p, _rec)| {
            // Fill in any hole at the end.
            while p - offset < size64 {
                let l = (size64 - (p - offset)) as usize;
                fill_hole(&mut sglist, &mut p, l);
            }
            sglist
        })
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
            boxfut!(fut)
        } else {
            boxfut!(Ok(()).into_future())
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
            iv.ctime = attr.ctime.unwrap_or(iv.ctime);
            iv.mtime = attr.mtime.unwrap_or_else(|| {
                if attr.size.is_some() {
                    // Always update mtime when truncating
                    iv.ctime
                } else {
                    iv.mtime
                }
            });
            iv.birthtime = attr.birthtime.unwrap_or(iv.birthtime);
            iv.flags = attr.flags.unwrap_or(iv.flags);

            let truncate_fut = if new_size < old_size {
                assert!(iv.file_type.dtype() == libc::DT_REG);
                let rs = iv.record_size() as u64;
                boxfut!(Fs::do_truncate(dataset.clone(), ino, new_size, rs))
            } else {
                let fut = Ok(()).into_future();
                boxfut!(fut)
            };

            dataset.insert(inode_key, FSValue::Inode(iv))
            .join(truncate_fut)
            .map(drop)
        })
    }

    /// Remove all of a file's data past `size`.  This routine does _not_ update
    /// the Inode
    fn do_truncate(dataset: Arc<ReadWriteFilesystem>, ino: u64, size: u64,
                   rs: u64)
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
        let partial_fut = if size % rs > 0 {
            let ofs = size - size % rs;
            let len = (size - ofs) as usize;
            let k = FSKey::new(ino, ObjKey::Extent(ofs));
            let fut = dataset.get(k)
            .and_then(move |v| {
                match v {
                    None => {
                        // It's a hole; nothing to do
                        boxfut!(Ok(()).into_future())
                    },
                    Some(FSValue::InlineExtent(ile)) => {
                        let mut b = ile.buf.try_mut().unwrap();
                        b.try_truncate(len).unwrap();
                        let extent = InlineExtent::new(ile.buf);
                        let v = FSValue::InlineExtent(extent);
                        boxfut!(dataset.insert(k, v).map(drop))
                    },
                    Some(FSValue::BlobExtent(be)) => {
                        let fut = dataset.remove_blob(be.rid)
                        .and_then(move |dbs: Box<DivBufShared>| {
                            dbs.try_mut()
                                .expect("DivBufShared wasn't uniquely owned")
                                .try_truncate(len)
                                .unwrap();
                            let adbs = Arc::from(dbs);
                            let extent = InlineExtent::new(adbs);
                            let v = FSValue::InlineExtent(extent);
                            dataset.insert(k, v)
                            .map(drop)
                        });
                        boxfut!(fut)
                    },
                    x => panic!("Unexpectec value {:?} for key {:?}",
                                x, k)
                }
            });
            boxfut!(fut)
        } else {
            let fut = Ok(()).into_future();
            boxfut!(fut)
        };
        full_fut.join(partial_fut).map(drop)
    }

    /// Unlink a file whose inode number is known and whose directory entry is
    /// already deleted.
    fn do_unlink(dataset: Arc<ReadWriteFilesystem>,
                 lookup_count: u64,
                 ino: u64)
        -> impl Future<Item=(), Error=Error> + Send
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
            // 2a) Decrement the link count and touch the ctime
            iv.nlink = iv.nlink.saturating_sub(1);
            let nlink = iv.nlink;
            iv.ctime = time::get_time();
            // 2b) Update Inode, if we aren't immediately deleting it
            if nlink > 0 || lookup_count > 0 {
                let fut = if nlink == 0 {
                    // 2c) Record imminent death of inode
                    let dikey = FSKey::new(0, ObjKey::dying_inode(ino));
                    let dival = FSValue::DyingInode(DyingInode::from(ino));
                    let fut = dataset.insert(dikey, dival)
                    .map(|r|
                        if r.is_some() {
                            panic!("Hash collisions on dying inodes are TODO!")
                        }
                    ).map(drop);
                    boxfut!(fut)
                } else {
                    boxfut!(Ok(()).into_future())
                }.join(dataset.insert(key, FSValue::Inode(iv))
                    .map(drop)
                ).map(drop);
                boxfut!(fut)
            } else {
                boxfut!(Fs::do_inactive(dataset, ino))
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
                let last_key_fut = dataset.last_key();

                last_key_fut
                .join3(db2.get_prop(tree, PropertyName::Atime),
                       db2.get_prop(tree, PropertyName::RecordSize))
                .map(move |r| tx.send(r).unwrap())
            }).map_err(Error::unhandled)
        ).unwrap();
        let (last_key, (atimep, _), (recsizep, _)) = rx.wait().unwrap();
        let next_object = AtomicU64::new(last_key.unwrap().object() + 1);
        let atime = atimep.as_bool();
        let record_size = recsizep.as_u8();

        // In the background, delete all dying inodes.  If there are any, it
        // means that the previous mount was uncleanly dismounted.
        handle.spawn(
            database.fswrite(tree, move |dataset| {
                let ds = Arc::new(dataset);
                let ds2 = ds.clone();
                ds.range(FSKey::dying_inode_range())
                .for_each(move |(_k, v)| {
                    let ino = v.as_dying_inode().unwrap().ino();
                    Fs::do_delete_inode(ds.clone(), ino)
                }).and_then(move |_| {
                    // Finally, range delete all of the dying inodes
                    ds2.range_delete(FSKey::dying_inode_range())
                })
            }).map_err(Error::unhandled)
        ).unwrap();

        Fs {
            db: database,
            next_object,
            handle,
            tree,
            atime,
            record_size,
        }
    }

    fn next_object(&self) -> u64 {
        self.next_object.fetch_add(1, Ordering::Relaxed)
    }

    pub fn create(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> Result<FileData, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Reg(self.record_size));
        self.do_create(create_args)
    }

    /// Update the parent's ctime and mtime to the current time.
    fn create_ts_callback(dataset: &Arc<ReadWriteFilesystem>, parent: u64,
                          _ino: u64)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        let now = time::get_time();
        let mut attr = SetAttr::default();
        attr.ctime = Some(now);
        attr.mtime = Some(now);
        boxfut!(Fs::do_setattr(dataset.clone(), parent, attr))
    }

    /// Dump a YAMLized representation of the filesystem's Tree to a plain
    /// `std::fs::File`.
    #[cfg(not(test))]
    pub fn dump(&self, f: &mut dyn io::Write) -> Result<(), i32> {
        self.db.dump(f, self.tree)
        .map_err(|e| e.into())
    }

    /// Tell the file system that the given file is no longer needed by the
    /// client.  Its resources may be freed.
    // Fs::inactive consumes fd because the client should not longer need it.
    pub fn inactive(&self, fd: FileData) {
        let ino = fd.ino();

        let (tx, rx) = oneshot::channel::<Result<(), Error>>();
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                Fs::do_inactive(Arc::new(dataset), ino)
                .then(move |r| {
                    match r {
                        Ok(_) => tx.send(Ok(())),
                        Err(e) => tx.send(Err(e))
                    }.expect("FS::unlink: send failed");
                    Ok(()).into_future()
                })
            }).map_err(Error::unhandled)
        ).unwrap();
        rx.wait().unwrap().expect("Fs::inactive should never fail");
    }

    /// Sync a file's data and metadata to disk so it can be recovered after a
    /// crash.
    pub fn fsync(&self, _fd: &FileData) -> Result<(), i32> {
        // Until we come up with a better mechanism, we must sync the entire
        // file system.
        self.sync();
        Ok(())
    }

    pub fn getattr(&self, fd: &FileData) -> Result<GetAttr, i32> {
        let ino = fd.ino;
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
    pub fn getextattr(&self, fd: &FileData, ns: ExtAttrNamespace, name: &OsStr)
        -> Result<DivBuf, i32>
    {
        let owned_name = name.to_owned();
        let (tx, rx) = oneshot::channel();
        let objkey = ObjKey::extattr(ns, &name);
        let key = FSKey::new(fd.ino, objkey);
        type MyFut = Box<dyn Future<Item=Box<DivBuf>, Error=Error> + Send>;
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
    pub fn getextattrlen(&self, fd: &FileData, ns: ExtAttrNamespace,
                         name: &OsStr)
        -> Result<u32, i32>
    {
        let owned_name = name.to_owned();
        let (tx, rx) = oneshot::channel();
        let objkey = ObjKey::extattr(ns, &name);
        let key = FSKey::new(fd.ino, objkey);
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

    /// Create a hardlink from `fd` to `parent/name`.
    pub fn link(&self, parent: &FileData, fd: &FileData, name: &OsStr)
        -> Result<(), i32>
    {
        // Outline:
        // * Increase the target's link count
        // * Add the new directory entry
        // * Update the parent's mtime and ctime
        let (tx, rx) = oneshot::channel();
        let ino = fd.ino;
        let parent_ino = parent.ino;
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
                    let dirent_key = FSKey::new(parent_ino, dirent_objkey);
                    let dirent_value = FSValue::DirEntry(dirent);
                    let dfut = ds.insert(dirent_key, dirent_value);

                    let now = time::get_time();
                    let mut parent_attr = SetAttr::default();
                    parent_attr.ctime = Some(now);
                    parent_attr.mtime = Some(now);
                    let parent_fut = Fs::do_setattr(ds.clone(), parent_ino,
                        parent_attr);

                    let mut ctime_attr = SetAttr::default();
                    ctime_attr.ctime = Some(now);
                    let ctime_fut = Fs::do_setattr(ds, ino, ctime_attr);

                    ifut.join4(dfut, parent_fut, ctime_fut)
                }).map(move |_| tx.send(Ok(())).unwrap())
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
                    let s = boxstream!(stream::once(Ok(be.extent.rid)));
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
                    let s = boxstream!(stream::iter_ok(rids));
                    Some(s)
                }
                x => panic!("Unexpected value {:?} for key {:?}", x, k)
            }
        }).flatten()
    }

    /// Lookup a file by its file name.
    ///
    /// NB: The client is responsible for managing the lifetime of the returned
    /// `FileData` object.  In particular, the client must ensure that there are
    /// never two `FileData`s for the same directory entry at the same time.
    pub fn lookup(&self, grandparent: Option<&FileData>, parent: &FileData,
        name: &OsStr) -> Result<FileData, i32>
    {
        let (tx, rx) = oneshot::channel();

        let dot = name == OsStr::from_bytes(b".");
        let dotdot = name == OsStr::from_bytes(b"..");
        let parent_ino = if dot {
            parent.parent()
        } else if dotdot {
            grandparent.unwrap().parent()
        } else {
            Some(parent.ino())
        };

        let objkey = ObjKey::dir_entry(name);
        let owned_name = name.to_owned();
        let key = FSKey::new(parent.ino, objkey);
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                let rfs = htable::ReadFilesystem::ReadOnly(&dataset);
                htable::get::<Dirent>(&rfs, key, 0, owned_name)
                .then(move |r| {
                    match r {
                        Ok(de) => {
                            let fd_parent = if de.dtype == libc::DT_DIR {
                                parent_ino
                            } else {
                                None
                            };
                            let fd = FileData::new(fd_parent, de.ino);
                            tx.send(Ok(fd))
                        },
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
    /// - `size`:       A hint of the expected length of the attr list
    /// - `f`:          A function that filters each extended attribute
    ///                 and packs it into an expandable buffer
    ///
    /// # Returns
    ///
    /// A buffer containing all extended attributes' names packed by `f`.
    pub fn listextattr<F>(&self, fd: &FileData, size: u32, f: F)
        -> Result<Vec<u8>, i32>
        where F: Fn(&mut Vec<u8>, &ExtAttr<RID>) + Send + 'static
    {
        let (tx, rx) = oneshot::channel();
        let ino = fd.ino;
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
    pub fn listextattrlen<F>(&self, fd: &FileData, f: F) -> Result<u32, i32>
        where F: Fn(&ExtAttr<RID>) -> u32 + Send + 'static
    {
        let (tx, rx) = oneshot::channel();
        let ino = fd.ino;
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

    pub fn mkdir(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                 gid: u32) -> Result<FileData, i32>
    {
        let nlink = 2;  // One for the parent dir, and one for "."

        fn f(dataset: &Arc<ReadWriteFilesystem>, parent_ino: u64, ino: u64)
            -> Box<dyn Future<Item=(), Error=Error> + Send + 'static>
        {
            let dot_dirent = Dirent {
                ino,
                dtype: libc::DT_DIR,
                name:  OsString::from(".")
            };
            let dot_filename = OsString::from(".");
            let dot_dirent_objkey = ObjKey::dir_entry(&dot_filename);
            let dot_dirent_key = FSKey::new(ino, dot_dirent_objkey);

            let dotdot_dirent = Dirent {
                ino: parent_ino,
                dtype: libc::DT_DIR,
                name:  OsString::from("..")
            };
            let dotdot_filename = OsString::from("..");
            let dotdot_dirent_objkey = ObjKey::dir_entry(&dotdot_filename);
            let dotdot_dirent_key = FSKey::new(ino, dotdot_dirent_objkey);

            let parent_inode_key = FSKey::new(parent_ino, ObjKey::Inode);
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
            boxfut!(fut)
        }

        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Dir)
        .nlink(nlink)
        .callback(f);

        self.do_create(create_args)
    }

    /// Make a block device
    pub fn mkblock(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                   gid: u32, rdev: u32) -> Result<FileData, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Block(rdev));
        self.do_create(create_args)
    }

    /// Make a character device
    pub fn mkchar(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32, rdev: u32) -> Result<FileData, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Char(rdev));
        self.do_create(create_args)
    }

    pub fn mkfifo(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> Result<FileData, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Fifo);
        self.do_create(create_args)
    }

    pub fn mksock(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> Result<FileData, i32>
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
                    // If the directory weren't empty, the loop should've
                    // already discovered that, since DirEntrys's keys are
                    // sorted lower than Inodes'
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
                FSValue::DyingInode(_) => {
                    panic!("Directories should not have dying inodes")
                },
                FSValue::InlineExtent(_) | FSValue::BlobExtent(_) => {
                    panic!("Directories should not have extents")
                },
                FSValue::Property(_) => {
                    panic!("Directories should not have properties")
                },
                #[cfg(test)]
                FSValue::Invalid => unimplemented!()
            }
        }).map(move |found_inode| {
            assert!(found_inode,
                concat!("Inode {} not found, but parent ",
                        "direntry {}:{:?} exists!"),
                ino, parent, name);
        })
    }

    pub fn read(&self, fd: &FileData, offset: u64, size: usize)
        -> Result<SGList, i32>
    {
        let (tx, rx) = oneshot::channel();
        let ino = fd.ino;
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        // We only need a writeable FS reference if we're going to update atime.
        // If not, then only get a read reference.  Read references are better
        // because they can be held during txg syncs.
        if self.atime {
            self.handle.spawn(
                self.db.fswrite(self.tree, move |ds| {
                    ds.get(inode_key)
                    .and_then(move |r| {
                        let mut value = r.expect("Inode not found");
                        let inode = value.as_mut_inode()
                            .expect("Wrong Value type");
                        let now = time::get_time();
                        inode.atime = now;
                        let fsize = inode.size;
                        let rs = inode.record_size() as u64;
                        let afut = ds.insert(inode_key, value);
                        let dfut = Fs::do_read(ds, ino, fsize, rs, offset, size);
                        dfut.join(afut)
                    }).map(|(sglist, _)| {
                        tx.send(Ok(sglist)).unwrap();
                    })
                }).map_err(Error::unhandled)
            ).unwrap();
        } else {
            self.handle.spawn(
                self.db.fsread(self.tree, move |ds| {
                    ds.get(inode_key)
                    .and_then(move |r| {
                        let value = r.expect("Inode not found");
                        let inode = value.as_inode()
                            .expect("Wrong Value type");
                        let fsize = inode.size;
                        let rs = inode.record_size() as u64;
                        Fs::do_read(ds, ino, fsize, rs, offset, size)
                    }).map(|sglist| {
                        tx.send(Ok(sglist)).unwrap();
                    })
                }).map_err(Error::unhandled)
            ).unwrap();
        }
        rx.wait().unwrap()
    }

    // TODO: instead of the full size struct libc::dirent, use a variable size
    // structure in the mpsc channel
    pub fn readdir(&self, fd: &FileData, soffs: i64)
        -> impl Iterator<Item=Result<(libc::dirent, i64), i32>>
    {
        type T = Result<(libc::dirent, i64), i32>;
        type LoopFut = Box<dyn Future<Item=mpsc::Sender<T>, Error=Error> + Send>;

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
        let ino = fd.ino;
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
                boxfut!(fut)
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

    pub fn readlink(&self, fd: &FileData) -> Result<OsString, i32> {
        let (tx, rx) = oneshot::channel();
        let ino = fd.ino;
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

    /// Rename a file.  Return the inode number of the renamed file.
    ///
    /// # Arguments
    ///
    /// - `parent_fd`:  `FileData` of the parent directory, as returned by
    ///                 [`lookup`].
    /// - `fd`:         `FileData` of the directory entry to be moved, if
    ///                 known.  Must be provided if the file has been looked up!
    /// - `name`:       Name of the directory entry to move.
    /// - `newparent`:  `FileData` of the new parent directory
    /// - `newfd`:      `FileData` of the target file (if it exists).  Must be
    ///                 provided if the target already exists!
    /// - `newname`:    New name for the file
    ///
    /// # Returns
    ///
    /// On success, the inode number of the moved file.
    ///
    /// NB: This function performs no dirloop protection.  That is the
    /// responsibility of the client.
    // XXX newino should really have type Option<&FileData> for consistency,
    // but I can't figure out how to make such a type work with Mockall.
    pub fn rename(&self, parent: &FileData, fd: &FileData, name: &OsStr,
        newparent: &FileData, newino: Option<u64>, newname: &OsStr)
        -> Result<u64, i32>
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
        let (tx, rx) = oneshot::channel::<Result<u64, i32>>();
        let src_objkey = ObjKey::dir_entry(&name);
        let owned_name = name.to_owned();
        let dst_objkey = ObjKey::dir_entry(&newname);
        let owned_newname = newname.to_owned();
        let owned_newname2 = owned_newname.clone();
        let owned_newname3 = owned_newname.clone();
        let ino = fd.ino();
        let dst_ino = newino;
        let parent_ino = parent.ino;
        let newparent_ino = newparent.ino;
        let samedir = parent_ino == newparent_ino;

        if name == OsStr::from_bytes(b".") || name == OsStr::from_bytes(b"..") {
            return Err(libc::EINVAL);
        }

        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                let ds = Arc::new(dataset);
                let ds4 = ds.clone();
                let ds5 = ds.clone();
                let ds6 = ds.clone();
                let dst_de_key = FSKey::new(newparent_ino, dst_objkey);
                // 0) Check conditions
                htable::get(&htable::ReadFilesystem::ReadWrite(ds.as_ref()),
                            dst_de_key, 0, owned_newname)
                .then(move |r: Result<Dirent, Error>| {
                    match r {
                        Ok(dirent) => {
                            assert_eq!(dst_ino.expect(
                                "didn't lookup destination before rename"),
                                dirent.ino);
                            if dirent.dtype != libc::DT_DIR {
                                // Overwriting non-directories is allowed
                                boxfut!(Ok(()).into_future())
                            } else {
                                // Is it a nonempty directory?
                                boxfut!(Fs::ok_to_rmdir(&ds4, dirent.ino,
                                    newparent_ino, owned_newname3))
                            }
                        },
                        Err(Error::ENOENT) => {
                            // Destination doesn't exist.  No problem!
                            assert!(dst_ino.is_none());
                            boxfut!(Ok(()).into_future())
                        },
                        Err(e) => {
                            // Other errors should propagate upwards
                            boxfut!(Err(e).into_future())
                        }
                    }
                }).and_then(move |_| {
                    // 1) Remove the source directory entry
                    let src_de_key = FSKey::new(parent_ino, src_objkey);
                    htable::remove::<Arc<ReadWriteFilesystem>, Dirent>
                        (ds5.clone(), src_de_key, 0, owned_name)
                }).and_then(move |mut dirent| {
                    assert_eq!(ino, dirent.ino);
                    // 2) Insert the new directory entry
                    let isdir = dirent.dtype == libc::DT_DIR;
                    dirent.name = owned_newname2.clone();
                    let ino = dirent.ino;
                    htable::insert(ds6, dst_de_key, dirent, owned_newname2)
                    .map(move |r| {
                        let old_dst_ino = r.map(|dirent| dirent.ino);
                        (ino, old_dst_ino, isdir)
                    })
                }).and_then(move |(ino, old_dst_ino, isdir)| {
                    // 3a) Decrement parent dir's link count
                    let ds2 = ds.clone();
                    let parent_ino_key = FSKey::new(parent_ino, ObjKey::Inode);
                    let p_nlink_fut = ds.get(parent_ino_key)
                    .and_then(move |r| {
                        let mut value = r.unwrap();
                        {
                            let inode = value.as_mut_inode().unwrap();
                            let now = time::get_time();
                            inode.mtime = now;
                            inode.ctime = now;
                            if isdir && (!samedir || old_dst_ino.is_some()) {
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
                        let newparent_ino_key = FSKey::new(newparent_ino,
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
                        boxfut!(fut)
                    } else {
                        boxfut!(Ok(()).into_future())
                    };
                    let unlink_fut = if let Some(v) = old_dst_ino {
                        // 3ci) Decrement old dst's link count
                        if isdir {
                            let fut = Fs::do_rmdir(ds, newparent_ino, v, false);
                            boxfut!(fut)
                        } else {
                            let fut = Fs::do_unlink(ds.clone(), 0, v)
                            .map(drop);
                            boxfut!(fut)
                        }
                    } else {
                        boxfut!(Ok(()).into_future())
                    };
                    unlink_fut.join3(p_nlink_fut, np_nlink_fut)
                    .map(move |_| ino)
                }).then(move |r| {
                    match r {
                        Ok(ino) => tx.send(Ok(ino)),
                        Err(e) => tx.send(Err(e.into()))
                    }.expect("FS::rename: send failed");
                    Ok(()).into_future()
                })
            }).map_err(Error::unhandled)
        ).unwrap();
        rx.wait().unwrap()
    }

    /// Remove a directory entry for a directory
    ///
    /// - `parent_fd`:  `FileData` of the parent directory, as returned by
    ///                 [`lookup`].
    /// - `name`:       Name of the directory entry to remove.
    // Note, unlink unlink, rmdir takes no Option<&FileData> argument, because
    // there is no need to support open-but-deleted directories.
    pub fn rmdir(&self, parent: &FileData, name: &OsStr) -> Result<(), i32> {
        // Outline:
        // 1) Lookup the directory
        // 2) Check that the directory is empty
        // 3) Remove its parent's directory entry
        // 4) Actually remove it
        let (tx, rx) = oneshot::channel();
        let parent_ino = parent.ino;
        let owned_name = name.to_os_string();
        let owned_name2 = owned_name.clone();
        let owned_name3 = owned_name.clone();
        let objkey = ObjKey::dir_entry(&owned_name);
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                let ds = Arc::new(dataset);
                let ds2 = ds.clone();
                // 1) Lookup the directory
                let key = FSKey::new(parent_ino, objkey);
                htable::get(&htable::ReadFilesystem::ReadWrite(&ds), key, 0,
                            owned_name3)
                .and_then(move |de: Dirent| {
                    let ino = de.ino;
                    // 2) Check that the directory is empty
                    Fs::ok_to_rmdir(&ds, ino, parent_ino, owned_name2)
                    .map(move |_| ino)
                }).and_then(move |ino| {
                    // 3) Remove the parent dir's dir_entry
                    let de_key = FSKey::new(parent_ino, objkey);
                    let dirent_fut = htable::remove::<Arc<ReadWriteFilesystem>,
                                                      Dirent>
                        (ds2.clone(), de_key, 0, owned_name);

                    // 4) Actually remove the directory
                    let dfut = Fs::do_rmdir(ds2, parent_ino, ino, true);

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

    /// Lookup the root directory
    pub fn root(&self) -> FileData {
        FileData{ ino: 1 , lookup_count: 1, parent: None}
    }

    pub fn setattr(&self, fd: &FileData, mut attr: SetAttr) -> Result<(), i32> {
        let (tx, rx) = oneshot::channel();
        let ino = fd.ino;
        self.handle.spawn(
            self.db.fswrite(self.tree, move |dataset| {
                let ds = Arc::new(dataset);
                if attr.ctime.is_none() {
                    attr.ctime = Some(time::get_time());
                }
                Fs::do_setattr(ds, ino, attr)
                .map(move |_| tx.send(Ok(())).unwrap())
            }).map_err(Error::unhandled)
        ).unwrap();
        rx.wait().unwrap()
    }

    pub fn setextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
                      name: &OsStr, data: &[u8]) -> Result<(), i32>
    {
        let (tx, rx) = oneshot::channel();
        let ino = fd.ino;
        let objkey = ObjKey::extattr(ns, &name);
        let key = FSKey::new(ino, objkey);
        // Data copy
        let buf = Arc::new(DivBufShared::from(&data[..]));
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

    /// Change filesystem properties
    pub fn set_props(&mut self, props: Vec<Property>) {
        for prop in props.iter() {
            match prop {
                Property::Atime(atime) => self.atime = *atime,
                Property::RecordSize(exp) => self.record_size = *exp
            }
        }

        // Update on-disk properties in the background
        let db2 = self.db.clone();
        let tree_id = self.tree;
        self.handle.spawn(future::lazy(move || {
            future::join_all(
                props.into_iter()
                .map(move |prop| {
                    db2.set_prop(tree_id, prop)
                }).collect::<Vec<_>>()
            ).map_err(Error::unhandled)
            .map(drop)
        })).unwrap();
    }

    pub fn statvfs(&self) -> Result<libc::statvfs, i32> {
        let (tx, rx) = oneshot::channel::<Result<libc::statvfs, i32>>();
        let rs = 1 << self.record_size;
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
                    f_bsize: rs,
                    f_flag: 0,
                    f_frsize: 4096,
                    f_fsid: 0,
                    f_namemax: 255,
                };
                Ok(r).into_future()
            }).map_err(Error::into)
            .then(|r| {
                tx.send(r).expect("Fs::statvfs: send failed");
                Ok(())
            })
        ).unwrap();
        rx.wait().unwrap()
    }

    /// Create a symlink from `name` to `link`.  Returns the link's inode on
    /// success, or an errno on failure.
    pub fn symlink(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                   gid: u32, link: &OsStr) -> Result<FileData, i32>
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
            .map_err(Error::unhandled)
            .map(|_| tx.send(()).unwrap())
        ).unwrap();
        rx.wait().unwrap()
    }

    /// Remove a directory entry for a non-directory
    ///
    /// - `parent_fd`:  `FileData` of the parent directory, as returned by
    ///                 [`lookup`].
    /// - `fd`:         `FileData` of the directory entry to be removed, if
    ///                 known.  Must be provided if the file has been looked up!
    /// - `name`:       Name of the directory entry to remove.
    pub fn unlink(&self, parent_fd: &FileData, fd: Option<&FileData>,
        name: &OsStr) -> Result<(), i32>
    {
        // Outline:
        // 1) Lookup and remove the directory entry
        // 2a) Unlink the Inode
        // 2b) Update parent's mtime and ctime
        let (tx, rx) = oneshot::channel();
        let ino = fd.map(|fd| fd.ino);
        let lookup_count = fd.map_or(0, |fd_| fd_.lookup_count);
        let parent_ino = parent_fd.ino;
        let owned_name = name.to_os_string();
        let dekey = ObjKey::dir_entry(&owned_name);
        self.handle.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                // 1) Lookup and remove the directory entry
                let key = FSKey::new(parent_ino, dekey);
                htable::remove::<Arc<ReadWriteFilesystem>, Dirent>
                    (dataset.clone(), key, 0, owned_name)
                .and_then(move |dirent|  {
                    if let Some(ino) = ino {
                        assert_eq!(ino, dirent.ino);
                    }
                    // 2a) Unlink the inode
                    let unlink_fut = Fs::do_unlink(dataset.clone(),
                        lookup_count, dirent.ino);
                    // 2b) Update parent's timestamps
                    let now = time::get_time();
                    let mut attr = SetAttr::default();
                    attr.ctime = Some(now);
                    attr.mtime = Some(now);
                    let ts_fut = Fs::do_setattr(dataset, parent_ino, attr);
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

    pub fn write<IU>(&self, fd: &FileData, offset: u64, data: IU, _flags: u32)
        -> Result<u32, i32>
        where IU: Into<Uio>
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
        let ino = fd.ino;
        let uio = data.into();

        self.handle.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                let inode_key = FSKey::new(ino, ObjKey::Inode);
                dataset.get(inode_key)
                .and_then(move |r| {
                    let mut value = r.unwrap();
                    let inode = value.as_inode().unwrap();
                    let filesize = inode.size;
                    let rs = inode.record_size() as u64;

                    // Moving uio into the asynchronous domain is safe because
                    // the async domain blocks on rx.wait().
                    let datalen = uio.len();
                    let offset0 = (offset % rs) as usize;
                    let sglist = unsafe {
                        uio.into_chunks(offset0, inode.record_size(),
                            |chunk| Arc::new(DivBufShared::from(chunk)))
                    };

                    let data_futs = sglist.into_iter()
                        .enumerate()
                        .map(|(i, dbs)| {
                        let ds3 = dataset.clone();
                        Fs::write_record(ino, rs, offset, i, dbs, ds3)
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
                    .map(move |_| tx.send(Ok(datalen as u32)).unwrap())
                })
            }).map_err(Error::unhandled)
        ).unwrap();
        rx.wait().unwrap()
    }

    // Subroutine of write
    #[inline]
    fn write_record(ino: u64, rs: u64, offset: u64, i: usize,
                    data: Arc<DivBufShared>, dataset: Arc<ReadWriteFilesystem>)
        -> Box<dyn Future<Item=Option<FSValue<RID>>, Error=Error> + Send>
    {
        let baseoffset = offset - (offset % rs);
        let offs = baseoffset + i as u64 * rs;
        let offset_into_rec = if i == 0 {
            (offset - baseoffset) as usize
        } else {
            0
        };
        let k = FSKey::new(ino, ObjKey::Extent(offs));
        let writelen = data.len();
        if (writelen as u64) < rs {
            // We must read-modify-write
            let dataset4 = dataset.clone();
            let fut = dataset.remove(k)
            .and_then(move |r| {
                match r {
                    None => {
                        // Either a hole, or beyond EOF
                        let hsize = writelen + offset_into_rec;
                        let r = Arc::new(DivBufShared::uninitialized(hsize));
                        if offset_into_rec > 0 {
                            let zrange = 0..offset_into_rec;
                            for x in &mut r.try_mut().unwrap()[zrange] {
                                *x = 0;
                            }
                        }
                        boxfut!(Ok(r).into_future())
                    },
                    Some(FSValue::InlineExtent(ile)) => {
                        boxfut!(Ok(ile.buf).into_future())
                    },
                    Some(FSValue::BlobExtent(be)) => {
                        let fut = dataset4.remove_blob(be.rid)
                            .map(Arc::from);
                        boxfut!(fut)
                    },
                    x => panic!("Unexpected value {:?} for key {:?}",
                                x, k)
                }.and_then(move |dbs: Arc<DivBufShared>| {
                    let mut base = dbs.try_mut().unwrap();
                    let overlay = data.try_const().unwrap();
                    let l = overlay.len();
                    let r = offset_into_rec..(offset_into_rec + l);

                    // Extend the buffer, if necessary
                    let newsize = cmp::max(offset_into_rec + l, base.len());
                    base.try_resize(newsize, 0).unwrap();

                    // Overwrite with new data
                    base[r].copy_from_slice(&overlay[..]);
                    let extent = InlineExtent::new(dbs);
                    let new_v = FSValue::InlineExtent(extent);
                    dataset.insert(k, new_v)
                })
            });
            boxfut!(fut)
        } else {
            let v = FSValue::InlineExtent(InlineExtent::new(data));
            boxfut!(dataset.insert(k, v))
        }
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {

use super::*;
use crate::common::{
    dataset::RangeQuery,
    tree::{Key, Value}
};
use futures::Async;
use mockall::{Sequence, predicate::*};
use pretty_assertions::assert_eq;
use std::borrow::Borrow;

fn setup() -> (tokio_io_pool::Runtime, Database, TreeID) {
    let mut rt = tokio_io_pool::Builder::default()
        .pool_size(1)
        .build()
        .unwrap();
    let mut rods = ReadOnlyFilesystem::default();
    let mut rwds = ReadWriteFilesystem::default();
    rods.expect_last_key()
        .once()
        .returning(|| {
            let root_inode_key = FSKey::new(1, ObjKey::Inode);
            Box::new(Ok(Some(root_inode_key)).into_future())
        });
    rwds.expect_range()
        .once()
        .with(eq(FSKey::dying_inode_range()))
        .returning(move |_| {
            mock_range_query(Vec::new())
        });
    rwds.expect_range_delete()
        .once()
        .with(eq(FSKey::dying_inode_range()))
        .returning(|_| {
            Box::new(Ok(()).into_future())
        });
    let mut db = Database::default();
    db.expect_new_fs()
        .once()
        .returning(|_| Box::new(Ok(TreeID::Fs(0)).into_future()));
    db.expect_fsread_inner()
        .once()
        .return_once(move |_| rods);
    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| rwds);
    db.expect_get_prop()
        .times(2)
        .returning(|_tree_id, propname| {
            let prop = Property::default_value(propname);
            let source = PropertySource::Default;
            Box::new(Ok((prop, source)).into_future())
        });
    let tree_id = rt.block_on(db.new_fs(Vec::new())).unwrap();
    (rt, db, tree_id)
}

/// Helper that creates a mock RangeQuery from the vec of items that it should
/// return
fn mock_range_query<K, T, V>(items: Vec<(K, V)>) -> RangeQuery<K, T, V>
    where K: Key + Borrow<T>,
          T: Ord + Clone + Send,
          V: Value
{
    let mut rq = RangeQuery::new();
    let mut seq = Sequence::new();
    for item in items.into_iter() {
        rq.expect_poll()
            .once()
            .in_sequence(&mut seq)
            .return_once(|| Ok(Async::Ready(Some(item))));
    }
    rq.expect_poll()
        .once()
        .in_sequence(&mut seq)
        .return_once(|| Ok(Async::Ready(None)));
    rq
}

#[test]
fn create() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::default();
    let root_ino = 1;
    let ino = 2;
    let filename = OsString::from("x");
    let filename2 = filename.clone();
    let old_ts = time::Timespec::new(0, 0);
    ds.expect_get()
        .once()
        .with(eq(FSKey::new(root_ino, ObjKey::Inode)))
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
    ds.expect_insert()
        .once()
        .withf(|key, value| {
            key.is_inode() &&
            value.as_inode().unwrap().size == 0 &&
            value.as_inode().unwrap().nlink == 1 &&
            value.as_inode().unwrap().file_type == FileType::Reg(12) &&
            value.as_inode().unwrap().perm == 0o644 &&
            value.as_inode().unwrap().uid == 123 &&
            value.as_inode().unwrap().gid == 456
        }).returning(|_, _| Box::new(Ok(None).into_future()));
    ds.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_direntry() &&
            value.as_direntry().unwrap().dtype == libc::DT_REG &&
            value.as_direntry().unwrap().name == filename2 &&
            value.as_direntry().unwrap().ino == ino
        }).returning(|_, _| Box::new(Ok(None).into_future()));
    ds.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_inode() &&
            value.as_inode().unwrap().file_type == FileType::Dir &&
            value.as_inode().unwrap().atime == old_ts &&
            value.as_inode().unwrap().mtime != old_ts &&
            value.as_inode().unwrap().ctime != old_ts &&
            value.as_inode().unwrap().birthtime == old_ts
        }).returning(|_, _| Box::new(Ok(None).into_future()));
    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);

    let fd = fs.create(&fs.root(), &filename, 0o644, 123, 456).unwrap();
    assert_eq!(ino, fd.ino);
}

/// Create experiences a hash collision when adding the new directory entry
#[test]
fn create_hash_collision() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::default();
    let root_ino = 1;
    let ino = 2;
    let other_ino = 100;
    let filename = OsString::from("x");
    let filename2 = filename.clone();
    let filename3 = filename.clone();
    let filename4 = filename.clone();
    let other_filename = OsString::from("y");
    let other_filename2 = other_filename.clone();
    let old_ts = time::Timespec::new(0, 0);
    ds.expect_get()
        .once()
        .with(eq(FSKey::new(root_ino, ObjKey::Inode)))
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
    ds.expect_insert()
        .once()
        .withf(|key, _value| {
            key.is_inode()
        }).returning(|_, _| Box::new(Ok(None).into_future()));
    ds.expect_insert()
        .once()
        .withf(|key, _value| {
            key.is_direntry()
        }).returning(move |_, _| {
            // Return a different directory entry
            let name = other_filename2.clone();
            let dirent = Dirent{ino: other_ino, dtype: libc::DT_REG, name};
            let v = FSValue::DirEntry(dirent);
            Box::new(Ok(Some(v)).into_future())
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
            Box::new(Ok(Some(v)).into_future())
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
            Box::new(Ok(Some(v)).into_future())
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
        }).returning(|_, _| Box::new(Ok(None).into_future()));

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);

    let fd = fs.create(&fs.root(), &filename, 0o644, 123, 456).unwrap();
    assert_eq!(ino, fd.ino);
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
fn eq_getattr() {
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
#[test]
fn deleteextattr_3way_collision() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::default();
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
        boxfut!(Ok(Some(v)).into_future())
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
        Box::new(Ok(None).into_future())
    });

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let fd = FileData::new(Some(1), ino);
    let r = fs.deleteextattr(&fd, namespace, &name2);
    assert_eq!(Ok(()), r);
}

/// A 3-way hash collision of extended attributes.  Two are stored in one key,
/// and deleteextattr tries to delete a third that hashes to the same key, but
/// isn't stored there.
#[test]
fn deleteextattr_3way_collision_enoattr() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::default();
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
        boxfut!(Ok(Some(v)).into_future())
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
        Box::new(Ok(None).into_future())
    });

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let fd = FileData::new(Some(1), ino);
    let r = fs.deleteextattr(&fd, namespace, &name2);
    assert_eq!(Err(libc::ENOATTR), r);
}

#[test]
fn fsync() {
    let ino = 42;

    let (rt, mut db, tree_id) = setup();
    db.expect_sync_transaction()
        .once()
        .returning(|| Box::new(Ok(()).into_future()));
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);

    let fd = FileData::new(Some(1), ino);
    assert!(fs.fsync(&fd).is_ok());
}

/// Reading the source returns EIO.  Don't delete the dest
#[test]
fn rename_eio() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::default();
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
            Box::new(Ok(Some(v)).into_future())
        });
    ds.expect_remove()
        .once()
        .with(eq(FSKey::new(1, ObjKey::dir_entry(&srcname))))
        .returning(move |_| {
            Box::new(Err(Error::EIO).into_future())
        });

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);

    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let root = fs.root();
    let fd = FileData::new_for_tests(Some(root.ino), src_ino);
    let r = fs.rename(&root, &fd, &srcname, &root, Some(dst_ino), &dstname);
    assert_eq!(Err(libc::EIO), r);
}

// Rmdir a directory with extended attributes, and don't forget to free them
// too!
#[test]
fn rmdir_with_blob_extattr() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::default();
    let parent_ino = 1;
    let ino = 2;
    let xattr_blob_rid = RID(88888);
    let filename = OsString::from("x");
    let filename2 = filename.clone();
    let filename3 = filename.clone();

    // First it must do a lookup
    ds.expect_get()
        .once()
        .with(eq(FSKey::new(parent_ino, ObjKey::dir_entry(&filename))))
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
    ds.expect_range()
        .once()
        .with(eq(FSKey::obj_range(ino)))
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
            mock_range_query(items)
        });
    ds.expect_remove()
        .once()
        .with(eq(FSKey::new(parent_ino, ObjKey::dir_entry(&filename))))
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
        .once()
        .with(eq(FSKey::obj_range(ino)))
        .returning(|_| {
            Box::new(Ok(()).into_future())
        });
    ds.expect_range()
        .once()
        .with(eq(FSKey::extattr_range(ino)))
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
            mock_range_query(extents)
        });
    ds.expect_delete_blob()
        .once()
        .withf(move |rid: &RID| xattr_blob_rid == *rid)
        .returning(|_| {
            Box::new(Ok(()).into_future())
         });
    ds.expect_get()
        .once()
        .with(eq(FSKey::new(parent_ino, ObjKey::Inode)))
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
        .once()
        .withf(move |key, value| {
            *key == FSKey::new(parent_ino, ObjKey::Inode) &&
            value.as_inode().unwrap().nlink == 2
        }).returning(|_, _| {
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

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.rmdir(&fs.root(), &filename);
    assert_eq!(Ok(()), r);
}

/// Basic setextattr test, that does not rely on any other extattr
/// functionality.
#[test]
fn setextattr() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::default();
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
    }).returning(|_, _| Box::new(Ok(None).into_future()));

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.setextattr(&fs.root(), namespace, &name, value.as_bytes());
    assert_eq!(Ok(()), r);
}

/// setextattr with a 3-way hash collision.  It's hard to programmatically
/// generate 3-way hash collisions, so we simulate them using mocks
#[test]
fn setextattr_3way_collision() {
    let (rt, mut db, tree_id) = setup();
    let mut ds = ReadWriteFilesystem::default();
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
        boxfut!(Ok(Some(v)).into_future())
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
        boxfut!(Ok(Some(v)).into_future())
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
        boxfut!(Ok(Some(v)).into_future())
    });

    db.expect_fswrite_inner()
        .once()
        .return_once(move |_| ds);
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.setextattr(&fs.root(), namespace, &name2, value2.as_bytes());
    assert_eq!(Ok(()), r);
}

#[test]
fn set_props() {
    let (rt, mut db, tree_id) = setup();
    db.expect_set_prop()
        .once()
        .with(eq(tree_id), eq(Property::Atime(false)))
        .returning(|_, _| boxfut!(Ok(()).into_future()));
    db.expect_set_prop()
        .once()
        .with(eq(tree_id), eq(Property::RecordSize(13)))
        .returning(|_, _| boxfut!(Ok(()).into_future()));
    let mut fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    fs.set_props(vec![Property::Atime(false), Property::RecordSize(13)]);
    rt.shutdown_on_idle();
}

#[test]
fn sync() {
    let (rt, mut db, tree_id) = setup();
    db.expect_sync_transaction()
        .once()
        .returning(|| Box::new(Ok(()).into_future()));
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);

    fs.sync();
}

// Verify that storage is freed when unlinking a normal file.
#[test]
fn unlink() {
    let (rt, mut db, tree_id) = setup();
    let mut ds0 = ReadWriteFilesystem::default();
    let mut ds1 = ReadWriteFilesystem::default();
    let parent_ino = 1;
    let ino = 2;
    let blob_rid = RID(99999);
    let filename = OsString::from("x");
    let filename2 = filename.clone();
    let old_ts = time::Timespec::new(0, 0);
    let mut seq = Sequence::new();

    ds0.expect_remove()
        .once()
        .with(eq(FSKey::new(parent_ino, ObjKey::dir_entry(&filename))))
        .returning(move |_| {
            let dirent = Dirent {
                ino,
                dtype: libc::DT_REG,
                name: filename2.clone()
            };
            let v = Some(FSValue::DirEntry(dirent));
            Box::new(Ok(v).into_future())
        });
    ds0.expect_get()
        .once()
        .with(eq(FSKey::new(ino, ObjKey::Inode)))
        .returning(move |_| {
            let inode = Inode {
                size: 4098,
                nlink: 1,
                flags: 0,
                atime: old_ts,
                mtime: old_ts,
                ctime: old_ts,
                birthtime: old_ts,
                uid: 0,
                gid: 0,
                file_type: FileType::Reg(12),
                perm: 0o644,
            };
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });

    ds0.expect_get()
        .once()
        .with(eq(FSKey::new(1, ObjKey::Inode)))
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
    ds0.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_inode() &&
            key.object() == ino &&
            value.as_inode().unwrap().nlink == 0 &&
            value.as_inode().unwrap().ctime != old_ts
        }).returning(|_, _| Box::new(Ok(None).into_future()));
    ds0.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_inode() &&
            value.as_inode().unwrap().file_type == FileType::Dir &&
            value.as_inode().unwrap().atime == old_ts &&
            value.as_inode().unwrap().mtime != old_ts &&
            value.as_inode().unwrap().ctime != old_ts &&
            value.as_inode().unwrap().birthtime == old_ts
        }).returning(|_, _| Box::new(Ok(None).into_future()));
    ds0.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_dying_inode() &&
            value.as_dying_inode().unwrap().ino() == ino
        }).returning(|_, _| Box::new(Ok(None).into_future()));

    ds1.expect_get()
        .once()
        .withf(move |key| key.is_dying_inode())
        .returning(move |_| {
            let v = FSValue::DyingInode(DyingInode::from(ino));
            Box::new(Ok(Some(v)).into_future())
        });
    ds1.expect_range()
        .once()
        .with(eq(FSKey::extent_range(ino, ..)))
        .returning(move |_| {
            // Return one blob extent and one embedded extent
            let k0 = FSKey::new(ino, ObjKey::Extent(0));
            let be0 = BlobExtent{lsize: 4096, rid: blob_rid};
            let v0 = FSValue::BlobExtent(be0);
            let k1 = FSKey::new(ino, ObjKey::Extent(4096));
            let dbs0 = Arc::new(DivBufShared::from(vec![0u8; 1]));
            let v1 = FSValue::InlineExtent(InlineExtent::new(dbs0));
            let extents = vec![(k0, v0), (k1, v1)];
            mock_range_query(extents)
        });
    ds1.expect_range()
        .once()
        .with(eq(FSKey::extattr_range(ino)))
        .returning(move |_| {
            mock_range_query(Vec::new())
        });
    ds1.expect_delete_blob()
        .once()
        .withf(move |rid: &RID| blob_rid == *rid)
        .returning(|_| Box::new(Ok(()).into_future()));
    ds1.expect_range_delete()
        .once()
        .with(eq(FSKey::obj_range(ino)))
        .returning(|_| Box::new(Ok(()).into_future()));

    db.expect_fswrite_inner()
        .once()
        .in_sequence(&mut seq)
        .return_once(move |_| ds0);
    db.expect_fswrite_inner()
        .once()
        .in_sequence(&mut seq)
        .return_once(move |_| ds1);
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let fd = FileData::new(Some(parent_ino), ino);
    let r = fs.unlink(&fs.root(), Some(&fd), &filename);
    assert_eq!(Ok(()), r);
    fs.inactive(fd);
}

// Unlink a file with extended attributes, and don't forget to free them too!
#[test]
fn unlink_with_blob_extattr() {
    let (rt, mut db, tree_id) = setup();
    let mut ds0 = ReadWriteFilesystem::default();
    let mut ds1 = ReadWriteFilesystem::default();
    let parent_ino = 1;
    let ino = 2;
    let blob_rid = RID(99999);
    let xattr_blob_rid = RID(88888);
    let filename = OsString::from("x");
    let filename2 = filename.clone();
    let mut seq = Sequence::new();

    ds0.expect_remove()
        .once()
        .with(eq(FSKey::new(parent_ino, ObjKey::dir_entry(&filename))))
        .returning(move |_| {
            let dirent = Dirent {
                ino,
                dtype: libc::DT_REG,
                name: filename2.clone()
            };
            let v = Some(FSValue::DirEntry(dirent));
            Box::new(Ok(v).into_future())
        });
    ds0.expect_get()
        .once()
        .with(eq(FSKey::new(ino, ObjKey::Inode)))
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
                file_type: FileType::Reg(12),
                perm: 0o644,
            };
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });
    let old_ts = time::Timespec::new(0, 0);
    ds0.expect_get()
        .once()
        .with(eq(FSKey::new(1, ObjKey::Inode)))
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
    ds0.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_dying_inode() &&
            value.as_dying_inode().unwrap().ino() == ino
        }).returning(|_, _| Box::new(Ok(None).into_future()));

    ds1.expect_get()
        .once()
        .withf(move |key| key.is_dying_inode())
        .returning(move |_| {
            let v = FSValue::DyingInode(DyingInode::from(ino));
            Box::new(Ok(Some(v)).into_future())
        });
    ds1.expect_range()
        .once()
        .with(eq(FSKey::extent_range(ino, ..)))
        .returning(move |_| {
            // Return one blob extent and one embedded extent
            let k0 = FSKey::new(ino, ObjKey::Extent(0));
            let be0 = BlobExtent{lsize: 4096, rid: blob_rid};
            let v0 = FSValue::BlobExtent(be0);
            let k1 = FSKey::new(ino, ObjKey::Extent(4096));
            let dbs0 = Arc::new(DivBufShared::from(vec![0u8; 1]));
            let v1 = FSValue::InlineExtent(InlineExtent::new(dbs0));
            let extents = vec![(k0, v0), (k1, v1)];
            mock_range_query(extents)
        });
    ds1.expect_range()
        .once()
        .with(eq(FSKey::extattr_range(ino)))
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
            mock_range_query(extents)
        });
    ds1.expect_delete_blob()
        .once()
        .withf(move |rid: &RID| blob_rid == *rid)
        .returning(|_| Box::new(Ok(()).into_future()));
    ds1.expect_delete_blob()
        .once()
        .withf(move |rid: &RID| xattr_blob_rid == *rid)
        .returning(|_| Box::new(Ok(()).into_future()));
    ds1.expect_range_delete()
        .once()
        .with(eq(FSKey::obj_range(ino)))
        .returning(|_| Box::new(Ok(()).into_future()));
    ds0.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_inode() &&
            key.object() == ino &&
            value.as_inode().unwrap().nlink == 0 &&
            value.as_inode().unwrap().ctime != old_ts
        }).returning(|_, _| Box::new(Ok(None).into_future()));
    ds0.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_inode() &&
            value.as_inode().unwrap().file_type == FileType::Dir &&
            value.as_inode().unwrap().atime == old_ts &&
            value.as_inode().unwrap().mtime != old_ts &&
            value.as_inode().unwrap().ctime != old_ts &&
            value.as_inode().unwrap().birthtime == old_ts
        }).returning(|_, _| Box::new(Ok(None).into_future()));

    db.expect_fswrite_inner()
        .once()
        .in_sequence(&mut seq)
        .return_once(move |_| ds0);
    db.expect_fswrite_inner()
        .once()
        .in_sequence(&mut seq)
        .return_once(move |_| ds1);
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let fd = FileData::new(Some(parent_ino), ino);
    let r = fs.unlink(&fs.root(), Some(&fd), &filename);
    assert_eq!(Ok(()), r);
    fs.inactive(fd);
}

// Unlink a file with two extended attributes that hashed to the same bucket.
// One is a blob, and must be freed
#[test]
fn unlink_with_extattr_hash_collision() {
    let (rt, mut db, tree_id) = setup();
    let mut ds0 = ReadWriteFilesystem::default();
    let mut ds1 = ReadWriteFilesystem::default();
    let parent_ino = 1;
    let ino = 2;
    let xattr_blob_rid = RID(88888);
    let filename = OsString::from("x");
    let filename2 = filename.clone();
    let mut seq = Sequence::new();

    ds0.expect_remove()
        .once()
        .with(eq(FSKey::new(parent_ino, ObjKey::dir_entry(&filename))))
        .returning(move |_| {
            let dirent = Dirent {
                ino,
                dtype: libc::DT_REG,
                name: filename2.clone()
            };
            let v = Some(FSValue::DirEntry(dirent));
            Box::new(Ok(v).into_future())
        });
    ds0.expect_get()
        .once()
        .with(eq(FSKey::new(ino, ObjKey::Inode)))
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
                file_type: FileType::Reg(12),
                perm: 0o644,
            };
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });
    let old_ts = time::Timespec::new(0, 0);
    ds0.expect_get()
        .once()
        .with(eq(FSKey::new(1, ObjKey::Inode)))
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
    ds0.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_inode() &&
            value.as_inode().unwrap().file_type == FileType::Dir &&
            value.as_inode().unwrap().atime == old_ts &&
            value.as_inode().unwrap().mtime != old_ts &&
            value.as_inode().unwrap().ctime != old_ts &&
            value.as_inode().unwrap().birthtime == old_ts
        }).returning(|_, _| Box::new(Ok(None).into_future()));
    ds0.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_dying_inode() &&
            value.as_dying_inode().unwrap().ino() == ino
        }).returning(|_, _| Box::new(Ok(None).into_future()));

    ds1.expect_get()
        .once()
        .withf(move |key| key.is_dying_inode())
        .returning(move |_| {
            let v = FSValue::DyingInode(DyingInode::from(ino));
            Box::new(Ok(Some(v)).into_future())
        });
    ds1.expect_range()
        .once()
        .with(eq(FSKey::extent_range(ino, ..)))
        .returning(move |_| {
            // The file is empty
            let extents = vec![];
            mock_range_query(extents)
        });
    ds1.expect_range()
        .once()
        .with(eq(FSKey::extattr_range(ino)))
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
            mock_range_query(extents)
        });
    ds1.expect_delete_blob()
        .once()
        .withf(move |rid: &RID| xattr_blob_rid == *rid)
        .returning(|_| Box::new(Ok(()).into_future()));
    ds1.expect_range_delete()
        .once()
        .with(eq(FSKey::obj_range(ino)))
        .returning(|_| Box::new(Ok(()).into_future()));
    ds0.expect_insert()
        .once()
        .withf(move |key, value| {
            key.is_inode() &&
            key.object() == ino &&
            value.as_inode().unwrap().nlink == 0 &&
            value.as_inode().unwrap().ctime != old_ts
        }).returning(|_, _| Box::new(Ok(None).into_future()));

    db.expect_fswrite_inner()
        .once()
        .in_sequence(&mut seq)
        .return_once(move |_| ds0);
    db.expect_fswrite_inner()
        .once()
        .in_sequence(&mut seq)
        .return_once(move |_| ds1);
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let fd = FileData::new(Some(parent_ino), ino);
    let r = fs.unlink(&fs.root(), Some(&fd), &filename);
    assert_eq!(Ok(()), r);
    fs.inactive(fd);
}
}
