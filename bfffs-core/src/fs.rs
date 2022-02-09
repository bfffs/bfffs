// vim: tw=80
//! Common VFS implementation

use bitfield::*;
use crate::{
    database::*,
    dataset::{RangeQuery, ReadDataset},
    fs_tree::*,
    property::*,
    types::*,
    util::*
};
use divbuf::{DivBufShared, DivBuf};
use futures::{
    Future,
    FutureExt,
    Stream,
    StreamExt,
    TryFutureExt,
    TryStreamExt,
    future,
    stream::{self, FuturesUnordered},
    task::{Context, Poll}
};
use std::{
    cmp,
    ffi::{OsStr, OsString},
    fmt::Debug,
    io,
    mem,
    os::unix::ffi::OsStrExt,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    }
};

#[cfg(test)] mod tests;

pub type ExtAttr = crate::fs_tree::ExtAttr<RID>;
pub type ExtAttrNamespace = crate::fs_tree::ExtAttrNamespace;
pub type Timespec = crate::fs_tree::Timespec;

/// Operations used for data that is stored in in-BTree hash tables
mod htable {
    use crate::{
        dataset::ReadDataset,
        fs_tree::{FSKey, FSValue, HTItem, HTValue},
        types::*
    };
    use futures::{Future, FutureExt, TryFutureExt, future};
    use std::{
        ffi::OsString,
        mem,
        pin::Pin
    };
    use super::{ReadOnlyFilesystem, ReadWriteFilesystem};

    // Just makes use_graph.sh look better.
    #[allow(dead_code)]
    pub(super) type Dummy = ();

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
            -> Pin<Box<dyn Future<Output=Result<Option<FSValue<RID>>>> + Send>>
        {
            match self {
                ReadFilesystem::ReadOnly(ds) => Box::pin(ds.get(k)),
                ReadFilesystem::ReadWrite(ds) => Box::pin(ds.get(k))
            }
        }
    }

    /// Get an item from an in-BTree hash table
    pub(super) fn get<T>(dataset: &ReadFilesystem, key: FSKey,
                         aux: T::Aux, name: OsString)
        -> impl Future<Output=Result<T>> + Send
        where T: HTItem
    {
        dataset.get(key)
        .map(move |r| {
            match r {
                Ok(fsvalue) => {
                    match T::from_table(fsvalue) {
                        HTValue::Single(old) => {
                            if old.same(aux, &name) {
                                // Found the right item
                                Ok(old)
                            } else {
                                // Hash collision
                                Err(T::ENOTFOUND)
                            }
                        },
                        HTValue::Bucket(old) => {
                            assert!(old.len() > 1);
                            if let Some(v) = old.into_iter().find(|x| {
                                x.same(aux, &name)
                            }) {
                                // Found the right one
                                Ok(v)
                            } else {
                                // A 3 (or more) way hash collision.  The
                                // item we're looking up isn't found.
                                Err(T::ENOTFOUND)
                            }
                        },
                        HTValue::None => {
                            Err(T::ENOTFOUND)
                        },
                        HTValue::Other(x) =>
                            panic!("Unexpected value {:?} for key {:?}", x, key)
                    }
                },
                Err(e) => Err(e)
            }
        })
    }

    /// Insert an item that is stored in a BTree hash table
    pub(super) fn insert<D, T>(
        dataset: D,
        key: FSKey,
        value: T,
        name: OsString,
    ) -> impl Future<Output=Result<Option<T>>> + Send
        where D: AsRef<ReadWriteFilesystem> + Send + 'static,
              T: HTItem
    {
        let aux = value.aux();
        let fsvalue = value.into_fsvalue();
        dataset.as_ref().insert(key, fsvalue)
        .and_then(move |r| {
            match T::from_table(r) {
                HTValue::Single(old) => {
                    if old.same(aux, &name) {
                        // We're overwriting an existing item
                        future::ok::<Option<T>, Error>(Some(old)).boxed()
                    } else {
                        // We had a hash collision setting an unrelated
                        // item. Get the old value back, and pack them together.
                        dataset.as_ref().get(key)
                        .and_then(move |r| {
                            let v = r.unwrap();
                            let new = T::try_from(v).unwrap();
                            let values = vec![old, new];
                            let fsvalue = T::into_bucket(values);
                            dataset.as_ref()
                            .insert(key, fsvalue)
                            .map_ok(|_| None)
                        }).boxed()
                    }
                },
                HTValue::Bucket(mut old) => {
                    // There was previously a hash collision.  Get the new value
                    // back, then pack them together.
                    dataset.as_ref().get(key)
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
                        .map_ok(|_| r)
                    }).boxed()
                },
                HTValue::Other(x) => {
                    panic!("Unexpected value {:?} for key {:?}", x, key)
                },
                HTValue::None => {
                    future::ok::<Option<T>, Error>(None).boxed()
                }
            }
        })
    }

    /// Remove an item that is stored in a BTree hash table
    ///
    /// Return the old item, if any.
    pub(super) fn remove<D, T>(
        dataset: D,
        key: FSKey,
        aux: T::Aux,
        name: OsString,
    ) -> impl Future<Output=Result<T>> + Send
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
                        future::ok(old).boxed()
                    } else {
                        // Hash collision.  Put it back, and return not found
                        let value = old.into_fsvalue();
                        let fut = dataset.as_ref()
                        .insert(key, value)
                        .and_then(|_| future::err(T::ENOTFOUND));
                        fut.boxed()
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
                        let fut = dataset.as_ref()
                        .insert(key, v)
                        .map_ok(move |_| r);
                        fut.boxed()
                    } else {
                        // A 3 (or more) way hash collision between the
                        // accessed item and at least two different ones.
                        let v = T::into_bucket(old);
                        let fut = dataset.as_ref()
                        .insert(key, v)
                        .and_then(|_| future::err(T::ENOTFOUND));
                        fut.boxed()
                    }
                },
                HTValue::None => {
                    future::err(T::ENOTFOUND).boxed()
                },
                HTValue::Other(x) =>
                    panic!("Unexpected value {:?} for key {:?}", x, key)
            }
        })
    }
}

// Just to make use_graph.sh's output look better.  Logically fs uses
// fs::htable.  But it doesn't have any use statements except for this one.
#[allow(unused_imports)]
use htable::Dummy;

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
    /// Perhaps with async/await, bfffs::fs::Fs::write could be made
    /// safe with non-'static references.
    unsafe fn into_chunks<F, T>(self, offset0: usize, rs: usize, f: F) -> Vec<T>
        where F: Fn(Vec<u8>) -> T
    {
        let nrecs = self.nrecs(offset0, rs);
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

    /// Across how many records will this UIO be spread?
    fn nrecs(&self, offset0: usize, rs: usize) -> usize {
        (div_roundup(offset0 + self.len(), rs) - (offset0 / rs)) as usize
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
#[derive(Clone, Copy, Debug)]
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
    //cr: CreditRequirements,
    db: Arc<Database>,
    next_object: AtomicU64,
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
    /// File size in bytes
    pub bytes:      u64,
    /// access time
    pub atime:      Timespec,
    /// modification time
    pub mtime:      Timespec,
    /// change time
    pub ctime:      Timespec,
    /// birth time
    pub birthtime:  Timespec,
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
    /// Optimal I/O block size
    pub blksize:    u32,
    /// File flags
    pub flags:      u64,
}

/// File attributes, as set by `setattr`
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SetAttr {
    /// File size in bytes
    pub size:       Option<u64>,
    /// access time
    pub atime:      Option<Timespec>,
    /// modification time
    pub mtime:      Option<Timespec>,
    /// change time
    pub ctime:      Option<Timespec>,
    /// birth time
    pub birthtime:  Option<Timespec>,
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
        -> Pin<Box<dyn Future<Output=Result<()>> + Send + 'static>>;

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
    cb: CreateCallback,
    /// Credit needed by [`cb`], in multiples of the dataset's insert,
    /// range_delete, remove CreditRequirements.
    cb_credit: (usize, usize, usize),
}

impl<'a> CreateArgs<'a> {
    const DEFAULT_CB: CreateCallback = Fs::create_ts_callback;

    pub fn callback(
        mut self, f: CreateCallback,
        insert_cr: usize,
        range_delete_cr: usize,
        remove_cr: usize,
    ) -> Self
    {
        self.cb = f;
        self.cb_credit = (insert_cr, range_delete_cr, remove_cr);
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
        CreateArgs{
            parent,
            flags: 0,
            name: name.to_owned(),
            perm,
            file_type,
            uid,
            gid,
            nlink: 1,
            cb,
            cb_credit: (0, 0, 0),
        }
    }

    pub fn nlink(mut self, nlink: u64) -> Self {
        self.nlink = nlink;
        self
    }
}

/// For use with [`Fs::lseek`]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeekWhence {
    /// Find the next hole
    Hole = libc::SEEK_HOLE as isize,
    /// Find the next data region
    Data = libc::SEEK_DATA as isize,
}

impl Fs {
    /// Deallocate space.  The deallocated region may no longer take up space
    /// on disk, and will return zeros if read.
    pub async fn deallocate(&self, fd: &FileData, mut offset: u64, mut len: u64)
            -> std::result::Result<(), i32>
    {
        let ino = fd.ino;
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        self.db.fswrite(self.tree, 3, 0, 2, 1,
        move |dataset| async move {
            let ds = Arc::new(dataset);
            let mut inode_value = ds.get(inode_key).await?.unwrap();
            let mut inode = inode_value.as_mut_inode().unwrap();
            let rs = inode.record_size().unwrap() as u64;
            let filesize = inode.size;
            offset = filesize.min(offset);
            len = (filesize.saturating_sub(offset)).min(len);
            if len > 0 {
                let freed = Fs::do_deallocate(ds.clone(), ino, offset,
                    Some(len), rs).await?;
                let now = Timespec::now();
                inode.bytes = inode.bytes.saturating_sub(freed);
                inode.mtime = now;
                inode.ctime = now;
                ds.insert(inode_key, inode_value).await
                .map(drop)
            } else {
                Ok(())
            }
        }).map_err(Error::into)
        .await
    }

    /// Delete an extended attribute
    pub async fn deleteextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
                         name: &OsStr)
        -> std::result::Result<(), i32>
    {
        let objkey = ObjKey::extattr(ns, name);
        let name = name.to_owned();
        let key = FSKey::new(fd.ino, objkey);
        self.db.fswrite(self.tree, 1, 0, 1, 0, move |dataset| async move {
            htable::remove::<ReadWriteFilesystem, ExtAttr>(dataset,
                key, ns, name).await
        }).map_ok(drop)
        .map_err(Error::into)
        .await
    }

    fn do_create<'a>(&'a self, args: CreateArgs<'a>) ->
        impl Future<Output = std::result::Result<FileData, i32>>
    {
        let ino = self.next_object();
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        let parent_dirent_objkey = ObjKey::dir_entry(&args.name);
        let name2 = args.name.clone();
        let parent_dirent = Dirent {
            ino,
            dtype: args.file_type.dtype(),
            name:   args.name
        };
        let bb = parent_dirent.allocated_space();
        let parent_dirent_key = FSKey::new(args.parent.ino,
                                           parent_dirent_objkey);

        let cb = args.cb;
        let cb_credit = args.cb_credit;

        let parent_ino = args.parent.ino;
        let fd_parent = if args.file_type == FileType::Dir {
            Some(parent_ino)
        } else {
            // Non-directories may be multiply-linked, so the FileData can't
            // accurately store the parent ino.
            None
        };
        let now = Timespec::now();
        let inode = Inode {
            size: 0,
            bytes: 0,
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

        let ninsert = 5 + cb_credit.0;
        self.db.fswrite(self.tree, ninsert, cb_credit.1, cb_credit.2, bb,
        move |dataset| async move {
            let ds = Arc::new(dataset);
            let extra_fut = cb(&ds, parent_ino, ino);
            let inode_fut = ds.insert(inode_key, inode_value);
            let dirent_fut = htable::insert(ds, parent_dirent_key,
                                            parent_dirent, name2);
            let (inode_r, dirent_r, _) = future::try_join3(inode_fut,
                dirent_fut, extra_fut).await?;
            assert!(dirent_r.is_none(),
            "Create of an existing file.  The VFS should prevent this");
            assert!(inode_r.is_none(),
            "Inode double-create detected, ino={}", ino);
            Ok(FileData::new(fd_parent, ino))
        }).map_err(Error::into)
    }

    // Actually delete an inode, which must already be unlinked
    fn do_delete_inode(ds: Arc<ReadWriteFilesystem>, ino: u64)
        -> impl Future<Output=Result<()>>
    {
        let ds2 = ds.clone();
        // delete its blob extents and blob extended attributes
        let extent_stream = ds.range(FSKey::extent_range(ino, ..))
        .try_filter_map(move |(_k, v)| {
            if let Extent::Blob(be) = v.as_extent().unwrap()
            {
                future::ok(Some(be.rid))
            } else {
                future::ok(None)
            }
        });
        let extattr_stream = Fs::list_extattr_rids(&*ds, ino);
        extent_stream.chain(extattr_stream)
        .try_for_each_concurrent(None, move |rid| ds2.delete_blob(rid))
        .and_then(move |_| async move {
            // Finally, range_delete its key range, including inode,
            // inline extents, and inline extattrs
            ds.range_delete(FSKey::obj_range(ino)).await
        })
    }

    /// Remove the inode if this was its last reference
    async fn do_inactive(ds: Arc<ReadWriteFilesystem>, ino: u64)
        -> Result<()>
    {
        let dikey = FSKey::new(0, ObjKey::dying_inode(ino));
        let di = ds.remove(dikey).await?;
        match di {
            None => Ok(()),
            Some(di2) => {
                assert_eq!(ino, di2.as_dying_inode().unwrap().ino());
                Fs::do_delete_inode(ds, ino).await
            },
        }
    }

    /// Asynchronously read from a file.
    fn do_read<DS>(dataset: DS, ino: u64, fsize: u64, rs: u64, offset: u64,
                   size: usize)
        -> impl Future<Output=Result<SGList>>
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
        dataset.range(erange)
        .and_then(move |(k, v)| {
            let ofs = k.offset();
            match v.as_extent().unwrap() {
                Extent::Inline(ile) => {
                    let buf = ile.buf.try_const().unwrap();
                    future::ok((ofs, buf)).boxed()
                },
                Extent::Blob(be) => {
                    dataset.get_blob(be.rid)
                    .map_ok(move |bbuf| (ofs, *bbuf))
                    .boxed()
                }
            }
        }).try_fold(initial, move |acc, (ofs, mut db)| {
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
        }).map_ok(move |(mut sglist, mut p, _rec)| {
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
        -> impl Future<Output=Result<()>> + Send
    {
        // Outline:
        // 1) range_delete its key range
        // 2) Decrement the parent dir's link count and update its timestamps

        // 1) range_delete its key range
        let dataset2 = dataset.clone();
        let dataset3 = dataset.clone();
        let ino_fut = Fs::list_extattr_rids(&*dataset, ino)
        .try_for_each_concurrent(None, move |rid| {
            dataset2.delete_blob(rid)
        }).and_then(move |_|
            dataset3.range_delete(FSKey::obj_range(ino))
        );

        // 2) Update the parent dir's link count and timestamps
        let parent_ino_key = FSKey::new(parent, ObjKey::Inode);
        let nlink_fut = if dec_nlink {
            let fut = dataset.get(parent_ino_key)
            .and_then(move |r| {
                let mut value = r.unwrap();
                {
                    let inode = value.as_mut_inode().unwrap();
                    let now = Timespec::now();
                    inode.mtime = now;
                    inode.ctime = now;
                    inode.nlink -= 1;
                }
                dataset.insert(parent_ino_key, value)
                .map_ok(drop)
            });
            fut.boxed()
        } else {
            future::ok(()).boxed()
        };
        future::try_join(ino_fut, nlink_fut)
        .map_ok(drop)
    }

    async fn do_setattr(
        dataset: Arc<ReadWriteFilesystem>,
        ino: u64,
        attr: SetAttr
    ) -> Result<()>
    {
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        let r = dataset.get(inode_key).await?;
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

        let freed_bytes = if new_size < old_size {
            assert!(iv.file_type.dtype() == libc::DT_REG);
            let rs = iv.record_size().unwrap() as u64;
            let dsx = dataset.clone();
            Fs::do_deallocate(dsx, ino, new_size, None, rs).await?
        } else {
            0
        };

        iv.bytes = iv.bytes.saturating_sub(freed_bytes);
        dataset.insert(inode_key, FSValue::Inode(iv)).await
        .map(drop)
    }

    /// Deallocate a range of byte offsets from a file, but do not update its
    /// Inode.  Return the number of bytes deallocated.
    async fn do_deallocate(
        dataset: Arc<ReadWriteFilesystem>,
        ino: u64,
        offset: u64,
        len: Option<u64>,
        rs: u64)
        -> Result<u64>
    {
        let full_range = match len {
            Some(l) => FSKey::extent_range(
                ino, offset..((offset + l) - (offset + l) % rs).max(offset)
            ),
            None => FSKey::extent_range(ino, offset..)
        };
        let full_range2 = full_range.clone();
        let dataset2 = dataset.clone();
        let dataset3 = dataset.clone();
        let dataset4 = dataset.clone();

        // Deallocate any partial record on the left side of the range
        let left_fut = if offset % rs > 0 {
            // Offset within the record where deallocation starts
            let recofs = offset % rs;
            // Length of the deallocated portion of this record
            let len = (rs - recofs).min(len.unwrap_or(u64::MAX)) as usize;
            let k = FSKey::new(ino, ObjKey::Extent(offset - recofs));
            // NB: removing a Value and reinserting is inefficient. Better to
            // use a Tree::with_mut method to mutate it in place.
            // https://github.com/bfffs/bfffs/issues/74
            let v = dataset.remove(k).await?;
            match v {
                None => {
                    // It's a hole; nothing to do
                    future::ok(0)
                }.boxed(),
                Some(FSValue::InlineExtent(ile)) => async move {
                    let mut b = ile.buf.try_mut().unwrap();
                    let r = if len >= b.len() - recofs as usize {
                        // truncate the record, making it sparse
                        let old_len = b.len();
                        b.try_truncate(recofs as usize).unwrap();
                        old_len as u64 - recofs
                    } else {
                        // zero the deallocated portion of the record
                        for i in 0..len {
                            b[recofs as usize + i] = 0;
                        }
                        0
                    };
                    let extent = InlineExtent::new(ile.buf);
                    let v = FSValue::InlineExtent(extent);
                    dataset.insert(k, v).await?;
                    Ok(r)
                }.boxed(),
                Some(FSValue::BlobExtent(be)) => async move {
                    let dbs: Box<DivBufShared> = dataset.remove_blob(be.rid)
                        .await?;
                    let mut b = dbs.try_mut()
                        .expect("DivBufShared wasn't uniquely owned");
                    let r = if len >= b.len() - recofs as usize {
                        // truncate the record, making it sparse
                        let old_len = b.len();
                        b.try_truncate(recofs as usize).unwrap();
                        old_len as u64 - recofs
                    } else {
                        // zero the deallocated portion of the record
                        for i in 0..len {
                            b[recofs as usize + i] = 0;
                        }
                        0
                    };
                    let adbs = Arc::from(dbs);
                    let extent = InlineExtent::new(adbs);
                    let v = FSValue::InlineExtent(extent);
                    dataset.insert(k, v).await?;
                    Ok(r)
                }.boxed(),
                x => panic!("Unexpected value {:?} for key {:?}", x, k)
            }
        } else {
            future::ok(0).boxed()
        };

        // Deallocate all whole records in the range
        let full_fut = dataset2.range(full_range)
       .try_fold((FuturesUnordered::new(), 0u64), |(futs, mut s), (_k, v)| {
            match v.as_extent().unwrap() {
                Extent::Blob(be) => {
                    s += u64::from(be.lsize);
                    futs.push(dataset2.delete_blob(be.rid));
                },
                Extent::Inline(ile) => {
                    s += ile.len() as u64;
                }
            }
            future::ok((futs, s))
        }).and_then(move |(futs, old_len)| {
            futs.try_collect::<Vec<_>>()
            .map_ok(move |_| old_len)
        }).and_then(move |old_len| {
            dataset3.range_delete(full_range2)
            .map_ok(move |_| old_len)
        }).boxed();

        // Deallocate any partial record on the right side of the range, if the
        // range ends in a different record than it started.
        let right_fut = match len {
            Some(l) if ((offset + l) / rs > offset / rs) &&
                       (offset + l) % rs != 0 =>
            {
                let len = (offset + l) % rs;
                let k = FSKey::new(ino, ObjKey::Extent(offset + l - len));
                let v = dataset4.remove(k).await?;
                match v {
                    None => {
                        // It's a hole; nothing to do
                        future::ok(0).boxed()
                    },
                    Some(FSValue::InlineExtent(ile)) => async move {
                        let mut b = ile.buf.try_mut().unwrap();
                        for i in 0..len {
                            b[i as usize] = 0;
                        }
                        let extent = InlineExtent::new(ile.buf);
                        let v = FSValue::InlineExtent(extent);
                        dataset4.insert(k, v).await?;
                        Ok(0)
                    }.boxed(),
                    Some(FSValue::BlobExtent(be)) => async move {
                        let dbs = dataset4.remove_blob(be.rid)
                            .await?;
                        let mut dbm = dbs.try_mut()
                            .expect("DivBufShared wasn't uniquely owned");
                        for i in 0..len {
                            dbm[i as usize] = 0;
                        }
                        let adbs = Arc::from(dbs);
                        let extent = InlineExtent::new(adbs);
                        let v = FSValue::InlineExtent(extent);
                        dataset4.insert(k, v).await?;
                        Ok(0)
                    }.boxed(),
                    x => panic!("Unexpected value {:?} for key {:?}", x, k)
                }
            },
            _ => future::ok(0).boxed()
        };

        future::try_join3(full_fut, left_fut, right_fut)
            .map_ok(|(left, whole, right)| left + whole + right).await
    }

    /// Unlink a file whose inode number is known and whose directory entry is
    /// already deleted.
    fn do_unlink(dataset: Arc<ReadWriteFilesystem>,
                 lookup_count: u64,
                 ino: u64)
        -> impl Future<Output=Result<()>> + Send
    {
        // 1) Lookup the inode
        let key = FSKey::new(ino, ObjKey::Inode);
        dataset.get(key)
        .map_ok(move |r| {
            match r {
                Some(v) => {
                    v.as_inode().unwrap().clone()
                },
                None => {
                    panic!("Orphan directory entry")
                },
            }
        }).and_then(move |mut iv| async move {
            // 2a) Decrement the link count and touch the ctime
            iv.nlink = iv.nlink.saturating_sub(1);
            let nlink = iv.nlink;
            iv.ctime = Timespec::now();
            // 2b) Update Inode, if we aren't immediately deleting it
            if nlink > 0 || lookup_count > 0 {
                let fut = if nlink == 0 {
                    // 2c) Record imminent death of inode
                    let dikey = FSKey::new(0, ObjKey::dying_inode(ino));
                    let dival = FSValue::DyingInode(DyingInode::from(ino));
                    let fut = dataset.insert(dikey, dival)
                    .map_ok(|r|
                        if r.is_some() {
                            panic!("Hash collisions on dying inodes are TODO!")
                        }
                    ).map_ok(drop);
                    fut.boxed()
                } else {
                    future::ok(()).boxed()
                };
                future::try_join(
                    fut,
                    dataset.insert(key, FSValue::Inode(iv))
                    .map_ok(drop)
                ).await?;
            } else {
                // Delete the inode straight away
                Fs::do_delete_inode(dataset, ino).await?;
            }
            Ok(())
        })
    }

    /// Create a new Fs object (in memory, not on disk).
    ///
    /// # Arguments
    ///
    /// * `database` -  An already open `Database` object
    /// * `name`    -   The dataset's name, excluding the pool
    // Should be private.  Is only public so it can be used by the functional
    // tests.
    #[doc(hidden)]
    pub async fn new<S>(database: Arc<Database>, name: S) -> Self
        where S: AsRef<str> + 'static
    {
        let db2 = database.clone();
        let db3 = database.clone();
        let db4 = database.clone();
        let tree_id = database.lookup_fs(name.as_ref()).await
            .unwrap()
            .expect("Filesystem does not yet exist");
        let (last_key, (atimep, _), (recsizep, _), _) =
        db4.fsread(tree_id, move |dataset| {
            let last_key_fut = dataset.last_key();
            let atime_fut = db2.get_prop(tree_id, PropertyName::Atime);
            let recsize_fut = db2.get_prop(tree_id, PropertyName::RecordSize);
            let di_fut = db3.fswrite(tree_id, 0, 1, 0, 0,
            move |dataset| async move {
                // Delete all dying inodes.  If there are any, it means that
                // the previous mount was uncleanly dismounted.
                let ds = Arc::new(dataset);
                let ds2 = ds.clone();
                let had_dying_inodes = ds.range(FSKey::dying_inode_range())
                .try_fold(false, move |_acc, (_k, v)| {
                    let ds3 = ds.clone();
                    async move {
                        let ino = v.as_dying_inode().unwrap().ino();
                        Fs::do_delete_inode(ds3, ino).await?;
                        Ok(true)
                    }
                }).await?;
                // Finally, range delete all of the dying inodes, if any
                if had_dying_inodes {
                    ds2.range_delete(FSKey::dying_inode_range())
                        .await?;
                }
                Ok(())
            }).boxed();
            future::try_join4(last_key_fut, atime_fut, recsize_fut, di_fut)
        }).map_err(Error::unhandled)
        .await.unwrap();
        let next_object = AtomicU64::new(last_key.unwrap().object() + 1);
        let atime = atimep.as_bool();
        let record_size = recsizep.as_u8();

        Fs {
            db: database,
            next_object,
            tree: tree_id,
            atime,
            record_size,
        }
    }

    fn next_object(&self) -> u64 {
        self.next_object.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn create(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> std::result::Result<FileData, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Reg(self.record_size));
        self.do_create(create_args).await
    }

    /// Update the parent's ctime and mtime to the current time.
    fn create_ts_callback(dataset: &Arc<ReadWriteFilesystem>, parent: u64,
                          _ino: u64)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        let now = Timespec::now();
        let attr = SetAttr {
            ctime: Some(now),
            mtime: Some(now),
            .. Default::default()
        };
        Fs::do_setattr(dataset.clone(), parent, attr).boxed()
    }

    /// Dump a YAMLized representation of the filesystem's Tree to a plain
    /// `std::fs::File`.
    pub async fn dump(&self, f: &mut dyn io::Write) -> std::result::Result<(), i32> {
        self.db.dump(f, self.tree)
        .map_err(|e| e.into())
        .await
    }

    /// Tell the file system that the given file is no longer needed by the
    /// client.  Its resources may be freed.
    // Fs::inactive consumes fd because the client should not longer need it.
    pub async fn inactive(&self, fd: FileData) {
        let ino = fd.ino();

        self.db.fswrite(self.tree, 0, 1, 1, 0, move |dataset| {
            Fs::do_inactive(Arc::new(dataset), ino)
            .map(|r| r.map(drop))
        }).await
        .expect("Fs::inactive should never fail");
    }

    /// Sync a file's data and metadata to disk so it can be recovered after a
    /// crash.
    pub async fn fsync(&self, _fd: &FileData) -> std::result::Result<(), i32> {
        // Until we come up with a better mechanism, we must sync the entire
        // file system.
        self.sync().await;
        Ok(())
    }

    pub async fn getattr(&self, fd: &FileData) -> std::result::Result<GetAttr, i32> {
        self.getattr_priv(fd.ino).map_err(Error::into).await
    }

    async fn getattr_priv(&self, ino: u64) -> Result<GetAttr> {
        self.db.fsread(self.tree, move |dataset| {
            Fs::do_getattr(&dataset, ino)
        }).await
    }

    pub fn do_getattr(dataset: &ReadOnlyFilesystem, ino: u64)
        -> impl Future<Output=Result<GetAttr>>
    {
        let key = FSKey::new(ino, ObjKey::Inode);
        dataset.get(key)
        .map(move |r| {
            match r {
                Ok(Some(v)) => {
                    let inode = v.as_inode().unwrap();
                    let rdev = match inode.file_type {
                        FileType::Char(x) | FileType::Block(x) => x,
                        _ => 0
                    };
                    // Non-regular files don't have a defined block
                    // size, but we need to pick something.  4kB seems
                    // as good as anything else.
                    let blksize = inode.record_size()
                        .unwrap_or(4096)
                        as u32;
                    let attr = GetAttr {
                        ino,
                        size: inode.size,
                        bytes: inode.bytes,
                        atime: inode.atime,
                        mtime: inode.mtime,
                        ctime: inode.ctime,
                        birthtime: inode.birthtime,
                        mode: Mode(inode.file_type.mode() | inode.perm),
                        nlink: inode.nlink,
                        uid: inode.uid,
                        gid: inode.gid,
                        rdev,
                        blksize,
                        flags: inode.flags,
                    };
                    Ok(attr)
                },
                Ok(None) => {
                    Err(Error::ENOENT)
                },
                Err(e) => {
                    Err(e)
                }
            }
        })
    }

    /// Retrieve the value of an extended attribute
    pub async fn getextattr(&self, fd: &FileData, ns: ExtAttrNamespace, name: &OsStr)
        -> std::result::Result<DivBuf, i32>
    {
        let owned_name = name.to_owned();
        let objkey = ObjKey::extattr(ns, name);
        let key = FSKey::new(fd.ino, objkey);
        self.db.fsread(self.tree, move |dataset| {
            htable::get(&htable::ReadFilesystem::ReadOnly(&dataset), key,
                        ns, owned_name)
            .and_then(move |extattr| {
                match extattr {
                    ExtAttr::Inline(iea) => {
                        let buf = Box::new(iea.extent.buf.try_const()
                                           .unwrap());
                        future::ok(buf).boxed()
                    },
                    ExtAttr::Blob(bea) => {
                        dataset.get_blob(bea.extent.rid).boxed()
                    }
                }
            })
        }).map(|r| {
            match r {
                Ok(buf) => Ok(*buf),
                Err(e) => Err(e.into())
            }
        }).await
    }

    /// Retrieve the length of the value of an extended attribute
    pub async fn getextattrlen(&self, fd: &FileData, ns: ExtAttrNamespace,
                         name: &OsStr)
        -> std::result::Result<u32, i32>
    {
        let owned_name = name.to_owned();
        let objkey = ObjKey::extattr(ns, name);
        let key = FSKey::new(fd.ino, objkey);
        self.db.fsread(self.tree, move |dataset| {
            dataset.get(key)
            .map(move |r| {
                match r {
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
                            Err(Error::ENOATTR)
                        }
                    }
                    Err(e) => {
                        Err(e)
                    }
                    _ => {
                        Err(Error::ENOATTR)
                    },
                }
            })
        }).map_err(Error::into)
        .await
    }

    /// Get an inode's attributes
    ///
    /// For testing purposes only!  Production code must use [`Fs::getattr`]
    /// instead.
    #[cfg(debug_assertions)]
    pub async fn igetattr(&self, ino: u64) -> std::result::Result<GetAttr, i32> {
        self.getattr_priv(ino).map_err(Error::into).await
    }

    /// Lookup a file by its inode number
    ///
    /// This is needed by NFS servers, which sometimes lookup a file without
    /// knowing its parent directory.  In the kernel, that goes through
    /// VFS_VGET.  FUSE has no equivalent, so FreeBSD's fusefs driver translates
    /// it into a lookup of ".".  Crucially, bfffs's fusefs layer might not have
    /// the parent directory in the file data cache.
    ///
    /// In general such an interface is racy, because the file system might
    /// delete the file in question, then recreate a different file with the
    /// same inode number.  But BFFFS never reuses inode numbers.
    pub async fn ilookup(&self, ino: u64) -> std::result::Result<FileData, i32>
    {
        let name = OsString::from(r"..");
        let objkey = ObjKey::dir_entry(&name);
        let key = FSKey::new(ino, objkey);
        self.db.fsread(self.tree, move |dataset| {
            let rfs = htable::ReadFilesystem::ReadOnly(&dataset);
            let inode_fut = Fs::do_getattr(&dataset, ino);
            let dirent_fut = htable::get::<Dirent>(&rfs, key, 0, name);
            future::join(inode_fut, dirent_fut)
            .map(move |r| {
                match r {
                    (Ok(_), Ok(de)) => {
                        // The file is a directory
                        let fd = FileData::new(Some(de.ino), ino);
                        Ok(fd)
                    },
                    (Ok(_), Err(Error::ENOENT)) => {
                        // It's a regular file
                        let fd = FileData::new(None, ino);
                        Ok(fd)
                    },
                    (Ok(_), Err(e)) => Err(e),
                    (Err(e), _) => Err(e)
                }
            })
        }).map_err(Error::into)
        .await
    }

    /// Create a hardlink from `fd` to `parent/name`.
    pub async fn link(&self, parent: &FileData, fd: &FileData, name: &OsStr)
        -> std::result::Result<(), i32>
    {
        // Outline:
        // * Increase the target's link count
        // * Add the new directory entry
        // * Update the parent's mtime and ctime
        let ino = fd.ino;
        let parent_ino = parent.ino;
        let name = name.to_owned();
        self.db.fswrite(self.tree, 2, 0, 0, 0, move |dataset| async move {
            let ds = Arc::new(dataset);
            let inode_key = FSKey::new(ino, ObjKey::Inode);
            let r = ds.get(inode_key).await?;
            let mut iv = r.unwrap().as_mut_inode().unwrap().clone();
            iv.nlink += 1;
            let dtype = iv.file_type.dtype();
            // FUSE is single-threaded, so we don't have to worry that
            // the target gets deleted before we increase its link
            // count.  The real VFS will provide a held vnode rather
            // than an inode.  So in neither case is there a race here.
            // XXX TODO: fuse3 is _not_ single-threaded
            let ifut = ds.insert(inode_key, FSValue::Inode(iv));

            let dirent_objkey = ObjKey::dir_entry(&name);
            let dirent = Dirent { ino, dtype, name };
            let dirent_key = FSKey::new(parent_ino, dirent_objkey);
            let dirent_value = FSValue::DirEntry(dirent);
            let dfut = ds.insert(dirent_key, dirent_value);

            let now = Timespec::now();
            let parent_attr = SetAttr {
                ctime: Some(now),
                mtime: Some(now),
                .. Default::default()
            };
            let parent_fut = Fs::do_setattr(ds.clone(), parent_ino,
                parent_attr);

            let ctime_attr = SetAttr {
                ctime: Some(now),
                .. Default::default()
            };
            let ctime_fut = Fs::do_setattr(ds, ino, ctime_attr);

            future::try_join4(ifut, dfut, parent_fut, ctime_fut).await?;
            Ok(())
        }).map_err(Error::into)
        .await
    }

    /// List the RID of every blob extattr for a file
    fn list_extattr_rids(dataset: &ReadWriteFilesystem, ino: u64)
        -> impl Stream<Item=Result<RID>>
    {
        dataset.range(FSKey::extattr_range(ino))
        .try_filter_map(move |(k, v)| {
            match v {
                FSValue::ExtAttr(ExtAttr::Inline(_)) => future::ok(None),
                FSValue::ExtAttr(ExtAttr::Blob(be)) => {
                    let s = stream::once(future::ok(be.extent.rid)).boxed();
                    future::ok(Some(s))
                },
                FSValue::ExtAttrs(r) => {
                    let rids = r.iter().filter_map(|v| {
                        if let ExtAttr::Blob(be) = v {
                            Some(Ok(be.extent.rid))
                        } else {
                            None
                        }
                    }).collect::<Vec<_>>();
                    let s = stream::iter(rids).boxed();
                    future::ok(Some(s))
                }
                x => panic!("Unexpected value {:?} for key {:?}", x, k)
            }
        }).try_flatten()
    }

    /// Lookup a file by its file name.
    ///
    /// NB: The client is responsible for managing the lifetime of the returned
    /// `FileData` object.  In particular, the client must ensure that there are
    /// never two `FileData`s for the same directory entry at the same time.
    pub async fn lookup(&self, grandparent: Option<&FileData>, parent: &FileData,
        name: &OsStr) -> std::result::Result<FileData, i32>
    {
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
        self.db.fsread(self.tree, move |dataset| {
            let rfs = htable::ReadFilesystem::ReadOnly(&dataset);
            htable::get::<Dirent>(&rfs, key, 0, owned_name)
            .map(move |r| {
                match r {
                    Ok(de) => {
                        let fd_parent = if de.dtype == libc::DT_DIR {
                            parent_ino
                        } else {
                            None
                        };
                        let fd = FileData::new(fd_parent, de.ino);
                        Ok(fd)
                    },
                    Err(e) => Err(e)
                }
            })
        }).map_err(Error::into)
        .await
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
    pub async fn listextattr<F>(&self, fd: &FileData, size: u32, f: F)
        -> std::result::Result<Vec<u8>, i32>
        where F: Fn(&mut Vec<u8>, &ExtAttr) + Send + 'static
    {
        let ino = fd.ino;
        self.db.fsread(self.tree, move |dataset| {
            let buf = Vec::with_capacity(size as usize);
            dataset.range(FSKey::extattr_range(ino))
            .try_fold(buf, move |mut buf, (k, v)| {
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
            })
        }).map_err(Error::into)
        .await
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
    pub async fn listextattrlen<F>(&self, fd: &FileData, f: F) -> std::result::Result<u32, i32>
        where F: Fn(&ExtAttr) -> u32 + Send + 'static
    {
        let ino = fd.ino;
        self.db.fsread(self.tree, move |dataset| {
            dataset.range(FSKey::extattr_range(ino))
            .try_fold(0u32, move |mut len, (k, v)| {
                len += match v {
                    FSValue::ExtAttr(xattr) => f(&xattr),
                    FSValue::ExtAttrs(v) => {
                        v.iter().map(&f).sum()
                    },
                    _ => panic!("Unexpected value {:?} for key {:?}", v, k)
                };
                future::ok::<u32, Error>(len)
            })
        }).map_err(Error::into)
        .await
    }

    /// Find the next hole or data region in a file
    ///
    /// # Returns
    ///
    /// - `Ok(offset)`  - The requested region begins at `offset`, or `offset`
    ///                   is the file size and there are no more holes between
    ///                   the requested `offset` and EoF.`
    /// - `Err(ENXIO)` -  For `Data`, there are no more data regions past
    ///                   the supplied offset.  Or, the supplied offset already
    ///                   points past EoF.
    pub async fn lseek(&self,
        fd: &FileData,
        mut offset: u64,
        whence: SeekWhence) -> std::result::Result<u64, i32>
    {
        let ino = fd.ino;
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        self.db.fsread(self.tree, move |ds| async move {
            let r = ds.get(inode_key).await
                .unwrap()
                .expect("Inode not found");
            let inode = r.as_inode()
                .expect("Wrong value type");
            let fsize = inode.size;
            if offset > fsize {
                Err(Error::ENXIO)
            } else {
                let rs = inode.record_size().unwrap() as u64;
                let baseoffset = offset - (offset % rs);
                let erange = FSKey::extent_range(ino, baseoffset..);
                let mut ex_stream = ds.range(erange);
                let mut r = match whence {
                    SeekWhence::Data => Err(Error::ENXIO),
                    SeekWhence::Hole => Ok(fsize)
                };
                while let Some((k, v)) = ex_stream.try_next().await? {
                    let end = k.offset() + v.as_extent().unwrap().len() as u64;
                    if end <= offset {
                        // This extent is too low
                        continue
                    } else if k.offset() <= offset {
                        // In the middle of a data region
                        match whence {
                            SeekWhence::Data => {
                                r = Ok(offset);
                                break
                            },
                            SeekWhence::Hole => {
                                offset = end;
                                r = Ok(offset);
                                // Keep searching for holes
                            }
                        }
                    } else {
                        // The next data region is to the right
                        match whence {
                            SeekWhence::Data => {
                                r = Ok(k.offset());
                                break
                            },
                            SeekWhence::Hole => {
                                r = Ok(offset);
                                break
                            }
                        }
                    }
                }
                r
            }
        }).map_err(Error::into)
        .await
    }

    pub async fn mkdir(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                 gid: u32) -> std::result::Result<FileData, i32>
    {
        let nlink = 2;  // One for the parent dir, and one for "."

        fn f(
            dataset: &Arc<ReadWriteFilesystem>,
            parent_ino: u64,
            ino: u64,
        ) -> Pin<Box<dyn Future<Output=Result<()>> + Send + 'static>>
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
                        let now = Timespec::now();
                        inode.mtime = now;
                        inode.ctime = now;
                    }
                    dataset2.insert(parent_inode_key, value)
                });

            let dot_fut = htable::insert(dataset3, dot_dirent_key,
                dot_dirent, dot_filename);
            let dotdot_fut = htable::insert(dataset4, dotdot_dirent_key,
                dotdot_dirent, dotdot_filename);
            let fut = future::try_join3(dot_fut, dotdot_fut, nlink_fut)
            .map_ok(drop);
            fut.boxed()
        }

        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Dir)
        .nlink(nlink)
        .callback(f, 3, 0, 0);

        self.do_create(create_args).await
    }

    /// Make a block device
    pub async fn mkblock(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                   gid: u32, rdev: u32) -> std::result::Result<FileData, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Block(rdev));
        self.do_create(create_args).await
    }

    /// Make a character device
    pub async fn mkchar(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32, rdev: u32) -> std::result::Result<FileData, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Char(rdev));
        self.do_create(create_args).await
    }

    pub async fn mkfifo(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> std::result::Result<FileData, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Fifo);
        self.do_create(create_args).await
    }

    pub async fn mksock(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                  gid: u32) -> std::result::Result<FileData, i32>
    {
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          FileType::Socket);
        self.do_create(create_args).await
    }

    /// Check that a directory is safe to delete
    fn ok_to_rmdir(ds: &ReadWriteFilesystem, ino: u64, parent: u64,
                   name: OsString)
        -> impl Future<Output=Result<()>> + Send
    {
        ds.range(FSKey::obj_range(ino))
        .try_fold(false, |found_inode, (_, v)| {
            match v {
                FSValue::DirEntry(dirent) => {
                    if dirent.name != OsStr::new(".") &&
                       dirent.name != OsStr::new("..") {
                        future::err(Error::ENOTEMPTY)
                    } else {
                        future::ok(found_inode)
                    }
                },
                FSValue::DirEntries(_) => {
                    // It is known that "." and ".." do not have a hash
                    // collision (otherwise it would be impossible to ever rmdir
                    // anything).  So the mere presence of this value type
                    // indicates ENOTEMPTY.  We don't need to check the
                    // contents.
                    future::err(Error::ENOTEMPTY)
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
                    future::ok(true)
                },
                FSValue::ExtAttr(_) | FSValue::ExtAttrs(_) => {
                    // It's fine to remove a directory with extended attributes.
                    future::ok(found_inode)
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
        }).map_ok(move |found_inode| {
            assert!(found_inode,
                concat!("Inode {} not found, but parent ",
                        "direntry {}:{:?} exists!"),
                ino, parent, name);
        })
    }

    pub async fn read(&self, fd: &FileData, offset: u64, size: usize)
        -> std::result::Result<SGList, i32>
    {
        let ino = fd.ino;
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        // We only need a writeable FS reference if we're going to update atime.
        // If not, then only get a read reference.  Read references are better
        // because they can be held during txg syncs.
        if self.atime {
            self.db.fswrite(self.tree, 1, 0, 0, 0, move |ds| async move {
                let r = ds.get(inode_key).await?;
                let mut value = r.expect("Inode not found");
                let inode = value.as_mut_inode()
                    .expect("Wrong Value type");
                let now = Timespec::now();
                inode.atime = now;
                let fsize = inode.size;
                let rs = inode.record_size().unwrap() as u64;
                let afut = ds.insert(inode_key, value);
                let dfut = Fs::do_read(ds, ino, fsize, rs, offset, size);
                let (sglist, _) = future::try_join(dfut, afut).await?;
                Ok(sglist)
            }).map_err(Error::into)
            .await
        } else {
            self.db.fsread(self.tree, move |ds| {
                ds.get(inode_key)
                .and_then(move |r| {
                    let value = r.expect("Inode not found");
                    let inode = value.as_inode()
                        .expect("Wrong Value type");
                    let fsize = inode.size;
                    let rs = inode.record_size().unwrap() as u64;
                    Fs::do_read(ds, ino, fsize, rs, offset, size)
                })
            }).map_err(Error::into)
            .await
        }
    }

    // TODO: change Ok type to just libc::dirent after switching to a FreeBSD
    // 12+ ABI, since that has a builtin offset field.  Depends on
    // https://github.com/rust-lang/libc/pull/2406
    pub fn readdir(&self, fd: &FileData, soffs: i64)
        -> impl Stream<Item=std::result::Result<(libc::dirent, i64), i32>> + Send
    {

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

        /// Stores the state of `ReaddirStream` as it iterates through a bucket
        struct Bucketing {
            /// Contents of the bucket that haven't been returned yet.
            ///
            /// Invariant: never empty
            bucket: Vec<Dirent>,
            /// Offset of the FSKey of this bucket
            kofs: u64,
            /// Number of dirents already returned from this bucket
            returned: u8
        }

        struct ReaddirStream {
            /// Number of entries to skip from the first bucket
            bucket_idx: u8,
            rq: RangeQuery<FSKey, FSKey, FSValue<RID>>,
            /// If the stream is currently positioned in the middle of a bucket,
            /// store that bucket
            bucketing: Option<Bucketing>
        }
        impl ReaddirStream {
            /// Pop one entry from the contained bucket and return it
            ///
            /// # Panics
            ///
            /// Panics if there is no contained bucket
            fn pop_bucket(mut self: Pin<&mut Self>)
                -> (libc::dirent, i64)
            {
                let mut bucketing = self.bucketing.take().unwrap();
                let dirent = bucketing.bucket.pop().unwrap();
                bucketing.returned += 1;
                let curs = if bucketing.bucket.is_empty() {
                    Cursor::new(bucketing.kofs + 1, 0)
                } else {
                    let curs = Cursor::new(bucketing.kofs,
                                           usize::from(bucketing.returned));
                    self.bucketing = Some(bucketing);
                    curs
                };
                (dirent2dirent(dirent), curs.0 as i64)
            }
        }
        impl Stream for ReaddirStream {
            type Item = Result<(libc::dirent, i64)>;

            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context)
                -> Poll<Option<Self::Item>>
            {
                if self.bucketing.is_some() {
                    return Poll::Ready(Some(Ok(self.pop_bucket())));
                }

                match Pin::new(&mut self.rq).poll_next(cx) {
                    Poll::Ready(Some(Ok((k, v)))) => match v {
                        FSValue::DirEntry(dirent) => {
                            let curs = Cursor::new(k.offset() + 1, 0);
                            let de = dirent2dirent(dirent);
                            Poll::Ready(Some(Ok((de, curs.0 as i64))))
                        },
                        FSValue::DirEntries(mut bucket) => {
                            for _ in 0..self.bucket_idx {
                                // Ignore errors.  They indicate that the bucket
                                // has shrunk since the Cursor was created
                                let _ = bucket.pop();
                            }
                            self.bucket_idx = 0;
                            if !bucket.is_empty() {
                                self.bucketing = Some(Bucketing {
                                    bucket,
                                    kofs: k.offset(),
                                    returned: 0
                                });
                                Poll::Ready(Some(Ok(self.pop_bucket())))
                            } else {
                                self.poll_next(cx)
                            }
                        },
                        x => panic!("Unexpected value {:?} for key {:?}",
                                    x, k)
                    },
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                    Poll::Pending => Poll::Pending,
                }
            }
        }

        const DIRENT_SIZE: usize = mem::size_of::<libc::dirent>();

        fn dirent2dirent(bfffs_dirent: Dirent) -> libc::dirent {
            let namlen = bfffs_dirent.name.as_bytes().len();
            let mut fs_dirent = libc::dirent {
                d_fileno: bfffs_dirent.ino as u32,
                d_reclen: DIRENT_SIZE as u16,
                d_type: bfffs_dirent.dtype,
                d_namlen: namlen as u8,
                d_name: unsafe{mem::zeroed()}
            };
            // libc::dirent uses "char" when it should be using
            // "unsigned char", so we need an unsafe conversion
            let p = bfffs_dirent.name.as_bytes() as *const [u8]
                as *const [i8];
            fs_dirent.d_name[0..namlen].copy_from_slice(unsafe{&*p});
            fs_dirent
        }

        let ino = fd.ino;
        self.db.fsreads(self.tree, move |dataset| {
            let cursor = Cursor::from(soffs);
            let offs = cursor.offset();
            let rq = dataset.range(FSKey::dirent_range(ino, offs));
            let bucketing = None;
            let bucket_idx = cursor.bucket_idx();
            ReaddirStream{bucket_idx, bucketing, rq}
        }).map_err(Error::into)
    }

    pub async fn readlink(&self, fd: &FileData) -> std::result::Result<OsString, i32> {
        let ino = fd.ino;
        self.db.fsread(self.tree, move |dataset| {
            let key = FSKey::new(ino, ObjKey::Inode);
            dataset.get(key)
            .map(move |r| {
                match r {
                    Ok(Some(v)) => {
                        let inode = v.as_inode().unwrap();
                        if let FileType::Link(ref path) = inode.file_type {
                            Ok(path.clone())
                        } else {
                            Err(Error::EINVAL)
                        }
                    },
                    Ok(None) => Err(Error::ENOENT),
                    Err(e) => Err(e)
                }
            })
        }).map_err(Error::into)
        .await
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
    pub async fn rename(&self, parent: &FileData, fd: &FileData, name: &OsStr,
        newparent: &FileData, newino: Option<u64>, newname: &OsStr)
        -> std::result::Result<u64, i32>
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
        let src_objkey = ObjKey::dir_entry(name);
        let owned_name = name.to_owned();
        let dst_objkey = ObjKey::dir_entry(newname);
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

        self.db.fswrite(self.tree, 8, 1, 1, 0, move |dataset| {
            let ds = Arc::new(dataset);
            let ds4 = ds.clone();
            let ds5 = ds.clone();
            let ds6 = ds.clone();
            let dst_de_key = FSKey::new(newparent_ino, dst_objkey);
            // 0) Check conditions
            htable::get(&htable::ReadFilesystem::ReadWrite(ds.as_ref()),
                        dst_de_key, 0, owned_newname)
            .then(move |r: Result<Dirent>| {
                match r {
                    Ok(dirent) => {
                        assert_eq!(dst_ino.expect(
                            "didn't lookup destination before rename"),
                            dirent.ino);
                        if dirent.dtype != libc::DT_DIR {
                            // Overwriting non-directories is allowed
                            future::ok(()).boxed()
                        } else {
                            // Is it a nonempty directory?
                            Fs::ok_to_rmdir(&ds4, dirent.ino,
                                newparent_ino, owned_newname3)
                            .boxed()
                        }
                    },
                    Err(Error::ENOENT) => {
                        // Destination doesn't exist.  No problem!
                        assert!(dst_ino.is_none());
                        future::ok(()).boxed()
                    },
                    Err(e) => {
                        // Other errors should propagate upwards
                        future::err(e).boxed()
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
                .map_ok(move |r| {
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
                        let now = Timespec::now();
                        inode.mtime = now;
                        inode.ctime = now;
                        if isdir && (!samedir || old_dst_ino.is_some()) {
                            inode.nlink -= 1;
                        }
                    }
                    ds2.insert(parent_ino_key, value)
                    .map_ok(drop)
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
                            let now = Timespec::now();
                            inode.mtime = now;
                            inode.ctime = now;
                            if isdir && old_dst_ino.is_none() {
                                inode.nlink += 1;
                            }
                        }
                        ds3.insert(newparent_ino_key, value)
                        .map_ok(drop)
                    });
                    fut.boxed()
                } else {
                    future::ok(()).boxed()
                };
                let unlink_fut = if let Some(v) = old_dst_ino {
                    // 3ci) Decrement old dst's link count
                    if isdir {
                        let fut = Fs::do_rmdir(ds, newparent_ino, v, false,
                                               );
                        fut.boxed()
                    } else {
                        let fut = Fs::do_unlink(ds.clone(), 0, v)
                        .map_ok(drop);
                        fut.boxed()
                    }
                } else {
                    future::ok(()).boxed()
                };
                future::try_join3(unlink_fut, p_nlink_fut, np_nlink_fut)
                .map_ok(move |_| ino)
            })
        }).map_err(Error::into)
        .await
    }

    /// Remove a directory entry for a directory
    ///
    /// - `parent_fd`:  `FileData` of the parent directory, as returned by
    ///                 [`lookup`].
    /// - `name`:       Name of the directory entry to remove.
    // Note, unlike unlink, rmdir takes no Option<&FileData> argument, because
    // there is no need to support open-but-deleted directories.
    pub async fn rmdir(&self, parent: &FileData, name: &OsStr) -> std::result::Result<(), i32> {
        // Outline:
        // 1) Lookup the directory
        // 2) Check that the directory is empty
        // 3) Remove its parent's directory entry
        // 4) Actually remove it
        let parent_ino = parent.ino;
        let owned_name = name.to_os_string();
        let owned_name2 = owned_name.clone();
        let owned_name3 = owned_name.clone();
        let objkey = ObjKey::dir_entry(&owned_name);
        self.db.fswrite(self.tree, 2, 1, 1, 0, move |dataset| async move {
            let ds = Arc::new(dataset);
            let ds2 = ds.clone();
            // 1) Lookup the directory
            let key = FSKey::new(parent_ino, objkey);
            let rde = htable::get::<Dirent>(
                &htable::ReadFilesystem::ReadWrite(&ds), key, 0,
                owned_name3).await;
            if let Err(e) = rde {
                return Err(e);
            };
            let de = rde.unwrap();
            // 2) Check that the directory is empty
            let ino = de.ino;
            if let Err(e) = Fs::ok_to_rmdir(&ds, ino, parent_ino,
                                            owned_name2).await
            {
                return Err(e);
            }
            // 3) Remove the parent dir's dir_entry
            let de_key = FSKey::new(parent_ino, objkey);
            let dirent_fut = htable::remove::<Arc<ReadWriteFilesystem>,
                                              Dirent>
                (ds2.clone(), de_key, 0, owned_name);

            // 4) Actually remove the directory
            let dfut = Fs::do_rmdir(ds2, parent_ino, ino, true);

            future::try_join(dirent_fut, dfut).await?;
            Ok(())
        }).map_err(Error::into)
        .await
    }

    /// Lookup the root directory
    pub fn root(&self) -> FileData {
        FileData{ ino: 1 , lookup_count: 1, parent: None}
    }

    pub async fn setattr(&self, fd: &FileData, mut attr: SetAttr) -> std::result::Result<(), i32> {
        let ino = fd.ino;
        let mut ninsert = 1;
        let mut nrange_delete = 0;
        let mut nremove = 0;
        if attr.size.is_some() {
            // We're truncating
            ninsert += 1;
            nrange_delete += 1;
            nremove += 1;
        }
        self.db.fswrite(self.tree, ninsert, nrange_delete, nremove, 0,
        move |dataset| {
            let ds = Arc::new(dataset);
            if attr.ctime.is_none() {
                attr.ctime = Some(Timespec::now());
            }
            Fs::do_setattr(ds, ino, attr)
            .map_ok(drop)
        }).map_err(Error::into)
        .await
    }

    pub async fn setextattr(&self, fd: &FileData, ns: ExtAttrNamespace,
                      name: &OsStr, data: &[u8]) -> std::result::Result<(), i32>
    {
        let ino = fd.ino;
        let objkey = ObjKey::extattr(ns, name);
        let key = FSKey::new(ino, objkey);
        // Data copy
        let buf = Arc::new(DivBufShared::from(data));
        let extent = InlineExtent::new(buf);
        let owned_name = name.to_owned();
        let extattr = ExtAttr::Inline(InlineExtAttr {
            namespace: ns,
            name: owned_name.clone(),
            extent
        });
        let bb = extattr.allocated_space();
        self.db.fswrite(self.tree, 2, 0, 0, bb, move |dataset| async move {
            htable::insert(dataset, key, extattr, owned_name)
                .await?;
            Ok(())
        }).map_err(Error::into)
        .await
    }

    /// Change filesystem properties
    pub async fn set_props(&mut self, props: Vec<Property>) {
        for prop in props.iter() {
            match prop {
                Property::Atime(atime) => self.atime = *atime,
                Property::RecordSize(exp) => self.record_size = *exp
            }
        }

        // Update on-disk properties in the background
        let db2 = self.db.clone();
        let tree_id = self.tree;
        props.into_iter()
        .map(move |prop| db2.set_prop(tree_id, prop))
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop)
        .await
        .expect("Fs::set_props failed");
    }

    pub async fn statvfs(&self) -> std::result::Result<libc::statvfs, i32> {
        let rs = 1 << self.record_size;
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
            future::ok(r)
        }).map_err(Error::into)
        .await
    }

    /// Create a symlink from `name` to `link`.  Returns the link's inode on
    /// success, or an errno on failure.
    pub async fn symlink(&self, parent: &FileData, name: &OsStr, perm: u16, uid: u32,
                   gid: u32, link: &OsStr) -> std::result::Result<FileData, i32>
    {
        let file_type = FileType::Link(link.to_os_string());
        let create_args = CreateArgs::new(parent, name, perm, uid, gid,
                                          file_type);
        self.do_create(create_args).await
    }

    pub async fn sync(&self) {
        self.db.sync_transaction()
        .await
        .expect("Fs::sync failed");
    }

    /// Remove a directory entry for a non-directory
    ///
    /// - `parent_fd`:  `FileData` of the parent directory, as returned by
    ///                 [`lookup`].
    /// - `fd`:         `FileData` of the directory entry to be removed, if
    ///                 known.  Must be provided if the file has been looked up!
    /// - `name`:       Name of the directory entry to remove.
    pub async fn unlink(&self, parent_fd: &FileData, fd: Option<&FileData>,
        name: &OsStr) -> std::result::Result<(), i32>
    {
        // Outline:
        // 1) Lookup and remove the directory entry
        // 2a) Unlink the Inode
        // 2b) Update parent's mtime and ctime
        let ino = fd.map(|fd| fd.ino);
        let lookup_count = fd.map_or(0, |fd_| fd_.lookup_count);
        let parent_ino = parent_fd.ino;
        let owned_name = name.to_os_string();
        let dekey = ObjKey::dir_entry(&owned_name);
        self.db.fswrite(self.tree, 3, 0, 1, 0, move |ds| async move {
            let dataset = Arc::new(ds);
            // 1) Lookup and remove the directory entry
            let key = FSKey::new(parent_ino, dekey);
            let dirent = htable::remove::<Arc<ReadWriteFilesystem>, Dirent>
                (dataset.clone(), key, 0, owned_name).await?;
            if let Some(ino) = ino {
                assert_eq!(ino, dirent.ino);
            }
            // 2a) Unlink the inode
            let unlink_fut = Fs::do_unlink(dataset.clone(),
                lookup_count, dirent.ino);
            // 2b) Update parent's timestamps
            let now = Timespec::now();
            let attr = SetAttr {
                ctime: Some(now),
                mtime: Some(now),
                .. Default::default()
            };
            let ts_fut = Fs::do_setattr(dataset, parent_ino, attr);
            future::try_join(unlink_fut, ts_fut).await?;
            Ok(())
        }).map_err(Error::into)
        .await
    }

    pub async fn write<IU>(&self, fd: &FileData, offset: u64, data: IU, _flags: u32)
        -> std::result::Result<u32, i32>
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
        let ino = fd.ino;
        let uio = data.into();

        let inode_key = FSKey::new(ino, ObjKey::Inode);
        let mut value = self.db.fsread(self.tree, move |dataset| {
            let inode_key = FSKey::new(ino, ObjKey::Inode);
            dataset.get(inode_key)
        }).map_err::<i32, _>(Error::into)
        .await?.unwrap();

        let rs = value.as_inode().unwrap().record_size().unwrap();
        let offset0 = (offset % rs as u64) as usize;
        // Get WriteBack credit sufficient for nrecs full dirty records.  At
        // this point, we don't know if any of the records we're writing to are
        // already dirty.
        let nrecs = uio.nrecs(offset0, rs);
        let bb = FSValue::<RID>::extent_space(rs, nrecs);

        self.db.fswrite(self.tree, 1 + nrecs, 0, nrecs, bb,
        move |ds| async move {
            let dataset = Arc::new(ds);
            let inode = value.as_inode().unwrap();
            let filesize = inode.size;

            // Moving uio into the asynchronous domain is safe because
            // the async domain blocks on rx.wait().
            let datalen = uio.len();
            let sglist = unsafe {
                uio.into_chunks(offset0, rs,
                    |chunk| Arc::new(DivBufShared::from(chunk)))
            };

            let data_futs = sglist.into_iter()
                .enumerate()
                .map(|(i, dbs)| {
                    let ds3 = dataset.clone();
                    Fs::write_record(ino, rs as u64, offset, i, dbs, ds3)
                }).collect::<FuturesUnordered<_>>();
            let delta_len: i64 = data_futs.try_collect::<Vec<_>>().await?
                .into_iter()
                .sum();
            let new_size = cmp::max(filesize, offset + datalen as u64);
            {
                let inode = value.as_mut_inode().unwrap();
                inode.size = new_size;
                // It's a bug if inode.bytes would drop below zero, but it's
                // not worth panicking over.
                // TODO: use saturating_add_unsigned when it stabilizes.
                // https://github.com/rust-lang/rust/issues/87840
                inode.bytes = (inode.bytes as i64).saturating_add(delta_len)
                    as u64;
                let now = Timespec::now();
                inode.mtime = now;
                inode.ctime = now;
            }
            dataset.insert(inode_key, value).await?;
            Ok(datalen as u32)
        }).map_err(Error::into)
        .await
    }

    /// Subroutine of write.  Returns the amount by which the file's on-disk
    /// space changed.
    #[inline]
    async fn write_record(ino: u64, rs: u64, offset: u64, i: usize,
                    data: Arc<DivBufShared>,
                    dataset: Arc<ReadWriteFilesystem>)
        -> Result<i64>
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
            let r = dataset.remove(k).await?;
            let (dbs, old_len) = match r {
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
                    (r, 0)
                },
                Some(FSValue::InlineExtent(ile)) => {
                    let old_len = ile.len() as i64;
                    (ile.buf, old_len)
                },
                Some(FSValue::BlobExtent(be)) => {
                    (
                        Arc::from(dataset.remove_blob(be.rid).await?),
                        be.lsize.into()
                    )
                },
                x => panic!("Unexpected value {:?} for key {:?}", x, k)
            };
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
            let new_len = extent.len() as i64;
            let new_v = FSValue::InlineExtent(extent);
            dataset.insert(k, new_v).await
            .map(|_| new_len - old_len)
        } else {
            let new_len = data.len() as i64;
            let v = FSValue::InlineExtent(InlineExtent::new(data));
            dataset.insert(k, v).await
            .map(|ov| new_len - ov.map_or(0, |fsv| fsv.stat_space()))
        }
    }
}
