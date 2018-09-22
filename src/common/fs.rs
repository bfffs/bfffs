// vim: tw=80
//! Common VFS implementation

use atomic::*;
use common::*;
#[cfg(not(test))] use common::database::*;
#[cfg(test)] use common::database_mock::DatabaseMock as Database;
#[cfg(test)] use common::database_mock::ReadWriteFilesystem;
use common::database::TreeID;
use common::fs_tree::*;
use divbuf::{DivBufShared, DivBuf};
use futures::{
    Future,
    IntoFuture,
    Sink,
    Stream,
    future,
    sync::{mpsc, oneshot}
};
use libc;
use std::{
    cmp,
    ffi::{OsStr, OsString},
    mem,
    os::unix::ffi::OsStrExt,
    sync::Arc,
};
use time;
use tokio_io_pool;

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

/// File attributes, as returned by `getattr`
#[derive(Debug, PartialEq)]
pub struct Attr {
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
    /// File mode
    pub mode:       u16,
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

impl Fs {
    fn do_create<F, B>(&self, parent: u64, dtype: u8, flags: u64, name: &OsStr,
                 mode: u16, nlink: u64, f: F)
        -> Result<u64, i32>
        where F: FnOnce(&Arc<ReadWriteFilesystem>, u64) -> B + Send + 'static,
              B: Future<Item = (), Error = Error> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let ino = self.next_object();
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        let now = time::get_time();
        let inode = Inode {
            size: 0,
            nlink,
            flags,
            atime: now,
            mtime: now,
            ctime: now,
            birthtime: now,
            uid: 0,
            gid: 0,
            mode
        };
        let inode_value = FSValue::Inode(inode);

        let parent_dirent = Dirent {
            ino,
            dtype,
            name:   name.to_owned()
        };
        let parent_dirent_objkey = ObjKey::dir_entry(name);
        let parent_dirent_key = FSKey::new(parent, parent_dirent_objkey);
        let parent_dirent_value = FSValue::DirEntry(parent_dirent);

        self.handle.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                let extra_fut = f(&dataset, ino);
                dataset.insert(inode_key, inode_value).join3(
                    dataset.insert(parent_dirent_key, parent_dirent_value),
                    extra_fut
                ).map(move |_| tx.send(Ok(ino)).unwrap())
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
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
    pub fn create(&self, parent: u64, name: &OsStr, mode: u32)
        -> Result<u64, i32>
    {
        let f = |_: &Arc<ReadWriteFilesystem>, _| {
            Ok(()).into_future()
        };
        self.do_create(parent, libc::DT_REG, 0, name,
                       libc::S_IFREG | (mode as u16), 1, f)
    }

    pub fn getattr(&self, ino: u64) -> Result<Attr, i32> {
        let (tx, rx) = oneshot::channel();
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                let key = FSKey::new(ino, ObjKey::Inode);
                dataset.get(key)
                .then(move |r| {
                    match r {
                        Ok(Some(v)) => {
                            let inode = v.as_inode().unwrap();
                            let attr = Attr {
                                ino: ino,
                                size: inode.size,
                                blocks: 0,
                                atime: inode.atime,
                                mtime: inode.mtime,
                                ctime: inode.ctime,
                                birthtime: inode.birthtime,
                                mode: inode.mode,
                                nlink: inode.nlink,
                                uid: inode.uid,
                                gid: inode.gid,
                                rdev: 0,
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

    pub fn lookup(&self, parent: u64, name: &OsStr) -> Result<u64, i32>
    {
        let (tx, rx) = oneshot::channel();
        let objkey = ObjKey::dir_entry(name);
        let key = FSKey::new(parent, objkey);
        self.handle.spawn(
            self.db.fsread(self.tree, move |dataset| {
                dataset.get(key)
                .then(move |r| {
                    match r {
                        Ok(Some(v)) => {
                            tx.send(Ok(v.as_direntry().unwrap().ino))
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

    pub fn mkdir(&self, parent: u64, name: &OsStr, mode: u32)
        -> Result<u64, i32>
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

            dataset.insert(dot_dirent_key, dot_dirent_value).join3(
                dataset.insert(dotdot_dirent_key, dotdot_dirent_value),
                nlink_fut)
            .map(|_| ())
        };

        self.do_create(parent, libc::DT_DIR, 0, name,
                       libc::S_IFDIR | (mode as u16), nlink, f)
    }

    pub fn read(&self, ino: u64, offset: u64, size: usize)
        -> Result<SGList, i32>
    {
        let (tx, rx) = oneshot::channel();
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        self.handle.spawn(
            self.db.fsread(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                dataset.get(inode_key)
                .and_then(move |value| {
                    let fsize = value.unwrap().as_inode().unwrap().size;
                    let rs = RECORDSIZE as u64;
                    let nrecs = div_roundup(offset + size as u64, rs)
                        - (offset / rs);
                    let baseoffset = offset - (offset % rs);
                    let futs = (0..nrecs).map(|rec| {
                        let dataset2 = dataset.clone();
                        let offs = baseoffset + rec * rs;
                        if fsize <= offs {
                            let dbs = DivBufShared::from(Vec::new());
                            let db = dbs.try().unwrap();
                            Box::new(Ok(db).into_future())
                                as Box<Future<Item=DivBuf, Error=Error> + Send>
                        } else {
                            let k = FSKey::new(ino, ObjKey::Extent(offs));
                            let fut = dataset2.get(k)
                            .and_then(move |v| {
                                match v.unwrap().as_extent().unwrap() {
                                    Extent::Inline(ile) => {
                                        let buf = ile.buf.try().unwrap();
                                        Box::new(Ok(buf).into_future())
                                            as Box<Future<Item=DivBuf,
                                                          Error=Error> + Send>
                                    },
                                    Extent::Blob(be) => {
                                        let bfut = dataset2.get_blob(&be.rid)
                                            .map(|bdb| *bdb);
                                        Box::new(bfut)
                                            as Box<Future<Item=DivBuf,
                                                          Error=Error> + Send>
                                    }
                                }
                            }).map(move |mut db| {
                                if rec == 0 {
                                    // Trim the beginning
                                    db.split_to((offset - baseoffset) as usize);
                                }
                                if rec == nrecs - 1 {
                                    // Trim the end
                                    db.split_off(RECORDSIZE
                                        - ((nrecs * rs) as usize - size));
                                }
                                db
                            });
                            Box::new(fut)
                                as Box<Future<Item=DivBuf, Error=Error> + Send>
                        }
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

    pub fn rmdir(&self, parent: u64, name: &OsStr) -> Result<(), i32> {
        // Outline:
        // 1) Lookup the directory
        // 2) Check that the directory is empty
        // 3) range_delete its key range
        // 4) Remove the parent dir's dir_entry
        // 5) Decrement the parent dir's link count
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
                            assert_eq!(inode.nlink, 2,
                                concat!("Hard links to directories are ",
                                        "forbidden.  nlink={}"), inode.nlink);
                            Ok(true).into_future()
                        } else {
                            Ok(found_inode).into_future()
                        }
                    }).map(move |found_inode| {
                        if ! found_inode {
                            panic!(concat!("Inode {} not found, but parent ",
                                           "direntry {}:{:?} exists!"),
                                    ino, parent, owned_name);
                        }
                        ino
                    })
                }).and_then(move |ino| {
                    // 3) range_delete its key range
                    let ino_fut = ds2.range_delete(FSKey::obj_range(ino));

                    // 4) Remove the parent dir's dir_entry
                    let de_key = FSKey::new(parent, objkey);
                    let dirent_fut = ds2.remove(de_key);

                    // 5) Decrement the parent dir's link count
                    let parent_ino_key = FSKey::new(parent, ObjKey::Inode);
                    let nlink_fut = ds2.get(parent_ino_key)
                    .and_then(move |r| {
                        let mut value = r.unwrap();
                        value.as_mut_inode().unwrap().nlink -= 1;
                        ds2.insert(parent_ino_key, value)
                    });
                    ino_fut.join3(dirent_fut, nlink_fut)
                }).then(move |r| {
                    match r {
                        Ok(_) => tx.send(Ok(()).into()),
                        Err(e) => tx.send(Err(e.into()))
                    }.ok().expect("FS::rmdir: send failed");
                    Ok(()).into_future()
                })
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
        // 1) Lookup the file
        // 2) Delete the file's blob extents
        // 3) range_delete the file's key range
        // 4) Remove the parent dir's dir_entry
        let (tx, rx) = oneshot::channel();
        let owned_name = name.to_os_string();
        let dekey = ObjKey::dir_entry(&owned_name);
        self.handle.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                let dataset2 = dataset.clone();
                // 1) Lookup the file
                let key = FSKey::new(parent, dekey);
                dataset.get(key)
                .and_then(|r| {
                    match r {
                        Some(v) => {
                            let de = v.as_direntry().unwrap();
                            Ok(de.ino).into_future()
                        },
                        None => Err(Error::ENOENT).into_future()
                    }
                }).and_then(move |ino|  {
                    // 2) delete its blob extents
                    dataset2.range(FSKey::extent_range(ino))
                    .filter_map(move |(_k, v)| {
                        if let Extent::Blob(be) = v.as_extent().unwrap() {
                            Some(be.rid)
                        } else {
                            None
                        }
                    }).and_then(move |rid| {
                        dataset2.delete_blob(&rid)
                    }).collect()
                    .map(move |_| ino)
                }).and_then(move |ino|  {
                    // 3) range_delete its key range
                    let ino_fut = dataset.range_delete(FSKey::obj_range(ino));
                    // 4) Remove the parent dir's dir_entry
                    let de_key = FSKey::new(parent, dekey);
                    let dirent_fut = dataset.remove(de_key);

                    ino_fut.join(dirent_fut)
                }).then(move |r| {
                    match r {
                        Ok(_) => tx.send(Ok(()).into()),
                        Err(e) => tx.send(Err(e.into()))
                    }.ok().expect("FS::unlink: send failed");
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
                        Box::new(dataset4.get_blob(&be.rid))
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
                        &mut base[r].copy_from_slice(&overlay[..]);
                    } else {
                        &mut base[0..l as usize]
                            .copy_from_slice(&overlay[..]);
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
use common::tree::MinValue;
#[cfg(test)] use common::database_mock::ReadOnlyFilesystem;
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
            args.1.as_inode().unwrap().mode == libc::S_IFREG | 0o644
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

    assert_eq!(ino, fs.create(1, &filename, 0o644).unwrap());
}

// Pet kcov
#[test]
fn debug_attr() {
    let attr = Attr {
        ino: 1,
        size: 4096,
        blocks: 1,
        atime: time::Timespec::new(1, 2),
        mtime: time::Timespec::new(3, 4),
        ctime: time::Timespec::new(5, 6),
        birthtime: time::Timespec::new(7, 8),
        mode: libc::S_IFREG | 0o644,
        nlink: 1,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        flags: 0,
    };
    let s = format!("{:?}", attr);
    assert_eq!("Attr { ino: 1, size: 4096, blocks: 1, atime: Timespec { sec: 1, nsec: 2 }, mtime: Timespec { sec: 3, nsec: 4 }, ctime: Timespec { sec: 5, nsec: 6 }, birthtime: Timespec { sec: 7, nsec: 8 }, mode: 33188, nlink: 1, uid: 1000, gid: 1000, rdev: 0, flags: 0 }", s);
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
    ds.then().expect_delete_blob()
        .called_once()
        .with(passes(move |rid: &*const RID| blob_rid == unsafe {**rid}))
        .returning(|_| Box::new(Ok(()).into_future()));
    ds.then().expect_range_delete()
        .called_once()
        .with(FSKey::obj_range(ino))
        .returning(|_| Box::new(Ok(()).into_future()));
    ds.then().expect_remove()
        .called_once()
        .with(FSKey::new(parent_ino, ObjKey::dir_entry(&filename)))
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
                mode: libc::S_IFREG | 0o644,
            };
            Box::new(Ok(Some(FSValue::Inode(inode))).into_future())
        });
    let mut opt_ds = Some(ds);

    db.expect_fswrite()
        .called_once()
        .returning(move |_| opt_ds.take().unwrap());
    let fs = Fs::new(Arc::new(db), rt.handle().clone(), tree_id);
    let r = fs.unlink(1, &filename);
    assert_eq!(Ok(()), r);
}

}
