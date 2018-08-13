// vim: tw=80
//! Common VFS implementation

use atomic::*;
use common::Error;
use common::database::*;
use common::fs_tree::*;
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
    ffi::{OsStr, OsString},
    mem,
    os::unix::ffi::OsStrExt,
    sync::Arc,
};
use time;
use tokio_io_pool;

/// Generic Filesystem layer.
///
/// Bridges the synchronous with Tokio domains, and the system-independent with
/// system-dependent filesystem interfaces.
pub struct Fs {
    db: Arc<Database>,
    next_object: Atomic<u64>,
    // TODO: wrap Runtime in ARC so it can be shared by multiple filesystems
    runtime: tokio_io_pool::Runtime,
    tree: TreeID,
}

/// File attributes, as returned by `getattr`
#[derive(Debug)]
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
    pub fn new(database: Arc<Database>, mut runtime: tokio_io_pool::Runtime,
               tree: TreeID) -> Self
    {
        let last_key = runtime.block_on(
            database.fsread(tree, |dataset| {
                dataset.last_key()
            })
        ).unwrap();
        let next_object = Atomic::new(last_key.unwrap().object() + 1);

        Fs{db: database, next_object, runtime, tree}
    }

    fn next_object(&self) -> u64 {
        self.next_object.fetch_add(1, Ordering::Relaxed)
    }
}

impl Fs {
    pub fn getattr(&self, ino: u64) -> Result<Attr, i32> {
        let (tx, rx) = oneshot::channel();
        self.runtime.spawn(
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
        self.runtime.spawn(
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
        let (tx, rx) = oneshot::channel();
        let ino = self.next_object();
        let inode_key = FSKey::new(ino, ObjKey::Inode);
        let now = time::get_time();
        let inode = Inode {
            size: 0,
            nlink: 2,   // One for the parent dir, and one for "."
            flags: 0,
            atime: now,
            mtime: now,
            ctime: now,
            birthtime: now,
            uid: 0,
            gid: 0,
            mode: libc::S_IFDIR | (mode as u16)
        };
        let inode_value = FSValue::Inode(inode);

        let parent_dirent = Dirent {
            ino,
            dtype:  libc::DT_DIR,
            name:   name.to_owned()
        };
        let parent_dirent_objkey = ObjKey::dir_entry(name);
        let parent_dirent_key = FSKey::new(parent, parent_dirent_objkey);
        let parent_dirent_value = FSValue::DirEntry(parent_dirent);

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

        self.runtime.spawn(
            self.db.fswrite(self.tree, move |ds| {
                let dataset = Arc::new(ds);
                let dataset2 = dataset.clone();
                let nlink_fut = dataset.get(parent_inode_key)
                    .and_then(move |r| {
                        let mut value = r.unwrap();
                        value.as_mut_inode().unwrap().nlink += 1;
                        dataset2.insert(parent_inode_key, value)
                    });
                dataset.insert(inode_key, inode_value).join5(
                    dataset.insert(parent_dirent_key, parent_dirent_value),
                    dataset.insert(dot_dirent_key, dot_dirent_value),
                    dataset.insert(dotdot_dirent_key, dotdot_dirent_value),
                    nlink_fut
                ).map(move |_| tx.send(Ok(ino)).unwrap())
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
        self.runtime.spawn(
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

    pub fn statvfs(&self) -> libc::statvfs {
        let (tx, rx) = oneshot::channel::<libc::statvfs>();
        self.runtime.spawn(
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
}
