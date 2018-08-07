// vim: tw=80
//! Common VFS implementation

use common::database::*;
use common::fs_tree::*;
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
use nix::{Error, errno};
use std::{mem, sync::Arc};
use tokio_io_pool;

/// Generic Filesystem layer.
///
/// Bridges the synchronous with Tokio domains, and the system-independent with
/// system-dependent filesystem interfaces.
pub struct Fs {
    db: Arc<Database>,
    // TODO: wrap Runtime in ARC so it can be shared by multiple filesystems
    runtime: tokio_io_pool::Runtime,
    tree: TreeID,
}

impl Fs {
    pub fn new(database: Arc<Database>, runtime: tokio_io_pool::Runtime,
               tree: TreeID) -> Self
    {
        Fs{db: database, runtime, tree}
    }
}

impl Fs {
    pub fn getattr(&self, ino: u64) -> Result<Inode, Error> {
        let (tx, rx) = oneshot::channel();
        self.runtime.spawn(
            self.db.fsread(self.tree, move |dataset| {
                let key = FSKey::new(ino, ObjKey::Inode);
                dataset.get(key)
                .then(|r| {
                    match r {
                        Ok(Some(v)) => {
                            tx.send(Ok(v.as_inode().unwrap().clone()))
                        },
                        Ok(None) => {
                            tx.send(Err(Error::Sys(errno::Errno::ENOENT)))
                        },
                        Err(e) => {
                            tx.send(Err(e))
                        }
                    }.unwrap();
                    future::ok::<(), Error>(())
                })
            }).map_err(|e| panic!("{:?}", e))
        ).unwrap();
        rx.wait().unwrap()
    }

    // TODO: instead of the full size struct libc::dirent, use a variable size
    // structure in the mpsc channel
    pub fn readdir(&self, ino: u64, _fh: u64, offset: i64)
        -> impl Iterator<Item=Result<(libc::dirent, i64), i32>>
    {
        // Big enough to fill a 4KB page with full-size dirents
        let chansize: usize = 14;
        let dirent_size = mem::size_of::<libc::dirent>() as u16;

        let (tx, rx) = mpsc::channel(chansize);
        self.runtime.spawn(
            self.db.fsread(self.tree, move |_dataset| {
                if ino == 1 {
                    // Create a stream of directory entries.  Overkill for this
                    // stub, but it's similar to how a complete readdir
                    // implementation will work
                    let s = stream::unfold(offset, move |offs| {
                        if offs < 2 {
                            let (fut, next_offset) = if offs < 1 {
                                let mut dirent = libc::dirent {
                                    d_fileno: 9999,
                                    d_reclen: dirent_size,
                                    d_type: libc::DT_DIR,
                                    d_namlen: 3,
                                    d_name: unsafe{mem::zeroed()}
                                };
                                dirent.d_name[0] = '.' as i8;
                                dirent.d_name[1] = '.' as i8;
                                (Ok((dirent, 1)).into_future(), 1)
                            } else {
                                let mut dirent = libc::dirent {
                                    d_fileno: 1,
                                    d_reclen: dirent_size,
                                    d_type: libc::DT_DIR,
                                    d_namlen: 2,
                                    d_name: unsafe{mem::zeroed()}
                                };
                                dirent.d_name[0] = '.' as i8;
                                (Ok((dirent, 2)).into_future(), 2)
                            };
                            Some(fut.map(move |r| (r, next_offset)))
                        } else {
                            None
                        }
                    });
                    let fut = s.fold(tx, |tx, dirent|
                        tx.send(Ok(dirent))
                            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                    ).map(|_| ());
                    Box::new(fut) as Box<Future<Item=(), Error=Error> + Send>

                } else {
                    let fut = tx.send(Err(errno::Errno::ENOENT as i32))
                        .map(|_| ())
                        .map_err(|_| Error::Sys(errno::Errno::EPIPE));
                    Box::new(fut) as Box<Future<Item=(), Error=Error> + Send>
                }
            }).map_err(|e| panic!("{:?}", e))
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
