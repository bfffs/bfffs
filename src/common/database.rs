// vim: tw=80

//! The Database layer owns all of the Datasets
//!
//! Clients use the Database to obtain references to the Datasets.  The Database
//! also owns the Forest and manages Transactions.

use crate::common::*;
use crate::common::cleaner::*;
use crate::common::dataset::*;
use crate::common::dml::DML;
use crate::common::fs_tree::*;
use crate::common::label::*;
use crate::common::tree::{MinValue, TreeOnDisk};
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
use std::collections::BTreeMap;
use std::{
    ffi::{OsString, OsStr},
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
        Mutex
    },
    time::{Duration, Instant}
};
use time;
use tokio::executor::Executor;
use tokio::timer;

#[cfg(not(test))] use crate::common::idml::IDML;
#[cfg(test)] use crate::common::idml_mock::IDMLMock as IDML;
#[cfg(not(test))] use crate::common::tree::Tree;
#[cfg(test)] use crate::common::tree_mock::TreeMock as Tree;

pub type ReadOnlyFilesystem = ReadOnlyDataset<FSKey, FSValue<RID>>;
pub type ReadWriteFilesystem = ReadWriteDataset<FSKey, FSValue<RID>>;

/// Keys into the Forest
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum TreeID {
    /// A filesystem, snapshot, or clone
    Fs(u32)
}

impl MinValue for TreeID {
    fn min_value() -> Self {
        TreeID::Fs(u32::min_value())
    }
}

#[derive(Debug)]
enum SyncerMsg {
    Kick,
    Shutdown(oneshot::Sender<()>),
}

struct Syncer {
    tx: mpsc::Sender<SyncerMsg>
}

impl Syncer {
    fn kick(&self) -> impl Future<Item=(), Error=Error> {
        self.tx.clone()
            .send(SyncerMsg::Kick)
            .map(drop)
            .map_err(|e| panic!("{:?}", e))
    }

    fn new<E: Executor + 'static>(handle: E, inner: Arc<Inner>) -> Self {
        let (tx, rx) = mpsc::channel(1);
        Syncer::run(handle, inner, rx);
        Syncer{tx}
    }

    // Start a task that will sync the database at a fixed interval, but will
    // reset the timer if it gets a message on a channel.  While conceptually
    // simple, this is very hard to express in the world of Futures.
    fn run<E>(mut handle: E, inner: Arc<Inner>, rx: mpsc::Receiver<SyncerMsg>)
        where E: Executor + 'static
    {
        // The Future type used for the accumulator in the fold loop
        type LoopFut = Box<Future<Item=(Option<SyncerMsg>,
                                        mpsc::Receiver<SyncerMsg>),
                                  Error=()> + Send>;

        // Fixed 5-second duration
        let duration = Duration::new(5, 0);
        let initial = Box::new(
            rx.into_future()
              .map_err(|e| panic!("{:?}", e))
        ) as LoopFut;

        let taskfut = stream::repeat(())
        .fold(initial, move |rx_fut: LoopFut, _| {
            let i2 = inner.clone();
            let wakeup_time = Instant::now() + duration;
            let delay = timer::Delay::new(wakeup_time);
            let delay_fut = delay
                .map_err(|e| panic!("{:?}", e));

            delay_fut.select2(rx_fut)
                .map_err(drop)
                .and_then(move |r| {
                    type LoopFutFut = Box<Future<Item=LoopFut,
                                                 Error=()> + Send>;
                    match r {
                        future::Either::A((_, rx_fut)) => {
                            //Time's up.  Sync the database
                            Box::new(
                                Database::sync_transaction_priv(&i2)
                                .map_err(|e| panic!("{:?}", e))
                                .map(move |_| rx_fut)
                                //)
                            ) as LoopFutFut
                        }, future::Either::B(((Some(sm), remainder), _)) => {
                            match sm {
                                SyncerMsg::Kick => {
                                    // We got kicked.  Restart the wait
                                    let b = Box::new(
                                        remainder.into_future()
                                         .map_err(|e| panic!("{:?}", e))
                                    ) as LoopFut;
                                    Box::new(Ok(b).into_future()) as LoopFutFut
                                }, SyncerMsg::Shutdown(tx) => {
                                    // Error out of the loop
                                    tx.send(()).unwrap();
                                    Box::new(Err(()).into_future())
                                        as LoopFutFut
                                }
                            }
                        }, future::Either::B(((None, _), _)) => {
                            // Sender got dropped, which implies that the
                            // Database got dropped  Error out of the loop
                            Box::new(Err(()).into_future()) as LoopFutFut
                        }
                    }
                })
        }).map(drop);
        handle.spawn(Box::new(taskfut)).unwrap();
    }

    fn shutdown(&self) -> impl Future<Item=(), Error=()> + Send {
        let (tx, rx) = oneshot::channel();
        self.tx.clone()
        .send(SyncerMsg::Shutdown(tx))
        .then(|r| {
            match r {
                Ok(_) => Box::new(rx.map_err(|e| panic!("{:?}", e)))
                    as Box<Future<Item=_, Error=_> + Send>,
                Err(_) => {
                    // Syncer must already be shutdown
                    Box::new(Ok(()).into_future())
                    as Box<Future<Item=_, Error=_> + Send>
                }
            }
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Label {
    forest: TreeOnDisk
}

struct Inner {
    /// Has any part of the database been modified since the last transaction
    /// sync?
    // NB: This is likely to be highly contended and very slow.  Better to
    // replace it with a per-cpu counter.
    dirty: AtomicBool,
    fs_trees: Mutex<BTreeMap<TreeID, Arc<ITree<FSKey, FSValue<RID>>>>>,
    forest: ITree<TreeID, TreeOnDisk>,
    idml: Arc<IDML>,
}

impl Inner {
    fn open_filesystem(inner: &Arc<Inner>, tree_id: TreeID)
        -> Box<Future<Item=Arc<ITree<FSKey, FSValue<RID>>>, Error=Error> + Send>
    {
        if let Some(fs) = inner.fs_trees.lock().unwrap().get(&tree_id) {
            return Box::new(Ok(fs.clone()).into_future());
        }

        let idml2 = inner.idml.clone();
        let inner2 = inner.clone();
        let fut = inner.forest.get(tree_id)
            .map(move |tod| {
                let tree = Arc::new(ITree::open(idml2, &tod.unwrap()).unwrap());
                inner2.fs_trees.lock().unwrap()
                    .insert(tree_id, tree.clone());
                tree
            });
        Box::new(fut)
    }

    fn rw_filesystem(inner: &Arc<Inner>, tree_id: TreeID, txg: TxgT)
        -> impl Future<Item=ReadWriteFilesystem, Error=Error>
    {
        let idml2 = inner.idml.clone();
        Inner::open_filesystem(&inner, tree_id)
            .map(move |fs| ReadWriteFilesystem::new(idml2, fs, txg))
    }

    /// Asynchronously write this `Database`'s label to its `IDML`
    ///
    /// # Parameters
    ///
    /// - `label`:      A Database::Label to serialize
    /// - `label_idx`:  0-based index of the label to write
    /// - `txg`:        The current transaction group of the database
    fn write_label(&self, label: &Label, label_idx: u32, txg: TxgT)
        -> impl Future<Item=(), Error=Error>
    {
        let mut labeller = LabelWriter::new(label_idx);
        labeller.serialize(label).unwrap();
        self.idml.write_label(labeller, txg)
    }
}

pub struct Database {
    cleaner: Cleaner,
    inner: Arc<Inner>,
    syncer: Syncer
}

impl Database {
    /// Foreground consistency check.  Prints any irregularities to stderr
    ///
    /// # Returns
    ///
    /// `true` on success, `false` on failure
    pub fn check(&self) -> impl Future<Item=bool, Error=Error> {
        // Should check that:
        // * RAID parity is consistent and checksums match
        //   - For each entry in the RIDT, read it from disk in "verify mode",
        //     which bypasses cache, reads from all RAID members, verifies
        //     RAID parity, but does not decompress.
        //   - Somehow walk through the DTrees doing the same.
        // * RIDT and AllocT are exact inverses
        //   - For each entry in the Alloct, check that the corresponding entry
        //     in the RIDT is an inverse.
        //   - For each entry in the RIDT, check that an entry exists in the
        //     AllocT.
        // * RIDT's refcounts are correct.
        //   - Walk through the FSTrees building a new RIDT, and compare it to
        //     the real RIDT.  Use some kind of external database.
        // * Spacemaps match actual usage
        //   - For each zone, calculate the actual usage by comparing entries
        //     from the Alloct and by a TXG-limited scan through the DTrees.
        //     Compare that to the FreeSpaceMap
        // * All Trees' are consistent and satisfy their invariants.
        // * All files' link counts are correct
        let idml_fut = self.inner.idml.check();
        let forest_fut = self.check_forest();
        idml_fut.and_then(|passed| forest_fut.map(move |r| passed & r))
    }

    fn check_forest(&self) -> impl Future<Item=bool, Error=Error> {
        let inner2 = self.inner.clone();
        self.inner.forest.range(..)
            .fold(true, move |passed, (tree_id, _)| {
                Inner::open_filesystem(&inner2, tree_id)
                .and_then(move |tree| tree.check())
                .map(move |r| r && passed)
            })
    }

    /// Clean zones immediately.  Does not wait for the result to be polled!
    ///
    /// The returned `Receiver` will deliver notification when cleaning is
    /// complete.  However, there is no requirement to poll it.  The client may
    /// drop it, and cleaning will continue in the background.
    pub fn clean(&self) -> oneshot::Receiver<()> {
        self.inner.dirty.store(true, Ordering::Relaxed);
        self.cleaner.clean()
    }

    /// Construct a new `Database` from its `IDML`.
    pub fn create<E>(idml: Arc<IDML>, handle: E) -> Self
        where E: Clone + Executor + 'static
    {
        let forest = ITree::create(idml.clone());
        Database::new(idml, forest, handle)
    }

    /// Dump a YAMLized representation of the given Tree to a plain
    /// `std::fs::File`.
    ///
    /// Must be called from the synchronous domain.
    pub fn dump(&self, f: &mut io::Write, tree: TreeID) {
        self.inner.fs_trees.lock().unwrap()
        .get(&tree).unwrap()
        .dump(f).unwrap();
    }

    /// Create a new, blank filesystem
    pub fn new_fs(&self) -> impl Future<Item=TreeID, Error=Error> {
        self.inner.dirty.store(true, Ordering::Relaxed);
        let mut guard = self.inner.fs_trees.lock().unwrap();
        let k = (0..=u32::max_value()).filter(|i| {
            !guard.contains_key(&TreeID::Fs(*i))
        }).nth(0).expect("Maximum number of filesystems reached");
        let tree_id = TreeID::Fs(k);
        let fs = Arc::new(ITree::create(self.inner.idml.clone()));
        guard.insert(tree_id, fs);

        // Create the filesystem's root directory
        self.fswrite(tree_id, move |dataset| {
            let ino = 1;    // FUSE requires root dir to have inode 1
            let inode_key = FSKey::new(ino, ObjKey::Inode);
            let now = time::get_time();
            let inode = Inode {
                size: 0,
                nlink: 1,   // for "."
                flags: 0,
                atime: now,
                mtime: now,
                ctime: now,
                birthtime: now,
                uid: 0,
                gid: 0,
                file_type: FileType::Dir,
                perm: 0o755
            };
            let inode_value = FSValue::Inode(inode);

            // Create the /. and /.. directory entries
            let dot_dirent = Dirent {
                ino,
                dtype: libc::DT_DIR,
                name:  OsString::from(".")
            };
            let dot_objkey = ObjKey::dir_entry(OsStr::new("."));
            let dot_key = FSKey::new(ino, dot_objkey);
            let dot_value = FSValue::DirEntry(dot_dirent);

            let dotdot_dirent = Dirent {
                ino: 1,     // The VFS replaces this
                dtype: libc::DT_DIR,
                name:  OsString::from("..")
            };
            let dotdot_objkey = ObjKey::dir_entry(OsStr::new(".."));
            let dotdot_key = FSKey::new(ino, dotdot_objkey);
            let dotdot_value = FSValue::DirEntry(dotdot_dirent);

            dataset.insert(inode_key, inode_value)
                .join3(dataset.insert(dot_key, dot_value),
                       dataset.insert(dotdot_key, dotdot_value))
        }).map(move |_| tree_id)
    }

    /// Perform a read-only operation on a Filesystem
    pub fn fsread<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadOnlyFilesystem) -> B + 'static,
              B: IntoFuture<Item = R, Error = Error> + 'static,
              R: 'static
    {
        self.ro_filesystem(tree_id)
            .and_then(|ds| f(ds).into_future())
    }

    fn new<E>(idml: Arc<IDML>, forest: ITree<TreeID, TreeOnDisk>, handle: E)
        -> Self
        where E: Clone + Executor + 'static
    {
        let cleaner = Cleaner::new(handle.clone(), idml.clone(), None);
        let dirty = AtomicBool::new(true);
        let fs_trees = Mutex::new(BTreeMap::new());
        let inner = Arc::new(Inner{dirty, fs_trees, idml, forest});
        let syncer = Syncer::new(handle, inner.clone());
        Database{cleaner, inner, syncer}
    }

    /// Open an existing `Database`
    ///
    /// # Parameters
    ///
    /// * `idml`:           An already-opened `IDML`
    /// * `label_reader`:   A `LabelReader` that has already consumed all labels
    ///                     prior to this layer.
    pub fn open<E>(idml: Arc<IDML>, handle: E, mut label_reader: LabelReader)
        -> Self
        where E: Clone + Executor + 'static
    {
        let l: Label = label_reader.deserialize().unwrap();
        let forest = Tree::open(idml.clone(), &l.forest).unwrap();
        Database::new(idml, forest, handle)
    }

    fn ro_filesystem(&self, tree_id: TreeID)
        -> impl Future<Item=ReadOnlyFilesystem, Error=Error>
    {
        let idml2 = self.inner.idml.clone();
        Inner::open_filesystem(&self.inner, tree_id)
            .map(|fs| ReadOnlyFilesystem::new(idml2, fs))
    }

    /// Shutdown all background tasks
    pub fn shutdown(&mut self) -> impl Future<Item=(), Error=()> + Send
    {
        let idml2 = self.inner.idml.clone();
        self.syncer.shutdown()
        .join(self.cleaner.shutdown())
        .map(move |_| idml2.shutdown())
    }

    /// Finish the current transaction group and start a new one.
    pub fn sync_transaction(&self) -> impl Future<Item=(), Error=Error> {
        self.syncer.kick().join(Database::sync_transaction_priv(&self.inner))
            .map(drop)
    }

    fn sync_transaction_priv(inner: &Arc<Inner>)
        -> impl Future<Item=(), Error=Error>
    {
        // Outline:
        // 1) Flush the trees
        // 2) Sync the pool, so the label will be accurate.
        // 3) Write the label
        // 4) Sync the pool again, to commit the first label
        // 5) Write the second label
        // 6) Sync the pool again, in case we're about to physically pull the
        //    disk or power off.
        let inner2 = inner.clone();
        if !inner.dirty.swap(false, Ordering::Relaxed) {
            return Box::new(Ok(()).into_future())
                as Box<Future<Item=(), Error=Error> + Send>;
        }
        let fut = inner.idml.advance_transaction(move |txg| {
            let inner3 = inner2.clone();
            let inner5 = inner2.clone();
            let idml2 = inner2.idml.clone();
            let fsfuts = {
                let guard = inner2.fs_trees.lock().unwrap();
                guard.iter()
                .map(move |(_, itree)| {
                    itree.flush(txg)
                }).collect::<Vec<_>>()
            };
            future::join_all(fsfuts)
            .and_then(move |_| {
                 let forest_futs = inner3.fs_trees.lock().unwrap().iter()
                .map(|(tree_id, itree)| {
                    let tod = itree.serialize().unwrap();
                    inner3.forest.insert(*tree_id, tod, txg)
                }).collect::<Vec<_>>();
                future::join_all(forest_futs)
                .map(move |_| inner3)
            }).and_then(move |inner3| {
                Tree::flush(&inner3.forest, txg)
            }).and_then(move |_| idml2.flush(0, txg).map(move |_| idml2))
            .and_then(move |idml2| idml2.sync_all(txg).map(move |_| idml2))
            .and_then(move |idml2| {
                let forest = inner2.forest.serialize().unwrap();
                let label = Label {forest};
                inner2.write_label(&label, 0, txg)
                .map(|_| (idml2, label))
            }).and_then(move |(idml2, label)| {
                idml2.flush(1, txg).map(move |_| (idml2, label))
            }).and_then(move |(idml2, label)| {
                // The only time we need to read the second label is if we lose
                // power while writing the first.  The fact that we reached this
                // point means that that won't happen, at least not until the
                // _next_ transaction sync.  So we don't need an additional
                // sync_all between idml2.flush(1, ...) and idml2.sync_all(...).
                idml2.sync_all(txg)
                .map(move |_| (idml2, label))
            }).and_then(move |(idml2, label)| {
                inner5.write_label(&label, 1, txg)
                .map(move |_| idml2)
            }).and_then(move |idml2| idml2.sync_all(txg))
        });
        Box::new(fut) as Box<Future<Item=(), Error=Error> + Send>
    }

    /// Perform a read-write operation on a Filesystem
    ///
    /// All operations conducted by the supplied closure will be completed
    /// within the same Pool transaction group.  Thus, after a power failure and
    /// recovery, either all will have completed, or none will have.
    // IDMLMock::txg has a slightly different signature than IDML::txg
    #[cfg_attr(all(feature = "cargo-clippy", test), allow(clippy::drop_ref))]
    pub fn fswrite<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadWriteFilesystem) -> B,
              B: Future<Item = R, Error = Error>,
    {
        self.inner.dirty.store(true, Ordering::Relaxed);
        let inner2 = self.inner.clone();
        self.inner.idml.txg()
            .map_err(|_| Error::EPIPE)
            .and_then(move |txg| {
                Inner::rw_filesystem(&inner2, tree_id, *txg)
                    .and_then(|ds| f(ds).into_future())
                    .map(move |r| {
                        drop(txg);
                        r
                    })
            })
    }
}


// LCOV_EXCL_START
#[cfg(test)]
#[cfg(feature = "mocks")]
mod t {
    use super::*;
    use futures::future;
    use tokio::{
        executor::current_thread::TaskExecutor,
        runtime::current_thread
    };

    // pet kcov
    #[test]
    fn debug() {
        let label = Label{forest: TreeOnDisk::default()};
        format!("{:?}", label);
    }

    #[test]
    fn shutdown() {
        let mut idml = IDML::default();
        idml.expect_shutdown()
            .called_once()
            .returning(drop);
        let forest = Tree::new();

        let mut rt = current_thread::Runtime::new().unwrap();

        rt.block_on(future::lazy(|| {
            let task_executor = TaskExecutor::current();
            let mut db = Database::new(Arc::new(idml), forest, task_executor);
            db.shutdown()
        })).unwrap();
    }

    /// shutdown should be idempotent
    #[test]
    fn shutdown_twice() {
        let mut idml = IDML::default();
        idml.expect_shutdown()
            .called_times(2)
            .returning(drop);
        let forest = Tree::new();

        let mut rt = current_thread::Runtime::new().unwrap();

        rt.block_on(future::lazy(|| {
            let task_executor = TaskExecutor::current();
            let mut db = Database::new(Arc::new(idml), forest, task_executor);
            db.shutdown()
            .and_then(move |_| db.shutdown())
        })).unwrap();
    }

    #[test]
    fn sync_transaction() {
        let mut idml = IDML::default();
        let mut forest = Tree::new();

        let mut rt = current_thread::Runtime::new().unwrap();

        idml.expect_advance_transaction()
            .called_once()
            .returning(|_| TxgT::from(0));

        // forest.flush should be called before IDML::sync_all, but
        // Simulacrum isn't able to verify the order of calls to different
        // objects
        forest.expect_flush()
            .called_once()
            .with(TxgT::from(0))
            .returning(|_| {
                Box::new(future::ok::<(), Error>(()))
            });
        forest.expect_serialize()
            .called_once()
            .returning(|_| {
                Ok(TreeOnDisk::default())
            });

        idml.expect_flush()
            .called_once()
            .with((0, TxgT::from(0)))
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        idml.then().expect_sync_all()
            .called_once()
            .with(TxgT::from(0))
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        idml.then().expect_write_label()
            .called_once()
            .returning(|_| Box::new(future::ok::<(), Error>(())));

        idml.expect_flush()
            .called_once()
            .with((1, TxgT::from(0)))
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        idml.then().expect_sync_all()
            .called_once()
            .with(TxgT::from(0))
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        idml.then().expect_write_label()
            .called_once()
            .returning(|_| Box::new(future::ok::<(), Error>(())));

        idml.then().expect_sync_all()
            .called_once()
            .with(TxgT::from(0))
            .returning(|_| Box::new(future::ok::<(), Error>(())));

        rt.block_on(future::lazy(|| {
            let task_executor = TaskExecutor::current();
            let db = Database::new(Arc::new(idml), forest, task_executor);
            db.sync_transaction()
            .and_then(move |_| {
                // Syncing a 2nd time should be a no-op, since the database
                // isn't dirty.
                db.sync_transaction()
            })
        })).unwrap();
    }

    /// Syncing a transaction that isn't dirty should be a no-op
    #[test]
    fn sync_transaction_empty() {
        let idml = IDML::default();
        let forest = Tree::new();

        let mut rt = current_thread::Runtime::new().unwrap();

        rt.block_on(future::lazy(|| {
            let task_executor = TaskExecutor::current();
            let db = Database::new(Arc::new(idml), forest, task_executor);
            db.inner.dirty.store(false, Ordering::Relaxed);
            db.sync_transaction()
        })).unwrap();
    }
}
