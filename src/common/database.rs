// vim: tw=80

//! The Database layer owns all of the Datasets
//!
//! Clients use the Database to obtain references to the Datasets.  The Database
//! also owns the Forest and manages Transactions.

use common::*;
use common::dataset::*;
use common::dml::DML;
use common::fs_tree::*;
use common::label::*;
use common::tree::{MinValue, TreeOnDisk};
use futures::{
    Future,
    IntoFuture,
    Sink,
    Stream,
    future,
    stream,
    sync::mpsc
};
use libc;
use std::collections::BTreeMap;
use std::{
    ffi::{OsString, OsStr},
    sync::{Arc, Mutex},
    time::{Duration, Instant}
};
use time;
use tokio::executor::Executor;
use tokio::timer;

#[cfg(not(test))] use common::idml::IDML;
#[cfg(test)] use common::idml_mock::IDMLMock as IDML;
#[cfg(not(test))] use common::tree::Tree;
#[cfg(test)] use common::tree_mock::TreeMock as Tree;

pub type ReadOnlyFilesystem = ReadOnlyDataset<FSKey, FSValue>;
pub type ReadWriteFilesystem = ReadWriteDataset<FSKey, FSValue>;

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

struct Syncer {
    tx: mpsc::Sender<()>
}

impl Syncer {
    fn kick(&self) -> impl Future<Item=(), Error=Error> {
        self.tx.clone()
            .send(())
            .map(|_| ())
            .map_err(|e| panic!("{:?}", e))
    }

    fn new<E: Executor + 'static>(handle: E, inner: Arc<Inner>) -> Self {
        let (tx, rx) = mpsc::channel(1);
        Syncer::run(handle, inner, rx);
        Syncer{tx: tx}
    }

    // Start a task that will sync the database at a fixed interval, but will
    // reset the timer if it gets a message on a channel.  While conceptually
    // simple, this is very hard to express in the world of Futures.
    fn run<E>(mut handle: E, inner: Arc<Inner>, rx: mpsc::Receiver<()>)
        where E: Executor + 'static
    {
        // The Future type used for the accumulator in the fold loop
        type LoopFut = Box<Future<Item=(Option<()>, mpsc::Receiver<()>),
                                  Error=()> + Send>;

        // Fixed 5-second duration
        let duration = Duration::new(5, 0);
        let initial = Box::new(
            rx.into_future()
              .map_err(|e| panic!("{:?}", e))
        ) as LoopFut;

        let taskfut = stream::repeat(())
        .fold(initial, move |rif, _| {
            let i2 = inner.clone();
            let wakeup_time = Instant::now() + duration;
            let delay = timer::Delay::new(wakeup_time);
            let delay_fut = delay
                .map(|_| None)
                .map_err(|e| panic!("{:?}", e));

            let rx_fut = rif
                .and_then(|(rvalue, remainder)| {
                    if rvalue.is_some() {
                        Ok(Some(remainder)).into_future()
                    } else {
                        // The Sender got dropped, which implies that
                        // the Database got dropped.  Error out of the
                        // loop.
                        Err(()).into_future()
                    }
                });

            delay_fut.select(rx_fut)
                .map_err(|_| ())
                .and_then(move |(remainder, other)| {
                    type LoopFutFut = Box<Future<Item=(LoopFut),
                                                 Error=()> + Send>;
                    if let Some(s) = remainder {
                        // We got kicked.  Restart the wait
                        let b = Box::new(
                            s.into_future()
                             .map_err(|e| panic!("{:?}", e))
                        ) as LoopFut;
                        Box::new(Ok(b).into_future()) as LoopFutFut
                    } else {
                        // Time's up.  Sync the database
                        Box::new(
                            Database::sync_transaction_priv(&i2)
                            .map_err(|e| panic!("{:?}", e))
                            .map(move |_| Box::new(
                                    other.map(|o| (None, o.unwrap()))
                                ) as LoopFut
                            )
                        ) as LoopFutFut
                    }
                })
        }).map(|_| ());
        handle.spawn(Box::new(taskfut)).unwrap();
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Label {
    forest:             TreeOnDisk
}

struct Inner {
    fs_trees: Mutex<BTreeMap<TreeID, Arc<ITree<FSKey, FSValue>>>>,
    forest: ITree<TreeID, TreeOnDisk>,
    idml: Arc<IDML>,
}

impl Inner {
    fn open_filesystem(inner: Arc<Inner>, tree_id: TreeID)
        -> Box<Future<Item=Arc<ITree<FSKey, FSValue>>, Error=Error> + Send>
    {
        if let Some(fs) = inner.fs_trees.lock().unwrap().get(&tree_id) {
            return Box::new(Ok(fs.clone()).into_future());
        }

        let idml2 = inner.idml.clone();
        let inner2 = inner.clone();
        let fut = inner.forest.get(tree_id)
            .map(move |tod| {
                let tree = Arc::new(ITree::open(idml2, tod.unwrap()).unwrap());
                inner2.fs_trees.lock().unwrap()
                    .insert(tree_id, tree.clone());
                tree
            });
        Box::new(fut)
    }

    fn rw_filesystem(inner: Arc<Inner>, tree_id: TreeID, txg: TxgT)
        -> impl Future<Item=ReadWriteFilesystem, Error=Error>
    {
        let idml2 = inner.idml.clone();
        Inner::open_filesystem(inner.clone(), tree_id)
            .map(move |fs| ReadWriteFilesystem::new(idml2, fs, txg))
    }

    /// Asynchronously write this `Database`'s label to its `IDML`
    fn write_label(&self, txg: TxgT)
        -> impl Future<Item=(), Error=Error>
    {
        let mut labeller = LabelWriter::new();
        let idml2 = self.idml.clone();
        Tree::flush(&self.forest, txg)
            .and_then(move |tod| {
                let label = Label { forest: tod };
                labeller.serialize(label).unwrap();
                idml2.write_label(labeller, txg)
            })
    }
}

pub struct Database {
    inner: Arc<Inner>,
    syncer: Syncer
}

impl Database {
    /// Construct a new `Database` from its `IDML`.
    pub fn create<E: Executor + 'static>(idml: Arc<IDML>, handle: E) -> Self {
        let forest = ITree::create(idml.clone());
        Database::new(idml, forest, handle)
    }

    /// Create a new, blank filesystem
    pub fn new_fs(&self) -> impl Future<Item=TreeID, Error=Error> {
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
                mode: libc::S_IFDIR | 0o755
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
        where E: Executor + 'static
    {
        let fs_trees = Mutex::new(BTreeMap::new());
        let inner = Arc::new(Inner{fs_trees, idml, forest});
        let syncer = Syncer::new(handle, inner.clone());
        Database{inner, syncer}
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
        where E: Executor + 'static
    {
        let l: Label = label_reader.deserialize().unwrap();
        let forest = Tree::open(idml.clone(), l.forest).unwrap();
        Database::new(idml, forest, handle)
    }

    fn ro_filesystem(&self, tree_id: TreeID)
        -> impl Future<Item=ReadOnlyFilesystem, Error=Error>
    {
        let idml2 = self.inner.idml.clone();
        Inner::open_filesystem(self.inner.clone(), tree_id)
            .map(|fs| ReadOnlyFilesystem::new(idml2, fs))
    }

    /// Finish the current transaction group and start a new one.
    pub fn sync_transaction(&self) -> impl Future<Item=(), Error=Error> {
        self.syncer.kick().join(Database::sync_transaction_priv(&self.inner))
            .map(|_| ())
    }

    fn sync_transaction_priv(inner: &Arc<Inner>)
        -> impl Future<Item=(), Error=Error>
    {
        // Outline:
        // 1) Flush the trees
        // 2) Sync the pool
        // 3) Write the label
        // 4) Sync the pool again
        // TODO: use two labels, so the pool will be recoverable even if power
        // is lost while writing a label.
        let inner2 = inner.clone();
        inner.idml.advance_transaction(move |txg| {
            let inner4 = inner2.clone();
            let idml2 = inner2.idml.clone();
            let idml3 = inner2.idml.clone();
            let fsfuts = {
                let guard = inner2.fs_trees.lock().unwrap();
                guard.iter()
                    .map(move |(tree_id, itree)| {
                        let inner5 = inner4.clone();
                        let tree_id2 = *tree_id;
                        itree.flush(txg)
                            .and_then(move |tod| {
                                inner5.forest.insert(tree_id2, tod, txg)
                            })
                    }).collect::<Vec<_>>()
            };
            future::join_all(fsfuts)
            .and_then(move |_| idml2.sync_all(txg))
            .and_then(move |_| inner2.write_label(txg))
            .and_then(move |_| idml3.sync_all(txg))
        })
    }

    /// Perform a read-write operation on a Filesystem
    ///
    /// All operations conducted by the supplied closure will be completed
    /// within the same Pool transaction group.  Thus, after a power failure and
    /// recovery, either all will have completed, or none will have.
    pub fn fswrite<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadWriteFilesystem) -> B,
              B: Future<Item = R, Error = Error>,
    {
        let inner2 = self.inner.clone();
        self.inner.idml.txg()
            .map_err(|_| Error::EPIPE)
            .and_then(move |txg| {
                Inner::rw_filesystem(inner2, tree_id, *txg)
                    .and_then(|ds| f(ds).into_future())
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

    #[test]
    fn sync_transaction() {
        let mut idml = IDML::new();
        let mut forest = Tree::new();

        let mut rt = current_thread::Runtime::new().unwrap();

        idml.expect_advance_transaction()
            .called_once()
            .returning(|_| TxgT::from(0));

        idml.expect_sync_all()
            .called_once()
            .with(TxgT::from(0))
            .returning(|_| Box::new(future::ok::<(), Error>(())));

        // forest.flush should be called inbetween the two sync_all()s, but
        // Simulacrum isn't able to verify the order of calls to different
        // objects
        forest.expect_flush()
            .called_once()
            .with(TxgT::from(0))
            .returning(|_| {
                let tod = TreeOnDisk::default();
                Box::new(future::ok::<TreeOnDisk, Error>(tod))
            });

        idml.then().expect_write_label()
            .called_once()
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        idml.expect_sync_all()
            .called_once()
            .with(TxgT::from(0))
            .returning(|_| Box::new(future::ok::<(), Error>(())));

        rt.block_on(future::lazy(|| {
            let task_executor = TaskExecutor::current();
            let db = Database::new(Arc::new(idml), forest, task_executor);
            db.sync_transaction()
        })).unwrap();
    }
}
