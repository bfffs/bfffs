// vim: tw=80

use crate::{
    *,
    common::{
        *,
        cleaner::*,
        dataset::{ITree, ReadWriteDataset},
        dml::DML,
        fs_tree::*,
        idml::*,
        label::*,
        property::*,
        tree::{Tree, TreeOnDisk}
    }
};
#[cfg(not(test))] use crate::common::dataset::ReadOnlyDataset;
use futures::{
    Future,
    IntoFuture,
    Sink,
    Stream,
    future,
    stream,
    sync::{mpsc, oneshot}
};
use futures_locks::Mutex;
#[cfg(not(test))] use libc;
use std::collections::BTreeMap;
use std::{
    io,
    ops::Range,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant}
};
#[cfg(not(test))] use std::ffi::{OsString, OsStr};
use super::*;
#[cfg(not(test))] use time;
use tokio::{
    executor::Executor,
    runtime::current_thread
};
use tokio::timer;

#[cfg(not(test))]
pub type ReadOnlyFilesystem = ReadOnlyDataset<FSKey, FSValue<RID>>;
pub type ReadWriteFilesystem = ReadWriteDataset<FSKey, FSValue<RID>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
struct PropCacheKey {
    prop: PropertyName,
    tree: TreeID
}

impl PropCacheKey {
    fn new(prop: PropertyName, tree: TreeID) -> Self {
        PropCacheKey{prop, tree}
    }

    /// Construct a range that encompasses the named property for every dataset
    fn range(name: PropertyName) -> Range<Self> {
        let start = PropCacheKey::new(name, TreeID::Fs(0));
        let end = PropCacheKey::new(name.next(), TreeID::Fs(0));
        start..end
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
            .map_err(Error::unhandled_error)
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
        type LoopFut = Box<dyn Future<Item=(Option<SyncerMsg>,
                                            mpsc::Receiver<SyncerMsg>),
                                      Error=()> + Send>;

        // Fixed 5-second duration
        let duration = Duration::new(5, 0);
        let initial = Box::new(
            rx.into_future()
              .map_err(Error::unhandled)
        ) as LoopFut;

        let taskfut = stream::repeat(())
        .fold(initial, move |rx_fut: LoopFut, _| {
            let i2 = inner.clone();
            let wakeup_time = Instant::now() + duration;
            let delay = timer::Delay::new(wakeup_time);
            let delay_fut = delay
                .map_err(Error::unhandled);

            delay_fut.select2(rx_fut)
                .map_err(drop)
                .and_then(move |r| {
                    type LoopFutFut = Box<dyn Future<Item=LoopFut,
                                                     Error=()> + Send>;
                    match r {
                        future::Either::A((_, rx_fut)) => {
                            //Time's up.  Sync the database
                            Box::new(
                                Database::sync_transaction_priv(&i2)
                                .map_err(Error::unhandled)
                                .map(move |_| rx_fut)
                            ) as LoopFutFut
                        }, future::Either::B(((Some(sm), remainder), _)) => {
                            match sm {
                                SyncerMsg::Kick => {
                                    // We got kicked.  Restart the wait
                                    let b = Box::new(
                                        remainder.into_future()
                                         .map_err(Error::unhandled)
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
                Ok(_) => boxfut!(rx.map_err(Error::unhandled)),
                Err(_) => {
                    // Syncer must already be shutdown
                    boxfut!(Ok(()).into_future())
                }
            }
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Label {
    forest: TreeOnDisk<RID>
}

struct Inner {
    /// Has any part of the database been modified since the last transaction
    /// sync?
    // NB: This is likely to be highly contended and very slow.  Better to
    // replace it with a per-cpu counter.
    dirty: AtomicBool,
    fs_trees: Mutex<BTreeMap<TreeID, Arc<ITree<FSKey, FSValue<RID>>>>>,
    forest: ITree<TreeID, TreeOnDisk<RID>>,
    idml: Arc<IDML>,
    propcache: Mutex<BTreeMap<PropCacheKey, (Property, PropertySource)>>,
}

impl Inner {
    fn new(idml: Arc<IDML>, forest: ITree<TreeID, TreeOnDisk<RID>>) -> Self
    {
        let dirty = AtomicBool::new(true);
        let fs_trees = Mutex::new(BTreeMap::new());
        let propcache = Mutex::new(BTreeMap::new());
        Inner{dirty, fs_trees, idml, forest, propcache}
    }

    // Must be called from within a Tokio executor context
    fn open_filesystem(inner: &Arc<Inner>, tree_id: TreeID)
        -> impl Future<Item=Arc<ITree<FSKey, FSValue<RID>>>, Error=Error> + Send
    {
        let inner2 = inner.clone();
        inner.fs_trees.with(move |mut guard| {
            if let Some(fs) = guard.get(&tree_id) {
                boxfut!(Ok(fs.clone()).into_future())
            } else {
                let fut = inner2.forest.get(tree_id)
                .map(move |tod| {
                    let idml2 = inner2.idml.clone();
                    let tree = ITree::open(idml2, false, tod.unwrap());
                    let atree = Arc::new(tree);
                    guard.insert(tree_id, atree.clone());
                    atree
                });
                boxfut!(fut)
            }
        }).unwrap()
    }

    #[cfg(not(test))]
    fn rw_filesystem(inner: &Arc<Inner>, tree_id: TreeID, txg: TxgT)
        -> impl Future<Item=ReadWriteFilesystem, Error=Error>
    {
        let idml2 = inner.idml.clone();
        Inner::open_filesystem(&inner, tree_id)
            .map(move |fs| ReadWriteFilesystem::new(idml2, fs, txg))
    }

    #[cfg(not(test))]
    fn fswrite<F, B, R>(inner: Arc<Self>, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadWriteFilesystem) -> B,
              B: Future<Item = R, Error = Error>,
    {
        inner.dirty.store(true, Ordering::Relaxed);
        inner.idml.txg()
        .map_err(|_| Error::EPIPE)
        .and_then(move |txg| {
            Inner::rw_filesystem(&inner, tree_id, *txg)
                .and_then(|ds| f(ds).into_future())
                .map(move |r| {
                    drop(txg);
                    r
                })
        })
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

// Some of these methods have no unit tests.  Their test coverage is provided
// instead by integration tests.
#[cfg_attr(test, allow(unused))]
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
        // Compression ratio is a total guess; it hasn't been measured yet.
        let forest = ITree::create(idml.clone(), true, 4.0, 2.0);
        Database::new(idml, forest, handle)
    }

    /// Dump a YAMLized representation of the given Tree to a plain
    /// `std::fs::File`.
    ///
    /// Must be called from the synchronous domain.
    pub fn dump(&self, f: &mut io::Write, tree: TreeID) -> Result<(), Error> {
        let mut rt = current_thread::Runtime::new().unwrap();
        rt.block_on(future::lazy(|| {
            Inner::open_filesystem(&self.inner, tree)
        })).unwrap();
        self.inner.fs_trees.try_lock()
        .map_err(|_| Error::EDEADLK)
        .map(|guard| {
            guard.get(&tree).unwrap()
            .dump(f).unwrap();
        })
    }

    /// Get the value of the `name` property for the dataset identified by
    /// `tree`.
    pub fn get_prop(&self, tree_id: TreeID, name: PropertyName)
        -> impl Future<Item=(Property, PropertySource), Error=Error>
    {
        // Outline:
        // 1) Look for the property in the propcache
        // 2) If not present, open the dataset's tree and look for it there.
        // 3) If not present, open that dataset's parent and recurse
        // 4) If present nowhere, get the default value.
        // 5) Add it to the propcache.
        let inner2 = self.inner.clone();
        self.inner.propcache.with(move |mut guard| {
            let key = PropCacheKey::new(name, tree_id);
            if let Some((prop, source)) = guard.get(&key) {
                boxfut!(Ok((prop.clone(), *source)).into_future())
            } else {
                let objkey = ObjKey::Property(name);
                let key = FSKey::new(PROPERTY_OBJECT, objkey);
                let fut = Inner::open_filesystem(&inner2, tree_id)
                .and_then(move |fs| {
                    fs.get(key)
                }).map(move |r| {
                    let (prop, source) = if let Some(v) = r {
                        match v {
                            FSValue::Property(p) => (p, PropertySource::Local),
                            _ => panic!("Unexpected value {:?} for key {:?}",
                                        v, key)
                        }
                    } else {
                        // No value found; use the default
                        // TODO: look through parent datasets
                        (Property::default_value(name), PropertySource::Default)
                    };
                    let cache_key = PropCacheKey::new(name, tree_id);
                    guard.insert(cache_key, (prop.clone(), source));
                    (prop, source)
                });
                boxfut!(fut)
            }
        }).unwrap()
    }

    /// Insert a property into the filesystem, but don't modify the propcache
    fn insert_prop(dataset: &ReadWriteFilesystem, prop: Property)
        -> impl Future<Item=(), Error=Error>
    {
        let objkey = ObjKey::Property(prop.name());
        let key = FSKey::new(PROPERTY_OBJECT, objkey);
        let value = FSValue::Property(prop);
        dataset.insert(key, value)
        .map(drop)
    }

    /// Create a new, blank filesystem
    ///
    /// Must be called from the tokio domain.
    #[cfg(not(test))]
    pub fn new_fs(&self, props: Vec<Property>)
        -> impl Future<Item=TreeID, Error=Error>
    {
        self.inner.dirty.store(true, Ordering::Relaxed);
        let idml2 = self.inner.idml.clone();
        let inner2 = self.inner.clone();
        self.inner.fs_trees.with(move |mut guard| {
            let k = (0..=u32::max_value()).filter(|i| {
                !guard.contains_key(&TreeID::Fs(*i))
            }).nth(0).expect("Maximum number of filesystems reached");
            let tree_id: TreeID = TreeID::Fs(k);
            // The FS tree's compressibility varies greatly, especially based on
            // whether the write pattern is sequential or random.  5.98x is the
            // lower value for random access.  We'll use that rather than the
            // upper value, to keep cache usage lower.
            let fs = Arc::new(ITree::create(idml2, false, 9.00, 1.61));
            guard.insert(tree_id, fs);
            drop(guard);

            // Create the filesystem's root directory
            Inner::fswrite(inner2, tree_id, move |dataset| {
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

                // Set initial properties
                let props_fut = future::join_all(
                    props.iter().map(|prop| {
                        Database::insert_prop(&dataset, prop.clone())
                    }).collect::<Vec<_>>()
                );

                dataset.insert(inode_key, inode_value)
                    .join4(dataset.insert(dot_key, dot_value),
                           dataset.insert(dotdot_key, dotdot_value),
                           props_fut)
            }).map(move |_| tree_id)
        }).unwrap()
    }

    /// Perform a read-only operation on a Filesystem
    #[cfg(not(test))]
    pub fn fsread<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadOnlyFilesystem) -> B + 'static,
              B: IntoFuture<Item = R, Error = Error> + 'static,
              R: 'static
    {
        self.ro_filesystem(tree_id)
            .and_then(|ds| f(ds).into_future())
    }

    fn new<E>(idml: Arc<IDML>, forest: ITree<TreeID, TreeOnDisk<RID>>,
              handle: E) -> Self
        where E: Clone + Executor + 'static
    {
        let cleaner = Cleaner::new(handle.clone(), idml.clone(), None);
        let inner = Arc::new(Inner::new(idml, forest));
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
        let forest = Tree::open(idml.clone(), true, l.forest);
        Database::new(idml, forest, handle)
    }

    #[cfg(not(test))]
    fn ro_filesystem(&self, tree_id: TreeID)
        -> impl Future<Item=ReadOnlyFilesystem, Error=Error>
    {
        let inner2 = self.inner.clone();
        let idml2 = self.inner.idml.clone();
        future::lazy(move || {
            Inner::open_filesystem(&inner2, tree_id)
                .map(|fs| ReadOnlyFilesystem::new(idml2, fs))
        })
    }

    // TODO: Make prop an Option.  A None value will signify that the property
    // should be inherited.
    #[cfg(not(test))]
    pub fn set_prop(&self, tree_id: TreeID, prop: Property)
        -> impl Future<Item=(), Error=Error>
    {
        // Outline:
        // 1) Open the dataset's tree and set the property there.
        // 2) Invalidate that property from the propcache.  Since it's hard to
        //    tell which datasets might be inherited from this one, just
        //    invalidate all cached values for this property.
        // 3) Insert the new value into the propcache.
        let inner2 = self.inner.clone();
        self.inner.propcache.with(move |mut guard| {
            let name = prop.name();
            let prop2 = prop.clone();
            Inner::fswrite(inner2, tree_id, move |dataset| {
                Database::insert_prop(&dataset, prop)
            }).then(move |r| {
                // BTreeMap sadly doesn't have a range_delete method.
                // https://github.com/rust-lang/rust/issues/42849
                let keys = guard.range(PropCacheKey::range(name))
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
                for k in keys.into_iter() {
                    guard.remove(&k);
                }

                let key = PropCacheKey::new(name, tree_id);
                guard.insert(key, (prop2, PropertySource::Local));
                r
            })
        }).unwrap()
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
            return boxfut!(Ok(()).into_future());
        }
        let fut = inner.idml.advance_transaction(move |txg| {
            let inner3 = inner2.clone();
            let inner5 = inner2.clone();
            let idml2 = inner2.idml.clone();
            inner2.fs_trees.lock()
            .map_err(|_| Error::EPIPE)
            .and_then(move |guard| {
                let fsfuts = guard.iter()
                .map(move |(_, itree)| {
                    itree.flush(txg)
                }).collect::<Vec<_>>();
                future::join_all(fsfuts)
                .and_then(move |_| {
                    let forest_futs = guard.iter()
                    .map(|(tree_id, itree)| {
                        let tod = itree.serialize().unwrap();
                        inner3.forest.insert(*tree_id, tod, txg)
                    }).collect::<Vec<_>>();
                    drop(guard);
                    future::join_all(forest_futs)
                    .map(move |_| inner3)
                })
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
        boxfut!(fut)
    }

    /// Perform a read-write operation on a Filesystem
    ///
    /// All operations conducted by the supplied closure will be completed
    /// within the same Pool transaction group.  Thus, after a power failure and
    /// recovery, either all will have completed, or none will have.
    #[cfg(not(test))]
    pub fn fswrite<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadWriteFilesystem) -> B,
              B: Future<Item = R, Error = Error>,
    {
        Inner::fswrite(self.inner.clone(), tree_id, f)
    }
}


// LCOV_EXCL_START
#[cfg(test)]
mod t {
mod prop_cache_key {
    use pretty_assertions::assert_eq;
    use super::super::*;

    #[test]
    fn range() {
        let atime_range = PropCacheKey::range(PropertyName::Atime);
        let recsize_range = PropCacheKey::range(PropertyName::RecordSize);
        assert_eq!(atime_range.end, recsize_range.start);
    }
}

mod database {
    use super::super::*;
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
        let forest = Tree::default();

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
        let forest = Tree::default();

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
        let mut forest = Tree::default();

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
        let forest = Tree::default();

        let mut rt = current_thread::Runtime::new().unwrap();

        rt.block_on(future::lazy(|| {
            let task_executor = TaskExecutor::current();
            let db = Database::new(Arc::new(idml), forest, task_executor);
            db.inner.dirty.store(false, Ordering::Relaxed);
            db.sync_transaction()
        })).unwrap();
    }
}

mod syncer_msg {
    use super::super::*;

    //pet kcov
    #[test]
    fn debug() {
        let (tx, _rx) = oneshot::channel();
        let sm = SyncerMsg::Shutdown(tx);
        format!("{:?}", sm);
    }
}
}
