// vim: tw=80

use crate::{
    common::{
        *,
        cleaner::*,
        dataset::{ITree, ReadOnlyDataset, ReadWriteDataset},
        dml::DML,
        fs_tree::*,
        idml::*,
        label::*,
        property::*,
        tree::{Tree, TreeOnDisk}
    }
};
use futures::{
    Future,
    FutureExt,
    SinkExt,
    StreamExt,
    TryFutureExt,
    TryStreamExt,
    channel::{mpsc, oneshot},
    future,
    select,
};
use futures_locks::Mutex;
#[cfg(not(test))] use libc;
#[cfg(test)] use mockall::automock;
use std::collections::BTreeMap;
use std::{
    ffi::{OsString, OsStr},
    ops::Range,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
#[cfg(not(test))] use std::io;
use super::*;
#[cfg(not(test))] use time;
#[cfg(not(test))] use tokio::runtime;
use tokio::{
    runtime::Handle,
    task::JoinHandle,
    time::{Duration, Instant, delay_until},
};

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
    /// Tell the Syncer that we manually synced, and it can reset its timer
    Kick,
    /// Tell the Syncer to shut down, and wait for it to do so
    Shutdown,
}

struct Syncer {
    jh: JoinHandle<()>,
    tx: mpsc::Sender<SyncerMsg>
}

impl Syncer {
    fn kick(&self) -> impl Future<Output=Result<(), Error>> {
        let mut tx2 = self.tx.clone();
        async move {
            tx2.send(SyncerMsg::Kick)
            .map_err(Error::unhandled_error)
            .await
        }
    }

    fn new(handle: Handle, inner: Arc<Inner>) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let jh = Syncer::run(handle, inner, rx);
        Syncer{jh, tx}
    }

    // Start a task that will sync the database at a fixed interval, but will
    // reset the timer if it gets a message on a channel.
    fn run(handle: Handle, inner: Arc<Inner>, mut rx: mpsc::Receiver<SyncerMsg>)
        -> JoinHandle<()>
    {
        // Fixed 5-second duration
        let duration = Duration::new(5, 0);
        let taskfut = async move {
            loop {
                let wakeup_time = Instant::now() + duration;
                let mut delay_fut = delay_until(wakeup_time).fuse();
                select! {
                    _ = delay_fut => {
                        //Time's up.  Sync the database
                        Database::sync_transaction_priv(&inner)
                        .await
                        .unwrap();
                    },
                    sm = rx.select_next_some() => {
                        match sm {
                            SyncerMsg::Kick => {
                                // We got kicked.  Restart the wait
                            },
                            SyncerMsg::Shutdown => {
                                // Error out of the loop
                                break;
                            }
                        }
                    },
                    complete => break,
                };
            }
        };
        handle.spawn(taskfut)
    }

    async fn shutdown(mut self) {
        self.tx.send(SyncerMsg::Shutdown).await.unwrap();
        self.jh.await.unwrap();
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
        -> impl Future<Output=Result<Arc<ITree<FSKey, FSValue<RID>>>, Error>> + Send
    {
        let inner2 = inner.clone();
        inner.fs_trees.with(move |mut guard| {
            if let Some(fs) = guard.get(&tree_id) {
                future::ok(fs.clone()).boxed()
            } else {
                let fut = inner2.forest.get(tree_id)
                .map_ok(move |tod| {
                    let idml2 = inner2.idml.clone();
                    let tree = ITree::open(idml2, false, tod.unwrap());
                    let atree = Arc::new(tree);
                    guard.insert(tree_id, atree.clone());
                    atree
                });
                fut.boxed()
            }
        })
    }

    fn rw_filesystem(inner: &Arc<Inner>, tree_id: TreeID, txg: TxgT)
        -> impl Future<Output=Result<ReadWriteFilesystem, Error>>
    {
        let idml2 = inner.idml.clone();
        Inner::open_filesystem(&inner, tree_id)
            .map_ok(move |fs| ReadWriteFilesystem::new(idml2, fs, txg))
    }

    // The txg is a ref in test mode, but a RwlockWriteGuard in normal mode
    #[cfg_attr(test, allow(clippy::drop_ref))]
    fn fswrite<F, B, R>(inner: Arc<Self>, tree_id: TreeID, f: F)
        -> impl Future<Output=Result<R, Error>>
        where F: FnOnce(ReadWriteFilesystem) -> B,
              B: Future<Output=Result<R, Error>>,
    {
        inner.dirty.store(true, Ordering::Relaxed);
        inner.idml.txg()
        .then(move |txg| {
            Inner::rw_filesystem(&inner, tree_id, *txg)
                .and_then(|ds| f(ds))
                .map_ok(move |r| {
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
        -> impl Future<Output=Result<(), Error>>
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
#[cfg_attr(test, automock)]
impl Database {
    /// Foreground consistency check.  Prints any irregularities to stderr
    ///
    /// # Returns
    ///
    /// `true` on success, `false` on failure
    pub fn check(&self) -> impl Future<Output=Result<bool, Error>> {
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
        idml_fut.and_then(|passed| forest_fut.map_ok(move |r| passed & r))
    }

    fn check_forest(&self) -> impl Future<Output=Result<bool, Error>> {
        let inner2 = self.inner.clone();
        self.inner.forest.range(..)
        .try_fold(true, move |passed, (tree_id, _)| {
            Inner::open_filesystem(&inner2, tree_id)
            .and_then(move |tree| tree.check())
            .map_ok(move |r| r && passed)
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
    pub fn create(idml: Arc<IDML>, handle: Handle) -> Self
    {
        // Compression ratio is a total guess; it hasn't been measured yet.
        let forest = ITree::create(idml.clone(), true, 4.0, 2.0);
        Database::new(idml, forest, handle)
    }

    /// Dump a YAMLized representation of the given Tree to a plain
    /// `std::fs::File`.
    ///
    /// Must be called from the synchronous domain.
    #[cfg(not(test))]
    pub fn dump(&self, f: &mut dyn io::Write, tree: TreeID) -> Result<(), Error>
    {
        let mut rt = runtime::Builder::new()
            .basic_scheduler()
            .enable_io()
            .build()
            .unwrap();
        rt.block_on(async {
            Inner::open_filesystem(&self.inner, tree).await
        }).unwrap();
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
        -> impl Future<Output=Result<(Property, PropertySource), Error>> + Send
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
                future::ok((prop.clone(), *source)).boxed()
            } else {
                let objkey = ObjKey::Property(name);
                let key = FSKey::new(PROPERTY_OBJECT, objkey);
                let fut = Inner::open_filesystem(&inner2, tree_id)
                .and_then(move |fs| {
                    fs.get(key)
                }).map_ok(move |r| {
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
                fut.boxed()
            }
        })
    }

    /// Insert a property into the filesystem, but don't modify the propcache
    fn insert_prop(dataset: &ReadWriteFilesystem, prop: Property)
        -> impl Future<Output=Result<(), Error>>
    {
        let objkey = ObjKey::Property(prop.name());
        let key = FSKey::new(PROPERTY_OBJECT, objkey);
        let value = FSValue::Property(prop);
        dataset.insert(key, value)
        .map_ok(drop)
    }

    /// Create a new, blank filesystem
    ///
    /// Must be called from the tokio domain.
    pub fn new_fs(&self, props: Vec<Property>)
        -> impl Future<Output=Result<TreeID, Error>> + Send
    {
        self.inner.dirty.store(true, Ordering::Relaxed);
        let idml2 = self.inner.idml.clone();
        let inner2 = self.inner.clone();
        self.inner.fs_trees.with(move |mut guard| {
            let k = (0..=u32::max_value()).find(|i| {
                !guard.contains_key(&TreeID::Fs(*i))
            }).expect("Maximum number of filesystems reached");
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
                let props_fut = future::try_join_all(
                    props.iter().map(|prop| {
                        Database::insert_prop(&dataset, prop.clone())
                    }).collect::<Vec<_>>()
                );

                future::try_join4(
                    dataset.insert(inode_key, inode_value),
                    dataset.insert(dot_key, dot_value),
                    dataset.insert(dotdot_key, dotdot_value),
                    props_fut)
            }).map_ok(move |_| tree_id)
        })
    }

    /// Perform a read-only operation on a Filesystem
    #[cfg(not(test))]
    pub fn fsread<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Output=Result<R, Error>> + Send
        where F: FnOnce(ReadOnlyFilesystem) -> B + Send + 'static,
              B: Future<Output = Result<R, Error>> + Send + 'static,
              R: 'static,
    {
        self.ro_filesystem(tree_id)
            .and_then(|ds| f(ds))
    }

    /// See comments for `fswrite_inner`.
    #[cfg(test)]
    #[doc(hidden)]
    pub fn fsread_inner(&self, tree_id: TreeID)
        -> ReadOnlyDataset<FSKey, FSValue<RID>>
    {
        unimplemented!()
    }
    fn new(idml: Arc<IDML>, forest: ITree<TreeID, TreeOnDisk<RID>>,
           handle: Handle) -> Self
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
    pub fn open(idml: Arc<IDML>, handle: Handle, mut label_reader: LabelReader)
        -> Self
    {
        let l: Label = label_reader.deserialize().unwrap();
        let forest = Tree::<RID, IDML, TreeID, TreeOnDisk<RID>>::open(
            idml.clone(), true, l.forest);
        Database::new(idml, forest, handle)
    }

    fn ro_filesystem(&self, tree_id: TreeID)
        -> impl Future<Output=Result<ReadOnlyFilesystem, Error>>
    {
        let inner2 = self.inner.clone();
        let idml2 = self.inner.idml.clone();
        async move {
            Inner::open_filesystem(&inner2, tree_id)
            .map_ok(|fs| ReadOnlyFilesystem::new(idml2, fs))
            .await
        }
    }

    // TODO: Make prop an Option.  A None value will signify that the property
    // should be inherited.
    // Silence clippy: the borrow checker frowns upon its suggestion
    #[allow(clippy::needless_collect)]
    pub fn set_prop(&self, tree_id: TreeID, prop: Property)
        -> impl Future<Output=Result<(), Error>> + Send
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
            }).map(move |r| {
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
        })
    }

    /// Shutdown all background tasks and close the Database
    pub async fn shutdown(self) {
        future::join(self.syncer.shutdown(),
                     self.cleaner.shutdown())
        .await;
    }

    /// Finish the current transaction group and start a new one.
    pub fn sync_transaction(&self)
        -> impl Future<Output=Result<(), Error>> + Send
    {
        future::try_join(self.syncer.kick(),
                         Database::sync_transaction_priv(&self.inner))
            .map_ok(drop)
    }

    fn sync_transaction_priv(inner: &Arc<Inner>)
        -> impl Future<Output=Result<(), Error>>
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
            return future::ok(()).boxed();
        }
        let fut = inner.idml.advance_transaction(move |txg| {
            let inner3 = inner2.clone();
            let inner5 = inner2.clone();
            let idml2 = inner2.idml.clone();
            inner2.fs_trees.lock()
            .then(move |guard| {
                let fsfuts = guard.iter()
                .map(move |(_, itree)| {
                    itree.flush(txg)
                }).collect::<Vec<_>>();
                future::try_join_all(fsfuts)
                .and_then(move |_| {
                    let forest_futs = guard.iter()
                    .map(|(tree_id, itree)| {
                        let tod = itree.serialize().unwrap();
                        inner3.forest.insert(*tree_id, tod, txg)
                    }).collect::<Vec<_>>();
                    drop(guard);
                    future::try_join_all(forest_futs)
                    .map_ok(move |_| inner3)
                })
            }).and_then(move |inner3| {
                Tree::flush(&inner3.forest, txg)
            }).and_then(move |_| idml2.flush(0, txg).map_ok(move |_| idml2))
            .and_then(move |idml2| idml2.sync_all(txg).map_ok(move |_| idml2))
            .and_then(move |idml2| {
                let forest = inner2.forest.serialize().unwrap();
                let label = Label {forest};
                inner2.write_label(&label, 0, txg)
                .map_ok(|_| (idml2, label))
            }).and_then(move |(idml2, label)| {
                idml2.flush(1, txg).map_ok(move |_| (idml2, label))
            }).and_then(move |(idml2, label)| {
                // The only time we need to read the second label is if we lose
                // power while writing the first.  The fact that we reached this
                // point means that that won't happen, at least not until the
                // _next_ transaction sync.  So we don't need an additional
                // sync_all between idml2.flush(1, ...) and idml2.sync_all(...).
                idml2.sync_all(txg)
                .map_ok(move |_| (idml2, label))
            }).and_then(move |(idml2, label)| {
                inner5.write_label(&label, 1, txg)
                .map_ok(move |_| idml2)
            }).and_then(move |idml2| idml2.sync_all(txg))
        });
        fut.boxed()
    }

    /// Perform a read-write operation on a Filesystem
    ///
    /// All operations conducted by the supplied closure will be completed
    /// within the same Pool transaction group.  Thus, after a power failure and
    /// recovery, either all will have completed, or none will have.
    #[cfg(not(test))]
    pub fn fswrite<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Output=Result<R, Error>> + Send
        where F: FnOnce(ReadWriteFilesystem) -> B + Send + 'static,
              B: Future<Output=Result<R, Error>> + Send,
              R: 'static
    {
        Inner::fswrite(self.inner.clone(), tree_id, f)
    }

    /// Helper for MockDatabase::fswrite.
    ///
    /// Mockall can't mock fswrite.  Even though it can handle the closure, it
    /// requires that the closures's output type be named, which isn't feasible
    /// in this case.  Instead we'll let mockall mock this helper method, and
    /// manually write `MockDatabase::fswrite`.
    #[cfg(test)]
    #[doc(hidden)]
    pub fn fswrite_inner(&self, tree_id: TreeID)
        -> ReadWriteDataset<FSKey, FSValue<RID>>
    {
        unimplemented!()
    }
}

#[cfg(test)]
impl MockDatabase {
    pub fn fsread<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Output=Result<R, Error>> + Send
        where F: FnOnce(ReadOnlyFilesystem) -> B + Send + 'static,
              B: Future<Output=Result<R, Error>> + Send + 'static,
              R: 'static,
    {
        f(self.fsread_inner(tree_id))
    }

    pub fn fswrite<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Output=Result<R, Error>> + Send
        where F: FnOnce(ReadWriteFilesystem) -> B + Send + 'static,
              B: Future<Output=Result<R, Error>> + Send,
              R: 'static
    {
        f(self.fswrite_inner(tree_id))
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
    use mockall::{Sequence, predicate::*};

    // pet kcov
    #[test]
    fn debug() {
        let label = Label{forest: TreeOnDisk::default()};
        format!("{:?}", label);
    }

    #[test]
    fn shutdown() {
        let idml = IDML::default();
        let forest = Tree::default();

        let mut rt = basic_runtime();
        let handle = rt.handle().clone();

        rt.block_on(async {
            let db = Database::new(Arc::new(idml), forest, handle);
            db.shutdown().await
        });
    }

    #[test]
    fn sync_transaction() {
        let mut seq = Sequence::new();
        let mut idml = IDML::default();
        let mut forest = Tree::default();

        let mut rt = basic_runtime();
        let handle = rt.handle().clone();

        idml.expect_advance_transaction_inner()
            .once()
            .returning(|| TxgT::from(0));

        forest.expect_flush()
            .once()
            .in_sequence(&mut seq)
            .with(eq(TxgT::from(0)))
            .returning(|_| {
                Box::pin(future::ok::<(), Error>(()))
            });
        idml.expect_flush()
            .once()
            .in_sequence(&mut seq)
            .with(eq(0), eq(TxgT::from(0)))
            .returning(|_, _| Box::pin(future::ok::<(), Error>(())));
        idml.expect_sync_all()
            .once()
            .in_sequence(&mut seq)
            .with(eq(TxgT::from(0)))
            .returning(|_| Box::pin(future::ok::<(), Error>(())));
        forest.expect_serialize()
            .once()
            .in_sequence(&mut seq)
            .returning(|| {
                Ok(TreeOnDisk::default())
            });
        idml.expect_write_label()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

        idml.expect_flush()
            .once()
            .in_sequence(&mut seq)
            .with(eq(1), eq(TxgT::from(0)))
            .returning(|_, _| Box::pin(future::ok::<(), Error>(())));
        idml.expect_sync_all()
            .once()
            .in_sequence(&mut seq)
            .with(eq(TxgT::from(0)))
            .returning(|_| Box::pin(future::ok::<(), Error>(())));
        idml.expect_write_label()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

        idml.expect_sync_all()
            .once()
            .in_sequence(&mut seq)
            .with(eq(TxgT::from(0)))
            .returning(|_| Box::pin(future::ok::<(), Error>(())));

        rt.block_on(async {
            let db = Database::new(Arc::new(idml), forest, handle);
            db.sync_transaction()
            .and_then(move |_| {
                // Syncing a 2nd time should be a no-op, since the database
                // isn't dirty.
                db.sync_transaction()
            }).await
        }).unwrap();
    }

    /// Syncing a transaction that isn't dirty should be a no-op
    #[test]
    fn sync_transaction_empty() {
        let idml = IDML::default();
        let forest = Tree::default();

        let mut rt = basic_runtime();
        let handle = rt.handle().clone();

        rt.block_on(async {
            let db = Database::new(Arc::new(idml), forest, handle);
            db.inner.dirty.store(false, Ordering::Relaxed);
            db.sync_transaction().await
        }).unwrap();
    }
}

mod syncer_msg {
    use super::super::*;

    //pet kcov
    #[test]
    fn debug() {
        let sm = SyncerMsg::Shutdown;
        format!("{:?}", sm);
    }
}
}
