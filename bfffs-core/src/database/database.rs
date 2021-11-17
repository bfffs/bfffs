// vim: tw=80

use crate::{
    cleaner::*,
    dataset::{ITree, ReadOnlyDataset, ReadWriteDataset},
    dml::DML,
    fs_tree::*,
    idml::*,
    label::*,
    property::*,
    tree::{Tree, TreeOnDisk},
    types::*,
    writeback::Credit
};
use futures::{
    Future,
    FutureExt,
    SinkExt,
    Stream,
    StreamExt,
    TryFutureExt,
    TryStreamExt,
    channel::{mpsc, oneshot},
    future,
    select,
    stream::{self, FuturesUnordered},
};
use futures_locks::{Mutex, RwLock};
#[cfg(test)] use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::{
    ffi::{OsString, OsStr},
    io,
    ops::Range,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use super::TreeID;
use tokio::{
    task::JoinHandle,
    time::{Duration, Instant, sleep_until},
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

    fn new(inner: Arc<Inner>) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let jh = Syncer::run(inner, rx);
        Syncer{jh, tx}
    }

    // Start a task that will sync the database at a fixed interval, but will
    // reset the timer if it gets a message on a channel.
    fn run(inner: Arc<Inner>, mut rx: mpsc::Receiver<SyncerMsg>)
        -> JoinHandle<()>
    {
        // Fixed 5 second sync duration
        let sync_duration = Duration::new(5, 0);
        // Fixed 0.1 second flush duration
        let flush_duration = Duration::new(0, 100_000_000);
        let taskfut = async move {
            let mut sync_time = Instant::now() + sync_duration;
            loop {
                let wakeup_time = Instant::now() + flush_duration;
                let mut delay_fut = Box::pin(sleep_until(wakeup_time).fuse());
                select! {
                    _ = delay_fut => {
                        let now = Instant::now();
                        if now > sync_time {
                            //Time's up.  Sync the database
                            Database::sync_transaction_priv(&inner)
                            .await
                            .unwrap();
                            sync_time = Instant::now() + sync_duration;
                        } else {
                            // Time's up.  Flush the database
                            Database::flush(&inner).await
                            .unwrap();
                        }
                    },
                    sm = rx.select_next_some() => {
                        match sm {
                            SyncerMsg::Kick => {
                                // We got kicked.  Restart the wait
                                sync_time = Instant::now() + sync_duration;
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
        tokio::spawn(taskfut)
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
    fs_trees: RwLock<BTreeMap<TreeID, Arc<ITree<FSKey, FSValue<RID>>>>>,
    forest: Arc<ITree<TreeID, TreeOnDisk<RID>>>,
    idml: Arc<IDML>,
    propcache: Mutex<BTreeMap<PropCacheKey, (Property, PropertySource)>>,
}

impl Inner {
    fn new(idml: Arc<IDML>, forest: ITree<TreeID, TreeOnDisk<RID>>) -> Self
    {
        let dirty = AtomicBool::new(true);
        let fs_trees = RwLock::new(BTreeMap::new());
        let propcache = Mutex::new(BTreeMap::new());
        Inner{dirty, fs_trees, idml, forest: Arc::new(forest), propcache}
    }

    fn new_filesystem(
        inner: &Arc<Inner>,
        tree_id: TreeID,
        tod: TreeOnDisk<RID>)
    -> impl Future<Output=Result<Arc<ITree<FSKey, FSValue<RID>>>, Error>> + Send
    {
        let idml2 = inner.idml.clone();
        let tree = ITree::<FSKey, FSValue<RID>>::open(idml2, false, tod);
        let atree = Arc::new(tree);
        let atree2 = atree.clone();
        inner.fs_trees.with_write(move |mut wguard| {
            future::ready(wguard.insert(tree_id, atree2))
        }).map(|_| Ok(atree))
    }

    // Must be called from within a Tokio executor context
    fn open_filesystem(inner: &Arc<Inner>, tree_id: TreeID)
        -> impl Future<Output=Result<Arc<ITree<FSKey, FSValue<RID>>>, Error>> + Send
    {
        let inner2 = inner.clone();
        async move {
            let rguard = inner2.fs_trees.read().await;
            match rguard.get(&tree_id) {
                Some(fs) => Ok(fs.clone()),
                None => {
                    drop(rguard);
                    match inner2.forest.get(tree_id).await? {
                        Some(tod) => {
                            Inner::new_filesystem(&inner2, tree_id, tod).await
                        },
                        None => Err(Error::ENOENT)
                    }
                }
            }
        }
    }

    // The txg is a ref in test mode, but a RwlockWriteGuard in normal mode
    #[cfg_attr(test, allow(clippy::drop_ref))]
    fn fswrite<F, B, R>(
        inner: Arc<Self>,
        tree_id: TreeID,
        ninsert: usize,
        nrange_delete: usize,
        nremove: usize,
        blob_bytes: usize,
        f: F,
    ) -> impl Future<Output=Result<R, Error>>
        where F: FnOnce(ReadWriteFilesystem) -> B,
              B: Future<Output=Result<R, Error>>,
    {
        inner.dirty.store(true, Ordering::Relaxed);
        Inner::open_filesystem(&inner, tree_id)
        .and_then(move |itree| async move {
            let cr = itree.credit_requirements();
            let credit = inner.idml.borrow_credit(
                ninsert * cr.insert +
                nrange_delete * cr.range_delete +
                nremove * cr.remove +
                blob_bytes
            ).await;
            let idml2 = inner.idml.clone();
            let txg = inner.idml.txg().await;
            let ds = ReadWriteFilesystem::new(idml2, itree, *txg, credit);
            let r = f(ds).await;
            drop(txg);
            r
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
    /// Get the maximum size of bytes in the cache
    pub fn cache_size(&self) -> usize {
        self.inner.idml.cache_size()
    }

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
        .try_fold(true, move |passed, (tree_id, tod)| {
            Inner::new_filesystem(&inner2, tree_id, tod)
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
    ///
    /// Must be constructed from the context of a Tokio runtime.
    pub fn create(idml: Arc<IDML>) -> Self
    {
        // Compression ratio is a total guess; it hasn't been measured yet.
        let forest = ITree::create(idml.clone(), true, 4.0, 2.0);
        Database::new(idml, forest)
    }

    /// Dump a YAMLized representation of the given Tree in text format.
    pub async fn dump(&self, f: &mut dyn io::Write, tree: TreeID)
        -> Result<(), Error>
    {
        Inner::open_filesystem(&self.inner, tree).await.unwrap();
        match self.inner.fs_trees.try_read() {
            Err(_) => Err(Error::EDEADLK),
            Ok(guard) => guard.get(&tree).unwrap().dump(f).await
        }
    }

    /// Flush the database's dirty data to disk.
    ///
    /// Does not sync a transaction.  Does not rewrite the labels.
    fn flush(inner: &Arc<Inner>)
        -> impl Future<Output=Result<(), Error>> + Send
    {
        if !inner.dirty.load(Ordering::Relaxed) {
            return future::ok(()).boxed();
        }
        let inner2 = inner.clone();
        let idml2 = inner.idml.clone();
        async move {
            let txg_guard = inner2.idml.txg().await;
            let txg = *txg_guard;
            let guard = inner2.fs_trees.read().await;
            stream::iter(guard.iter().map(Ok))
                .try_fold((), move |_acc, (_tree_id, itree)|
                          itree.clone().flush(txg)
                ).await?;
            idml2.clone().flush(None, txg).await
        }.boxed()
    }

    /// Get the value of the `name` property for the dataset identified by
    /// `tree`.
    // Note: it returns a Boxed future rather than `impl Future` to prevent
    // type-length limit errors in the compiler.
    pub fn get_prop(&self, tree_id: TreeID, name: PropertyName)
        -> Pin<Box<dyn Future<Output=Result<(Property, PropertySource), Error>>
            + Send>>
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
        }).boxed()
    }

    /// Insert a property into the filesystem, but don't modify the propcache
    fn insert_prop(
        dataset: &ReadWriteFilesystem,
        prop: Property,
    ) -> impl Future<Output=Result<(), Error>>
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
        let inner3 = self.inner.clone();
        // The FS tree's compressibility varies greatly, especially based on
        // whether the write pattern is sequential or random.  5.98x is the
        // lower value for random access.  We'll use that rather than the
        // upper value, to keep cache usage lower.
        let fs = Arc::new(ITree::create(idml2, false, 9.00, 1.61));
        let ninsert = props.len() + 3;
        self.inner.fs_trees.with_write(move |mut guard| async move {
            let k = (0..=u32::max_value()).find(|i| {
                !guard.contains_key(&TreeID::Fs(*i))
            }).expect("Maximum number of filesystems reached");
            let tree_id: TreeID = TreeID::Fs(k);
            guard.insert(tree_id, fs);
            drop(guard);

            // Create the filesystem's root directory
            Inner::fswrite(inner3, tree_id, ninsert, 0, 0, 0, move |dataset| {
                let ino = 1;    // FUSE requires root dir to have inode 1
                let inode_key = FSKey::new(ino, ObjKey::Inode);
                let now = Timespec::now();

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
                let futs = props.iter()
                .map(|prop|
                     Database::insert_prop(&dataset, prop.clone()).boxed()
                ).collect::<FuturesUnordered<_>>();
                futs.push(
                    dataset.insert(inode_key, inode_value).map_ok(drop)
                    .boxed()
                );
                futs.push(
                    dataset.insert(dot_key, dot_value).map_ok(drop)
                    .boxed()
                );
                futs.push(
                    dataset.insert(dotdot_key, dotdot_value).map_ok(drop)
                    .boxed()
                );
                futs.try_collect::<Vec<_>>()
            }).await
            .map(|_| tree_id)
        })
    }

    fn fsread_real<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Output=Result<R, Error>> + Send
        where F: FnOnce(ReadOnlyFilesystem) -> B + Send + 'static,
              B: Future<Output = Result<R, Error>> + Send + 'static,
              R: 'static,
    {
        self.ro_filesystem(tree_id)
            .and_then(f)
    }

    /// Perform a read-only operation on a Filesystem
    #[cfg(not(test))]
    pub fn fsread<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Output=Result<R, Error>> + Send
        where F: FnOnce(ReadOnlyFilesystem) -> B + Send + 'static,
              B: Future<Output = Result<R, Error>> + Send + 'static,
              R: 'static,
    {
        self.fsread_real(tree_id, f)
    }

    /// Perform a streaming read-only operation on a Filesystem
    pub fn fsreads<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Stream<Item=Result<R, Error>> + Send
        where F: FnOnce(ReadOnlyFilesystem) -> B + Send + 'static,
              B: Stream<Item = Result<R, Error>> + Send + 'static,
              R: 'static,
    {
        self.ro_filesystem(tree_id)
            .map_ok(f)
            .try_flatten_stream()
    }

    /// See comments for `fswrite_inner`.
    // LCOV_EXCL_START
    #[cfg(test)]
    #[doc(hidden)]
    pub fn fsread_inner(&self, tree_id: TreeID)
        -> ReadOnlyDataset<FSKey, FSValue<RID>>
    {
        unimplemented!()
    }
    // LCOV_EXCL_STOP

    fn new(idml: Arc<IDML>, forest: ITree<TreeID, TreeOnDisk<RID>>) -> Self
    {
        let cleaner = Cleaner::new(idml.clone(), None);
        let inner = Arc::new(Inner::new(idml, forest));
        let syncer = Syncer::new(inner.clone());
        Database{cleaner, inner, syncer}
    }

    /// Open an existing `Database`
    ///
    /// # Parameters
    ///
    /// * `idml`:           An already-opened `IDML`
    /// * `label_reader`:   A `LabelReader` that has already consumed all labels
    ///                     prior to this layer.
    pub fn open(idml: Arc<IDML>, mut label_reader: LabelReader) -> Self
    {
        let l: Label = label_reader.deserialize().unwrap();
        let forest = Tree::<RID, IDML, TreeID, TreeOnDisk<RID>>::open(
            idml.clone(), true, l.forest);
        Database::new(idml, forest)
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
            Inner::fswrite(inner2, tree_id, 1, 0, 0, 0, move |dataset| {
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
        if !inner.dirty.swap(false, Ordering::Relaxed) {
            return future::ok(()).boxed();
        }
        let inner2 = inner.clone();
        let fut = inner.idml.advance_transaction(move |txg| async move {
            let guard = inner2.fs_trees.read().await;
            guard.iter()
                .map(move |(_, itree)| {
                    itree.clone().flush(txg)
                }).collect::<FuturesUnordered<_>>()
                .try_collect::<Vec<_>>().await?;
            let forest_futs = guard.iter()
                .map(|(tree_id, itree)| {
                    let tod = itree.serialize().unwrap();
                    inner2.forest.clone()
                    .insert(*tree_id, tod, txg, Credit::null())
                }).collect::<FuturesUnordered<_>>();
            drop(guard);
            forest_futs.try_collect::<Vec<_>>().await?;
            inner2.forest.clone().flush(txg).await?;
            inner2.idml.clone().flush(Some(0), txg).await?;
            inner2.idml.sync_all(txg).await?;
            let forest = inner2.forest.serialize().unwrap();
            let label = Label {forest};
            inner2.write_label(&label, 0, txg).await?;
            inner2.idml.clone().flush(Some(1), txg).await?;
            // The only time we need to read the second label is if we lose
            // power while writing the first.  The fact that we reached this
            // point means that that won't happen, at least not until the
            // _next_ transaction sync.  So we don't need an additional
            // sync_all between inner2.idml.clone().flush(1, ...) and
            // inner2.idml.sync_all(...).
            inner2.idml.sync_all(txg).await?;
            inner2.write_label(&label, 1, txg).await?;
            inner2.idml.sync_all(txg).await
        });
        fut.boxed()
    }

    /// Perform a read-write operation on a Filesystem
    ///
    /// All operations conducted by the supplied closure will be completed
    /// within the same Pool transaction group.  Thus, after a power failure and
    /// recovery, either all will have completed, or none will have.
    ///
    /// # Arguments
    ///
    /// `tree_id` -         Identifies the Tree to write to
    /// `ninsert`-          Maximum number of [`ReadWriteFilesystem::insert`]
    ///                     operations that will be called.
    /// `nrange_delete`-    Maximum number of
    ///                     [`ReadWriteFilesystem::range_delete`] operations
    ///                     that will be called.
    /// `nremove`-          Maximum number of [`ReadWriteFilesystem::remove`]
    ///                     operations that will be called.
    /// `blob_bytes` -      Maximum number of bytes that will be written into
    ///                     the tree as blobs.
    #[cfg(not(test))]
    pub fn fswrite<F, B, R>(
        &self,
        tree_id: TreeID,
        ninsert: usize,
        nrange_delete: usize,
        nremove: usize,
        blob_bytes: usize,
        f: F
    ) -> impl Future<Output=Result<R, Error>> + Send
        where F: FnOnce(ReadWriteFilesystem) -> B + Send + 'static,
              B: Future<Output=Result<R, Error>> + Send,
              R: 'static
    {
        Inner::fswrite(self.inner.clone(), tree_id, ninsert, nrange_delete,
            nremove, blob_bytes, f)
    }

    /// Helper for MockDatabase::fswrite.
    ///
    /// Mockall can't mock fswrite.  Even though it can handle the closure, it
    /// requires that the closures's output type be named, which isn't feasible
    /// in this case.  Instead we'll let mockall mock this helper method, and
    /// manually write `MockDatabase::fswrite`.
    // LCOV_EXCL_START
    #[cfg(test)]
    #[doc(hidden)]
    pub fn fswrite_inner(&self, tree_id: TreeID)
        -> ReadWriteDataset<FSKey, FSValue<RID>>
    {
        unimplemented!()
    }
    // LCOV_EXCL_STOP

    /// Get the maximum size of the writeback cache
    pub fn writeback_size(&self) -> usize {
        self.inner.idml.writeback_size()
    }
}

// LCOV_EXCL_START
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

    pub fn fswrite<F, B, R>(
        &self,
        tree_id: TreeID,
        _ninsert: usize,
        _nrange_delete: usize,
        _nremove: usize,
        _blob_bytes: usize,
        f: F
    ) -> impl Future<Output=Result<R, Error>> + Send
        where F: FnOnce(ReadWriteFilesystem) -> B + Send + 'static,
              B: Future<Output=Result<R, Error>> + Send,
              R: 'static
    {
        f(self.fswrite_inner(tree_id))
    }
}

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
    use crate::{
        tree::{OPEN_MTX, RangeQuery},
        util::basic_runtime
    };
    use futures::{
        task::Poll,
        future
    };
    use mockall::{Sequence, predicate::*};
    use std::ops::RangeFull;

    // pet kcov
    #[test]
    fn debug() {
        let label = Label{forest: TreeOnDisk::default()};
        format!("{:?}", label);
    }

    #[test]
    fn check_ok() {
        let _guard = OPEN_MTX.lock().unwrap();

        let rt = basic_runtime();
        let tree_id = TreeID::Fs(42);
        let mut seq = Sequence::new();

        let mut fs_tree = Tree::default();
        fs_tree.expect_check()
            .returning(|| Ok(true));
        let ctx = ITree::<FSKey, FSValue<RID>>::open_context();
        ctx.expect()
            .once()
            .return_once(|_, _, _| fs_tree);
        let mut idml = IDML::default();
        idml.expect_check()
            .once()
            .returning(|| Box::pin(future::ok::<bool, Error>(true)));

        let tod = TreeOnDisk::default();
        let mut rq = RangeQuery::default();
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Some(Ok((tree_id, tod))));
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Poll::Ready(None));
        let mut forest = Tree::default();
        forest.expect_range()
            .once()
            .return_once(|_: RangeFull| rq);

        let r = rt.block_on(async {
            let db = Database::new(Arc::new(idml), forest);
            db.check().await
        }).unwrap();
        assert!(r);
    }

    #[test]
    fn check_idml_fails() {
        let _guard = OPEN_MTX.lock().unwrap();

        let rt = basic_runtime();
        let tree_id = TreeID::Fs(42);
        let mut seq = Sequence::new();

        let mut fs_tree = Tree::default();
        fs_tree.expect_check()
            .returning(|| Ok(true));
        let ctx = ITree::<FSKey, FSValue<RID>>::open_context();
        ctx.expect()
            .once()
            .return_once(|_, _, _| fs_tree);
        let mut idml = IDML::default();
        idml.expect_check()
            .once()
            .returning(|| Box::pin(future::ok::<bool, Error>(false)));

        let tod = TreeOnDisk::default();
        let mut rq = RangeQuery::default();
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Some(Ok((tree_id, tod))));
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Poll::Ready(None));
        let mut forest = Tree::default();
        forest.expect_range()
            .once()
            .return_once(|_: RangeFull| rq);

        let r = rt.block_on(async {
            let db = Database::new(Arc::new(idml), forest);
            db.check().await
        }).unwrap();
        assert!(!r);
    }

    #[test]
    fn check_tree_fails() {
        let _guard = OPEN_MTX.lock().unwrap();

        let rt = basic_runtime();
        let tree_id = TreeID::Fs(42);
        let mut seq = Sequence::new();

        let mut fs_tree = Tree::default();
        fs_tree.expect_check()
            .returning(|| Ok(false));
        let ctx = ITree::<FSKey, FSValue<RID>>::open_context();
        ctx.expect()
            .once()
            .return_once(|_, _, _| fs_tree);
        let mut idml = IDML::default();
        idml.expect_check()
            .once()
            .returning(|| Box::pin(future::ok::<bool, Error>(true)));

        let tod = TreeOnDisk::default();
        let mut rq = RangeQuery::default();
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Some(Ok((tree_id, tod))));
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Poll::Ready(None));
        let mut forest = Tree::default();
        forest.expect_range()
            .once()
            .return_once(|_: RangeFull| rq);

        let r = rt.block_on(async {
            let db = Database::new(Arc::new(idml), forest);
            db.check().await
        }).unwrap();
        assert!(!r);
    }

    #[test]
    fn flush() {
        const TXG: TxgT = TxgT(42);

        let rt = basic_runtime();

        let mut idml = IDML::default();
        idml.expect_txg()
            .once()
            .returning(|| Box::pin(future::ready::<&'static TxgT>(&TXG)));
        idml.expect_flush()
            .once()
            .with(eq(None), eq(TxgT::from(42)))
            .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

        let forest = Tree::default();

        rt.block_on(async {
            let db = Database::new(Arc::new(idml), forest);
            Database::flush(&db.inner).await
        }).unwrap();
    }

    /// Flushing should be a no-op when there is no dirty data.
    #[test]
    fn flush_empty() {
        let idml = IDML::default();
        let forest = Tree::default();

        let rt = basic_runtime();

        rt.block_on(async {
            let db = Database::new(Arc::new(idml), forest);
            db.inner.dirty.store(false, Ordering::Relaxed);
            Database::flush(&db.inner).await
        }).unwrap();
    }

    #[test]
    fn shutdown() {
        let idml = IDML::default();
        let forest = Tree::default();

        let rt = basic_runtime();

        rt.block_on(async {
            let db = Database::new(Arc::new(idml), forest);
            db.shutdown().await
        });
    }

    #[test]
    fn sync_transaction() {
        let mut seq = Sequence::new();
        let mut idml = IDML::default();
        let mut forest = Tree::default();

        let rt = basic_runtime();

        idml.expect_advance_transaction_inner()
            .once()
            .returning(|| TxgT::from(0));

        forest.expect_flush()
            .once()
            .in_sequence(&mut seq)
            .with(eq(TxgT::from(0)))
            .return_const(Ok(()));
        idml.expect_flush()
            .once()
            .in_sequence(&mut seq)
            .with(eq(Some(0)), eq(TxgT::from(0)))
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
            .with(eq(Some(1)), eq(TxgT::from(0)))
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
            let db = Database::new(Arc::new(idml), forest);
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

        let rt = basic_runtime();

        rt.block_on(async {
            let db = Database::new(Arc::new(idml), forest);
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
