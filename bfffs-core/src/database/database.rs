// vim: tw=80

use crate::{
    cleaner::*,
    dataset::{ITree, ReadOnlyDataset, ReadWriteDataset},
    dml::DML,
    fs_tree::{self, FSKey, FSValue, Inode, ObjKey, FileType, Timespec},
    idml::*,
    label::*,
    tree::TreeOnDisk,
    types::*,
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
use futures_locks::RwLock;
#[cfg(test)] use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::{
    ffi::{OsString, OsStr},
    fmt::Debug,
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use super::{Forest, Status, TreeID};
use tokio::{
    task::JoinHandle,
    time::{Duration, Instant, sleep_until},
};
use tracing::instrument;
use tracing_futures::Instrument;

pub type ReadOnlyFilesystem = ReadOnlyDataset<FSKey, FSValue>;
pub type ReadWriteFilesystem = ReadWriteDataset<FSKey, FSValue>;

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
    // Clippy false positive: the async block is needed for lifetime reasons
    #[allow(clippy::redundant_async_block)]
    fn kick(&self) -> impl Future<Output=Result<()>> {
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
    // Owner for the file system trees.  They must be owned by the Database
    // rather than the Fs so that the Database may flush and sync them all.
    fs_trees: RwLock<BTreeMap<TreeID, Arc<ITree<FSKey, FSValue>>>>,
    /// Stores the Tree of Trees on disk.
    /// * keys are of type ForestKey
    /// * Values are of two types:
    ///   - with offset 0, stores a ForestValue::TreeOnDisk
    ///   - with offset > 0, stores a ForestValue::TreeEnt
    // Would access be faster if we keyed the forest by (<parent TreeID,
    // TreeID>) or by (<parent name>, <name>) or by <parent TreeID, hash(name)>?
    forest: Forest,
    idml: Arc<IDML>,
}

impl Inner {
    async fn destroy_fs(
        inner: Arc<Self>,
        parent: Option<TreeID>,
        tree_id: TreeID,
        name: &str
        ) -> Result<()>
    {
        // First, remove the tree from the forest.  If this succeeds, delete it
        // from disk.  Ensure that it does not remain in the cache, too.  Do
        // this all in a single transaction.
        let tname = name.split('/').last().unwrap();
        inner.dirty.store(true, Ordering::Relaxed);

        // First check that the tree exists ane ensure it is cached.
        Inner::open_filesystem(&inner, tree_id).await?;

        let txg = inner.idml.txg().await;
        // Delete it from the forest while locking fs_trees
        let itree = {
            let mut wg = inner.fs_trees.write().await;
            inner.forest.unlink(parent, tree_id, tname, *txg).await?;
            wg.remove(&tree_id).unwrap()
        };

        // Finally delete its contents
        let cr = itree.credit_requirements();
        let credit = inner.idml.borrow_credit(cr.range_delete).await;
        // XXX range_delete is effective, but it prevents any transactions from
        // syncing, and it holds all of the affected metadata in RAM at the same
        // time.  TODO: for better efficiency, create a restartable
        // Tree::destroy method that partially destroys a tree, iterating like
        // Tree::flush.
        itree.range_delete(.., *txg, credit).await
    }

    fn new(idml: Arc<IDML>, forest: Forest) -> Self
    {
        let dirty = AtomicBool::new(true);
        let fs_trees = RwLock::new(BTreeMap::new());
        Inner{dirty, fs_trees, idml, forest}
    }

    fn new_filesystem(
        inner: &Arc<Inner>,
        tree_id: TreeID,
        tod: TreeOnDisk<RID>)
    -> impl Future<Output=Result<Arc<ITree<FSKey, FSValue>>>> + Send
    {
        let idml2 = inner.idml.clone();
        let tree = ITree::<FSKey, FSValue>::open(idml2, false, tod);
        let atree = Arc::new(tree);
        let atree2 = atree.clone();
        inner.fs_trees.with_write(move |mut wguard| {
            wguard.entry(tree_id).or_insert(atree2);
            future::ready(())
        }).map(|_| {
            Ok(atree)
        })
    }

    // Must be called from within a Tokio executor context
    fn open_filesystem(inner: &Arc<Inner>, tree_id: TreeID)
        -> impl Future<Output=Result<Arc<ITree<FSKey, FSValue>>>> + Send
    {
        let inner2 = inner.clone();
        async move {
            let rguard = inner2.fs_trees.read().await;
            match rguard.get(&tree_id) {
                Some(fs) => Ok(fs.clone()),
                None => {
                    drop(rguard);
                    let tod = inner2.forest.get_tree(tree_id).await?;
                    Inner::new_filesystem(&inner2, tree_id, tod).await
                }
            }
        }
    }

    // The txg is a ref in test mode, but a RwlockWriteGuard in normal mode
    #[cfg_attr(test, allow(dropping_references))]
    fn fswrite<F, B, R>(
        inner: Arc<Self>,
        tree_id: TreeID,
        ninsert: usize,
        nrange_delete: usize,
        nremove: usize,
        blob_bytes: usize,
        f: F,
    ) -> impl Future<Output=Result<R>>
        where F: FnOnce(ReadWriteFilesystem) -> B,
              B: Future<Output=Result<R>>,
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
        -> impl Future<Output=Result<()>>
    {
        let mut labeller = LabelWriter::new(label_idx);
        labeller.serialize(label).unwrap();
        self.idml.write_label(labeller, txg)
    }
}

/// A directory entry in the Forest.
///
/// Each dirent corresponds to one file system.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dirent {
    /// Dataset name, excluding pool and parent file system name, if any.
    pub name: String,
    pub id: TreeID,
    pub offs: u64
}

/// Information about the overall properties of a bfffs pool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stat {
    /// Number of blocks that are in-use across the entire pool.
    pub used: LbaT,
    /// The approximate usable size of the Pool in blocks.
    pub size: LbaT,
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
    #[tracing::instrument(skip(self))]
    pub fn check(&self) -> impl Future<Output=Result<bool>> {
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
            .in_current_span()
    }

    fn check_forest(&self) -> impl Future<Output=Result<bool>> {
        let inner2 = self.inner.clone();
        self.inner.forest.trees()
        .try_fold(true, move |passed, tree_id| {
            Inner::open_filesystem(&inner2, tree_id)
            .and_then(move |tree| tree.check())
            .map_ok(move |r| r && passed)
            // TODO: check for dangling TreeEnts
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
        let forest = Forest::create(idml.clone());
        Database::new(idml, forest)
    }

    /// Drop all data from the cache, for testing or benchmarking purposes
    pub fn drop_cache(&self) {
        self.inner.idml.drop_cache()
    }

    /// Dump a YAMLized representation of the Forest in text format.
    pub async fn dump_forest(&self, f: &mut dyn io::Write)
        -> Result<()>
    {
        self.inner.forest.dump(f).await
    }

    /// Dump a YAMLized representation of the given Tree in text format.
    pub async fn dump_fs(&self, f: &mut dyn io::Write, tree: TreeID)
        -> Result<()>
    {
        Inner::open_filesystem(&self.inner, tree).await.unwrap();
        match self.inner.fs_trees.try_read() {
            Err(_) => Err(Error::EDEADLK),
            Ok(guard) => guard.get(&tree).unwrap().dump(f).await
        }
    }

    /// Dump the FreeSpaceMap in human-readable form, for debugging purposes
    pub fn dump_fsm(&self) -> Vec<String> {
        self.inner.idml.dump_fsm()
    }

    pub async fn dump_alloct(&self, f: &mut dyn io::Write) -> Result<()>
    {
        self.inner.idml.dump_alloct(f).await
    }

    pub async fn dump_ridt(&self, f: &mut dyn io::Write) -> Result<()>
    {
        self.inner.idml.dump_ridt(f).await
    }

    /// Flush the database's dirty data to disk.
    ///
    /// Does not sync a transaction.  Does not rewrite the labels.
    fn flush(inner: &Arc<Inner>)
        -> impl Future<Output=Result<()>> + Send
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

    /// Lookup a Tree's parent
    ///
    /// # Returns
    ///
    /// The parent's id (if any)
    pub fn lookup_parent(&self, tree: TreeID)
        -> impl Future<Output=Result<Option<TreeID>>>
            + Send + 'static
    {
        self.inner.forest.lookup_parent(tree)
    }

    /// Lookup a TreeID by its name.
    ///
    /// # Returns
    ///
    /// A tuple of the parent's id (if any) and the tree's id (if it exists)
    pub fn lookup_fs<'a>(&'a self, name: &'a str)
        -> impl Future<Output=Result<(Option<TreeID>, Option<TreeID>)>>
            + Send + 'static
    {
        self.inner.forest.lookup(name)
    }

    /// Create a new, blank filesystem
    ///
    /// Must be called from the tokio domain.
    #[tracing::instrument(skip(self))]
    pub async fn create_fs<S>(&self, parent: Option<TreeID>, name: S)
        -> Result<TreeID>
        where S: Into<String> + Debug + 'static
    {
        self.inner.dirty.store(true, Ordering::Relaxed);
        let idml2 = self.inner.idml.clone();
        let idml3 = self.inner.idml.clone();
        let inner3 = self.inner.clone();
        // The FS tree's compressibility varies greatly, especially based on
        // whether the write pattern is sequential or random.  5.98x is the
        // lower value for random access.  We'll use that rather than the
        // upper value, to keep cache usage lower.
        let fs = Arc::new(
            ITree::<FSKey, FSValue>::create(idml2, false, 9.00, 1.61)
        );
        let txg_guard = idml3.txg().await;
        fs.clone().flush(*txg_guard).await?;

        // Write the new Tree to the Forest, the source-of-truth for trees
        let tod = fs.serialize().unwrap();
        let tree_id = inner3.forest.insert_tree(parent, name.into(), tod,
            *txg_guard).await?;

        // Create the filesystem's root directory
        Inner::fswrite(inner3, tree_id, 3, 0, 0, 0, move |dataset|
        {
            let ino = 1;    // FUSE requires root dir to have inode 1
            let inode_key = FSKey::new(ino, ObjKey::Inode);
            let now = Timespec::now();

            let inode = Inode {
                size: 0,
                bytes: 0,
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
            let inode_value = FSValue::inode(inode);

            // Create the /. and /.. directory entries
            let dot_dirent = fs_tree::Dirent {
                ino,
                dtype: libc::DT_DIR,
                name:  OsString::from(".")
            };
            let dot_objkey = ObjKey::dir_entry(OsStr::new("."));
            let dot_key = FSKey::new(ino, dot_objkey);
            let dot_value = FSValue::DirEntry(dot_dirent);

            let dotdot_dirent = fs_tree::Dirent {
                ino: 1,     // The VFS replaces this
                dtype: libc::DT_DIR,
                name:  OsString::from("..")
            };
            let dotdot_objkey = ObjKey::dir_entry(OsStr::new(".."));
            let dotdot_key = FSKey::new(ino, dotdot_objkey);
            let dotdot_value = FSValue::DirEntry(dotdot_dirent);

            // Set initial properties
            let futs = FuturesUnordered::new();
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
        }).await?;
        Ok(tree_id)
    }

    /// Destroy an unmounted file system
    // Outline:
    // 1) Move its entry in the Forest to a "destroying" area.
    // 2) Wakeup the Reaper task
    //
    // TODO: create a Reaper task that destroys file systems in the background.
    // On startup, it should check the destroying area.  It should check it
    // again whenever it gets woken.  For each dataset to destroy, it should get
    // credit and tell the Tree layer to destroy it.
    #[instrument(skip(self))]
    pub async fn destroy_fs(
        &self,
        parent: Option<TreeID>,
        tree_id: TreeID,
        name: &str
        ) -> Result<()>
    {
        Inner::destroy_fs(self.inner.clone(), parent, tree_id, name).await
    }

    fn fsread_real<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Output=Result<R>> + Send
        where F: FnOnce(ReadOnlyFilesystem) -> B + Send + 'static,
              B: Future<Output = Result<R>> + Send + 'static,
              R: 'static,
    {
        self.ro_filesystem(tree_id)
            .and_then(f)
    }

    /// Perform a read-only operation on a Filesystem
    #[cfg(not(test))]
    pub fn fsread<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Output=Result<R>> + Send
        where F: FnOnce(ReadOnlyFilesystem) -> B + Send + 'static,
              B: Future<Output = Result<R>> + Send + 'static,
              R: 'static,
    {
        self.fsread_real(tree_id, f)
    }

    /// Perform a streaming read-only operation on a Filesystem
    pub fn fsreads<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Stream<Item=Result<R>> + Send
        where F: FnOnce(ReadOnlyFilesystem) -> B + Send + 'static,
              B: Stream<Item = Result<R>> + Send + 'static,
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
        -> ReadOnlyDataset<FSKey, FSValue>
    {
        unimplemented!()
    }
    // LCOV_EXCL_STOP

    /// List all of the children of `dataset`.
    ///
    /// Note that `dataset` may or may not actually exist.  `offs`, if provided,
    /// is a resume cookie provided by a previous call.
    pub fn readdir(&self, dataset: TreeID, offs: u64)
        -> impl Stream<Item=Result<Dirent>> + Send
    {
        self.inner.forest.readdir(dataset, offs)
            .map_ok(|(te, offs)| Dirent {
                name: te.name,
                id: te.tree_id,
                offs
            })
    }

    fn new(idml: Arc<IDML>, forest: Forest) -> Self
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
        let forest = Forest::open(idml.clone(), l.forest);
        Database::new(idml, forest)
    }

    pub fn pool_name(&self) -> &str {
        self.inner.idml.pool_name()
    }

    fn ro_filesystem(&self, tree_id: TreeID)
        -> impl Future<Output=Result<ReadOnlyFilesystem>>
    {
        let inner2 = self.inner.clone();
        let idml2 = self.inner.idml.clone();
        Inner::open_filesystem(&inner2, tree_id)
        .map_ok(|fs| ReadOnlyFilesystem::new(idml2, fs))
    }

    /// Shutdown all background tasks and close the Database
    pub async fn shutdown(self) {
        future::join(self.syncer.shutdown(),
                     self.cleaner.shutdown())
        .await;
    }

    /// Retrieve information about a pool's space usage
    pub fn stat(&self) -> Stat {
        Stat {
            size: self.inner.idml.size(),
            used: self.inner.idml.used(),
        }
    }

    /// Retrieve the topology and health of the storage pool
    pub fn status(&self) -> Status {
        self.inner.idml.status()
    }

    /// Finish the current transaction group and start a new one.
    pub fn sync_transaction(&self)
        -> impl Future<Output=Result<()>> + Send
    {
        future::try_join(self.syncer.kick(),
                         Database::sync_transaction_priv(&self.inner))
            .map_ok(drop)
    }

    fn sync_transaction_priv(inner: &Arc<Inner>)
        -> impl Future<Output=Result<()>>
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
            // TODO: only write out the dirty trees
            let forest_futs = guard.iter()
                .map(|(tree_id, itree)| {
                    inner2.forest
                        .update_tree(*tree_id, itree.serialize().unwrap(), txg)
                }).collect::<FuturesUnordered<_>>();
            drop(guard);
            forest_futs.try_collect::<Vec<_>>().await?;
            inner2.forest.flush(txg).await?;
            inner2.idml.clone().flush(Some(0), txg).await?;
            inner2.idml.sync_all(txg).await?;
            let forest = inner2.forest.serialize();
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
    #[tracing::instrument(skip(self, tree_id, f))]
    pub fn fswrite<F, B, R>(
        &self,
        tree_id: TreeID,
        ninsert: usize,
        nrange_delete: usize,
        nremove: usize,
        blob_bytes: usize,
        f: F
    ) -> impl Future<Output=Result<R>> + Send
        where F: FnOnce(ReadWriteFilesystem) -> B + Send + 'static,
              B: Future<Output=Result<R>> + Send,
              R: 'static
    {
        Inner::fswrite(self.inner.clone(), tree_id, ninsert, nrange_delete,
            nremove, blob_bytes, f)
            .in_current_span()
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
        -> ReadWriteDataset<FSKey, FSValue>
    {
        unimplemented!()
    }
    // LCOV_EXCL_STOP

    /// Get the soft limit for the size of the writeback cache
    pub fn writeback_size(&self) -> usize {
        self.inner.idml.writeback_size()
    }
}

// LCOV_EXCL_START
#[cfg(test)]
impl MockDatabase {
    pub fn fsread<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Output=Result<R>> + Send
        where F: FnOnce(ReadOnlyFilesystem) -> B + Send + 'static,
              B: Future<Output=Result<R>> + Send + 'static,
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
    ) -> impl Future<Output=Result<R>> + Send
        where F: FnOnce(ReadWriteFilesystem) -> B + Send + 'static,
              B: Future<Output=Result<R>> + Send,
              R: 'static
    {
        f(self.fswrite_inner(tree_id))
    }
}

#[cfg(test)]
mod t {
mod database {
    use super::super::*;
    use super::super::super::{ForestKey, ForestValue};
    use crate::tree::{OPEN_MTX, RangeQuery, Tree};
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
        format!("{label:?}");
    }

    // await_holding_lock is ok, because the tests don't share reactors
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn check_ok() {
        let _guard = OPEN_MTX.lock().unwrap();

        let forest_key = ForestKey::tree(TreeID(42));
        let mut seq = Sequence::new();

        let mut fs_tree = Tree::default();
        fs_tree.expect_check()
            .returning(|| Ok(true));
        let ctx = ITree::<FSKey, FSValue>::open_context();
        ctx.expect()
            .once()
            .return_once(|_, _, _| fs_tree);
        let mut idml = IDML::default();
        idml.expect_check()
            .once()
            .returning(|| Box::pin(future::ok::<bool, Error>(true)));

        let tree = ForestValue::Tree(crate::database::Tree {
            parent: None,
            tod: TreeOnDisk::default()
        });
        let mut rq = RangeQuery::default();
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Some(Ok((forest_key, tree.clone()))));
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Poll::Ready(None));
        let mut forest = Tree::default();
        forest.expect_range()
            .once()
            .return_once(|_: RangeFull| rq);
        forest.expect_get()
            .once()
            .with(eq(forest_key))
            .return_once(|_| Box::pin(future::ok(Some(tree))));

        let db = Database::new(Arc::new(idml), forest.into());
        let r = db.check().await.unwrap();
        assert!(r);
    }

    // await_holding_lock is ok, because the tests don't share reactors
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn check_idml_fails() {
        let _guard = OPEN_MTX.lock().unwrap();

        let forest_key = ForestKey::tree(TreeID(42));
        let mut seq = Sequence::new();

        let mut fs_tree = Tree::default();
        fs_tree.expect_check()
            .returning(|| Ok(true));
        let ctx = ITree::<FSKey, FSValue>::open_context();
        ctx.expect()
            .once()
            .return_once(|_, _, _| fs_tree);
        let mut idml = IDML::default();
        idml.expect_check()
            .once()
            .returning(|| Box::pin(future::ok::<bool, Error>(false)));

        let tree = ForestValue::Tree(crate::database::Tree {
            parent: None,
            tod: TreeOnDisk::default()
        });
        let mut rq = RangeQuery::default();
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Some(Ok((forest_key, tree.clone()))));
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Poll::Ready(None));
        let mut forest = Tree::default();
        forest.expect_range()
            .once()
            .return_once(|_: RangeFull| rq);
        forest.expect_get()
            .once()
            .with(eq(forest_key))
            .return_once(|_| Box::pin(future::ok(Some(tree))));

        let db = Database::new(Arc::new(idml), forest.into());
        let r = db.check().await.unwrap();
        assert!(!r);
    }

    // await_holding_lock is ok, because the tests don't share reactors
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn check_tree_fails() {
        let _guard = OPEN_MTX.lock().unwrap();

        let forest_key = ForestKey::tree(TreeID(42));
        let mut seq = Sequence::new();

        let mut fs_tree = Tree::default();
        fs_tree.expect_check()
            .returning(|| Ok(false));
        let ctx = ITree::<FSKey, FSValue>::open_context();
        ctx.expect()
            .once()
            .return_once(|_, _, _| fs_tree);
        let mut idml = IDML::default();
        idml.expect_check()
            .once()
            .returning(|| Box::pin(future::ok::<bool, Error>(true)));

        let tree = ForestValue::Tree(crate::database::Tree {
            parent: None,
            tod: TreeOnDisk::default()
        });
        let mut rq = RangeQuery::default();
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Some(Ok((forest_key, tree.clone()))));
        rq.expect_poll_next()
            .once()
            .in_sequence(&mut seq)
            .return_const(Poll::Ready(None));
        let mut forest = Tree::default();
        forest.expect_range()
            .once()
            .return_once(|_: RangeFull| rq);
        forest.expect_get()
            .once()
            .with(eq(forest_key))
            .return_once(|_| Box::pin(future::ok(Some(tree))));

        let db = Database::new(Arc::new(idml), forest.into());
        let r = db.check().await.unwrap();
        assert!(!r);
    }

    #[tokio::test]
    async fn flush() {
        const TXG: TxgT = TxgT(42);

        let mut idml = IDML::default();
        idml.expect_txg()
            .once()
            .returning(|| Box::pin(future::ready::<&'static TxgT>(&TXG)));
        idml.expect_flush()
            .once()
            .with(eq(None), eq(TxgT::from(42)))
            .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

        let forest = Tree::default();

        let db = Database::new(Arc::new(idml), forest.into());
        Database::flush(&db.inner).await.unwrap();
    }

    /// Flushing should be a no-op when there is no dirty data.
    #[tokio::test]
    async fn flush_empty() {
        let idml = IDML::default();
        let forest = Tree::default();

        let db = Database::new(Arc::new(idml), forest.into());
        db.inner.dirty.store(false, Ordering::Relaxed);
        Database::flush(&db.inner).await.unwrap();
    }

    #[tokio::test]
    async fn shutdown() {
        let idml = IDML::default();
        let forest = Tree::default();

        let db = Database::new(Arc::new(idml), forest.into());
        db.shutdown().await
    }

    #[tokio::test]
    async fn sync_transaction() {
        let mut seq = Sequence::new();
        let mut idml = IDML::default();
        let mut forest = Tree::default();

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

        let db = Database::new(Arc::new(idml), forest.into());
        db.sync_transaction().await.unwrap();
        // Syncing a 2nd time should be a no-op, since the database
        // isn't dirty.
        db.sync_transaction().await.unwrap();
    }

    /// Syncing a transaction that isn't dirty should be a no-op
    #[tokio::test]
    async fn sync_transaction_empty() {
        let idml = IDML::default();
        let forest = Tree::default();

        let db = Database::new(Arc::new(idml), forest.into());
        db.inner.dirty.store(false, Ordering::Relaxed);
        db.sync_transaction().await.unwrap();
    }
}

mod syncer_msg {
    use super::super::*;

    //pet kcov
    #[test]
    fn debug() {
        let sm = SyncerMsg::Shutdown;
        format!("{sm:?}");
    }
}
}
