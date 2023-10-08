// vim: tw=80
//! Entry point for all bfffs control-path operations.  There is only one
//! controller for each running daemon, even if it has multiple pools.

use crate::{
    Error,
    database::{self, Database},
    fs::Fs,
    property::{Property, PropertyName, PropertySource},
    Result
};
use futures::{
    Future,
    FutureExt,
    Stream,
    channel::oneshot,
    future,
    stream,
    task::{Context, Poll}
};
use futures_locks::RwLock;
use std::{
    collections::BTreeMap,
    io,
    ops::Deref,
    pin::Pin,
    sync::{Arc, Weak}
};

pub use crate::database::Status;
pub type TreeID = crate::database::TreeID;

/// A directory entry in the Forest.
///
/// Each dirent corresponds to one file system.
// Analogous to a struct dirent from readdir
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsDirent {
    /// Dataset name, including pool and parent file system name
    pub name: String,
    pub id: TreeID,
    pub offs: u64
}

/// Information about one imported Pool.
// Analogous to a struct dirent from readdir
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolDirent {
    /// Pool name.
    pub name: String,
    /// Stream resume token
    pub offs: u64
}

pub struct Controller {
    db: Arc<Database>,
    /// Collection of all currently-mounted file systems
    filesystems: RwLock<BTreeMap<TreeID, Weak<Fs>>>,
}

impl Controller {
    /// Foreground consistency check.  Prints any irregularities to stderr
    ///
    /// # Returns
    ///
    /// `true` on success, `false` on failure
    pub fn check(&self) -> impl Future<Output=Result<bool>> {
        self.db.check()
    }

    /// Clean zones immediately.  Does not wait for the result to be polled!
    ///
    /// The returned `Receiver` will deliver notification when cleaning is
    /// complete.  However, there is no requirement to poll it.  The client may
    /// drop it, and cleaning will continue in the background.
    pub fn clean(&self, pool: &str) -> Result<oneshot::Receiver<()>> {
        if pool == self.db.pool_name() {
            Ok(self.db.clean())
        } else {
            Err(Error::ENOENT)
        }
    }

    /// Create a new, blank filesystem
    ///
    /// # Arguments
    ///
    /// - `name`    -   Name of the file system to create, including pool name
    pub async fn create_fs(&self, name: &str)
        -> Result<TreeID>
    {
        let fsname = self.strip_pool_name(name)?;
        let r = fsname.rsplit_once('/');
        if let Some((parent_name, dsname)) = r {
            let (_, parent) = self.db.lookup_fs(parent_name).await?;
            if parent.is_none() {
                return Err(Error::ENOENT);
            }
            self.db.create_fs(parent, dsname.to_owned())
        } else if fsname.is_empty() {
            // Creating the pool's root file system
            self.db.create_fs(None, fsname.to_owned())
        } else {
            // Creating a child of the root file system
            self.db.create_fs(Some(database::TreeID(0)), fsname.to_owned())
        }.await
    }

    /// Destroy a filesystem
    ///
    ///
    /// # Arguments
    ///
    /// - `name`    -   Name of the file system to destroy, including pool name
    pub async fn destroy_fs(&self, name: &str) -> Result<()>
    {
        let dsname = self.strip_pool_name(name)?;
        let guard = self.filesystems.read().await;
        let (parent, tree_id) = self.db.lookup_fs(dsname).await?;
        match tree_id {
            Some(id) => {
                if let Some(_fs) = guard.get(&id) {
                    Err(Error::EBUSY)
                } else {
                    self.db.destroy_fs(parent, id, dsname).await
                }
            }
            None => Err(Error::ENOENT)
        }
    }

    /// Drop all data from the cache, for testing or benchmarking purposes
    pub fn drop_cache(&self) {
        self.db.drop_cache()
    }

    /// Dump a YAMLized representation of the Forest in text format.
    pub async fn dump_forest(&self, f: &mut dyn io::Write) -> Result<()>
    {
        self.db.dump_forest(f).await
    }

    /// Dump a YAMLized representation of the given Tree in text format.
    pub async fn dump_fs(&self, f: &mut dyn io::Write, tree: TreeID)
        -> Result<()>
    {
        self.db.dump_fs(f, tree).await
    }

    pub async fn fault(&self, pool: &str, device: &str) -> Result<()> {
        if pool != self.db.pool_name() {
            return Err(Error::ENOENT);
        }
        self.db.fault(device).await
    }

    pub async fn get_pool_status(&self, pool: &str) -> Result<Status> {
        if pool != self.db.pool_name() {
            return Err(Error::ENOENT);
        }
        Ok(self.db.status().await)
    }

    /// Get the value of the `propname` property on the given dataset
    #[tracing::instrument(skip(self))]
    pub async fn get_prop(&self, dataset: String, propname: PropertyName)
        -> Result<(Property, PropertySource)>
    {
        let dsname = self.strip_pool_name(&dataset)?;
        let guard = self.filesystems.read().await;
        match self.db.lookup_fs(dsname).await? {
            (_parent, Some(tree_id)) => {
                self.get_prop_locked(&guard, &dataset, tree_id, propname).await
            }
            (_, None) => {
                tracing::debug!("no property found");
                Err(Error::ENOENT)
            }
        }
    }

    async fn get_prop_locked<T>(
        &self,
        guard: &T,
        dataset: &str,
        tree_id: TreeID,
        propname: PropertyName)
        -> Result<(Property, PropertySource)>
        where T: Deref<Target = BTreeMap<TreeID, Weak<Fs>>>
    {
        if propname == PropertyName::Name {
            // Hard-code this pseudoproperty
            return Ok((Property::Name(dataset.to_owned()),
                       PropertySource::None));
        }

        let inheritable_propname = propname.inheritable();
        if let Some(fs) = guard.get(&tree_id).and_then(Weak::upgrade) {
            fs.get_prop(inheritable_propname).await
        } else {
            let db2 = self.db.clone();
            Fs::get_prop_unmounted(tree_id, db2, inheritable_propname).await
        }.map(|(prop, source)| {
            if let Property::BaseMountpoint(bmp) = prop {
                if propname == PropertyName::Mountpoint {
                    // To construct the Mountpoint property, we must combine the
                    // inherited BaseMountpoint with 0 or more components of the
                    // dataset name
                    let mp = match source {
                        PropertySource::Default => format!("/{dataset}"),
                        PropertySource::LOCAL => bmp,
                        PropertySource::Set(i) => {
                            debug_assert!(i > 0);
                            let nparts = dataset.chars()
                                .filter(|c| *c == '/')
                                .count();
                            format!("{}/{}", bmp,
                                dataset.splitn(nparts + 2 - usize::from(i), '/')
                                    .last()
                                    .unwrap()
                            )
                        },
                        PropertySource::None => unreachable!()
                    };
                    (Property::Mountpoint(mp), source)
                } else {
                    assert_eq!(propname, PropertyName::BaseMountpoint);
                    (Property::Mountpoint(bmp), source)
                }
            } else {
                (prop, source)
            }
        })
    }

    /// List a dataset's immediate childen
    ///
    /// # Arguments
    ///
    /// `fsname`    -   The dataset to list, including pool name
    /// `offs`      -   A stream resume token.  It must be either None or the
    ///                 value returned from a previous call to `list_fs`.
    ///                 Children will be returned beginning after the entry
    ///                 whose offset is `offs`.
    // TODO: list properties
    pub fn list_fs(&self, dataset: &str, offs: Option<u64>)
        -> impl Stream<Item=Result<FsDirent>> + Send
    {
        enum LookupOrList {
            Lookup(Pin<Box<dyn Future<Output=Result<(Option<TreeID>, Option<TreeID>)>> + Send>>),
            List(Pin<Box<dyn Stream<Item=Result<database::Dirent>> + Send>>)
        }

        struct ListFs {
            db: Arc<Database>,
            /// Name of the dataset whose children we are listing, including
            /// the pool.
            parentname: String,
            lol: LookupOrList,
            offs: Option<u64>,
        }
        impl Stream for ListFs {
            type Item=Result<FsDirent>;

            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context)
                -> Poll<Option<Self::Item>>
            {
                match &mut self.lol {
                    LookupOrList::Lookup(lookup_fut) => {
                        match lookup_fut.as_mut().poll(cx) {
                            Poll::Pending => Poll::Pending,
                            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                            Poll::Ready(Ok((_, None))) =>
                                Poll::Ready(Some(Err(Error::ENOENT))),
                            Poll::Ready(Ok((_, Some(tree_id)))) => {
                                let offs = self.offs.unwrap_or(0);
                                let mut s = self.db.readdir(tree_id, offs);
                                let o = Pin::new(&mut s).poll_next(cx);
                                self.lol = LookupOrList::List(Box::pin(s));
                                o
                            }
                        }
                    },
                    LookupOrList::List(s) => s.as_mut().poll_next(cx)
                }.map_ok(|de| {
                    let dsname = if de.name.is_empty() {
                        self.parentname.to_owned()
                    } else {
                        format!("{}/{}", self.parentname, de.name)
                    };
                    FsDirent {
                        name: dsname,
                        id: de.id,
                        offs: de.offs
                    }
                })
            }
        }

        let fut = match self.strip_pool_name(dataset) {
            Ok(fsname) => self.db.lookup_fs(fsname).boxed(),
            Err(e) => future::err(e).boxed(),
        };
        let lol = LookupOrList::Lookup(fut);
        ListFs{db: self.db.clone(), parentname: dataset.to_owned(), lol, offs}
    }

    /// List pools
    ///
    /// # Arguments
    ///
    /// `pool`    -     The pool name.  If `None`, list all pools
    /// `offs`    -     A stream resume token.  It must be either None or the
    ///                 value returned from a previous call to `list_pool`.
    ///                 Pools will be returned beginning after the entry
    ///                 whose offset is `offs`.
    pub fn list_pool(&self, pool: Option<String>, _offs: Option<u64>)
        -> impl Stream<Item=Result<PoolDirent>> + Send
    {
        if let Some(p) = pool {
            if p != self.db.pool_name() {
                return stream::once(future::err(Error::ENOENT));
            }
        }
        let dirent = PoolDirent {
            name: self.db.pool_name().to_string(),
            offs: 0
        };
        stream::once(future::ok(dirent))
    }

    pub fn new(db: Database) -> Self {
        Controller{
            db: Arc::new(db),
            filesystems: Default::default()
        }
    }

    /// Create a new Fs object (in memory, not on disk).
    ///
    /// # Arguments
    ///
    /// - `name`    -   Name of the file system to create, including pool name
    // Clippy false positive.
    #[allow(clippy::unnecessary_to_owned)]
    #[tracing::instrument(skip(self))]
    pub fn new_fs(&self, name: &str)
        -> impl Future<Output = Result<Arc<Fs>>> + Send
    {
        // Outline:
        // 1) strip pool name
        // 2) lookup id in the database
        // 3) lock filesystems
        // 4) If present, return an error
        // 5) Otherwise, create a new file system
        // 6) Insert into filesystems
        // 7) return a clone of it
        // TODO: after switching to cryptographic GUIDs for Tree IDs, eliminate
        // the String::from by moving lookup_fs to outside of the rwlock
        // acquisition.
        match self.strip_pool_name(name).map(String::from) {
            Ok(fsname) => {
                let db2 = self.db.clone();
                self.filesystems.write()
                .then(|mut guard| async move {
                    match db2.lookup_fs(&fsname).await? {
                        (_parent, Some(tree_id)) => {
                            if let Some(weak) = guard.get(&tree_id) {
                                if weak.strong_count() > 0 {
                                    return Err(Error::EBUSY);
                                }
                            }
                            let fs = Arc::new(Fs::new(db2, tree_id).await);
                            let fsw = Arc::downgrade(&fs);
                            guard.insert(tree_id, fsw);
                            Ok(fs)
                        },
                        (_, None) => {
                            tracing::debug!("file system not found");
                            Err(Error::ENOENT)
                        }
                    }
                }).boxed()
            }
            Err(e) => future::err(e).boxed()
        }
    }

    /// Set the value of a property on the given dataset.
    // TODO: when setting a property, update the in-memory property on all of
    // its child datasets.
    pub async fn set_prop(&self, dataset: &str, prop: Property) -> Result<()>
    {
        let prop = prop.inheritable();
        let dsname = self.strip_pool_name(dataset)?;
        let tree_id = match self.db.lookup_fs(dsname).await? {
            (_parent, Some(tree_id)) => tree_id,
            (_, None) => return Err(Error::ENOENT)
        };
        let guard = self.filesystems.read().await;
        if let Some(weak) = guard.get(&tree_id) {
            if let Some(fs) = weak.upgrade() {
                return fs.set_prop(prop).await;
            }
        }
        Fs::set_prop_unmounted(tree_id, &self.db, prop).await
    }

    // Strip the pool name.  For now, only one pool is supported.
    fn strip_pool_name<'a>(&self, name: &'a str) -> Result<&'a str> {
        match name.strip_prefix(self.db.pool_name()) {
            Some(s) => Ok(s.strip_prefix('/').unwrap_or(s)),
            None => {
                tracing::debug!("pool name not provided");
                Err(Error::ENOENT)
            },
        }
    }

    /// Finish the current transaction group and start a new one.
    // TODO: specify one pool or all of them
    pub async fn sync_transaction(&self) -> Result<()> {
        self.db.sync_transaction().await
    }

    pub async fn unmount(&self, name: &str, force: bool) -> Result<()>
    {
        use nix::mount::{unmount, MntFlags};

        let dsname = self.strip_pool_name(name)?;
        let mut flags = MntFlags::empty();
        if force {
            flags.insert(MntFlags::MNT_FORCE);
        }
        let mut guard = self.filesystems.write().await;
        let tree_id = match self.db.lookup_fs(dsname).await? {
            (_, Some(id)) => id,
            (_, None) => return Err(Error::ENOENT)
        };
        let (prop, _) = self.get_prop_locked(&guard, name, tree_id,
                                             PropertyName::Mountpoint)
            .await?;

        // unmount(2) can block waiting for the daemon to respond.  And the
        // daemon might be using this thread to read from /dev/fuse.  So we must
        // spawn a separate thread for unmount(2).
        tokio::task::spawn_blocking(move || {
            unmount(prop.as_str(), flags)
                .map_err(Error::from)
        }).await.unwrap()?;
        guard.remove(&tree_id);
        Ok(())
    }
}
