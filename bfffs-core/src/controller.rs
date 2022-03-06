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
    task::{Context, Poll}
};
use futures_locks::RwLock;
use std::{
    collections::BTreeMap,
    io,
    pin::Pin,
    sync::{Arc, Weak}
};

pub type TreeID = crate::database::TreeID;

/// A directory entry in the Forest.
///
/// Each dirent corresponds to one file system.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dirent {
    /// Dataset name, including pool and parent file system name
    pub name: String,
    pub id: TreeID,
    pub offs: u64
}

pub struct Controller {
    db: Arc<Database>,
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

    /// Get the value of the `propname` property on the given dataset
    pub async fn get_prop(&self, dataset: &str, propname: PropertyName)
        -> Result<(Property, PropertySource)>
    {
        let dsname = self.strip_pool_name(dataset)?;
        let tree_id = match self.db.lookup_fs(dsname).await? {
            (_parent, Some(tree_id)) => tree_id,
            (_, None) => return Err(Error::ENOENT)
        };
        let guard = self.filesystems.read().await;
        if let Some(weak) = guard.get(&tree_id) {
            if let Some(fs) = weak.upgrade() {
                return fs.get_prop(propname).await;
            }
        }
        let db2 = self.db.clone();
        Fs::get_prop_locked(tree_id, db2, propname).await
    }

    /// List a dataset and all of its immediate childen
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
        -> impl Stream<Item=Result<Dirent>> + Send
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
            type Item=Result<Dirent>;

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
                                let o = if self.offs.is_some() {
                                    Pin::new(&mut s).poll_next(cx)
                                } else {
                                    // BUG: using 0 as the offset for the parent
                                    // create a potential for a hash collision
                                    // with a child that happens to have an offs
                                    // of zero, too.
                                    let de = database::Dirent {
                                        name: String::new(),
                                        id: tree_id,
                                        offs: 0
                                    };
                                    Poll::Ready(Some(Ok(de)))
                                };
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
                    Dirent {
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
                        (_, None) => Err(Error::ENOENT)
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
        match name.strip_prefix(&self.db.pool_name()) {
            Some(s) => Ok(s.strip_prefix('/').unwrap_or(s)),
            None => Err(Error::ENOENT),
        }
    }
}
