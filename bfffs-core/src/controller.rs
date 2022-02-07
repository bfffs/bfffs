// vim: tw=80
//! Entry point for all bfffs control-path operations.  There is only one
//! controller for each running daemon, even if it has multiple pools.

use crate::{
    Error,
    database::Database,
    fs::Fs,
    property::Property
};
use futures::{
    Future,
    FutureExt,
    channel::oneshot,
    future
};
use std::{
    io,
    sync::Arc
};

pub type TreeID = crate::database::TreeID;

#[derive(Clone)]
pub struct Controller {
    db: Arc<Database>
}

impl Controller {
    /// Foreground consistency check.  Prints any irregularities to stderr
    ///
    /// # Returns
    ///
    /// `true` on success, `false` on failure
    pub fn check(&self) -> impl Future<Output=Result<bool, Error>> {
        self.db.check()
    }

    /// Clean zones immediately.  Does not wait for the result to be polled!
    ///
    /// The returned `Receiver` will deliver notification when cleaning is
    /// complete.  However, there is no requirement to poll it.  The client may
    /// drop it, and cleaning will continue in the background.
    pub fn clean(&self) -> oneshot::Receiver<()> {
        self.db.clean()
    }

    /// Create a new, blank filesystem
    ///
    /// # Arguments
    ///
    /// - `name`    -   Name of the file system to create, including pool name
    /// - `props`   -   Properties to set on the newly created file system.
    pub async fn create_fs(&self, name: &str, props: Vec<Property>)
        -> Result<TreeID, Error>
    {
        let fsname = self.strip_pool_name(name)?;
        let (parent, dsname) = match fsname.rsplit_once('/') {
            None => (Some(crate::database::TreeID(0)), fsname),
            Some((parent_name, dsname)) =>
                (self.db.lookup_fs(parent_name).await?, dsname)
        };
        if parent.is_none() {
            return Err(Error::ENOENT);
        }
        self.db.create_fs(parent, dsname.to_owned(), props).await
    }

    /// Dump a YAMLized representation of the given Tree in text format.
    pub async fn dump(&self, f: &mut dyn io::Write, tree: TreeID)
        -> Result<(), Error>
    {
        self.db.dump(f, tree).await
    }

    pub fn new(db: Database) -> Self {
        Controller{db: Arc::new(db)}
    }

    /// Create a new Fs object (in memory, not on disk).
    ///
    /// # Arguments
    ///
    /// - `name`    -   Name of the file system to create, including pool name
    // Clippy false positive.
    #[allow(clippy::unnecessary_to_owned)]
    pub fn new_fs(&self, name: String)
        -> impl Future<Output = Result<Fs, Error>> + Send
    {
        match self.strip_pool_name(&name) {
            Ok(fsname) => {
                Fs::new(self.db.clone(), fsname.to_owned())
                    .map(Ok)
                    .boxed()
            }
            Err(e) => future::err(e).boxed()
        }
    }

    // Strip the pool name.  For now, only one pool is supported.
    fn strip_pool_name<'a>(&self, name: &'a str) -> Result<&'a str, Error> {
        match name.strip_prefix(&self.db.pool_name()) {
            Some(s) => Ok(s.strip_prefix('/').unwrap_or(s)),
            None => Err(Error::ENOENT),
        }
    }
}
