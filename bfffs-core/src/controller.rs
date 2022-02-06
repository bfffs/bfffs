// vim: tw=80
//! Entry point for all bfffs control-path operations.  There is only one
//! controller for each running daemon, even if it has multiple pools.

use crate::{
    Error,
    database::Database,
    fs::Fs
};
use futures::{
    Future,
    channel::oneshot,
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

    /// Dump a YAMLized representation of the given Tree in text format.
    pub async fn dump(&self, f: &mut dyn io::Write, tree: TreeID)
        -> Result<(), Error>
    {
        self.db.dump(f, tree).await
    }

    pub fn new(db: Database) -> Self {
        Controller{db: Arc::new(db)}
    }

    pub fn new_fs(&self, tree: TreeID) -> impl Future<Output=Fs> {
        Fs::new(self.db.clone(), tree)
    }
}
