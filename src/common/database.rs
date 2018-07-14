// vim: tw=80

//! The Database layer owns all of the Datasets
//!
//! Clients use the Database to obtain references to the Datasets.  The Database
//! also owns the Forest and manages Transactions.

use common::*;
use common::dataset::*;
use common::dml::DML;
use common::idml::*;
use common::tree::*;
use futures::{Future, IntoFuture};
use nix::{Error, errno};
#[cfg(not(test))] use std::path::Path;
use std::sync::Arc;
use tokio::executor::SpawnError;
#[cfg(not(test))] use tokio::reactor::Handle;

// TODO: define real keys and values for filesystems
type FSKey = u32;
type FSValue = u32;

type ReadOnlyFilesystem = ReadOnlyDataset<FSKey, FSValue>;
type ReadWriteFilesystem = ReadWriteDataset<FSKey, FSValue>;

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

struct Inner {
    dummy: Arc<ITree<FSKey, FSValue>>,
    idml: Arc<IDML>,
}

impl Inner {
    fn ro_filesystem(&self, _tree_id: TreeID) -> ReadOnlyFilesystem
    {
        ReadOnlyFilesystem::new(self.dummy.clone())
    }

    fn rw_filesystem(&self, _tree_id: TreeID, _txg: TxgT) -> ReadWriteFilesystem
    {
        unimplemented!()
    }
}

pub struct Database {
    // TODO: store all filesystem and device trees
    inner: Arc<Inner>,
}

impl<'a> Database {
    fn new(idml: Arc<IDML>) -> Self {
        let dummy = Arc::new(ITree::create(idml.clone()));
        let inner = Arc::new(Inner{dummy, idml});
        Database{inner}
    }

    #[cfg(not(test))]
    pub fn open<P>(poolname: String, paths: Vec<P>, handle: Handle)
        -> impl Future<Item=Self, Error=Error>
        where P: AsRef<Path> + 'static
    {
        IDML::open(poolname, paths, handle).map(|idml| {
            let arc_idml = Arc::new(idml);
            Database::new(arc_idml)
        })
    }

    /// Perform a read-only operation on a Filesystem
    pub fn fsread<F, B, E, R>(&self, tree_id: TreeID, f: F)
        -> Result<impl Future<Item = R, Error = Error>, SpawnError>
        where F: FnOnce(ReadOnlyFilesystem) -> B + 'static,
              B: IntoFuture<Item = R, Error = Error> + 'static,
              R: 'static
    {
        let ds = self.inner.ro_filesystem(tree_id);
        Ok(f(ds).into_future())
    }

    /// Finish the current transaction group and start a new one.
    pub fn sync_transaction(&'a self) -> impl Future<Item=(), Error=Error> + 'a
    {
        // Outline:
        // 1) Flush the trees
        // 2) Sync the pool
        // 3) Write the label
        // 4) Sync the pool again
        // TODO: use two labels, so the pool will be recoverable even if power
        // is lost while writing a label.
        self.inner.idml.advance_transaction(move |txg|{
            self.inner.dummy.flush(txg)
            .and_then(move |_| self.inner.idml.sync_all(txg))
            .and_then(move |_| self.inner.idml.write_label(txg))
            .and_then(move |_| self.inner.idml.sync_all(txg))
        })
    }

    /// Perform a read-write operation on a Filesystem
    ///
    /// All operations conducted by the supplied closure will be completed
    /// within the same Pool transaction group.  Thus, after a power failure and
    /// recovery, either all will have completed, or none will have.
    pub fn fswrite<F, B, E, R>(&'a self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadWriteFilesystem) -> B + 'a,
              B: Future<Item = R, Error = Error> + 'a,
    {
        let inner = self.inner.clone();
        self.inner.idml.txg()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |txg| {
                let ds = inner.rw_filesystem(tree_id, *txg);
                f(ds)
            })
    }
}


// LCOV_EXCL_START
#[cfg(test)]
#[cfg(feature = "mocks")]
mod t {
    use super::*;
    use common::cache_mock::CacheMock as Cache;
    use common::dml::*;
    use common::ddml::DRP;
    use common::ddml_mock::DDMLMock as DDML;
    use futures::future;
    use simulacrum::validators::trivial::any;
    use std::sync::Mutex;
    use tokio::runtime::current_thread;

    #[ignore = "Simulacrum can't mock a single generic method with different type parameters more than once in the same test https://github.com/pcsm/simulacrum/issues/55"]
    #[test]
    fn sync_transaction() {
        let cache = Cache::new();
        let mut ddml = DDML::new();
        ddml.expect_put::<Arc<tree::Node<RID, FSKey, FSValue>>>()
            .called_any()
            .returning(move |(_, _, _)|
                (DRP::random(Compression::None, 4096),
                 Box::new(future::ok::<(), Error>(())))
            );
        // Can't set this expectation, because RidtEntry isn't pubic
        //ddml.expect_put::<Arc<tree::Node<DRP, RID, RidtEntry>>>()
            //.called_any()
            //.returning(move |(_, _, _)|
                //(DRP::random(Compression::None, 4096),
                 //Box::new(future::ok::<(), Error>(())))
            //);
        ddml.expect_put::<Arc<tree::Node<DRP, PBA, RID>>>()
            .called_any()
            .returning(move |(_, _, _)|
                (DRP::random(Compression::None, 4096),
                 Box::new(future::ok::<(), Error>(())))
            );
        ddml.expect_sync_all()
            .called_once()
            .with(TxgT::from(0))
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        ddml.expect_write_label()
            .called_once()
            .with(any())
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        ddml.expect_sync_all()
            .called_once()
            .with(TxgT::from(0))
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let db = Database::new(Arc::new(idml));
        let mut rt = current_thread::Runtime::new().unwrap();

        rt.block_on(db.sync_transaction()).unwrap();
    }
}
