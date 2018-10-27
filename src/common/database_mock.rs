// vim: tw=80
// LCOV_EXCL_START
use crate::common::{
    *,
    dataset_mock::*,
    database::TreeID,
    fs_tree::*,
};
use futures::{
    Future,
    IntoFuture,
    sync::oneshot
};
use simulacrum::*;
use std::io;

pub type ReadOnlyFilesystem = ReadOnlyDatasetMock<FSKey, FSValue<RID>>;
pub type ReadWriteFilesystem = ReadWriteDatasetMock<FSKey, FSValue<RID>>;

pub struct DatabaseMock {
    e: Expectations
}

impl DatabaseMock {
    pub fn clean(&self) -> oneshot::Receiver<()> {
        self.e.was_called_returning::<(), oneshot::Receiver<()>>("clean", ())
    }

    pub fn expect_clean(&mut self) -> Method<(), oneshot::Receiver<()>>
    {
        self.e.expect::<(), oneshot::Receiver<()>>("clean")
    }

    pub fn dump(&self, _f: &io::Write, tree: TreeID) {
        self.e.was_called_returning::<TreeID, ()>("dump", tree)
    }

    pub fn new_fs(&self) -> impl Future<Item=TreeID, Error=Error> + Send {
        self.e.was_called_returning::<(),
            Box<Future<Item=TreeID, Error=Error> + Send>>("new_fs", ())
    }

    pub fn expect_new_fs(&mut self) -> Method<(),
        Box<Future<Item=TreeID, Error=Error> + Send>>
    {
        self.e.expect::<(), Box<Future<Item=TreeID, Error=Error> + Send>>
            ("new_fs")
    }

    pub fn fsread<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadOnlyDatasetMock<FSKey, FSValue<RID>>)
                -> B + 'static,
              B: IntoFuture<Item = R, Error = Error> + 'static,
              R: 'static
    {
        let ds = self.e.was_called_returning::<TreeID,
            ReadOnlyDatasetMock<FSKey, FSValue<RID>>>("fsread", tree_id);
        f(ds).into_future()
    }

    pub fn expect_fsread(&mut self) -> Method<TreeID,
        ReadOnlyDatasetMock<FSKey, FSValue<RID>>>
    {
        self.e.expect::<TreeID, ReadOnlyDatasetMock<FSKey, FSValue<RID>>>
            ("fsread")
    }

    pub fn new() -> Self {
        Self {
            e: Expectations::new()
        }
    }

    pub fn sync_transaction(&self) -> impl Future<Item=(), Error=Error> + Send
    {
        self.e.was_called_returning::<(),
            Box<Future<Item=(), Error=Error> + Send>>("sync_transaction", ())
    }

    pub fn expect_sync_transaction(&mut self) -> Method<(),
        Box<Future<Item=(), Error=Error> + Send>>
    {
        self.e.expect::<(), Box<Future<Item=(), Error=Error> + Send>>
            ("sync_transaction")
    }

    pub fn fswrite<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadWriteDatasetMock<FSKey, FSValue<RID>>) -> B,
              B: Future<Item = R, Error = Error>,
    {
        let ds = self.e.was_called_returning::<TreeID,
            ReadWriteDatasetMock<FSKey, FSValue<RID>>>("fswrite", tree_id);
        f(ds).into_future()
    }

    pub fn expect_fswrite(&mut self)
        -> Method<TreeID, ReadWriteDatasetMock<FSKey, FSValue<RID>>>
    {
        self.e.expect::<TreeID, ReadWriteDatasetMock<FSKey, FSValue<RID>>>
            ("fswrite")
    }

    pub fn then(&mut self) -> &mut Self {
        self.e.then();
        self
    }
}
