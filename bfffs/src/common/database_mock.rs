// vim: tw=80
// LCOV_EXCL_START
use crate::common::{
    *,
    dataset_mock::*,
    database::TreeID,
    fs_tree::*,
    property::*
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

#[derive(Default)]
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

    pub fn dump(&self, _f: &io::Write, tree: TreeID) -> Result<(), Error> {
        self.e.was_called_returning::<TreeID, Result<(), Error>>("dump", tree)
    }

    pub fn new_fs(&self, props: Vec<Property>)
        -> impl Future<Item=TreeID, Error=Error> + Send
    {
        self.e.was_called_returning::<Vec<Property>,
            Box<Future<Item=TreeID, Error=Error> + Send>>
            ("new_fs", props)
    }

    pub fn expect_new_fs(&mut self) -> Method<Vec<Property>,
        Box<Future<Item=TreeID, Error=Error> + Send>>
    {
        self.e.expect::<Vec<Property>,
            Box<Future<Item=TreeID, Error=Error> + Send>>
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

    pub fn get_prop(&self, tree_id: TreeID, name: PropertyName)
        -> impl Future<Item=(Property, PropertySource), Error=Error>
    {
        self.e.was_called_returning::<(TreeID, PropertyName),
            Box<Future<Item=(Property, PropertySource), Error=Error> + Send>>
                ("get_prop", (tree_id, name))
    }

    pub fn expect_get_prop(&mut self) -> Method<(TreeID, PropertyName),
        Box<Future<Item=(Property, PropertySource), Error=Error> + Send>>
    {
        self.e.expect::<(TreeID, PropertyName),
            Box<Future<Item=(Property, PropertySource), Error=Error> + Send>>
                ("get_prop")
    }

    pub fn set_prop(&self, tree_id: TreeID, prop: Property)
        -> impl Future<Item=(), Error=Error> + Send
    {
        self.e.was_called_returning::<(TreeID, Property),
            Box<Future<Item=(), Error=Error> + Send>>
                ("set_prop", (tree_id, prop))
    }

    pub fn expect_set_prop(&mut self) -> Method<(TreeID, Property),
        Box<Future<Item=(), Error=Error> + Send>>
    {
        self.e.expect::<(TreeID, Property),
            Box<Future<Item=(), Error=Error> + Send>>
                ("set_prop")
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

// XXX totally unsafe!  But Simulacrum doesn't support mocking Send traits.  So
// we have to cheat.  This works as long as DatabaseMock is only used in
// single-threaded unit tests.
unsafe impl Send for DatabaseMock {}
unsafe impl Sync for DatabaseMock {}
// LCOV_EXCL_STOP
