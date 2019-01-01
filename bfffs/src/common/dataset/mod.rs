// vim: tw=80

//! Dataset layer
//!
//! An individual dataset is a file system, or a snapshot, or a block device, or
//! a specialized key-value store.  Datasets may be created, destroyed, cloned,
//! and snapshotted.  The also support the same CRUD operations as Trees.

use cfg_if::cfg_if;
use crate::common::{
    *,
    idml::IDML,
    tree::{Key, Tree, Value}
};
use divbuf::DivBuf;
use futures::Future;
use std::{
    borrow::Borrow,
    ops::RangeBounds,
};

cfg_if! {
    if #[cfg(test)]{
        mod dataset_mock;
        pub use self::dataset_mock::ReadOnlyDatasetMock as ReadOnlyDataset;
        pub use self::dataset_mock::ReadWriteDatasetMock as ReadWriteDataset;
        pub use self::dataset_mock::RangeQuery;
    } else {
        mod dataset;
        use crate::common::tree;
        pub use self::dataset::{ReadOnlyDataset, ReadWriteDataset};
        /// Return type of `Dataset::range`
        pub type RangeQuery<K, T, V> = tree::RangeQuery<RID, IDML, K, T, V>;
    }
}

pub type ITree<K, V> = Tree<RID, IDML, K, V>;

/// A Dataset that can be read from
pub trait ReadDataset<K: Key, V: Value> {
    fn get(&self, k: K) -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>;

    /// Read directly from the IDML, bypassing the Tree
    fn get_blob(&self, rid: RID)
        -> Box<dyn Future<Item=Box<DivBuf>, Error=Error> + Send>;

    fn range<R, T>(&self, range: R) -> RangeQuery<K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T> + 'static,
              T: Ord + Clone + Send + 'static;
}
