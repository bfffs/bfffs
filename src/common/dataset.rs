// vim: tw=80

//! Dataset layer
//!
//! An individual dataset is a file system, or a snapshot, or a block device, or
//! a specialized key-value store.  Datasets may be created, destroyed, cloned,
//! and snapshotted.  The also support the same CRUD operations as Trees.

use common::*;
use common::idml::*;
use common::tree::*;
use futures::Future;
use std::sync::Arc;

pub type ITree<K, V> = Tree<RID, IDML, K, V>;

/// Inner Dataset structure, not directly exposed to user
struct Dataset<K: Key, V: Value>  {
    idml: Arc<IDML>,
    tree: Arc<ITree<K, V>>
}

impl<K: Key, V: Value> Dataset<K, V> {
    fn allocated(&self) -> LbaT {
        self.idml.allocated()
    }

    fn get(&self, k: K) -> impl Future<Item=Option<V>, Error=Error>
    {
        self.tree.get(k)
    }

    fn insert(&self, txg: TxgT, k: K, v: V)
        -> impl Future<Item=Option<V>, Error=Error>
    {
        self.tree.insert(k, v, txg)
    }

    fn last_key(&self) -> impl Future<Item=Option<K>, Error=Error>
    {
        self.tree.last_key()
    }

    fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>) -> Self {
        Dataset{idml, tree}
    }

    fn size(&self) -> LbaT {
        self.idml.size()
    }
}

/// A dataset handle with read-only access
pub struct ReadOnlyDataset<K: Key, V: Value>  {
    dataset: Dataset<K, V>
}

impl<K: Key, V: Value> ReadOnlyDataset<K, V> {
    pub fn allocated(&self) -> LbaT {
        self.dataset.allocated()
    }

    pub fn get(&self, k: K) -> impl Future<Item=Option<V>, Error=Error>
    {
        self.dataset.get(k)
    }

    pub fn last_key(&self) -> impl Future<Item=Option<K>, Error=Error>
    {
        self.dataset.last_key()
    }

    pub fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>) -> Self {
        ReadOnlyDataset{dataset: Dataset::new(idml, tree)}
    }

    pub fn size(&self) -> LbaT {
        self.dataset.size()
    }
}

/// A dataset handle with read/write access
pub struct ReadWriteDataset<K: Key, V: Value>  {
    dataset: Dataset<K, V>,
    txg: TxgT
}

impl<K: Key, V: Value> ReadWriteDataset<K, V> {
    pub fn allocated(&self) -> LbaT {
        self.dataset.allocated()
    }

    pub fn get(&self, k: K) -> impl Future<Item=Option<V>, Error=Error>
    {
        self.dataset.get(k)
    }

    pub fn insert(&self, k: K, v: V)
        -> impl Future<Item=Option<V>, Error=Error>
    {
        self.dataset.insert(self.txg, k, v)
    }

    pub fn new(idml: Arc<IDML>, tree: Arc<ITree<K, V>>, txg: TxgT) -> Self {
        ReadWriteDataset{dataset: Dataset::new(idml, tree), txg}
    }
}
