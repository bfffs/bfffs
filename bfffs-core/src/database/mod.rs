// vim: tw=80

//! The Database layer owns all of the Datasets
//!
//! Clients use the Database to obtain references to the Datasets.  The Database
//! also owns the Forest and manages Transactions.

use crate::{
    dataset::ITree,
    idml::IDML,
    tree::{Key, MinValue, RangeQuery, TreeOnDisk, Value},
    types::*,
    writeback::Credit
};
use futures::{
    Future,
    FutureExt,
    Stream,
    StreamExt,
    TryFutureExt,
    TryStream,
    TryStreamExt,
    future,
    task::{Context, Poll}
};
use metrohash::MetroHash64;
use mockall_double::*;
use serde_derive::{Deserialize, Serialize};
use std::{
    hash::Hasher,
    io,
    ops::Range,
    pin::Pin,
    sync::Arc
};

mod database;

#[double]
pub use self::database::Database;
pub use self::database::Dirent;

pub use self::database::ReadOnlyFilesystem;
pub use self::database::ReadWriteFilesystem;

/// Unique identifier for a tree, like a ZFS guid
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
// NB: might need to make this cryptographic, to support send/recv
pub struct TreeID(pub u64);

impl TreeID {
    /// Get the sequentially next Tree ID
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(TreeID)
    }
}

/// Keys into the Forest
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
struct ForestKey {
    /// The TreeID of this tree's parent, or 0 if this is the root file system
    tree_id: TreeID,
    /// A hash of the child's name
    offset: u64
}

impl ForestKey {
    /// Construct a key to lookup a tree with a known id
    pub fn tree(tree_id: TreeID) -> Self  {
        ForestKey{ tree_id, offset: 0}
    }

    pub fn tree_ent(parent: TreeID, name: &str) -> Self {
        if name.as_bytes().contains(&(b'/')) {
            panic!("File system names may not contain '/'");
        }

        let mut hasher = MetroHash64::new();
        hasher.write(name.as_bytes());
        // TODO: use some salt to defend against DOS attacks
        // TODO: consider using hash buckets for hash collisions.
        let offset = hasher.finish() & ( (1<<56) - 1);
        assert!(offset > 0, "this type of collision is TODO");
        ForestKey{ tree_id: parent, offset}
    }

    /// Create a range of `ForestKey` that will include all the TreeEnt children
    /// of dataset `parent` beginning at `offset`
    pub fn tree_ent_range(parent: TreeID, offset: u64) -> Range<Self> {
        let start = ForestKey{tree_id: parent, offset: offset.max(1)};
        let end = ForestKey{tree_id: parent.next().unwrap(), offset: 0};
        start..end
    }

}

impl Key for ForestKey {
    const USES_CREDIT: bool = false;
}

impl TypicalSize for ForestKey {
    const TYPICAL_SIZE: usize = 16;
}

impl MinValue for ForestKey {
    fn min_value() -> Self {
        Self {
            tree_id: TreeID(u64::min_value()),
            offset: 0
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Tree {
    parent: Option<TreeID>,
    /// Serialized version of this Tree.
    ///
    /// NB: if the tree is dirty, then it won't exist on disk.
    tod: TreeOnDisk<RID>
}

impl Tree {
    fn new(parent: Option<TreeID>, tod: TreeOnDisk<RID>) -> Self {
        Tree{parent, tod}
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TreeEnt {
    /// ID of the tree
    pub tree_id:    TreeID,
    /// Name of the file system or other object contained within the tree,
    /// excluding it's parent's component
    pub name:   String
}


#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum ForestValue {
    Tree(Tree),
    TreeEnt(TreeEnt)
}

impl TypicalSize for ForestValue {
    // This is the size of a Tree entry.  For TreeEnt, the entry is variable.
    const TYPICAL_SIZE: usize = 4 + TreeOnDisk::<RID>::TYPICAL_SIZE;
}

impl Value for ForestValue {}


/// The Tree of Tree Roots
#[derive(Clone)]
struct Forest(Arc<ITree<ForestKey, ForestValue>>);

impl Forest {
    /// Create a brand-new forest that does not yet exist on disk
    pub fn create(idml: Arc<IDML>) -> Self {
        // Compression ratio is a total guess; it hasn't been measured yet.
        Self(Arc::new(ITree::create(idml, true, 4.0, 2.0)))
    }

    /// Dump a a YAMLized representation of the Forest
    pub async fn dump(&self, f: &mut dyn io::Write)
        -> Result<()>
    {
        self.0.dump(f).await
    }

    /// Flush all in-memory Nodes to disk.
    pub async fn flush(&self, txg: TxgT) -> Result<()> {
        self.0.clone().flush(txg).await
    }

    /// Lookup a tree whose ID is known
    pub async fn get_tree(&self, tree_id: TreeID)
        -> Result<TreeOnDisk<RID>>
    {
        match self.0.get(ForestKey::tree(tree_id)).await? {
            Some(ForestValue::Tree(tree)) => Ok(tree.tod),
            Some(ForestValue::TreeEnt(te)) => {
                panic!("TreeEnt unexpected with offset 0 {te:?}");
            },
            None => Err(Error::ENOENT)
        }
    }

    /// Insert a new Tree.  There must not already be a Tree by this name
    pub async fn insert_tree(&self,
                       parent: Option<TreeID>,
                       name: String,
                       tod: TreeOnDisk<RID>,
                       txg: TxgT)
        -> Result<TreeID>
    {
        let tree_id = match self.0.last_key().await? {
            Some(last) => last.tree_id.next()
                .expect("Maximum number of file systems reached"),
            None => TreeID(0)
        };
        let new_tree_key = ForestKey::tree(tree_id);
        let new_v = ForestValue::Tree(Tree::new(parent,tod));
        let old_v = self.0.clone()
            .insert(new_tree_key, new_v, txg, Credit::null())
            .await?;
        assert!(old_v.is_none(), "Races creating trees are TODO");
        if let Some(p) = parent {
            let new_te_key = ForestKey::tree_ent(p, &name);
            let te = ForestValue::TreeEnt(TreeEnt { tree_id, name });
            let oold_te = self.0.clone()
                .insert(new_te_key, te, txg, Credit::null())
                .await?;
            if let Some(old_te) = oold_te {
                // Tree already exists.
                let new_v = self.0.clone()
                    .remove(new_tree_key, txg, Credit::null())
                    .await?;
                assert!(new_v.is_some(),
                    "Race resolving EEXIST in tree creation");
                self.0.clone()
                    .insert(new_te_key, old_te, txg, Credit::null())
                    .await?;
                return Err(Error::EEXIST);
            }
        }
        Ok(tree_id)
    }

    /// List all of the children of a dataset
    pub fn readdir(&self, parent: TreeID, offs: u64)
        -> impl Stream<Item=Result<(TreeEnt, u64)>> + Send
    {
        struct ReaddirStream {
            rq: RangeQuery<RID, IDML, ForestKey, ForestKey, ForestValue>,
        }
        impl Stream for ReaddirStream {
            type Item = Result<(TreeEnt, u64)>;

            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context)
                -> Poll<Option<Self::Item>>
            {
                match Pin::new(&mut self.rq).poll_next(cx) {
                    Poll::Ready(Some(Ok((k, v)))) => match v {
                        ForestValue::TreeEnt(tree_ent) => {
                            Poll::Ready(Some(Ok((tree_ent, k.offset + 1))))
                        }
                        x => panic!("Unexpected value {x:?} for key {k:?}")
                    },
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                    Poll::Pending => Poll::Pending,
                }
            }
        }
        let rq = self.0.range(ForestKey::tree_ent_range(parent, offs));
        ReaddirStream{rq}
    }

    /// Lookup a TreeID by its name.
    ///
    /// # Returns
    ///
    /// A tuple of the parent's id (if any) and the tree's id (if it exists)
    pub fn lookup(&self, name: &str) ->
        impl Future<Output=Result<(Option<TreeID>, Option<TreeID>)>> + Send
    {
        let mut tree_id = TreeID(0);
        if name.is_empty() {
            // Special case: the pool's root file system has no parent, and
            // therefore no TreeEnt.  We just need to check that the Tree exists
            let key = ForestKey::tree(tree_id);
            return self.0.get(key)
                .map_ok(move |otv| {
                    let tree_id = otv.map(|tv| {
                        assert!(matches!(tv, ForestValue::Tree(_)));
                        tree_id
                    });
                    (None, tree_id)
                }).boxed();
        }

        let inner = self.0.clone();
        let name2 = name.to_owned();
        async move {
            let mut parent = None;
            for component in name2.split('/') {
                parent = Some(tree_id);
                let te_key = ForestKey::tree_ent(parent.unwrap(), component);
                match inner.get(te_key).await? {
                    Some(ForestValue::TreeEnt(te)) => {
                        assert_eq!(te.name, component);
                        tree_id = te.tree_id;
                    }
                    Some(ForestValue::Tree(tv)) => {
                        panic!("Unexpected ForestValue::Tree for key {te_key:?}: {tv:?}");
                    }
                    None => return Ok((None, None))
                }
            }
            Ok((parent, Some(tree_id)))
        }.boxed()
    }

    /// Lookup a Tree's parent
    ///
    /// # Returns
    ///
    /// The parent's id (if any)
    pub fn lookup_parent(&self, tree_id: TreeID)
        -> impl Future<Output=Result<Option<TreeID>>> + Send
    {
            let key = ForestKey::tree(tree_id);
            self.0.get(key)
            .map_ok(move |otv: Option<ForestValue>| {
                match otv {
                    Some(ForestValue::Tree(tree)) => tree.parent,
                    Some(ForestValue::TreeEnt(te)) => panic!(
                        "Unexpected ForestValue::TreeEnt for key {key:?}: {te:?}"),
                    None => None
                }
            })
            //.flatten()
    }

    /// Open an existing Forest from disk
    pub fn open(idml: Arc<IDML>, tod: TreeOnDisk<RID>) -> Self {
        Self(Arc::new(ITree::open(idml, true, tod)))
    }

    /// Serialize the forest so it may be written to a Label
    pub fn serialize(&self) -> TreeOnDisk<RID> {
        self.0.serialize().unwrap()
    }

    /// List the IDs of all trees.
    pub fn trees(&self) -> impl TryStream<Ok=TreeID, Error=Error>
    {
        self.0.range(..)
        .try_filter_map(|(key, value)| {
            future::ok(
                match value {
                    ForestValue::Tree(_tree) => Some(key.tree_id),
                    ForestValue::TreeEnt(_te) => None,
                }
            )
        })
    }

    /// Remove a Tree from the Forest
    pub async fn unlink(
        &self,
        parent: Option<TreeID>,
        tree_id: TreeID,
        name: &str,
        txg: TxgT
        ) -> Result <()>
    {
        assert!(!name.contains('/'), "name must be terminal component only");
        assert!(parent.is_some() ^ name.is_empty());
        let tree_key = ForestKey::tree(tree_id);
        let tree_fut = self.0.clone().remove(tree_key, txg, Credit::null());
        let child_range = ForestKey::tree_ent_range(tree_id, 0);
        if self.0.clone().range(child_range).next().await.is_some() {
            return Err(Error::EBUSY);
        }
        if let Some(p) = parent {
            let te_key = ForestKey::tree_ent(p, name);
            let te_fut = self.0.clone().remove(te_key, txg, Credit::null());
            future::try_join(tree_fut, te_fut).await?;
        } else {
            tree_fut.await?;
        }
        Ok(())
    }

    /// Write out a tree root for an already-existing Tree
    pub async fn update_tree(&self,
                             tree_id: TreeID,
                             tod: TreeOnDisk<RID>,
                             txg: TxgT)
        -> Result<Option<TreeOnDisk<RID>>>
    {
        let key = ForestKey::tree(tree_id);
        let parent = match self.0.get(key).await? {
            None => return Err(Error::ENOENT),
            Some(ForestValue::Tree(tree)) => tree.parent,
            Some(ForestValue::TreeEnt(te)) =>
                panic!("TreeEnt unexpected with offset 0 {te:?}")
        };
        let v = ForestValue::Tree(Tree::new(parent, tod));
        match self.0.clone().insert(key, v, txg, Credit::null()).await? {
            Some(ForestValue::Tree(tree)) => Ok(Some(tree.tod)),
            Some(ForestValue::TreeEnt(te)) =>
                panic!("TreeEnt unexpected with offset 0 {te:?}"),
            None => Ok(None)
        }
    }
}

#[cfg(test)]
impl From<ITree<ForestKey, ForestValue>> for Forest {
    fn from(mock_tree: ITree<ForestKey, ForestValue>) -> Self {
        Self(Arc::new(mock_tree))
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {
mod forest_key {
    use pretty_assertions::assert_eq;
    use super::super::*;

    #[test]
    fn typical_size() {
        let key = ForestKey::min_value();
        let size = bincode::serialized_size(&key).unwrap() as usize;
        assert_eq!(ForestKey::TYPICAL_SIZE, size);
    }
}
}
