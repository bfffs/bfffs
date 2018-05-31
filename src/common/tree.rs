// vim: tw=80

//! COW B+-Trees, based on B-trees, Shadowing, and Clones[^CowBtrees]
//!
//! [^CowBtrees]: Rodeh, Ohad. "B-trees, shadowing, and clones." ACM Transactions on Storage (TOS) 3.4 (2008): 2.

use bincode;
use common::*;
use common::ddml::*;
use futures::{Future, future, future::IntoFuture};
use futures_locks::*;
use nix::{Error, errno};
use serde::{Serialize, Serializer, de::{Deserializer, DeserializeOwned}};
#[cfg(test)] use serde_yaml;
#[cfg(test)] use std::fmt::{self, Display, Formatter};
#[cfg(test)] use simulacrum::*;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    fmt::Debug,
    mem,
    rc::Rc,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering}
};

mod atomic_usize_serializer {
    use super::*;
    use serde::Deserialize;

    pub fn deserialize<'de, D>(d: D) -> Result<AtomicUsize, D::Error>
        where D: Deserializer<'de>
    {
        usize::deserialize(d)
            .map(|u| AtomicUsize::new(u))
    }

    pub fn serialize<S>(x: &AtomicUsize, s: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        s.serialize_u64(x.load(Ordering::Relaxed) as u64)
    }
}

#[cfg(test)]
pub struct DDMLMock {
    e: Expectations
}
#[cfg(test)]
impl DDMLMock {
    pub fn new() -> Self {
        Self {
            e: Expectations::new()
        }
    }

    pub fn delete(&self, drp: &DRP) {
        self.e.was_called::<*const DRP, ()>("delete", drp as *const DRP)
    }

    pub fn expect_delete(&mut self) -> Method<*const DRP, ()> {
        self.e.expect::<*const DRP, ()>("delete")
    }

    pub fn evict(&self, drp: &DRP) {
        self.e.was_called::<*const DRP, ()>("evict", drp as *const DRP)
    }

    pub fn get<T: CacheRef>(&self, drp: &DRP)
        -> Box<Future<Item=Box<T>, Error=Error>> {
        self.e.was_called_returning::<*const DRP,
            Box<Future<Item=Box<T>, Error=Error>>>
            ("get", drp as *const DRP)
    }

    pub fn expect_get<T: CacheRef>(&mut self) -> Method<*const DRP,
        Box<Future<Item=Box<T>, Error=Error>>>
    {
        self.e.expect::<*const DRP, Box<Future<Item=Box<T>, Error=Error>>>
            ("get")
    }

    pub fn pop<T: Cacheable>(&self, drp: &DRP)
        -> Box<Future<Item=Box<T>, Error=Error>>
    {
        self.e.was_called_returning::<*const DRP,
            Box<Future<Item=Box<T>, Error=Error>>>
            ("pop", drp as *const DRP)
    }

    pub fn put<T: Cacheable>(&self, cacheable: T, compression: Compression)
        -> (DRP, Box<Future<Item=(), Error=Error>>)
    {
        self.e.was_called_returning::<(T, Compression),
                                      (DRP, Box<Future<Item=(), Error=Error>>)>
            ("put", (cacheable, compression))
    }

    pub fn expect_put<T: Cacheable>(&mut self) -> Method<(T, Compression),
        (DRP, Box<Future<Item=(), Error=Error>>)>
    {
        self.e.expect::<(T, Compression),
                        (DRP, Box<Future<Item=(), Error=Error>>)>
            ("put")
    }

    pub fn sync_all(&self) -> Box<Future<Item=(), Error=Error>> {
        self.e.was_called_returning::<(), Box<Future<Item=(), Error=Error>>>
            ("sync_all", ())
    }

    pub fn expect_sync_all(&mut self)
        -> Method<(), Box<Future<Item=(), Error=Error>>>
    {
        self.e.expect::<(), Box<Future<Item=(), Error=Error>>>("sync_all")
    }
}

#[cfg(test)]
pub type DDMLLike = DDMLMock;
#[cfg(not(test))]
#[doc(hidden)]
pub type DDMLLike = DDML;

/// Anything that has a min_value method.  Too bad libstd doesn't define this.
pub trait MinValue {
    fn min_value() -> Self;
}

impl MinValue for u32 {
    fn min_value() -> Self {
        u32::min_value()
    }
}

pub trait Key: Copy + Debug + DeserializeOwned + Ord + MinValue + Serialize
    + 'static {}

impl<T> Key for T
where T: Copy + Debug + DeserializeOwned + Ord + MinValue + Serialize
    + 'static {}

pub trait Value: Copy + Debug + DeserializeOwned + Serialize + 'static {}

impl<T> Value for T
where T: Copy + Debug + DeserializeOwned + Serialize + 'static {}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned, V: DeserializeOwned"))]
enum TreePtr<K: Key, V: Value> {
    /// Dirty btree nodes live only in RAM, not on disk or in cache.  Being
    /// RAM-resident, we don't need to store their checksums or lsizes.
    #[cfg_attr(not(test), serde(skip_serializing))]
    #[cfg_attr(not(test), serde(skip_deserializing))]
    #[cfg_attr(test, serde(with = "node_serializer"))]
    Mem(Box<Node<K, V>>),
    /// Direct Record Pointers point directly to a disk location
    DRP(DRP),
    /// Indirect Record Pointers point to the Record Indirection Table
    _IRP(u64),
    /// Used temporarily while syncing nodes to disk.  Should never be visible
    /// during a traversal, because the parent's xlock must be held at all times
    /// while the ptr is None.
    None,
}

impl<K: Key, V: Value> TreePtr<K, V> {
    #[cfg(test)]
    fn as_drp(&self) -> Option<&DRP> {
        if let &TreePtr::DRP(ref drp) = self {
            Some(drp)
        } else {
            None
        }
    }

    fn into_node(self) -> Result<Box<Node<K, V>>, TreePtr<K, V>> {
        if let TreePtr::Mem(node) = self {
            Ok(node)
        } else {
            Err(self)
        }
    }

    fn is_dirty(&self) -> bool {
        self.is_mem()
    }

    fn is_mem(&self) -> bool {
        if let &TreePtr::Mem(_) = self {
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod node_serializer {
    use super::*;
    use serde::Deserialize;
    use tokio::executor::current_thread;

    pub(super) fn deserialize<'de, D, K, V>(deserializer: D)
        -> Result<Box<Node<K, V>>, D::Error>
        where D: Deserializer<'de>, K: Key, V: Value
    {
        NodeData::deserialize(deserializer)
            .map(|node_data| Box::new(Node(RwLock::new(node_data))))
    }

    pub(super) fn serialize<S, K, V>(node: &Node<K, V>,
                                     serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer, K: Key, V: Value {

        let guard = current_thread::block_on_all(node.0.read()).unwrap();
        (*guard).serialize(serializer)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned, V: DeserializeOwned"))]
struct LeafData<K: Key, V> {
    items: BTreeMap<K, V>
}

impl<K: Key, V: Value> LeafData<K, V> {
    fn split(&mut self) -> (K, LeafData<K, V>) {
        // Split the node in two.  Make the left node larger, on the assumption
        // that we're more likely to insert into the right node than the left
        // one.
        let half = div_roundup(self.items.len(), 2);
        let cutoff = *self.items.keys().nth(half).unwrap();
        let new_items = self.items.split_off(&cutoff);
        (cutoff, LeafData{items: new_items})
    }
}

impl<K: Key, V: Value> LeafData<K, V> {
    fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.items.insert(k, v)
    }

    fn lookup(&self, k: K) -> Result<V, Error> {
        self.items.get(&k)
            .cloned()
            .ok_or(Error::Sys(errno::Errno::ENOENT))
    }

    fn remove(&mut self, k: K) -> Option<V> {
        self.items.remove(&k)
    }
}

/// Guard that holds the Node lock object for reading
enum TreeReadGuard<K: Key, V: Value> {
    Mem(RwLockReadGuard<NodeData<K, V>>),
    DRP(RwLockReadGuard<NodeData<K, V>>, Node<K, V>)
}

impl<K: Key, V: Value> Deref for TreeReadGuard<K, V> {
    type Target = NodeData<K, V>;

    fn deref(&self) -> &Self::Target {
        match self {
            &TreeReadGuard::Mem(ref guard) => &**guard,
            &TreeReadGuard::DRP(ref guard, _) => &**guard,
        }
    }
}

/// Guard that holds the Node lock object for writing
enum TreeWriteGuard<K: Key, V: Value> {
    Mem(RwLockWriteGuard<NodeData<K, V>>),
    DRP(RwLockWriteGuard<NodeData<K, V>>, Node<K, V>)
}

impl<K: Key, V: Value> Deref for TreeWriteGuard<K, V> {
    type Target = NodeData<K, V>;

    fn deref(&self) -> &Self::Target {
        match self {
            &TreeWriteGuard::Mem(ref guard) => &**guard,
            &TreeWriteGuard::DRP(ref guard, _) => &**guard,
        }
    }
}

impl<K: Key, V: Value> DerefMut for TreeWriteGuard<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            &mut TreeWriteGuard::Mem(ref mut guard) => &mut **guard,
            &mut TreeWriteGuard::DRP(ref mut guard, _) => &mut **guard,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
struct IntElem<K: Key + DeserializeOwned, V: Value> {
    key: K,
    ptr: TreePtr<K, V>
}

impl<'a, K: Key, V: Value> IntElem<K, V> {
    /// Is the child node dirty?  That is, does it differ from the on-disk
    /// version?
    fn is_dirty(&mut self) -> bool {
        self.ptr.is_dirty()
    }

    fn read(&self, ddml: &'a DDMLLike, drp: &DRP) -> Box<Future<Item=Node<K, V>, Error=Error> + 'a> {
        Box::new(
            ddml.get(drp)
                .map(|db: Box<DivBuf>| {
                    let node_data: NodeData<K, V> =
                        bincode::deserialize(&db[..]).unwrap();
                    // TODO: cache the deserialized NodeData
                    Node(RwLock::new(node_data))
                })
        )
    }

    fn rlock(&self, ddml: &'a DDMLLike)
        -> Box<Future<Item=TreeReadGuard<K, V>, Error=Error> + 'a>
    {
        match self.ptr {
            TreePtr::Mem(ref node) => {
                Box::new(
                    node.0.read()
                        .map(|guard| TreeReadGuard::Mem(guard))
                        .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                )
            },
            TreePtr::DRP(ref drp) => {
                Box::new(
                    self.read(ddml, drp).and_then(|node| {
                        node.0.read()
                            .map(move |guard| {
                                TreeReadGuard::DRP(guard, node)
                            })
                            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                    })
                )
            },
            _ => {
                unimplemented!()
            }
        }
    }

    fn xlock(&self, ddml: &'a DDMLLike)
        -> Box<Future<Item=TreeWriteGuard<K, V>, Error=Error> + 'a>
    {
        match self.ptr {
            TreePtr::Mem(ref node) => {
                Box::new(
                    node.0.write()
                        .map(|guard| TreeWriteGuard::Mem(guard))
                        .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                )
            },
            TreePtr::DRP(ref drp) => {
                Box::new(
                    self.read(ddml, drp).and_then(|node| {
                        node.0.write()
                            .map(move |guard| {
                                TreeWriteGuard::DRP(guard, node)
                            })
                            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                    })
                )
            },
            _ => {
                unimplemented!()
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
struct IntData<K: Key, V: Value> {
    children: Vec<IntElem<K, V>>
}

impl<K: Key, V: Value> IntData<K, V> {
    fn position(&self, k: &K) -> usize {
        // Find rightmost child whose key is less than or equal to k
        self.children
            .binary_search_by_key(k, |ref child| child.key)
            .unwrap_or_else(|k| k - 1)
    }

    fn split(&mut self) -> (K, IntData<K, V>) {
        // Split the node in two.  Make the left node larger, on the assumption
        // that we're more likely to insert into the right node than the left
        // one.
        let cutoff = div_roundup(self.children.len(), 2);
        let new_children = self.children.split_off(cutoff);
        (new_children[0].key, IntData{children: new_children})
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
enum NodeData<K: Key, V: Value> {
    Leaf(LeafData<K, V>),
    Int(IntData<K, V>),
}

impl<K: Key, V: Value> NodeData<K, V> {
    fn as_int(&self) -> Option<&IntData<K, V>> {
        if let &NodeData::Int(ref int) = self {
            Some(int)
        } else {
            None
        }
    }

    fn as_int_mut(&mut self) -> Option<&mut IntData<K, V>> {
        if let &mut NodeData::Int(ref mut int) = self {
            Some(int)
        } else {
            None
        }
    }

    fn as_leaf_mut(&mut self) -> Option<&mut LeafData<K, V>> {
        if let &mut NodeData::Leaf(ref mut leaf) = self {
            Some(leaf)
        } else {
            None
        }
    }

    /// Can this child be merged with `other` without violating constraints?
    fn can_merge(&self, other: &NodeData<K, V>, max_fanout: usize) -> bool {
        self.len() + other.len() <= max_fanout
    }

    /// Return this `NodeData`s lower bound key, suitable for use in its
    /// parent's `children` array.
    fn key(&self) -> K {
        match self {
            &NodeData::Leaf(ref leaf) => *leaf.items.keys().nth(0).unwrap(),
            &NodeData::Int(ref int) => int.children[0].key,
        }
    }

    /// Number of children or items in this `NodeData`
    fn len(&self) -> usize {
        match self {
            &NodeData::Leaf(ref leaf) => leaf.items.len(),
            &NodeData::Int(ref int) => int.children.len()
        }
    }

    /// Should this node be fixed because it's too small?
    fn should_fix(&self, min_fanout: usize) -> bool {
        let len = self.len();
        debug_assert!(len >= min_fanout,
                      "Underfull nodes shouldn't be possible");
        len <= min_fanout
    }

    /// Should this node be split because it's too big?
    fn should_split(&self, max_fanout: usize) -> bool {
        let len = self.len();
        debug_assert!(len <= max_fanout,
                      "Overfull nodes shouldn't be possible");
        len >= max_fanout
    }

    fn split(&mut self) -> (K, NodeData<K, V>) {
        match *self {
            NodeData::Leaf(ref mut leaf) => {
                let (k, new_leaf) = leaf.split();
                (k, NodeData::Leaf(new_leaf))
            },
            NodeData::Int(ref mut int) => {
                let (k, new_int) = int.split();
                (k, NodeData::Int(new_int))
            },

        }
    }

    /// Merge all of `other`'s data into `self`.  Afterwards, `other` may be
    /// deleted.
    fn merge(&mut self, other: &mut NodeData<K, V>) {
        match *self {
            NodeData::Int(ref mut int) =>
                int.children.append(&mut other.as_int_mut().unwrap().children),
            NodeData::Leaf(ref mut leaf) =>
                leaf.items.append(&mut other.as_leaf_mut().unwrap().items),
        }
    }

    /// Take `other`'s highest keys and merge them into ourself
    fn take_high_keys(&mut self, other: &mut NodeData<K, V>) {
        let keys_to_share = (other.len() - self.len()) / 2;
        match *self {
            NodeData::Int(ref mut int) => {
                let other_children = &mut other.as_int_mut().unwrap().children;
                let cutoff_idx = other_children.len() - keys_to_share;
                let mut other_right_half =
                    other_children.split_off(cutoff_idx);
                int.children.splice(0..0, other_right_half.into_iter());
            },
            NodeData::Leaf(ref mut leaf) => {
                let other_items = &mut other.as_leaf_mut().unwrap().items;
                let cutoff_idx = other_items.len() - keys_to_share;
                let cutoff = *other_items.keys().nth(cutoff_idx).unwrap();
                let mut other_right_half = other_items.split_off(&cutoff);
                leaf.items.append(&mut other_right_half);
            }
        }
    }

    /// Take `other`'s lowest keys and merge them into ourself
    fn take_low_keys(&mut self, other: &mut NodeData<K, V>) {
        let keys_to_share = (other.len() - self.len()) / 2;
        match *self {
            NodeData::Int(ref mut int) => {
                let other_children = &mut other.as_int_mut().unwrap().children;
                let other_left_half = other_children.drain(0..keys_to_share);
                let nchildren = int.children.len();
                int.children.splice(nchildren.., other_left_half);
            },
            NodeData::Leaf(ref mut leaf) => {
                let other_items = &mut other.as_leaf_mut().unwrap().items;
                let cutoff = *other_items.keys().nth(keys_to_share).unwrap();
                let other_right_half = other_items.split_off(&cutoff);
                let mut other_left_half =
                    mem::replace(other_items, other_right_half);
                leaf.items.append(&mut other_left_half);
            }
        }
    }
}

#[derive(Debug)]
struct Node<K: Key, V: Value> (RwLock<NodeData<K, V>>);

mod tree_root_serializer {
    use super::*;
    use serde::{Deserialize, ser::Error};

    pub(super) fn deserialize<'de, D, K, V>(d: D)
        -> Result<RwLock<IntElem<K, V>>, D::Error>
        where D: Deserializer<'de>, K: Key, V: Value
    {
        IntElem::deserialize(d)
            .map(|int_elem| RwLock::new(int_elem))
    }

    pub(super) fn serialize<K, S, V>(x: &RwLock<IntElem<K, V>>, s: S)
        -> Result<S::Ok, S::Error>
        where K: Key, S: Serializer, V: Value
    {
        match x.try_read() {
            Ok(guard) => (*guard).serialize(s),
            Err(_) => Err(S::Error::custom("EAGAIN"))
        }
    }
}

#[derive(Debug)]
#[derive(Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
struct Inner<K: Key, V: Value> {
    /// Tree height.  1 if the Tree consists of a single Leaf node.
    // Use atomics so it can be modified from an immutable reference.  Accesses
    // should be very rare, so performance is not a concern.
    #[serde(with = "atomic_usize_serializer")]
    height: AtomicUsize,
    /// Minimum node fanout.  Smaller nodes will be merged, or will steal
    /// children from their neighbors.
    min_fanout: usize,
    /// Maximum node fanout.  Larger nodes will be split.
    max_fanout: usize,
    /// Maximum node size in bytes.  Larger nodes will be split or their message
    /// buffers flushed
    _max_size: usize,
    /// Root node
    #[serde(with = "tree_root_serializer")]
    root: RwLock<IntElem<K, V>>
}

/// In-memory representation of a COW B+-Tree
///
/// # Generic Parameters
///
/// *`K`:   Key type.  Must be ordered and copyable; should be compact
/// *`V`:   Value type in the leaves.
pub struct Tree<K: Key, V: Value> {
    ddml: DDMLLike,
    i: Inner<K, V>
}

impl<'a, K: Key, V: Value> Tree<K, V> {
    #[cfg(not(test))]
    pub fn create(ddml: DDML) -> Self {
        Tree::new(ddml,
                  4,        // BetrFS's min fanout
                  16,       // BetrFS's max fanout
                  1<<22,    // BetrFS's max size
        )
    }

    #[cfg(test)]
    pub fn from_str(ddml: DDMLLike, s: &str) -> Self {
        let i: Inner<K, V> = serde_yaml::from_str(s).unwrap();
        Tree{ddml, i}
    }

    /// Insert value `v` into the tree at key `k`, returning the previous value
    /// for that key, if any.
    pub fn insert(&'a self, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        Box::new(
            self.i.root.read()
                .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                .and_then(move |guard| {
                    guard.xlock(&self.ddml)
                         .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                         .and_then(move |guard| {
                             self.insert_locked(guard, k, v)
                         })
                })
        )
    }

    /// Insert value `v` into an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn insert_int(&'a self, mut parent: TreeWriteGuard<K, V>,
                  child_idx: usize,
                  mut child: TreeWriteGuard<K, V>, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        // First, split the node, if necessary
        if (*child).should_split(self.i.max_fanout) {
            let (new_key, new_node_data) = child.split();
            let new_node = Node(RwLock::new(new_node_data));
            let new_ptr = TreePtr::Mem(Box::new(new_node));
            let new_elem = IntElem{key: new_key, ptr: new_ptr};
            parent.as_int_mut().unwrap()
                .children.insert(child_idx + 1, new_elem);
            // Reinsert into the parent, which will choose the correct child
            self.insert_no_split(parent, k, v)
        } else {
            drop(parent);
            self.insert_no_split(child, k, v)
        }
    }

    /// Helper for `insert`.  Handles insertion once the tree is locked
    fn insert_locked(&'a self, mut root: TreeWriteGuard<K, V>, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        // First, split the root node, if necessary
        if root.should_split(self.i.max_fanout) {
            let (new_key, new_node_data) = root.split();
            let new_node = Node(RwLock::new(new_node_data));
            let new_ptr = TreePtr::Mem(Box::new(new_node));
            let new_elem = IntElem{key: new_key, ptr: new_ptr};
            let new_root_data = NodeData::Int(
                IntData {
                    children: vec![new_elem]
                }
            );
            let old_root_data = mem::replace(root.deref_mut(), new_root_data);
            let old_root_node = Node(RwLock::new(old_root_data));
            let old_ptr = TreePtr::Mem(Box::new(old_root_node));
            let old_elem = IntElem{ key: K::min_value(), ptr: old_ptr };
            root.as_int_mut().unwrap().children.insert(0, old_elem);
            self.i.height.fetch_add(1, Ordering::Relaxed);
        }

        self.insert_no_split(root, k, v)
    }

    fn insert_no_split(&'a self, mut node: TreeWriteGuard<K, V>, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        let (child_idx, child_fut) = match *node {
            NodeData::Leaf(ref mut leaf) => {
                return Box::new(Ok(leaf.insert(k, v)).into_future())
            },
            NodeData::Int(ref int) => {
                let child_idx = int.position(&k);
                let fut = int.children[child_idx].xlock(&self.ddml);
                (child_idx, fut)
            }
        };
        Box::new(child_fut.and_then(move |child| {
                self.insert_int(node, child_idx, child, k, v)
            })
        )
    }

    /// Lookup the value of key `k`.  Return an error if no value is present.
    pub fn lookup(&'a self, k: K) -> Box<Future<Item=V, Error=Error> + 'a> {
        Box::new(
            self.i.root.read()
                .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                .and_then(move |guard| {
                    guard.rlock(&self.ddml)
                         .and_then(move |guard| self.lookup_node(guard, k))
                })
        )
    }

    /// Lookup the value of key `k` in a node, which must already be locked.
    fn lookup_node(&'a self, node: TreeReadGuard<K, V>, k: K)
        -> Box<Future<Item=V, Error=Error> + 'a> {

        let next_node_fut = match *node {
            NodeData::Leaf(ref leaf) => {
                return Box::new(leaf.lookup(k).into_future())
            },
            NodeData::Int(ref int) => {
                let child_elem = &int.children[int.position(&k)];
                child_elem.rlock(&self.ddml)
            }
        };
        drop(node);
        Box::new(
            next_node_fut
            .and_then(move |next_node| self.lookup_node(next_node, k))
        )
    }

    fn new(ddml: DDMLLike, min_fanout: usize, max_fanout: usize,
           max_size: usize) -> Self
    {
        let i: Inner<K, V> = Inner {
            height: AtomicUsize::new(1),
            min_fanout, max_fanout,
            _max_size: max_size,
            root: RwLock::new(
                IntElem{
                    key: K::min_value(),
                    ptr:
                        TreePtr::Mem(
                            Box::new(
                                Node(
                                    RwLock::new(
                                        NodeData::Leaf(
                                            LeafData{
                                                items: BTreeMap::new()
                                            }
                                        )
                                    )
                                )
                            )
                        )
                }
            )
        };
        Tree{ ddml, i }
    }

    /// Remove and return the value at key `k`, if any.
    pub fn remove(&'a self, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        Box::new(
            self.i.root.read()
                .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                .and_then(move |guard| {
                    guard.xlock(&self.ddml)
                        .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                        .and_then(move |guard| {
                            self.remove_locked(guard, k)
                        })
            })
        )
    }

    /// Remove key `k` from an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn remove_int(&'a self, mut parent: TreeWriteGuard<K, V>,
                  child_idx: usize, mut child: TreeWriteGuard<K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        // First, fix the node, if necessary
        if child.should_fix(self.i.min_fanout) {
            // Outline:
            // First, try to merge with the right sibling
            // Then, try to steal keys from the right sibling
            // Then, try to merge with the left sibling
            // Then, try to steal keys from the left sibling
            let nchildren = parent.as_int().unwrap().children.len();
            let (fut, right) = if child_idx < nchildren - 1 {
                (parent.as_int_mut().unwrap().children[child_idx + 1]
                 .xlock(&self.ddml),
                 true)
            } else {
                (parent.as_int_mut().unwrap().children[child_idx - 1]
                 .xlock(&self.ddml),
                 false)
            };
            Box::new(
                fut.map(move |mut sibling| {
                    if right {
                        if child.can_merge(&sibling, self.i.max_fanout) {
                            child.merge(&mut sibling);
                            parent.as_int_mut().unwrap()
                                .children.remove(child_idx + 1);
                        } else {
                            child.take_low_keys(&mut sibling);
                            parent.as_int_mut().unwrap().children[child_idx+1]
                                .key = sibling.key();
                        }
                    } else {
                        if sibling.can_merge(&child, self.i.max_fanout) {
                            sibling.merge(&mut child);
                            parent.as_int_mut().unwrap()
                                .children.remove(child_idx);
                        } else {
                            child.take_high_keys(&mut sibling);
                            parent.as_int_mut().unwrap().children[child_idx]
                                .key = child.key();
                        }
                    };
                    parent
                }).and_then(move |parent| self.remove_no_fix(parent, k))
            )
        } else {
            drop(parent);
            self.remove_no_fix(child, k)
        }
    }

    /// Helper for `remove`.  Handles removal once the tree is locked
    fn remove_locked(&'a self, mut root: TreeWriteGuard<K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        // First, fix the root node, if necessary
        let new_root_data = if let NodeData::Int(ref mut int) = *root {
            if int.children.len() == 1 {
                // Merge root node with its child
                let child = int.children.pop().unwrap();
                Some(match child.ptr {
                    TreePtr::Mem(node) => node.0.try_unwrap().unwrap(),
                    _ => unimplemented!()
                })
            } else {
                None
            }
        } else {
            None
        };
        if new_root_data.is_some() {
            mem::replace(root.deref_mut(), new_root_data.unwrap());
            self.i.height.fetch_sub(1, Ordering::Relaxed);
        }

        self.remove_no_fix(root, k)
    }

    /// Remove key `k` from a node, but don't try to fixup the node.
    fn remove_no_fix(&'a self, mut node: TreeWriteGuard<K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        let (child_idx, child_fut) = match *node {
            NodeData::Leaf(ref mut leaf) => {
                return Box::new(Ok(leaf.remove(k)).into_future())
            },
            NodeData::Int(ref int) => {
                let child_idx = int.position(&k);
                let fut = int.children[child_idx].xlock(&self.ddml);
                (child_idx, fut)
            }
        };
        Box::new(child_fut.and_then(move |child| {
                self.remove_int(node, child_idx, child, k)
            })
        )
    }

    /// Sync all records written so far to stable storage.
    pub fn sync_all(&'a self) -> Box<Future<Item=(), Error=Error> + 'a> {
        Box::new( self.i.root.write()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                  .and_then(move |mut root_guard| {
            if root_guard.ptr.is_dirty() {
                // If the root is dirty, then we have ownership over it.  But
                // another task may still have a lock on it.  We must acquire
                // then release the lock to ensure that we have the sole
                // reference.
                let fut = root_guard.xlock(&self.ddml).and_then(move |guard| {
                    drop(guard);
                    let ptr = mem::replace(&mut root_guard.ptr, TreePtr::None);
                    Box::new(
                        self.write_node(ptr.into_node().unwrap())
                            .and_then(move |drp| {
                                root_guard.ptr = TreePtr::DRP(drp);
                                self.ddml.sync_all()
                            })
                    )
                });
                Box::new(fut) as Box<Future<Item=(), Error=Error>>
            } else {
                Box::new(future::ok::<(), Error>(()))
            }
        }))
    }

    fn write_leaf(&'a self, node: &NodeData<K, V>)
        -> Box<Future<Item=DRP, Error=Error> + 'a>
    {
        let buf = DivBufShared::from(bincode::serialize(&node).unwrap());
        let (drp, fut) = self.ddml.put(buf, Compression::None);
        Box::new(fut.map(move |_| drp))
    }

    fn write_node(&'a self, node: Box<Node<K, V>>)
        -> Box<Future<Item=DRP, Error=Error> + 'a>
    {
        let ndata = node.0.try_unwrap().unwrap();
        if let NodeData::Leaf(_) = ndata {
            return self.write_leaf(&ndata);
        }

        // Rust's borrow checker doesn't understand that children_fut will
        // complete before its continuation will run, so it won't let ndata
        // be borrowed in both places.  So we'll have to use RefCell to allow
        // dynamic borrowing and Rc to allow moving into both closures.
        let rndata = Rc::new(RefCell::new(ndata));
        let rndata2 = rndata.clone();
        let nchildren = rndata.borrow().as_int().unwrap().children.len();
        let children_fut = (0..nchildren)
        .filter_map(move |idx| {
            let rndata3 = rndata.clone();
            if rndata.borrow_mut()
                     .as_int_mut().unwrap()
                     .children[idx].is_dirty()
            {
                // If the child is dirty, then we have ownership over it.  We
                // need to lock it, then release the lock.  Then we'll know that
                // we have exclusive access to it, and we can move it into the
                // Cache.
                let fut = rndata.borrow_mut()
                                .as_int_mut().unwrap()
                                .children[idx]
                                .xlock(&self.ddml)
                                .and_then(move |guard| {
                    drop(guard);

                    let ptr = mem::replace(&mut rndata3.borrow_mut()
                                                       .as_int_mut().unwrap()
                                                       .children[idx].ptr,
                                           TreePtr::None);
                    self.write_node(ptr.into_node().unwrap())
                        .map(move |drp| {
                            rndata3.borrow_mut()
                                   .as_int_mut().unwrap()
                                   .children[idx].ptr = TreePtr::DRP(drp);
                        })
                });
                Some(fut)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
        Box::new(
            future::join_all(children_fut)
            .and_then(move |_| {
                let n: &NodeData<K, V> = &*rndata2.borrow();
                let buf = DivBufShared::from(bincode::serialize(n).unwrap());
                let (drp, fut) = self.ddml.put(buf, Compression::None);
                fut.map(move |_| drp)
            })
        )
    }
}

#[cfg(test)]
impl<K: Key, V: Value> Display for Tree<K, V> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(&serde_yaml::to_string(&self.i).unwrap())
    }
}



// LCOV_EXCL_START
/// Tests regarding in-memory manipulation of Trees
#[cfg(test)]
mod in_mem {

use super::*;
use futures::future;
use tokio::executor::current_thread;

#[test]
fn insert() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let r = current_thread::block_on_all(tree.insert(0, 0.0));
    assert_eq!(r, Ok(None));
    assert_eq!(format!("{}", tree),
r#"---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Leaf:
        items:
          0: 0.0"#);
}

#[test]
fn insert_dup() {
    let ddml = DDMLMock::new();
    let tree = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Leaf:
        items:
          0: 0.0
"#);
    let r = current_thread::block_on_all(tree.insert(0, 100.0));
    assert_eq!(r, Ok(Some(0.0)));
    assert_eq!(format!("{}", tree),
r#"---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Leaf:
        items:
          0: 100.0"#);
}

/// Insert a key that splits a non-root interior node
#[test]
fn insert_split_int() {
    let ddml = DDMLMock::new();
    let tree = Tree::from_str(ddml, r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                              2: 2.0
                    - key: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                              5: 5.0
                    - key: 6
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
                              8: 8.0
          - key: 9
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                              11: 11.0
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                              14: 14.0
                    - key: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                              17: 17.0
                    - key: 18
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                              20: 20.0
                    - key: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                              23: 23.0"#);
    let r2 = current_thread::block_on_all(tree.insert(24, 24.0));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                              2: 2.0
                    - key: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                              5: 5.0
                    - key: 6
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
                              8: 8.0
          - key: 9
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                              11: 11.0
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                              14: 14.0
                    - key: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                              17: 17.0
          - key: 18
            ptr:
              Mem:
                Int:
                  children:
                    - key: 18
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                              20: 20.0
                    - key: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                              23: 23.0
                              24: 24.0"#);
}

/// Insert a key that splits a non-root leaf node
#[test]
fn insert_split_leaf() {
    let ddml = DDMLMock::new();
    let tree = Tree::from_str(ddml, r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
                    2: 2.0
          - key: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
                    5: 5.0
                    6: 6.0
                    7: 7.0
"#);
    let r2 = current_thread::block_on_all(tree.insert(8, 8.0));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", tree),
r#"---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
                    2: 2.0
          - key: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
                    5: 5.0
          - key: 6
            ptr:
              Mem:
                Leaf:
                  items:
                    6: 6.0
                    7: 7.0
                    8: 8.0"#);
}

/// Insert a key that splits the root IntNode
#[test]
fn insert_split_root_int() {
    let ddml = DDMLMock::new();
    let tree = Tree::from_str(ddml, r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
                    2: 2.0
          - key: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
                    5: 5.0
          - key: 6
            ptr:
              Mem:
                Leaf:
                  items:
                    6: 6.0
                    7: 7.0
                    8: 8.0
          - key: 9
            ptr:
              Mem:
                Leaf:
                  items:
                    9: 9.0
                    10: 10.0
                    11: 11.0
          - key: 12
            ptr:
              Mem:
                Leaf:
                  items:
                    12: 12.0
                    13: 13.0
                    14: 14.0
"#);
    let r2 = current_thread::block_on_all(tree.insert(15, 15.0));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                              2: 2.0
                    - key: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                              5: 5.0
                    - key: 6
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
                              8: 8.0
          - key: 9
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                              11: 11.0
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                              14: 14.0
                              15: 15.0"#);
}

/// Insert a key that splits the root leaf node
#[test]
fn insert_split_root_leaf() {
    let ddml = DDMLMock::new();
    let tree = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Leaf:
        items:
          0: 0.0
          1: 1.0
          2: 2.0
          3: 3.0
          4: 4.0
"#);
    let r2 = current_thread::block_on_all(tree.insert(5, 5.0));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
                    2: 2.0
          - key: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
                    5: 5.0"#);
}

#[test]
fn lookup() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let r = current_thread::block_on_all(future::lazy(|| {
        tree.insert(0, 0.0)
            .and_then(|_| tree.lookup(0))
    }));
    assert_eq!(r, Ok(0.0));
}

#[test]
fn lookup_nonexistent() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let r = current_thread::block_on_all(tree.lookup(0));
    assert_eq!(r, Err(Error::Sys(errno::Errno::ENOENT)))
}

#[test]
fn remove_last_key() {
    let ddml = DDMLMock::new();
    let tree = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Leaf:
        items:
          0: 0.0
"#);
    let r = current_thread::block_on_all(tree.remove(0));
    assert_eq!(r, Ok(Some(0.0)));
    assert_eq!(format!("{}", tree),
r#"---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Leaf:
        items: {}"#);
}

#[test]
fn remove_from_leaf() {
    let ddml = DDMLMock::new();
    let tree = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Leaf:
        items:
          0: 0.0
          1: 1.0
          2: 2.0
"#);
    let r = current_thread::block_on_all(tree.remove(1));
    assert_eq!(r, Ok(Some(1.0)));
    assert_eq!(format!("{}", tree),
r#"---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Leaf:
        items:
          0: 0.0
          2: 2.0"#);
}

#[test]
fn remove_and_merge_int_left() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::from_str(ddml, r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                    - key: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                    - key: 6
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
          - key: 9
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
          - key: 18
            ptr:
              Mem:
                Int:
                  children:
                    - key: 18
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                    - key: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                              23: 23.0"#);
    let r2 = current_thread::block_on_all(tree.remove(23));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                    - key: 3
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              3: 3.0
                              4: 4.0
                    - key: 6
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
          - key: 9
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                    - key: 18
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                    - key: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0"#);
}

#[test]
fn remove_and_merge_int_right() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::from_str(ddml, r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                    - key: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 2.0
                              3: 3.0
                              4: 4.0
          - key: 9
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
          - key: 18
            ptr:
              Mem:
                Int:
                  children:
                    - key: 18
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                    - key: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0"#);
    let r2 = current_thread::block_on_all(tree.remove(4));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                    - key: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 2.0
                              3: 3.0
                    - key: 9
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
          - key: 18
            ptr:
              Mem:
                Int:
                  children:
                    - key: 18
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              18: 18.0
                              19: 19.0
                    - key: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0"#);
}

#[test]
fn remove_and_merge_leaf_left() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::from_str(ddml, r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
          - key: 5
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    7: 7.0
"#);
    let r2 = current_thread::block_on_all(tree.remove(7));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
                    5: 5.0"#);
}

#[test]
fn remove_and_merge_leaf_right() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::from_str(ddml, r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
          - key: 5
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    6: 6.0
                    7: 7.0
"#);
    let r2 = current_thread::block_on_all(tree.remove(4));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    5: 5.0
                    6: 6.0
                    7: 7.0"#);
}

#[test]
fn remove_and_steal_int_left() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::from_str(ddml, r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                    - key: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 2.0
                              3: 3.0
          - key: 9
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                    - key: 17
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              17: 17.0
                              18: 18.0
                    - key: 19
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              19: 19.0
                              20: 20.0
          - key: 21
            ptr:
              Mem:
                Int:
                  children:
                    - key: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                    - key: 24
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              25: 25.0
                              26: 26.0"#);
    let r2 = current_thread::block_on_all(tree.remove(26));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                    - key: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 2.0
                              3: 3.0
          - key: 9
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                    - key: 17
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              17: 17.0
                              18: 18.0
          - key: 19
            ptr:
              Mem:
                Int:
                  children:
                    - key: 19
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              19: 19.0
                              20: 20.0
                    - key: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                    - key: 24
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              25: 25.0"#);
}

#[test]
fn remove_and_steal_int_right() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::from_str(ddml, r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                    - key: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 2.0
                              3: 3.0
          - key: 9
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                              14: 14.0
          - key: 15
            ptr:
              Mem:
                Int:
                  children:
                    - key: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
                    - key: 17
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              17: 17.0
                              18: 18.0
                    - key: 19
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              19: 19.0
                              20: 20.0
                    - key: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                    - key: 24
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              26: 26.0"#);
    let r2 = current_thread::block_on_all(tree.remove(14));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Int:
                  children:
                    - key: 0
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              0: 0.0
                              1: 1.0
                    - key: 2
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              2: 2.0
                              3: 3.0
          - key: 9
            ptr:
              Mem:
                Int:
                  children:
                    - key: 9
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              9: 9.0
                              10: 10.0
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              12: 12.0
                              13: 13.0
                    - key: 15
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              15: 15.0
                              16: 16.0
          - key: 17
            ptr:
              Mem:
                Int:
                  children:
                    - key: 17
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              17: 17.0
                              18: 18.0
                    - key: 19
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              19: 19.0
                              20: 20.0
                    - key: 21
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              21: 21.0
                              22: 22.0
                    - key: 24
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              26: 26.0"#);
}

#[test]
fn remove_and_steal_leaf_left() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::from_str(ddml, r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 2
            ptr:
              Mem:
                Leaf:
                  items:
                    2: 2.0
                    3: 3.0
                    4: 4.0
                    5: 5.0
                    6: 6.0
          - key: 8
            ptr:
              Mem:
                Leaf:
                  items:
                    8: 8.0
                    9: 9.0
"#);
    let r2 = current_thread::block_on_all(tree.remove(8));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 2
            ptr:
              Mem:
                Leaf:
                  items:
                    2: 2.0
                    3: 3.0
                    4: 4.0
                    5: 5.0
          - key: 6
            ptr:
              Mem:
                Leaf:
                  items:
                    6: 6.0
                    9: 9.0"#);
}

#[test]
fn remove_and_steal_leaf_right() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::from_str(ddml, r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
          - key: 5
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    6: 6.0
                    7: 7.0
                    8: 8.0
                    9: 9.0
"#);
    let r2 = current_thread::block_on_all(tree.remove(4));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
r#"---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0.0
                    1: 1.0
          - key: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    5: 5.0
          - key: 6
            ptr:
              Mem:
                Leaf:
                  items:
                    6: 6.0
                    7: 7.0
                    8: 8.0
                    9: 9.0"#);
}

#[test]
fn remove_nonexistent() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let r = current_thread::block_on_all(tree.remove(3));
    assert_eq!(r, Ok(None));
}

}

#[cfg(test)]
/// Tests regarding disk I/O for Trees
mod io {

use super::*;
use futures::future;
use tokio::executor::current_thread;

#[test]
/// Read an IntNode.  The public API doesn't provide any way to read an IntNode
/// without also reading its children, so we'll test this through the private
/// IntElem::rlock API.
fn read_int() {
    let serialized = vec![ 1u8, 0, 0, 0, // enum variant 0 for IntNode
        2, 0, 0, 0, 0, 0, 0, 0,     // 2 elements in the vector
           0, 0, 0, 0,              // K=0
           1u8, 0, 0, 0,            // enum variant 1 for TreePtr::DRP
               0, 0,                // Cluster 0
               0, 0, 0, 0, 0, 0, 0, 0,  // LBA 0
           0, 0, 0, 0,              // enum variant 0 for Compression::None
           0x40, 0x9c, 0, 0,        // lsize=40000
           0x40, 0x9c, 0, 0,         // csize=40000
           0xef, 0xbe, 0xad, 0xde, 0, 0, 0, 0,  // checksum
           0, 1, 0, 0,              // K=256
           1u8, 0, 0, 0,            // enum variant 1 for TreePtr::DRP
               0, 0,                // Cluster 0
               0, 1, 0, 0, 0, 0, 0, 0,  // LBA 256
           1, 0, 0, 0,              // enum variant 0 for ZstdL9NoShuffle
           0x80, 0x3e, 0, 0,        // lsize=16000
           0x40, 0x1f, 0, 0,        // csize=8000
           0xbe, 0xba, 0x7e, 0x1a, 0, 0, 0, 0,  // checksum
    ];
    let drp = DRP::random(Compression::None, serialized.len());
    let drp2 = drp.clone();
    let dbs = DivBufShared::from(serialized);
    let db = dbs.try().unwrap();
    let mut ddml = DDMLMock::new();
    ddml.expect_get::<DivBuf>()
        .called_once()
        .with(passes(move |arg: & *const DRP| unsafe {**arg == drp} ))
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must clone db here.
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(db.clone());
            Box::new(future::ok::<Box<DivBuf>, Error>(res))
        });

    let elem: IntElem<u32, u32> = IntElem {
        key: 0,
        ptr: TreePtr::DRP(drp2)
    };
    let r = current_thread::block_on_all(future::lazy(|| {
        elem.rlock(&ddml).map(|node| {
            let int_data = (*node).as_int().unwrap();
            assert_eq!(int_data.children.len(), 2);
            // Validate DRPs as well as possible using their public API
            assert_eq!(int_data.children[0].key, 0);
            assert!(!int_data.children[0].ptr.is_mem());
            assert_eq!(int_data.children[1].key, 256);
            assert!(!int_data.children[1].ptr.is_mem());
        })
    }));
    assert!(r.is_ok());
}

#[test]
fn read_leaf() {
    let mut ddml = DDMLMock::new();
    let serialized = vec![
        0u8, 0, 0, 0,               // enum variant 0 for LeafNode
        3, 0, 0, 0, 0, 0, 0, 0,     // 3 elements in the map
        0, 0, 0, 0, 100, 0, 0, 0,   // K=0, V=100 in little endian
        1, 0, 0, 0, 200, 0, 0, 0,   // K=1, V=200
        99, 0, 0, 0, 80, 195, 0, 0  // K=99, V=50000
    ];
    let dbs = DivBufShared::from(serialized);
    let db = dbs.try().unwrap();
    ddml.expect_get::<DivBuf>()
        .called_once()
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must clone db here.
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(db.clone());
            Box::new(future::ok::<Box<DivBuf>, Error>(res))
        });
    let tree: Tree<u32, u32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    DRP:
      pba:
        cluster: 0
        lba: 0
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);

    let r = current_thread::block_on_all(tree.lookup(1));
    assert_eq!(Ok(200), r);
}

#[test]
fn write_int() {
    let mut ddml = DDMLMock::new();
    let serialized = vec![1u8, 0, 0, 0, // enum variant 0 for IntNode
        2, 0, 0, 0, 0, 0, 0, 0,     // 2 elements in the vector
           0, 0, 0, 0,              // K=0
           1u8, 0, 0, 0,            // enum variant 1 for TreePtr::DRP
               0, 0,                // Cluster 0
               0, 0, 0, 0, 0, 0, 0, 0,  // LBA 0
           0, 0, 0, 0,              // enum variant 0 for Compression::None
           0x40, 0x9c, 0, 0,        // lsize=40000
           0x40, 0x9c, 0, 0,         // csize=40000
           0xef, 0xbe, 0xad, 0xde, 0, 0, 0, 0,  // checksum
           0, 1, 0, 0,              // K=256
           1u8, 0, 0, 0,            // enum variant 1 for TreePtr::DRP
               0, 0,                // Cluster 0
               0, 1, 0, 0, 0, 0, 0, 0,  // LBA 256
           1, 0, 0, 0,              // enum variant 0 for ZstdL9NoShuffle
           0x80, 0x3e, 0, 0,        // lsize=16000
           0x40, 0x1f, 0, 0,        // csize=8000
           0xbe, 0xba, 0x7e, 0x1a, 0, 0, 0, 0,  // checksum
    ];
    let drp = DRP::random(Compression::None, serialized.len());
    ddml.expect_put::<DivBufShared>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(DivBufShared, _)| {
            let dbs = arg;
            &dbs.try().unwrap()[..] == &serialized[..]
        }))
        .returning(move |_| (drp, Box::new(future::ok::<(), Error>(()))));
    ddml.expect_sync_all()
        .called_once()
        .returning(|_| Box::new(future::ok::<(), Error>(())));
    let mut tree: Tree<u32, u32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Int:
        children:
          - key: 0
            ptr:
              DRP:
                pba:
                  cluster: 0
                  lba: 0
                compression: None
                lsize: 40000
                csize: 40000
                checksum: 0xdeadbeef
          - key: 256
            ptr:
              DRP:
                pba:
                  cluster: 0
                  lba: 256
                compression: ZstdL9NoShuffle
                lsize: 16000
                csize: 8000
                checksum: 0x1a7ebabe
"#);

    let r = current_thread::block_on_all(tree.sync_all());
    assert!(r.is_ok());
    assert_eq!(*tree.i.root.get_mut().unwrap().ptr.as_drp().unwrap(), drp);
}

#[test]
fn write_leaf() {
    let mut ddml = DDMLMock::new();
    let serialized = vec![0u8, 0, 0, 0, // enum variant 0 for LeafNode
        3, 0, 0, 0, 0, 0, 0, 0,     // 3 elements in the map
        0, 0, 0, 0, 100, 0, 0, 0,   // K=0, V=100 in little endian
        1, 0, 0, 0, 200, 0, 0, 0,   // K=1, V=200
        99, 0, 0, 0, 80, 195, 0, 0  // K=99, V=50000
    ];
    let drp = DRP::random(Compression::None, serialized.len());
    ddml.expect_put::<DivBufShared>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(DivBufShared, _)| {
            let dbs = arg;
            &dbs.try().unwrap()[..] == &serialized[..]
        })).returning(move |_| (drp, Box::new(future::ok::<(), Error>(()))));
    ddml.expect_sync_all()
        .called_once()
        .returning(|_| Box::new(future::ok::<(), Error>(())));
    let mut tree: Tree<u32, u32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Mem:
      Leaf:
        items:
          0: 100
          1: 200
          99: 50000
"#);

    let r = current_thread::block_on_all(tree.sync_all());
    assert!(r.is_ok());
    assert_eq!(*tree.i.root.get_mut().unwrap().ptr.as_drp().unwrap(), drp);
}

}
// LCOV_EXCL_STOP
