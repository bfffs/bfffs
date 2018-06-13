// vim: tw=80

//! COW B+-Trees, based on B-trees, Shadowing, and Clones[^CowBtrees]
//!
//! [^CowBtrees]: Rodeh, Ohad. "B-trees, shadowing, and clones." ACM Transactions on Storage (TOS) 3.4 (2008): 2.

use bincode;
use common::*;
use common::ddml::*;
use futures::{Async, Future, future, future::IntoFuture, Poll, stream::Stream};
use futures_locks::*;
use nix::{Error, errno};
use serde::{Serialize, Serializer, de::{Deserializer, DeserializeOwned}};
#[cfg(test)] use serde_yaml;
#[cfg(test)] use std::fmt::{self, Display, Formatter};
#[cfg(test)] use simulacrum::*;
use std::{
    borrow::Borrow,
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    fmt::Debug,
    mem,
    rc::Rc,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering}
    }
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

// LCOV_EXCL_START
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

    pub fn expect_pop<T: Cacheable>(&mut self) -> Method<*const DRP,
        Box<Future<Item=Box<T>, Error=Error>>>
    {
        self.e.expect::<*const DRP, Box<Future<Item=Box<T>, Error=Error>>>
            ("pop")
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

    pub fn then(&mut self) -> &mut Self {
        self.e.then();
        self
    }
}

#[cfg(test)]
pub type DDMLLike = DDMLMock;
#[cfg(not(test))]
#[doc(hidden)]
pub type DDMLLike = DDML;
// LCOV_EXCL_STOP

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
    fn as_drp(&self) -> &DRP {
        if let TreePtr::DRP(drp) = self {
            drp
        } else {
            panic!("Not a TreePtr::DRP")    // LCOV_EXCL_LINE
        }
    }

    fn as_mem(&self) -> &Box<Node<K, V>> {
        if let TreePtr::Mem(mem) = self {
            mem
        } else {
            panic!("Not a TreePtr::Mem")    // LCOV_EXCL_LINE
        }
    }

    fn into_node(self) -> Box<Node<K, V>> {
        if let TreePtr::Mem(node) = self {
            node
        } else {
            panic!("Not a TreePtr::Mem")    // LCOV_EXCL_LINE
        }
    }

    fn is_dirty(&self) -> bool {
        self.is_mem()
    }

// LCOV_EXCL_START  exclude test code
    #[cfg(test)]
    fn is_drp(&self) -> bool {
        if let TreePtr::DRP(_) = self {
            true
        } else {
            false
        }
    }
// LCOV_EXCL_STOP

    fn is_mem(&self) -> bool {
        if let TreePtr::Mem(_) = self {
            true
        } else {
            false
        }
    }
}

// LCOV_EXCL_START
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
// LCOV_EXCL_STOP

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

    fn get<Q>(&self, k: &Q) -> Option<V>
        where K: Borrow<Q>, Q: Ord
    {
        self.items.get(k).cloned()
    }

    /// Lookup a range of values from a single Leaf Node.
    ///
    /// # Returns
    ///
    /// A `VecDeque` of partial results, and a bool.  If the bool is true, then
    /// there may be more results from other Nodes.  If false, then there will
    /// be no more results.
    fn range<R, T>(&self, range: R) -> (VecDeque<(K, V)>, bool)
        where K: Borrow<T>,
              R: RangeBounds<T>,
              T: Ord + Clone
    {
        let l = self.items.keys().next_back().unwrap();
        let more = match range.end_bound() {
            Bound::Included(i) | Bound::Excluded(i) if i <= l.borrow() => false,
            _ => true
        };
        let items = self.items.range(range)
            .map(|(&k, &v)| (k.clone(), v.clone()))
            .collect::<VecDeque<(K, V)>>();
        (items, more)
    }

    /// Delete all keys within the given range, possibly leaving an empty
    /// LeafNode.
    fn range_delete<R, T>(&mut self, range: R)
        where K: Borrow<T>,
              R: RangeBounds<T>,
              T: Ord + Clone
    {
        let keys = self.items.range(range)
            .map(|(k, _)| *k)
            .collect::<Vec<K>>();
        for k in keys {
            self.items.remove(k.borrow());
        }
    }


    fn remove<Q>(&mut self, k: &Q) -> Option<V>
        where K: Borrow<Q>, Q: Ord
    {
        self.items.remove(k)
    }
}

/// Guard that holds the Node lock object for reading
enum TreeReadGuard<K: Key, V: Value> {
    Mem(RwLockReadGuard<NodeData<K, V>>),
    DRP(RwLockReadGuard<NodeData<K, V>>, Box<Arc<Node<K, V>>>)
}

impl<K: Key, V: Value> Deref for TreeReadGuard<K, V> {
    type Target = NodeData<K, V>;

    fn deref(&self) -> &Self::Target {
        match self {
            TreeReadGuard::Mem(guard) => &**guard,
            TreeReadGuard::DRP(guard, _) => &**guard,
        }
    }
}

/// Guard that holds the Node lock object for writing
enum TreeWriteGuard<K: Key, V: Value> {
    Mem(RwLockWriteGuard<NodeData<K, V>>),
    DRP(RwLockWriteGuard<NodeData<K, V>>, Box<Arc<Node<K, V>>>)
}

impl<K: Key, V: Value> Deref for TreeWriteGuard<K, V> {
    type Target = NodeData<K, V>;

    fn deref(&self) -> &Self::Target {
        match self {
            TreeWriteGuard::Mem(guard) => &**guard,
            TreeWriteGuard::DRP(_, _) => unreachable!( // LCOV_EXCL_LINE
                "Can only write to in-memory Nodes")
        }
    }
}

impl<K: Key, V: Value> DerefMut for TreeWriteGuard<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            TreeWriteGuard::Mem(guard) => &mut **guard,
            TreeWriteGuard::DRP(_, _) => unreachable!( // LCOV_EXCL_LINE
                "Can only write to in-memory Nodes")
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
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
struct IntData<K: Key, V: Value> {
    children: Vec<IntElem<K, V>>
}

impl<K: Key, V: Value> IntData<K, V> {
    /// Find index of rightmost child whose key is less than or equal to k
    fn position<Q>(&self, k: &Q) -> usize
        where K: Borrow<Q>, Q: Ord
    {
        self.children
            .binary_search_by(|child| child.key.borrow().cmp(k))
            .unwrap_or_else(|k| k - 1)
    }

    /// Find index of rightmost child whose key is less than k
    fn xposition<Q>(&self, k: &Q) -> usize
        where K: Borrow<Q>, Q: Ord
    {
        self.children
            .binary_search_by(|child| child.key.borrow().cmp(k))
            .map(|i| i + 1)
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
    fn as_int(&self) -> &IntData<K, V> {
        if let NodeData::Int(int) = self {
            int
        } else {
            panic!("Not a NodeData::Int")   // LCOV_EXCL_LINE
        }
    }

    fn as_int_mut(&mut self) -> &mut IntData<K, V> {
        if let NodeData::Int(int) = self {
            int
        } else {
            panic!("Not a NodeData::Int")   // LCOV_EXCL_LINE
        }
    }

    #[cfg(test)]
    fn as_leaf(&self) -> &LeafData<K, V> {
        if let NodeData::Leaf(leaf) = self {
            leaf
        } else {
            panic!("Not a NodeData::Leaf")  // LCOV_EXCL_LINE
        }
    }

    fn as_leaf_mut(&mut self) -> &mut LeafData<K, V> {
        if let NodeData::Leaf(leaf) = self {
            leaf
        } else {
            panic!("Not a NodeData::Leaf")  // LCOV_EXCL_LINE
        }
    }

    fn is_leaf(&self) -> bool {
        if let NodeData::Leaf(_) = self {
            true
        } else {
            false
        }
    }

    /// Can this child be merged with `other` without violating constraints?
    fn can_merge(&self, other: &NodeData<K, V>, max_fanout: usize) -> bool {
        self.len() + other.len() <= max_fanout
    }

    /// Return this `NodeData`s lower bound key, suitable for use in its
    /// parent's `children` array.
    fn key(&self) -> &K {
        match self {
            NodeData::Leaf(ref leaf) => leaf.items.keys().nth(0).unwrap(),
            NodeData::Int(ref int) => &int.children[0].key,
        }
    }

    /// Number of children or items in this `NodeData`
    fn len(&self) -> usize {
        match self {
            NodeData::Leaf(leaf) => leaf.items.len(),
            NodeData::Int(int) => int.children.len()
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
        match self {
            NodeData::Leaf(leaf) => {
                let (k, new_leaf) = leaf.split();
                (k, NodeData::Leaf(new_leaf))
            },
            NodeData::Int(int) => {
                let (k, new_int) = int.split();
                (k, NodeData::Int(new_int))
            },

        }
    }

    /// Merge all of `other`'s data into `self`.  Afterwards, `other` may be
    /// deleted.
    fn merge(&mut self, other: &mut NodeData<K, V>) {
        match self {
            NodeData::Int(int) =>
                int.children.append(&mut other.as_int_mut().children),
            NodeData::Leaf(leaf) =>
                leaf.items.append(&mut other.as_leaf_mut().items),
        }
    }

    /// Take `other`'s highest keys and merge them into ourself
    fn take_high_keys(&mut self, other: &mut NodeData<K, V>) {
        let keys_to_share = (other.len() - self.len()) / 2;
        match self {
            NodeData::Int(int) => {
                let other_children = &mut other.as_int_mut().children;
                let cutoff_idx = other_children.len() - keys_to_share;
                let mut other_right_half =
                    other_children.split_off(cutoff_idx);
                int.children.splice(0..0, other_right_half.into_iter());
            },
            NodeData::Leaf(leaf) => {
                let other_items = &mut other.as_leaf_mut().items;
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
        match self {
            NodeData::Int(int) => {
                let other_children = &mut other.as_int_mut().children;
                let other_left_half = other_children.drain(0..keys_to_share);
                let nchildren = int.children.len();
                int.children.splice(nchildren.., other_left_half);
            },
            NodeData::Leaf(leaf) => {
                let other_items = &mut other.as_leaf_mut().items;
                let cutoff = *other_items.keys().nth(keys_to_share).unwrap();
                let other_right_half = other_items.split_off(&cutoff);
                let mut other_left_half =
                    mem::replace(other_items, other_right_half);
                leaf.items.append(&mut other_left_half);
            }
        }
    }
}

impl<K: Key, V: Value> Cacheable for Arc<Node<K, V>> {
    fn deserialize(dbs: DivBufShared) -> Self where Self: Sized {
        let db = dbs.try().unwrap();
        let node_data: NodeData<K, V> = bincode::deserialize(&db[..]).unwrap();
        Arc::new(Node(RwLock::new(node_data)))
    }

    fn len(&self) -> usize {
        if let Ok(guard) = self.0.try_read() {
            match guard.deref() {
                NodeData::Leaf(leaf) => {
                    // Rust's BTreeMap doesn't have any method to get its memory
                    // consumption.  But it's dominated by two vecs in each
                    // leaf, one storing keys and the other storing values.  As
                    // of 1.26.1, the vecs are of length 11 and have minimum
                    // size 5.  Each leaf has (on 64-bit arches) and additional
                    // 12 bytes.  Each internal node has 12 children plus an
                    // internal leaf node.  If each node on average has an
                    // occupancy of 8, then an average tree will have n / 8
                    // total nodes, a height of log(n, 8), and n / 7 internal
                    // nodes.
                    //
                    // So the memory consumption will be roughly as follows,
                    // assuming 64-bit pointers:
                    let n = leaf.items.len();
                    let nodes = n >> 3;
                    let non_leaves = nodes / 7;
                    let leaf_memory = 12 * (mem::size_of::<K>() +
                                            mem::size_of::<V>()) + 12;
                    let non_leaf_memory = 12 * 8;
                    leaf_memory * nodes + non_leaf_memory * non_leaves
                },
                NodeData::Int(int) => {
                    // IntData is layed out contiguously in memory
                    mem::size_of_val(int)
                }
            }
        } else {
            panic!("There's probably no good reason to call this method on a Node that's locked exclusively, because such a Node can't be in the Cache");
        }
    }

    fn make_ref(&self) -> Box<CacheRef> {
        Box::new(self.clone())
    }

    fn safe_to_expire(&self) -> bool {
        true    // The Arc guarantees that we can expire at any time
    }

    fn serialize(&self) -> (DivBuf, Option<DivBufShared>) {
        let g = self.0.try_read().expect(
            "Shouldn't be serializing a Node that's locked for writing");
        let v = bincode::serialize(&g.deref()).unwrap();
        let dbs = DivBufShared::from(v);
        let db = dbs.try().unwrap();
        (db, Some(dbs))
    }

    fn truncate(&self, _len: usize) {
        unimplemented!()
    }
}

impl<K: Key, V: Value> CacheRef for Arc<Node<K, V>> {
    fn deserialize(dbs: DivBufShared) -> Box<Cacheable> where Self: Sized {
        let db = dbs.try().unwrap();
        let node_data: NodeData<K, V> = bincode::deserialize(&db[..]).unwrap();
        let node = Arc::new(Node(RwLock::new(node_data)));
        Box::new(node)
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

pub struct Range<'tree, K, T, V>
    where K: Key + Borrow<T>,
          T: 'tree + Ord + Clone,
          V: Value
{
    /// If Some, then there are more nodes in the Tree to query
    cursor: Option<Bound<T>>,
    /// Data that can be returned immediately
    data: VecDeque<(K, V)>,
    end: Bound<T>,
    last_fut: Option<Box<Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                       Error=Error> + 'tree>>,
    /// Handle to the tree
    tree: &'tree Tree<K, V>
}

impl<'tree, K, T, V> Range<'tree, K, T, V>
    where K: Key + Borrow<T>,
          T: Ord + Clone,
          V: Value
    {

    fn new<R>(range: R, tree: &'tree Tree<K, V>) -> Range<'tree, K, T, V>
        where R: RangeBounds<T>
    {
        let cursor: Option<Bound<T>> = Some(match range.start_bound() {
            Bound::Included(&ref b) => Bound::Included(b.clone()),
            Bound::Excluded(&ref b) => Bound::Excluded(b.clone()),
            Bound::Unbounded => Bound::Unbounded,
        });
        let end: Bound<T> = match range.end_bound() {
            Bound::Included(&ref e) => Bound::Included(e.clone()),
            Bound::Excluded(&ref e) => Bound::Excluded(e.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Range{cursor, data: VecDeque::new(), end, last_fut: None, tree: tree}
    }
}

impl<'tree, K, T, V> Stream for Range<'tree, K, T, V>
    where K: Key + Borrow<T>,
          T: Ord + Clone + 'static,
          V: Value
{
    type Item = (K, V);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.data.pop_front()
            .map(|x| Ok(Async::Ready(Some(x))))
            .unwrap_or_else(|| {
                if self.cursor.is_some() {
                    let mut fut = self.last_fut.take().unwrap_or_else(|| {
                        let l = self.cursor.clone().unwrap();
                        let r = (l, self.end.clone());
                        Box::new(self.tree.get_range(r))
                    });
                    match fut.poll() {
                        Ok(Async::Ready((v, bound))) => {
                            self.data = v;
                            self.cursor = bound;
                            self.last_fut = None;
                            Ok(Async::Ready(self.data.pop_front()))
                        },
                        Ok(Async::NotReady) => {
                            self.last_fut = Some(fut);
                            Ok(Async::NotReady)
                        },
                        Err(e) => Err(e)
                    }
                } else {
                    Ok(Async::Ready(None))
                }
            })
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

    /// Fix an Int node in danger of being underfull, returning the parent guard
    /// back to the caller
    fn fix_int<Q>(&'a self, parent: TreeWriteGuard<K, V>,
                  child_idx: usize, mut child: TreeWriteGuard<K, V>)
        -> impl Future<Item=TreeWriteGuard<K, V>, Error=Error> + 'a
        where K: Borrow<Q>, Q: Ord
    {
        // Outline:
        // First, try to merge with the right sibling
        // Then, try to steal keys from the right sibling
        // Then, try to merge with the left sibling
        // Then, try to steal keys from the left sibling
        let nchildren = parent.as_int().children.len();
        let (fut, right) = {
            if child_idx < nchildren - 1 {
                (self.xlock(parent, child_idx + 1), true)
            } else {
                (self.xlock(parent, child_idx - 1), false)
            }
        };
        fut.map(move |(mut parent, mut sibling)| {
            if right {
                if child.can_merge(&sibling, self.i.max_fanout) {
                    child.merge(&mut sibling);
                    parent.as_int_mut().children.remove(child_idx + 1);
                } else {
                    child.take_low_keys(&mut sibling);
                    let sib_idx = child_idx + 1;
                    parent.as_int_mut().children[sib_idx].key = *sibling.key();
                }
            } else {
                if sibling.can_merge(&child, self.i.max_fanout) {
                    sibling.merge(&mut child);
                    parent.as_int_mut().children.remove(child_idx);
                } else {
                    child.take_high_keys(&mut sibling);
                    parent.as_int_mut().children[child_idx].key = *child.key();
                }
            };
            parent
        })
    }

#[cfg(test)]
    pub fn from_str(ddml: DDMLLike, s: &str) -> Self {
        let i: Inner<K, V> = serde_yaml::from_str(s).unwrap();
        Tree{ddml, i}
    }

    /// Insert value `v` into the tree at key `k`, returning the previous value
    /// for that key, if any.
    pub fn insert(&'a self, k: K, v: V)
        -> impl Future<Item=Option<V>, Error=Error> + 'a {

        self.i.root.write()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |guard| {
                self.xlock_root(guard)
                     .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                     .and_then(move |(_root_guard, child_guard)| {
                         self.insert_locked(child_guard, k, v)
                     })
            })
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
            parent.as_int_mut().children.insert(child_idx + 1, new_elem);
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
            root.as_int_mut().children.insert(0, old_elem);
            self.i.height.fetch_add(1, Ordering::Relaxed);
        }

        self.insert_no_split(root, k, v)
    }

    fn insert_no_split(&'a self, mut node: TreeWriteGuard<K, V>, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
    {
        if node.is_leaf() {
            let old_v = node.as_leaf_mut().insert(k, v);
            return Box::new(Ok(old_v).into_future())
        } else {
            let child_idx = node.as_int().position(&k);
            let fut = self.xlock(node, child_idx);
            Box::new(fut.and_then(move |(parent, child)| {
                    self.insert_int(parent, child_idx, child, k, v)
                })
            )
        }
    }

    /// Lookup the value of key `k`.  Return `None` if no value is present.
    pub fn get<Q>(&'a self, k: &'a Q)
        -> impl Future<Item=Option<V>, Error=Error> + 'a
        where K: Borrow<Q>, Q: Ord
    {
        self.i.root.read()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |guard| {
                self.rlock(&guard)
                     .and_then(move |guard| self.get_node(guard, k))
            })
    }

    /// Lookup the value of key `k` in a node, which must already be locked.
    fn get_node<Q>(&'a self, node: TreeReadGuard<K, V>, k: &'a Q)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
        where K: Borrow<Q>, Q: Ord
    {

        let next_node_fut = match *node {
            NodeData::Leaf(ref leaf) => {
                return Box::new(Ok(leaf.get(k)).into_future())
            },
            NodeData::Int(ref int) => {
                let child_elem = &int.children[int.position(k)];
                self.rlock(&child_elem)
            }
        };
        drop(node);
        Box::new(
            next_node_fut
            .and_then(move |next_node| self.get_node(next_node, k))
        )
    }

    /// Private helper for `Range::poll`.  Returns a subset of the total
    /// results, consisting of all matching (K,V) pairs within a single Leaf
    /// Node, plus an optional Bound for the next iteration of the search.  If
    /// the Bound is `None`, then the search is complete.
    fn get_range<R, T>(&'a self, range: R)
        -> impl Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                       Error=Error> + 'a
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static
    {
        self.i.root.read()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |guard| {
                self.rlock(&guard)
                     .and_then(move |g| self.get_range_node(g, None, range))
            })
    }

    /// Range lookup beginning in the node `guard`.  `next_guard`, if present,
    /// must be the node immediately to the right (and possibly up one or more
    /// levels) from `guard`.
    fn get_range_node<R, T>(&'a self, guard: TreeReadGuard<K, V>,
                            next_guard: Option<TreeReadGuard<K, V>>, range: R)
        -> Box<Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                      Error=Error> + 'a>
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static
    {
        let (child_fut, next_fut) = match *guard {
            NodeData::Leaf(ref leaf) => {
                let (v, more) = leaf.range(range.clone());
                let ret = if v.is_empty() && more && next_guard.is_some() {
                    // We must've started the query with a key that's not
                    // present, and lies between two leaves.  Check the next
                    // node
                    self.get_range_node(next_guard.unwrap(), None, range)
                } else if v.is_empty() {
                    // The range is truly empty
                    Box::new(Ok((v, None)).into_future())
                } else {
                    let bound = if more && next_guard.is_some() {
                        Some(Bound::Included(next_guard.unwrap()
                                                       .key()
                                                       .borrow()
                                                       .clone()))
                    } else {
                        None
                    };
                    Box::new(Ok((v, bound)).into_future())
                };
                return ret;
            },
            NodeData::Int(ref int) => {
                let child_idx = match range.start_bound() {
                    Bound::Included(i) | Bound::Excluded(i) => int.position(i),
                    Bound::Unbounded => 0
                };
                let child_elem = &int.children[child_idx];
                let next_fut = if child_idx < int.children.len() - 1 {
                    Box::new(
                        self.rlock(&int.children[child_idx + 1])
                            .map(|guard| Some(guard))
                    ) as Box<Future<Item=Option<TreeReadGuard<K, V>>,
                                    Error=Error>>
                } else {
                    Box::new(Ok(next_guard).into_future())
                        as Box<Future<Item=Option<TreeReadGuard<K, V>>,
                                      Error=Error>>
                };
                let child_fut = self.rlock(&child_elem);
                (child_fut, next_fut)
            } // LCOV_EXCL_LINE kcov false negative
        };
        drop(guard);
        Box::new(
            child_fut.join(next_fut)
                .and_then(move |(child_guard, next_guard)| {
                self.get_range_node(child_guard, next_guard, range)
            })
        ) as Box<Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>), Error=Error>>
    }

    /// Lookup a range of (key, value) pairs for keys within the range `range`.
    pub fn range<R, T>(&'a self, range: R) -> Range<'a, K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T>,
              T: Ord + Clone
    {
        Range::new(range, self)
    }

    /// Delete a range of keys
    pub fn range_delete<R, T>(&'a self, range: R)
        -> impl Future<Item=(), Error=Error> + 'a
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        // Outline:
        // 1) Traverse the tree removing all requested KV-pairs, leaving damaged
        //    nodes
        // 2) Traverse the tree again, fixing in-danger nodes from top-down
        self.i.root.write()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |guard| {
                self.xlock_root(guard)
                     .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                     .and_then(move |(root_guard, child_guard)| {
                         self.range_delete_pass1(child_guard, range, None)
                             // Keep the whole tree locked during range_delete
                             .map(|_| drop(root_guard))
                     })
            })
    }

    /// Depth-first traversal deleting keys without reshaping tree
    /// `ubound` is the first key in the Node immediately to the right of
    /// this one, unless this is the rightmost Node on its level.
    fn range_delete_pass1<R, T>(&'a self, mut guard: TreeWriteGuard<K, V>,
                                range: R, ubound: Option<K>)
        -> impl Future<Item=(), Error=Error> + 'a
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        if guard.is_leaf() {
            guard.as_leaf_mut().range_delete(range);
            return Box::new(Ok(()).into_future())
                as Box<Future<Item=(), Error=Error>>;
        }

        // We must recurse into at most two children (at the limits of the
        // range), and completely delete 0 or more children (in the middle
        // of the range)
        let l = guard.as_int().children.len();
        let start_idx_bound = match range.start_bound() {
            Bound::Unbounded => Bound::Included(0),
            Bound::Included(t) | Bound::Excluded(t)
                if t < guard.key().borrow() =>
            {
                Bound::Included(0)
            },
            Bound::Included(t) => {
                let idx = guard.as_int().position(t);
                if guard.as_int().children[idx].key.borrow() == t {
                    // Remove the entire Node
                    Bound::Included(idx)
                } else {
                    // Recurse into the first Node
                    Bound::Excluded(idx)
                }
            },
            Bound::Excluded(t) => {
                // Recurse into the first Node
                let idx = guard.as_int().position(t);
                Bound::Excluded(idx)
            },
        };
        let end_idx_bound = match range.end_bound() {
            Bound::Unbounded => Bound::Excluded(l + 1),
            Bound::Included(t) => {
                let idx = guard.as_int().position(t);
                if ubound.is_some() && t >= ubound.unwrap().borrow() {
                    Bound::Excluded(idx + 1)
                } else {
                    Bound::Included(idx)
                }
            },
            Bound::Excluded(t) => {
                let idx = guard.as_int().xposition(t);
                if ubound.is_some() && t >= ubound.unwrap().borrow() {
                    Bound::Excluded(idx + 1)
                } else if idx < l - 1 &&
                    guard.as_int().children[idx + 1].key.borrow() == t
                {
                    Bound::Excluded(idx + 1)
                } else {
                    Bound::Included(idx)
                }
            }
        };
        let fut: Box<Future<Item=TreeWriteGuard<K, V>, Error=Error>>
            = match (start_idx_bound, end_idx_bound) {
            (Bound::Included(_), Bound::Excluded(_)) => {
                // Don't recurse
                Box::new(Ok(guard).into_future())
            },
            (Bound::Included(_), Bound::Included(j)) => {
                // Recurse into a Node at the end
                let ubound = if j < l - 1 {
                    Some(guard.as_int().children[j + 1].key)
                } else {
                    ubound
                };
                Box::new(self.xlock(guard, j)
                    .and_then(move |(parent_guard, child_guard)| {
                        self.range_delete_pass1(child_guard, range, ubound)
                            .map(move |_| parent_guard)
                    }))
            },
            (Bound::Excluded(i), Bound::Excluded(_)) => {
                // Recurse into a Node at the beginning
                let ubound = if i < l - 1 {
                    Some(guard.as_int().children[i + 1].key)
                } else {
                    ubound
                };
                Box::new(self.xlock(guard, i)
                    .and_then(move |(parent_guard, child_guard)| {
                        self.range_delete_pass1(child_guard, range, ubound)
                            .map(move |_| parent_guard)
                    }))
            },
            (Bound::Excluded(i), Bound::Included(j)) if j > i => {
                // Recurse into a Node at the beginning and end
                let range2 = range.clone();
                let ub_l = Some(guard.as_int().children[i + 1].key);
                let ub_h = if j < l - 1 {
                    Some(guard.as_int().children[i + 1].key)
                } else {
                    ubound
                };
                Box::new(self.xlock(guard, i)
                    .and_then(move |(parent_guard, child_guard)| {
                        self.range_delete_pass1(child_guard, range, ub_l)
                            .map(|_| parent_guard)
                    }).and_then(move |parent_guard| {
                        self.xlock(parent_guard, j)
                    }).and_then(move |(parent_guard, child_guard)| {
                        self.range_delete_pass1(child_guard, range2, ub_h)
                            .map(|_| parent_guard)
                    }))
            },
            (Bound::Excluded(i), Bound::Included(j)) if j <= i => {
                // Recurse into a single Node
                let ubound = if i < l - 1 {
                    Some(guard.as_int().children[i + 1].key)
                } else {
                    ubound
                };
                Box::new(self.xlock(guard, i)
                    .and_then(move |(parent_guard, child_guard)| {
                        self.range_delete_pass1(child_guard, range, ubound)
                            .map(move |_| parent_guard)
                    }))
            },
            (Bound::Excluded(_), Bound::Included(_)) => unreachable!(),
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => unreachable!(),
        };

        // Finally, remove nodes in the middle
        Box::new(fut.map(move |mut guard| {
            let low = match start_idx_bound {
                Bound::Excluded(i) => i + 1,
                Bound::Included(i) => i,
                Bound::Unbounded => unreachable!()
            };
            let high = match end_idx_bound {
                Bound::Excluded(j) => j - 1,
                Bound::Included(j) => j,
                Bound::Unbounded => unreachable!()
            };
            if high > low {
                guard.as_int_mut().children.drain(low..high);
            }
        }))
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
    pub fn remove<Q>(&'a self, k: &'a Q)
        -> impl Future<Item=Option<V>, Error=Error> + 'a
        where K: Borrow<Q>, Q: Ord
    {
        self.i.root.write()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |guard| {
                self.xlock_root(guard)
                    .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                    .and_then(move |(_root_guard, child_guard)| {
                        self.remove_locked(child_guard, k)
                    })
        })
    }

    /// Remove key `k` from an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn remove_int<Q>(&'a self, parent: TreeWriteGuard<K, V>,
                  child_idx: usize, child: TreeWriteGuard<K, V>, k: &'a Q)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
        where K: Borrow<Q>, Q: Ord
    {

        // First, fix the node, if necessary
        if child.should_fix(self.i.min_fanout) {
            Box::new(
                self.fix_int(parent, child_idx, child)
                    .and_then(move |parent| self.remove_no_fix(parent, k))
            )
        } else {
            drop(parent);
            self.remove_no_fix(child, k)
        }
    }

    /// Helper for `remove`.  Handles removal once the tree is locked
    fn remove_locked<Q>(&'a self, mut root: TreeWriteGuard<K, V>, k: &'a Q)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
        where K: Borrow<Q>, Q: Ord
    {

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
    fn remove_no_fix<Q>(&'a self, mut node: TreeWriteGuard<K, V>, k: &'a Q)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
        where K: Borrow<Q>, Q: Ord
    {

        if node.is_leaf() {
            let old_v = node.as_leaf_mut().remove(k);
            return Box::new(Ok(old_v).into_future());
        } else {
            let child_idx = node.as_int().position(k);
            let fut = self.xlock(node, child_idx);
            Box::new(fut.and_then(move |(parent, child)| {
                    self.remove_int(parent, child_idx, child, k)
                })
            )
        }
    }

    /// Sync all records written so far to stable storage.
    pub fn sync_all(&'a self) -> impl Future<Item=(), Error=Error> + 'a {
        self.i.root.write()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                  .and_then(move |root_guard| {
            if root_guard.ptr.is_dirty() {
                // If the root is dirty, then we have ownership over it.  But
                // another task may still have a lock on it.  We must acquire
                // then release the lock to ensure that we have the sole
                // reference.
                let fut = self.xlock_root(root_guard)
                    .and_then(move |(mut root_guard, child_guard)|
                {
                    drop(child_guard);
                    let ptr = mem::replace(&mut root_guard.ptr, TreePtr::None);
                    Box::new(
                        self.write_node(ptr.into_node())
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
        })
    }

    fn write_leaf(&'a self, node: Box<Node<K, V>>)
        -> impl Future<Item=DRP, Error=Error> + 'a
    {
        let arc: Arc<Node<K, V>> = Arc::new(*node);
        let (drp, fut) = self.ddml.put(arc, Compression::None);
        fut.map(move |_| drp)
    }

    fn write_node(&'a self, mut node: Box<Node<K, V>>)
        -> Box<Future<Item=DRP, Error=Error> + 'a>
    {
        if node.0.get_mut().unwrap().is_leaf() {
            return Box::new(self.write_leaf(node));
        }
        let ndata = node.0.try_write().unwrap();

        // Rust's borrow checker doesn't understand that children_fut will
        // complete before its continuation will run, so it won't let ndata
        // be borrowed in both places.  So we'll have to use RefCell to allow
        // dynamic borrowing and Rc to allow moving into both closures.
        let rndata = Rc::new(RefCell::new(ndata));
        let nchildren = RefCell::borrow(&Rc::borrow(&rndata)).as_int().children.len();
        let children_fut = (0..nchildren)
        .filter_map(move |idx| {
            let rndata3 = rndata.clone();
            if rndata.borrow_mut()
                     .as_int_mut()
                     .children[idx].is_dirty()
            {
                // If the child is dirty, then we have ownership over it.  We
                // need to lock it, then release the lock.  Then we'll know that
                // we have exclusive access to it, and we can move it into the
                // Cache.
                let fut = self.xlock_dirty(&rndata.borrow_mut()
                                            .as_int_mut()
                                            .children[idx])
                              .and_then(move |guard|
                {
                    drop(guard);

                    let ptr = mem::replace(&mut rndata3.borrow_mut()
                                                       .as_int_mut()
                                                       .children[idx].ptr,
                                           TreePtr::None);
                    self.write_node(ptr.into_node())
                        .map(move |drp| {
                            rndata3.borrow_mut()
                                   .as_int_mut()
                                   .children[idx].ptr = TreePtr::DRP(drp);
                        })
                });
                Some(fut)
            } else { // LCOV_EXCL_LINE kcov false negative
                None
            }
        })
        .collect::<Vec<_>>();
        Box::new(
            future::join_all(children_fut)
            .and_then(move |_| {
                let arc: Arc<Node<K, V>> = Arc::new(*node);
                let (drp, fut) = self.ddml.put(arc, Compression::None);
                fut.map(move |_| drp)
            })
        )
    }

    /// Lock the provided `IntElem` nonexclusively
    fn rlock(&'a self, elem: &IntElem<K, V>)
        -> Box<Future<Item=TreeReadGuard<K, V>, Error=Error> + 'a>
    {
        match elem.ptr {
            TreePtr::Mem(ref node) => {
                Box::new(
                    node.0.read()
                        .map(|guard| TreeReadGuard::Mem(guard))
                        .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                )
            },
            TreePtr::DRP(ref drp) => {
                Box::new(
                    self.ddml.get::<Arc<Node<K, V>>>(drp).and_then(|node| {
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

    /// Lock the indicated `IntElem` exclusively.  If it is not already resident
    /// in memory, then COW the target node.
    fn xlock(&'a self, mut guard: TreeWriteGuard<K, V>, child_idx: usize)
        -> (Box<Future<Item=(TreeWriteGuard<K, V>,
                             TreeWriteGuard<K, V>), Error=Error> + 'a>)
    {
        if guard.as_int().children[child_idx].ptr.is_mem() {
            Box::new(
                self.xlock_dirty(&guard.as_int().children[child_idx])
                    .map(move |child_guard| {
                          (guard, child_guard)
                     })
            )
        } else {
            let drp = *guard.as_int()
                            .children[child_idx]
                            .ptr
                            .as_drp();
                Box::new(
                    self.ddml.pop::<Arc<Node<K, V>>>(&drp).map(move |arc| {
                        let child_node = Box::new(Arc::try_unwrap(*arc)
                            .expect("We should be the Node's only owner"));
                        let child_guard = {
                            let elem = &mut guard.as_int_mut()
                                                 .children[child_idx];
                            elem.ptr = TreePtr::Mem(child_node);
                            TreeWriteGuard::Mem(
                                elem.ptr.as_mem()
                                    .0.try_write().unwrap()
                            )
                        };
                        (guard, child_guard)
                    })
                )
        }
    }

    /// Lock the indicated `IntElem` exclusively, if it is known to be already
    /// dirty.
    fn xlock_dirty(&'a self, elem: &IntElem<K, V>)
        -> (Box<Future<Item=TreeWriteGuard<K, V>, Error=Error> + 'a>)
    {
        debug_assert!(elem.ptr.is_mem(),
            "Must use Tree::xlock for non-dirty nodes");
        Box::new(
            elem.ptr.as_mem()
                .0.write()
                .map(|guard| TreeWriteGuard::Mem(guard))
                .map_err(|_| Error::Sys(errno::Errno::EPIPE))
        )
    }

    /// Lock the root `IntElem` exclusively.  If it is not already resident in
    /// memory, then COW it.
    fn xlock_root(&'a self, mut guard: RwLockWriteGuard<IntElem<K, V>>)
        -> (Box<Future<Item=(RwLockWriteGuard<IntElem<K, V>>,
                             TreeWriteGuard<K, V>), Error=Error> + 'a>)
    {
        if guard.ptr.is_mem() {
            Box::new(
                guard.ptr.as_mem().0.write()
                     .map(move |child_guard| {
                          (guard, TreeWriteGuard::Mem(child_guard))
                     }).map_err(|_| Error::Sys(errno::Errno::EPIPE))
            )
        } else {
            let drp = *guard.ptr.as_drp();
            Box::new(
                self.ddml.pop::<Arc<Node<K, V>>>(&drp).map(move |arc| {
                    let child_node = Box::new(Arc::try_unwrap(*arc)
                        .expect("We should be the Node's only owner"));
                    guard.ptr = TreePtr::Mem(child_node);
                    let child_guard = TreeWriteGuard::Mem(
                        guard.ptr.as_mem().0.try_write().unwrap()
                    );
                    (guard, child_guard)
                })
            )
        }
    }
}

#[cfg(test)]
impl<K: Key, V: Value> Display for Tree<K, V> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(&serde_yaml::to_string(&self.i).unwrap())
    }
}



// LCOV_EXCL_START
/// Tests for serialization/deserialization of Nodes
#[cfg(test)]
mod serialization {

use super::*;

#[test]
fn deserialize_int() {
    let serialized = DivBufShared::from(vec![
        1u8, 0, 0, 0, // enum variant 0 for IntNode
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
    ]);
    let drp0 = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                        0xdeadbeef);
    let drp1 = DRP::new(PBA::new(0, 256), Compression::ZstdL9NoShuffle,
                        16000, 8000, 0x1a7ebabe);
    let node: Arc<Node<u32, u32>> = Cacheable::deserialize(serialized);
    let guard = node.0.try_read().unwrap();
    let int_data = guard.deref().as_int();
    assert_eq!(int_data.children.len(), 2);
    assert_eq!(int_data.children[0].key, 0);
    assert_eq!(*int_data.children[0].ptr.as_drp(), drp0);
    assert_eq!(int_data.children[1].key, 256);
    assert_eq!(*int_data.children[1].ptr.as_drp(), drp1);
}

#[test]
fn deserialize_leaf() {
    let serialized = DivBufShared::from(vec![
        0u8, 0, 0, 0, // enum variant 0 for LeafNode
        3, 0, 0, 0, 0, 0, 0, 0,     // 3 elements in the map
            0, 0, 0, 0, 100, 0, 0, 0,   // K=0, V=100 in little endian
            1, 0, 0, 0, 200, 0, 0, 0,   // K=1, V=200
            99, 0, 0, 0, 80, 195, 0, 0  // K=99, V=50000
        ]);
    let node: Arc<Node<u32, u32>> = Cacheable::deserialize(serialized);
    let guard = node.0.try_read().unwrap();
    let leaf_data = guard.deref().as_leaf();
    assert_eq!(leaf_data.items.len(), 3);
    assert_eq!(leaf_data.items[&0], 100);
    assert_eq!(leaf_data.items[&1], 200);
    assert_eq!(leaf_data.items[&99], 50_000);
}

#[test]
fn serialize_int() {
    let expected = vec![1u8, 0, 0, 0, // enum variant 0 for IntNode
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
    let drp0 = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                        0xdeadbeef);
    let drp1 = DRP::new(PBA::new(0, 256), Compression::ZstdL9NoShuffle,
                        16000, 8000, 0x1a7ebabe);
    let children = vec![
        IntElem{key: 0u32, ptr: TreePtr::DRP(drp0)},
        IntElem{key: 256u32, ptr: TreePtr::DRP(drp1)},
    ];
    let node_data = NodeData::Int(IntData{children});
    let node: Node<u32, u32> = Node(RwLock::new(node_data));
    let (db, _dbs) = Arc::new(node).serialize();
    assert_eq!(&expected[..], &db[..]);
    drop(db);
}

#[test]
fn serialize_leaf() {
    let expected = vec![0u8, 0, 0, 0, // enum variant 0 for LeafNode
        3, 0, 0, 0, 0, 0, 0, 0,     // 3 elements in the map
        0, 0, 0, 0, 100, 0, 0, 0,   // K=0, V=100 in little endian
        1, 0, 0, 0, 200, 0, 0, 0,   // K=1, V=200
        99, 0, 0, 0, 80, 195, 0, 0  // K=99, V=50000
    ];
    let mut items: BTreeMap<u32, u32> = BTreeMap::new();
    items.insert(0, 100);
    items.insert(1, 200);
    items.insert(99, 50_000);
    let node = Arc::new(Node(RwLock::new(NodeData::Leaf(LeafData{items}))));
    let (db, _dbs) = node.serialize();
    assert_eq!(&expected[..], &db[..]);
    drop(db);
}

}

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
fn get() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let r = current_thread::block_on_all(future::lazy(|| {
        tree.insert(0, 0.0)
            .and_then(|_| tree.get(&0))
    }));
    assert_eq!(r, Ok(Some(0.0)));
}

#[test]
fn get_deep() {
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
"#);
    let r = current_thread::block_on_all(tree.get(&3));
    assert_eq!(r, Ok(Some(3.0)))
}

#[test]
fn get_nonexistent() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let r = current_thread::block_on_all(tree.get(&0));
    assert_eq!(r, Ok(None))
}

// The range delete example from Figures 13-14 of B-Trees, Shadowing, and
// Range-operations
#[test]
fn range_delete() {
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
          - key: 1
            ptr:
              Mem:
                Int:
                  children:
                    - key: 1
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              1: 1.0
                              2: 2.0
                    - key: 5
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              5: 5.0
                              6: 6.0
                              7: 7.0
                    - key: 10
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              10: 10.0
                              11: 11.0
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
                    - key: 20
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              25: 25.0
          - key: 31
            ptr:
              Mem:
                Int:
                  children:
                    - key: 31
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              31: 31.0
                              32: 32.0
                    - key: 37
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              37: 37.0
                              40: 40.0
"#);
    let r = current_thread::block_on_all(
        tree.range_delete(11..=31)
    );
    assert!(r.is_ok());
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
          - key: 1
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 5
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    6: 6.0
                    7: 7.0
          - key: 10
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    32: 32.0
          - key: 37
            ptr:
              Mem:
                Leaf:
                  items:
                    37: 37.0
                    40: 40.0"#);
}

// Unbounded range lookup
#[test]
fn range_full() {
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
          - key: 3
            ptr:
              Mem:
                Leaf:
                  items:
                    3: 3.0
                    4: 4.0
"#);
    let r = current_thread::block_on_all(
        tree.range(..).collect()
    );
    assert_eq!(r, Ok(vec![(0, 0.0), (1, 1.0), (3, 3.0), (4, 4.0)]));
}

#[test]
fn range_exclusive_start() {
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
"#);
    // A query that starts on a leaf
    let r = current_thread::block_on_all(
        tree.range((Bound::Excluded(0), Bound::Excluded(4)))
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (3, 3.0)]));

    // A query that starts between leaves
    let r = current_thread::block_on_all(
        tree.range((Bound::Excluded(1), Bound::Excluded(4)))
            .collect()
    );
    assert_eq!(r, Ok(vec![(3, 3.0)]));
}

#[test]
fn range_leaf() {
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
    let r = current_thread::block_on_all(
        tree.range(1..3)
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (2, 2.0)]));
}

#[test]
fn range_leaf_inclusive_end() {
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
    let r = current_thread::block_on_all(
        tree.range(3..=4)
            .collect()
    );
    assert_eq!(r, Ok(vec![(3, 3.0), (4, 4.0)]));
}

#[test]
fn range_nonexistent_between_two_leaves() {
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
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    5: 5.0
                    6: 6.0
"#);
    let r = current_thread::block_on_all(
        tree.range(2..4)
            .collect()
    );
    assert_eq!(r, Ok(vec![]));
}

#[test]
fn range_two_ints() {
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
"#);
    let r = current_thread::block_on_all(
        tree.range(1..10)
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (9, 9.0)]));
}

#[test]
fn range_starts_between_two_leaves() {
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
"#);
    let r = current_thread::block_on_all(
        tree.range(2..4)
            .collect()
    );
    assert_eq!(r, Ok(vec![(3, 3.0)]));
}

#[test]
fn range_two_leaves() {
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
"#);
    let r = current_thread::block_on_all(
        tree.range(1..4)
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (3, 3.0)]));
}

#[test]
fn remove_last_key() {
    let ddml = DDMLMock::new();
    let tree: Tree<u32, f32> = Tree::from_str(ddml, r#"
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
    let r = current_thread::block_on_all(tree.remove(&0));
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
    let tree: Tree<u32, f32> = Tree::from_str(ddml, r#"
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
    let r = current_thread::block_on_all(tree.remove(&1));
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
fn remove_and_merge_down() {
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
                    2: 2.0
"#);
    let r2 = current_thread::block_on_all(tree.remove(&1));
    assert!(r2.is_ok());
    assert_eq!(format!("{}", &tree),
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
    let r2 = current_thread::block_on_all(tree.remove(&23));
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
    let r2 = current_thread::block_on_all(tree.remove(&4));
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
    let r2 = current_thread::block_on_all(tree.remove(&7));
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
    let r2 = current_thread::block_on_all(tree.remove(&4));
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
    let r2 = current_thread::block_on_all(tree.remove(&26));
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
    let r2 = current_thread::block_on_all(tree.remove(&14));
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
    let r2 = current_thread::block_on_all(tree.remove(&8));
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
    let r2 = current_thread::block_on_all(tree.remove(&4));
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
    let r = current_thread::block_on_all(tree.remove(&3));
    assert_eq!(r, Ok(None));
}

}

#[cfg(test)]
/// Tests regarding disk I/O for Trees
mod io {

use super::*;
use futures::future;
use tokio::prelude::task::current;
use tokio::executor::current_thread;

/// Insert an item into a Tree that's not dirty
#[test]
fn insert_below_root() {
    let mut ddml = DDMLMock::new();
    let items: BTreeMap<u32, u32> = BTreeMap::new();
    let node = Arc::new(Node(RwLock::new(NodeData::Leaf(LeafData{items}))));
    let node_holder = RefCell::new(Some(node));
    let drpl = DRP::new(PBA{cluster: 0, lba: 0}, Compression::None, 36, 36, 0);
    ddml.expect_pop::<Arc<Node<u32, u32>>>()
        .called_once()
        .with(passes(move |arg: & *const DRP| unsafe {**arg == drpl} ))
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node_holder.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<u32, u32>>>, Error>(res))
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
                lsize: 36
                csize: 36
                checksum: 0
          - key: 256
            ptr:
              DRP:
                pba:
                  cluster: 0
                  lba: 256
                compression: ZstdL9NoShuffle
                lsize: 16000
                csize: 8000
                checksum: 1234567
"#);

    let r = current_thread::block_on_all(tree.insert(0, 0));
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
      Int:
        children:
          - key: 0
            ptr:
              Mem:
                Leaf:
                  items:
                    0: 0
          - key: 256
            ptr:
              DRP:
                pba:
                  cluster: 0
                  lba: 256
                compression: ZstdL9NoShuffle
                lsize: 16000
                csize: 8000
                checksum: 1234567"#);
}

/// Insert an item into a Tree that's not dirty
#[test]
fn insert_root() {
    let mut ddml = DDMLMock::new();
    let items: BTreeMap<u32, u32> = BTreeMap::new();
    let node = Arc::new(Node(RwLock::new(NodeData::Leaf(LeafData{items}))));
    let node_holder = RefCell::new(Some(node));
    let drpl = DRP::new(PBA{cluster: 0, lba: 0}, Compression::None, 36, 36, 0);
    ddml.expect_pop::<Arc<Node<u32, u32>>>()
        .called_once()
        .with(passes(move |arg: & *const DRP| unsafe {**arg == drpl} ))
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node_holder.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<u32, u32>>>, Error>(res))
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

    let r = current_thread::block_on_all(tree.insert(0, 0));
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
          0: 0"#);
}

#[test]
fn range_leaf() {
    struct FutureMock {
        e: Expectations
    }
    impl FutureMock {
        pub fn new() -> Self {
            Self {
                e: Expectations::new()
            }
        }

        pub fn expect_poll(&mut self)
            -> Method<(), Poll<Box<Arc<Node<u32, f32>>>, Error>>
        {
            self.e.expect::<(), Poll<Box<Arc<Node<u32, f32>>>, Error>>("poll")
        }

        pub fn then(&mut self) -> &mut Self {
            self.e.then();
            self
        }
    }

    impl Future for FutureMock {
        type Item = Box<Arc<Node<u32, f32>>>;
        type Error = Error;

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            self.e.was_called_returning::<(),
                Poll<Box<Arc<Node<u32, f32>>>, Error>>("poll", ())
        }
    }

    let mut ddml = DDMLMock::new();
    let mut items: BTreeMap<u32, f32> = BTreeMap::new();
    items.insert(0, 0.0);
    items.insert(1, 1.0);
    items.insert(2, 2.0);
    items.insert(3, 3.0);
    items.insert(4, 4.0);
    let node1 = Arc::new(Node(RwLock::new(NodeData::Leaf(LeafData{items}))));
    ddml.expect_get::<Arc<Node<u32, f32>>>()
        .called_once()
        .returning(move |_| {
            let mut fut = FutureMock::new();
            let node2 = node1.clone();
            fut.expect_poll()
                .called_once()
                .returning(|_| {
                    current().notify();
                    Ok(Async::NotReady)
                });
            FutureMock::then(&mut fut).expect_poll()
                .called_once()
                .returning(move |_| {
                    // XXX simulacrum can't return a uniquely owned object in an
                    // expectation, so we must clone db here.
                    // https://github.com/pcsm/simulacrum/issues/52
                    let res = Box::new(node2.clone());
                    Ok(Async::Ready(res))
                });
            Box::new(fut)
        });
    let tree: Tree<u32, f32> = Tree::from_str(ddml, r#"
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
    let r = current_thread::block_on_all(
        tree.range(1..3).collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (2, 2.0)]));
}

#[test]
/// Read an IntNode.  The public API doesn't provide any way to read an IntNode
/// without also reading its children, so we'll test this through the private
/// IntElem::rlock API.
fn read_int() {
    let drp0 = DRP::random(Compression::None, 40000);
    let drp1 = DRP::random(Compression::ZstdL9NoShuffle, 16000);
    let children = vec![
        IntElem{key: 0u32, ptr: TreePtr::DRP(drp0)},
        IntElem{key: 256u32, ptr: TreePtr::DRP(drp1)},
    ];
    let node = Arc::new(Node(RwLock::new(NodeData::Int(IntData{children}))));
    let drpl = DRP::new(PBA{cluster: 1, lba: 2}, Compression::None, 36, 36, 0);
    let mut ddml = DDMLMock::new();
    ddml.expect_get::<Arc<Node<u32, u32>>>()
        .called_once()
        .with(passes(move |arg: & *const DRP| unsafe {**arg == drpl} ))
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must clone db here.
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node.clone());
            Box::new(future::ok::<Box<Arc<Node<u32, u32>>>, Error>(res))
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
        cluster: 1
        lba: 2
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);

    let r = current_thread::block_on_all(future::lazy(|| {
        let root_guard = tree.i.root.try_read().unwrap();
        tree.rlock(&root_guard).map(|node| {
            let int_data = (*node).as_int();
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
    let mut items: BTreeMap<u32, u32> = BTreeMap::new();
    items.insert(0, 100);
    items.insert(1, 200);
    items.insert(99, 50_000);
    let node = Arc::new(Node(RwLock::new(NodeData::Leaf(LeafData{items}))));
    ddml.expect_get::<Arc<Node<u32, u32>>>()
        .called_once()
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must clone db here.
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node.clone());
            Box::new(future::ok::<Box<Arc<Node<u32, u32>>>, Error>(res))
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

    let r = current_thread::block_on_all(tree.get(&1));
    assert_eq!(Ok(Some(200)), r);
}

// If the tree isn't dirty, then there's nothing to do
#[test]
fn write_clean() {
    let ddml = DDMLMock::new();
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

    let r = current_thread::block_on_all(tree.sync_all());
    assert!(r.is_ok());
}

/// Sync a Tree with both dirty Int nodes and dirty Leaf nodes
#[test]
fn write_deep() {
    let mut ddml = DDMLMock::new();
    let drp = DRP::random(Compression::None, 1000);
    ddml.expect_put::<Arc<Node<u32, u32>>>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(Arc<Node<u32, u32>>, _)| {
            let node_data = arg.0.try_read().unwrap();
            let leaf_data = node_data.as_leaf();
            leaf_data.items[&0] == 100 &&
            leaf_data.items[&1] == 200
        }))
        .returning(move |_| (drp, Box::new(future::ok::<(), Error>(()))));
    ddml.then().expect_put::<Arc<Node<u32, u32>>>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(Arc<Node<u32, u32>>, _)| {
            let node_data = arg.0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            int_data.children[0].ptr.is_drp() &&
            int_data.children[1].key == 256 &&
            int_data.children[1].ptr.is_drp()
        }))
        .returning(move |_| (drp, Box::new(future::ok::<(), Error>(()))));
    ddml.expect_sync_all()
        .called_once()
        .returning(|_| Box::new(future::ok::<(), Error>(())));
    let mut tree: Tree<u32, u32> = Tree::from_str(ddml, r#"
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
                    0: 100
                    1: 200
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
    assert_eq!(*tree.i.root.get_mut().unwrap().ptr.as_drp(), drp);
}

#[test]
fn write_int() {
    let mut ddml = DDMLMock::new();
    let drp = DRP::random(Compression::None, 1000);
    ddml.expect_put::<Arc<Node<u32, u32>>>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(Arc<Node<u32, u32>>, _)| {
            let node_data = arg.0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            !int_data.children[0].ptr.is_mem() &&
            int_data.children[1].key == 256 &&
            !int_data.children[1].ptr.is_mem()
        }))
        .returning(move |_| (drp, Box::new(future::ok::<(), Error>(()))));
    ddml.expect_sync_all()
        .called_once()
        .returning(|_| Box::new(future::ok::<(), Error>(())));
    let mut tree: Tree<u32, u32> = Tree::from_str(ddml, r#"
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
    assert_eq!(*tree.i.root.get_mut().unwrap().ptr.as_drp(), drp);
}

#[test]
fn write_leaf() {
    let mut ddml = DDMLMock::new();
    let drp = DRP::random(Compression::None, 1000);
    ddml.expect_put::<Arc<Node<u32, u32>>>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(Arc<Node<u32, u32>>, _)| {
            let node_data = arg.0.try_read().unwrap();
            let leaf_data = node_data.as_leaf();
            leaf_data.items[&0] == 100 &&
            leaf_data.items[&1] == 200
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
"#);

    let r = current_thread::block_on_all(tree.sync_all());
    assert!(r.is_ok());
    assert_eq!(*tree.i.root.get_mut().unwrap().ptr.as_drp(), drp);
}

}
// LCOV_EXCL_STOP
