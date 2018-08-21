// vim: tw=80

//! COW B+-Trees, based on B-trees, Shadowing, and Clones[^CowBtrees]
//!
//! [^CowBtrees]: Rodeh, Ohad. "B-trees, shadowing, and clones." ACM Transactions on Storage (TOS) 3.4 (2008): 2.

// use the atomic crate since libstd's AtomicU64 type is still unstable
// https://github.com/rust-lang/rust/issues/32976
use atomic::{Atomic, Ordering};
use bincode;
use common::*;
use common::dml::*;
use futures::{
    Async,
    Future,
    future::{self, IntoFuture},
    Poll,
    stream::{self, Stream}
};
use futures_locks::*;
#[cfg(test)]
use serde::Serializer;
use serde::de::{Deserializer, DeserializeOwned};
#[cfg(test)] use serde_yaml;
#[cfg(test)] use std::fmt::{self, Display, Formatter};
use std::{
    borrow::Borrow,
    cell::RefCell,
    collections::VecDeque,
    fmt::Debug,
    mem,
    ops::{Bound, DerefMut, Range, RangeBounds},
    sync::{
        Arc,
    }
};
mod node;
use self::node::*;
// Node must be visible for the IDML's unit tests
pub(super) use self::node::Node;

pub use self::node::{Key, MinValue, Value};

/// Are there any elements in common between the two Ranges?
#[cfg_attr(feature = "cargo-clippy", allow(if_same_then_else))]
fn ranges_overlap<T: PartialOrd>(x: &Range<T>, y: &Range<T>) -> bool {
    if x.end <= x.start || y.end <= y.start {
        // One of the Ranges is empty
        false
    } else if x.end <= y.start {
        // x precedes y
        false
    } else if y.end <= x.start {
        // y precedes x
        false
    } else {
        true
    }
}

mod atomic_u64_serializer {
    use super::*;
    use serde::Deserialize;

    pub fn deserialize<'de, D>(d: D) -> Result<Atomic<u64>, D::Error>
        where D: Deserializer<'de>
    {
        u64::deserialize(d)
            .map(|u| Atomic::new(u))
    }

    #[cfg(test)]
    pub fn serialize<S>(x: &Atomic<u64>, s: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        s.serialize_u64(x.load(Ordering::Relaxed) as u64)
    }
}

/// Uniquely identifies any Node in the Tree.
#[derive(Debug)]
struct NodeId<K: Key> {
    /// Tree level of the Node.  Leaves are 0.
    height: u8,
    /// Less than or equal to the Node's first child/item.  Greater than the
    /// previous Node's last child/item.
    key: K
}

mod tree_root_serializer {
    use super::*;
    #[cfg(test)]
    use serde::{Serialize, ser::Error};
    use serde::Deserialize;

    pub(super) fn deserialize<'de, A, DE, K, V>(d: DE)
        -> Result<RwLock<IntElem<A, K, V>>, DE::Error>
        where A: Addr, DE: Deserializer<'de>, K: Key, V: Value
    {
        IntElem::deserialize(d)
            .map(|int_elem| RwLock::new(int_elem))
    }

    #[cfg(test)]
    pub(super) fn serialize<A, K, S, V>(x: &RwLock<IntElem<A, K, V>>, s: S)
        -> Result<S::Ok, S::Error>
        where A: Addr, K: Key, S: Serializer, V: Value
    {
        match x.try_read() {
            Ok(guard) => (*guard).serialize(s),
            Err(_) => Err(S::Error::custom("EAGAIN"))
        }
    }
}

pub struct RangeQuery<A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A>,
          K: Key + Borrow<T>,
          T: Ord + Clone + Send,
          V: Value
{
    /// If Some, then there are more nodes in the Tree to query
    cursor: Option<Bound<T>>,
    /// Data that can be returned immediately
    data: VecDeque<(K, V)>,
    end: Bound<T>,
    last_fut: Option<Box<Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                       Error=Error> + Send>>,
    /// Handle to the tree's inner
    inner: Arc<Inner<A, K, V>>,
    /// Handle to the tree's DML
    dml: Arc<D>
}

impl<A, D, K, T, V> RangeQuery<A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A>,
          K: Key + Borrow<T>,
          T: Ord + Clone + Send,
          V: Value
    {

    fn new<R>(inner: Arc<Inner<A, K, V>>, dml: Arc<D>, range: R)
        -> RangeQuery<A, D, K, T, V>
        where R: RangeBounds<T>
    {
        let cursor: Option<Bound<T>> = Some(match range.start_bound() {
            Bound::Included(b) => Bound::Included(b.clone()),
            Bound::Excluded(b) => Bound::Excluded(b.clone()),
            Bound::Unbounded => Bound::Unbounded,
        });
        let end: Bound<T> = match range.end_bound() {
            Bound::Included(e) => Bound::Included(e.clone()),
            Bound::Excluded(e) => Bound::Excluded(e.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let data = VecDeque::new();
        RangeQuery{cursor, data, end, last_fut: None, inner, dml}
    }
}

impl<A, D, K, T, V> Stream for RangeQuery<A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A> + 'static,
          K: Key + Borrow<T>,
          T: Ord + Clone + Send + 'static,
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
                        let dml2 = self.dml.clone();
                        Box::new(Tree::get_range(&self.inner, dml2, r))
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

struct CleanZonePass1Inner<D, K, V>
    where D: DML<Addr=ddml::DRP>,
          K: Key,
          V: Value
{
    /// If Some, then there are more nodes in the Tree to query
    cursor: Option<K>,

    /// Data that can be returned immediately
    data: VecDeque<NodeId<K>>,

    /// Level of the Tree that this object is meant to clean.  Leaves are 0.
    echelon: u8,

    /// Used when an operation must block
    last_fut: Option<Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>),
                       Error=Error>>>,

    /// Range of addresses to move
    pbas: Range<PBA>,

    /// Range of transactions that may contain PBAs of interest
    txgs: Range<TxgT>,

    /// Handle to the tree's inner struct
    inner: Arc<Inner<ddml::DRP, K, V>>,

    /// Handle to the tree's DML
    dml: Arc<D>
}

/// Result type of `Tree::clean_zone`
struct CleanZonePass1<D, K, V>
    where D: DML<Addr=ddml::DRP>,
          K: Key,
          V: Value
{
    inner: RefCell<CleanZonePass1Inner<D, K, V>>
}

impl<D, K, V> CleanZonePass1<D, K, V>
    where D: DML<Addr=ddml::DRP>,
          K: Key,
          V: Value
    {

    fn new(inner: Arc<Inner<ddml::DRP, K, V>>, dml: Arc<D>, pbas: Range<PBA>,
           txgs: Range<TxgT>, echelon: u8)
        -> CleanZonePass1<D, K, V>
    {
        let cursor = Some(K::min_value());
        let data = VecDeque::new();
        let last_fut = None;
        let inner = CleanZonePass1Inner{cursor, data, echelon, last_fut, pbas,
                                        txgs, inner, dml};
        CleanZonePass1{inner: RefCell::new(inner)}
    }
}

impl<D, K, V> Stream for CleanZonePass1<D, K, V>
    where D: DML<Addr=ddml::DRP> + 'static,
          K: Key,
          V: Value
{
    type Item = NodeId<K>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let first = {
            let mut i = self.inner.borrow_mut();
            i.data.pop_front()
        };  // LCOV_EXCL_LINE   kcov false negative
        first.map(|x| Ok(Async::Ready(Some(x))))
            .unwrap_or_else(|| {
                let i = self.inner.borrow();
                if i.cursor.is_some() {
                    drop(i);
                    let mut f = stream::poll_fn(|| -> Poll<Option<()>, Error> {
                        let mut i = self.inner.borrow_mut();
                        let mut f = i.last_fut.take().unwrap_or_else(|| {
                            let l = i.cursor.unwrap();
                            let pbas = i.pbas.clone();
                            let txgs = i.txgs.clone();
                            let e = i.echelon;
                            Box::new(Tree::get_dirty_nodes(i.inner.clone(),
                                i.dml.clone(), l, pbas, txgs, e))
                        });
                        match f.poll() {
                            Ok(Async::Ready((v, bound))) => {
                                i.data = v;
                                i.cursor = bound;
                                i.last_fut = None;
                                if i.data.is_empty() && i.cursor.is_some() {
                                    // Restart the search at the next bound
                                    Ok(Async::Ready(Some(())))
                                } else {
                                    // Search is done or data is ready
                                    Ok(Async::Ready(None))
                                }
                            },
                            Ok(Async::NotReady) => {
                                i.last_fut = Some(f);
                                Ok(Async::NotReady)
                            },
                            Err(e) => Err(e)
                        }
                    }).fold((), |_, _| future::ok::<(), Error>(()));
                    match f.poll() {
                        Ok(Async::Ready(())) => {
                            let mut i = self.inner.borrow_mut();
                            if i.last_fut.is_some() {
                                Ok(Async::NotReady)
                            } else {
                                Ok(Async::Ready(i.data.pop_front()))
                            }
                        },
                        Ok(Async::NotReady) => {
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
#[cfg_attr(test, derive(Deserialize, Serialize))]
#[cfg_attr(not(test), derive(Deserialize))]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
struct Inner<A: Addr, K: Key, V: Value> {
    /// Tree height.  1 if the Tree consists of a single Leaf node.
    // Use atomics so it can be modified from an immutable reference.  Accesses
    // should be very rare, so performance is not a concern.
    #[serde(with = "atomic_u64_serializer")]
    height: Atomic<u64>,
    /// Minimum node fanout.  Smaller nodes will be merged, or will steal
    /// children from their neighbors.
    min_fanout: u64,
    /// Maximum node fanout.  Larger nodes will be split.
    max_fanout: u64,
    /// Maximum node size in bytes.  Larger nodes will be split or their message
    /// buffers flushed
    _max_size: u64,
    /// Root node
    #[serde(with = "tree_root_serializer")]
    root: RwLock<IntElem<A, K, V>>
}

/// A version of `Inner` that is serializable
#[derive(Debug)]
#[derive(Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
struct InnerOnDisk<A: Addr, K: Key, V: Value> {
    height: u64,
    min_fanout: u64,
    max_fanout: u64,
    _max_size: u64,
    root: IntElem<A, K, V>
}

/// In-memory representation of a COW B+-Tree
///
/// # Generic Parameters
///
/// *`K`:   Key type.  Must be ordered and copyable; should be compact
/// *`V`:   Value type in the leaves.
pub struct Tree<A: Addr, D: DML<Addr=A>, K: Key, V: Value> {
    dml: Arc<D>,
    i: Arc<Inner<A, K, V>>
}

impl<A, D, K, V> Tree<A, D, K, V>
    where A: Addr,
          D: DML<Addr=A> + 'static,
          K: Key,
          V: Value
{
    pub fn create(dml: Arc<D>) -> Self {
        Tree::new(dml,
                  4,        // BetrFS's min fanout
                  16,       // BetrFS's max fanout
                  1<<22,    // BetrFS's max size
        )
    }

    /// Fix an Int node in danger of being underfull, returning the parent guard
    /// back to the caller
    fn fix_int<Q>(inner: Arc<Inner<A, K, V>>, dml: Arc<D>,
                  parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize, mut child: TreeWriteGuard<A, K, V>,
                  txg: TxgT)
        -> impl Future<Item=(TreeWriteGuard<A, K, V>, i8, i8), Error=Error>
        where K: Borrow<Q>, Q: Ord
    {
        // Outline:
        // First, try to merge with the right sibling
        // Then, try to steal keys from the right sibling
        // Then, try to merge with the left sibling
        // Then, try to steal keys from the left sibling
        let nchildren = parent.as_int().nchildren();
        let (fut, sib_idx, right) = {
            if child_idx < nchildren - 1 {
                let sib_idx = child_idx + 1;
                (parent.xlock(dml, sib_idx, txg), sib_idx, true)
            } else {
                let sib_idx = child_idx - 1;
                (parent.xlock(dml, sib_idx, txg), sib_idx, false)
            }
        };
        fut.map(move |(mut parent, mut sibling)| {
            let (before, after) = {
                let children = &mut parent.as_int_mut().children;
                if right {
                    if child.can_merge(&sibling, inner.max_fanout) {
                        child.merge(sibling);
                        children[child_idx].txgs.start = child.start_txg(txg);
                        children.remove(sib_idx);
                        (0, 1)
                    } else {
                        child.take_low_keys(&mut sibling);
                        children[sib_idx].key = *sibling.key();
                        children[sib_idx].txgs.start = sibling.start_txg(txg);
                        children[child_idx].txgs.start = child.start_txg(txg);
                        (0, 0)
                    }
                } else {
                    if sibling.can_merge(&child, inner.max_fanout) {
                        sibling.merge(child);
                        children[sib_idx].txgs.start = sibling.start_txg(txg);
                        children.remove(child_idx);
                        (1, 0)
                    } else {
                        child.take_high_keys(&mut sibling);
                        children[child_idx].key = *child.key();
                        children[sib_idx].txgs.start = sibling.start_txg(txg);
                        children[child_idx].txgs.start = child.start_txg(txg);
                        (1, 1)
                    }
                }
            };
            (parent, before, after)
        })
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Subroutine of range_delete.  Fixes a node that is in danger of an
    /// underflow.  Returns the node guard, and two ints which are the number of
    /// nodes that were merged before and after the fixed child, respectively.
    fn fix_if_in_danger<R, T>(inner: Arc<Inner<A, K, V>>, dml: Arc<D>,
        parent: TreeWriteGuard<A, K, V>, child_idx: usize,
        child: TreeWriteGuard<A, K, V>, range: R, ubound: Option<K>, txg: TxgT)
        -> Box<Future<Item=(TreeWriteGuard<A, K, V>, i8, i8), Error=Error>>
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        // Is the child underfull?  The limit here is b-1, not b as in
        // Tree::remove, because we've already removed keys
        if child.underflow(inner.min_fanout - 1) {
            Box::new(Tree::fix_int(inner, dml, parent,
                                   child_idx, child, txg))
        } else if !child.is_leaf() {
            // How many grandchildren are in the cut?
            let (start_idxbound, end_idxbound)
                = Tree::<A, D, K, V>::range_delete_get_bounds(&child,
                                                              &range, ubound);
            let cut_grandkids = match (start_idxbound, end_idxbound) {
                (Bound::Included(_), Bound::Excluded(_)) => 0,
                (Bound::Included(_), Bound::Included(_)) => 1,
                (Bound::Excluded(_), Bound::Excluded(_)) => 1,
                (Bound::Excluded(i), Bound::Included(j)) if j > i => 2,
                (Bound::Excluded(i), Bound::Included(j)) if j <= i => 1,
                // LCOV_EXCL_START  kcov false negative
                (Bound::Excluded(_), Bound::Included(_)) => unreachable!(),
                (Bound::Unbounded, _) | (_, Bound::Unbounded) => unreachable!(),
                // LCOV_EXCL_STOP
            };
            let b = inner.min_fanout as usize;
            if child.as_int().nchildren() - cut_grandkids <= b - 1 {
                Box::new(Tree::fix_int(inner, dml, parent,
                                       child_idx, child, txg))
            } else {
                Box::new(Ok((parent, 0, 0)).into_future())
            }
        } else {
            Box::new(Ok((parent, 0, 0)).into_future())
        }
    }

    #[cfg(test)]
    pub fn from_str(dml: Arc<D>, s: &str) -> Self {
        let i: Arc<Inner<A, K, V>> = Arc::new(serde_yaml::from_str(s).unwrap());
        Tree{dml, i}
    }

    /// Insert value `v` into the tree at key `k`, returning the previous value
    /// for that key, if any.
    pub fn insert(&self, k: K, v: V, txg: TxgT)
        -> impl Future<Item=Option<V>, Error=Error>
    {
        let dml2 = self.dml.clone();
        let dml3 = self.dml.clone();
        let inner2 = self.i.clone();
        self.write()
            .and_then(move |guard| {
                Tree::xlock_root(dml2, guard, txg)
                     .and_then(move |(root_guard, child_guard)| {
                         Tree::insert_locked(inner2, dml3, root_guard, child_guard, k, v, txg)
                     })
            })
    }

    /// Insert value `v` into an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn insert_int(inner: Arc<Inner<A, K, V>>, dml: Arc<D>,
                  mut parent: TreeWriteGuard<A, K, V>, child_idx: usize,
                  mut child: TreeWriteGuard<A, K, V>, k: K, v: V, txg: TxgT)
        -> Box<Future<Item=Option<V>, Error=Error> + Send> {

        // First, split the node, if necessary
        if (*child).should_split(inner.max_fanout) {
            let (old_txgs, new_elem) = child.split(txg);
            parent.as_int_mut().children[child_idx].txgs = old_txgs;
            parent.as_int_mut().children.insert(child_idx + 1, new_elem);
            // Reinsert into the parent, which will choose the correct child
            Box::new(Tree::insert_int_no_split(inner, dml, parent, k, v, txg))
        } else {
            if child.is_leaf() {
                let elem = &mut parent.as_int_mut().children[child_idx];
                Box::new(Tree::<A, D, K, V>::insert_leaf_no_split(elem,
                    child, k, v, txg))
            } else {
                drop(parent);
                Box::new(Tree::insert_int_no_split(inner, dml, child, k, v, txg))
            }
        }
    }

    /// Insert a value into a leaf node without splitting it
    fn insert_leaf_no_split(elem: &mut IntElem<A, K, V>,
                  mut child: TreeWriteGuard<A, K, V>, k: K, v: V, txg: TxgT)
        -> impl Future<Item=Option<V>, Error=Error>
    {
        let old_v = child.as_leaf_mut().insert(k, v);
        elem.txgs = txg..txg + 1;
        return Ok(old_v).into_future()
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Helper for `insert`.  Handles insertion once the tree is locked
    fn insert_locked(inner: Arc<Inner<A, K, V>>, dml: Arc<D>,
                     mut relem: RwLockWriteGuard<IntElem<A, K, V>>,
                     mut rnode: TreeWriteGuard<A, K, V>, k: K, v: V, txg: TxgT)
        -> Box<Future<Item=Option<V>, Error=Error> + Send>
    {
        // First, split the root node, if necessary
        if rnode.should_split(inner.max_fanout) {
            let (old_txgs, new_elem) = rnode.split(txg);
            let new_root_data = NodeData::Int( IntData::new(vec![new_elem]));
            let old_root_data = mem::replace(rnode.deref_mut(), new_root_data);
            let old_root_node = Node::new(old_root_data);
            let old_ptr = TreePtr::Mem(Box::new(old_root_node));
            let old_elem = IntElem::new(K::min_value(), old_txgs, old_ptr );
            rnode.as_int_mut().children.insert(0, old_elem);
            inner.height.fetch_add(1, Ordering::Relaxed);
        }

        if rnode.is_leaf() {
            Box::new(Tree::<A, D, K, V>::insert_leaf_no_split(&mut *relem,
                                                              rnode, k, v, txg))
        } else {
            drop(relem);
            Box::new(Tree::insert_int_no_split(inner, dml, rnode, k, v, txg))
        }
    }

    /// Insert a value into an int node without splitting it
    fn insert_int_no_split(inner: Arc<Inner<A, K, V>>, dml: Arc<D>,
                           node: TreeWriteGuard<A, K, V>, k: K, v: V, txg: TxgT)
        -> impl Future<Item=Option<V>, Error=Error>
    {
        let child_idx = node.as_int().position(&k);
        let fut = node.xlock(dml.clone(), child_idx, txg);
        fut.and_then(move |(parent, child)| {
                Tree::insert_int(inner, dml, parent, child_idx, child, k, v, txg)
        })
    }

    /// Lookup the value of key `k`.  Return `None` if no value is present.
    pub fn get(&self, k: K) -> impl Future<Item=Option<V>, Error=Error>
    {
        let dml2 = self.dml.clone();
        let dml3 = self.dml.clone();
        self.read()
            .and_then(move |guard| {
                guard.rlock(dml2)
                     .and_then(move |guard| Tree::get_r(dml3, guard, k))
            })
    }

    /// Lookup the value of key `k` in a node, which must already be locked.
    fn get_r(dml: Arc<D>, node: TreeReadGuard<A, K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + Send>
    {
        let dml2 = dml.clone();
        let next_node_fut = match *node {
            NodeData::Leaf(ref leaf) => {
                return Box::new(Ok(leaf.get(&k)).into_future())
            },
            NodeData::Int(ref int) => {
                let child_elem = &int.children[int.position(&k)];
                child_elem.rlock(dml)
            }
        };
        drop(node);
        Box::new(
            next_node_fut
            .and_then(move |next_node| Tree::get_r(dml2, next_node, k))
        )
    }

    /// Private helper for `Range::poll`.  Returns a subset of the total
    /// results, consisting of all matching (K,V) pairs within a single Leaf
    /// Node, plus an optional Bound for the next iteration of the search.  If
    /// the Bound is `None`, then the search is complete.
    fn get_range<R, T>(inner: &Inner<A, K, V>, dml: Arc<D>, range: R)
        -> impl Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                       Error=Error> + Send
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + Send + 'static
    {
        Tree::<A, D, K, V>::read_root(&inner)
            .and_then(move |guard| {
                guard.rlock(dml.clone())
                     .and_then(move |g| Tree::get_range_r(dml, g, None, range))
            })
    }

    /// Range lookup beginning in the node `guard`.  `next_guard`, if present,
    /// must be the node immediately to the right (and possibly up one or more
    /// levels) from `guard`.
    fn get_range_r<R, T>(dml: Arc<D>, guard: TreeReadGuard<A, K, V>,
                         next_guard: Option<TreeReadGuard<A, K, V>>, range: R)
        -> Box<Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                      Error=Error> + Send>
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + Send + 'static
    {
        let dml2 = dml.clone();
        let (child_fut, next_fut) = match *guard {
            NodeData::Leaf(ref leaf) => {
                let (v, more) = leaf.range(range.clone());
                let ret = if v.is_empty() && more && next_guard.is_some() {
                    // We must've started the query with a key that's not
                    // present, and lies between two leaves.  Check the next
                    // node
                    Tree::get_range_r(dml2, next_guard.unwrap(), None, range)
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
                let next_fut = if child_idx < int.nchildren() - 1 {
                    let dml3 = dml2.clone();
                    Box::new(
                        int.children[child_idx + 1].rlock(dml3)
                            .map(|guard| Some(guard))
                    ) as Box<Future<Item=Option<TreeReadGuard<A, K, V>>,
                                    Error=Error> + Send>
                } else {
                    Box::new(Ok(next_guard).into_future())
                        as Box<Future<Item=Option<TreeReadGuard<A, K, V>>,
                                      Error=Error> + Send>
                };
                let child_fut = child_elem.rlock(dml2);
                (child_fut, next_fut)
            } // LCOV_EXCL_LINE kcov false negative
        };
        drop(guard);
        Box::new(
            child_fut.join(next_fut)
                .and_then(move |(child_guard, next_guard)| {
                Tree::get_range_r(dml, child_guard, next_guard, range)
            })
        ) as Box<Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                        Error=Error> + Send>
    }

    /// Return the highest valued key in the `Tree`
    pub fn last_key(&self) -> impl Future<Item=Option<K>, Error=Error> {
        let dml2 = self.dml.clone();
        let dml3 = self.dml.clone();
        self.read()
            .and_then(move |guard| {
                guard.rlock(dml2)
                     .and_then(move |guard| Tree::last_key_r(dml3, guard))
            })
    }

    /// Find the last key amongst a node (which must already be locked), and its
    /// children
    fn last_key_r(dml: Arc<D>, node: TreeReadGuard<A, K, V>)
        -> Box<Future<Item=Option<K>, Error=Error> + Send>
    {
        let dml2 = dml.clone();
        let next_node_fut = match *node {
            NodeData::Leaf(ref leaf) => {
                return Box::new(Ok(leaf.last_key()).into_future())
            },
            NodeData::Int(ref int) => {
                let child_elem = &int.children[int.children.len() - 1];
                child_elem.rlock(dml)
            }
        };
        drop(node);
        Box::new(
            next_node_fut
            .and_then(move |next_node| Tree::last_key_r(dml2, next_node))
        )
    }

    /// Merge the root node with its children, if necessary
    fn merge_root(inner: &Inner<A, K, V>,
                  root_guard: &mut TreeWriteGuard<A, K, V>)
    {
        if ! root_guard.is_leaf() && root_guard.as_int().nchildren() == 1
        {
            // Merge root node with its child
            let child = root_guard.as_int_mut().children.pop().unwrap();
            let new_root_data = match child.ptr {
                TreePtr::Mem(n) => n.0.try_unwrap().unwrap(),
                // LCOV_EXCL_START
                _ => unreachable!(
                    "Can't merge_root without first dirtying the tree"),
                //LCOV_EXCL_STOP
            };
            mem::replace(root_guard.deref_mut(), new_root_data);
            inner.height.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Open a `Tree` from its serialized representation
    pub fn open(dml: Arc<D>, on_disk: TreeOnDisk) -> bincode::Result<Self> {
        let inner = Arc::new(bincode::deserialize(&on_disk.0[..])?);
        Ok(Tree{ dml, i: inner })
    }

    /// Lookup a range of (key, value) pairs for keys within the range `range`.
    pub fn range<R, T>(&self, range: R) -> RangeQuery<A, D, K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T>,
              T: Ord + Clone + Send
    {
        RangeQuery::new(self.i.clone(), self.dml.clone(), range)
    }

    /// Delete a range of keys
    pub fn range_delete<R, T>(&self, range: R, txg: TxgT)
        -> impl Future<Item=(), Error=Error>
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        // Outline:
        // 1) Traverse the tree removing all requested KV-pairs, leaving damaged
        //    nodes
        // 2) Traverse the tree again, fixing in-danger nodes from top-down
        // 3) Collapse the root node, if it has 1 child
        let rangeclone = range.clone();
        let dml2 = self.dml.clone();
        let dml3 = self.dml.clone();
        let dml4 = self.dml.clone();
        let dml6 = self.dml.clone();
        let dml7 = self.dml.clone();
        let inner2 = self.i.clone();
        let inner3 = self.i.clone();
        self.write()
            .and_then(move |guard| {
                Tree::xlock_root(dml2, guard, txg)
                     .and_then(move |(tree_guard, root_guard)| {
                         Tree::range_delete_pass1(dml6, root_guard, range, None,
                                                  txg)
                             .map(|_| tree_guard)
                     })
            })
            .and_then(move |guard| {
                Tree::xlock_root(dml3, guard, txg)
                    .and_then(move |(tree_guard, root_guard)| {
                        Tree::range_delete_pass2(inner2, dml7, root_guard,
                                                 rangeclone, None, txg)
                            .map(|_| tree_guard)
                    })
            })
            // Finally, collapse the root node, if it has 1 child.  During
            // normal remove operations we can't do this, because we drop the
            // lock on the root node before fixing all of its children.  But the
            // entire tree stays locked during range_delete, so it's possible to
            // fix the root node at the end
            .and_then(move |tree_guard| {
                Tree::xlock_root(dml4, tree_guard, txg)
                    .map(move |(tree_guard, mut root_guard)| {
                        Tree::<A, D, K, V>::merge_root(&*inner3,
                                                       &mut root_guard);
                        // Keep the whole tree locked during range_delete
                        drop(tree_guard)
                    })  // LCOV_EXCL_LINE   kcov false negative
            })
    }

    /// Subroutine of range_delete.  Returns the bounds, as indices, of the
    /// affected children of this node.
    fn range_delete_get_bounds<R, T>(guard: &TreeWriteGuard<A, K, V>,
                                     range: &R, ubound: Option<K>)
        -> (Bound<usize>, Bound<usize>)
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        debug_assert!(!guard.is_leaf());
        let l = guard.as_int().nchildren();
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
                let idx = guard.as_int().position(t);
                if ubound.is_some() && t >= ubound.unwrap().borrow() {
                    Bound::Excluded(idx + 1)
                } else if guard.as_int().children[idx].key.borrow() == t {
                    Bound::Excluded(idx)
                } else {
                    Bound::Included(idx)
                }
            }
        };
        (start_idx_bound, end_idx_bound)
    }

    /// Depth-first traversal deleting keys without reshaping tree
    /// `ubound` is the first key in the Node immediately to the right of
    /// this one, unless this is the rightmost Node on its level.
    fn range_delete_pass1<R, T>(dml: Arc<D>, mut guard: TreeWriteGuard<A, K, V>,
                                range: R, ubound: Option<K>, txg: TxgT)
        -> impl Future<Item=(), Error=Error>
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
        let l = guard.as_int().nchildren();
        let (start_idx_bound, end_idx_bound)
            = Tree::<A, D, K, V>::range_delete_get_bounds(&guard, &range,
                                                          ubound);
        let dml2 = dml.clone();
        let dml3 = dml.clone();
        let fut: Box<Future<Item=TreeWriteGuard<A, K, V>, Error=Error>>
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
                Box::new(guard.xlock(dml2, j, txg)
                    .and_then(move |(parent_guard, child_guard)| {
                        Tree::range_delete_pass1(dml3, child_guard, range,
                                                 ubound, txg)
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
                Box::new(guard.xlock(dml2, i, txg)
                    .and_then(move |(parent_guard, child_guard)| {
                        Tree::range_delete_pass1(dml3, child_guard, range,
                                                 ubound, txg)
                            .map(move |_| parent_guard)
                    }))
            },
            (Bound::Excluded(i), Bound::Included(j)) if j > i => {
                // Recurse into a Node at the beginning and end
                let range2 = range.clone();
                let ub_l = Some(guard.as_int().children[i + 1].key);
                let ub_h = if j < l - 1 {
                    Some(guard.as_int().children[j + 1].key)
                } else {
                    ubound
                };
                let dml4 = dml.clone();
                let dml5 = dml.clone();
                Box::new(guard.xlock(dml2, i, txg)
                    .and_then(move |(parent_guard, child_guard)| {
                        Tree::range_delete_pass1(dml3, child_guard, range, ub_l,
                                                 txg)
                            .map(|_| parent_guard)
                    }).and_then(move |parent_guard| {
                        parent_guard.xlock(dml4, j, txg)
                    }).and_then(move |(parent_guard, child_guard)| {
                        Tree::range_delete_pass1(dml5, child_guard, range2,
                                                 ub_h, txg)
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
                Box::new(guard.xlock(dml2, i, txg)
                    .and_then(move |(parent_guard, child_guard)| {
                        Tree::range_delete_pass1(dml3, child_guard, range,
                                                 ubound, txg)
                            .map(move |_| parent_guard)
                    }))
            },
            // LCOV_EXCL_START  kcov false negative
            (Bound::Excluded(_), Bound::Included(_)) => unreachable!(),
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => unreachable!(),
            // LCOV_EXCL_STOP
        };

        // Finally, remove nodes in the middle
        Box::new(fut.map(move |mut guard| {
            let low = match start_idx_bound {
                Bound::Excluded(i) => i + 1,
                Bound::Included(i) => i,
                Bound::Unbounded => unreachable!()  // LCOV_EXCL_LINE
            };
            let high = match end_idx_bound {
                Bound::Excluded(j) | Bound::Included(j) => j,
                Bound::Unbounded => unreachable!()  // LCOV_EXCL_LINE
            };
            if high > low {
                guard.as_int_mut().children.drain(low..high);
            }
        }))
    }

    /// Depth-first traversal reshaping the tree after some keys were deleted by
    /// range_delete_pass1.
    fn range_delete_pass2<R, T>(inner: Arc<Inner<A, K, V>>, dml: Arc<D>,
                                guard: TreeWriteGuard<A, K, V>,
                                range: R, ubound: Option<K>, txg: TxgT)
        -> Box<Future<Item=(), Error=Error>>
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        // Outline:
        // Traverse the tree just as in range_delete_pass1, but fixup any nodes
        // that are in danger.  A node is in-danger if it is in the cut and:
        // a) it has an underflow, or
        // b) it has b entries and one child in the cut, or
        // c) it has b + 1 entries and two children in the cut
        if guard.is_leaf() {
            // This node was already fixed.  No need to recurse further
            return Box::new(Ok(()).into_future());
        }

        let (start_idx_bound, end_idx_bound)
            = Tree::<A, D, K, V>::range_delete_get_bounds( &guard, &range,
                                                           ubound);
        let range2 = range.clone();
        let range3 = range.clone();
        let children_to_fix = match (start_idx_bound, end_idx_bound) {
            (Bound::Included(_), Bound::Excluded(_)) => (None, None),
            (Bound::Included(_), Bound::Included(j)) => (None, Some(j)),
            (Bound::Excluded(i), Bound::Excluded(_)) => (Some(i), None),
            (Bound::Excluded(i), Bound::Included(j)) if j > i =>
                (Some(i), Some(j)),
            (Bound::Excluded(i), Bound::Included(j)) if j <= i =>
                (Some(i), None),
            // LCOV_EXCL_START  kcov false negative
            (Bound::Excluded(_), Bound::Included(_)) => unreachable!(),
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => unreachable!(),
            // LCOV_EXCL_STOP
        };
        let fixit = move |parent_guard: TreeWriteGuard<A, K, V>, idx: usize,
                          range: R|
        {
            let l = parent_guard.as_int().nchildren();
            let child_ubound = if idx < l - 1 {
                Some(parent_guard.as_int().children[idx + 1].key)
            } else {
                ubound
            };
            let range2 = range.clone();
            let dml2 = dml.clone();
            let dml3 = dml.clone();
            let dml4 = dml.clone();
            let dml5 = dml.clone();
            let inner2 = inner.clone();
            let inner3 = inner.clone();
            Box::new(
                parent_guard.xlock(dml2, idx, txg)
                .and_then(move |(parent_guard, child_guard)| {
                    Tree::fix_if_in_danger(inner2, dml4, parent_guard, idx,
                        child_guard, range, child_ubound, txg)
                }).and_then(move |(parent_guard, merged_before, merged_after)| {
                    let merged = merged_before + merged_after;
                    parent_guard.xlock(dml3, idx - merged_before as usize,
                                       txg)
                        .map(move |(parent, child)| (parent, child, merged))
                }).and_then(move |(parent_guard, child_guard, merged)| {
                    Tree::range_delete_pass2(inner3, dml5, child_guard, range2,
                                             child_ubound, txg)
                        .map(move |_| (parent_guard, merged))
                })
            ) as Box<Future<Item=(TreeWriteGuard<A, K, V>, i8), Error=Error>>
        };
        let fut = match children_to_fix.0 {
            None => Box::new(Ok((guard, 0i8)).into_future())
                as Box<Future<Item=(TreeWriteGuard<A, K, V>, i8), Error=Error>>,
            Some(idx) => fixit(guard, idx, range2)
        }
        .and_then(move |(parent_guard, merged)|
            match children_to_fix.1 {
                None => Box::new(Ok((parent_guard, merged)).into_future())
                    as Box<Future<Item=(TreeWriteGuard<A, K, V>, i8),
                                  Error=Error>>,
                Some(idx) => fixit(parent_guard, idx - merged as usize, range3)
            }
        ).map(|_| ());
        Box::new(fut)
    }

    fn new(dml: Arc<D>, min_fanout: u64, max_fanout: u64, max_size: u64) -> Self
    {
        // Since there are no on-disk children, the initial TXG range is empty
        let txgs = TxgT::from(0)..TxgT::from(0);
        let i: Arc<Inner<A, K, V>> = Arc::new(Inner {
            height: Atomic::new(1),
            min_fanout, max_fanout,
            _max_size: max_size,
            root: RwLock::new(
                IntElem::new(K::min_value(),
                    txgs,
                    TreePtr::Mem(
                        Box::new(
                            Node::new(
                                NodeData::Leaf(
                                    LeafData::new()
                                )
                            )
                        )
                    )
                )
            )
        });
        Tree{ dml, i }
    }

    /// Remove and return the value at key `k`, if any.
    pub fn remove(&self, k: K, txg: TxgT)
        -> impl Future<Item=Option<V>, Error=Error> + Send
    {
        let dml2 = self.dml.clone();
        let dml3 = self.dml.clone();
        let i2 = self.i.clone();
        self.write()
            .and_then(move |guard| {
                Tree::xlock_root(dml2, guard, txg)
                    .and_then(move |(_root_guard, child_guard)| {
                        Tree::remove_locked(i2, dml3, child_guard, k, txg)
                    })
        })
    }

    /// Remove key `k` from an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn remove_int(inner: Arc<Inner<A, K, V>>, dml: Arc<D>,
                  parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize, child: TreeWriteGuard<A, K, V>, k: K,
                  txg: TxgT)
        -> Box<Future<Item=Option<V>, Error=Error> + Send>
    {
        let dml2 = dml.clone();
        // First, fix the node, if necessary
        if child.underflow(inner.min_fanout) {
            let i2 = inner.clone();
            Box::new(
                Tree::fix_int(i2, dml.clone(), parent, child_idx, child, txg)
                    .and_then(move |(parent, _, _)| {
                        let child_idx = parent.as_int().position(&k);
                        parent.xlock(dml, child_idx, txg)
                    }).and_then(move |(parent, child)| {
                        drop(parent);
                        Tree::remove_no_fix(inner, dml2, child, k, txg)
                    })
            )
        } else {
            drop(parent);
            Tree::remove_no_fix(inner, dml, child, k, txg)
        }
    }

    /// Helper for `remove`.  Handles removal once the tree is locked
    fn remove_locked(inner: Arc<Inner<A, K, V>>, dml: Arc<D>,
                     mut root: TreeWriteGuard<A, K, V>, k: K, txg: TxgT)
        -> Box<Future<Item=Option<V>, Error=Error> + Send>
    {
        Tree::<A, D, K, V>::merge_root(&*inner, &mut root);
        Tree::remove_no_fix(inner, dml, root, k, txg)
    }

    /// Remove key `k` from a node, but don't try to fixup the node.
    fn remove_no_fix(inner: Arc<Inner<A, K, V>>, dml: Arc<D>,
                     mut node: TreeWriteGuard<A, K, V>, k: K, txg: TxgT)
        -> Box<Future<Item=Option<V>, Error=Error> + Send>
    {

        if node.is_leaf() {
            let old_v = node.as_leaf_mut().remove(&k);
            return Box::new(Ok(old_v).into_future());
        } else {
            let child_idx = node.as_int().position(&k);
            let fut = node.xlock(dml.clone(), child_idx, txg);
            Box::new(fut.and_then(move |(parent, child)| {
                    Tree::remove_int(inner, dml, parent, child_idx, child, k,
                                     txg)
                })
            )
        }
    }

    /// Flush all in-memory Nodes to disk, returning a serialized `TreeOnDisk`
    /// object
    // Like range_delete, keep the entire Tree locked during flush.  That's
    // because we need to write child nodes before we have valid addresses for
    // their parents' child pointers.  It's also the only way to guarantee that
    // the Tree will be completely clean by the time that flush returns.  Flush
    // will probably only happen during TXG flush, which is once every few
    // seconds.
    //
    // Alternatively, it would be possible to create a streaming flusher like
    // RangeQuery that would descend through the tree multiple times, flushing a
    // portion at each time.  But it wouldn't be able to guarantee a clean tree.
    pub fn flush(&self, txg: TxgT) -> impl Future<Item=TreeOnDisk, Error=Error>
    {
        let dml2 = self.dml.clone();
        let inner2 = self.i.clone();
        self.write()
            .and_then(move |root_guard| {
            if root_guard.ptr.is_dirty() {
                // If the root is dirty, then we have ownership over it.  But
                // another task may still have a lock on it.  We must acquire
                // then release the lock to ensure that we have the sole
                // reference.
                let dml3 = dml2.clone();
                let fut = Tree::xlock_root(dml2, root_guard, txg)
                    .and_then(move |(mut root_guard, child_guard)|
                {
                    drop(child_guard);
                    let ptr = mem::replace(&mut root_guard.ptr, TreePtr::None);
                    Tree::flush_r(dml3, ptr.into_node(), txg)
                        .map(move |(addr, txgs)| {
                            root_guard.ptr = TreePtr::Addr(addr);
                            root_guard.txgs = txgs;
                            root_guard
                        })
                });
                Box::new(fut) as Box<Future<Item=_, Error=Error> + Send>
            } else {
                Box::new(future::ok::<_, Error>(root_guard))
            }
        })
        .map(move |root_guard| {
            let root = IntElem::<A, K, V>{
                key: root_guard.key.clone(),
                txgs: root_guard.txgs.clone(),
                ptr: TreePtr::Addr(root_guard.ptr.as_addr().clone())
            };
            let iod = InnerOnDisk{
                height: inner2.height.load(Ordering::Relaxed),
                min_fanout: inner2.min_fanout,
                max_fanout: inner2.max_fanout,
                _max_size: inner2._max_size,
                root
            };
            TreeOnDisk(bincode::serialize(&iod).unwrap())
        })
    }

    fn write_leaf(dml: Arc<D>, node: Box<Node<A, K, V>>, txg: TxgT)
        -> impl Future<Item=A, Error=Error>
    {
        let arc: Arc<Node<A, K, V>> = Arc::new(*node);
        dml.put(arc, Compression::None, txg)
    }

    fn flush_r(dml: Arc<D>, mut node: Box<Node<A, K, V>>, txg: TxgT)
        -> Box<Future<Item=(D::Addr, Range<TxgT>), Error=Error> + Send>
    {
        if node.0.get_mut().unwrap().is_leaf() {
            let fut = Tree::write_leaf(dml, node, txg)
                .map(move |addr| {
                    (addr, txg..txg + 1)
                });
            return Box::new(fut);
        }
        let mut ndata = node.0.try_write().unwrap();

        // We need to flush each dirty child, rewrite its TreePtr, and update
        // the Node's txg range.  Satisfying the borrow checker requires that
        // xlock's continuation have ownership over the child IntElem.  So we
        // need to deconstruct the entire NodeData.children vector and
        // reassemble it after the join_all
        let dml2 = dml.clone();
        let children_fut = ndata.as_int_mut().children.drain(..)
        .map(move |mut elem| {
            if elem.is_dirty()
            {
                // If the child is dirty, then we have ownership over it.  We
                // need to lock it, then release the lock.  Then we'll know that
                // we have exclusive access to it, and we can move it into the
                // Cache.
                let key = elem.key;
                let dml3 = dml.clone();
                let fut = elem.ptr.as_mem().xlock().and_then(move |guard|
                {
                    drop(guard);
                    Tree::flush_r(dml3, elem.ptr.into_node(), txg)
                }).map(move |(addr, txgs)| {
                    IntElem::new(key, txgs, TreePtr::Addr(addr))
                });
                Box::new(fut)
                    as Box<Future<Item=IntElem<A, K, V>, Error=Error> + Send>
            } else { // LCOV_EXCL_LINE kcov false negative
                Box::new(future::ok(elem))
                    as Box<Future<Item=IntElem<A, K, V>, Error=Error> + Send>
            }
        })
        .collect::<Vec<_>>();
        Box::new(
            future::join_all(children_fut)
            .and_then(move |elems| {
                let start_txg = elems.iter()
                    .map(|e| e.txgs.start)
                    .min()
                    .unwrap();
                ndata.as_int_mut().children = elems;
                drop(ndata);
                let arc: Arc<Node<A, K, V>> = Arc::new(*node);
                dml2.put(arc, Compression::None, txg)
                    .map(move |addr| (addr, start_txg..txg + 1))
            })
        )
    }

    /// Lock the Tree for reading
    fn read(&self) -> impl Future<Item=RwLockReadGuard<IntElem<A, K, V>>,
                                     Error=Error>
    {
        Tree::<A, D, K, V>::read_root(&self.i)
    }

    fn read_root(inner: &Inner<A, K, V>)
        -> impl Future<Item=RwLockReadGuard<IntElem<A, K, V>>, Error=Error>
    {
        inner.root.read().map_err(|_| Error::EPIPE)
    }

    /// Lock the Tree for writing
    fn write(&self) -> impl Future<Item=RwLockWriteGuard<IntElem<A, K, V>>,
                                      Error=Error>
    {
        Tree::<A, D, K, V>::write_root(&self.i)
    }

    fn write_root(inner: &Inner<A, K, V>)
        -> impl Future<Item=RwLockWriteGuard<IntElem<A, K, V>>, Error=Error>
    {
        inner.root.write().map_err(|_| Error::EPIPE)
    }

    /// Lock the root `IntElem` exclusively.  If it is not already resident in
    /// memory, then COW it.
    fn xlock_root(dml: Arc<D>, mut guard: RwLockWriteGuard<IntElem<A, K, V>>,
                  txg: TxgT)
        -> (Box<Future<Item=(RwLockWriteGuard<IntElem<A, K, V>>,
                             TreeWriteGuard<A, K, V>), Error=Error> + Send>)
    {
        guard.txgs.end = txg + 1;
        if guard.ptr.is_mem() {
            Box::new(
                guard.ptr.as_mem().0.write()
                     .map(move |child_guard| {
                          (guard, TreeWriteGuard::Mem(child_guard))
                     }).map_err(|_| Error::EPIPE)
            )
        } else {
            let addr = *guard.ptr.as_addr();
            Box::new(
                dml.pop::<Arc<Node<A, K, V>>, Arc<Node<A, K, V>>>(
                    &addr, txg).map(move |arc|
                {
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
impl<A: Addr, D: DML<Addr=A>, K: Key, V: Value> Display for Tree<A, D, K, V> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(&serde_yaml::to_string(&*self.i).unwrap())
    }
}

// These methods are only for direct trees
impl<D, K, V> Tree<ddml::DRP, D, K, V>
    where D: DML<Addr=ddml::DRP> + 'static,
          K: Key,
          V: Value
{
    /// Clean `zone` by moving all of its records to other zones.
    pub fn clean_zone(&self, pbas: Range<PBA>, txgs: Range<TxgT>, txg: TxgT)
        -> impl Future<Item=(), Error=Error>
    {
        // We can't rewrite children before their parents while sticking to a
        // lock-coupling discipline.  And we can't rewrite parents before their
        // children, because we can't tell which parents have children that must
        // be modified.  So we'll use a two-pass approach.
        // Pass 1) Build a list of Nodes that must be rewritten
        // Pass 2) Rewrite each affected Node.
        // It's safe to do this without locking the entire tree, because we
        // should only be cleaning Closed zones, and no new Nodes will be
        // written to a closed zone until after it gets erased and reopened, and
        // that won't happen before we finish cleaning it.
        //
        // Furthermore, we'll repeat both passes for each level of the tree.
        // Not because we strictly need to, but because rewriting the lowest
        // levels first will modify many mid-level nodes, obviating the need
        // to rewrite them.  It simplifies the first pass, too.

        // It's ok to read the tree height before we lock the tree, because:
        // 1) If tree height increases before we lock the tree, then the new
        //    root node obviously can't be stored in the target zone
        // 2) If the tree height decreases before we lock the tree, then that's
        //    just one level we won't have to clean anymore
        let tree_height = self.i.height.load(Ordering::Relaxed) as u8;
        let inner2 = self.i.clone();
        let dml2 = self.dml.clone();
        stream::iter_ok(0..tree_height).for_each(move |echelon| {
            let inner3 = inner2.clone();
            let dml3 = dml2.clone();
            CleanZonePass1::new(inner2.clone(), dml2.clone(), pbas.clone(),
                                txgs.clone(), echelon)
                .collect()
                .and_then(move |nodes| {
                    stream::iter_ok(nodes.into_iter()).for_each(move |node| {
                        // TODO: consider attempting to rewrite multiple nodes
                        // at once, so as not to spend so much time traversing
                        // the tree
                        Tree::rewrite_node(inner3.clone(), dml3.clone(), node,
                                           txg)
                    })
                })
        })
    }

    /// Find all Nodes starting at `key` at a given level of the Tree which lay
    /// in the indicated range of PBAs.  `txgs` must include all transactions in
    /// which anything was written to any block in `pbas`.
    fn get_dirty_nodes(inner: Arc<Inner<ddml::DRP, K, V>>, dml: Arc<D>, key: K,
                       pbas: Range<PBA>, txgs: Range<TxgT>, echelon: u8)
        -> impl Future<Item=(VecDeque<NodeId<K>>, Option<K>), Error=Error>
    {
        Tree::<ddml::DRP, D, K, V>::read_root(&*inner)
            .and_then(move |guard| {
                let h = inner.height.load(Ordering::Relaxed) as u8;
                if h == echelon + 1 {
                    // Clean the tree root
                    let dirty = if guard.ptr.is_addr() &&
                        guard.ptr.as_addr().pba() >= pbas.start &&
                        guard.ptr.as_addr().pba() < pbas.end {
                        let mut v = VecDeque::new();
                        v.push_back(NodeId{height: echelon, key: guard.key});
                        v
                    } else {   // LCOV_EXCL_LINE   kcov false negative
                        VecDeque::new()
                    };
                    Box::new(future::ok((dirty, None)))
                        as Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>),
                                      Error=Error>>
                } else {
                    let fut = guard.rlock(dml.clone())
                         .and_then(move |guard| {
                             Tree::get_dirty_nodes_r(dml, guard, h - 1, None,
                                                     key, pbas, txgs, echelon)
                         });
                    Box::new(fut)
                        as Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>),
                                      Error=Error>>
                }
            })
    }

    /// Find dirty nodes in `PBA` range `pbas`, beginning at `key`.
    /// `next_key`, if present, must be the key of the node immediately to the
    /// right (and possibly up one or more levels) from `guard`.  `height` is
    /// the tree height of `guard`, where leaves are 0.
    fn get_dirty_nodes_r(dml: Arc<D>, guard: TreeReadGuard<ddml::DRP, K, V>,
                         height: u8,
                         next_key: Option<K>,
                         key: K, pbas: Range<PBA>, txgs: Range<TxgT>,
                         echelon: u8)
        -> Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>), Error=Error>>
    {
        if height == echelon + 1 {
            let nodes = guard.as_int().children.iter().filter_map(|child| {
                if child.ptr.is_addr() &&
                    child.ptr.as_addr().pba() >= pbas.start &&
                    child.ptr.as_addr().pba() < pbas.end
                {
                    if height == 1 {
                        assert!(ranges_overlap(&txgs, &child.txgs),
                            "Child node's TXG range {:?} did not overlap the query range {:?}",
                            &child.txgs, &txgs);
                    }
                    Some(NodeId{height: height - 1, key: child.key})
                } else {
                    if height == 1 {
                        assert!(!ranges_overlap(&txgs, &child.txgs),
                        "Child node's TXG range {:?} overlapped the query range {:?}, but did not have blocks in the PBA range {:?}",
                            &child.txgs, &txgs, &pbas);
                    }
                    None
                }
            }).collect::<VecDeque<_>>();
            return Box::new(future::ok((nodes, next_key)))
        }
        // Find the first child >= key whose txg range overlaps with txgs
        let idx0 = guard.as_int().position(&key);
        let idx_in_range = (idx0..guard.as_int().nchildren()).filter(|idx| {
            ranges_overlap(&guard.as_int().children[*idx].txgs, &txgs)
        }).nth(0);
        if let Some(idx) = idx_in_range {
            let next_key = if idx < guard.as_int().nchildren() - 1 {
                Some(guard.as_int().children[idx + 1].key)
            } else {
                next_key
            };
            let child_fut = guard.as_int().children[idx].rlock(dml.clone());
            drop(guard);
            Box::new(
                child_fut.and_then(move |child_guard| {
                    Tree::get_dirty_nodes_r(dml, child_guard, height - 1,
                                            next_key, key, pbas, txgs, echelon)
                })
            ) as Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>), Error=Error>>
        } else {
            Box::new(future::ok((VecDeque::new(), next_key)))
        }
    }

    /// Rewrite `node`, without modifying its contents
    fn rewrite_node(inner: Arc<Inner<ddml::DRP, K, V>>, dml: Arc<D>,
                    node: NodeId<K>, txg: TxgT)
        -> impl Future<Item=(), Error=Error>
    {
        let dml2 = dml.clone();
        Tree::<ddml::DRP, D, K, V>::write_root(&*inner)
            .and_then(move |mut guard| {
            let h = inner.height.load(Ordering::Relaxed) as u8;
            if h == node.height + 1 {
                // Clean the root node
                if guard.ptr.is_mem() {
                    // Another thread has already dirtied the root.  Nothing to
                    // do!
                    let fut = Box::new(future::ok(()));
                    return fut as Box<Future<Item=(), Error=Error>>;
                }
                let fut = dml.pop::<Arc<Node<ddml::DRP, K, V>>,
                                     Arc<Node<ddml::DRP, K, V>>>(
                                        guard.ptr.as_addr(), txg)
                    .and_then(move |arc| {
                        dml2.put(*arc, Compression::None, txg)
                    }).map(move |addr| {
                        let new = TreePtr::Addr(addr);
                        guard.ptr = new;
                    });
                Box::new(fut) as Box<Future<Item=(), Error=Error>>
            } else {
                let fut = Tree::xlock_root(dml, guard, txg)
                     .and_then(move |(_root_guard, child_guard)| {
                         Tree::rewrite_node_r(dml2, child_guard, h - 1, node,
                                              txg)
                     });
                Box::new(fut) as Box<Future<Item=(), Error=Error>>
            }
        })
    }

    fn rewrite_node_r(dml: Arc<D>, mut guard: TreeWriteGuard<ddml::DRP, K, V>,
                      height: u8, node: NodeId<K>, txg: TxgT)
        -> Box<Future<Item=(), Error=Error>>
    {
        debug_assert!(height > 0);
        let child_idx = guard.as_int().position(&node.key);
        if height == node.height + 1 {
            if guard.as_int().children[child_idx].ptr.is_mem() {
                // Another thread has already dirtied this node.  Nothing to do!
                return Box::new(future::ok(()));
            }
            // TODO: bypass the cache for this part
            // Need a solution for this issue first:
            // https://github.com/pcsm/simulacrum/issues/55
            let dml2 = dml.clone();
            let fut = dml.pop::<Arc<Node<ddml::DRP, K, V>>,
                                Arc<Node<ddml::DRP, K, V>>>(
                        guard.as_int().children[child_idx].ptr.as_addr(), txg)
                .and_then(move |arc| {
                    #[cfg(debug_assertions)]
                    {
                        if let Ok(guard) = arc.0.try_read() {
                            assert!(node.key <= *guard.key());
                        }
                    }
                    dml2.put(*arc, Compression::None, txg)
                }).map(move |addr| {
                    let new = TreePtr::Addr(addr);
                    guard.as_int_mut().children[child_idx].ptr = new;
                });
            Box::new(fut)
        } else {
            let fut = guard.xlock(dml.clone(), child_idx, txg)
                .and_then(move |(parent_guard, child_guard)| {
                    drop(parent_guard);
                    Tree::rewrite_node_r(dml, child_guard, height - 1, node, txg)
                });
            Box::new(fut)
        }
    }
}

/// The serialized, on-disk representation of a `Tree`
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TreeOnDisk(Vec<u8>);

#[cfg(test)] mod tests;

// LCOV_EXCL_START
#[cfg(test)]
mod t {
    use super::*;

    // pet kcov
    #[test]
    fn debug() {
        let tod = TreeOnDisk(vec![]);
        format!("{:?}", tod);
    }
}
// LCOV_EXCL_STOP
