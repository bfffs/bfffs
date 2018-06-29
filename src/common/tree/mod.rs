// vim: tw=80

//! COW B+-Trees, based on B-trees, Shadowing, and Clones[^CowBtrees]
//!
//! [^CowBtrees]: Rodeh, Ohad. "B-trees, shadowing, and clones." ACM Transactions on Storage (TOS) 3.4 (2008): 2.

use common::*;
use common::dml::*;
#[cfg(test)]
use common::ddml::DRP;
use futures::{
    Async,
    Future,
    future::{self, IntoFuture},
    Poll,
    stream::{self, Stream}
};
use futures_locks::*;
use nix::{Error, errno};
use serde::{Serializer, de::{Deserializer, DeserializeOwned}};
#[cfg(test)] use serde_yaml;
#[cfg(test)] use std::fmt::{self, Display, Formatter};
use std::{
    borrow::Borrow,
    cell::RefCell,
    collections::VecDeque,
    fmt::Debug,
    mem,
    rc::Rc,
    ops::{self, Bound, DerefMut, RangeBounds},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering}
    }
};
mod node;
use self::node::*;
// Node must be visible for the IDML's unit tests
pub(super) use self::node::Node;

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
    use serde::{Deserialize, Serialize, ser::Error};

    pub(super) fn deserialize<'de, A, DE, K, V>(d: DE)
        -> Result<RwLock<IntElem<A, K, V>>, DE::Error>
        where A: Addr, DE: Deserializer<'de>, K: Key, V: Value
    {
        IntElem::deserialize(d)
            .map(|int_elem| RwLock::new(int_elem))
    }

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

pub struct Range<'tree, A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A> + 'tree,
          K: Key + Borrow<T>,
          T: Ord + Clone + 'tree,
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
    tree: &'tree Tree<A, D, K, V>
}

impl<'tree, A, D, K, T, V> Range<'tree, A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A>,
          K: Key + Borrow<T>,
          T: Ord + Clone,
          V: Value
    {

    fn new<R>(range: R, tree: &'tree Tree<A, D, K, V>)
        -> Range<'tree, A, D, K, T, V>
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

impl<'tree, A, D, K, T, V> Stream for Range<'tree, A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A>,
          K: Key + Borrow<T>,
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

struct CleanZonePass1Inner<'tree, D, K, V>
    where D: DML<Addr=ddml::DRP> + 'tree,
          K: Key,
          V: Value
{
    /// If Some, then there are more nodes in the Tree to query
    cursor: Option<K>,

    /// Data that can be returned immediately
    data: VecDeque<NodeId<K>>,

    /// Used when an operation must block
    last_fut: Option<Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>),
                       Error=Error> + 'tree>>,

    /// Range of addresses to move
    range: ops::Range<PBA>,

    /// Handle to the tree
    tree: &'tree Tree<ddml::DRP, D, K, V>
}

/// Result type of `Tree::clean_zone`
struct CleanZonePass1<'tree, D, K, V>
    where D: DML<Addr=ddml::DRP> + 'tree,
          K: Key,
          V: Value
{
    inner: RefCell<CleanZonePass1Inner<'tree, D, K, V>>
}

impl<'tree, D, K, V> CleanZonePass1<'tree, D, K, V>
    where D: DML<Addr=ddml::DRP>,
          K: Key,
          V: Value
    {

    fn new(range: ops::Range<PBA>, tree: &'tree Tree<ddml::DRP, D, K, V>)
        -> CleanZonePass1<'tree, D, K, V>
    {
        let cursor = Some(K::min_value());
        let data = VecDeque::new();
        let last_fut = None;
        let inner = CleanZonePass1Inner{cursor, data, last_fut, range, tree};
        CleanZonePass1{inner: RefCell::new(inner)}
    }
}

impl<'tree, D, K, V> Stream for CleanZonePass1<'tree, D, K, V>
    where D: DML<Addr=ddml::DRP>,
          K: Key,
          V: Value
{
    type Item = NodeId<K>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let first = {
            let mut i = self.inner.borrow_mut();
            i.data.pop_front()
        };
        first.map(|x| Ok(Async::Ready(Some(x))))
            .unwrap_or_else(|| {
                let i = self.inner.borrow();
                if i.cursor.is_some() {
                    drop(i);
                    let mut f = stream::poll_fn(|| -> Poll<Option<()>, Error> {
                        let mut i = self.inner.borrow_mut();
                        let mut f = i.last_fut.take().unwrap_or_else(|| {
                            let l = i.cursor.clone().unwrap();
                            let range = i.range.clone();
                            Box::new(i.tree.get_dirty_nodes(l, range))
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
#[derive(Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
struct Inner<A: Addr, K: Key, V: Value> {
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
    root: RwLock<IntElem<A, K, V>>
}

/// In-memory representation of a COW B+-Tree
///
/// # Generic Parameters
///
/// *`K`:   Key type.  Must be ordered and copyable; should be compact
/// *`V`:   Value type in the leaves.
pub struct Tree<A: Addr, D: DML<Addr=A>, K: Key, V: Value> {
    dml: Arc<D>,
    i: Inner<A, K, V>
}

impl<'a, A: Addr, D: DML<Addr=A>, K: Key, V: Value> Tree<A, D, K, V> {

    pub fn create(dml: Arc<D>) -> Self {
        Tree::new(dml,
                  4,        // BetrFS's min fanout
                  16,       // BetrFS's max fanout
                  1<<22,    // BetrFS's max size
        )
    }

    /// Fix an Int node in danger of being underfull, returning the parent guard
    /// back to the caller
    fn fix_int<Q>(&'a self, parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize, mut child: TreeWriteGuard<A, K, V>)
        -> impl Future<Item=(TreeWriteGuard<A, K, V>, i8, i8), Error=Error> + 'a
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
                (self.xlock(parent, sib_idx), sib_idx, true)
            } else {
                let sib_idx = child_idx - 1;
                (self.xlock(parent, sib_idx), sib_idx, false)
            }
        };
        fut.map(move |(mut parent, mut sibling)| {
            let (before, after) = if right {
                if child.can_merge(&sibling, self.i.max_fanout) {
                    child.merge(sibling);
                    parent.as_int_mut().children.remove(sib_idx);
                    (0, 1)
                } else {
                    child.take_low_keys(&mut sibling);
                    parent.as_int_mut().children[sib_idx].key = *sibling.key();
                    (0, 0)
                }
            } else {
                if sibling.can_merge(&child, self.i.max_fanout) {
                    sibling.merge(child);
                    parent.as_int_mut().children.remove(child_idx);
                    (1, 0)
                } else {
                    child.take_high_keys(&mut sibling);
                    parent.as_int_mut().children[child_idx].key = *child.key();
                    (1, 1)
                }
            };
            (parent, before, after)
        })
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Subroutine of range_delete.  Fixes a node that is in danger of an
    /// underflow.  Returns the node guard, and two ints which are the number of
    /// nodes that were merged before and after the fixed child, respectively.
    fn fix_if_in_danger<R, T>(&'a self, parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize, child: TreeWriteGuard<A, K, V>, range: R,
                  ubound: Option<K>)
        -> Box<Future<Item=(TreeWriteGuard<A, K, V>, i8, i8), Error=Error> + 'a>
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        // Is the child underfull?  The limit here is b-1, not b as in
        // Tree::remove, because we've already removed keys
        if child.underflow(self.i.min_fanout - 1) {
            Box::new(self.fix_int(parent, child_idx, child))
        } else if !child.is_leaf() {
            // How many grandchildren are in the cut?
            let (start_idx_bound, end_idx_bound) = self.range_delete_get_bounds(
                &child, &range, ubound);
            let cut_grandkids = match (start_idx_bound, end_idx_bound) {
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
            let b = self.i.min_fanout;
            if child.as_int().nchildren() - cut_grandkids <= b - 1 {
                Box::new(self.fix_int(parent, child_idx, child))
            } else {
                Box::new(Ok((parent, 0, 0)).into_future())
            }
        } else {
            Box::new(Ok((parent, 0, 0)).into_future())
        }
    }

    #[cfg(test)]
    pub fn from_str(dml: Arc<D>, s: &str) -> Self {
        let i: Inner<A, K, V> = serde_yaml::from_str(s).unwrap();
        Tree{dml, i}
    }

    /// Insert value `v` into the tree at key `k`, returning the previous value
    /// for that key, if any.
    pub fn insert(&'a self, k: K, v: V)
        -> impl Future<Item=Option<V>, Error=Error> + 'a {

        self.write()
            .and_then(move |guard| {
                self.xlock_root(guard)
                     .and_then(move |(_root_guard, child_guard)| {
                         self.insert_locked(child_guard, k, v)
                     })
            })
    }

    /// Insert value `v` into an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn insert_int(&'a self, mut parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize,
                  mut child: TreeWriteGuard<A, K, V>, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        // First, split the node, if necessary
        if (*child).should_split(self.i.max_fanout) {
            let (new_key, new_node_data) = child.split();
            let new_node = Node::new(new_node_data);
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
    fn insert_locked(&'a self, mut root: TreeWriteGuard<A, K, V>, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        // First, split the root node, if necessary
        if root.should_split(self.i.max_fanout) {
            let (new_key, new_node_data) = root.split();
            let new_node = Node::new(new_node_data);
            let new_ptr = TreePtr::Mem(Box::new(new_node));
            let new_elem = IntElem{key: new_key, ptr: new_ptr};
            let new_root_data = NodeData::Int(
                IntData {
                    children: vec![new_elem]
                }
            );
            let old_root_data = mem::replace(root.deref_mut(), new_root_data);
            let old_root_node = Node::new(old_root_data);
            let old_ptr = TreePtr::Mem(Box::new(old_root_node));
            let old_elem = IntElem{ key: K::min_value(), ptr: old_ptr };
            root.as_int_mut().children.insert(0, old_elem);
            self.i.height.fetch_add(1, Ordering::Relaxed);
        }

        self.insert_no_split(root, k, v)
    }

    fn insert_no_split(&'a self, mut node: TreeWriteGuard<A, K, V>, k: K, v: V)
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
    pub fn get(&'a self, k: K) -> impl Future<Item=Option<V>, Error=Error> + 'a
    {
        self.read()
            .and_then(move |guard| {
                self.rlock(&guard)
                     .and_then(move |guard| self.get_r(guard, k))
            })
    }

    /// Lookup the value of key `k` in a node, which must already be locked.
    fn get_r(&'a self, node: TreeReadGuard<A, K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
    {

        let next_node_fut = match *node {
            NodeData::Leaf(ref leaf) => {
                return Box::new(Ok(leaf.get(&k)).into_future())
            },
            NodeData::Int(ref int) => {
                let child_elem = &int.children[int.position(&k)];
                self.rlock(&child_elem)
            }
        };
        drop(node);
        Box::new(
            next_node_fut
            .and_then(move |next_node| self.get_r(next_node, k))
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
        self.read()
            .and_then(move |guard| {
                self.rlock(&guard)
                     .and_then(move |g| self.get_range_r(g, None, range))
            })
    }

    /// Range lookup beginning in the node `guard`.  `next_guard`, if present,
    /// must be the node immediately to the right (and possibly up one or more
    /// levels) from `guard`.
    fn get_range_r<R, T>(&'a self, guard: TreeReadGuard<A, K, V>,
                            next_guard: Option<TreeReadGuard<A, K, V>>, range: R)
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
                    self.get_range_r(next_guard.unwrap(), None, range)
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
                    Box::new(
                        self.rlock(&int.children[child_idx + 1])
                            .map(|guard| Some(guard))
                    ) as Box<Future<Item=Option<TreeReadGuard<A, K, V>>,
                                    Error=Error>>
                } else {
                    Box::new(Ok(next_guard).into_future())
                        as Box<Future<Item=Option<TreeReadGuard<A, K, V>>,
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
                self.get_range_r(child_guard, next_guard, range)
            })
        ) as Box<Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>), Error=Error>>
    }

    /// Merge the root node with its children, if necessary
    fn merge_root(&self, root_guard: &mut TreeWriteGuard<A, K, V>) {
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
            self.i.height.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Lookup a range of (key, value) pairs for keys within the range `range`.
    pub fn range<R, T>(&'a self, range: R) -> Range<'a, A, D, K, T, V>
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
        // 3) Collapse the root node, if it has 1 child
        let rangeclone = range.clone();
        self.write()
            .and_then(move |guard| {
                self.xlock_root(guard)
                     .and_then(move |(tree_guard, root_guard)| {
                         self.range_delete_pass1(root_guard, range, None)
                             .map(|_| tree_guard)
                     })
            })
            .and_then(move |guard| {
                self.xlock_root(guard)
                    .and_then(move |(tree_guard, root_guard)| {
                        self.range_delete_pass2(root_guard, rangeclone, None)
                            .map(|_| tree_guard)
                    })
            })
            // Finally, collapse the root node, if it has 1 child.  During
            // normal remove operations we can't do this, because we drop the
            // lock on the root node before fixing all of its children.  But the
            // entire tree stays locked during range_delete, so it's possible to
            // fix the root node at the end
            .and_then(move |tree_guard| {
                self.xlock_root(tree_guard)
                    .map(move |(tree_guard, mut root_guard)| {
                        self.merge_root(&mut root_guard);
                        // Keep the whole tree locked during range_delete
                        drop(tree_guard)
                    })  // LCOV_EXCL_LINE   kcov false negative
            })
    }

    /// Subroutine of range_delete.  Returns the bounds, as indices, of the
    /// affected children of this node.
    fn range_delete_get_bounds<R, T>(&self, guard: &TreeWriteGuard<A, K, V>,
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
    fn range_delete_pass1<R, T>(&'a self, mut guard: TreeWriteGuard<A, K, V>,
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
        let l = guard.as_int().nchildren();
        let (start_idx_bound, end_idx_bound) = self.range_delete_get_bounds(
            &guard, &range, ubound);
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
                    Some(guard.as_int().children[j + 1].key)
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
    fn range_delete_pass2<R, T>(&'a self, guard: TreeWriteGuard<A, K, V>,
                                range: R, ubound: Option<K>)
        -> Box<Future<Item=(), Error=Error> + 'a>
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

        let (start_idx_bound, end_idx_bound) = self.range_delete_get_bounds(
            &guard, &range, ubound);
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
            Box::new(
                self.xlock(parent_guard, idx)
                .and_then(move |(parent_guard, child_guard)| {
                    self.fix_if_in_danger(parent_guard, idx, child_guard,
                                          range, child_ubound)
                }).and_then(move |(parent_guard, merged_before, merged_after)| {
                    let merged = merged_before + merged_after;
                    self.xlock(parent_guard, idx - merged_before as usize)
                        .map(move |(parent, child)| (parent, child, merged))
                }).and_then(move |(parent_guard, child_guard, merged)| {
                    self.range_delete_pass2(child_guard, range2, child_ubound)
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

    fn new(dml: Arc<D>, min_fanout: usize, max_fanout: usize,
           max_size: usize) -> Self
    {
        let i: Inner<A, K, V> = Inner {
            height: AtomicUsize::new(1),
            min_fanout, max_fanout,
            _max_size: max_size,
            root: RwLock::new(
                IntElem{
                    key: K::min_value(),
                    ptr:
                        TreePtr::Mem(
                            Box::new(
                                Node::new(
                                    NodeData::Leaf(
                                        LeafData::new()
                                    )
                                )
                            )
                        )
                }
            )
        };
        Tree{ dml, i }
    }

    /// Remove and return the value at key `k`, if any.
    pub fn remove(&'a self, k: K)
        -> impl Future<Item=Option<V>, Error=Error> + 'a
    {
        self.write()
            .and_then(move |guard| {
                self.xlock_root(guard)
                    .and_then(move |(_root_guard, child_guard)| {
                        self.remove_locked(child_guard, k)
                    })
        })
    }

    /// Remove key `k` from an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn remove_int(&'a self, parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize, child: TreeWriteGuard<A, K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
    {
        // First, fix the node, if necessary
        if child.underflow(self.i.min_fanout) {
            Box::new(
                self.fix_int(parent, child_idx, child)
                    .and_then(move |(parent, _, _)| {
                        let child_idx = parent.as_int().position(&k);
                        self.xlock(parent, child_idx)
                    }).and_then(move |(parent, child)| {
                        drop(parent);
                        self.remove_no_fix(child, k)
                    })
            )
        } else {
            drop(parent);
            self.remove_no_fix(child, k)
        }
    }

    /// Helper for `remove`.  Handles removal once the tree is locked
    fn remove_locked(&'a self, mut root: TreeWriteGuard<A, K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
    {
        self.merge_root(&mut root);
        self.remove_no_fix(root, k)
    }

    /// Remove key `k` from a node, but don't try to fixup the node.
    fn remove_no_fix(&'a self, mut node: TreeWriteGuard<A, K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
    {

        if node.is_leaf() {
            let old_v = node.as_leaf_mut().remove(&k);
            return Box::new(Ok(old_v).into_future());
        } else {
            let child_idx = node.as_int().position(&k);
            let fut = self.xlock(node, child_idx);
            Box::new(fut.and_then(move |(parent, child)| {
                    self.remove_int(parent, child_idx, child, k)
                })
            )
        }
    }

    /// Flush all in-memory Nodes to disk.
    pub fn flush(&'a self) -> impl Future<Item=(), Error=Error> + 'a {
        self.write()
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
                        self.flush_r(ptr.into_node())
                            .map(move |addr| {
                                root_guard.ptr = TreePtr::Addr(addr);
                            })
                    )
                });
                Box::new(fut) as Box<Future<Item=(), Error=Error>>
            } else {
                Box::new(future::ok::<(), Error>(()))
            }
        })
    }

    fn write_leaf(&'a self, node: Box<Node<A, K, V>>)
        -> impl Future<Item=A, Error=Error> + 'a
    {
        let arc: Arc<Node<A, K, V>> = Arc::new(*node);
        let (addr, fut) = self.dml.put(arc, Compression::None);
        fut.map(move |_| addr)
    }

    fn flush_r(&'a self, mut node: Box<Node<A, K, V>>)
        -> Box<Future<Item=D::Addr, Error=Error> + 'a>
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
        let nchildren = RefCell::borrow(&Rc::borrow(&rndata)).as_int().nchildren();
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
                let fut = rndata.borrow_mut()
                                .as_int_mut()
                                .children[idx]
                                .ptr
                                .as_mem()
                                .xlock()
                                .and_then(move |guard|
                {
                    drop(guard);

                    let ptr = mem::replace(&mut rndata3.borrow_mut()
                                                       .as_int_mut()
                                                       .children[idx].ptr,
                                           TreePtr::None);
                    self.flush_r(ptr.into_node())
                        .map(move |addr| {
                            rndata3.borrow_mut()
                                   .as_int_mut()
                                   .children[idx].ptr = TreePtr::Addr(addr);
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
                let arc: Arc<Node<A, K, V>> = Arc::new(*node);
                let (addr, fut) = self.dml.put(arc, Compression::None);
                fut.map(move |_| addr)
            })
        )
    }

    /// Lock the Tree for reading
    fn read(&'a self) -> impl Future<Item=RwLockReadGuard<IntElem<A, K, V>>,
                                     Error=Error> + 'a
    {
        self.i.root.read().map_err(|_| Error::Sys(errno::Errno::EPIPE))
    }

    /// Lock the provided `IntElem` nonexclusively
    fn rlock(&'a self, elem: &IntElem<A, K, V>)
        -> Box<Future<Item=TreeReadGuard<A, K, V>, Error=Error> + 'a>
    {
        match elem.ptr {
            TreePtr::Mem(ref node) => {
                Box::new(
                    node.0.read()
                        .map(|guard| TreeReadGuard::Mem(guard))
                        .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                )
            },
            TreePtr::Addr(ref addr) => {
                Box::new(
                    self.dml.get::<Arc<Node<A, K, V>>, Arc<Node<A, K, V>>>(addr)
                    .and_then(|node| {
                        node.0.read()
                            .map(move |guard| {
                                TreeReadGuard::Addr(guard, node)
                            })
                            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                    })
                )
            },
            // LCOV_EXCL_START
            TreePtr::None => unreachable!("None is just a temporary value")
            // LCOV_EXCL_STOP
        }
    }

    /// Lock the Tree for writing
    fn write(&'a self) -> impl Future<Item=RwLockWriteGuard<IntElem<A, K, V>>,
                                      Error=Error> + 'a
    {
        self.i.root.write().map_err(|_| Error::Sys(errno::Errno::EPIPE))
    }

    /// Lock the indicated `IntElem` exclusively.  If it is not already resident
    /// in memory, then COW the target node.
    fn xlock(&'a self, mut guard: TreeWriteGuard<A, K, V>, child_idx: usize)
        -> (Box<Future<Item=(TreeWriteGuard<A, K, V>,
                             TreeWriteGuard<A, K, V>), Error=Error> + 'a>)
    {
        if guard.as_int().children[child_idx].ptr.is_mem() {
            Box::new(
                guard.as_int().children[child_idx].ptr.as_mem().xlock()
                    .map(move |child_guard| {
                          (guard, child_guard)
                     })
            )
        } else {
            let addr = *guard.as_int()
                             .children[child_idx]
                             .ptr
                             .as_addr();
                Box::new(
                    self.dml.pop::<Arc<Node<A, K, V>>, Arc<Node<A, K, V>>>(
                        &addr).map(move |arc|
                    {
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

    /// Lock the root `IntElem` exclusively.  If it is not already resident in
    /// memory, then COW it.
    fn xlock_root(&'a self, mut guard: RwLockWriteGuard<IntElem<A, K, V>>)
        -> (Box<Future<Item=(RwLockWriteGuard<IntElem<A, K, V>>,
                             TreeWriteGuard<A, K, V>), Error=Error> + 'a>)
    {
        if guard.ptr.is_mem() {
            Box::new(
                guard.ptr.as_mem().0.write()
                     .map(move |child_guard| {
                          (guard, TreeWriteGuard::Mem(child_guard))
                     }).map_err(|_| Error::Sys(errno::Errno::EPIPE))
            )
        } else {
            let addr = *guard.ptr.as_addr();
            Box::new(
                self.dml.pop::<Arc<Node<A, K, V>>, Arc<Node<A, K, V>>>(
                    &addr).map(move |arc|
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
        f.write_str(&serde_yaml::to_string(&self.i).unwrap())
    }
}

// These methods are only for direct trees
impl<'a, D: DML<Addr=ddml::DRP>, K: Key, V: Value> Tree<ddml::DRP, D, K, V> {
    /// Clean `zone` by moving all of its records to other zones.
    pub fn clean_zone(&'a self, range: ops::Range<PBA>)
        -> impl Future<Item=(), Error=Error> + 'a
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
        // TODO: Store the TXG range of each zone and the TXG range of the
        // subtree represented by each Node.  Use that information to prune the
        // number of Nodes that must be walked.
        CleanZonePass1::new(range, self)
            .collect()
            .and_then(move |nodes| {
                stream::iter_ok(nodes.into_iter()).for_each(move |node| {
                    // TODO: consider attempting to rewrite multiple nodes at
                    // once, so as not to spend so much time traversing the tree
                    self.rewrite_node(node)
                })
            })
    }

    fn get_dirty_nodes(&'a self, key: K, range: ops::Range<PBA>)
        -> impl Future<Item=(VecDeque<NodeId<K>>, Option<K>), Error=Error> + 'a
    {
        self.read()
            .and_then(move |guard| {
                self.rlock(&guard)
                     .and_then(move |guard| {
                         let h = self.i.height.load(Ordering::Relaxed) as u8;
                         self.get_dirty_nodes_r(guard, h - 1, None, key, range)
                     })
            })
    }

    /// Find dirty nodes in `PBA` range `range`, beginning at `key`.
    /// `next_guard`, if present, must be the node immediately to the right (and
    /// possibly up one or more levels) from `guard`.  `height` is the tree
    /// height of `guard`, where leaves are 0.
    fn get_dirty_nodes_r(&'a self, guard: TreeReadGuard<ddml::DRP, K, V>,
                         height: u8,
                         next_guard: Option<TreeReadGuard<ddml::DRP, K, V>>,
                         key: K, range: ops::Range<PBA>)
        -> Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>), Error=Error> + 'a>
    {
        if height == 1 {
            let nodes = guard.as_int().children.iter().filter_map(|child| {
                if child.ptr.is_addr() &&
                    child.ptr.as_addr().pba() >= range.start &&
                    child.ptr.as_addr().pba() < range.end {
                    Some(NodeId{height: height - 1, key: child.key})
                } else {
                    None
                }
            }).collect::<VecDeque<_>>();
            let bound = next_guard.map(|g| g.key().clone());
            return Box::new(future::ok((nodes, bound)))
        }
        let idx = guard.as_int().position(&key);
        let next_fut = if idx < guard.as_int().nchildren() - 1 {
            Box::new(
                // TODO: store the next node's height, too
                self.rlock(&guard.as_int().children[idx + 1])
                    .map(|guard| Some(guard))
            ) as Box<Future<Item=Option<TreeReadGuard<ddml::DRP, K, V>>,
                            Error=Error>>
        } else {
            Box::new(Ok(next_guard).into_future())
                as Box<Future<Item=Option<TreeReadGuard<ddml::DRP, K, V>>,
                              Error=Error>>
        };
        // TODO: consider searching the tree multiple times, once for each
        // level, to eliminate this "dirty_self" hack.
        let dirty_self = if guard.as_int().children[idx].ptr.is_addr() &&
           guard.as_int().children[idx].ptr.as_addr().pba() >= range.start &&
           guard.as_int().children[idx].ptr.as_addr().pba() < range.end {
            Some(NodeId {
                height: height - 1,
                key: guard.as_int().children[idx].key
            })
        } else {
            None
        };
        let nchildren = guard.as_int().nchildren();
        let next_sibling_key = if idx == nchildren - 1 {
            None
        } else {
            Some(guard.as_int().children[idx + 1].key)
        };
        let child_fut = self.rlock(&guard.as_int().children[idx]);
        drop(guard);
        Box::new(
            child_fut.join(next_fut)
                .and_then(move |(child_guard, next_guard)| {
                self.get_dirty_nodes_r(child_guard, height - 1, next_guard,
                                       key, range)
                    .map(move |(mut nodes, bound)| {
                        // If this traversal was the last one through this node,
                        // then optionally add this node to the list
                        if (bound.is_none() ||
                              (next_sibling_key.is_some() &&
                               bound.unwrap() >= next_sibling_key.unwrap())) &&
                            dirty_self.is_some() {
                            nodes.push_back(dirty_self.unwrap());
                        }
                        (nodes, bound)
                    })
            })
        ) as Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>), Error=Error>>
    }

    /// Rewrite `node`, without modifying its contents
    fn rewrite_node(&'a self, node: NodeId<K>)
        -> impl Future<Item=(), Error=Error> + 'a
    {
        self.write()
            .and_then(move |guard| {
                self.xlock_root(guard)
                     .and_then(move |(_root_guard, child_guard)| {
                         let h = self.i.height.load(Ordering::Relaxed) as u8;
                         self.rewrite_node_r(child_guard, h - 1, node)
                     })
            })
    }

    fn rewrite_node_r(&'a self, mut guard: TreeWriteGuard<ddml::DRP, K, V>,
                      height: u8, node: NodeId<K>)
        -> Box<Future<Item=(), Error=Error> + 'a>
    {
        debug_assert!(height > 0);
        let child_idx = guard.as_int().position(&node.key);
        if height == node.height + 1 {
            if guard.as_int().children[child_idx].ptr.is_mem() {
                // Another thread has already dirtied this node.  Nothing to do!
                return Box::new(future::ok(()));
            }
            // No need to xlock since the target is not dirty and we hold the
            // parent's lock.  It's sufficient to rlock.
            // TODO: bypass the cache for this part
            let fut = self.dml.pop::<Arc<Node<ddml::DRP, K, V>>,
                                     Arc<Node<ddml::DRP, K, V>>>(
                guard.as_int().children[child_idx].ptr.as_addr())
                .and_then(move |arc| {
                    #[cfg(debug_assertions)]
                    {
                        if let Ok(guard) = arc.0.try_read() {
                            assert!(node.key <= *guard.key());
                        }
                    }
                    let (addr, fut) = self.dml.put(*arc, Compression::None);
                    let new = TreePtr::Addr(addr);
                    guard.as_int_mut().children[child_idx].ptr = new;
                    fut
                });
            Box::new(fut)
        } else {
            let fut = self.xlock(guard, child_idx)
                .and_then(move |(parent_guard, child_guard)| {
                    drop(parent_guard);
                    self.rewrite_node_r(child_guard, height - 1, node)
                });
            Box::new(fut)
        }
    }
}

// LCOV_EXCL_START
/// Tests regarding in-memory manipulation of Trees
#[cfg(test)]
mod in_mem {

use super::*;
use common::ddml_mock::*;
use futures::future;
use tokio::runtime::current_thread;

#[test]
fn insert() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.insert(0, 0.0));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.insert(0, 100.0));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.insert(24, 24.0));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.insert(8, 8.0));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.insert(15, 15.0));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.insert(5, 5.0));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(future::lazy(|| {
        tree.insert(0, 0.0)
            .and_then(|_| tree.get(0))
    }));
    assert_eq!(r, Ok(Some(0.0)));
}

#[test]
fn get_deep() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.get(3));
    assert_eq!(r, Ok(Some(3.0)))
}

#[test]
fn get_nonexistent() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.get(0));
    assert_eq!(r, Ok(None))
}

// The range delete example from Figures 13-14 of B-Trees, Shadowing, and
// Range-operations.  Our result is slightly different than in the paper,
// however, because in the second pass:
// a) We fix nodes [10, -] and [-, 32] by merging them with their outer
//    neighbors rather than themselves (this is legal).
// b) When removing key 31 from [31, 32], we don't adjust the key in its parent
//    IntElem.  This is legal because it still obeys the minimum-key-rule.  It's
//    not generally possible to fix a parent's IntElem's key when removing a key
//    from a child, because it may require holding the locks on more than 2
//    nodes.
#[test]
fn range_delete() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
          - key: 30
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
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
                    10: 10.0
          - key: 31
            ptr:
              Mem:
                Leaf:
                  items:
                    32: 32.0
                    37: 37.0
                    40: 40.0"#);
}

// After removing keys, one node is in danger because it has too many children
// in the cut
#[test]
fn range_delete_danger() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
                              8: 8.0
                              9: 9.0
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
          - key: 30
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(5..6)
    );
    assert!(r.is_ok());
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
                              6: 6.0
                              7: 7.0
                              8: 8.0
                              9: 9.0
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
          - key: 30
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
                              40: 40.0"#);
}

// Delete a range that's exclusive on the left and right
#[test]
fn range_delete_exc_exc() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
          - key: 1
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 4
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0
                    7: 7.0
                    8: 8.0
          - key: 10
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    11: 11.0
                    12: 12.0
                    13: 13.0
          - key: 20
            ptr:
              Mem:
                Leaf:
                  items:
                    20: 20.0
                    21: 21.0
                    22: 22.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete((Bound::Excluded(4), Bound::Excluded(10)))
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
          - key: 4
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    10: 10.0
                    11: 11.0
                    12: 12.0
                    13: 13.0
          - key: 20
            ptr:
              Mem:
                Leaf:
                  items:
                    20: 20.0
                    21: 21.0
                    22: 22.0"#);
}

// Delete a range that's exclusive on the left and inclusive on the right
#[test]
fn range_delete_exc_inc() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
          - key: 1
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 4
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0
                    7: 7.0
                    8: 8.0
          - key: 10
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    11: 11.0
                    12: 12.0
                    13: 13.0
          - key: 20
            ptr:
              Mem:
                Leaf:
                  items:
                    20: 20.0
                    21: 21.0
                    22: 22.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete((Bound::Excluded(4), Bound::Included(10)))
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
          - key: 4
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    11: 11.0
                    12: 12.0
                    13: 13.0
          - key: 20
            ptr:
              Mem:
                Leaf:
                  items:
                    20: 20.0
                    21: 21.0
                    22: 22.0"#);
}

// Delete a range that's contained within a single LeafNode
#[test]
fn range_delete_single_node() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
                    - key: 4
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4: 4.0
                              5: 5.0
                              6: 6.0
                              7: 7.0
                              8: 8.0
                    - key: 10
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              10: 10.0
                              11: 11.0
                              12: 12.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(5..7)
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
          - key: 4
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    7: 7.0
                    8: 8.0
          - key: 10
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    11: 11.0
                    12: 12.0"#);
}

// Delete a range that includes a whole Node at the end of the Tree
#[test]
fn range_delete_to_end() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
          - key: 1
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
          - key: 4
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
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
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(7..20)
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
          - key: 4
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0"#);
}

// Delete a range that passes the right side of an int node, with another
// int node to its right
#[test]
fn range_delete_to_end_of_int_node() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
                    - key: 4
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4: 4.0
                              5: 5.0
                              6: 6.0
                    - key: 7
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7: 7.0
                              8: 8.0
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
                    - key: 20
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              21: 21.0
                              22: 22.0
                    - key: 24
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              26: 26.0
          - key: 30
            ptr:
              Mem:
                Int:
                  children:
                    - key: 30
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              30: 30.0
                              31: 31.0
                    - key: 40
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              40: 40.0
                              41: 41.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(10..16)
    );
    assert!(r.is_ok());
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
                    - key: 4
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              4: 4.0
                              5: 5.0
                              6: 6.0
                    - key: 7
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              7: 7.0
                              8: 8.0
          - key: 15
            ptr:
              Mem:
                Int:
                  children:
                    - key: 20
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              20: 20.0
                              21: 21.0
                              22: 22.0
                    - key: 24
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              24: 24.0
                              26: 26.0
                    - key: 30
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              30: 30.0
                              31: 31.0
                    - key: 40
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              40: 40.0
                              41: 41.0"#);
}

// Delete a range that includes only whole nodes
#[test]
fn range_delete_whole_nodes() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
          - key: 1
            ptr:
              Mem:
                Leaf:
                  items:
                    1: 1.0
                    2: 2.0
                    3: 3.0
          - key: 4
            ptr:
              Mem:
                Leaf:
                  items:
                    4: 4.0
                    5: 5.0
                    6: 6.0
          - key: 10
            ptr:
              Mem:
                Leaf:
                  items:
                    10: 10.0
                    11: 11.0
          - key: 20
            ptr:
              Mem:
                Leaf:
                  items:
                    20: 20.0
                    21: 21.0
                    22: 22.0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range_delete(4..20)
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
                    3: 3.0
          - key: 20
            ptr:
              Mem:
                Leaf:
                  items:
                    20: 20.0
                    21: 21.0
                    22: 22.0"#);
}


// Unbounded range lookup
#[test]
fn range_full() {
    let ddml = Arc::new(DDMLMock::new());
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(..).collect()
    );
    assert_eq!(r, Ok(vec![(0, 0.0), (1, 1.0), (3, 3.0), (4, 4.0)]));
}

#[test]
fn range_exclusive_start() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range((Bound::Excluded(0), Bound::Excluded(4)))
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (3, 3.0)]));

    // A query that starts between leaves
    let r = rt.block_on(
        tree.range((Bound::Excluded(1), Bound::Excluded(4)))
            .collect()
    );
    assert_eq!(r, Ok(vec![(3, 3.0)]));
}

#[test]
fn range_leaf() {
    let ddml = Arc::new(DDMLMock::new());
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(1..3)
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (2, 2.0)]));
}

#[test]
fn range_leaf_inclusive_end() {
    let ddml = Arc::new(DDMLMock::new());
    let tree = Tree::<DRP, DDMLMock, u32, f32>::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(3..=4)
            .collect()
    );
    assert_eq!(r, Ok(vec![(3, 3.0), (4, 4.0)]));
}

#[test]
fn range_nonexistent_between_two_leaves() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(2..4)
            .collect()
    );
    assert_eq!(r, Ok(vec![]));
}

#[test]
fn range_two_ints() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(1..10)
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (9, 9.0)]));
}

#[test]
fn range_starts_between_two_leaves() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(2..4)
            .collect()
    );
    assert_eq!(r, Ok(vec![(3, 3.0)]));
}

#[test]
fn range_two_leaves() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
        tree.range(1..4)
            .collect()
    );
    assert_eq!(r, Ok(vec![(1, 1.0), (3, 3.0)]));
}

#[test]
fn remove_last_key() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.remove(0));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.remove(1));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(1));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(23));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(4));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(7));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(4));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(26));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(14));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(8));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
    let mut rt = current_thread::Runtime::new().unwrap();
    let r2 = rt.block_on(tree.remove(4));
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
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::new(ddml, 2, 5, 1<<22);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.remove(3));
    assert_eq!(r, Ok(None));
}

}

#[cfg(test)]
/// Tests regarding disk I/O for Trees
mod io {

use super::*;
use common::ddml_mock::*;
use futures::future;
use simulacrum::*;
use tokio::prelude::task::current;
use tokio::runtime::current_thread;

#[test]
fn clean_zone() {
    // On-disk internal node with on-disk children outside the target zone
    let drpl0 = DRP::new(PBA{cluster: 0, lba: 0}, Compression::None, 0, 0, 0);
    let drpl1 = DRP::new(PBA{cluster: 0, lba: 1}, Compression::None, 0, 0, 0);
    let children0 = vec![
        IntElem{key: 0u32, ptr: TreePtr::Addr(drpl0)},
        IntElem{key: 2u32, ptr: TreePtr::Addr(drpl1)},
    ];
    let in0 = Arc::new(Node::new(NodeData::Int(IntData{children: children0}))
    );
    let drpi0 = DRP::new(PBA{cluster: 0, lba: 2}, Compression::None, 0, 0, 0);

    // On-disk internal node with children both in and outside of target zone
    let drpl2 = DRP::new(PBA{cluster: 0, lba: 3}, Compression::None, 0, 0, 0);
    let drpl3 = DRP::new(PBA{cluster: 0, lba: 100}, Compression::None, 0, 0, 0);
    // We must make two copies of in1, one for DDMLMock::get and one for ::pop
    let children1 = vec![
        IntElem{key: 4u32, ptr: TreePtr::Addr(drpl2)},
        IntElem{key: 6u32, ptr: TreePtr::Addr(drpl3)},
    ];
    let children1_c = vec![
        IntElem{key: 4u32, ptr: TreePtr::Addr(drpl2)},
        IntElem{key: 6u32, ptr: TreePtr::Addr(drpl3)},
    ];
    let in1 = Arc::new(Node::new(NodeData::Int(IntData{children: children1}))
    );
    let mut in1_c = Some(Arc::new(Node::new(
                NodeData::Int(IntData{children: children1_c}))
    ));
    let drpi1 = DRP::new(PBA{cluster: 0, lba: 4}, Compression::None, 0, 0, 0);

    let mut ld3 = LeafData::new();
    ld3.insert(6, 6.0);
    ld3.insert(7, 7.0);
    let mut ln3 = Some(Arc::new(Node::new(NodeData::Leaf(ld3))));

    // On-disk internal node in the target zone, but with children outside
    let drpl4 = DRP::new(PBA{cluster: 0, lba: 5}, Compression::None, 0, 0, 0);
    let drpl5 = DRP::new(PBA{cluster: 0, lba: 6}, Compression::None, 0, 0, 0);
    // We must make two copies of in2, one for DDMLMock::get and one for ::pop
    let children2 = vec![
        IntElem{key: 8u32, ptr: TreePtr::Addr(drpl4)},
        IntElem{key: 10u32, ptr: TreePtr::Addr(drpl5)},
    ];
    let children2_c = vec![
        IntElem{key: 8u32, ptr: TreePtr::Addr(drpl4)},
        IntElem{key: 10u32, ptr: TreePtr::Addr(drpl5)},
    ];
    let in2 = Arc::new(Node::new(NodeData::Int(IntData{children: children2})));
    let mut in2_c = Some(Arc::new(Node::new(
                NodeData::Int(IntData{children: children2_c}))
    ));
    let drpi2 = DRP::new(PBA{cluster: 0, lba: 101}, Compression::None, 0, 0, 0);

    // On-disk leaf node in the target zone
    let drpl8 = DRP::new(PBA{cluster: 0, lba: 102}, Compression::None, 0, 0, 0);
    let mut ld8 = LeafData::new();
    ld8.insert(16, 16.0);
    ld8.insert(17, 17.0);
    let mut ln8 = Some(Arc::new(Node::new(NodeData::Leaf(ld8))));

    let mut mock = DDMLMock::new();
    mock.expect_get::<Arc<Node<DRP, u32, f32>>>()
        .called_any()
        .with(passes(move |arg: & *const DRP| {
            unsafe {
            **arg == drpi0 || **arg == drpi1 || **arg == drpi2
        }} ))
        .returning(move |arg: *const DRP| {
            let res = Box::new( unsafe {
                if *arg == drpi0 {
                    in0.clone()
                } else if *arg == drpi1 {
                    in1.clone()
                } else if *arg == drpi2 {
                    in2.clone()
                } else {
                    panic!("unexpected DDMLMock::get {:?}", *arg);
                }
            });
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, f32>>>, Error>(res))
        });
    mock.expect_pop::<Arc<Node<DRP, u32, f32>>, Arc<Node<DRP, u32, f32>>>()
        .called_times(4)
        .with(passes(move |arg: & *const DRP| unsafe {
            **arg == drpl3 || **arg == drpi2 || **arg == drpl8 || **arg == drpi1
        } ))
        .returning(move |arg: *const DRP| {
            //println!("DDMLMock::pop({:?}", unsafe{*arg});
            let res = Box::new( unsafe {
                if *arg == drpl3 {
                    ln3.take().unwrap()
                } else if *arg == drpi1 {
                    in1_c.take().unwrap()
                } else if *arg == drpi2 {
                    in2_c.take().unwrap()
                } else if *arg == drpl8 {
                    ln8.take().unwrap()
                } else {
                    panic!("unexpected DDMLMock::pop {:?}", *arg);
                }
            });
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, f32>>>, Error>(res))
        });
    mock.expect_put::<Arc<Node<DRP, u32, f32>>>()
        .called_times(3)
        .returning(move |(_cacheable, _compression)| {
            let drp = DRP::random(Compression::None, 1024);
            (drp, Box::new(future::ok::<(), Error>(())))
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
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
              Addr:
                pba:
                  cluster: 0
                  lba: 2
                compression: None
                lsize: 0
                csize: 0
                checksum: 0
          - key: 4
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 4
                compression: None
                lsize: 0
                csize: 0
                checksum: 0
          - key: 8
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 101
                compression: None
                lsize: 0
                csize: 0
                checksum: 0
          - key: 12
            ptr:
              Mem:  # In-memory Int node with a child in the target zone
                Int:
                  children:
                    - key: 12
                      ptr:
                        Mem:
                          Leaf:
                            items:
                              6: 6.0
                              7: 7.0
                    - key: 16
                      ptr:
                        Addr:
                          pba:
                            cluster: 0
                            lba: 102
                          compression: None
                          lsize: 0
                          csize: 0
                          checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let start = PBA::new(0, 100);
    let end = PBA::new(0, 200);
    rt.block_on(tree.clean_zone(start..end)).unwrap();
}

/// Insert an item into a Tree that's not dirty
#[test]
fn insert_below_root() {
    let mut mock = DDMLMock::new();
    let node = Arc::new(Node::new(NodeData::Leaf(LeafData::new())));
    let node_holder = RefCell::new(Some(node));
    let drpl = DRP::new(PBA{cluster: 0, lba: 0}, Compression::None, 36, 36, 0);
    mock.expect_pop::<Arc<Node<DRP, u32, u32>>, Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |arg: & *const DRP| unsafe {**arg == drpl} ))
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node_holder.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, u32>>>, Error>(res))
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
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
              Addr:
                pba:
                  cluster: 0
                  lba: 0
                compression: None
                lsize: 36
                csize: 36
                checksum: 0
          - key: 256
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 256
                compression: ZstdL9NoShuffle
                lsize: 16000
                csize: 8000
                checksum: 1234567
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.insert(0, 0));
    assert_eq!(r, Ok(None));
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
                    0: 0
          - key: 256
            ptr:
              Addr:
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
    let mut mock = DDMLMock::new();
    let node = Arc::new(Node::new(NodeData::Leaf(LeafData::new())));
    let node_holder = RefCell::new(Some(node));
    let drpl = DRP::new(PBA{cluster: 0, lba: 0}, Compression::None, 36, 36, 0);
    mock.expect_pop::<Arc<Node<DRP, u32, u32>>, Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |arg: & *const DRP| unsafe {**arg == drpl} ))
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must hack it with RefCell<Option<T>>
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node_holder.borrow_mut().take().unwrap());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, u32>>>, Error>(res))
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Addr:
      pba:
        cluster: 0
        lba: 0
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.insert(0, 0));
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
            -> Method<(), Poll<Box<Arc<Node<DRP, u32, f32>>>, Error>>
        {
            self.e.expect::<(), Poll<Box<Arc<Node<DRP, u32, f32>>>, Error>>("poll")
        }

        pub fn then(&mut self) -> &mut Self {
            self.e.then();
            self
        }
    }

    impl Future for FutureMock {
        type Item = Box<Arc<Node<DRP, u32, f32>>>;
        type Error = Error;

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            self.e.was_called_returning::<(),
                Poll<Box<Arc<Node<DRP, u32, f32>>>, Error>>("poll", ())
        }
    }

    let mut mock = DDMLMock::new();
    let mut ld1 = LeafData::new();
    ld1.insert(0, 0.0);
    ld1.insert(1, 1.0);
    ld1.insert(2, 2.0);
    ld1.insert(3, 3.0);
    ld1.insert(4, 4.0);
    let node1 = Arc::new(Node::new(NodeData::Leaf(ld1)));
    mock.expect_get::<Arc<Node<DRP, u32, f32>>>()
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
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, f32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Addr:
      pba:
        cluster: 0
        lba: 0
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);
    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(
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
        IntElem{key: 0u32, ptr: TreePtr::Addr(drp0)},
        IntElem{key: 256u32, ptr: TreePtr::Addr(drp1)},
    ];
    let node = Arc::new(Node::new(NodeData::Int(IntData{children})));
    let drpl = DRP::new(PBA{cluster: 1, lba: 2}, Compression::None, 36, 36, 0);
    let mut mock = DDMLMock::new();
    mock.expect_get::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |arg: & *const DRP| unsafe {**arg == drpl} ))
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must clone db here.
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node.clone());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, u32>>>, Error>(res))
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Addr:
      pba:
        cluster: 1
        lba: 2
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(future::lazy(|| {
        let root_guard = tree.i.root.try_read().unwrap();
        tree.rlock(&root_guard).map(|node| {
            let int_data = (*node).as_int();
            assert_eq!(int_data.nchildren(), 2);
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
    let mut mock = DDMLMock::new();
    let mut ld = LeafData::new();
    ld.insert(0, 100);
    ld.insert(1, 200);
    ld.insert(99, 50_000);
    let node = Arc::new(Node::new(NodeData::Leaf(ld)));
    mock.expect_get::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .returning(move |_| {
            // XXX simulacrum can't return a uniquely owned object in an
            // expectation, so we must clone db here.
            // https://github.com/pcsm/simulacrum/issues/52
            let res = Box::new(node.clone());
            Box::new(future::ok::<Box<Arc<Node<DRP, u32, u32>>>, Error>(res))
        });
    let ddml = Arc::new(mock);
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Addr:
      pba:
        cluster: 0
        lba: 0
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.get(1));
    assert_eq!(Ok(Some(200)), r);
}

// If the tree isn't dirty, then there's nothing to do
#[test]
fn write_clean() {
    let ddml = Arc::new(DDMLMock::new());
    let tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  key: 0
  ptr:
    Addr:
      pba:
        cluster: 0
        lba: 0
      compression: None
      lsize: 36
      csize: 36
      checksum: 0
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush());
    assert!(r.is_ok());
}

/// Sync a Tree with both dirty Int nodes and dirty Leaf nodes
#[test]
fn write_deep() {
    let mut mock = DDMLMock::new();
    let drp = DRP::random(Compression::None, 1000);
    mock.expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(Arc<Node<DRP, u32, u32>>, _)| {
            let node_data = arg.0.try_read().unwrap();
            let leaf_data = node_data.as_leaf();
            leaf_data.get(&0) == Some(100) &&
            leaf_data.get(&1) == Some(200)
        }))
        .returning(move |_| (drp, Box::new(future::ok::<(), Error>(()))));
    mock.then().expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(Arc<Node<DRP, u32, u32>>, _)| {
            let node_data = arg.0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            int_data.children[0].ptr.is_addr() &&
            int_data.children[1].key == 256 &&
            int_data.children[1].ptr.is_addr()
        }))
        .returning(move |_| (drp, Box::new(future::ok::<(), Error>(()))));
    let ddml = Arc::new(mock);
    let mut tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
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
              Addr:
                pba:
                  cluster: 0
                  lba: 256
                compression: ZstdL9NoShuffle
                lsize: 16000
                csize: 8000
                checksum: 0x1a7ebabe
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush());
    assert!(r.is_ok());
    assert_eq!(*tree.i.root.get_mut().unwrap().ptr.as_addr(), drp);
}

#[test]
fn write_int() {
    let mut mock = DDMLMock::new();
    let drp = DRP::random(Compression::None, 1000);
    mock.expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(Arc<Node<DRP, u32, u32>>, _)| {
            let node_data = arg.0.try_read().unwrap();
            let int_data = node_data.as_int();
            int_data.children[0].key == 0 &&
            !int_data.children[0].ptr.is_mem() &&
            int_data.children[1].key == 256 &&
            !int_data.children[1].ptr.is_mem()
        }))
        .returning(move |_| (drp, Box::new(future::ok::<(), Error>(()))));
    let ddml = Arc::new(mock);
    let mut tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
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
              Addr:
                pba:
                  cluster: 0
                  lba: 0
                compression: None
                lsize: 40000
                csize: 40000
                checksum: 0xdeadbeef
          - key: 256
            ptr:
              Addr:
                pba:
                  cluster: 0
                  lba: 256
                compression: ZstdL9NoShuffle
                lsize: 16000
                csize: 8000
                checksum: 0x1a7ebabe
"#);

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush());
    assert!(r.is_ok());
    assert_eq!(*tree.i.root.get_mut().unwrap().ptr.as_addr(), drp);
}

#[test]
fn write_leaf() {
    let mut mock = DDMLMock::new();
    let drp = DRP::random(Compression::None, 1000);
    mock.expect_put::<Arc<Node<DRP, u32, u32>>>()
        .called_once()
        .with(passes(move |&(ref arg, _): &(Arc<Node<DRP, u32, u32>>, _)| {
            let node_data = arg.0.try_read().unwrap();
            let leaf_data = node_data.as_leaf();
            leaf_data.get(&0) == Some(100) &&
            leaf_data.get(&1) == Some(200)
        })).returning(move |_| (drp, Box::new(future::ok::<(), Error>(()))));
    let ddml = Arc::new(mock);
    let mut tree: Tree<DRP, DDMLMock, u32, u32> = Tree::from_str(ddml, r#"
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

    let mut rt = current_thread::Runtime::new().unwrap();
    let r = rt.block_on(tree.flush());
    assert!(r.is_ok());
    assert_eq!(*tree.i.root.get_mut().unwrap().ptr.as_addr(), drp);
}

}
// LCOV_EXCL_STOP
