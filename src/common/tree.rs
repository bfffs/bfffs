// vim: tw=80

//! COW B+-Trees, based on B-trees, Shadowing, and Clones[^CowBtrees]
//!
//! [^CowBtrees]: Rodeh, Ohad. "B-trees, shadowing, and clones." ACM Transactions on Storage (TOS) 3.4 (2008): 2.

use common::*;
use common::ddml::DRP;
use futures::future::IntoFuture;
use futures::Future;
use futures_locks::*;
use nix::{Error, errno};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::mem;
use std::ops::{Deref, DerefMut};

/// Anything that has a min_value method.  Too bad libstd doesn't define this.
pub trait MinValue {
    fn min_value() -> Self;
}

impl MinValue for u32 {
    fn min_value() -> Self {
        u32::min_value()
    }
}

pub trait Key: Copy + Debug + Ord + MinValue + 'static {}

impl<T> Key for T where T: Copy + Debug + Ord + MinValue + 'static {}

pub trait Value: Copy + Debug + 'static {}

impl<T> Value for T where T: Copy + Debug + 'static {}

#[derive(Debug)]
enum TreePtr<K: Key, V: Value> {
    /// Dirty btree nodes live only in RAM, not on disk or in cache.  Being
    /// RAM-resident, we don't need to store their checksums or lsizes.
    Mem(RwLock<Box<Node<K, V>>>),
    /// Direct Record Pointers point directly to a disk location
    _DRP(RwLock<DRP>),
    /// Indirect Record Pointers point to the Record Indirection Table
    _IRP(RwLock<u64>)
}

impl<K: Key, V: Value> TreePtr<K, V> {
    #[cfg(test)]
    fn as_mem(&mut self) -> Option<&mut RwLock<Box<Node<K, V>>>> {
        if let &mut TreePtr::Mem(ref mut ptr) = self {
            Some(ptr)
        } else {
            None
        }
    }

    fn read(&self) -> Box<Future<Item=TreeReadGuard<K, V>, Error=Error>> {
        Box::new(
            match self {
                &TreePtr::Mem(ref lock) => {
                    lock.read()
                        .map(|guard| TreeReadGuard::Mem(guard))
                        .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                },
                _ => unimplemented!()
            }
        )
    }

    fn write(&self) -> Box<Future<Item=TreeWriteGuard<K, V>, Error=Error>> {
        Box::new(
            match self {
                &TreePtr::Mem(ref lock) => {
                    lock.write()
                        .map(|guard| TreeWriteGuard::Mem(guard))
                        .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                },
                _ => unimplemented!()
            }
        )
    }
}

#[derive(Debug)]
struct LeafNode<K: Key, V: Value> {
    items: BTreeMap<K, V>
}

impl<K: Key, V: Value> LeafNode<K, V> {
    fn split(&mut self) -> (K, LeafNode<K, V>) {
        // Split the node in two.  Make the left node larger, on the assumption
        // that we're more likely to insert into the right node than the left
        // one.
        let half = div_roundup(self.items.len(), 2);
        let cutoff = *self.items.keys().nth(half).unwrap();
        let new_items = self.items.split_off(&cutoff);
        (cutoff, LeafNode{items: new_items})
    }
}

impl<K: Key, V: Value> LeafNode<K, V> {
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
    Mem(RwLockReadGuard<Box<Node<K, V>>>),
}

impl<K: Key, V: Value> Deref for TreeReadGuard<K, V> {
    type Target = Node<K, V>;

    fn deref(&self) -> &Self::Target {
        match self {
            &TreeReadGuard::Mem(ref guard) => &**guard,
        }
    }
}

/// Guard that holds the Node lock object for writing
enum TreeWriteGuard<K: Key, V: Value> {
    Mem(RwLockWriteGuard<Box<Node<K, V>>>),
}

impl<K: Key, V: Value> Deref for TreeWriteGuard<K, V> {
    type Target = Node<K, V>;

    fn deref(&self) -> &Self::Target {
        match self {
            &TreeWriteGuard::Mem(ref guard) => &**guard,
        }
    }
}

impl<K: Key, V: Value> DerefMut for TreeWriteGuard<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            &mut TreeWriteGuard::Mem(ref mut guard) => &mut **guard,
        }
    }
}

#[derive(Debug)]
struct IntElem<K: Key, V: Value> {
    key: K,
    ptr: TreePtr<K, V>
}

impl<K: Key, V: Value> IntElem<K, V> {
    fn read(&self) -> Box<Future<Item=TreeReadGuard<K, V>, Error=Error>> {
        self.ptr.read()
    }

    fn write(&self) -> Box<Future<Item=TreeWriteGuard<K, V>, Error=Error>> {
        self.ptr.write()
    }
}

#[derive(Debug)]
struct IntNode<K: Key, V: Value> {
    /// position in the tree.  Leaves have rank 0, `IntNodes` with Leaf children
    /// have rank 1, etc.  The root has the highest rank.
    rank: u8,
    children: Vec<IntElem<K, V>>
}

impl<K: Key, V: Value> IntNode<K, V> {
    fn position(&self, k: &K) -> usize {
        // Find rightmost child whose key is less than or equal to k
        self.children
            .binary_search_by_key(k, |ref child| child.key)
            .unwrap_or_else(|k| k - 1)
    }

    fn split(&mut self) -> (K, IntNode<K, V>) {
        // Split the node in two.  Make the left node larger, on the assumption
        // that we're more likely to insert into the right node than the left
        // one.
        let cutoff = div_roundup(self.children.len(), 2);
        let new_children = self.children.split_off(cutoff);
        (new_children[0].key, IntNode{rank: self.rank, children: new_children})
    }
}

#[derive(Debug)]
enum Node<K: Key, V: Value> {
    Leaf(LeafNode<K, V>),
    Int(IntNode<K, V>)
}

impl<K: Key, V: Value> Node<K, V> {
    fn as_int_mut(&mut self) -> Option<&mut IntNode<K, V>> {
        if let &mut Node::Int(ref mut int) = self {
            Some(int)
        } else {
            None
        }
    }

    fn as_leaf_mut(&mut self) -> Option<&mut LeafNode<K, V>> {
        if let &mut Node::Leaf(ref mut leaf) = self {
            Some(leaf)
        } else {
            None
        }
    }

    /// Can this child be merged with `other` without violating constraints?
    fn can_merge(&self, other: &Node<K, V>, max_fanout: usize) -> bool {
        self.len() + other.len() <= max_fanout
    }

    /// Number of children or items in this `Node`
    fn len(&self) -> usize {
        match self {
            &Node::Leaf(ref leaf) => leaf.items.len(),
            &Node::Int(ref int) => int.children.len()
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

    fn split(&mut self) -> (K, Node<K, V>) {
        match *self {
            Node::Leaf(ref mut leaf) => {
                let (k, new_leaf) = leaf.split();
                (k, Node::Leaf(new_leaf))
            },
            Node::Int(ref mut int) => {
                let (k, new_int) = int.split();
                (k, Node::Int(new_int))
            },

        }
    }

    /// Merge all of `other`'s data into `self`.  Afterwards, `other` may be
    /// deleted.
    fn merge(&mut self, other: &mut Node<K, V>) {
        match *self {
            Node::Int(ref mut int) =>
                int.children.append(&mut other.as_int_mut().unwrap().children),
            Node::Leaf(ref mut leaf) =>
                leaf.items.append(&mut other.as_leaf_mut().unwrap().items),
        }
    }

    /// Take `other`'s highest keys and merge them into ourself
    fn take_high_keys(&mut self, other: &mut Node<K, V>) {
        let keys_to_share = (other.len() - self.len()) / 2;
        match *self {
            Node::Int(ref mut int) => {
                let other_children = &mut other.as_int_mut().unwrap().children;
                let cutoff_idx = other_children.len() - keys_to_share;
                let mut other_right_half =
                    other_children.split_off(cutoff_idx);
                int.children.splice(0..0, other_right_half.into_iter());
            },
            Node::Leaf(ref mut leaf) => {
                let other_items = &mut other.as_leaf_mut().unwrap().items;
                let cutoff_idx = other_items.len() - keys_to_share;
                let cutoff = *other_items.keys().nth(cutoff_idx).unwrap();
                let mut other_right_half = other_items.split_off(&cutoff);
                leaf.items.append(&mut other_right_half);
            }
        }
    }

    /// Take `other`'s lowest keys and merge them into ourself
    fn take_low_keys(&mut self, other: &mut Node<K, V>) {
        let keys_to_share = (other.len() - self.len()) / 2;
        match *self {
            Node::Int(ref mut int) => {
                let other_children = &mut other.as_int_mut().unwrap().children;
                let other_left_half = other_children.drain(0..keys_to_share);
                let nchildren = int.children.len();
                int.children.splice(nchildren.., other_left_half);
            },
            Node::Leaf(ref mut leaf) => {
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

/// In-memory representation of a COW B+-Tree
///
/// # Generic Parameters
///
/// *`K`:   Key type.  Must be ordered and copyable; should be compact
/// *`V`:   Value type in the leaves.
#[derive(Debug)]
pub struct Tree<K: Key, V: Value> {
    /// Minimum node fanout.  Smaller nodes will be merged, or will steal
    /// children from their neighbors.
    min_fanout: usize,
    /// Maximum node fanout.  Larger nodes will be split.
    max_fanout: usize,
    /// Maximum node size in bytes.  Larger nodes will be split or their message
    /// buffers flushed
    _max_size: usize,
    /// Root node
    root: TreePtr<K, V>
}

impl<'a, K: Key, V: Value> Tree<K, V> {
    pub fn create() -> Self {
        Tree::new(4,        // BetrFS's min fanout
                  16,       // BetrFS's max fanout
                  1<<22,    // BetrFS's max size
        )
    }

    /// Insert value `v` into the tree at key `k`, returning the previous value
    /// for that key, if any.
    pub fn insert(&'a self, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        Box::new(
            self.root.write()
                .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                .and_then(move |root| {
                    self.insert_locked(root, k, v)
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
        if child.should_split(self.max_fanout) {
            let (new_key, new_node) = child.split();
            let new_ptr = TreePtr::Mem(RwLock::new(Box::new(new_node)));
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
    fn insert_locked(&'a self, mut root: TreeWriteGuard<K, V>,
                     k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        // First, split the root node, if necessary
        if root.should_split(self.max_fanout) {
            let (new_key, new_node) = root.split();
            let new_ptr = TreePtr::Mem(RwLock::new(Box::new(new_node)));
            let new_elem = IntElem{key: new_key, ptr: new_ptr};
            let new_root = Node::Int(
                IntNode {
                    children: vec![new_elem],
                    rank: 100 //TODO calculate correctly
                }
            );
            let old_root = mem::replace(root.deref_mut(), new_root);
            let old_ptr = TreePtr::Mem(RwLock::new(Box::new(old_root)));
            let old_elem = IntElem{ key: K::min_value(), ptr: old_ptr };
            root.as_int_mut().unwrap().children.insert(0, old_elem);
        }

        self.insert_no_split(root, k, v)
    }

    fn insert_no_split(&'a self,
                       mut node: TreeWriteGuard<K, V>,
                       k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        let (child_idx, child_fut) = match *node {
            Node::Leaf(ref mut leaf) => {
                return Box::new(Ok(leaf.insert(k, v)).into_future())
            },
            Node::Int(ref int) => {
                let child_idx = int.position(&k);
                let fut = int.children[child_idx].write();
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
            self.root.read()
                .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                .and_then(move |root| self.lookup_node(root, k))
        )
    }

    /// Lookup the value of key `k` in a node, which must already be locked.
    fn lookup_node(&'a self, node: TreeReadGuard<K, V>, k: K)
        -> Box<Future<Item=V, Error=Error> + 'a> {

        let next_node_fut = match *node {
            Node::Leaf(ref leaf) => {
                return Box::new(leaf.lookup(k).into_future())
            },
            Node::Int(ref int) => {
                let child_elem = &int.children[int.position(&k)];
                child_elem.read()
            }
        };
        drop(node);
        Box::new(
            next_node_fut
            .and_then(move |next_node| self.lookup_node(next_node, k))
        )
    }

    fn new(min_fanout: usize, max_fanout: usize, max_size: usize) -> Self {
        Tree{
            min_fanout, max_fanout,
            _max_size: max_size,
            root: TreePtr::Mem(
                RwLock::new(
                    Box::new(
                        Node::Leaf(
                            LeafNode{
                                items: BTreeMap::new()
                            }
                        )
                    )
                )
            )
        }
    }

    /// Remove and return the value at key `k`, if any.
    pub fn remove(&'a self, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        Box::new(
            self.root.write()
                .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                .and_then(move |root| {
                    self.remove_locked(root, k)
            })
        )
    }

    /// Remove key `k` from an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn remove_int(&'a self, mut parent: TreeWriteGuard<K, V>,
                  child_idx: usize, mut child: TreeWriteGuard<K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        // First, fix the node, if necessary
        if child.should_fix(self.min_fanout) {
            // Outline:
            // First, try to merge with the right sibling
            // Then, try to steal keys from the right sibling
            // Then, try to merge with the left sibling
            // Then, try to steal keys from the left sibling
            let nchildren = parent.as_int_mut().unwrap().children.len();
            let (fut, right) = if child_idx < nchildren - 1 {
                (parent.as_int_mut().unwrap().children[child_idx + 1].write(),
                 true)
            } else {
                (parent.as_int_mut().unwrap().children[child_idx - 1].write(),
                 false)
            };
            Box::new(
                fut.map(move |mut sibling| {
                    if right {
                        if child.can_merge(&sibling, self.max_fanout) {
                            child.merge(&mut sibling);
                            parent.as_int_mut().unwrap()
                                .children.remove(child_idx + 1);
                        } else {
                            child.take_low_keys(&mut sibling);
                        }
                    } else {
                        if sibling.can_merge(&child, self.max_fanout) {
                            sibling.merge(&mut child);
                            parent.as_int_mut().unwrap()
                                .children.remove(child_idx);
                        } else {
                            child.take_high_keys(&mut sibling);
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
    fn remove_locked(&'a self, root: TreeWriteGuard<K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        // First, fix the root node, if necessary
        if root.len() == 1 {
            // TODO: reduce tree height
            unimplemented!()
        }

        self.remove_no_fix(root, k)
    }

    /// Remove key `k` from a node, but don't try to fixup the node.
    fn remove_no_fix(&'a self, mut node: TreeWriteGuard<K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        let (child_idx, child_fut) = match *node {
            Node::Leaf(ref mut leaf) => {
                return Box::new(Ok(leaf.remove(k)).into_future())
            },
            Node::Int(ref int) => {
                let child_idx = int.position(&k);
                let fut = int.children[child_idx].write();
                (child_idx, fut)
            }
        };
        Box::new(child_fut.and_then(move |child| {
                self.remove_int(node, child_idx, child, k)
            })
        )
    }

}

#[cfg(test)]
mod t {

use super::*;
use futures::future;
use tokio::executor::current_thread;

#[test]
fn insert() {
    let tree: Tree<u32, f32> = Tree::create();
    let r = current_thread::block_on_all(future::lazy(|| {
        tree.insert(0, 0.0)
    }));
    assert_eq!(r, Ok(None));
}

#[test]
fn insert_dup() {
    let tree: Tree<u32, f32> = Tree::create();
    let r = current_thread::block_on_all(future::lazy(|| {
        tree.insert(0, 0.0)
            .and_then(|_| tree.insert(0, 100.0))
    }));
    assert_eq!(r, Ok(Some(0.0)));
}

/// Insert a key that splits a non-root interior node
#[test]
fn insert_split_int() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..24).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    assert_eq!(tree.root.as_mem().unwrap().get_mut().unwrap().len(), 2);
    let r2 = current_thread::block_on_all(tree.insert(24, 24.0));
    assert!(r2.is_ok());
    assert_eq!(tree.root.as_mem().unwrap().get_mut().unwrap().len(), 3);
}

/// Insert a key that splits a non-root leaf node
#[test]
fn insert_split_leaf() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..6).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    assert_eq!(tree.root.as_mem().unwrap().get_mut().unwrap().len(), 2);
    let r2 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (6..9).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r2.is_ok());
    assert_eq!(tree.root.as_mem().unwrap().get_mut().unwrap().len(), 3);
}

/// Insert a key that splits the root IntNode
#[test]
fn insert_split_root_int() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..15).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    assert!(tree.root.as_mem().unwrap()
            .get_mut().unwrap()
            .as_int_mut().is_some());
    let r2 = current_thread::block_on_all(tree.insert(15, 15.0));
    assert!(r2.is_ok());
    assert!(tree.root.as_mem().unwrap()
            .get_mut().unwrap()
            .as_int_mut().unwrap()
            .children[0].ptr.as_mem().unwrap()
            .get_mut().unwrap()
            .as_int_mut().is_some());
    for i in 0..15 {
        assert_eq!(current_thread::block_on_all(tree.lookup(i)), Ok(i as f32));
    }
}

/// Insert a key that splits the root leaf node
#[test]
fn insert_split_root_leaf() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..5).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    assert!(tree.root.as_mem().unwrap()
            .get_mut().unwrap()
            .as_leaf_mut().is_some());
    let r2 = current_thread::block_on_all(tree.insert(5, 5.0));
    assert!(r2.is_ok());
    assert!(tree.root.as_mem().unwrap()
            .get_mut().unwrap()
            .as_int_mut().is_some());
}

#[test]
fn lookup() {
    let tree: Tree<u32, f32> = Tree::create();
    let r = current_thread::block_on_all(future::lazy(|| {
        tree.insert(0, 0.0)
            .and_then(|_| tree.lookup(0))
    }));
    assert_eq!(r, Ok(0.0));
}

#[test]
fn lookup_nonexistent() {
    let tree: Tree<u32, f32> = Tree::create();
    let r = current_thread::block_on_all(tree.lookup(0));
    assert_eq!(r, Err(Error::Sys(errno::Errno::ENOENT)))
}

#[test]
fn remove_from_leaf() {
    let tree: Tree<u32, f32> = Tree::create();
    let r = current_thread::block_on_all(future::lazy(|| {
        tree.insert(0, 0.0)
            .and_then(|_| tree.insert(1, 1.0))
            .and_then(|_| tree.insert(2, 2.0))
            .and_then(|_| tree.remove(1))
    }));
    assert_eq!(r, Ok(Some(1.0)));
}

#[test]
fn remove_and_merge_int_left() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..25).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    assert_eq!(tree.root.as_mem().unwrap().get_mut().unwrap().len(), 3);
    let r2 = current_thread::block_on_all(tree.remove(24));
    assert!(r2.is_ok());
    assert_eq!(tree.root.as_mem().unwrap().get_mut().unwrap().len(), 2);
}

#[test]
fn remove_and_merge_int_right() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..25).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    assert_eq!(tree.root.as_mem().unwrap().get_mut().unwrap().len(), 3);
    let r2 = current_thread::block_on_all(future::lazy(|| {
        tree.remove(0)
            .and_then(|_| tree.remove(1))
            .and_then(|_| tree.remove(2))
            .and_then(|_| tree.remove(3))
    }));
    assert!(r2.is_ok());
    assert_eq!(tree.root.as_mem().unwrap().get_mut().unwrap().len(), 2);
}

#[test]
fn remove_and_merge_leaf_left() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..9).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    let r2 = current_thread::block_on_all(future::lazy(|| {
        tree.remove(7)
            .and_then(|_| tree.remove(8))
    }));
    assert!(r2.is_ok());
    assert_eq!(tree.root.as_mem().unwrap().get_mut().unwrap().len(), 2);
}

#[test]
fn remove_and_merge_leaf_right() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..9).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    let r2 = current_thread::block_on_all(future::lazy(|| {
        tree.remove(5)
            .and_then(|_| tree.remove(4))
    }));
    assert!(r2.is_ok());
    assert_eq!(tree.root.as_mem().unwrap().get_mut().unwrap().len(), 2);
}

#[test]
fn remove_and_steal_int_left() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 24, 25, 26, 27, 21, 22, 23].iter().map(|k| {
            tree1.insert(*k, *k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    let r2 = current_thread::block_on_all(tree.remove(26));
    assert!(r2.is_ok());
    assert_eq!(tree.root.as_mem().unwrap()
               .get_mut().unwrap()
               .as_int_mut().unwrap()
               .children[1].ptr.as_mem().unwrap()
               .get_mut().unwrap()
               .len(), 3);
    assert_eq!(tree.root.as_mem().unwrap()
               .get_mut().unwrap()
               .as_int_mut().unwrap()
               .children[2].ptr.as_mem().unwrap()
               .get_mut().unwrap()
               .len(), 3);
}

#[test]
fn remove_and_steal_int_right() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..24).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    let r2 = current_thread::block_on_all(future::lazy(|| {
        tree.remove(0)
            .and_then(|_| tree.remove(1))
            .and_then(|_| tree.remove(2))
    }));
    assert!(r2.is_ok());
    assert_eq!(tree.root.as_mem().unwrap()
               .get_mut().unwrap()
               .as_int_mut().unwrap()
               .children[1].ptr.as_mem().unwrap()
               .get_mut().unwrap()
               .len(), 4);
    assert_eq!(tree.root.as_mem().unwrap()
               .get_mut().unwrap()
               .as_int_mut().unwrap()
               .children[0].ptr.as_mem().unwrap()
               .get_mut().unwrap()
               .len(), 3);
}

#[test]
fn remove_and_steal_leaf_left() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = [0, 1, 2, 5, 6, 7, 3, 4].iter().map(|k| {
            tree1.insert(*k, *k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    let r2 = current_thread::block_on_all(future::lazy(|| {
        tree.remove(7)
            .and_then(|_| tree.remove(6))
    }));
    assert!(r2.is_ok());
    assert_eq!(tree.root.as_mem().unwrap()
               .get_mut().unwrap()
               .as_int_mut().unwrap()
               .children[0].ptr.as_mem().unwrap()
               .get_mut().unwrap()
               .as_leaf_mut().unwrap()
               .items.keys().cloned().collect::<Vec<_>>(),
               vec![0, 1, 2, 3]);
    assert_eq!(tree.root.as_mem().unwrap()
               .get_mut().unwrap()
               .as_int_mut().unwrap()
               .children[1].ptr.as_mem().unwrap()
               .get_mut().unwrap()
               .as_leaf_mut().unwrap()
               .items.keys().cloned().collect::<Vec<_>>(),
               vec![4, 5]);
}

#[test]
fn remove_and_steal_leaf_right() {
    let mut tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..11).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    let r2 = current_thread::block_on_all(future::lazy(|| {
        tree.remove(5)
            .and_then(|_| tree.remove(4))
    }));
    assert!(r2.is_ok());
    assert_eq!(tree.root.as_mem().unwrap().get_mut().unwrap().len(), 3);
    assert_eq!(tree.root.as_mem().unwrap()
               .get_mut().unwrap()
               .as_int_mut().unwrap()
               .children[1].ptr.as_mem().unwrap()
               .get_mut().unwrap()
               .as_leaf_mut().unwrap()
               .items.keys().cloned().collect::<Vec<_>>(),
               vec![3, 6]);
    assert_eq!(tree.root.as_mem().unwrap()
               .get_mut().unwrap()
               .as_int_mut().unwrap()
               .children[2].ptr.as_mem().unwrap()
               .get_mut().unwrap()
               .as_leaf_mut().unwrap()
               .items.keys().cloned().collect::<Vec<_>>(),
               vec![7, 8, 9, 10]);
}

#[test]
fn remove_nonexistent() {
    let tree: Tree<u32, f32> = Tree::create();
    let r = current_thread::block_on_all(future::lazy(|| {
        tree.insert(0, 0.0)
            .and_then(|_| tree.insert(1, 1.0))
            .and_then(|_| tree.insert(2, 2.0))
            .and_then(|_| tree.remove(3))
    }));
    assert_eq!(r, Ok(None));
}

}
