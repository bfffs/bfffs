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

    #[cfg(test)]
    fn as_leaf_mut(&mut self) -> Option<&mut LeafNode<K, V>> {
        if let &mut Node::Leaf(ref mut leaf) = self {
            Some(leaf)
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        match self {
            &Node::Leaf(ref leaf) => leaf.items.len(),
            &Node::Int(ref int) => int.children.len()
        }
    }

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
    _min_fanout: usize,
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
        Tree{
            _min_fanout: 4,      // BetrFS's value
            max_fanout: 16,     // BetrFS's value
            _max_size: 1<<22,    // 4MB: BetrFS's value
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
}

#[cfg(test)]
mod t {

use super::*;
use futures::future;
use tokio::executor::current_thread;

/// An empty tree
#[test]
fn empty() {
    let tree: Tree<u32, f32> = Tree::create();
    let r = current_thread::block_on_all(tree.lookup(0));
    assert_eq!(r, Err(Error::Sys(errno::Errno::ENOENT)))
}

/// A tree with one element
#[test]
fn one_elem() {
    let tree: Tree<u32, f32> = Tree::create();
    let r = current_thread::block_on_all(future::lazy(|| {
        tree.insert(0, 0.0)
            .and_then(|_| tree.lookup(0))
    }));
    assert_eq!(r, Ok(0.0));
}

/// A Tree with enough elements to split an internal node
#[test]
fn three_levels() {
    let mut tree: Tree<u32, f32> = Tree::create();
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..129).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    assert!(tree.root.as_mem().unwrap()
            .get_mut().unwrap()
            .as_int_mut().is_some());
    let r2 = current_thread::block_on_all(tree.insert(129, 129.0));
    assert!(r2.is_ok());
    assert!(tree.root.as_mem().unwrap()
            .get_mut().unwrap()
            .as_int_mut().unwrap()
            .children[0].ptr.as_mem().unwrap()
            .get_mut().unwrap()
            .as_int_mut().is_some());
    for i in 0..129 {
        assert_eq!(current_thread::block_on_all(tree.lookup(i)), Ok(i as f32));
    }
}

/// A Tree with enough elements to split the root node
#[test]
fn two_levels() {
    let mut tree: Tree<u32, f32> = Tree::create();
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..16).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    assert!(tree.root.as_mem().unwrap()
            .get_mut().unwrap()
            .as_leaf_mut().is_some());
    let r2 = current_thread::block_on_all(tree.insert(16, 16.0));
    assert!(r2.is_ok());
    assert!(tree.root.as_mem().unwrap()
            .get_mut().unwrap()
            .as_int_mut().is_some());
}

}
