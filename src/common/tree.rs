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
use std::ops::DerefMut;

/// Anything that has a min_value method.  Too bad libstd doesn't define this.
pub trait MinValue {
    fn min_value() -> Self;
}

impl MinValue for u32 {
    fn min_value() -> Self {
        u32::min_value()
    }
}

pub trait Key: Copy + Debug + Ord + MinValue {}

impl<T> Key for T where T: Copy + Debug + Ord + MinValue {}

pub trait Value: Copy + Debug {}

impl<T> Value for T where T: Copy + Debug {}

#[derive(Debug)]
enum TreePtr<K: Key, V: Value> {
    /// Dirty btree nodes live only in RAM, not on disk or in cache.  Being
    /// RAM-resident, we don't need to store their checksums or lsizes.
    Mem(Box<Node<K, V>>),
    /// Direct Recird Pointers point directly to a disk location
    _DRP(DRP),
    /// Indirect Record Pointers point to the Record Indirection Table
    _IRP(u64)
}

#[derive(Debug)]
struct LeafData<K: Key, V: Value> {
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
}

#[derive(Debug)]
struct IntElem<K: Key, V: Value> {
    key: K,
    ptr: TreePtr<K, V>
}

#[derive(Debug)]
struct IntData<K: Key, V: Value> {
    /// position in the tree.  Leaves have rank 0, `IntNodes` with Leaf children
    /// have rank 1, etc.  The root has the highest rank.
    rank: u8,
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
        (new_children[0].key, IntData{rank: self.rank, children: new_children})
    }
}

#[derive(Debug)]
enum NodeData<K: Key, V: Value> {
    Leaf(LeafData<K, V>),
    Int(IntData<K, V>)
}

impl<K: Key, V: Value> NodeData<K, V> {
    fn as_int_mut(&mut self) -> Option<&mut IntData<K, V>> {
        if let &mut NodeData::Int(ref mut data) = self {
            Some(data)
        } else {
            None
        }
    }

    #[cfg(test)]
    fn as_leaf_mut(&mut self) -> Option<&mut LeafData<K, V>> {
        if let &mut NodeData::Leaf(ref mut data) = self {
            Some(data)
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        match self {
            &NodeData::Leaf(ref data) => data.items.len(),
            &NodeData::Int(ref data) => data.children.len()
        }
    }

    fn should_split(&self, max_fanout: usize) -> bool {
        let len = self.len();
        debug_assert!(len <= max_fanout,
                      "Overfull nodes shouldn't be possible");
        len >= max_fanout
    }

    fn split(&mut self) -> (K, NodeData<K, V>) {
        match *self {
            NodeData::Leaf(ref mut data) => {
                let (k, new_data) = data.split();
                (k, NodeData::Leaf(new_data))
            },
            NodeData::Int(ref mut data) => {
                let (k, new_data) = data.split();
                (k, NodeData::Int(new_data))
            },

        }
    }
}

#[derive(Debug)]
struct Node<K: Key, V: Value> {
    data: RwLock<NodeData<K, V>>
}

impl<K: Key, V: Value> Node<K, V> {
    fn read(&self) -> RwLockReadFut<NodeData<K, V>> {
        self.data.read()
    }

    fn write(&self) -> RwLockWriteFut<NodeData<K, V>> {
        self.data.write()
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
    root: Node<K, V>
}

impl<'a, K: Key, V: Value> Tree<K, V> {
    pub fn create() -> Self {
        Tree{
            _min_fanout: 4,      // BetrFS's value
            max_fanout: 16,     // BetrFS's value
            _max_size: 1<<22,    // 4MB: BetrFS's value
            root: Node {
                data: RwLock::new(NodeData::Leaf(
                    LeafData{
                        items: BTreeMap::new()
                    }
                ))
            }
        }
    }

    /// Insert value `v` into the tree at key `k`, returning the previous value
    /// for that key, if any.
    pub fn insert(&'a self, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        Box::new(
            self.root.write()
                .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                .and_then(move |root_data| {
                    self.insert_locked(root_data, k, v)
            })
        )
    }

    /// Insert value `v` into an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn insert_int(&'a self, mut parent: RwLockWriteGuard<NodeData<K, V>>,
                  child_idx: usize,
                  mut child_data: RwLockWriteGuard<NodeData<K, V>>, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        let parent_int_data = parent.as_int_mut().unwrap();
        // First, split the node, if necessary
        if child_data.should_split(self.max_fanout) {
            let (new_key, new_child) = child_data.split();
            let new_node = Node{data: RwLock::new(new_child)};
            let new_elem = IntElem{key: new_key,
                                   ptr: TreePtr::Mem(Box::new(new_node))};
            parent_int_data.children.insert(child_idx + 1, new_elem);
        }
        drop(parent_int_data);

        // Now insert the value into the child
        self.insert_no_split(child_data, k, v)
    }

    /// Helper for `insert`.  Handles insertion once the tree is locked
    fn insert_locked(&'a self, mut root_data: RwLockWriteGuard<NodeData<K, V>>,
                     k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        // First, split the root node, if necessary
        if root_data.should_split(self.max_fanout) {
            let (new_key, new_child) = root_data.split();
            let new_node = Node{data: RwLock::new(new_child)};
            let new_elem = IntElem{key: new_key,
                                   ptr: TreePtr::Mem(Box::new(new_node))};
            let new_root_data = NodeData::Int(
                IntData {
                    children: vec![new_elem],
                    rank: 100 //TODO calculate correctly
                }
            );
            let old_root_data = mem::replace(root_data.deref_mut(), new_root_data);
            let old_root = Node{
                data: RwLock::new(old_root_data)
            };
            let old_elem = IntElem{
                key: K::min_value(),
                ptr: TreePtr::Mem(Box::new(old_root))
            };
            root_data.as_int_mut().unwrap().children.insert(0, old_elem);
        }

        self.insert_no_split(root_data, k, v)
    }

    fn insert_no_split(&'a self,
                       mut node_data: RwLockWriteGuard<NodeData<K, V>>,
                       k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        let (child_idx, child_fut) = match *node_data {
            NodeData::Leaf(ref mut data) => {
                return Box::new(Ok(data.insert(k, v)).into_future())
            },
            NodeData::Int(ref data) => {
                let child_idx = data.position(&k);
                let fut = match data.children[child_idx].ptr {
                    TreePtr::Mem(ref node) => node.write()
                        .map_err(|_| Error::Sys(errno::Errno::EPIPE)),
                    _ => unimplemented!()
                };
                (child_idx, fut)
            }
        };
        Box::new(child_fut.and_then(move |child_data| {
                self.insert_int(node_data, child_idx, child_data, k, v)
            })
        )
    }

    /// Lookup the value of key `k`.  Return an error if no value is present.
    pub fn lookup(&'a self, k: K) -> Box<Future<Item=V, Error=Error> + 'a> {
        Box::new(
            self.root.read()
                .map_err(|_| Error::Sys(errno::Errno::EPIPE))
                .and_then(move |root_data| self.lookup_node(root_data, k))
        )
    }

    /// Lookup the value of key `k` in a node, which must already be locked.
    fn lookup_node(&'a self, node_data: RwLockReadGuard<NodeData<K, V>>, k: K)
        -> Box<Future<Item=V, Error=Error> + 'a> {

        let next_node_fut = match *node_data {
            NodeData::Leaf(ref data) => {
                return Box::new(data.lookup(k).into_future())
            },
            NodeData::Int(ref data) => {
                let child_elem = &data.children[data.position(&k)];
                match child_elem.ptr {
                    TreePtr::Mem(ref node) => node.read()
                        .map_err(|_| Error::Sys(errno::Errno::EPIPE)),
                    _ => unimplemented!()
                }
            }
        };
        drop(node_data);
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

/// A Tree with enough elements to split the root node
#[test]
fn root_split() {
    let mut tree: Tree<u32, f32> = Tree::create();
    let r1 = current_thread::block_on_all(future::lazy(|| {
        let tree1 = &tree;
        let inserts = (0..16).map(|k| {
            tree1.insert(k, k as f32)
        }).collect::<Vec<_>>();
        future::join_all(inserts)
    }));
    assert!(r1.is_ok());
    assert!(tree.root.data.get_mut().unwrap().as_leaf_mut().is_some());
    let r2 = current_thread::block_on_all(tree.insert(16, 16.0));
    assert!(r2.is_ok());
    assert!(tree.root.data.get_mut().unwrap().as_int_mut().is_some());
}

}
