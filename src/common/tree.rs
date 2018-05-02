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
use serde::{Serialize, Serializer};
use serde::de::{self, Deserialize, Deserializer, DeserializeOwned, Visitor,
                MapAccess};
#[cfg(test)] use serde_yaml;
use std::collections::BTreeMap;
use std::fmt::{self, Debug};
#[cfg(test)] use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::executor::current_thread;

mod atomic_usize_serializer {
    use super::*;

    pub fn deserialize<'de, D>(d: D) -> Result<AtomicUsize, D::Error>
        where D: Deserializer<'de>
    {
        struct UsizeVisitor;

        impl<'de> Visitor<'de> for UsizeVisitor {
            type Value = usize;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an integer between -2^31 and 2^31")
            }

            fn visit_u8<E>(self, value: u8) -> Result<usize, E>
                where E: de::Error
            {
                Ok(value as usize)
            }
            fn visit_u16<E>(self, value: u16) -> Result<usize, E>
                where E: de::Error
            {
                Ok(value as usize)
            }
            fn visit_u32<E>(self, value: u32) -> Result<usize, E>
                where E: de::Error
            {
                Ok(value as usize)
            }
            fn visit_u64<E>(self, value: u64) -> Result<usize, E>
                where E: de::Error
            {
                Ok(value as usize)
            }
        }
        d.deserialize_u64(UsizeVisitor).map(|x| AtomicUsize::new(x as usize))
    }

    pub fn serialize<S>(x: &AtomicUsize, s: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        s.serialize_u64(x.load(Ordering::Relaxed) as u64)
    }
}

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

#[derive(Serialize, Debug)]
enum SerializableTreePtr<'a, K: Key, V: Value> {
    Mem(&'a Node<K, V>),
}

impl<K: Key, V: Value> TreePtr<K, V> {
    /// Lock the `TreePtr` in shared mode "read lock"
    fn rlock(&self) -> Box<Future<Item=TreeReadGuard<K, V>, Error=Error>> {
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

    /// Lock the `TreePtr` in exclusive mode
    fn xlock(&self) -> Box<Future<Item=TreeWriteGuard<K, V>, Error=Error>> {
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

impl<'de, K: Key, V: Value> Deserialize<'de> for TreePtr<K, V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {

        #[derive(Deserialize)]
        #[serde(field_identifier)]
        enum Field { Mem };

        struct TreePtrVisitor<K: Key, V: Value> {
            _k: PhantomData<K>,
            _v: PhantomData<V>
        }

        impl<'de, K: Key, V: Value> Visitor<'de> for TreePtrVisitor<K, V> {
            type Value = TreePtr<K, V>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("enum TreePtr")
            }

            fn visit_map<Q>(self, mut map: Q) -> Result<TreePtr<K, V>, Q::Error>
                where Q: MapAccess<'de>
            {
                let mut ptr = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Mem => {
                            if ptr.is_some() {
                                return Err(de::Error::duplicate_field("mem"));
                            }
                            ptr = Some(
                                TreePtr::Mem(
                                    RwLock::new(
                                        Box::new(map.next_value()?
                                        )
                                    )
                                )
                            );
                        }
                    }
                }
                ptr.ok_or(de::Error::missing_field("mem"))
            }
        }

        const FIELDS: &'static [&'static str] = &["Mem"];
        let visitor = TreePtrVisitor{_k: PhantomData, _v: PhantomData};
        deserializer.deserialize_struct("Mem", FIELDS, visitor)
    }
}

impl<K: Key, V: Value> Serialize for TreePtr<K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer {

        match self {
            &TreePtr::Mem(ref lock) => {
                let guard = current_thread::block_on_all(lock.read()).unwrap();
                let t = SerializableTreePtr::Mem(&*guard);
                t.serialize(serializer)
            },
            _ => unimplemented!()
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned, V: DeserializeOwned"))]
struct LeafNode<K: Key, V> {
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
struct IntElem<K: Key + DeserializeOwned, V: Value> {
    key: K,
    ptr: TreePtr<K, V>
}

impl<K: Key, V: Value> IntElem<K, V> {
    /// Lock the element in shared mode "read lock"
    fn rlock(&self) -> Box<Future<Item=TreeReadGuard<K, V>, Error=Error>> {
        self.ptr.rlock()
    }

    /// Lock the element in exclusive mode
    fn xlock(&self) -> Box<Future<Item=TreeWriteGuard<K, V>, Error=Error>> {
        self.ptr.xlock()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
struct IntNode<K: Key, V: Value> {
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
        (new_children[0].key, IntNode{children: new_children})
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
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

    /// Return this `Node`s lower bound key, suitable for use in its parent's
    /// `children` array.
    fn key(&self) -> K {
        match self {
            &Node::Leaf(ref leaf) => *leaf.items.keys().nth(0).unwrap(),
            &Node::Int(ref int) => int.children[0].key,
        }
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
#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
pub struct Tree<K: Key, V: Value> {
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
    root: TreePtr<K, V>
}

impl<'a, K: Key, V: Value> Tree<K, V> {
    pub fn create() -> Self {
        Tree::new(4,        // BetrFS's min fanout
                  16,       // BetrFS's max fanout
                  1<<22,    // BetrFS's max size
        )
    }

    #[cfg(test)]
    pub fn from_str(s: &str) -> Self {
        serde_yaml::from_str(s).unwrap()
    }

    /// Insert value `v` into the tree at key `k`, returning the previous value
    /// for that key, if any.
    pub fn insert(&'a self, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        Box::new(
            self.root.xlock()
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
                    children: vec![new_elem]
                }
            );
            let old_root = mem::replace(root.deref_mut(), new_root);
            let old_ptr = TreePtr::Mem(RwLock::new(Box::new(old_root)));
            let old_elem = IntElem{ key: K::min_value(), ptr: old_ptr };
            root.as_int_mut().unwrap().children.insert(0, old_elem);
            self.height.fetch_add(1, Ordering::Relaxed);
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
                let fut = int.children[child_idx].xlock();
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
            self.root.rlock()
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
                child_elem.rlock()
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
            height: AtomicUsize::new(1),
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
            self.root.xlock()
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
                (parent.as_int_mut().unwrap().children[child_idx + 1].xlock(),
                 true)
            } else {
                (parent.as_int_mut().unwrap().children[child_idx - 1].xlock(),
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
                            parent.as_int_mut().unwrap().children[child_idx+1]
                                .key = sibling.key();
                        }
                    } else {
                        if sibling.can_merge(&child, self.max_fanout) {
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
        let new_root = if let Node::Int(ref mut int) = *root {
            if int.children.len() == 1 {
                // Merge root node with its child
                let child = int.children.pop().unwrap();
                Some(match child.ptr {
                    TreePtr::Mem(lock) => *lock.try_unwrap().unwrap(),
                    _ => unimplemented!()
                })
            } else {
                None
            }
        } else {
            None
        };
        if new_root.is_some() {
            mem::replace(root.deref_mut(), new_root.unwrap());
            self.height.fetch_sub(1, Ordering::Relaxed);
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
                let fut = int.children[child_idx].xlock();
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
impl<K: Key, V: Value> Display for Tree<K, V> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(&serde_yaml::to_string(self).unwrap())
    }
}



#[cfg(test)]
mod t {

use super::*;
use futures::future;

#[test]
fn insert() {
    let tree: Tree<u32, f32> = Tree::new(2, 5, 1<<22);
    let r = current_thread::block_on_all(tree.insert(0, 0.0));
    assert_eq!(r, Ok(None));
    assert_eq!(format!("{}", tree),
r#"---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
  Mem:
    Leaf:
      items:
        0: 0.0"#);
}

#[test]
fn insert_dup() {
    let tree = Tree::from_str(r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
  Mem:
    Leaf:
      items:
        0: 100.0"#);
}

/// Insert a key that splits a non-root interior node
#[test]
fn insert_split_int() {
    let tree = Tree::from_str(r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    let tree = Tree::from_str(r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    let tree = Tree::from_str(r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    let tree = Tree::from_str(r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
fn remove_last_key() {
    let tree = Tree::from_str(r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
  Mem:
    Leaf:
      items: {}"#);
}

#[test]
fn remove_from_leaf() {
    let tree = Tree::from_str(r#"
---
height: 1
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
  Mem:
    Leaf:
      items:
        0: 0.0
        2: 2.0"#);
}

#[test]
fn remove_and_merge_int_left() {
    let tree: Tree<u32, f32> = Tree::from_str(r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    let tree: Tree<u32, f32> = Tree::from_str(r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    println!("{}", &tree);
    assert_eq!(format!("{}", &tree),
r#"---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    let tree: Tree<u32, f32> = Tree::from_str(r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    let tree: Tree<u32, f32> = Tree::from_str(r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    let tree: Tree<u32, f32> = Tree::from_str(r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    let tree: Tree<u32, f32> = Tree::from_str(r#"
---
height: 3
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    let tree: Tree<u32, f32> = Tree::from_str(r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    let tree: Tree<u32, f32> = Tree::from_str(r#"
---
height: 2
min_fanout: 2
max_fanout: 5
_max_size: 4194304
root:
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
    let tree: Tree<u32, f32> = Tree::create();
    let r = current_thread::block_on_all(tree.remove(3));
    assert_eq!(r, Ok(None));
}

}
