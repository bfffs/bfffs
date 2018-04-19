// vim: tw=80

//! COW B+-Trees, based on B-trees, Shadowing, and Clones[^CowBtrees]
//!
//! [^CowBtrees]: Rodeh, Ohad. "B-trees, shadowing, and clones." ACM Transactions on Storage (TOS) 3.4 (2008): 2.

use common::*;
use futures::future::IntoFuture;
use futures::Future;
use futures_locks::{RwLock, RwLockReadGuard};
use nix::{Error, errno};
use std::cell::UnsafeCell;
use std::mem;

/// ArkFS uses the standard 64-bit checksum size for noncryptographic purposes.
const CHECKSUM_LEN: usize = 8;
/// Dirty nodes don't get checksummed until they're written to disk.  In the
/// meantime, their checksum field is blank.
const UNCHECKSUMMED: [u8; CHECKSUM_LEN] = [0, 0, 0, 0, 0, 0, 0, 0];

/// Anything that has a min_value method.  Too bad libstd doesn't define this.
pub trait MinValue {
    fn min_value() -> Self;
}

impl MinValue for u32 {
    fn min_value() -> Self {
        u32::min_value()
    }
}

enum BTreePtr<K: Copy + Ord, V: Copy> {
    /// Dirty btree nodes live only in RAM, not on disk or in cache
    Mem(Box<Node<K, V>>),
    /// Direct Block Pointers point directly to a disk location
    _DBP(ClusterT, LbaT),
    /// Indirect Block Pointers point to the Block Indirection Table
    _IBP(u64)
}

struct BTreeIntElem<K: Copy + Ord, V: Copy> {
    _checksum: [u8; CHECKSUM_LEN],
    key: K,
    /// lock is really protecting `ptr`.  However, Rust's borrow checker won't
    /// allow us to drop the lock while retaining any reference to the child,
    /// even though we can safely do so using lock-coupling.
    lock: RwLock<()>,
    ptr: UnsafeCell<BTreePtr<K, V>>
}

impl<'a, K: Copy + Ord + 'static, V: Copy + 'static> BTreeIntElem<K, V> {
    fn lookup(&'a self, parent_guard: RwLockReadGuard<()>,
                  k: K) -> Box<Future<Item=V, Error=Error> + 'a> {
        Box::new(self.lock.read()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |child_guard| {
                let child = unsafe{ self.ptr.get().as_mut()}.unwrap();
                // Drop the parent guard after locking the child.  This
                // implements deadlock-free lock coupling.
                drop(parent_guard);
                match *child {
                    BTreePtr::Mem(ref node) => node.lookup(child_guard, k),
                    _ => unimplemented!()
                }
            })
        )
    }
}

struct BTreeLeafElem<K: Copy + Ord, V: Copy> {
    key: K,
    value: V
}

/// COW B+-Tree interior node, in-memory version
struct IntNode<K: Copy + Ord, V: Copy> {
    children: Vec<BTreeIntElem<K, V>>
}

impl<'a, K: Copy + Ord + 'static, V: Copy + 'static> IntNode<K, V> {
    fn lookup(&'a self, parent_guard: RwLockReadGuard<()>,
                  k: K) -> Box<Future<Item=V, Error=Error> + 'a> {
        // Find the greatest key among children that is less than or equal to k
        let child_idx = self.children.iter()
            .enumerate()
            .filter(|&(_, c)| c.key > k )
            .nth(0)
            .map_or(self.children.len() - 1,
                    |(idx, _)| idx - 1);
        let child = &self.children[child_idx];
        child.lookup(parent_guard, k)
    }
}

/// COW B+-Tree leaf node, in-memory representation
struct LeafNode<K: Copy + Ord, V: Copy> {
    items: Vec<BTreeLeafElem<K, V>>
}

impl<K: Copy + Ord, V: Copy + 'static> LeafNode<K, V> {
    fn lookup(&self, _parent_guard: RwLockReadGuard<()>,
                  k: K) -> Box<Future<Item=V, Error=Error>> {
        Box::new(self.items.iter().filter(|c| c.key == k )
            .nth(0)
            .map(|elem| elem.value)
            .ok_or(Error::Sys(errno::Errno::ENOENT))
            .into_future()
        )
    }
}

enum Node<K: Copy + Ord, V: Copy> {
    _Int(IntNode<K, V>),
    Leaf(LeafNode<K, V>)
}

impl<'a, K: Copy + Ord + 'static, V: Copy + 'static> Node<K, V> {
    fn lookup(&'a self, self_guard: RwLockReadGuard<()>,
                  k: K) -> Box<Future<Item=V, Error=Error> + 'a> {
        match *self {
            Node::_Int(ref int) => int.lookup(self_guard, k),
            Node::Leaf(ref leaf) => leaf.lookup(self_guard, k)
        }
    }
}

/// In-memory representation of a COW B+-Tree
///
/// # Generic Parameters
///
/// *`K`:   Key type.  Must be ordered and copyable; should be compact
/// *`V`:   Value type in the leaves.
pub struct BTree<K: Copy + Ord, V: Copy> {
    /// Fanout parameter.  Every node except the root must contain [b, 2*b+1]
    /// entries.  The root may contain [0, 2b+1] entries.
    _b: usize,
    lock: RwLock<()>,
    root: UnsafeCell<BTreeIntElem<K, V>>
}

impl<'a, K: Copy + MinValue + Ord + 'static, V: Copy + 'static> BTree<K, V> {
    pub fn create() -> Self {
        // Choose b such that interior nodes can fit within one LBA
        // Assume 2 bytes of overhead for storing the number of keys, and 1 for
        // storing the type of node
        //
        // TODO: increase b by considering the on-disk size of K and P, which
        // may be less than the in-memory size.
        // TODO: increase by by considering on-disk compression.
        const OVERHEAD: usize = 3;
        let _b = (BYTES_PER_LBA - OVERHEAD) /
            (CHECKSUM_LEN + mem::size_of::<K>() + mem::size_of::<BTreePtr<K, V>>());
        let items: Vec<BTreeLeafElem<K, V>> = Vec::with_capacity(2 * _b + 1);
        let root_node: Box<Node<K, V>> = Box::new(Node::Leaf(LeafNode{items}));
        let rootelem = BTreeIntElem::<K, V> {
            _checksum: UNCHECKSUMMED,
            key: K::min_value(),
            lock: RwLock::new(()),
            ptr: UnsafeCell::new(BTreePtr::Mem(root_node))
        };
        BTree {_b, lock: RwLock::new(()), root: UnsafeCell::new(rootelem)}
    }

    pub fn delete() -> Self {
        unimplemented!();
    }

    pub fn insert(&self, _k: K, _v: V) {
        unimplemented!();
    }

    pub fn lookup(&'a self, k: K) -> Box<Future<Item=V, Error=Error> + 'a> {
        Box::new(self.lock.read()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |guard| {
                let root = unsafe{ self.root.get().as_ref() }.unwrap();
                root.lookup(guard, k)
            }
        ))
    }

    pub fn remove(&self, _k: K) -> Box<Future<Item=Option<V>, Error=Error>> {
        unimplemented!();
    }
}

#[cfg(test)]
mod t {

use super::*;
use tokio::executor::current_thread;

/// Test lookup through an artificially constructed BTree with internal nodes
#[test]
fn test_lookup() {
    let mut btree = BTree::<u32, u32>::create();
    let leaf = Box::new(Node::Leaf({
        LeafNode{items: vec![BTreeLeafElem{key: 5, value: 55}]}
    }));
    let intnode = Box::new(Node::_Int(IntNode{
        children: vec![BTreeIntElem{_checksum: UNCHECKSUMMED,
                                    key: 5,
                                    lock: RwLock::new(()),
                                    ptr: UnsafeCell::new(BTreePtr::Mem(leaf))}]}));
    btree.root = UnsafeCell::new(
        BTreeIntElem{_checksum: UNCHECKSUMMED,
                     key: 5,
                     lock: RwLock::new(()),
                     ptr: UnsafeCell::new(BTreePtr::Mem(intnode))});
    let v = current_thread::block_on_all(btree.lookup(5)).unwrap();
    assert_eq!(v, 55);
}

}
