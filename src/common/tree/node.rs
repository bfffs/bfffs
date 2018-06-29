// vim: tw=80

//! Nodes for Trees (private module)
use bincode;
use common::*;
use common::dml::*;
use futures_locks::*;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    borrow::Borrow,
    collections::{BTreeMap, VecDeque},
    fmt::Debug,
    mem,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    sync::{
        Arc,
    }
};


/// Anything that has a min_value method.  Too bad libstd doesn't define this.
pub trait MinValue {
    fn min_value() -> Self;
}

impl MinValue for u32 {
    fn min_value() -> Self {
        u32::min_value()
    }
}

impl MinValue for PBA {
    fn min_value() -> Self {
        PBA::new(ClusterT::min_value(), LbaT::min_value())
    }
}

impl MinValue for RID {
    fn min_value() -> Self {
        RID(u64::min_value())
    }
}

pub trait Addr: Copy + Debug + DeserializeOwned + Serialize + 'static {}

impl<T> Addr for T
where T: Copy + Debug + DeserializeOwned + Serialize + 'static {}

pub trait Key: Copy + Debug + DeserializeOwned + Ord + MinValue + Serialize
    + 'static {}

impl<T> Key for T
where T: Copy + Debug + DeserializeOwned + Ord + MinValue + Serialize
    + 'static {}

pub trait Value: Copy + Debug + DeserializeOwned + Serialize + 'static {}

impl<T> Value for T
where T: Copy + Debug + DeserializeOwned + Serialize + 'static {}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned, K: DeserializeOwned,
                             V: DeserializeOwned"))]
pub(super) enum TreePtr<A: Addr, K: Key, V: Value> {
    /// Dirty btree nodes live only in RAM, not on disk or in cache.  Being
    /// RAM-resident, we don't need to store their checksums or lsizes.
    #[cfg_attr(not(test), serde(skip_serializing))]
    #[cfg_attr(not(test), serde(skip_deserializing))]
    #[cfg_attr(test, serde(with = "node_serializer"))]
    Mem(Box<Node<A, K, V>>),
    /// DML Addresses point to a disk location
    Addr(A),
    /// Used temporarily while syncing nodes to disk.  Should never be visible
    /// during a traversal, because the parent's xlock must be held at all times
    /// while the ptr is None.
    None,
}

impl<A: Addr, K: Key, V: Value> TreePtr<A, K, V> {
    pub fn as_addr(&self) -> &A {
        if let TreePtr::Addr(addr) = self {
            addr
        } else {
            panic!("Not a TreePtr::A")    // LCOV_EXCL_LINE
        }
    }

    pub fn as_mem(&self) -> &Box<Node<A, K, V>> {
        if let TreePtr::Mem(mem) = self {
            mem
        } else {
            panic!("Not a TreePtr::Mem")    // LCOV_EXCL_LINE
        }
    }

    pub fn into_node(self) -> Box<Node<A, K, V>> {
        if let TreePtr::Mem(node) = self {
            node
        } else {
            panic!("Not a TreePtr::Mem")    // LCOV_EXCL_LINE
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.is_mem()
    }

    pub fn is_addr(&self) -> bool {
        if let TreePtr::Addr(_) = self {
            true
        } else {
            false
        }
    }

    pub fn is_mem(&self) -> bool {
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
    use serde::{Deserialize, de::Deserializer, Serializer};
    use tokio::runtime::current_thread;

    pub(super) fn deserialize<'de, A, DE, K, V>(deserializer: DE)
        -> Result<Box<Node<A, K, V>>, DE::Error>
        where A: Addr, DE: Deserializer<'de>, K: Key, V: Value
    {
        NodeData::deserialize(deserializer)
            .map(|node_data| Box::new(Node(RwLock::new(node_data))))
    }

    pub(super) fn serialize<A, S, K, V>(node: &Node<A, K, V>,
                                     serializer: S) -> Result<S::Ok, S::Error>
        where A: Addr, S: Serializer, K: Key, V: Value {

        let mut rt = current_thread::Runtime::new().unwrap();
        let guard = rt.block_on(node.0.read()).unwrap();
        (*guard).serialize(serializer)
    }
}
// LCOV_EXCL_STOP

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned, V: DeserializeOwned"))]
pub(super) struct LeafData<K: Key, V> {
    items: BTreeMap<K, V>
}

impl<K: Key, V: Value> LeafData<K, V> {
    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.items.insert(k, v)
    }

    pub fn get<Q>(&self, k: &Q) -> Option<V>
        where K: Borrow<Q>, Q: Ord
    {
        self.items.get(k).cloned()
    }

    pub fn new() -> Self {
        LeafData{items: BTreeMap::new()}
    }

    /// Lookup a range of values from a single Leaf Node.
    ///
    /// # Returns
    ///
    /// A `VecDeque` of partial results, and a bool.  If the bool is true, then
    /// there may be more results from other Nodes.  If false, then there will
    /// be no more results.
    pub fn range<R, T>(&self, range: R) -> (VecDeque<(K, V)>, bool)
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
    pub fn range_delete<R, T>(&mut self, range: R)
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

    pub fn remove<Q>(&mut self, k: &Q) -> Option<V>
        where K: Borrow<Q>, Q: Ord
    {
        self.items.remove(k)
    }

    pub fn split(&mut self) -> (K, LeafData<K, V>) {
        // Split the node in two.  Make the left node larger, on the assumption
        // that we're more likely to insert into the right node than the left
        // one.
        let half = div_roundup(self.items.len(), 2);
        let cutoff = *self.items.keys().nth(half).unwrap();
        let new_items = self.items.split_off(&cutoff);
        (cutoff, LeafData{items: new_items})
    }
}

/// Guard that holds the Node lock object for reading
pub(super) enum TreeReadGuard<A: Addr, K: Key, V: Value> {
    Mem(RwLockReadGuard<NodeData<A, K, V>>),
    Addr(RwLockReadGuard<NodeData<A, K, V>>, Box<Arc<Node<A, K, V>>>)
}

impl<A: Addr, K: Key, V: Value> Deref for TreeReadGuard<A, K, V> {
    type Target = NodeData<A, K, V>;

    fn deref(&self) -> &Self::Target {
        match self {
            TreeReadGuard::Mem(guard) => &**guard,
            TreeReadGuard::Addr(guard, _) => &**guard,
        }
    }
}

/// Guard that holds the Node lock object for writing
pub(super) enum TreeWriteGuard<A: Addr, K: Key, V: Value> {
    Mem(RwLockWriteGuard<NodeData<A, K, V>>),
    Addr(RwLockWriteGuard<NodeData<A, K, V>>, Box<Arc<Node<A, K, V>>>)
}

impl<A: Addr, K: Key, V: Value> Deref for TreeWriteGuard<A, K, V> {
    type Target = NodeData<A, K, V>;

    fn deref(&self) -> &Self::Target {
        match self {
            TreeWriteGuard::Mem(guard) => &**guard,
            TreeWriteGuard::Addr(_, _) => unreachable!( // LCOV_EXCL_LINE
                "Can only write to in-memory Nodes")
        }
    }
}

impl<A: Addr, K: Key, V: Value> DerefMut for TreeWriteGuard<A, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            TreeWriteGuard::Mem(guard) => &mut **guard,
            TreeWriteGuard::Addr(_, _) => unreachable!( // LCOV_EXCL_LINE
                "Can only write to in-memory Nodes")
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
pub(super) struct IntElem<A: Addr, K: Key + DeserializeOwned, V: Value> {
    pub key: K,
    pub ptr: TreePtr<A, K, V>
}

impl<'a, A: Addr, K: Key, V: Value> IntElem<A, K, V> {
    /// Is the child node dirty?  That is, does it differ from the on-disk
    /// version?
    pub fn is_dirty(&mut self) -> bool {
        self.ptr.is_dirty()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned, K: DeserializeOwned"))]
pub(super) struct IntData<A: Addr, K: Key, V: Value> {
    pub children: Vec<IntElem<A, K, V>>
}

impl<A: Addr, K: Key, V: Value> IntData<A, K, V> {
    /// How many children does this node have?
    pub fn nchildren(&self) -> usize {
        self.children.len()
    }

    /// Find index of rightmost child whose key is less than or equal to k
    pub fn position<Q>(&self, k: &Q) -> usize
        where K: Borrow<Q>, Q: Ord
    {
        self.children
            .binary_search_by(|child| child.key.borrow().cmp(k))
            .unwrap_or_else(|k| if k == 0 {k} else {k - 1})
    }

    pub fn split(&mut self) -> (K, IntData<A, K, V>) {
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
pub(super) enum NodeData<A: Addr, K: Key, V: Value> {
    Leaf(LeafData<K, V>),
    Int(IntData<A, K, V>),
}

impl<A: Addr, K: Key, V: Value> NodeData<A, K, V> {
    pub fn as_int(&self) -> &IntData<A, K, V> {
        if let NodeData::Int(int) = self {
            int
        } else {
            panic!("Not a NodeData::Int")   // LCOV_EXCL_LINE
        }
    }

    pub fn as_int_mut(&mut self) -> &mut IntData<A, K, V> {
        if let NodeData::Int(int) = self {
            int
        } else {
            panic!("Not a NodeData::Int")   // LCOV_EXCL_LINE
        }
    }

    #[cfg(test)]
    pub fn as_leaf(&self) -> &LeafData<K, V> {
        if let NodeData::Leaf(leaf) = self {
            leaf
        } else {
            panic!("Not a NodeData::Leaf")  // LCOV_EXCL_LINE
        }
    }

    pub fn as_leaf_mut(&mut self) -> &mut LeafData<K, V> {
        if let NodeData::Leaf(leaf) = self {
            leaf
        } else {
            panic!("Not a NodeData::Leaf")  // LCOV_EXCL_LINE
        }
    }

    pub fn is_leaf(&self) -> bool {
        if let NodeData::Leaf(_) = self {
            true
        } else {
            false
        }
    }

    /// Can this child be merged with `other` without violating constraints?
    pub fn can_merge(&self, other: &NodeData<A, K, V>, max_fanout: usize) -> bool {
        self.len() + other.len() <= max_fanout
    }

    /// Return this `NodeData`s lower bound key, suitable for use in its
    /// parent's `children` array.
    pub fn key(&self) -> &K {
        match self {
            NodeData::Leaf(ref leaf) => leaf.items.keys().nth(0).unwrap(),
            NodeData::Int(ref int) => &int.children[0].key,
        }
    }

    /// Number of children or items in this `NodeData`
    pub fn len(&self) -> usize {
        match self {
            NodeData::Leaf(leaf) => leaf.items.len(),
            NodeData::Int(int) => int.children.len()
        }
    }

    /// Is this node currently underflowing?
    pub fn underflow(&self, min_fanout: usize) -> bool {
        let len = self.len();
        len <= min_fanout
    }

    /// Should this node be split because it's too big?
    pub fn should_split(&self, max_fanout: usize) -> bool {
        let len = self.len();
        debug_assert!(len <= max_fanout,
                      "Overfull nodes shouldn't be possible");
        len >= max_fanout
    }

    pub fn split(&mut self) -> (K, NodeData<A, K, V>) {
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
    pub fn merge(&mut self, mut other: TreeWriteGuard<A, K, V>) {
        match self {
            NodeData::Int(int) =>
                int.children.append(&mut other.as_int_mut().children),
            NodeData::Leaf(leaf) =>
                leaf.items.append(&mut other.as_leaf_mut().items),
        }
    }

    /// Take `other`'s highest keys and merge them into ourself
    pub fn take_high_keys(&mut self, other: &mut NodeData<A, K, V>) {
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
    pub fn take_low_keys(&mut self, other: &mut NodeData<A, K, V>) {
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

impl<A: Addr, K: Key, V: Value> Cacheable for Arc<Node<A, K, V>> {
    fn deserialize(dbs: DivBufShared) -> Self where Self: Sized {
        let db = dbs.try().unwrap();
        let node_data: NodeData<A, K, V> = bincode::deserialize(&db[..]).unwrap();
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

impl<A: Addr, K: Key, V: Value> CacheRef for Arc<Node<A, K, V>> {
    fn deserialize(dbs: DivBufShared) -> Box<Cacheable> where Self: Sized {
        let db = dbs.try().unwrap();
        let node_data: NodeData<A, K, V> = bincode::deserialize(&db[..]).unwrap();
        let node = Arc::new(Node(RwLock::new(node_data)));
        Box::new(node)
    }

    fn to_owned(self) -> Box<Cacheable> {
        Box::new(self)
    }
}
#[derive(Debug)]
pub(in common) struct Node<A, K, V> (pub(super) RwLock<NodeData<A, K, V>>)
    where A: Addr, K: Key, V: Value;

impl<A: Addr, K: Key, V: Value> Node<A, K, V> {
    pub(super) fn new(node_data: NodeData<A, K, V>) -> Self {
        Node(RwLock::new(node_data))
    }
}

// LCOV_EXCL_START
/// Tests for serialization/deserialization of Nodes
#[cfg(test)]
mod serialization {

use common::ddml::DRP;
use std::ops::Deref;
use super::*;

#[test]
fn deserialize_int() {
    let serialized = DivBufShared::from(vec![
        1u8, 0, 0, 0, // enum variant 0 for IntNode
        2, 0, 0, 0, 0, 0, 0, 0,     // 2 elements in the vector
           0, 0, 0, 0,              // K=0
           1u8, 0, 0, 0,            // enum variant 1 for TreePtr::Addr
               0, 0,                // Cluster 0
               0, 0, 0, 0, 0, 0, 0, 0,  // LBA 0
           0, 0, 0, 0,              // enum variant 0 for Compression::None
           0x40, 0x9c, 0, 0,        // lsize=40000
           0x40, 0x9c, 0, 0,         // csize=40000
           0xef, 0xbe, 0xad, 0xde, 0, 0, 0, 0,  // checksum
           0, 1, 0, 0,              // K=256
           1u8, 0, 0, 0,            // enum variant 1 for TreePtr::Addr
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
    let node: Arc<Node<DRP, u32, u32>> = Cacheable::deserialize(serialized);
    let guard = node.0.try_read().unwrap();
    let int_data = guard.deref().as_int();
    assert_eq!(int_data.children.len(), 2);
    assert_eq!(int_data.children[0].key, 0);
    assert_eq!(*int_data.children[0].ptr.as_addr(), drp0);
    assert_eq!(int_data.children[1].key, 256);
    assert_eq!(*int_data.children[1].ptr.as_addr(), drp1);
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
    let node: Arc<Node<DRP, u32, u32>> = Cacheable::deserialize(serialized);
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
           1u8, 0, 0, 0,            // enum variant 1 for TreePtr::Addr
               0, 0,                // Cluster 0
               0, 0, 0, 0, 0, 0, 0, 0,  // LBA 0
           0, 0, 0, 0,              // enum variant 0 for Compression::None
           0x40, 0x9c, 0, 0,        // lsize=40000
           0x40, 0x9c, 0, 0,         // csize=40000
           0xef, 0xbe, 0xad, 0xde, 0, 0, 0, 0,  // checksum
           0, 1, 0, 0,              // K=256
           1u8, 0, 0, 0,            // enum variant 1 for TreePtr::Addr
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
        IntElem{key: 0u32, ptr: TreePtr::Addr(drp0)},
        IntElem{key: 256u32, ptr: TreePtr::Addr(drp1)},
    ];
    let node_data = NodeData::Int(IntData{children});
    let node: Node<DRP, u32, u32> = Node(RwLock::new(node_data));
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
    let node: Arc<Node<DRP, u32, u32>> =
        Arc::new(Node(RwLock::new(NodeData::Leaf(LeafData{items}))));
    let (db, _dbs) = node.serialize();
    assert_eq!(&expected[..], &db[..]);
    drop(db);
}

}
// LCOV_EXCL_STOP

