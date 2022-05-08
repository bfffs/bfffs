// vim: tw=80

//! Nodes for Trees (private module)
use crate::{
    dml::{Cacheable, CacheRef, DML},
    types::*,
    util::*,
    writeback::Credit
};
use divbuf::{DivBuf, DivBufShared};
use futures::{
    Future,
    FutureExt,
    TryFutureExt,
    TryStreamExt,
    future,
    stream::{FuturesOrdered, FuturesUnordered}
};
use futures_locks::*;
use serde::{
    ser::{Serialize, Serializer},
    de::{self, Deserialize, DeserializeOwned, Deserializer, SeqAccess, Visitor},
    ser::SerializeStruct,
};
use serde_derive::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    cmp::max,
    collections::{BTreeMap, VecDeque},
    fmt::{self, Debug},
    marker::PhantomData,
    mem,
    ops::{Bound, Deref, DerefMut, Range, RangeBounds},
    pin::Pin,
    sync::{
        Arc,
    }
};

use tracing::instrument;
use tracing_futures::Instrument;

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

impl MinValue for TxgT {
    fn min_value() -> Self {
        TxgT(u32::min_value())
    }
}

pub trait Addr: Copy + Debug + DeserializeOwned + Eq + Ord + PartialEq + Send +
    Sync + Serialize + TypicalSize + 'static {}

impl<T> Addr for T
where T: Copy + Debug + DeserializeOwned + Eq + Ord + PartialEq + Send +
    Sync + Serialize + TypicalSize + 'static {}

pub trait Key: Copy + Debug + DeserializeOwned + Ord + PartialEq + MinValue +
    Send + Sync + Serialize + TypicalSize + 'static
{
    /// Do write operations in trees that use this Key type require writeback
    /// credit?
    // This is bit of a hack; whether or not to use writeback credit is really a
    // function of how the tree is used, not what type of key it uses.  But
    // it's easier to discriminate based on Key rather than by adding
    // an additional generic parameter all the way up the stack.
    const USES_CREDIT: bool = true;
}

impl Key for PBA {
    const USES_CREDIT: bool = false;
}

impl Key for RID {
    const USES_CREDIT: bool = false;
}

#[cfg(test)]
impl Key for u32 {}

pub trait Value: Clone + Debug + DeserializeOwned + PartialEq + Send + Sync +
    Serialize + TypicalSize + 'static
{
    /// Does this Value type require flushing?
    const NEEDS_FLUSH: bool = false;

    /// How much allocated space does this object own, excluding the object
    /// itself?
    fn allocated_space(&self) -> usize {
        0
    }

    /// Clone this Value on disk.
    ///
    /// `Clone::clone` merely clones the value in RAM.  This method clones it on
    /// disk.  That is, it adds a reference to the on-disk structure.
    fn dclone<D>(&self, _dml: &D, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
        where D: DML + 'static, D::Addr: 'static
    {
        // should never be called since NEEDS_FLUSH is false.  Ideally, this
        // entire function should go away once generic specialization is stable
        // https://github.com/rust-lang/rust/issues/31844
        unreachable!()
    }

    /// Drop a reference to the on-disk data.
    fn ddrop<D>(&self, _dml: &D, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
        where D: DML + 'static, D::Addr: 'static
    {
        // should never be called since NEEDS_FLUSH is false.  Ideally, this
        // entire function should go away once generic specialization is stable
        // https://github.com/rust-lang/rust/issues/31844
        unreachable!()
    }

    /// Prepare this `Value` to be written to disk
    // LCOV_EXCL_START   unreachable code
    fn flush<D>(self, _dml: &D, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Self>> + Send>>
        where D: DML + 'static, D::Addr: 'static
    {
        // should never be called since needs_flush is false.  Ideally, this
        // entire function should go away once generic specialization is stable
        // https://github.com/rust-lang/rust/issues/31844
        unreachable!()
    }

    /// Drop a reference to the on-disk data, and read it into `self`, like the
    /// opposite of [`flush`].
    fn dpop<D>(self, _dml: &D, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Self>> + Send>>
        where D: DML + 'static, D::Addr: 'static
    {
        // should never be called since needs_flush is false.  Ideally, this
        // entire function should go away once generic specialization is stable
        // https://github.com/rust-lang/rust/issues/31844
        unreachable!()
    }

    // LCOV_EXCL_STOP
}

impl Value for RID {}
#[cfg(test)] impl Value for f32 {}
#[cfg(test)] impl Value for u32 {}

/// Uniquely identifies any Node in the Tree.
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct NodeId<K: Key> {
    /// Tree level of the Node.  Leaves are 0.
    pub(super) height: u8,
    /// Less than or equal to the Node's first child/item.  Greater than the
    /// previous Node's last child/item.
    pub(super) key: K
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned, K: DeserializeOwned,
                             V: DeserializeOwned"))]
pub enum TreePtr<A: Addr, K: Key, V: Value> {
    /// DML Addresses point to a disk location
    // This is the only variant that gets serialized, so put it first.  That
    // gives it discriminant 0, which is the most compressible.
    Addr(A),
    /// Used temporarily while syncing nodes to disk.  Should never be visible
    /// during a traversal, because the parent's xlock must be held at all times
    /// while the ptr is None.
    #[serde(skip_serializing)]
    None,
    /// Dirty btree nodes live only in RAM, not on disk or in cache.  Being
    /// RAM-resident, we don't need to store their checksums or lsizes.
    #[serde(with = "node_serializer")]
    Mem(Box<Node<A, K, V>>),
}

impl<A: Addr, K: Key, V: Value> TreePtr<A, K, V> {
    pub fn as_addr(&self) -> &A {
        if let TreePtr::Addr(addr) = self {
            addr
        } else {
            panic!("Not a TreePtr::A")    // LCOV_EXCL_LINE
        }
    }

    pub fn as_mem(&self) -> &Node<A, K, V> {
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
        matches!(self, TreePtr::Addr(_))
    }

    pub fn is_mem(&self) -> bool {
        matches!(self, TreePtr::Mem(_))
    }

    /// Remove and return the inner `TreePtr::Mem` member.
    ///
    /// # Panics
    ///
    /// Panics if `self` is not a `TreePtr::Mem`.
    pub fn take(&mut self) -> Box<Node<A, K, V>> {
        mem::replace(self, TreePtr::None).into_node()
    }
}

impl<A: Addr, K: Key, V: Value> PartialEq  for TreePtr<A, K, V> {
    fn eq(&self, other: &TreePtr<A, K, V>) -> bool {
        match (self, other) {
            (TreePtr::Addr(a1), TreePtr::Addr(a2)) => a1 == a2,
            (TreePtr::None, TreePtr::None) => true,
            (TreePtr::Mem(_), _) | (_, TreePtr::Mem(_)) => 
                panic!("Can't compare Nodes recursively"),
            _ => false
        }
    }
}

mod node_serializer {
    use futures_locks::RwLock;
    use serde::{Deserialize, de::Deserializer, Serialize, Serializer};
    use super::{Addr, Key, Node, NodeData, Value};

    pub(super) fn deserialize<'de, A, DE, K, V>(deserializer: DE)
        -> Result<Box<Node<A, K, V>>, DE::Error>
        where A: Addr, DE: Deserializer<'de>, K: Key, V: Value
    {
        NodeData::deserialize(deserializer)
            .map(|node_data| Box::new(Node(RwLock::new(node_data))))
    }

    pub(super) fn serialize<A, S, K, V>(node: &Node<A, K, V>,
                                     serializer: S) -> Result<S::Ok, S::Error>
        where A: Addr, S: Serializer, K: Key, V: Value
    {
        let guard = node.0.try_read().unwrap();
        (*guard).serialize(serializer)
    }
}

#[derive(Debug)]
pub struct LeafData<K: Key, V> {
    /// WriteBack credit, if this `LeafData` is dirty.  Anytime the node is
    /// dirty and not exclusively locked, this should be at least as great as
    /// `wb_space()`.
    // Is there any way to eliminate this based on K::USES_CREDIT?
    // TODO: don't serialize credit for Bincode.  For YAML, need to decide if
    // it's useful enough in the unit tests.
    credit: Credit,
    #[cfg(test)] pub(super) items: BTreeMap<K, V>,
    #[cfg(not(test))] items: BTreeMap<K, V>,
}

impl<K: Key, V: Value> LeafData<K, V> {
    /// Absorb as much credit as this LeafData needs.  Call this after
    /// deserializing a `LeafData` into a dirty state.
    pub fn acredit(&mut self, credit: &mut Credit) {
        debug_assert_eq!(self.credit, 0,
            "Attempting to acredit an already accredited leaf node");
        if K::USES_CREDIT {
            let need = self.wb_space();
            assert!(*credit >= need,
                    "Insufficient credit to xlock leaf node");
            self.credit = credit.split(need);
        }
    }

    fn assert_accredited(&self) {
        if K::USES_CREDIT {
            debug_assert_eq!(self.credit, self.wb_space(),
                "Invariant violation: inconsistent node credit");
        }
    }

    /// Flush all items to stable storage.
    ///
    /// For most item types, this is a nop.
    pub fn flush<A, D>(mut self, d: &D, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<(Self, Credit)>> + Send>>
        where D: DML<Addr=A> + 'static, A: 'static
    {
        let credit = self.credit.take();
        if V::NEEDS_FLUSH {
            self.items.into_iter().map(|(k, v)| {
                v.flush(d, txg)
                .map_ok(move |v| (k, v))
            }).collect::<FuturesOrdered<_>>()
            .try_collect::<Vec<_>>()
            .map_ok(|items| {
                let ld = LeafData{
                    credit: Credit::null(),
                    items: items.into_iter().collect()
                };
                (ld, credit)
            }).boxed()
        } else {
            future::ok((self, credit)).boxed()
        }
    }

    /// Discard the contents of this `LeafData` without flushing to disk.
    pub fn forget(mut self) -> Credit {
        self.credit.take()
    }

    /// Insert one key-value pair into the LeafData, returning the old value, if
    /// any.  Also, return any excess credit that was provided.
    pub fn insert<A, D>(
        &mut self,
        k: K,
        v: V,
        txg: TxgT,
        dml: &D,
        mut credit: Credit)
        -> impl Future<Output = (Result<Option<V>>, Credit)> + Send
        where D: DML<Addr=A> + 'static, A: 'static
    {
        self.assert_accredited();
        let v_space = v.allocated_space();
        let old_v = self.items.insert(k, v);
        let excess_credit = if K::USES_CREDIT {
            let kvs = mem::size_of::<(K, V)>();
            let old_space = old_v.as_ref()
                .map(|v| v.allocated_space() + kvs)
                .unwrap_or(0);
            let excess = (&mut credit + old_space).checked_sub(v_space + kvs)
                .expect("insufficient credit was provided for this insertion");
            self.credit.extend(credit);
            self.credit.split(excess)
        } else {
            debug_assert!(credit.is_null());
            Credit::null()
        };
        self.assert_accredited();
        // Return excess credit, which might've resulted from an insertion
        // to an already-dirty record.
        if V::NEEDS_FLUSH {
            if let Some(v) = old_v {
                return v.dpop(dml, txg)
                    .map(move |r| (Some(r).transpose(), excess_credit))
                    .boxed()
            }
        }
        future::ready((Ok(old_v), excess_credit)).boxed()
    }

    pub fn get<Q>(&self, k: &Q) -> Option<V>
        where K: Borrow<Q>, Q: Ord
    {
        self.items.get(k).cloned()
    }

    pub fn last_key(&self) -> Option<K> {
        self.items.keys().next_back().cloned()
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
        if let Some(l) = self.items.keys().next_back() {
            let more = !matches!(range.end_bound(),
                Bound::Included(i) | Bound::Excluded(i) if i <= l.borrow());
            let items = self.items.range(range)
                .map(|(k, v)| (*k, v.clone()))
                .collect::<VecDeque<(K, V)>>();
            (items, more)
        } else {
            (VecDeque::new(), false)
        }
    }

    /// Like [`range_delete`], but for nodes that may be dirty yet unaccredited.
    pub fn range_delete_unaccredited<D, R, T>(
        &mut self,
        dml: &D,
        txg: TxgT,
        range: R) -> impl Future<Output=Result<Credit>> + Send
        where D: DML + 'static,
              K: Borrow<T>,
              R: RangeBounds<T>,
              T: Ord + Clone
    {
        // TODO: use BTreeMap::drain_filter, when that feature stabilizes.
        // https://github.com/rust-lang/rust/issues/70530
        let keys = self.items.range(range)
            .map(|(k, _)| *k)
            .collect::<Vec<K>>();
        let l = keys.len();
        let fut = FuturesUnordered::new();
        let mut allocated_space = 0;
        for k in keys.into_iter() {
            let v = self.items.remove(k.borrow()).unwrap();
            allocated_space += v.allocated_space();
            if V::NEEDS_FLUSH {
                fut.push(v.ddrop::<D>(dml, txg));
            }
        }
        let kvs = mem::size_of::<(K, V)>();
        let credit = self.credit.split(allocated_space + l * kvs);
        self.assert_accredited();
        fut.try_fold((), |_, _| future::ok(()))
            .map_ok(move |_| credit)
    }

    /// Delete all keys within the given range, possibly leaving an empty
    /// LeafNode.
    pub fn range_delete<D, R, T>(&mut self, dml: &D, txg: TxgT, range: R) ->
        impl Future<Output=Result<Credit>> + Send
        where D: DML + 'static,
              K: Borrow<T>,
              R: RangeBounds<T>,
              T: Ord + Clone
    {
        self.assert_accredited();
        self.range_delete_unaccredited(dml, txg, range)
    }

    pub async fn remove<D, Q>(
        &mut self,
        dml: &D,
        txg: TxgT,
        k: &Q
    ) -> Result<(Option<V>, Credit)>
        where D: DML + 'static,
              K: Borrow<Q>, Q: Ord
    {
        self.assert_accredited();
        let old_v = self.items.remove(k);
        let credit = if K::USES_CREDIT {
            let old_space = old_v.as_ref()
                .map(|v| v.allocated_space() + mem::size_of::<(K, V)>())
                .unwrap_or(0);
            self.credit.split(old_space)
        } else {
            Credit::null()
        };
        self.assert_accredited();
        if V::NEEDS_FLUSH {
            if let Some(v) = old_v {
                return Ok((Some(v.dpop(dml, txg).await?), credit));
            }
        }
        Ok((old_v, credit))
    }

    /// Split this LeafNode in two.  Returns the transaction range of the rump
    /// node, and a new IntElem containing the new node.
    pub fn split<A: Addr>(&mut self, left_items: usize, txg: TxgT)
        -> (Range<TxgT>, IntElem<A, K, V>)
    {
        self.assert_accredited();
        let cutoff = *self.items.keys().nth(left_items).unwrap();
        let new_items = self.items.split_off(&cutoff);
        let mut ld = LeafData {
            credit: Credit::null(),
            items: new_items
        };
        if K::USES_CREDIT {
            ld.credit.extend(self.credit.split(ld.wb_space()));
        }
        self.assert_accredited();
        ld.assert_accredited();
        // There are no children, so the TXG range is just the current TXG
        let txgs = txg..txg + 1;
        let node = Box::new(Node::new(NodeData::Leaf(ld)));
        (txgs.clone(), IntElem::new(cutoff, txgs, TreePtr::Mem(node)))
    }

    fn wb_space(&self) -> usize {
        // Unlike cache_space, we need to be able to update the credit when
        // adding and removing individual items.  So we simplify the
        // calculation, trading accuracy for mutatability.
        let allocated: usize = self.items.iter()
            .map(|(_k, v)| v.allocated_space())
            .sum();
        let kvs = self.items.len() * mem::size_of::<(K, V)>();
        allocated + kvs
    }
}

impl<K: Key, V: Value> Default for LeafData<K, V> {
    fn default() -> Self {
        LeafData{credit: Credit::null(), items: BTreeMap::new()}
    }
}

impl<K: Key, V: Value> PartialEq for LeafData<K, V> {
    fn eq(&self, other: &Self) -> bool {
        // Ignore credit, for purposes of equality testing.
        // It's not considered part of the content, and it should always be None
        // for cached LeafData anyway.
        self.items == other.items
    }
}

impl<'de, K: Key, V: Value> Deserialize<'de> for LeafData<K, V> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field { Credit, Items }

        struct LeafDataVisitor<K: Key, V: Value> {
            _k: PhantomData<K>,
            _v: PhantomData<V>
        }
        impl<'de, K: Key, V: Value> Visitor<'de> for LeafDataVisitor<K, V> {
            type Value = LeafData<K, V>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct LeafData")
            }

            fn visit_seq<SV>(self, mut seq: SV)
                -> std::result::Result<Self::Value, SV::Error>
                where SV: SeqAccess<'de>
            {
                // The only support serializer that uses a Seq is Bincode, where
                // we don't serialize Credit
                let items = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let credit = Credit::null();
                Ok(LeafData{credit, items})
            }

            #[cfg(test)]
            fn visit_map<SV>(self, mut map: SV)
                -> std::result::Result<Self::Value, SV::Error>
                where SV: de::MapAccess<'de>
            {
                // The only support serializer that uses a Map is YAML, for unit
                // tests and Tree::dump, where we do serialize Credit.
                let mut credit = Credit::null();
                let mut items = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Credit => {
                            if !credit.is_null() {
                                return Err(
                                    de::Error::duplicate_field("credit")
                                );
                            }
                            credit = map.next_value()?;
                        },
                        Field::Items => {
                            if items.is_some() {
                                return Err(de::Error::duplicate_field("items"));
                            }
                            items = Some(map.next_value()?);
                        }
                    }
                }
                let items = items.ok_or_else(
                    || de::Error::missing_field("items")
                )?;
                let ld = LeafData{ credit, items };
                if K::USES_CREDIT {
                    assert_eq!(ld.credit, ld.wb_space(),
                        "Insufficient credit was present in the YAML tree");
                }
                Ok(ld)
            }
        }

        const FIELDS: &[&str] = &["credit", "items"];
        let visitor = LeafDataVisitor{_k: PhantomData, _v: PhantomData};
        deserializer.deserialize_struct("LeafData", FIELDS, visitor)
    }
}

impl<K: Key, V: Value> Serialize for LeafData<K, V> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where S: Serializer
    {
        if serializer.is_human_readable() {
            // When dumping dirty nodes to YAML, print the crecit
            let mut ss = serializer.serialize_struct("LeafData", 2)?;
            ss.serialize_field("credit", &self.credit)?;
            ss.serialize_field("items", &self.items)?;
            ss.end()
        } else {
            // But for Bincode, omit it, because it should always be 0.
            let mut ss = serializer.serialize_struct("LeafData", 1)?;
            ss.serialize_field("items", &self.items)?;
            ss.end()
        }
    }
}

/// Node size limits
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[cfg_attr(test, derive(Default))]
pub struct Limits {
    /// Minimum interior node fanout.  Smaller nodes will be merged, or will
    /// steal children from their neighbors.
    // Can't combine min_leaf_fanout with max_leaf_fanout in a Range, because
    // Range isn't Copy.
    min_int_fanout: u16,
    /// Maximum interior node fanout.  Larger nodes will be split.
    // Rodeh states that the minimum value of max_fanout that can guarantee
    // invariants is 2 * min_fanout + 1.  However, range_delete can be slightly
    // simplified if max_fanout is at least 2 * min_fanout + 4.  That would mean
    // that fix_int can always either merge two nodes, or steal 2 nodes.
    max_int_fanout: u16,
    min_leaf_fanout: u16,
    max_leaf_fanout: u16,
    /// Maximum node size in bytes.  Larger nodes will be split or their message
    /// buffers flushed
    _max_size: u64,
}

impl Limits {
    /// The maximum fanout that will be used for either leaf or int nodes
    pub(super) fn max_fanout(&self) -> u16 {
        max(self.max_leaf_fanout, self.max_int_fanout)
    }

    /// The largest number of of children a leaf node may ever have
    pub(super) fn max_leaf_fanout(&self) -> u16 {
        self.max_leaf_fanout
    }

    /// Construct a new fanout from inclusive limits
    pub(super) fn new(min_int_fanout: u16, max_int_fanout: u16,
                      min_leaf_fanout: u16, max_leaf_fanout: u16) -> Self
    {
        let _max_size = 1<<22;    // BetrFS's max size
        Limits {
            min_int_fanout,
            max_int_fanout,
            min_leaf_fanout,
            max_leaf_fanout,
            _max_size
        }
    }
}

/// Guard that holds the Node lock object for reading
pub enum TreeReadGuard<A: Addr, K: Key, V: Value> {
    Mem(RwLockReadGuard<NodeData<A, K, V>>),
    Addr(RwLockReadGuard<NodeData<A, K, V>>, Arc<Node<A, K, V>>)
}

impl<A: Addr, K: Key, V: Value> TreeReadGuard<A, K, V> {
    pub(super) fn is_mem(&self) -> bool {
        matches!(self, TreeReadGuard::Mem(_))
    }
}

impl<A: Addr, K: Key, V: Value> Deref for TreeReadGuard<A, K, V> {
    type Target = NodeData<A, K, V>;

    fn deref(&self) -> &Self::Target {
        match self {
            TreeReadGuard::Mem(guard) => &*guard,
            TreeReadGuard::Addr(guard, _) => &*guard,
        }
    }
}

/// Guard that holds the Node lock object for writing
pub struct TreeWriteGuard<A: Addr, K: Key, V: Value>(
    pub(super) RwLockWriteGuard<NodeData<A, K, V>>
);

impl<A: Addr, K: Key, V: Value> TreeWriteGuard<A, K, V> {
    /// Lock the indicated child exclusively.  If it is not already resident
    /// in memory, then COW the target node.  Return both the original guard and
    /// the child's guard, and any credit leftover after dirtying the child.
    // Consuming and returning self prevents lifetime checker issues that
    // interfere with lock coupling.
    pub fn xlock<D>(
        mut self,
        dml: &Arc<D>,
        child_idx: usize,
        txg: TxgT,
        mut credit: Credit
    ) -> Pin<Box<dyn Future<
            Output=Result<
                (TreeWriteGuard<A, K, V>, TreeWriteGuard<A, K, V>, Credit)>
            > + Send
        >>
        where D: DML<Addr=A> + 'static
    {
        self.as_int_mut().children[child_idx].txgs.end = txg + 1;
        if self.as_int().children[child_idx].ptr.is_mem() {
            self.as_int().children[child_idx].ptr.as_mem().xlock()
                .map(move |child_guard| {
                      Ok((self, child_guard, credit))
                 }).boxed()
        } else {
            let addr = *self.as_int()
                            .children[child_idx]
                            .ptr
                            .as_addr();
            let afut = dml.pop::<Arc<Node<A, K, V>>, Arc<Node<A, K, V>>>(&addr,
                                                                  txg);
            async move {
                let arc = afut.await?;
                let mut child_guard = arc.xlock().await;
                if child_guard.is_leaf() {
                    let ld = child_guard.as_leaf_mut();
                    ld.acredit(&mut credit);
                }
                drop(child_guard);
                let child_node = Arc::try_unwrap(*arc)
                    .expect("We should be the Node's only owner");
                let child_guard = {
                    let elem = &mut self.as_int_mut().children[child_idx];
                    elem.ptr = TreePtr::Mem(Box::new(child_node));
                    let guard = TreeWriteGuard(
                        elem.ptr.as_mem().0.try_write().unwrap()
                    );
                    elem.txgs.start = match *guard {
                        NodeData::Int(ref id) => id.start_txg(),
                        NodeData::Leaf(_) => txg
                    };
                    guard
                };
                Ok((self, child_guard, credit))
            }.boxed()
        }
    }

    /// Like [`xlock`](#method.xlock) but without using lock-coupling
    ///
    /// Lock the indicated child exclusively.  If it is not already resident
    /// in memory, then COW the target node.  Return both the child's guard and
    /// a new IntElem that points to it, if it's different from the old IntElem.
    /// The caller _must_ replace the old IntElem with the new one, or data will
    /// leak!  `height` is the height of `self`, not the target.  Leaves are 0.
    pub fn xlock_nc<D>(
        &mut self,
        dml: &Arc<D>,
        child_idx: usize,
        height: u8,
        txg: TxgT,
        mut credit: Credit
    ) -> Pin<Box<dyn Future<
            Output=Result<
                (Option<IntElem<A, K, V>>, TreeWriteGuard<A, K, V>, Credit)>
        > + Send >>
        where D: DML<Addr=A> + 'static
    {
        self.as_int_mut().children[child_idx].txgs.end = txg + 1;
        if self.as_int().children[child_idx].ptr.is_mem() {
            if height == 1 {
                self.as_int_mut().children[child_idx].txgs.start = txg;
            }
            self.as_int().children[child_idx].ptr.as_mem().xlock()
            .map(move |child_guard| {
                debug_assert!((height > 1) ^ child_guard.is_leaf());
                Ok((None, child_guard, credit))
            }).boxed()
        } else {
            let addr = *self.as_int().children[child_idx].ptr.as_addr();
            let afut = dml.pop::<Arc<Node<A, K, V>>, Arc<Node<A, K, V>>>(&addr,
                                                                         txg);
            async move {
                let arc = afut.await?;
                let mut child_guard = arc.xlock().await;
                if child_guard.is_leaf() {
                    let ld = child_guard.as_leaf_mut();
                    ld.acredit(&mut credit);
                }
                drop(child_guard);
                let child_node = Box::new(Arc::try_unwrap(*arc)
                    .expect("We should be the Node's only owner"));
                let guard = TreeWriteGuard(
                    child_node.0.try_write().unwrap()
                );
                let ptr = TreePtr::Mem(child_node);
                let start = match *guard {
                    NodeData::Int(ref id) => id.start_txg(),
                    NodeData::Leaf(_) => txg
                };
                let end = txg + 1;
                let elem = IntElem::new(*guard.key(), start..end, ptr);
                Ok((Some(elem), guard, credit))
            }.boxed()
        }
    }

    /// Remove the indicated children from the node and apply a function to
    /// each.
    ///
    /// If they are not already resident in memory, then COW the child nodes.
    /// Return both the original guard and the function's results.
    ///
    /// This method is unsuitable for lock-coupling because there is no way to
    /// release the parent guard while still holding the childrens' guards.
    /// OTOH, the children are evaluated in parallel, which cannot be done with
    /// lock-coupling.
    // TODO: consider not returning the Vec<R>, if callers don't need it.
    pub fn drain_xlock<B, D, F, R>(mut self, dml: Arc<D>, range: Range<usize>,
                                   txg: TxgT, f: F)
        -> impl Future<Output=Result<(TreeWriteGuard<A, K, V>, Vec<R>)>>
        where D: DML<Addr=A> + 'static,
              F: Fn(TreeWriteGuard<A, K, V>, &Arc<D>) -> B + Clone + Send
                  + 'static,
              B: Future<Output = Result<R>> + Send + 'static,
              R: Send + 'static
    {
        self.as_int_mut().children.drain(range)
        .map(move |elem| {
            let dml2 = dml.clone();
            let lock_fut = if elem.ptr.is_mem() {
                elem.ptr.as_mem()
                    .xlock()
                    .map(Ok)
                    .boxed()
            } else {
                let addr = *elem.ptr.as_addr();
                let afut = dml.pop::<Arc<Node<A, K, V>>, Arc<Node<A, K, V>>>(
                    &addr, txg);
                async move {
                    let arc = afut.await?;
                    let child_guard = arc.xlock().await;
                    drop(child_guard);
                    let child_node = Box::new(Arc::try_unwrap(*arc)
                        .expect("We should be the Node's only owner"));
                    Ok(TreeWriteGuard(child_node.0.try_write().unwrap()))
                }.boxed()
            };
            let f2 = f.clone();
            lock_fut.and_then(move |guard| {
                f2(guard, &dml2)
            })
        }).collect::<FuturesOrdered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(move |r| (self, r))
    }
}

impl<A: Addr, K: Key, V: Value> Deref for TreeWriteGuard<A, K, V> {
    type Target = NodeData<A, K, V>;

    fn deref(&self) -> &Self::Target {
        match self {
            TreeWriteGuard(guard) => &**guard,
        }
    }
}

impl<A: Addr, K: Key, V: Value> DerefMut for TreeWriteGuard<A, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            TreeWriteGuard(guard) => &mut **guard,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
pub struct IntElem<A: Addr, K: Key + DeserializeOwned, V: Value> {
    pub key: K,
    /// The range of transactions in which the target Node and all of its
    /// children were written.
    ///
    /// This is _not_ the transactions in which individual data
    /// items were added or modified; just the transaction in which the actual
    /// tree nodes were written.  It may be overly broad following insert and
    /// remove operations, but it will be fixed on flush.
    pub txgs: Range<TxgT>,
    pub ptr: TreePtr<A, K, V>
}

impl<A: Addr, K: Key, V: Value> TypicalSize for IntElem<A, K, V> {
    const TYPICAL_SIZE: usize =
        K::TYPICAL_SIZE     // key
        + 4 * 2             // Range<TxgT>
        + 4                 // TreePtr discriminant
        + A::TYPICAL_SIZE;  // TreePtr contents
}

impl<A: Addr, K: Key, V: Value> IntElem<A, K, V> {
    /// Is the child node dirty?  That is, does it differ from the on-disk
    /// version?
    pub fn is_dirty(&self) -> bool {
        self.ptr.is_dirty()
    }

    pub fn new(key: K, txgs: Range<TxgT>, ptr: TreePtr<A, K, V>) -> Self {
        IntElem{key, txgs, ptr}
    }

    /// Lock nonexclusively
    #[instrument(skip(dml))]
    pub fn rlock<D: DML<Addr=A>>(self: &IntElem<A, K, V>, dml: &Arc<D>)
        -> Pin<Box<dyn Future<
            Output=Result<TreeReadGuard<A, K, V>>> + Send
        >>
    {
        match self.ptr {
            TreePtr::Mem(ref node) => {
                node.0.read()
                    .map(|g| Ok(TreeReadGuard::Mem(g)))
                    .in_current_span()
                    .boxed()
            },
            TreePtr::Addr(ref addr) => {
                dml.get::<Arc<Node<A, K, V>>, Arc<Node<A, K, V>>>(addr)
                .and_then(|node| {
                    node.0.read()
                        .map(move |guard|
                             Ok(TreeReadGuard::Addr(guard, *node))
                        )
                }).in_current_span()
                .boxed()
            },
            // LCOV_EXCL_START
            TreePtr::None => unreachable!("None is just a temporary value")
            // LCOV_EXCL_STOP
        }
    }
}

impl<A: Addr, K: Key, V: Value> Default for IntElem<A, K, V> {
    /// Generate a new IntElem suitable for use as the root of a Tree
    fn default() -> Self {
        // Since there are no on-disk children, the initial TXG range is empty
        let txgs = TxgT::from(0)..TxgT::from(0);
        IntElem::new(K::min_value(),
            txgs,
            TreePtr::Mem(
                Box::new(
                    Node::new(
                        NodeData::Leaf(
                            LeafData::default()
                        )
                    )
                )
            )
        )
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(bound(deserialize = "A: DeserializeOwned, K: DeserializeOwned"))]
pub struct IntData<A: Addr, K: Key, V: Value> {
    pub children: Vec<IntElem<A, K, V>>
}

impl<A: Addr, K: Key, V: Value> IntData<A, K, V> {
    /// Are any of this node's children dirty?
    ///
    /// Note that the node itself could still be dirty, even if its children
    /// aren't.
    pub fn has_dirty_children(&self) -> bool {
        self.children.iter()
            .any(IntElem::is_dirty)
    }

    /// How many children does this node have?
    pub fn nchildren(&self) -> usize {
        self.children.len()
    }

    pub fn new(children: Vec<IntElem<A, K, V>>) -> IntData<A, K, V> {
        IntData{children}
    }

    /// Find index of rightmost child whose key is less than or equal to k
    pub fn position<Q>(&self, k: &Q) -> usize
        where K: Borrow<Q>, Q: Ord
    {
        self.children
            .binary_search_by(|child| child.key.borrow().cmp(k))
            .unwrap_or_else(|k| if k == 0 {k} else {k - 1})
    }

    /// Split this IntNode in two.  Returns the transaction range of the rump
    /// node, and a new IntElem containing the new node.
    pub fn split(&mut self, left_items: usize, txg: TxgT)
        -> (Range<TxgT>, IntElem<A, K, V>)
    {
        let new_children = self.children.split_off(left_items);
        let old_txgs = self.start_txg()..txg + 1;

        let key = new_children[0].key;
        let int_data = IntData::new(new_children);
        let start = int_data.start_txg();
        let node = Node::new(NodeData::Int(int_data));
        let txgs = start..txg + 1;
        let elem = IntElem::new(key, txgs, TreePtr::Mem(Box::new(node)));
        (old_txgs, elem)
    }

    /// Find the oldest txg included amongst this node's children
    fn start_txg(&self) -> TxgT {
        self.children.iter()
            .map(|child| child.txgs.start)
            .min()
            .unwrap()
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
pub enum NodeData<A: Addr, K: Key, V: Value> {
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

    /// Check invariants for a single NodeData
    // Don't check credit, because this node might not be dirty
    pub fn check(&self, key: K, height: u8, is_root: bool, limits: &Limits)
                 -> bool
    {
        let id = NodeId{height, key};
        let l = self.len();
        let len_ok = if (!is_root) && l < self.min_fanout(limits) as usize {
            eprintln!("Node underflow.  Node {:?} has {} items", id, l);
            false
        } else if l > self.max_fanout(limits) as usize {
            eprintln!("Node overflow.  Node {:?} has {} items", id, l);
            false
        } else {
            true
        };
        let key_ok = if !is_root && key > *self.key() {
            eprintln!("Bad key.  Node {:?} has lowest element {:?} but key {:?}"
                      , id, self.key(), key);
            false
        } else {
            true
        };
        len_ok && key_ok
    }

    /// Are any of this node's children dirty?
    ///
    /// Note that the node itself could still be dirty, even if its children
    /// aren't.
    pub fn has_dirty_children(&self) -> bool {
        match self {
            NodeData::Leaf(_) => false,
            NodeData::Int(ni) => ni.has_dirty_children()
        }
    }

    /// Is this node in danger of underflowing if one child gets merged?
    pub fn in_danger(&self, limits: &Limits) -> bool {
        self.len() <= self.min_fanout(limits) as usize
    }

    /// Is this node in danger of underflowing if two childen get merged?
    pub fn in_extra_danger(&self, limits: &Limits) -> bool {
        self.len() <= self.min_fanout(limits) as usize + 1
    }

    pub fn into_leaf(self) -> LeafData<K, V> {
        if let NodeData::Leaf(leaf) = self {
            leaf
        } else {
            panic!("Not a NodeData::Leaf")  // LCOV_EXCL_LINE
        }
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self, NodeData::Leaf(_))
    }

    /// Can this child be merged with `rhs` without violating constraints?
    pub fn can_merge(&self, rhs: &NodeData<A, K, V>, limits: &Limits) -> bool {
        self.len() + rhs.len() <= usize::from(self.max_fanout(limits))
    }

    /// Return this `NodeData`s lower bound key, suitable for use in its
    /// parent's `children` array.
    pub fn key(&self) -> &K {
        match self {
            NodeData::Leaf(ref leaf) => leaf.items.keys().next().unwrap(),
            NodeData::Int(ref int) => &int.children[0].key,
        }
    }

    /// Number of children or items in this `NodeData`
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        match self {
            NodeData::Leaf(leaf) => leaf.items.len(),
            NodeData::Int(int) => int.children.len()
        }
    }

    /// Return the minimum allowable fanout
    pub(super) fn min_fanout(&self, limits: &Limits) -> u16 {
        if self.is_leaf() {
            limits.min_leaf_fanout
        } else {
            limits.min_int_fanout
        }
    }

    /// Return the maximum allowable fanout (inclusive limit)
    pub(super) fn max_fanout(&self, limits: &Limits) -> u16 {
        if self.is_leaf() {
            limits.max_leaf_fanout
        } else {
            limits.max_int_fanout
        }
    }

    /// Is this node currently underflowing?
    pub fn underflow(&self, limits: &Limits) -> bool {
        let len = self.len();
        len < usize::from(self.min_fanout(limits))
    }

    /// Should this node be split because it's too big?
    // Normally, proactively split any node that's reached its maximum size on
    // the theory that it may gain a new child..
    // But as an optimization, don't split it if it's a leaf and we're only
    // overwriting.
    pub fn should_split(&self, key: &K, limits: &Limits) -> bool {
        if let NodeData::Leaf(leaf) = self {
            if leaf.items.contains_key(key) {
                return false;
            }
        }
        debug_assert!(self.len() <= usize::from(self.max_fanout(limits)),
                      "Overfull nodes shouldn't be possible");
        self.len() >= usize::from(self.max_fanout(limits))
    }

    /// Split this Node in two.  Returns the transaction range of the rump
    /// node, and a new IntElem containing the new node.
    pub fn split(&mut self, limits: &Limits, seq: bool, txg: TxgT)
        -> (Range<TxgT>, IntElem<A, K, V>)
    {
        let left_items = if seq {
            // Make the left node as large as possible, since we'll almost
            // certainly continue inserting into the right side.
            self.len() - usize::from(self.min_fanout(limits))
        } else {
            // Divide evenly, but make the left node slightly larger, on the
            // assumption that we're more likely to insert into the right node
            // than the left one.
            div_roundup(self.len(), 2)
        };
        match self {
            NodeData::Leaf(leaf) => {
                leaf.split(left_items, txg)
            },
            NodeData::Int(int) => {
                int.split(left_items, txg)
            },

        }
    }

    /// Find the oldest TXG amongst this Node and all its children
    /// `my_txg` is the TXG that this node was written (or will be written)
    pub fn start_txg(&self, my_txg: TxgT) -> TxgT {
        match self {
            NodeData::Int(int) => {
                int.start_txg()
            },
            NodeData::Leaf(_leaf) => {
                my_txg
            }
        }
    }

    /// Merge all of `other`'s data into `self`.  Afterwards, `other` may be
    /// deleted.
    pub fn merge(&mut self, mut other: TreeWriteGuard<A, K, V>)
    {
        match self {
            NodeData::Int(int) =>
                int.children.append(&mut other.as_int_mut().children),
            NodeData::Leaf(leaf) => {
                let other_ld = other.as_leaf_mut();
                leaf.items.append(&mut other_ld.items);
                leaf.credit.extend(other_ld.credit.take());
            }
        }
    }

    /// Take `other`'s highest keys and merge them into ourself
    pub fn take_high_keys(&mut self, other: &mut NodeData<A, K, V>) {
        // Try to even out the nodes, but always steal at least 1
        let keys_to_share = max(1, (other.len() - self.len()) / 2);
        match self {
            NodeData::Int(int) => {
                let other_children = &mut other.as_int_mut().children;
                let cutoff_idx = other_children.len() - keys_to_share;
                let other_right_half =
                    other_children.split_off(cutoff_idx);
                int.children.splice(0..0, other_right_half.into_iter());
            },
            NodeData::Leaf(leaf) => {
                let other_ld = other.as_leaf_mut();
                let other_items = &mut other_ld.items;
                let cutoff_idx = other_items.len() - keys_to_share;
                let cutoff = *other_items.keys().nth(cutoff_idx).unwrap();
                let mut other_right_half = other_items.split_off(&cutoff);
                let allocated: usize = other_right_half.iter()
                    .map(|(_k, v)| v.allocated_space())
                    .sum();
                let kvs = other_right_half.len() * mem::size_of::<(K, V)>();
                leaf.items.append(&mut other_right_half);
                leaf.credit.extend(other_ld.credit.split(allocated + kvs));
            }
        }
    }

    /// Take `other`'s lowest keys and merge them into ourself
    pub fn take_low_keys(&mut self, other: &mut NodeData<A, K, V>) {
        // Try to even out the nodes, but always steal at least 1
        let keys_to_share = max(1, (other.len() - self.len()) / 2);
        match self {
            NodeData::Int(int) => {
                let other_children = &mut other.as_int_mut().children;
                let other_left_half = other_children.drain(0..keys_to_share);
                let nchildren = int.children.len();
                int.children.splice(nchildren.., other_left_half);
            },
            NodeData::Leaf(leaf) => {
                let other_ld = other.as_leaf_mut();
                let other_items = &mut other_ld.items;
                let cutoff = *other_items.keys().nth(keys_to_share).unwrap();
                let other_right_half = other_items.split_off(&cutoff);
                let mut other_left_half =
                    mem::replace(other_items, other_right_half);
                let allocated: usize = other_left_half.iter()
                    .map(|(_k, v)| v.allocated_space())
                    .sum();
                let kvs = other_left_half.len() * mem::size_of::<(K, V)>();
                leaf.items.append(&mut other_left_half);
                leaf.credit.extend(other_ld.credit.split(allocated + kvs));
            }
        }
    }
}

impl<A: Addr, K: Key, V: Value> Cacheable for Arc<Node<A, K, V>> {
    fn deserialize(dbs: DivBufShared) -> Self where Self: Sized {
        let db = dbs.try_const().unwrap();
        let node_data: NodeData<A, K, V> = bincode::deserialize(&db[..]).unwrap();
        Arc::new(Node(RwLock::new(node_data)))
    }

    fn eq(&self, o: &dyn Cacheable) -> bool {
        if let Ok(other) = o.downcast_ref::<Arc<Node<A, K, V>>>() {
            // Since the cache is strictly read-only, try_read is guaranteed to
            // work if both values are cached, which is probably the only
            // context that should be using this method.
            let self_data = self.0.try_read().unwrap();
            let other_data = other.0.try_read().unwrap();
            self_data.eq(&*other_data)
        } else {
            // other isn't even the same concrete type
            false
        }
    }

    fn cache_space(&self) -> usize {
        let arcsize = mem::size_of::<Arc<Node<A, K, V>>>();
        let rwlocksize = mem::size_of::<RwLock<NodeData<A, K, V>>>();
        let nodesize = mem::size_of::<Node<A, K, V>>();
        if let Ok(guard) = self.0.try_read() {
            match guard.deref() {
                NodeData::Leaf(leaf) => {
                    let allocated: usize = leaf.items.iter()
                        .map(|(_k, v)| v.allocated_space())
                        .sum();
                    // Rust's BTreeMap doesn't have any method to get its memory
                    // consumption.  It's hard to calculate theoretically, so
                    // this model is experimentally determined.
                    let kvs = mem::size_of::<K>() + mem::size_of::<V>();
                    let n = leaf.items.len() as f64;
                    ( 3.269795136 * n +
                      1.836840272 * n * (kvs as f64) +
                      476.8535918 ) as usize +
                    allocated
                },
                NodeData::Int(int) => {
                    // IntData is layed out contiguously in memory
                    let nkids = int.children.capacity();
                    let kidsize = mem::size_of::<IntElem<A, K, V>>();
                    let fudge = 280;    // Experimentally determined
                    arcsize + rwlocksize + nodesize + nkids * kidsize + fudge
                }
            }
        } else {
            panic!("There's probably no good reason to call this method on a Node that's locked exclusively, because such a Node can't be in the Cache");
        }
    }

    fn make_ref(&self) -> Box<dyn CacheRef> {
        Box::new(self.clone())
    }

    fn wb_space(&self) -> usize {
        if let Ok(guard) = self.0.try_read() {
            match guard.deref() {
                NodeData::Leaf(leaf) => leaf.wb_space(),
                NodeData::Int(_int) => {
                    // We don't currently track WB credit for Int nodes.  We
                    // treat them as free.
                    0
                }
            }
        } else {
            unimplemented!()
        }

    }
}

impl<A: Addr, K: Key, V: Value> CacheRef for Arc<Node<A, K, V>> {
    fn deserialize(dbs: DivBufShared) -> Box<dyn Cacheable> where Self: Sized {
        let db = dbs.try_const().unwrap();
        let node_data: NodeData<A, K, V> = bincode::deserialize(&db[..]).unwrap();
        let node = Arc::new(Node(RwLock::new(node_data)));
        Box::new(node)
    }

    fn serialize(&self) -> DivBuf {
        let g = self.0.try_read().expect(
            "Shouldn't be serializing a Node that's locked for writing");
        let v = bincode::serialize(&g.deref()).unwrap();
        let dbs = DivBufShared::from(v);
        dbs.try_const().unwrap()
    }

    fn into_owned(self) -> Box<dyn Cacheable> {
        Box::new(self)
    }
}
#[derive(Debug)]
pub struct Node<A, K, V> (
    pub(super) RwLock<NodeData<A, K, V>>
)
    where A: Addr, K: Key, V: Value;

impl<A: Addr, K: Key, V: Value> Node<A, K, V> {
    pub fn new(node_data: NodeData<A, K, V>) -> Self {
        Node(RwLock::new(node_data))
    }

    /// Attempt to unwrap Self into a [`NodeData`].
    ///
    /// Probably should only be used from test code.
    pub fn try_unwrap(self) -> std::result::Result<NodeData<A, K, V>, Self> {
        self.0.try_unwrap()
        .map_err(Node)
    }

    /// Lock the indicated `Node` exclusively.
    pub(super) fn xlock(&self) -> impl Future<Output=TreeWriteGuard<A, K, V>>
    {
        self.0.write().map(TreeWriteGuard)
    }
}

// LCOV_EXCL_START
#[cfg(test)]
macro_rules! leaf_node {
    ( $items: ident ) => {
        Node(RwLock::new(NodeData::Leaf(LeafData{
            credit: Credit::null(),
            $items
        })))
    }
}

/// Basic tests of node types
#[cfg(test)]
mod t {

use crate::ddml::DRP;
use divbuf::*;
use pretty_assertions::assert_eq;
use super::*;

// pet kcov
#[test]
fn debug() {
    let ld = LeafData {
        credit: Credit::null(),
        items: BTreeMap::<u32, u32>::new()
    };
    let items: BTreeMap<u32, u32> = BTreeMap::new();
    let node: Node<DRP, u32, u32> = leaf_node!(items);
    format!("{:?} {:?}", ld, node);

    let mut children: Vec<IntElem<u32, u32, u32>> = Vec::new();
    let txgs = TxgT(1)..TxgT(3);
    children.push(IntElem::new(0, txgs, TreePtr::Addr(4)));
    format!("{:?}", NodeData::Int(IntData{children}));
}

#[test]
fn arc_node_eq() {
    let items: BTreeMap<u32, u32> = BTreeMap::new();
    let node: Arc<Node<DRP, u32, u32>> = Arc::new(leaf_node!(items));
    assert!(node.eq(&node));
    let dbs = DivBufShared::from(Vec::new());
    assert!(!node.eq(&dbs));
}

#[allow(clippy::eq_op)]
#[test]
fn treeptr_eq() {
    assert_eq!(TreePtr::Addr::<u32, u32, u32>(0),
               TreePtr::Addr::<u32, u32, u32>(0));
    assert_ne!(TreePtr::Addr::<u32, u32, u32>(0),
               TreePtr::Addr::<u32, u32, u32>(1));
    assert_eq!(TreePtr::None::<u32, u32, u32>,
               TreePtr::None::<u32, u32, u32>);
    assert_ne!(TreePtr::Addr::<u32, u32, u32>(0),
               TreePtr::None::<u32, u32, u32>);
}

#[test]
#[should_panic(expected = "recursively")]
fn treeptr_eq_mem() {
    let x = TreePtr::Addr(0);
    let items: BTreeMap<u32, u32> = BTreeMap::new();
    let node: Box<Node<u32, u32, u32>> = Box::new(leaf_node!(items));
    let y = TreePtr::Mem(node);
    assert_ne!(x, y);
}

#[test]
fn txgt_min_value() {
    assert_eq!(TxgT(0), TxgT::min_value());
}
}

/// Tests for NodeData.has_dirty_children
#[cfg(test)]
mod has_dirty_children {
use super::*;

#[test]
fn nothing_dirty() {
    let children = vec![
        IntElem::new(0u32, TxgT::from(1)..TxgT::from(9), TreePtr::Addr(0)),
        IntElem::new(256u32, TxgT::from(2)..TxgT::from(8), TreePtr::Addr(4u32)),
    ];
    let node_data = NodeData::<u32, u32, u32>::Int(IntData::new(children));
    assert!(!node_data.has_dirty_children());
}

#[test]
fn yes() {
    let mut items: BTreeMap<u32, u32> = BTreeMap::new();
    items.insert(0, 100);
    items.insert(1, 200);
    items.insert(99, 50_000);
    let leaf: Box<Node<u32, u32, u32>> = Box::new(leaf_node!(items));
    let children = vec![
        IntElem::new(0u32, TxgT::from(1)..TxgT::from(9), TreePtr::Mem(leaf)),
        IntElem::new(256u32, TxgT::from(2)..TxgT::from(8), TreePtr::Addr(4u32)),
    ];
    let node_data = NodeData::Int(IntData::new(children));
    assert!(node_data.has_dirty_children());
}
}

/// Tests for serialization/deserialization of Nodes
#[cfg(test)]
mod serialization {

use crate::{
    dml::Compression,
    ddml::DRP
};
use pretty_assertions::assert_eq;
use std::ops::Deref;
use super::*;

#[test]
fn deserialize_int() {
    let serialized = DivBufShared::from(vec![
        1u8, 0, 0, 0, // enum variant 0 for IntNode
        2, 0, 0, 0, 0, 0, 0, 0,     // 2 elements in the vector
           0, 0, 0, 0,              // K=0
           1, 0, 0, 0, 9, 0, 0, 0,  // TXG range 1..9
           0u8, 0, 0, 0,            // enum variant 0 for TreePtr::Addr
               0, 0,                // Cluster 0
               0, 0, 0, 0, 0, 0, 0, 0,  // LBA 0
           0,                       // Not compressed
           0x40, 0x9c, 0, 0,        // lsize=40000
           0x40, 0x9c, 0, 0,         // csize=40000
           0xef, 0xbe, 0xad, 0xde, 0, 0, 0, 0,  // checksum
           0, 1, 0, 0,              // K=256
           2, 0, 0, 0, 8, 0, 0, 0,  // TXG range 1..9
           0u8, 0, 0, 0,            // enum variant 0 for TreePtr::Addr
               0, 0,                // Cluster 0
               0, 1, 0, 0, 0, 0, 0, 0,  // LBA 256
           1,                       // Compressed
           0x80, 0x3e, 0, 0,        // lsize=16000
           0x40, 0x1f, 0, 0,        // csize=8000
           0xbe, 0xba, 0x7e, 0x1a, 0, 0, 0, 0,  // checksum
    ]);
    let drp0 = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                        0xdead_beef);
    let drp1 = DRP::new(PBA::new(0, 256), Compression::Zstd(None),
                        16000, 8000, 0x1a7e_babe);
    let node: Arc<Node<DRP, u32, u32>> = Cacheable::deserialize(serialized);
    let guard = node.0.try_read().unwrap();
    let int_data = guard.deref().as_int();
    assert_eq!(int_data.children.len(), 2);
    assert_eq!(int_data.children[0].key, 0);
    assert_eq!(*int_data.children[0].ptr.as_addr(), drp0);
    assert_eq!(int_data.children[1].key, 256);
    assert_eq!(*int_data.children[1].ptr.as_addr(), drp1);
    assert_eq!(int_data.children[0].txgs, TxgT::from(1)..TxgT::from(9));
    assert_eq!(int_data.children[1].txgs, TxgT::from(2)..TxgT::from(8));
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
fn intelem_typical_size() {
    let pba = PBA::new(0, 1);
    let drp = DRP::random(Compression::None, 12345);
    let int_elem = IntElem::<DRP, PBA, RID>::new(pba,
                                                 TxgT::from(1)..TxgT::from(9),
                                                 TreePtr::Addr(drp));
    let size = bincode::serialized_size(&int_elem).unwrap() as usize;
    assert_eq!(IntElem::<DRP, PBA, RID>::TYPICAL_SIZE, size);
}

#[test]
fn serialize_int() {
    let expected = vec![1u8, 0, 0, 0, // enum variant 0 for IntNode
        2, 0, 0, 0, 0, 0, 0, 0,     // 2 elements in the vector
           0, 0, 0, 0,              // K=0
           1, 0, 0, 0, 9, 0, 0, 0,  // TXG range 1..9
           0u8, 0, 0, 0,            // enum variant 0 for TreePtr::Addr
               0, 0,                // Cluster 0
               0, 0, 0, 0, 0, 0, 0, 0,  // LBA 0
           0,                       // Not compressed
           0x40, 0x9c, 0, 0,        // lsize=40000
           0x40, 0x9c, 0, 0,         // csize=40000
           0xef, 0xbe, 0xad, 0xde, 0, 0, 0, 0,  // checksum
           0, 1, 0, 0,              // K=256
           2, 0, 0, 0, 8, 0, 0, 0,  // TXG range 1..9
           0u8, 0, 0, 0,            // enum variant 0 for TreePtr::Addr
               0, 0,                // Cluster 0
               0, 1, 0, 0, 0, 0, 0, 0,  // LBA 256
           1,                       // Compressed
           0x80, 0x3e, 0, 0,        // lsize=16000
           0x40, 0x1f, 0, 0,        // csize=8000
           0xbe, 0xba, 0x7e, 0x1a, 0, 0, 0, 0,  // checksum
    ];
    let drp0 = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                        0xdead_beef);
    let drp1 = DRP::new(PBA::new(0, 256), Compression::Zstd(None),
                        16000, 8000, 0x1a7e_babe);
    let children = vec![
        IntElem::new(0u32, TxgT::from(1)..TxgT::from(9), TreePtr::Addr(drp0)),
        IntElem::new(256u32, TxgT::from(2)..TxgT::from(8), TreePtr::Addr(drp1)),
    ];
    let node_data = NodeData::Int(IntData::new(children));
    let node: Node<DRP, u32, u32> = Node(RwLock::new(node_data));
    let db = Arc::new(node).serialize();
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
    let node: Arc<Node<DRP, u32, u32>> = Arc::new(leaf_node!(items));
    let db = node.serialize();
    assert_eq!(&expected[..], &db[..]);
    drop(db);
}

}
// LCOV_EXCL_STOP
