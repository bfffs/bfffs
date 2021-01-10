// vim: tw=80
use crate::{
    common::*
};
use futures::{
    Future,
    FutureExt,
    SinkExt,
    StreamExt,
    TryFutureExt,
    TryStreamExt,
    channel::mpsc,
    future,
    stream::{self, FuturesOrdered, FuturesUnordered, Stream},
    task::{Context, Poll}
};
use futures_locks::*;
#[cfg(test)] use mockall::automock;
use serde::Serializer;
#[cfg(test)] use std::fmt::{self, Display, Formatter};
use std::{
    borrow::Borrow,
    cell::RefCell,
    collections::{BTreeMap, HashSet, VecDeque},
    convert::identity,
    fmt::Debug,
    io,
    mem,
    num::NonZeroU8,
    ops::{Bound, Deref, DerefMut, Range, RangeBounds, RangeInclusive},
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    }
};
use super::*;
use tokio::runtime;

#[cfg(test)] mod tests;

lazy_static::lazy_static! {
    static ref ATOMIC_CLUE: AtomicU64 = AtomicU64::new(0);
}

/// Are there any elements in common between the two Ranges?
#[allow(clippy::if_same_then_else)]
#[allow(clippy::needless_bool)]
fn ranges_overlap<R, T, U>(x: &R, y: &Range<U>) -> bool
    where U: Borrow<T> + PartialOrd,
          R: RangeBounds<T>,
          T: PartialOrd
{
    let x_is_empty = match (x.start_bound(), x.end_bound()) {
        (Bound::Included(s), Bound::Included(e)) => e < s,
        (Bound::Included(s), Bound::Excluded(e)) => e <= s,
        (Bound::Excluded(s), Bound::Included(e)) => e <= s,
        (Bound::Excluded(s), Bound::Excluded(e)) => e <= s,
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => false,
    };
    let x_precedes_y = match x.end_bound() {
        Bound::Included(e) => e < y.start.borrow(),
        Bound::Excluded(e) => e <= y.start.borrow(),
        Bound::Unbounded => false
    };
    let y_precedes_x = match x.start_bound() {
        Bound::Included(s) | Bound::Excluded(s) => s >= y.end.borrow(),
        Bound::Unbounded => false
    };

    if x_is_empty || y.end <= y.start {
        // One of the Ranges is empty
        false
    } else if x_precedes_y || y_precedes_x {
        false
    } else {
        true
    }
}

mod atomic_u64_serializer {
    use super::*;

    pub fn serialize<S>(x: &AtomicU64, s: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        s.serialize_u64(x.load(Ordering::Relaxed) as u64)
    }
}

mod tree_root_serializer {
    use super::*;
    use serde::{Serialize, ser::Error};

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
    last_fut: Option<Pin<Box<dyn Future<
        Output=Result<(VecDeque<(K, V)>, Option<Bound<T>>),
                      Error>>
        + Send
    >>>,
    /// Handle to the tree's inner
    inner: Arc<Inner<A, D, K, V>>,
}

impl<A, D, K, T, V> RangeQuery<A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A>,
          K: Key + Borrow<T>,
          T: Ord + Clone + Send,
          V: Value
{

    fn new<R>(inner: Arc<Inner<A, D, K, V>>, range: R)
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
        RangeQuery{cursor, data, end, last_fut: None, inner}
    }

    fn pin_get_cursor(self: Pin<&mut Self>) -> &mut Option<Bound<T>> {
        // // This is okay because `cursor` is never considered pinned
        unsafe { &mut self.get_unchecked_mut().cursor }
    }
    fn pin_get_data(self: Pin<&mut Self>) -> &mut VecDeque<(K, V)> {
        // // This is okay because `data` is never considered pinned
        unsafe { &mut self.get_unchecked_mut().data }
    }

    fn pin_get_last_fut(self: Pin<&mut Self>) ->
        &mut Option<Pin<Box<dyn Future<Output=
            Result<(VecDeque<(K, V)>, Option<Bound<T>>), Error>
        > + Send >>>
    {
        // // This is okay because `last_fut` is never considered pinned
        unsafe { &mut self.get_unchecked_mut().last_fut }
    }
}

#[cfg_attr(test, automock)]
impl<A, D, K, T, V> Stream for RangeQuery<A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A> + 'static,
          K: Key + Borrow<T>,
          T: Ord + Clone + Send + 'static,
          V: Value
{
    type Item = Result<(K, V), Error>;

    fn poll_next<'a>(mut self: Pin<&mut Self>, cx: &mut Context<'a>)
        -> Poll<Option<Self::Item>>
    {
        self.as_mut()
            .pin_get_data()
            .pop_front()
            .map(|x| Poll::Ready(Some(Ok(x))))
            .unwrap_or_else(|| {
                if self.as_mut().pin_get_cursor().is_some() {
                    let mut fut = self.as_mut()
                    .pin_get_last_fut()
                    .take()
                    .unwrap_or_else(|| {
                        let l = self.as_mut().pin_get_cursor().clone().unwrap();
                        let r = (l, self.end.clone());
                        Box::pin(Tree::get_range(&self.inner, r))
                    });
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(Ok((v, bound))) => {
                            *self.as_mut().pin_get_data() = v;
                            *self.as_mut().pin_get_cursor() = bound;
                            *self.as_mut().pin_get_last_fut() = None;
                            let datum = self.as_mut()
                                .pin_get_data()
                                .pop_front();
                            Poll::Ready(Ok(datum).transpose())
                        },
                        Poll::Pending => {
                            *self.as_mut().pin_get_last_fut() = Some(fut);
                            Poll::Pending
                        },
                        Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e)))
                    }
                } else {
                    Poll::Ready(None)
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
    last_fut: Option<Pin<Box<dyn Future<
        Output=Result<(VecDeque<NodeId<K>>, Option<K>),
                      Error>>
        + Send
    >>>,

    /// Range of addresses to move
    pbas: Range<PBA>,

    /// Range of transactions that may contain PBAs of interest
    txgs: Range<TxgT>,

    /// Handle to the tree's inner struct
    inner: Arc<Inner<ddml::DRP, D, K, V>>,
}

/// Result type of `Tree::clean_zone`
struct CleanZonePass1<D, K, V>
    where D: DML<Addr=ddml::DRP>,
          K: Key,
          V: Value
{
    inner: RefCell<CleanZonePass1Inner<D, K, V>>
}

/// Arguments to Tree::get_dirty_nodes which are static
struct GetDirtyNodeParams<K: Key> {
    /// Starting point for search
    key: K,
    /// Range of PBAs considered dirty
    pbas: Range<PBA>,
    /// Range of transactions in which the dirty PBAs were written
    txgs: Range<TxgT>,
    /// Level of the tree to search
    echelon: u8
}

impl<D, K, V> CleanZonePass1<D, K, V>
    where D: DML<Addr=ddml::DRP>,
          K: Key,
          V: Value
    {

    fn new(inner: Arc<Inner<ddml::DRP, D, K, V>>, pbas: Range<PBA>,
           txgs: Range<TxgT>, echelon: u8)
        -> CleanZonePass1<D, K, V>
    {
        let cursor = Some(K::min_value());
        let data = VecDeque::new();
        let last_fut = None;
        let inner = CleanZonePass1Inner{cursor, data, echelon, last_fut, pbas,
                                        txgs, inner};
        CleanZonePass1{inner: RefCell::new(inner)}
    }
}

impl<D, K, V> Stream for CleanZonePass1<D, K, V>
    where D: DML<Addr=ddml::DRP> + 'static,
          K: Key,
          V: Value
{
    type Item = Result<NodeId<K>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context)
        -> Poll<Option<Self::Item>>
    {
        let first = {
            let mut i = self.inner.borrow_mut();
            i.data.pop_front()
        };  // LCOV_EXCL_LINE   kcov false negative
        first.map(|x| Poll::Ready(Some(Ok(x))))
            .unwrap_or_else(|| {
                let i = self.inner.borrow();
                if i.cursor.is_some() {
                    drop(i);
                    let mut f = stream::poll_fn(|cx| -> Poll<Option<Result<(), Error>>> {
                        let mut i = self.inner.borrow_mut();
                        let mut f = i.last_fut.take().unwrap_or_else(|| {
                            let l = i.cursor.unwrap();
                            let pbas = i.pbas.clone();
                            let txgs = i.txgs.clone();
                            let e = i.echelon;
                            let params = GetDirtyNodeParams {
                                key: l, pbas, txgs, echelon: e
                            };
                            Box::pin(Tree::get_dirty_nodes(i.inner.clone(),
                                params))
                        });
                        match f.as_mut().poll(cx) {
                            Poll::Ready(Ok((v, bound))) => {
                                i.data = v;
                                i.cursor = bound;
                                i.last_fut = None;
                                if i.data.is_empty() && i.cursor.is_some() {
                                    // Restart the search at the next bound
                                    Poll::Ready(Some(Ok(())))
                                } else {
                                    // Search is done or data is ready
                                    Poll::Ready(None)
                                }
                            },
                            Poll::Pending => {
                                i.last_fut = Some(f);
                                Poll::Pending
                            },
                            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e)))
                        }
                    }).try_fold((), |_, _| future::ok(()));
                    match Pin::new(&mut f).poll(cx) {
                        Poll::Ready(Ok(())) => {
                            let mut i = self.inner.borrow_mut();
                            if i.last_fut.is_some() {
                                Poll::Pending
                            } else {
                                Poll::Ready(Ok(i.data.pop_front()).transpose())
                            }
                        },
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e)))
                    }
                } else {
                    Poll::Ready(None)
                }
            })
    }
}

#[derive(Debug)]
#[derive(Serialize)]
struct Inner<A: Addr, D: DML, K: Key, V: Value> {
    /// Tree height.  1 if the Tree consists of a single Leaf node.
    // Use atomics so it can be modified from an immutable reference.  Accesses
    // should be very rare, so performance is not a concern.
    #[serde(with = "atomic_u64_serializer")]
    height: AtomicU64,
    limits: Limits,
    /// Root node
    #[serde(with = "tree_root_serializer")]
    root: RwLock<IntElem<A, K, V>>,
    #[serde(skip)]
    dml: Arc<D>,
    /// Compression function used for interior nodes
    #[serde(skip)]
    int_compressor: Compression,
    /// Compression function used for leaves
    #[serde(skip)]
    leaf_compressor: Compression,
    /// Should tree operations assume that access will be mostly sequential in
    /// increasing order?
    #[serde(skip)]
    sequentially_optimized: bool
}

impl<A: Addr, D: DML, K: Key, V: Value> Inner<A, D, K, V> {
    const INT_ELEM_SIZE: usize = IntElem::<A, K, V>::TYPICAL_SIZE;
    const LEAF_ELEM_SIZE: usize = K::TYPICAL_SIZE + V::TYPICAL_SIZE;

    #[cfg(test)]
    pub fn from_str(dml: Arc<D>, seq: bool, s: &str) -> Self {
        let il: InnerLiteral<A, K, V> = serde_yaml::from_str(s).unwrap();
        Inner::new(dml, il.height, il.limits, il.root, seq)
    }

    pub fn new(dml: Arc<D>, height: u64, limits: Limits,
               root_elem: IntElem<A, K, V>, seq: bool) -> Self
    {
        debug_assert!(Self::INT_ELEM_SIZE < u8::max_value as usize);
        debug_assert!(Self::INT_ELEM_SIZE > 0);
        let int_ts = unsafe{
            // Safe because we checked that INT_ELEM_SIZE > 0
            NonZeroU8::new_unchecked(Self::INT_ELEM_SIZE as u8)
        };
        let int_compressor = Compression::LZ4(Some(int_ts));
        debug_assert!(Self::LEAF_ELEM_SIZE < u8::max_value as usize);
        debug_assert!(Self::LEAF_ELEM_SIZE > 0);
        let leaf_ts = unsafe{
            // Safe because we checked that LEAF_ELEM_SIZE > 0
            NonZeroU8::new_unchecked(Self::LEAF_ELEM_SIZE as u8)
        };
        let leaf_compressor = Compression::LZ4(Some(leaf_ts));
        Inner {
            height: AtomicU64::new(height),
            limits,
            root: RwLock::new(root_elem),
            dml,
            int_compressor,
            leaf_compressor,
            sequentially_optimized: seq
        }
    }

    pub fn open(dml: Arc<D>, seq: bool, on_disk: TreeOnDisk<A>) -> Self {
        let iod: InnerOnDisk<A> = on_disk.0;
        let root_elem = IntElem::new(K::min_value(), iod.txgs,
                                     TreePtr::Addr(iod.root));
        Inner::new(dml, iod.height, iod.limits, root_elem, seq)
    }
}

/// A version of `Inner` that can be represented as a String.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
#[cfg(test)]
struct InnerLiteral<A: Addr, K: Key, V: Value> {
    height: u64,
    limits: Limits,
    root: IntElem<A, K, V>
}

/// The return type of `Tree::check_r`
type CheckR<K> = Pin<Box<dyn Future<
    Output=Result<(bool, RangeInclusive<K>, Range<TxgT>),
                  Error>>
    + Send>>;


/// In-memory representation of a COW B+-Tree
///
/// # Generic Parameters
///
/// *`K`:   Key type.  Must be ordered and copyable; should be compact
/// *`V`:   Value type in the leaves.
pub struct Tree<A: Addr, D: DML<Addr=A>, K: Key, V: Value> {
    i: Arc<Inner<A, D, K, V>>
}

impl<A, D, K, V> Tree<A, D, K, V>
    where A: Addr,
          D: DML<Addr=A> + 'static,
          K: Key,
          V: Value
{
    /// Return the address (not key) of every Node within a given TXG range in
    /// the Tree.
    ///
    /// Guaranteed to return every address used by the Tree, and no others.
    pub fn addresses<R, T>(&self, txgs: R)
        -> impl Stream<Item=A>
        where TxgT: Borrow<T>,
              R: Clone + RangeBounds<T> + Send + Sync + 'static,
              T: Ord + Clone + Send
    {
        // Keep the whole tree locked so no new addresses come into use and no
        // old ones get freed while we're working
        let (mut tx, rx) = mpsc::channel(self.i.limits.max_fanout() as usize);
            let tx2 = tx.clone();
        let height = self.i.height.load(Ordering::Relaxed) as u8;
        let dml = self.i.dml.clone();
        let txgs2 = txgs.clone();
        let tgf = self.read();
        tokio::spawn( async move {
            let tree_guard = tgf.await;
            if tree_guard.ptr.is_addr()
                && ranges_overlap(&txgs, &tree_guard.txgs)
            {
                tx.send(*tree_guard.ptr.as_addr())
                .await
                .unwrap();
            }
            if height > 1 {
                let guard = tree_guard.rlock(&dml)
                    .await
                    .unwrap();
                Tree::addresses_r(dml, height - 1, guard, tx2, txgs2)
                    .await
                    .unwrap();
            }
            drop(tree_guard);
        });
        rx
    }

    fn addresses_r<R, T>(dml: Arc<D>, height: u8, guard: TreeReadGuard<A, K, V>,
                         mut tx: mpsc::Sender<A>, txgs: R)
        //-> Result<(), Error>
        //-> impl Future<Output=Result<(), Error>> + Send
        -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>
        where TxgT: Borrow<T>,
              R: Clone + RangeBounds<T> + Send + Sync + 'static,
              T: Ord + Clone + Send
    {
        let child_addresses = guard.as_int().children.iter()
        .filter(|c| c.ptr.is_addr() && ranges_overlap(&txgs, &c.txgs))
        .map(|c| Ok(*c.ptr.as_addr()))
        .collect::<Vec<Result<A, _>>>();
        async move {
            tx.send_all(&mut stream::iter(child_addresses.into_iter()))
            .map_err(|_| Error::EPIPE)
            .await?;
            if height > 1 {
                let txgs2 = txgs.clone();
                let dml2 = dml.clone();
                let child_iter = guard.as_int().children.iter()
                .filter(|c| ranges_overlap(&txgs2, &c.txgs));
                for c in child_iter {
                    let guard = c.rlock(&dml2.clone()).await?;
                    Tree::addresses_r(
                        dml2.clone(),
                        height - 1,
                        guard,
                        tx.clone(),
                        txgs.clone()
                    ).await?;
                }
                Ok(())
            } else {
                Ok(())
            }
        }.boxed()
    }

    /// Audit the whole Tree for consistency and invariants
    // TODO: check node size limits, too
    pub fn check(&self) -> impl Future<Output=Result<bool, Error>> + Send {
        // Keep the whole tree locked and use LIFO lock discipline
        let height = self.i.height.load(Ordering::Relaxed) as u8;
        let inner = self.i.clone();
        self.read()
        .then(move |tree_guard| {
            tree_guard.rlock(&inner.dml)
            .and_then(move |guard| {
                let root_ok = guard.check(tree_guard.key, height - 1, true,
                                          &inner.limits);
                if height == 1 {
                    future::ok(root_ok).boxed()
                } else {
                    let fut = Tree::check_r(&inner.dml, height - 1, &guard,
                                            inner.limits)
                    .map_ok(move |r| {
                        if r.2.start < tree_guard.txgs.start ||
                           r.2.end > tree_guard.txgs.end {
                            eprintln!(concat!("TXG inconsistency! Tree ",
                                "contained TXGs {:?} but Root node recorded ",
                                "{:?}"), r.1, tree_guard.txgs);
                            false
                        } else {
                            root_ok && r.0
                        }
                    });
                    fut.boxed()
                }
            })
        })
    }

    /// # Parameters
    ///
    /// - `height`:     The height of `node`.  Leaves are 0.
    ///
    /// # Returns
    ///
    /// Whether this node or any children were error-free, the range of keys
    /// contained by this node or any child, and the TXG range of this Node's
    /// children.
    fn check_r(dml: &Arc<D>, height: u8, node: &TreeReadGuard<A, K, V>,
               limits: Limits)
        -> CheckR<K>
    {
        debug_assert!(height > 0);
        let children = &node.as_int().children;
        let start = children.iter().map(|c| c.txgs.start).min().unwrap();
        let end = children.iter().map(|c| c.txgs.end).max().unwrap();
        node.as_int().children.iter().map(|c| {
            let range = c.txgs.clone();
            let range2 = range.clone();
            let dml2 = dml.clone();
            let key = c.key;
            c.rlock(&dml)
            .and_then(move |guard| {
                let data_ok = guard.check(key, height - 1, false, &limits);
                if guard.is_leaf() {
                    let first_key = *guard.key();
                    // We can unwrap() the last_key because last_key should only
                    // ever be None for a root leaf node, and check_r doesn't
                    // get called in that case.
                    let last_key = guard.as_leaf().last_key().unwrap();
                    let keys = first_key..=last_key;
                    future::ok((data_ok, keys, range)).boxed()
                } else {
                    let fut = Tree::check_r(&dml2, height - 1, &guard, limits)
                    .map_ok(move |(passed, keys, txgs)| {
                        // For IntNodes, use the key as recorded by the parent,
                        // which is <= the lowest child's key
                        (passed && data_ok, key..=*keys.end(), txgs)
                    });
                    fut.boxed()
                }
            }).map_ok(move |(passed, keys, r)| {
                if r.start < range2.start || r.end > range2.end {
                    let id = NodeId {height: height - 1, key};
                    eprintln!(concat!("TXG inconsistency!  Node {:?} ",
                        "contained TXGs {:?} but its parent recorded TXGs ",
                        "{:?}"), id, r, range2);
                    (false, keys)
                } else {
                    (passed, keys)
                }
            })
        }).collect::<FuturesOrdered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(move |r| {
            let children_passed = r.iter().all(|x| (*x).0);
            let first_key = *r[0].1.start();
            let mut last_key = K::min_value();
            let sorted = r.iter().map(|(_passed, keys)| {
                let r = if last_key > *keys.start() {
                    eprintln!(concat!("Unsorted Tree!  ",
                                      "Key {:?} precedes key {:?}"),
                              last_key, *keys.start());
                    false
                } else {
                    true
                };
                last_key = *keys.end();
                r
            }).all(identity);
            (children_passed && sorted, first_key..=last_key, start..end)
        }).boxed()
    }

    /// Create a new tree.  `sequentially_optimized` controls whether some
    /// internal operations will assume a mostly-sequential or mostly-random
    /// write pattern.  `leaf_compression_ratio` and `int_compression_ratio` are
    /// the expected compression ratios for leaf nodes and int nodes,
    /// respectively.
    /// It should be > 1.
    pub fn create(dml: Arc<D>, sequentially_optimized: bool,
                  leaf_compression_ratio: f32, int_compression_ratio: f32)
        -> Self
    {
        // Calculate good fanout parameters.
        // For leaves, the serialization format is 4 bytes for the enum variant,
        // followed by 8 bytes for the BTreeMap size, followed by K, V pairs.
        // For interior nodes, the serialization format is 4 bytes for the enum
        // variant, followed by 8 bytes for the vector size, followed by a
        // series of IntElems.

        let mk_fanout = |elem_size: usize, compression_ratio: f32| {
            // A fanout spread of 4 is what BetrFS uses.
            // [^CowBtrees] uses a fanout spread of 3 for its benchmarks
            let spread = 4u16;
            let overhead = 12;  // enum variant size and vector size
            let raw_bytes = (compression_ratio * BYTES_PER_LBA as f32) as usize;
            // split_size is the amount of data in the left side after a split
            let split_size = raw_bytes - overhead;
            let max_fanout = if sequentially_optimized {
                // Sequentially optimized trees will almost never randomly
                // insert.  Most nodes will be what remains after a split.  So
                // we should increase the target node size so that the left side
                // of a split just fits into a whole LBA.
                split_size * usize::from(spread) / usize::from(spread - 1)
                    / elem_size
            } else {
                // Randomly optimized nodes should split evenly.  But sequential
                // access is still more common.  Size the max fanout so that
                // left side of a split just fits into a whole LBA.  Use an even
                // number of elements, because the split method will put the
                // larger half on the left side.
                2 * (split_size / elem_size)
            };
            debug_assert!(max_fanout <= usize::from(u16::max_value()));
            let min_fanout = div_roundup(max_fanout as u16, spread);
            (min_fanout, max_fanout as u16)
            // TODO: take account of compression
        };

        let (minlf, maxlf) = mk_fanout(Inner::<A, D, K, V>::LEAF_ELEM_SIZE,
                                       leaf_compression_ratio);
        let (minif, maxif) = mk_fanout(IntElem::<A, K, V>::TYPICAL_SIZE,
                                       int_compression_ratio);

        let limits = Limits::new(minif, maxif, minlf, maxlf);
        Tree::new(dml,
                  limits,
                  sequentially_optimized
        )
    }

    /// Dump a YAMLized representation of the Tree to stdout.
    ///
    /// Must be called from the synchronous domain.
    ///
    /// To save RAM, the Tree is actually dumped as several independent YAML
    /// records.  The first one is the Tree itself, and the rest are other
    /// on-disk Nodes.  All the Nodes can be combined into a single YAML map by
    /// simply removing the `---` separators.
    // `&mut Formatter` isn't `Send`, so these Futures can only be used with the
    // single-threaded Runtime.  Given that limitation, we may as well
    // instantiate our own Runtime
    pub fn dump(&self, f: &mut dyn io::Write) -> Result<(), Error> {
        // Outline:
        // * Lock the whole tree and proceed bottom-up.
        // * YAMLize each Node
        // * If that Node is on-disk, print it and its address in a form that
        //   can be deserialized into a BTreeMap<A, NodeData>.  Otherwise,
        //   extend its parent's representation.
        // * Last of all, print the root's representation.
        let inner = self.i.clone();
        let rrf = Rc::new(RefCell::new(f));
        let rrf2 = rrf.clone();
        let rrf3 = rrf.clone();
        let mut rt = runtime::Builder::new()
            .basic_scheduler()
            .enable_io()
            .build()
            .unwrap();
        let fut = self.read()
            .then(move |tree_guard| {
                tree_guard.rlock(&inner.dml)
                .and_then(move |guard| {
                    let mut f2 = rrf2.borrow_mut();
                    let s = serde_yaml::to_string(&*inner).unwrap();
                    writeln!(f2, "{}", &s).unwrap();
                    Tree::dump_r(inner.dml.clone(), guard, rrf)
                }).map_ok(move |guard| {
                    let mut f3 = rrf3.borrow_mut();
                    let mut hmap = BTreeMap::new();
                    if !guard.is_mem() {
                        hmap.insert(*tree_guard.ptr.as_addr(), guard.deref());
                    }
                    if ! hmap.is_empty() {
                        let s = serde_yaml::to_string(&hmap).unwrap();
                        writeln!(f3, "{}", &s).unwrap();
                    }
                })
            });
        rt.block_on(fut)
    }

    fn dump_r<'a>(dml: Arc<D>, node: TreeReadGuard<A, K, V>,
                  f: Rc<RefCell<&'a mut dyn io::Write>>)
        -> Pin<Box<dyn Future<
            Output=Result<TreeReadGuard<A, K, V>, Error>> + 'a
        >>
    {
        let f2 = f.clone();
        let fut = if let NodeData::Int(ref int) = *node {
            let futs = int.children.iter().map(move |child| {
                let dml2 = dml.clone();
                let f3 = f.clone();
                child.rlock(&dml)
                .and_then(move |child_node| {
                    Tree::dump_r(dml2, child_node, f3)
                })
            }).collect::<FuturesOrdered<_>>()
            .try_collect::<Vec<_>>();
            Box::pin(futs) as Pin<Box<_>>
        } else {
            Box::pin(future::ok(Vec::new()))
                as Pin<Box<dyn Future<
                    Output=Result<Vec<TreeReadGuard<A, K, V>>, _>
                >>>
        }.map_ok(move |v: Vec<TreeReadGuard<A, K, V>>| {
            if !node.is_leaf() {
                let mut hmap = BTreeMap::new();
                let citer = node.as_int().children.iter();
                for (child, guard) in citer.zip(v.iter()) {
                    if !guard.is_mem() {
                        hmap.insert(*child.ptr.as_addr(), guard.deref());
                    }
                }
                if ! hmap.is_empty() {
                    let s = serde_yaml::to_string(&hmap).unwrap();
                    writeln!(f2.borrow_mut(), "{}", &s).unwrap();
                }
            }
            node
        });
        Box::pin(fut) as Pin<Box<_>>
    }

    /// Fix an Int node in danger of being underfull, returning the parent guard
    /// back to the caller
    #[allow(clippy::collapsible_if)]
    fn fix_int<Q>(inner: &Arc<Inner<A, D, K, V>>,
                  parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize, mut child: TreeWriteGuard<A, K, V>,
                  txg: TxgT)
        -> impl Future<Output=Result<(TreeWriteGuard<A, K, V>, i8, i8), Error>>
        where K: Borrow<Q>, Q: Ord
    {
        // Outline:
        // First, try to merge with the right sibling
        // Then, try to steal keys from the right sibling
        // Then, try to merge with the left sibling
        // Then, try to steal keys from the left sibling
        let nchildren = parent.as_int().nchildren();
        debug_assert!(nchildren >= 2,
            "nchildren < 2 for tree with parent key {:?}, idx={:?}",
            parent.as_int().children[child_idx].key, child_idx);
        let (fut, sib_idx, right) = {
            if child_idx < nchildren - 1 {
                let sib_idx = child_idx + 1;
                (parent.xlock(&inner.dml, sib_idx, txg), sib_idx, true)
            } else {
                let sib_idx = child_idx - 1;
                (parent.xlock(&inner.dml, sib_idx, txg), sib_idx, false)
            }
        };
        let limits = inner.limits;
        fut.map_ok(move |(mut parent, mut sibling)| {
            let (before, after) = {
                let children = &mut parent.as_int_mut().children;
                if right {
                    if child.can_merge(&sibling, &limits) {
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
                    if sibling.can_merge(&child, &limits) {
                        sibling.merge(child);
                        children[sib_idx].txgs.start = sibling.start_txg(txg);
                        children.remove(child_idx);
                        (1, 0)
                    } else {
                        child.take_high_keys(&mut sibling);
                        children[child_idx].key = *child.key();
                        children[sib_idx].txgs.start = sibling.start_txg(txg);
                        children[child_idx].txgs.start = child.start_txg(txg);
                        (0, 0)
                    }
                }
            };
            (parent, before, after)
        })
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Flush all in-memory Nodes to disk.
    pub fn flush(&self, txg: TxgT)
        -> impl Future<Output=Result<(), Error>>
    {
        let i = self.i.clone();
        async move {
            while let true = Tree::flush_once(i.clone(), txg).await? {
            }
            Ok(())
        }
    }

    /// Progressively flush all in-memory Nodes to disk.
    ///
    /// Each invocation will flush the lowest echelon of dirty nodes.  So
    /// multiple invocations are necessary to flush an entire Tree.
    ///
    /// Note that the tree is not locked during flush, so by the time it flush
    /// finishes, the tree may have already been re-dirtied.
    ///
    /// # Returns
    ///
    /// `false` is the Tree is no longer dirty, `true` otherwise.  Of course,
    /// absent locking it may immediately become dirtied again.
    // flush_once scans through the Tree in Key order, flushing as it goes.
    // That means that it can only flush the lowest dirty echelon of any given
    // branch of the tree.  Alternatively, it could start over at Key 0 on each
    // pass.  That would allow it to flush the entire Tree on a single
    // invocation.  However, that would be unfair.  It would bias the flushing
    // process toward low keys.  It would also flush low keyed Int nodes before
    // high key Leaf nodes.  That would be inefficient, because it's likely that
    // the Int nodes could become redirtied again quickly.  Better to treat all
    // keys fairly, and to flush leaf nodes before int nodes.
    fn flush_once(i: Arc<Inner<A, D, K, V>>, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<bool, Error>> + Send>>
    {
        let dml = i.dml.clone();
        let lcomp = i.leaf_compressor;
        let icomp = i.int_compressor;
        let i2 = i.clone();

        stream::try_unfold((true, K::min_value()), move |(more, lowest)|
        {
            let i3 = i2.clone();
            let dml2 = dml.clone();
            async move {
                if !more {
                    Ok(None)
                } else {
                    let rg = Tree::write_root(&i3).await;
                    if rg.ptr.is_dirty() {
                        let (mut rg, guard) = Tree::xlock_root(&dml2, rg, txg)
                            .await?;
                        if guard.has_dirty_children() {
                            let height = i3.height.load(Ordering::Relaxed);
                            debug_assert!(height > 1);
                            drop(rg);
                            // It's ok to use height here even after dropping
                            // rg.  The height may only ever change at the root
                            // node.  If the root node grows, the tree height
                            // will grow but the height of guard will not.
                            Tree::flush_r(dml2, guard, lcomp, icomp, height, txg,
                                          lowest).await
                            .map(|kopt| kopt.map(|k| (true, (true, k))))
                        } else {
                            let height = i3.height.load(Ordering::Relaxed);
                            if height == 1 {
                                drop(guard);
                                let old_ptr = mem::replace(&mut rg.ptr,
                                                           TreePtr::None);
                                let addr = Tree::write_leaf(dml2, lcomp,
                                    *old_ptr.into_node(), txg)
                                    .await?;
                                let _ = mem::replace(&mut rg.ptr,
                                                     TreePtr::Addr(addr));
                                rg.txgs = txg .. txg + 1;
                                Ok(Some((false, (false, lowest))))
                            } else {
                                let start_txg = guard.as_int()
                                    .children.iter()
                                    .map(|e| e.txgs.start)
                                    .min()
                                    .unwrap();
                                drop(guard);
                                let rptr = mem::replace(&mut rg.ptr, TreePtr::None);
                                let rnode = *rptr.into_node();
                                let a = dml2.put(Arc::new(rnode), icomp, txg)
                                    .await?;
                                let _ = mem::replace(&mut rg.ptr, TreePtr::Addr(a));
                                let txgs = start_txg .. txg + 1;
                                rg.txgs = txgs;
                                Ok(Some((false, (false, lowest))))
                            }
                        }
                    } else {
                        Ok(Some((false, (false, lowest))))
                    }
                }
            }
        }).try_fold(true, |_acc, more_to_do| future::ok(more_to_do)).boxed()
    }

    /// Flush all of the children of the given node.
    ///
    /// They must all be leaves, which is to say that the node must be a
    /// terminal int node.
    async fn flush_leaves(dml: Arc<D>, leaf_compressor: Compression,
        mut node: TreeWriteGuard<A, K, V>, txg: TxgT)
        -> Result<Option<K>, Error>
    {
        let int = node.as_int_mut();
        // TODO: write leaves concurrently
        for elem in int.children.iter_mut() {
            if elem.is_dirty() {
                // If the child is dirty, then we have ownership over it.  We
                // need to lock it, then release the lock.  Then we'll know that
                // we have exclusive access to it, and we can move it into the
                // Cache.
                let dml3 = dml.clone();
                let guard = elem.ptr.as_mem().xlock().await;
                drop(guard);
                let old_ptr = mem::replace(&mut elem.ptr, TreePtr::None);
                let addr = Tree::write_leaf(dml3, leaf_compressor,
                    *old_ptr.into_node(), txg)
                    .await?;
                let txgs = txg .. txg + 1;
                let _ = mem::replace(&mut elem.ptr, TreePtr::Addr(addr));
                elem.txgs = txgs;
            }
        }
        debug_assert_eq!(int.children.iter()
            .map(|child| child.txgs.end)
            .max()
            .unwrap(),
            txg + 1,
            "called flush_leaves on a node with no dirty children"
        );
        Ok(None)
    }

    /// Progressive flush beginning in the node `guard`.
    ///
    /// `height` is the tree height of `guard`.  1 means that `guard` is a leaf.
    ///
    /// # Returns
    ///
    /// `None` if this node and all of its children have been flushed.
    /// `Some(k)` to indicate some node with key `k` has not yet been flushed.
    fn flush_r(
        dml: Arc<D>,
        mut guard: TreeWriteGuard<A, K, V>,
        leaf_compressor: Compression,
        int_compressor: Compression,
        height: u64,
        txg: TxgT,
        lowest: K)
        -> Pin<Box<dyn Future<Output=Result<Option<K>, Error>> + Send>>
    {
        debug_assert!(height >= 2);

        if height == 2 {
            return Tree::flush_leaves(dml, leaf_compressor, guard, txg).boxed();
        }

        let int = guard.as_int_mut();
        let mut idx = int.position(&lowest);
        while idx < int.children.len() && !int.children[idx].is_dirty() {
            idx += 1;
        }
        if idx >= int.children.len() {
            return future::ok(None).boxed();
        }
        async move {
            let int = guard.as_int_mut();
            let next_key = int.children.get(idx + 1).map(|c| c.key);
            let elem = &mut int.children[idx].ptr; 
            let child = elem.as_mem().xlock().await;
            if !child.has_dirty_children() {
                let start_txg = child.as_int().children.iter()
                    .map(|e| e.txgs.start)
                    .min()
                    .unwrap();
                drop(child);
                let node = *mem::replace(elem, TreePtr::None).into_node();
                let a = dml.put(Arc::new(node), int_compressor, txg).await?;
                let _ = mem::replace(elem, TreePtr::Addr(a));
                let txgs = start_txg .. txg + 1;
                int.children[idx].txgs = txgs;
                Ok(next_key)
            } else {
                drop(guard);
                let child_next_key = Tree::flush_r(dml, child,
                    leaf_compressor, int_compressor, height - 1, txg, lowest)
                    .await?;
                Ok(child_next_key.or(next_key))
            }
        }.boxed()
    }

    #[cfg(test)]
    pub fn from_str(dml: Arc<D>, seq: bool, s: &str) -> Self {
        Tree{i: Arc::new(Inner::from_str(dml, seq, s))}
    }

    /// Lookup the value of key `k`.  Return `None` if no value is present.
    pub fn get(&self, k: K) -> impl Future<Output=Result<Option<V>, Error>>
    {
        let dml2 = self.i.dml.clone();
        self.read()
            .then(move |tree_guard| {
                tree_guard.rlock(&dml2)
                     .and_then(move |guard| {
                         drop(tree_guard);
                         Tree::get_r(dml2, guard, k)
                     })
            })
    }

    /// Lookup the value of key `k` in a node, which must already be locked.
    fn get_r(dml: Arc<D>, node: TreeReadGuard<A, K, V>, k: K)
        -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>
    {
        let next_node_fut = match *node {
            NodeData::Leaf(ref leaf) => {
                return future::ok(leaf.get(&k)).boxed();
            },
            NodeData::Int(ref int) => {
                let child_elem = &int.children[int.position(&k)];
                child_elem.rlock(&dml)
            }
        };
        next_node_fut
        .and_then(move |next_node| {
            drop(node);
            Tree::get_r(dml, next_node, k)
        }).boxed()
    }

    /// Private helper for `RangeQuery::poll_next`.  Returns a subset of the
    /// total results, consisting of all matching (K,V) pairs within a single
    /// Leaf Node, plus an optional Bound for the next iteration of the search.
    /// If the Bound is `None`, then the search is complete.
    fn get_range<R, T>(inner: &Inner<A, D, K, V>, range: R)
        -> impl Future<Output=Result<(VecDeque<(K, V)>, Option<Bound<T>>),
                       Error>> + Send
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + Send + 'static
    {
        let dml2 = inner.dml.clone();
        Tree::<A, D, K, V>::read_root(&inner)
            .then(move |tree_guard| {
                tree_guard.rlock(&dml2)
                     .and_then(move |g| {
                         drop(tree_guard);
                         Tree::get_range_r(dml2, g, None, range)
                     })
            })
    }   // LCOV_EXCL_LINE kcov false negative

    /// Range lookup beginning in the node `guard`.  `next_guard`, if present,
    /// must be the node immediately to the right (and possibly up one or more
    /// levels) from `guard`.
    fn get_range_r<R, T>(dml: Arc<D>, guard: TreeReadGuard<A, K, V>,
                         next_guard: Option<TreeReadGuard<A, K, V>>, range: R)
        -> Pin<Box<dyn Future<Output=Result<(VecDeque<(K, V)>, Option<Bound<T>>),
                          Error>> + Send>>
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + Send + 'static
    {
        let dml2 = dml.clone();
        let (child_fut, next_fut) = match *guard {
            NodeData::Leaf(ref leaf) => {
                let (v, more) = leaf.range(range.clone());
                let ret = match next_guard {
                    Some(ng) if v.is_empty() && more => {
                        // We must've started the query with a key that's not
                        // present, and lies between two leaves.  Check the next
                        // node
                        Tree::get_range_r(dml2, ng, None, range)
                    },
                    _ if v.is_empty() => {
                        // The range is truly empty
                        future::ok((v, None)).boxed()
                    },
                    Some(ng) if more => {
                        let bound = Bound::Included(ng.key().borrow().clone());
                        future::ok((v, Some(bound))).boxed()
                    },
                    None if more => {
                        let t = leaf.last_key().unwrap().borrow().clone();
                        let bound = Bound::Excluded(t);
                        future::ok((v, Some(bound))).boxed()
                    },
                    _ => {
                        future::ok((v, None)).boxed()
                    }
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
                    let next_key = int.children[child_idx + 1].key;
                    if match range.end_bound() {
                        Bound::Included(x) => next_key.borrow() <= x,
                        Bound::Excluded(x) => next_key.borrow() < x,
                        Bound::Unbounded => true
                    } {
                        let eb: Bound<T> = match range.end_bound() {
                            Bound::Included(x) => Bound::Included(x.clone()),
                            Bound::Excluded(x) => Bound::Excluded(x.clone()),
                            Bound::Unbounded => Bound::Unbounded
                        };
                        int.children[child_idx + 1].rlock(&dml2)
                        .map_ok(move |g| {
                            // The minimum key rule means that even though
                            // we checked the bounds above, we must also
                            // check them after rlock()ing, as the guard's
                            // key may be higher than its parent pointer's
                            if match eb {
                                Bound::Included(x) => *g.key().borrow() <= x,
                                Bound::Excluded(x) => *g.key().borrow() < x,
                                Bound::Unbounded => true
                            } {
                                Some(g)
                            } else {
                                None
                            }
                        }).boxed()
                    } else {
                        future::ok(None).boxed()
                    }
                } else {
                    future::ok(next_guard).boxed()
                };
                let child_fut = child_elem.rlock(&dml2);
                (child_fut, next_fut)
            } // LCOV_EXCL_LINE kcov false negative
        };
        future::try_join(child_fut, next_fut)
        .and_then(move |(child_guard, next_guard)| {
            drop(guard);
            Tree::get_range_r(dml, child_guard, next_guard, range)
        }).boxed()
    }

    /// Insert value `v` into the tree at key `k`, returning the previous value
    /// for that key, if any.
    pub fn insert(&self, k: K, v: V, txg: TxgT)
        -> impl Future<Output=Result<Option<V>, Error>>
    {
        let inner2 = self.i.clone();
        self.write()
            .then(move |guard| {
                Tree::xlock_root(&inner2.dml, guard, txg)
                     .and_then(move |(root_guard, child_guard)| {
                         Tree::insert_locked(inner2, root_guard,
                                             child_guard, k, v, txg)
                     })
            })
    }

    /// Insert value `v` into an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn insert_int(inner: Arc<Inner<A, D, K, V>>,
                  mut parent: TreeWriteGuard<A, K, V>, child_idx: usize,
                  mut child: TreeWriteGuard<A, K, V>, k: K, v: V, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>
    {
        // First, split the node, if necessary
        if (*child).should_split(&k, &inner.limits) {
            let seq = inner.sequentially_optimized;
            let (old_txgs, new_elem) = child.split(&inner.limits, seq, txg);
            parent.as_int_mut().children[child_idx].txgs = old_txgs;
            parent.as_int_mut().children.insert(child_idx + 1, new_elem);
            // Reinsert into the parent, which will choose the correct child
            Box::pin(Tree::insert_int_no_split(inner, parent, k, v, txg))
        } else if child.is_leaf() {
            let elem = &mut parent.as_int_mut().children[child_idx];
            Box::pin(Tree::<A, D, K, V>::insert_leaf_no_split(elem,
                child, k, v, txg))
        } else {
            drop(parent);
            Box::pin(Tree::insert_int_no_split(inner, child, k, v, txg))
        }
    }

    /// Insert a value into an int node without splitting it
    fn insert_int_no_split(inner: Arc<Inner<A, D, K, V>>,
                           mut node: TreeWriteGuard<A, K, V>, k: K, v: V,
                           txg: TxgT)
        -> impl Future<Output=Result<Option<V>, Error>>
    {
        let child_idx = node.as_int().position(&k);
        if k < node.as_int().children[child_idx].key {
            debug_assert_eq!(child_idx, 0);
            node.as_int_mut().children[child_idx].key = k;
        }
        let fut = node.xlock(&inner.dml, child_idx, txg);
        fut.and_then(move |(parent, child)| {
                Tree::insert_int(inner, parent, child_idx, child, k, v, txg)
        })
    }

    /// Insert a value into a leaf node without splitting it
    fn insert_leaf_no_split(elem: &mut IntElem<A, K, V>,
                  mut child: TreeWriteGuard<A, K, V>, k: K, v: V, txg: TxgT)
        -> impl Future<Output=Result<Option<V>, Error>>
    {
        let old_v = child.as_leaf_mut().insert(k, v);
        elem.txgs = txg..txg + 1;
        future::ok(old_v)
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Helper for `insert`.  Handles insertion once the tree is locked
    fn insert_locked(inner: Arc<Inner<A, D, K, V>>,
                     mut relem: RwLockWriteGuard<IntElem<A, K, V>>,
                     mut rnode: TreeWriteGuard<A, K, V>, k: K, v: V, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>
    {
        // First, split the root node, if necessary
        if rnode.should_split(&k, &inner.limits) {
            let seq = inner.sequentially_optimized;
            let (old_txgs, new_elem) = rnode.split(&inner.limits, seq, txg);
            let new_root_data = NodeData::Int( IntData::new(vec![new_elem]));
            let old_root_data = mem::replace(rnode.deref_mut(), new_root_data);
            let old_root_node = Node::new(old_root_data);
            let old_ptr = TreePtr::Mem(Box::new(old_root_node));
            let old_elem = IntElem::new(K::min_value(), old_txgs, old_ptr );
            rnode.as_int_mut().children.insert(0, old_elem);
            inner.height.fetch_add(1, Ordering::Relaxed);
        }

        if rnode.is_leaf() {
            Tree::<A, D, K, V>::insert_leaf_no_split(&mut *relem, rnode, k, v,
                txg).boxed()
        } else {
            drop(relem);
            Tree::insert_int_no_split(inner, rnode, k, v, txg).boxed()
        }
    }

    /// Has the Tree been modified since the last time it was flushed to disk?
    pub fn is_dirty(&self) -> bool {
        // If the root IntElem is not dirty, then the Tree isn't dirty.  If we
        // can't get the lock, then somebody else must have it locked for
        // writing, which means that it must be dirty.
        self.i.root.try_read()
        .map(|guard| guard.is_dirty())
        .unwrap_or(true)
    }

    /// Return the highest valued key in the `Tree`
    pub fn last_key(&self) -> impl Future<Output=Result<Option<K>, Error>> {
        let dml2 = self.i.dml.clone();
        self.read()
            .then(move |tree_guard| {
                tree_guard.rlock(&dml2)
                     .and_then(move |guard| {
                         drop(tree_guard);
                         Tree::last_key_r(dml2, guard)
                     })
            })
    }

    /// Find the last key amongst a node (which must already be locked), and its
    /// children
    fn last_key_r(dml: Arc<D>, node: TreeReadGuard<A, K, V>)
        -> Pin<Box<dyn Future<Output=Result<Option<K>, Error>> + Send>>
    {
        let next_node_fut = match *node {
            NodeData::Leaf(ref leaf) => {
                return future::ok(leaf.last_key()).boxed()
            },
            NodeData::Int(ref int) => {
                let child_elem = &int.children[int.children.len() - 1];
                child_elem.rlock(&dml)
            }
        };
        next_node_fut
        .and_then(move |next_node| {
            drop(node);
            Tree::last_key_r(dml, next_node)
        }).boxed()
    }

    fn new(dml: Arc<D>, limits: Limits, seq: bool) -> Self {
        // Since there are no on-disk children, the initial TXG range is empty
        let root_elem = IntElem::default();
        let inner = Inner::new(dml, 1, limits, root_elem, seq);
        let i = Arc::new(inner);
        Tree{ i }
    }

    /// Open a `Tree` from its serialized representation
    pub fn open(dml: Arc<D>, seq: bool, on_disk: TreeOnDisk<A>) -> Self {
        let i = Inner::open(dml, seq, on_disk);
        Tree{i: Arc::new(i)}
    }

    /// Lookup a range of (key, value) pairs for keys within the range `range`.
    pub fn range<R, T>(&self, range: R) -> RangeQuery<A, D, K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T>,
              T: Ord + Clone + Send
    {
        // Sanity check the arguments
        match (range.start_bound(), range.end_bound()) {
            (Bound::Included(s), Bound::Included(e)) => debug_assert!(s <= e),
            (Bound::Included(s), Bound::Excluded(e)) => debug_assert!(s <= e),
            (Bound::Excluded(s), Bound::Included(e)) => debug_assert!(s < e),
            (Bound::Excluded(s), Bound::Excluded(e)) => debug_assert!(s < e),
            _ => ()
        };
        RangeQuery::new(self.i.clone(), range)
    }

    /// Delete a range of keys
    //
    // Based on the algorithm from [^EXODUS], pp 13-15.
    //
    // [^EXODUS]: Carey, Michael J., et al. "Storage management for objects in
    // EXODUS." Object-oriented concepts, databases, and applications (1989):
    // 341-369
    pub fn range_delete<R, T>(&self, range: R, txg: TxgT)
        -> impl Future<Output=Result<(), Error>> + Send
        where K: Borrow<T>,
              R: Debug + Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + 'static + Debug
    {
        // Sanity check the arguments
        match (range.start_bound(), range.end_bound()) {
            (Bound::Included(s), Bound::Included(e)) => debug_assert!(s <= e),
            (Bound::Included(s), Bound::Excluded(e)) => debug_assert!(s < e),
            (Bound::Excluded(s), Bound::Included(e)) => debug_assert!(s < e),
            (Bound::Excluded(s), Bound::Excluded(e)) => debug_assert!(s < e),
            _ => ()
        };
        // Outline:
        // 1) Traverse the tree removing all requested KV-pairs, leaving damaged
        //    nodes, deleting empty nodes, and recording which nodes are
        //    in-danger.
        // 2) Traverse the tree again, fixing underflowing nodes.
        let rangeclone = range.clone();
        let dml2 = self.i.dml.clone();
        let inner2 = self.i.clone();
        let inner3 = self.i.clone();
        let limits = self.i.limits;
        self.write()
            .then(move |guard| {
                Tree::xlock_root(&dml2, guard, txg)
                    .and_then(move |(tree_guard, root_guard)| {
                        let height = inner2.height.load(Ordering::Relaxed) as u8;
                        // ptr is guaranteed to be a TreePtr::Mem because we
                        // just xlock()ed it.
                        let id = tree_guard.ptr.as_mem()
                            as *const Node<A, K, V> as usize;
                        Tree::range_delete_pass1(limits, dml2,
                                                 height - 1, root_guard, range,
                                                 None, txg)
                            .map_ok(move |(mut m, danger, _)| {
                                if danger {
                                    m.insert(id);
                                }
                                (tree_guard, m)
                            })
                    })
            }).and_then(move |(tree_guard, m)| {
                Tree::range_delete_pass2(inner3, tree_guard, m, rangeclone, txg)
            })
    }

    /// Subroutine of range_delete.  Returns the bounds, as indices, of the
    /// affected children of this node.
    ///
    /// # Returns
    ///
    /// A pair of bounds that describe the range of affected children.  The
    /// start bound will be either `Included(i)`, signifying that the `ith`
    /// child is affected from its first child, or `Excluded(i)`, signifying
    /// that the `ith` child is affected but may have some unaffected children
    /// of its own.  The end bound will be either `Included(i)`, signifying that
    /// the `ith` node will be affected but may have some unaffected children,
    /// or `Excluded(i)`, signifying that the `ith` child is the lowest
    /// unaffected child, and the next lower child will be entirely deleted.
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
            Bound::Unbounded => Bound::Excluded(l),
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

    /// First pass of range_delete based on [^EXODUS].
    ///
    /// Descends through the tree removing all requested KV-pairs, leaving
    /// damaged nodes, deleting empty nodes, and recording which nodes are
    /// in-danger.  A node is in-danger if:
    /// a) It has an underflow, or
    /// b) It has b entries and one child that's in danger, or
    /// c) It has b + 1 entries and two children that are in danger
    ///
    /// # Parameters
    ///
    /// - `height`: The height of `guard`, where leaves are 0
    /// - `ubound`: The first key in the Node immediately to the right of this
    ///             one, unless this is the rightmost Node on its level.
    ///
    /// # Returns
    /// `
    /// A HashSet indicating which nodes in the cut are in-danger, indexed by
    /// the Node's memory address.
    fn range_delete_pass1<R, T>(limits: Limits, dml: Arc<D>,
                                height: u8, mut guard: TreeWriteGuard<A, K, V>,
                                range: R, ubound: Option<K>, txg: TxgT)
        -> impl Future<Output=Result<(HashSet<usize>, bool, usize),
                       Error>> + Send
        where K: Borrow<T>,
              R: Debug + Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + 'static + Debug
    {
        // Enough for two nodes on each of six levels of a tree
        const HASHSET_CAPACITY: usize = 12;

        if height == 0 {
            debug_assert!(guard.is_leaf());
            guard.as_leaf_mut().range_delete(range);
            let map = HashSet::<usize>::with_capacity(HASHSET_CAPACITY);
            let danger = guard.underflow(&limits);
            return future::ok((map, danger, guard.len())).boxed()
        } else {
            debug_assert!(!guard.is_leaf());
        }

        let (start_idx_bound, end_idx_bound)
            = Tree::<A, D, K, V>::range_delete_get_bounds(&guard, &range,
                                                          ubound);

        let left_in_cut = match start_idx_bound {
            Bound::Included(_) => None,
            Bound::Excluded(i) => Some(i),
            _ => unreachable!() // LCOV_EXCL_LINE unreachable
        };
        let right_in_cut = match end_idx_bound {
            Bound::Included(j) => Some(j),
            Bound::Excluded(_) => None,
            _ => unreachable!() // LCOV_EXCL_LINE unreachable
        };
        let wholly_deleted = match (start_idx_bound, end_idx_bound) {
            (Bound::Included(i), Bound::Excluded(j)) => i..j,
            (Bound::Included(i), Bound::Included(j)) => i..j,
            (Bound::Excluded(i), Bound::Excluded(j)) => (i + 1)..j,
            (Bound::Excluded(i), Bound::Included(j)) => (i + 1)..j,
            _ => unreachable!() // LCOV_EXCL_LINE unreachable
        };
        let wholly_deleted2 = wholly_deleted.clone();

        match (left_in_cut, right_in_cut) {
            (Some(i), Some(j)) if i == j => {
                debug_assert!(i <= guard.len());
                // Haven't found the lowest common ancestor (LCA) yet.  Keep
                // recursing
                let next_ubound = if i == guard.len() - 1 {
                    ubound
                } else {
                    Some(guard.as_int().children[i + 1].key)
                };
                return guard.xlock(&dml, i, txg)
                    .and_then(move |(mut parent_guard, child_guard)| {
                        Tree::range_delete_pass1(limits, dml, height - 1,
                            child_guard, range, next_ubound, txg)
                        .map_ok(move |(mut m, mut child_danger, child_len)| {
                            if child_len == 0 {
                                // Sometimes we delete every key in LCA of of
                                // the cut, but we don't know we'll do that
                                // until we've tried.  This is because parents
                                // may store a key less than any key actually
                                // contained in the child.
                                parent_guard.as_int_mut().children.remove(i);
                                child_danger = false;
                            } else {
                                // We just xlock()ed the child, so it's
                                // guaranteed to be a TreePtr::Mem
                                let id = parent_guard.as_int().children[i].ptr
                                    .as_mem() as *const Node<A, K, V> as usize;
                                if child_danger {
                                    m.insert(id);
                                }
                            }
                            let l = parent_guard.len();
                            let danger = parent_guard.in_danger(&limits)
                                || child_danger;
                            (m, danger, l)
                        })
                    }).boxed()
            },
            _ => ()
        }

        // We found the LCA.  Descend along both sides of the cut.
        let dml2 = dml.clone();
        let dml3 = dml.clone();
        let range2 = range.clone();
        let left_fut = if let Some(i) = left_in_cut {
            let new_ubound = guard.as_int().children.get(i + 1)
                .map_or(ubound, |elem| Some(elem.key));
            guard.xlock_nc(&dml2, i, height, txg)
            .and_then(move |(elem, child_guard)| {
                Tree::range_delete_pass1(limits, dml2, height - 1,
                                         child_guard, range, new_ubound,
                                         txg)
                .map_ok(move |(m, danger, child_len)|
                     (elem, m, Some(danger), Some(child_len))
                )
            }).boxed()
        } else {
            let map = HashSet::<usize>::with_capacity(HASHSET_CAPACITY);
            future::ok((None, map, None, None)).boxed()
        };
        let right_fut = if let Some(j) = right_in_cut {
            let new_ubound = guard.as_int().children.get(j + 1)
                .map_or(ubound, |elem| Some(elem.key));
            guard.xlock_nc(&dml3, j, height, txg)
            .and_then(move |(elem, child_guard)| {
                Tree::range_delete_pass1(limits, dml3, height - 1,
                                         child_guard, range2, new_ubound,
                                         txg)
                .map_ok(move |(m, danger, child_len)|
                     (elem, m, Some(danger), Some(child_len))
                )
            }).boxed()
        } else {
            let map = HashSet::<usize>::with_capacity(HASHSET_CAPACITY);
            future::ok((None, map, None, None)).boxed()
        };
        let middle_fut = Tree::range_delete_purge(dml, height, wholly_deleted,
                                                  guard, txg);
        future::try_join3(left_fut, middle_fut, right_fut)
        .map_ok(move |((lelem, mut lmap, ldanger, llen),
                    mut guard,
                    (relem, rmap, rdanger, rlen))|
        {
            let mut deleted_on_left = 0;
            lmap.extend(rmap.into_iter());
            if let Some(elem) = lelem {
                guard.as_int_mut().children[left_in_cut.unwrap()] = elem;
            }
            if llen == Some(0) {
                // Sometimes we delete every key in the node on the left
                // side of the cut, but we don't know we'll do that until
                // we've tried.  This is because parents may store a key
                // less than any key actually contained in the child.
                guard.as_int_mut().children.remove(left_in_cut.unwrap());
                deleted_on_left += 1;
            } else if let Some(danger) = ldanger {
                let elem = &guard.as_int().children[left_in_cut.unwrap()];
                // elem.ptr is guaranteed to be a TreePtr::Mem because we
                // got it from xlock_nc() above.
                let id = elem.ptr.as_mem() as *const Node<A, K, V> as usize;
                if danger {
                    lmap.insert(id);
                }
            }
            let deleted = wholly_deleted2.end - wholly_deleted2.start;
            if let Some(elem) = relem {
                let j = right_in_cut.unwrap() - deleted - deleted_on_left;
                guard.as_int_mut().children[j] = elem;
            }
            if let Some(danger) = rdanger {
                let j = right_in_cut.unwrap() - deleted - deleted_on_left;
                if rlen == Some(0) {
                    // Sometimes we delete every key in the node on the
                    // right side of the cut, but we don't know we'll do
                    // that until we've tried.  This is because parents only
                    // store their childrens' minimum keys, not maximum
                    // keys.
                    guard.as_int_mut().children.remove(j);
                } else {
                    let elem = &guard.as_int().children[j];
                    // elem.ptr is guaranteed to be a TreePtr::Mem because
                    // we got it from xlock_nc() above.
                    let id = elem.ptr.as_mem()
                        as *const Node<A, K, V> as usize;
                    if danger {
                        lmap.insert(id);
                    }
                }
            }
            let danger = guard.underflow(&limits) ||
                ldanger == Some(true) ||
                rdanger == Some(true);
            (lmap, danger, guard.len())
        }).boxed()
    }

    /// Reshape the tree after some keys were deleted by range_delete_pass1.
    //
    // Unlike most Tree methods, this doesn't use lock-coupling.  It keeps the
    // whole Tree locked.  Also, it operates partially bottom-up.  First it
    // descends all the way to the leaves, fixing any nodes with < 2 children.
    // On the way back up, it fixes every underflowing node.  This:
    // * Uses the same RAM as a top-down approach, since nothing is flushed
    //   until it's done.
    // * Performs less I/O, because it only fixes nodes that actually underflow,
    //   as opposed to nodes that might underflow if their children are merged.
    // * Has no less concurrency, because even a top-down approach keeps the
    //   whole Tree locked.
    fn range_delete_pass2<R, T>(inner: Arc<Inner<A, D, K, V>>,
        mut tree_guard: RwLockWriteGuard<IntElem<A, K, V>>, map: HashSet<usize>,
        range: R, txg: TxgT)
        -> impl Future<Output=Result<(), Error>> + Send
        where K: Borrow<T>,
              R: Debug + Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + 'static + Debug
    {
        let inner2 = inner.clone();
        let inner3 = inner.clone();
        // Keep merging down the root as long as it has 1 child
        async move {
            loop {
                let (tree_guard2, root_guard) =
                    Tree::xlock_and_merge_root(inner.clone(), tree_guard, txg)
                    .await?;
                if root_guard.is_leaf() || root_guard.as_int().nchildren() > 1
                {
                    break Ok((tree_guard2, Some(root_guard)));
                } else {
                    tree_guard = tree_guard2;
                }
            }
        }.and_then(move |(tree_guard, root_guard)| {
            Tree::range_delete_pass2_r(inner2, root_guard.unwrap(), map, range,
                                       txg)
            .map_ok(|_| tree_guard)
        }).and_then(move |mut tree_guard| async move {
            // Keep merging down the root as long as it has 1 child
            loop {
                let (tree_guard2, root_guard) = 
                    Tree::xlock_and_merge_root(inner3.clone(), tree_guard, txg)
                    .await?;
                if root_guard.is_leaf() || root_guard.as_int().nchildren() > 1 {
                    break Ok(());
                } else {
                    tree_guard = tree_guard2;
                }
            }
            //future::loop_fn(tree_guard, move |tree_guard| {
                //Tree::xlock_and_merge_root(inner3.clone(), tree_guard, txg)
                //.and_then(move |(tree_guard, root_guard)| {
                    //if !root_guard.is_leaf() &&
                        //root_guard.as_int().nchildren() == 1 {
                        //Ok(Loop::Continue(tree_guard))
                    //} else {
                        //Ok(Loop::Break(tree_guard))
                    //}
                //})
            //})
        })
    }

    #[allow(clippy::unnecessary_unwrap)]
    fn range_delete_pass2_r<R, T>(inner: Arc<Inner<A, D, K, V>>,
        guard: TreeWriteGuard<A, K, V>, mut map: HashSet<usize>, range: R,
        txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<HashSet<usize>, Error>> + Send>>
        where K: Borrow<T>,
              R: Debug + Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + 'static + Debug
    {
        if guard.is_leaf() {
            future::ok(map).boxed()
        } else {
            // Descend into every affected child
            let mut to_fix = Vec::with_capacity(2);
            to_fix.extend((0..guard.len())
            .filter(|i| {
                if let TreePtr::Mem(p) = &guard.as_int().children[*i].ptr {
                    let id = &**p as *const Node<A, K, V> as usize;
                    map.remove(&id)
                } else {
                    // This child obviously wasn't affected in pass1
                    false
                }
            }).take(2));    // No more than two children can be in the cut
            if to_fix.is_empty() {
                return future::ok(map).boxed()
            }
            let inner3 = inner.clone();
            let inner7 = inner.clone();
            let limits2 = inner.limits;
            let range3 = range.clone();
            let left_idx = to_fix[0];
            let right_idx = to_fix.get(1).cloned();
            let underflow = move |guard: &TreeWriteGuard<A, K, V>, common: bool|
            {
                if guard.is_leaf() {
                    guard.underflow(&limits2)
                } else {
                    // We need to fix IntNodes that might underflow if their
                    // children get merged.
                    if common {
                        // If this node is a common ancestor of all affected
                        // nodes, then it may have up to 2 children that get
                        // merged
                        guard.in_extra_danger(&limits2)
                    } else {
                        // If it's not a common ancestor, then at most one child
                        // will get merged
                        guard.in_danger(&limits2)
                    }
                }
            };
            let fut = guard.xlock(&inner.dml, left_idx, txg)
            .and_then(move |(guard, child_guard)| async move {
                // After a range_delete, fixing once may be insufficient to
                // fix a Node, because two nodes may merge that could have
                // as few as one child each.  We may need to fix multiple times.
                let mut state = (guard, child_guard, 0, 0, false);
                loop {
                    let (guard, child_guard, mb, mm, stole) = state;
                    // The left node may have become a common ancestor if we
                    // merged it with the right node, or if we stole some nodes
                    // from the right node.
                    let common = right_idx.is_none() || mm > 0 || stole;
                    // Don't fix if it has no siblings; that can happen if we
                    // need to merge the root down.
                    if !underflow(&child_guard, common) || guard.len() <= 1 {
                        break Ok((guard, child_guard, mb, mm));
                    }
                    let i = left_idx - mb;
                    let inner8 = inner7.clone();
                    match Tree::fix_int(&inner7, guard, i, child_guard, txg)
                        .await
                    {
                        Err(e) => {
                            break Err(e);
                        },
                        Ok((guard, nmb, nma)) => {
                            match guard.xlock(&inner8.dml, i - nmb as usize,
                                              txg)
                                .await
                            {
                                Err(e) => {
                                    break Err(e);
                                },
                                Ok((guard, child_guard)) => {
                                    let mb = mb + nmb as usize;
                                    let mm = mm + nma as usize;
                                    let stole = (nma == 0 && nmb == 0) || stole;
                                    state = (guard, child_guard, mb, mm, stole);
                                }
                            }
                        }
                    }
                }
            }).and_then(move |(guard, child_guard, mb, mm):
                (TreeWriteGuard<A, K, V>, TreeWriteGuard<A, K, V>, usize, usize)|
            {
                // Recurse into the left child
                Tree::range_delete_pass2_r(inner3, child_guard, map,
                                           range3, txg)
                .map_ok(move |map| (guard, map, mb, mm))
            }).and_then(move |(guard, map, mb, mm)| {
                // Fix into the right node, if it exists and was not merged
                // with the left node
                if right_idx.is_some() && mm == 0 {
                    let inner8 = inner.clone();
                    let j = right_idx.unwrap() - mb;
                    let fut = guard.xlock(&inner.dml, j, txg)
                    .and_then(move |(guard, child_guard)| {
                        if underflow(&child_guard, false) {
                            let fut = Tree::fix_int(&inner, guard, j,
                                                    child_guard, txg)
                            .and_then(move |(guard, nmb, _nma)| {
                                guard.xlock(&inner.dml, j - nmb as usize, txg)
                                .map_ok(move |(guard, child_guard)|
                                     (guard, child_guard, mb, mm + nmb as usize)
                                 )
                            });
                            fut.boxed()
                        } else {
                            future::ok((guard, child_guard, mb, mm)).boxed()
                        }
                    }).and_then(move |(guard, child_guard, mb, mm)| {
                        // Now recurse into the right child
                        Tree::range_delete_pass2_r(inner8, child_guard, map,
                                                   range, txg)
                        .map_ok(move |map| (guard, map, mb, mm))
                    });
                    fut.boxed()
                } else {
                    future::ok((guard, map, mb, mm)).boxed()
                }
            }).map_ok(|(_guard, map, _mb, _mm)| map);
            fut.boxed()
        }
    }

    /// Delete the indicated range of children from the Tree
    fn range_delete_purge(dml: Arc<D>, height: u8, range: Range<usize>,
                          mut guard: TreeWriteGuard<A, K, V>, txg: TxgT)
        -> impl Future<Output=Result<TreeWriteGuard<A, K, V>, Error>> + Send
    {
        if height == 0 {
            // Simply delete the leaves
            debug_assert!(guard.is_leaf());
            future::ok(guard).boxed()
        } else if height == 1 {
            debug_assert!(!guard.is_leaf());
            // If the child elements point to leaves, just delete them.
            guard.as_int_mut().children.drain(range)
            .map(move |elem| {
                if let TreePtr::Addr(addr) = elem.ptr {
                    // Delete on-disk leaves
                    dml.delete(&addr, txg).boxed()
                } else {
                    // Simply drop in-memory leaves
                    future::ok(()).boxed()
                }
            }).collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .map_ok(|_| guard)
            .boxed()
        } else {
            debug_assert!(!guard.is_leaf());
            // If the IntElems point to IntNodes, we must recurse
            let fut = guard.drain_xlock(dml, range, txg, move |guard, dml| {
                let range = 0..guard.len();
                Tree::range_delete_purge(dml.clone(), height - 1, range, guard,
                                         txg)
            }).map_ok(|(guard, _)| guard);
            fut.boxed()
        }
    }

    /// Lock the Tree for reading
    fn read(&self) -> impl Future<Output=RwLockReadGuard<IntElem<A, K, V>>>
    {
        Tree::<A, D, K, V>::read_root(&self.i)
    }

    fn read_root(inner: &Inner<A, D, K, V>)
        -> impl Future<Output=RwLockReadGuard<IntElem<A, K, V>>>
    {
        inner.root.read()
    }

    /// Remove and return the value at key `k`, if any.
    pub fn remove(&self, k: K, txg: TxgT)
        -> impl Future<Output=Result<Option<V>, Error>> + Send
    {
        let i2 = self.i.clone();
        self.write()
            .then(move |tree_guard| {
                Tree::remove_locked(i2, tree_guard, k, txg)
            })
    }

    /// Remove key `k` from an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn remove_int(inner: Arc<Inner<A, D, K, V>>,
                  parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize, child: TreeWriteGuard<A, K, V>, k: K,
                  txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>
    {
        // First, fix the node, if necessary.  Merge/steal even if the node
        // currently satifisfies the min fanout, because we may remove end up
        // removing a child
        if child.in_danger(&inner.limits) {
            let dml2 = inner.dml.clone();
            Tree::fix_int(&inner, parent, child_idx, child, txg)
                .and_then(move |(parent, _, _)| {
                    let child_idx = parent.as_int().position(&k);
                    parent.xlock(&dml2, child_idx, txg)
                }).and_then(move |(parent, child)| {
                    drop(parent);
                    Tree::remove_no_fix(inner, child, k, txg)
                }).boxed()
        } else {
            drop(parent);
            Tree::remove_no_fix(inner, child, k, txg)
        }
    }

    /// Helper for `remove`.  Handles removal once the tree is locked
    fn remove_locked(inner: Arc<Inner<A, D, K, V>>,
                     tree_guard: RwLockWriteGuard<IntElem<A, K, V>>, k: K,
                     txg: TxgT)
        -> impl Future<Output=Result<Option<V>, Error>> + Send
    {
        Tree::xlock_and_merge_root(inner.clone(), tree_guard, txg)
            .and_then(move |(tree_guard, root_guard)| {
                drop(tree_guard);
                Tree::remove_no_fix(inner, root_guard, k, txg)
            })
    }

    /// Remove key `k` from a node, but don't try to fixup the node.
    fn remove_no_fix(inner: Arc<Inner<A, D, K, V>>,
                     mut node: TreeWriteGuard<A, K, V>, k: K, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Option<V>, Error>> + Send>>
    {

        if node.is_leaf() {
            let old_v = node.as_leaf_mut().remove(&k);
            future::ok(old_v).boxed()
        } else {
            let child_idx = node.as_int().position(&k);
            let fut = node.xlock(&inner.dml, child_idx, txg);
            fut.and_then(move |(parent, child)| {
                Tree::remove_int(inner, parent, child_idx, child, k,
                                 txg)
            }).boxed()
        }
    }

    /// Render the Tree into a `TreeOnDisk` object.  Requires that the Tree
    /// already be flushed.  Will fail if the Tree is dirty.
    pub fn serialize(&self) -> Result<TreeOnDisk<A>, Error> {
        self.i.root.try_read()
        .map(|root_guard| {
            let iod = InnerOnDisk{
                height: self.i.height.load(Ordering::Relaxed),
                limits: self.i.limits,
                root: *root_guard.ptr.as_addr(),
                txgs: root_guard.txgs.clone(),
            };
            TreeOnDisk(iod)
        }).or(Err(Error::EDEADLK))
    }

    /// Lock the Tree for writing
    fn write(&self) -> impl Future<Output=RwLockWriteGuard<IntElem<A, K, V>>>
    {
        Tree::<A, D, K, V>::write_root(&self.i)
    }

    // Clippy has a false positive on `node`
    #[allow(clippy::needless_pass_by_value)]
    fn write_leaf(dml: Arc<D>, compressor: Compression, node: Node<A, K, V>,
                  txg: TxgT)
        -> impl Future<Output=Result<A, Error>>
    {
        node.0.try_unwrap().unwrap().into_leaf().flush(&*dml, txg)
        .and_then(move |leaf_data| {
            let node = Node::new(NodeData::Leaf(leaf_data));
            let arc: Arc<Node<A, K, V>> = Arc::new(node);
            dml.put(arc, compressor, txg)
        })
    }

    fn write_root(inner: &Inner<A, D, K, V>)
        -> impl Future<Output=RwLockWriteGuard<IntElem<A, K, V>>>
    {
        inner.root.write()
    }

    /// Lock the root `IntElem` exclusively.  If it is not already resident in
    /// memory, then COW it.  If it has an only child, merge the root node with
    /// its child.
    fn xlock_and_merge_root(inner: Arc<Inner<A, D, K, V>>,
                  tree_guard: RwLockWriteGuard<IntElem<A, K, V>>, txg: TxgT)
        -> impl Future<Output=Result<(RwLockWriteGuard<IntElem<A, K, V>>,
                             TreeWriteGuard<A, K, V>),
                       Error>> + Send
    {
        // First, lock the root IntElem
        // If it's not a leaf and has a single child:
        //     Drop the root's intelem lock
        //     Replace the root's IntElem with the child's
        //     Relock the root's intelem
        // If it's not a leaf and has no children:
        //     Make it a leaf
        //     Fix the tree's height
        // Sometimes crashes here when removing from the AllocT
        Tree::xlock_root(&inner.dml, tree_guard, txg)
            .and_then(move |(mut tree_guard, mut root_guard)| {
                let nchildren = root_guard.len();
                if !root_guard.is_leaf() && nchildren == 1 {
                    let new_root = root_guard.as_int_mut()
                        .children
                        .pop()
                        .unwrap();
                    drop(root_guard);
                    *tree_guard = new_root;
                    // The root's key must always be the absolute minimum
                    tree_guard.key = K::min_value();
                    inner.height.fetch_sub(1, Ordering::Relaxed);
                    Tree::xlock_root(&inner.dml, tree_guard, txg).boxed()
                } else if !root_guard.is_leaf() && nchildren == 0 {
                    drop(root_guard);
                    let new_root = IntElem::default();
                    *tree_guard = new_root;
                    // The root's key must always be the absolute minimum
                    tree_guard.key = K::min_value();
                    inner.height.store(1, Ordering::Relaxed);
                    Tree::xlock_root(&inner.dml, tree_guard, txg).boxed()
                } else {
                    future::ok((tree_guard, root_guard)).boxed()
                }
            })
    }

    /// Lock the root `IntElem` exclusively.  If it is not already resident in
    /// memory, then COW it.
    fn xlock_root(dml: &Arc<D>, mut guard: RwLockWriteGuard<IntElem<A, K, V>>,
                  txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<(RwLockWriteGuard<IntElem<A, K, V>>,
                                TreeWriteGuard<A, K, V>), Error>> + Send>>
    {
        guard.txgs.end = txg + 1;
        if guard.ptr.is_mem() {
            async move {
                let child_guard = guard.ptr.as_mem().0.write().await;
                Ok((guard, TreeWriteGuard(child_guard)))
            }.boxed()
        } else {
            let addr = *guard.ptr.as_addr();
            let afut = dml.pop::<Arc<Node<A, K, V>>, Arc<Node<A, K, V>>>(
                &addr, txg);
            async move {
                let arc = afut.await?;
                let child_guard = arc.xlock().await;
                drop(child_guard);
                let child_node = Arc::try_unwrap(*arc)
                    .expect("We should be the only owner");
                guard.ptr = TreePtr::Mem(Box::new(child_node));
                let child_guard = TreeWriteGuard(
                    guard.ptr.as_mem().0.try_write().unwrap()
                );
                Ok((guard, child_guard))
            }.boxed()
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
    /// Clean the zone by moving all of its records to other zones.
    ///
    /// # Arguments
    ///
    /// `pbas` -    All nodes stored in this range of PBAs will be rewritten.
    ///             Normally this will be the whole PBA range of an allocation
    ///             zone, but it technically could be more or less.
    /// `txgs` -    The range of transactions in which `pbas` were written.
    ///             It is an error if any block in `pbas` was written outside of
    ///             this transaction range.
    /// `txg` -     The current transaction number
    pub fn clean_zone(&self, pbas: Range<PBA>, txgs: Range<TxgT>, txg: TxgT)
        -> impl Future<Output=Result<(), Error>> + Send
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
        stream::iter(0..tree_height)
        .map(Ok)
        .try_for_each(move |echelon| {
            let inner3 = inner2.clone();
            CleanZonePass1::new(inner2.clone(), pbas.clone(),
                                txgs.clone(), echelon)
            .try_collect::<Vec<_>>()
            .and_then(move |nodes| {
                stream::iter(nodes.into_iter())
                .map(Ok)
                .try_for_each_concurrent(None, move |node| {
                    // TODO: consider attempting to rewrite multiple nodes
                    // at once, so as not to spend so much time traversing
                    // the tree
                    Tree::rewrite_node(inner3.clone(), node, txg)
                })
            })
        })
    }

    /// Find all Nodes starting at `key` at a given level of the Tree which lay
    /// in the indicated range of PBAs.  `txgs` must include all transactions in
    /// which anything was written to any block in `pbas`.
    fn get_dirty_nodes(inner: Arc<Inner<ddml::DRP, D, K, V>>,
                       params: GetDirtyNodeParams<K>)
        -> impl Future<Output=Result<(VecDeque<NodeId<K>>, Option<K>), Error>>
    {
        Tree::<ddml::DRP, D, K, V>::read_root(&*inner)
            .then(move |tree_guard| {
                let h = inner.height.load(Ordering::Relaxed) as u8;
                if h == params.echelon + 1 {
                    // Clean the tree root
                    let dirty = if tree_guard.ptr.is_addr() &&
                        tree_guard.ptr.as_addr().pba() >= params.pbas.start &&
                        tree_guard.ptr.as_addr().pba() < params.pbas.end {
                        let mut v = VecDeque::new();
                        v.push_back(NodeId{height: params.echelon,
                            key: tree_guard.key});
                        v
                    } else {   // LCOV_EXCL_LINE   kcov false negative
                        VecDeque::new()
                    };
                    future::ok((dirty, None)).boxed()
                } else {
                    let dml2 = inner.dml.clone();
                    let fut = tree_guard.rlock(&dml2)
                         .and_then(move |guard| {
                             drop(tree_guard);
                             Tree::get_dirty_nodes_r(dml2, guard, h - 1, None,
                                                     params)
                         });
                    fut.boxed()
                }
            })
    }

    /// Find dirty nodes as specified by 'params'.  `next_key`, if present, must
    /// be the key of the node immediately to the right (and possibly up one or
    /// more levels) from `guard`.  `height` is the tree height of `guard`,
    /// where leaves are 0.
    fn get_dirty_nodes_r(dml: Arc<D>, guard: TreeReadGuard<ddml::DRP, K, V>,
                         height: u8,
                         next_key: Option<K>,
                         params: GetDirtyNodeParams<K>)
        -> Pin<Box<dyn Future<Output=Result<(VecDeque<NodeId<K>>, Option<K>),
                          Error>> + Send>>
    {
        if height == params.echelon + 1 {
            let nodes = guard.as_int().children.iter().filter_map(|child| {
                if child.ptr.is_addr() &&
                    child.ptr.as_addr().pba() >= params.pbas.start &&
                    child.ptr.as_addr().pba() < params.pbas.end
                {
                    assert!(ranges_overlap(&params.txgs, &child.txgs),
                        "Node's PBA {:?} resides in the query range {:?} but its TXG range {:?} does not overlap the query range {:?}",
                        child.ptr.as_addr().pba(), &params.pbas, &child.txgs,
                        &params.txgs);
                    Some(NodeId{height: height - 1, key: child.key})
                } else {
                    None
                }
            }).collect::<VecDeque<_>>();
            return future::ok((nodes, next_key)).boxed();
        }
        // Find the first child >= key whose txg range overlaps with txgs
        let idx0 = guard.as_int().position(&params.key);
        let idx_in_range = (idx0..guard.as_int().nchildren())
        .find(|idx| {
            ranges_overlap(&guard.as_int().children[*idx].txgs, &params.txgs)
        });
        if let Some(idx) = idx_in_range {
            let next_key = if idx < guard.as_int().nchildren() - 1 {
                Some(guard.as_int().children[idx + 1].key)
            } else {
                next_key
            };
            let child_fut = guard.as_int().children[idx].rlock(&dml);
            child_fut.and_then(move |child_guard| {
                drop(guard);
                Tree::get_dirty_nodes_r(dml, child_guard, height - 1,
                                        next_key, params)
            }).boxed()
        } else {
            future::ok((VecDeque::new(), next_key)).boxed()
        }
    }

    /// Rewrite `node`, without modifying its contents
    fn rewrite_node(inner: Arc<Inner<ddml::DRP, D, K, V>>,
                    node: NodeId<K>, txg: TxgT)
        -> impl Future<Output=Result<(), Error>> + Send
    {
        Tree::<ddml::DRP, D, K, V>::write_root(&*inner)
        .then(move |mut guard| {
            let h = inner.height.load(Ordering::Relaxed) as u8;
            let dml2 = inner.dml.clone();
            if h == node.height + 1 {
                // Clean the root node
                if guard.ptr.is_mem() {
                    // Another thread has already dirtied the root.  Nothing to
                    // do!
                    return future::ok(()).boxed();
                }
                let fut = dml2.pop::<Arc<Node<ddml::DRP, K, V>>,
                                     Arc<Node<ddml::DRP, K, V>>>(
                                        guard.ptr.as_addr(), txg)
                    .and_then(move |arc| {
                        dml2.put(*arc, Compression::None, txg)
                    }).map_ok(move |addr| {
                        let new = TreePtr::Addr(addr);
                        guard.ptr = new;
                    });
                fut.boxed()
            } else {
                let fut = Tree::xlock_root(&dml2, guard, txg)
                     .and_then(move |(_root_guard, child_guard)| {
                         Tree::rewrite_node_r(dml2, child_guard, h - 1, node,
                                              txg)
                     });
                fut.boxed()
            }
        })
    }

    fn rewrite_node_r(dml: Arc<D>, mut guard: TreeWriteGuard<ddml::DRP, K, V>,
                      height: u8, node: NodeId<K>, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>
    {
        debug_assert!(height > 0);
        let child_idx = guard.as_int().position(&node.key);
        if height == node.height + 1 {
            if guard.as_int().children[child_idx].ptr.is_mem() {
                // Another thread has already dirtied this node.  Nothing to do!
                return future::ok(()).boxed()
            }
            // TODO: bypass the cache for this part
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
                }).map_ok(move |addr| {
                    let new = TreePtr::Addr(addr);
                    guard.as_int_mut().children[child_idx].ptr = new;
                    let start = if height == 1 {
                        // For leaves, there's only one TXG in the range
                        txg
                    } else {
                        // For interior nodes, we would ideally need to check
                        // all of the target node's children.  But that would
                        // require a read from disk.  For now, the best we can
                        // do is to not update the start txg.
                        // TODO: accurately update the start txg
                        guard.as_int().children[child_idx].txgs.start
                    };
                    let txgs = start..txg + 1;
                    guard.as_int_mut().children[child_idx].txgs = txgs;
                });
            fut.boxed()
        } else {
            guard.xlock(&dml, child_idx, txg)
                .and_then(move |(parent_guard, child_guard)| {
                    drop(parent_guard);
                    Tree::rewrite_node_r(dml, child_guard, height - 1, node, txg)
                }).boxed()
        }
    }
}
