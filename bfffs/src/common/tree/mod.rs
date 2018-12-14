// vim: tw=80

//! COW B+-Trees, based on B-trees, Shadowing, and Clones[^CowBtrees]
//!
//! [^CowBtrees]: Rodeh, Ohad. "B-trees, shadowing, and clones." ACM Transactions on Storage (TOS) 3.4 (2008): 2.

// use the atomic crate since libstd's AtomicU64 type is still unstable
// https://github.com/rust-lang/rust/issues/32976
use atomic::{Atomic, Ordering};
use bincode;
use crate::{
    boxfut,
    common::{*, dml::*}
};
use futures::{
    Async,
    Future,
    Poll,
    Sink,
    future::{self, IntoFuture, Loop},
    stream::{self, Stream},
    sync::mpsc
};
use futures_locks::*;
use serde::Serializer;
use serde::de::DeserializeOwned;
use serde_yaml;
#[cfg(test)] use std::fmt::{self, Display, Formatter};
use std::{
    borrow::Borrow,
    cell::RefCell,
    collections::{BTreeMap, HashSet, VecDeque},
    fmt::Debug,
    io,
    mem,
    ops::{Bound, Deref, DerefMut, Range, RangeBounds, RangeInclusive},
    rc::Rc,
    sync::{
        Arc,
    }
};
use tokio::{
    executor::{DefaultExecutor, Executor},
    runtime::current_thread
};

mod node;
use self::node::*;
// Node must be visible for the IDML's unit tests
pub(super) use self::node::Node;

pub use self::node::{Addr, Key, MinValue, Value};

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

    pub fn serialize<S>(x: &Atomic<u64>, s: S) -> Result<S::Ok, S::Error>
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
    last_fut: Option<Box<dyn Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                                    Error=Error> + Send>>,
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
                        Box::new(Tree::get_range(&self.inner, r))
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
    last_fut: Option<Box<dyn Future<Item=(VecDeque<NodeId<K>>, Option<K>),
                                    Error=Error> + Send>>,

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
                            let params = GetDirtyNodeParams {
                                key: l, pbas, txgs, echelon: e
                            };
                            Box::new(Tree::get_dirty_nodes(i.inner.clone(),
                                params))
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
#[derive(Serialize)]
struct Inner<A: Addr, D: DML, K: Key, V: Value> {
    /// Tree height.  1 if the Tree consists of a single Leaf node.
    // Use atomics so it can be modified from an immutable reference.  Accesses
    // should be very rare, so performance is not a concern.
    #[serde(with = "atomic_u64_serializer")]
    height: Atomic<u64>,
    /// Minimum node fanout.  Smaller nodes will be merged, or will steal
    /// children from their neighbors.
    min_fanout: u64,
    /// Maximum node fanout.  Larger nodes will be split.
    // Rodeh states that the minimum value of max_fanout that can guarantee
    // invariants is 2 * min_fanout + 1.  However, range_delete can be slightly
    // simplified if max_fanout is at least 2 * min_fanout + 4.  That would mean
    // that fix_int can always either merge two nodes, or steal 2 nodes.
    max_fanout: u64,
    /// Maximum node size in bytes.  Larger nodes will be split or their message
    /// buffers flushed
    _max_size: u64,
    /// Root node
    #[serde(with = "tree_root_serializer")]
    root: RwLock<IntElem<A, K, V>>,
    #[serde(skip)]
    dml: Arc<D>
}

impl<A: Addr, D: DML, K: Key, V: Value> Inner<A, D, K, V> {
    #[cfg(test)]
    pub fn from_str(dml: Arc<D>, s: &str) -> Self {
        let iod: InnerOnDisk<A, K, V> = serde_yaml::from_str(s).unwrap();
        Inner::new(dml, iod)
    }

    fn new(dml: Arc<D>, iod: InnerOnDisk<A, K, V>) -> Self {
        Inner {
            height: Atomic::new(iod.height),
            min_fanout: iod.min_fanout,
            max_fanout: iod.max_fanout,
            _max_size: iod._max_size,
            root: RwLock::new(iod.root),
            dml
        }
    }

    pub fn open(dml: Arc<D>, on_disk: &TreeOnDisk) -> bincode::Result<Self> {
        let iod: InnerOnDisk<A, K, V> = bincode::deserialize(&on_disk.0[..])?;
        Ok(Inner::new(dml, iod))
    }
}

/// A version of `Inner` that is serializable
#[derive(Debug)]
#[derive(Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
struct InnerOnDisk<A: Addr, K: Key, V: Value> {
    height: u64,
    min_fanout: u64,
    max_fanout: u64,
    _max_size: u64,
    root: IntElem<A, K, V>
}

/// The return type of `Tree::check_r`
type CheckR<K> = Box<dyn Future<Item=(bool, RangeInclusive<K>, Range<TxgT>),
                                Error=Error> + Send>;


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
        -> impl Stream<Item=A, Error=()>
        where TxgT: Borrow<T>,
              R: Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + Send
    {
        // Keep the whole tree locked so no new addresses come into use and no
        // old ones get freed while we're working
        type SenderFut<A, K, V> = Box<dyn Future<Item=(mpsc::Sender<A>,
            RwLockReadGuard<IntElem<A, K, V>>), Error=Error> + Send>;
        let (tx, rx) = mpsc::channel(self.i.max_fanout as usize);
        let height = self.i.height.load(Ordering::Relaxed) as u8;
        let dml = self.i.dml.clone();
        let txgs2 = txgs.clone();
        let task = self.read()
        .and_then(move |tree_guard| {
            if tree_guard.ptr.is_addr()
                && ranges_overlap(&txgs, &tree_guard.txgs)
            {
                let fut = tx.send(*tree_guard.ptr.as_addr())
                .map_err(|_| Error::EPIPE)
                .map(move |tx| (tx, tree_guard));
                Box::new(fut) as SenderFut<A, K, V>
            } else {
                Box::new(Ok((tx, tree_guard)).into_future())
                    as SenderFut<A, K, V>
            }
        }).and_then(move |(tx, tree_guard)| {
            if height > 1 {
                let fut = tree_guard.rlock(&dml)
                .and_then(move |guard| Tree::addresses_r(dml, height - 1,
                                                         guard, tx, txgs2))
                .map(move |_| drop(tree_guard));
                boxfut!(fut)
            } else {
                boxfut!(Ok(()).into_future())
            }
        }).map_err(Error::unhandled);
        DefaultExecutor::current().spawn(Box::new(task)).unwrap();
        rx
    }

    fn addresses_r<R, T>(dml: Arc<D>, height: u8, guard: TreeReadGuard<A, K, V>,
                         tx: mpsc::Sender<A>, txgs: R)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
        where TxgT: Borrow<T>,
              R: Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + Send
    {
        let child_addresses = guard.as_int().children.iter()
        .filter(|c| c.ptr.is_addr() && ranges_overlap(&txgs, &c.txgs))
        .map(|c| *c.ptr.as_addr())
        .collect::<Vec<_>>();
        let fut = tx.send_all(stream::iter_ok(child_addresses.into_iter()))
        .map_err(|_| Error::EPIPE)
        .and_then(move |(tx, _)| {
            if height > 1 {
                let txgs2 = txgs.clone();
                let child_futs = guard.as_int().children.iter()
                .filter(move |c| ranges_overlap(&txgs2, &c.txgs))
                .map(move |c| {
                    let dml2 = dml.clone();
                    let tx2 = tx.clone();
                    let txgs2 = txgs.clone();
                    c.rlock(&dml)
                    .and_then(move |guard| Tree::addresses_r(dml2, height - 1,
                                                             guard, tx2, txgs2))
                }).collect::<Vec<_>>();
                boxfut!(future::join_all(child_futs).map(drop))
            } else {
                boxfut!(Ok(()).into_future())
            }
        });
        boxfut!(fut)
    }

    /// Audit the whole Tree for consistency and invariants
    // TODO: check node size limits, too
    pub fn check(&self) -> impl Future<Item=bool, Error=Error> + Send {
        // Keep the whole tree locked and use LIFO lock discipline
        let height = self.i.height.load(Ordering::Relaxed) as u8;
        let inner = self.i.clone();
        let min_fanout = self.i.min_fanout as usize;
        let max_fanout = self.i.max_fanout as usize;
        self.read()
        .and_then(move |tree_guard| {
            tree_guard.rlock(&inner.dml)
            .and_then(move |guard| {
                let root_ok = guard.check(tree_guard.key, height - 1, true,
                                          min_fanout, max_fanout);
                if height == 1 {
                    boxfut!(Ok(root_ok).into_future())
                } else {
                    let fut = Tree::check_r(&inner.dml, height - 1, &guard,
                                            min_fanout, max_fanout)
                    .map(move |r| {
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
                    boxfut!(fut)
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
               min_fanout: usize, max_fanout: usize)
        -> CheckR<K>
    {
        debug_assert!(height > 0);
        let children = &node.as_int().children;
        let start = children.iter().map(|c| c.txgs.start).min().unwrap();
        let end = children.iter().map(|c| c.txgs.end).max().unwrap();
        let futs = node.as_int().children.iter().map(|c| {
            let range = c.txgs.clone();
            let range2 = range.clone();
            let dml2 = dml.clone();
            let key = c.key;
            c.rlock(&dml)
            .and_then(move |guard| {
                let data_ok = guard.check(key, height - 1, false,
                                          min_fanout, max_fanout);
                if guard.is_leaf() {
                    let first_key = *guard.key();
                    // We can unwrap() the last_key because last_key should only
                    // ever be None for a root leaf node, and check_r doesn't
                    // get called in that case.
                    let last_key = guard.as_leaf().last_key().unwrap();
                    let keys = first_key..=last_key;
                    Box::new(Ok((data_ok, keys, range)).into_future())
                        as CheckR<K>
                } else {
                    let fut = Tree::check_r(&dml2, height - 1, &guard,
                                            min_fanout, max_fanout)
                    .map(move |(passed, keys, txgs)| {
                        // For IntNodes, use the key as recorded by the parent,
                        // which is <= the lowest child's key
                        (passed && data_ok, key..=*keys.end(), txgs)
                    });
                    Box::new(fut) as CheckR<K>
                }
            }).map(move |(passed, keys, r)| {
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
        }).collect::<Vec<_>>();
        let fut = future::join_all(futs)
        .map(move |r| {
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
            }).all(|x| x);
            (children_passed && sorted, first_key..=last_key, start..end)
        });
        Box::new(fut)
    }

    pub fn create(dml: Arc<D>) -> Self {
        Tree::new(dml,
                  4,        // BetrFS's min fanout
                  16,       // BetrFS's max fanout
                  1<<22,    // BetrFS's max size
        )
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

    /// Dump a YAMLized representation of the Tree to stdout.
    ///
    /// Must be called from the synchronous domain.
    ///
    /// To save RAM, the Tree is actually dumped as several independent YAML
    /// records.  The first one is the Tree itself, and the rest are other
    /// on-disk Nodes.  All the Nodes can be combined into a single YAML map by
    /// simply removing the `---` separators.
    // `&mut Formatter` isn't `Send`, so these Futures can only be used with the
    // current_thread Runtime.  Given that limitation, we may as well
    // instantiate our own Runtime
    pub fn dump(&self, f: &mut io::Write) -> Result<(), Error> {
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
        let mut rt = current_thread::Runtime::new().unwrap();
        let fut = self.read()
            .and_then(move |tree_guard| {
                tree_guard.rlock(&inner.dml)
                .and_then(move |guard| {
                    let mut f2 = rrf2.borrow_mut();
                    let s = serde_yaml::to_string(&*inner).unwrap();
                    writeln!(f2, "{}", &s).unwrap();
                    Tree::dump_r(inner.dml.clone(), guard, rrf)
                }).map(move |guard| {
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
                  f: Rc<RefCell<&'a mut io::Write>>)
        -> Box<dyn Future<Item=TreeReadGuard<A, K, V>, Error=Error> + 'a>
    {
        type ChildFut<'a, A, K, V> =
            Box<dyn Future<Item=Vec<TreeReadGuard<A, K, V>>, Error=Error> + 'a>;
        let f2 = f.clone();
        let fut = if let NodeData::Int(ref int) = *node {
            let futs = int.children.iter().map(move |child| {
                let dml2 = dml.clone();
                let f3 = f.clone();
                child.rlock(&dml)
                    .and_then(move |child_node| {
                        Tree::dump_r(dml2, child_node, f3)
                    })
            }).collect::<Vec<_>>();
            Box::new(future::join_all(futs))
                as ChildFut<'a, A, K, V>
        } else {
            Box::new(Ok(Vec::new()).into_future())
                as ChildFut<'a, A, K, V>
        }
        .map(move |r| {
            if !node.is_leaf() {
                let mut hmap = BTreeMap::new();
                let citer = node.as_int().children.iter();
                for (child, guard) in citer.zip(r.iter()) {
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
        boxfut!(fut, _, _, 'a)
    }

    /// Fix an Int node in danger of being underfull, returning the parent guard
    /// back to the caller
    #[allow(clippy::collapsible_if)]
    fn fix_int<Q>(inner: &Arc<Inner<A, D, K, V>>,
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
        debug_assert!(nchildren >= 2,
            "nchildren < 2 for tree with parent key {:?}, idx={:?}",
            parent.as_int().children[child_idx].key, child_idx);
        let max_fanout = inner.max_fanout;
        let (fut, sib_idx, right) = {
            if child_idx < nchildren - 1 {
                let sib_idx = child_idx + 1;
                (parent.xlock(&inner.dml, sib_idx, txg), sib_idx, true)
            } else {
                let sib_idx = child_idx - 1;
                (parent.xlock(&inner.dml, sib_idx, txg), sib_idx, false)
            }
        };
        fut.map(move |(mut parent, mut sibling)| {
            let (before, after) = {
                let children = &mut parent.as_int_mut().children;
                if right {
                    if child.can_merge(&sibling, max_fanout) {
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
                    if sibling.can_merge(&child, max_fanout) {
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

    #[cfg(test)]
    pub fn from_str(dml: Arc<D>, s: &str) -> Self {
        Tree{i: Arc::new(Inner::from_str(dml, s))}
    }

    /// Insert value `v` into the tree at key `k`, returning the previous value
    /// for that key, if any.
    pub fn insert(&self, k: K, v: V, txg: TxgT)
        -> impl Future<Item=Option<V>, Error=Error>
    {
        let inner2 = self.i.clone();
        self.write()
            .and_then(move |guard| {
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
        -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>
    {
        // First, split the node, if necessary
        if (*child).should_split(inner.max_fanout) {
            let (old_txgs, new_elem) = child.split(txg);
            parent.as_int_mut().children[child_idx].txgs = old_txgs;
            parent.as_int_mut().children.insert(child_idx + 1, new_elem);
            // Reinsert into the parent, which will choose the correct child
            Box::new(Tree::insert_int_no_split(inner, parent, k, v, txg))
        } else if child.is_leaf() {
            let elem = &mut parent.as_int_mut().children[child_idx];
            Box::new(Tree::<A, D, K, V>::insert_leaf_no_split(elem,
                child, k, v, txg))
        } else {
            drop(parent);
            Box::new(Tree::insert_int_no_split(inner, child, k, v, txg))
        }
    }

    /// Insert a value into a leaf node without splitting it
    fn insert_leaf_no_split(elem: &mut IntElem<A, K, V>,
                  mut child: TreeWriteGuard<A, K, V>, k: K, v: V, txg: TxgT)
        -> impl Future<Item=Option<V>, Error=Error>
    {
        let old_v = child.as_leaf_mut().insert(k, v);
        elem.txgs = txg..txg + 1;
        Ok(old_v).into_future()
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Helper for `insert`.  Handles insertion once the tree is locked
    fn insert_locked(inner: Arc<Inner<A, D, K, V>>,
                     mut relem: RwLockWriteGuard<IntElem<A, K, V>>,
                     mut rnode: TreeWriteGuard<A, K, V>, k: K, v: V, txg: TxgT)
        -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>
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
            Box::new(Tree::insert_int_no_split(inner, rnode, k, v, txg))
        }
    }

    /// Insert a value into an int node without splitting it
    fn insert_int_no_split(inner: Arc<Inner<A, D, K, V>>,
                           mut node: TreeWriteGuard<A, K, V>, k: K, v: V,
                           txg: TxgT)
        -> impl Future<Item=Option<V>, Error=Error>
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

    /// Lookup the value of key `k`.  Return `None` if no value is present.
    pub fn get(&self, k: K) -> impl Future<Item=Option<V>, Error=Error>
    {
        let dml2 = self.i.dml.clone();
        self.read()
            .and_then(move |tree_guard| {
                tree_guard.rlock(&dml2)
                     .and_then(move |guard| {
                         drop(tree_guard);
                         Tree::get_r(dml2, guard, k)
                     })
            })
    }

    /// Lookup the value of key `k` in a node, which must already be locked.
    fn get_r(dml: Arc<D>, node: TreeReadGuard<A, K, V>, k: K)
        -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>
    {
        let next_node_fut = match *node {
            NodeData::Leaf(ref leaf) => {
                return Box::new(Ok(leaf.get(&k)).into_future())
            },
            NodeData::Int(ref int) => {
                let child_elem = &int.children[int.position(&k)];
                child_elem.rlock(&dml)
            }
        };
        Box::new(
            next_node_fut
            .and_then(move |next_node| {
                drop(node);
                Tree::get_r(dml, next_node, k)
            })
        )
    }

    /// Private helper for `Range::poll`.  Returns a subset of the total
    /// results, consisting of all matching (K,V) pairs within a single Leaf
    /// Node, plus an optional Bound for the next iteration of the search.  If
    /// the Bound is `None`, then the search is complete.
    fn get_range<R, T>(inner: &Inner<A, D, K, V>, range: R)
        -> impl Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                       Error=Error> + Send
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + Send + 'static
    {
        let dml2 = inner.dml.clone();
        Tree::<A, D, K, V>::read_root(&inner)
            .and_then(move |tree_guard| {
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
        -> Box<dyn Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
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
                    let bound = if more {
                        Some(match next_guard {
                            Some(g) => Bound::Included(g.key().borrow().clone()),
                            None => {
                                let t = leaf.last_key().unwrap().borrow().clone();
                                Bound::Excluded(t)
                            }
                        })
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
                        boxfut!(
                            int.children[child_idx + 1].rlock(&dml2)
                            .map(move |g| {
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
                            })
                        )
                    } else {
                        boxfut!(Ok(None).into_future())
                    }
                } else {
                    boxfut!(Ok(next_guard).into_future())
                };
                let child_fut = child_elem.rlock(&dml2);
                (child_fut, next_fut)
            } // LCOV_EXCL_LINE kcov false negative
        };
        boxfut!(
            child_fut.join(next_fut)
                .and_then(move |(child_guard, next_guard)| {
                    drop(guard);
                    Tree::get_range_r(dml, child_guard, next_guard, range)
                })
        )
    }

    /// Return the highest valued key in the `Tree`
    pub fn last_key(&self) -> impl Future<Item=Option<K>, Error=Error> {
        let dml2 = self.i.dml.clone();
        self.read()
            .and_then(move |tree_guard| {
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
        -> Box<dyn Future<Item=Option<K>, Error=Error> + Send>
    {
        let next_node_fut = match *node {
            NodeData::Leaf(ref leaf) => {
                return Box::new(Ok(leaf.last_key()).into_future())
            },
            NodeData::Int(ref int) => {
                let child_elem = &int.children[int.children.len() - 1];
                child_elem.rlock(&dml)
            }
        };
        Box::new(
            next_node_fut
            .and_then(move |next_node| {
                drop(node);
                Tree::last_key_r(dml, next_node)
            })
        )
    }

    /// Open a `Tree` from its serialized representation
    pub fn open(dml: Arc<D>, on_disk: &TreeOnDisk) -> bincode::Result<Self> {
        let i = Inner::open(dml, on_disk)?;
        Ok(Tree{i: Arc::new(i)})
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
        -> impl Future<Item=(), Error=Error> + Send
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
        let min_fanout = self.i.min_fanout as usize;
        let height = self.i.height.load(Ordering::Relaxed) as u8;
        self.write()
            .and_then(move |guard| {
                Tree::xlock_root(&dml2, guard, txg)
                    .and_then(move |(tree_guard, root_guard)| {
                        // ptr is guaranteed to be a TreePtr::Mem because we
                        // just xlock()ed it.
                        let id = tree_guard.ptr.as_mem()
                            as *const Node<A, K, V> as usize;
                        Tree::range_delete_pass1(min_fanout, dml2, height - 1,
                                                 root_guard, range, None, txg)
                            .map(move |(mut m, danger, _)| {
                                if danger {
                                    m.insert(id);
                                }
                                (tree_guard, m)
                            })
                    })
            }).and_then(move |(tree_guard, m)| {
                Tree::range_delete_pass2(inner2, tree_guard, m, rangeclone, txg)
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
    fn range_delete_pass1<R, T>(min_fanout: usize, dml: Arc<D>,
                                height: u8, mut guard: TreeWriteGuard<A, K, V>,
                                range: R, ubound: Option<K>, txg: TxgT)
        -> impl Future<Item=(HashSet<usize>, bool, usize),
                       Error=Error> + Send
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
            let danger = guard.len() < min_fanout;
            return boxfut!(Ok((map, danger, guard.len())).into_future());
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
                return Box::new(guard.xlock(&dml, i, txg)
                    .and_then(move |(mut parent_guard, child_guard)| {
                        Tree::range_delete_pass1(min_fanout, dml, height - 1,
                            child_guard, range, next_ubound, txg)
                        .map(move |(mut m, mut child_danger, child_len)| {
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
                            let danger = l <= min_fanout || child_danger;
                            (m, danger, l)
                        })
                    })
                )
            },
            _ => ()
        }

        // We found the LCA.  Descend along both sides of the cut.
        let dml2 = dml.clone();
        let dml3 = dml.clone();
        let range2 = range.clone();
        type CutFut<A, K, V> =
            Box<dyn Future<Item=(Option<IntElem<A, K, V>>, HashSet<usize>,
                                 Option<bool>, Option<usize>),
                           Error=Error> + Send>;
        let left_fut = if let Some(i) = left_in_cut {
            let new_ubound = guard.as_int().children.get(i + 1)
                .map_or(ubound, |elem| Some(elem.key));
            Box::new(
                guard.xlock_nc(&dml2, i, height, txg)
                .and_then(move |(elem, child_guard)| {
                    Tree::range_delete_pass1(min_fanout, dml2, height - 1,
                                             child_guard, range, new_ubound,
                                             txg)
                    .map(move |(m, danger, child_len)|
                         (elem, m, Some(danger), Some(child_len))
                    )
                })
            ) as CutFut<A, K, V>
        } else {
            let map = HashSet::<usize>::with_capacity(HASHSET_CAPACITY);
            Box::new(Ok((None, map, None, None)).into_future())
                as CutFut<A, K, V>
        };
        let right_fut = if let Some(j) = right_in_cut {
            let new_ubound = guard.as_int().children.get(j + 1)
                .map_or(ubound, |elem| Some(elem.key));
            Box::new(
                guard.xlock_nc(&dml3, j, height, txg)
                .and_then(move |(elem, child_guard)| {
                    Tree::range_delete_pass1(min_fanout, dml3, height - 1,
                                             child_guard, range2, new_ubound,
                                             txg)
                    .map(move |(m, danger, child_len)|
                         (elem, m, Some(danger), Some(child_len))
                    )
                })
            ) as CutFut<A, K, V>
        } else {
            let map = HashSet::<usize>::with_capacity(HASHSET_CAPACITY);
            Box::new(Ok((None, map, None, None)).into_future())
                as CutFut<A, K, V>
        };
        let middle_fut = Tree::range_delete_purge(dml, height, wholly_deleted,
                                                  guard, txg);
        boxfut!(
            left_fut.join3(middle_fut, right_fut)
            .map(move |((lelem, mut lmap, ldanger, llen),
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
                let l = guard.len();
                let danger = l < min_fanout ||
                    ldanger == Some(true) ||
                    rdanger == Some(true);
                (lmap, danger, guard.len())
            })
        )
    }

    /// Delete the indicated range of children from the Tree
    fn range_delete_purge(dml: Arc<D>, height: u8, range: Range<usize>,
                          mut guard: TreeWriteGuard<A, K, V>, txg: TxgT)
        -> impl Future<Item=TreeWriteGuard<A, K, V>, Error=Error> + Send
    {
        if height == 0 {
            // Simply delete the leaves
            debug_assert!(guard.is_leaf());
            boxfut!(Ok(guard).into_future())
        } else if height == 1 {
            debug_assert!(!guard.is_leaf());
            // If the child elements point to leaves, just delete them.
            let futs = guard.as_int_mut().children.drain(range)
            .map(move |elem| {
                if let TreePtr::Addr(addr) = elem.ptr {
                    // Delete on-disk leaves
                    dml.delete(&addr, txg)
                } else {
                    // Simply drop in-memory leaves
                    boxfut!(Ok(()).into_future())
                }
            }).collect::<Vec<_>>();
            let fut = future::join_all(futs).map(|_| guard);
            boxfut!(fut.into_future())
        } else {
            debug_assert!(!guard.is_leaf());
            // If the IntElems point to IntNodes, we must recurse
            let fut = guard.drain_xlock(dml, range, txg, move |guard, dml| {
                let range = 0..guard.len();
                Tree::range_delete_purge(dml.clone(), height - 1, range, guard,
                                         txg)
            }).map(|(guard, _)| guard);
            boxfut!(fut.into_future())
        }
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
        tree_guard: RwLockWriteGuard<IntElem<A, K, V>>, map: HashSet<usize>,
        range: R, txg: TxgT)
        -> impl Future<Item=(), Error=Error> + Send
        where K: Borrow<T>,
              R: Debug + Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + 'static + Debug
    {
        let initial_rg: Option<TreeWriteGuard<A, K, V>> = None;
        let inner2 = inner.clone();
        let inner3 = inner.clone();
        // Keep merging down the root as long as it has 1 child
        future::loop_fn((tree_guard, initial_rg), move |(tree_guard, _)| {
            Tree::xlock_and_merge_root(inner.clone(), tree_guard, txg)
            .and_then(move |(tree_guard, root_guard)| {
                if !root_guard.is_leaf() &&
                    root_guard.as_int().nchildren() == 1 {
                    Ok(Loop::Continue((tree_guard, None)))
                } else {
                    Ok(Loop::Break((tree_guard, Some(root_guard))))
                }
            })
        })
        .and_then(move |(tree_guard, root_guard)| {
            Tree::range_delete_pass2_r(inner2, root_guard.unwrap(), map, range,
                                       txg)
            .map(|_| tree_guard)
        }).and_then(move |tree_guard| {
            // Keep merging down the root as long as it has 1 child
            future::loop_fn(tree_guard, move |tree_guard| {
                Tree::xlock_and_merge_root(inner3.clone(), tree_guard, txg)
                .and_then(move |(tree_guard, root_guard)| {
                    if !root_guard.is_leaf() &&
                        root_guard.as_int().nchildren() == 1 {
                        Ok(Loop::Continue(tree_guard))
                    } else {
                        Ok(Loop::Break(tree_guard))
                    }
                })
            })
        }).map(drop)
    }

    fn range_delete_pass2_r<R, T>(inner: Arc<Inner<A, D, K, V>>,
        guard: TreeWriteGuard<A, K, V>, mut map: HashSet<usize>, range: R,
        txg: TxgT)
        -> Box<dyn Future<Item=HashSet<usize>, Error=Error> + Send>
        where K: Borrow<T>,
              R: Debug + Clone + RangeBounds<T> + Send + 'static,
              T: Ord + Clone + 'static + Debug
    {
        if guard.is_leaf() {
            Box::new(Ok(map).into_future())
        } else {
            // Descend into every affected child
            let mut to_fix = Vec::with_capacity(2);
            to_fix.extend((0..guard.len())
            .filter_map(|i| {
                if let TreePtr::Mem(p) = &guard.as_int().children[i].ptr {
                    let id = &**p as *const Node<A, K, V> as usize;
                    if map.remove(&id) {
                        Some(i)
                    } else {
                        None
                    }
                } else {
                    // This child obviously wasn't affected in pass1
                    None
                }
            }).take(2));    // No more than two children can be in the cut
            if to_fix.is_empty() {
                return Box::new(Ok(map).into_future());
            }
            let inner3 = inner.clone();
            let inner7 = inner.clone();
            let range3 = range.clone();
            let min_fanout = inner.min_fanout;
            let left_idx = to_fix[0];
            let right_idx = to_fix.get(1).cloned();
            let underflow = move |guard: &TreeWriteGuard<A, K, V>, common: bool|
            {
                if guard.is_leaf() {
                    guard.underflow(min_fanout)
                } else {
                    // We need to fix IntNodes that might underflow if their
                    // children get merged.
                    if common {
                        // If this node is a common ancestor of all affected
                        // nodes, then it may have up to 2 children that get
                        // merged
                        guard.underflow(min_fanout + 2)
                    } else {
                        // If it's not a common ancestor, then at most one child
                        // will get merged
                        guard.underflow(min_fanout + 1)
                    }
                }
            };
            let fut = guard.xlock(&inner.dml, left_idx, txg)
            .and_then(move |(guard, child_guard)| {
                // After a range_delete, fixing once may be insufficient to
                // fix a Node, because two nodes may merge that could have
                // as few as one child each.  We may need to fix multiple times.
                future::loop_fn((guard, child_guard, 0, 0, false),
                    move |(guard, child_guard, mb, mm, stole)|
                {
                    // The left node may have become a common ancestor if we
                    // merged it with the right node, or if we stole some nodes
                    // from the right node.
                    let common = right_idx.is_none() || mm > 0 || stole;
                    // Don't fix if it has no siblings; that can happen if we
                    // need to merge the root down.
                    if underflow(&child_guard, common) && guard.len() > 1 {
                        let i = left_idx - mb;
                        let inner8 = inner7.clone();
                        let fut = Tree::fix_int(&inner7, guard, i,
                                                child_guard, txg)
                        .and_then(move |(guard, nmb, nma)| {
                            guard.xlock(&inner8.dml, i - nmb as usize, txg)
                            .map(move |(guard, child_guard)| {
                                let mb = mb + nmb as usize;
                                let mm = mm + nma as usize;
                                let stole = (nma == 0 && nmb == 0) || stole;
                                let r = (guard, child_guard, mb, mm, stole);
                                Loop::Continue(r)
                            })
                        });
                        boxfut!(fut)
                    } else {
                        let r = Loop::Break((guard, child_guard, mb, mm));
                        boxfut!(Ok(r).into_future())
                    }
                })
            }).and_then(move |(guard, child_guard, mb, mm):
                (TreeWriteGuard<A, K, V>, TreeWriteGuard<A, K, V>, usize, usize)|
            {
                // Recurse into the left child
                Tree::range_delete_pass2_r(inner3, child_guard, map,
                                           range3, txg)
                .map(move |map| (guard, map, mb, mm))
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
                                .map(move |(guard, child_guard)|
                                     (guard, child_guard, mb, mm + nmb as usize)
                                 )
                            });
                            boxfut!(fut)
                        } else {
                            let r = Ok((guard, child_guard, mb, mm));
                            Box::new(r.into_future())
                        }
                    }).and_then(move |(guard, child_guard, mb, mm)| {
                        // Now recurse into the right child
                        Tree::range_delete_pass2_r(inner8, child_guard, map,
                                                   range, txg)
                        .map(move |map| (guard, map, mb, mm))
                    });
                    boxfut!(fut)
                } else {
                    boxfut!(Ok((guard, map, mb, mm)).into_future())
                }
            }).map(|(_guard, map, _mb, _mm)| map);
            boxfut!(fut)
        }
    }

    fn new(dml: Arc<D>, min_fanout: u64, max_fanout: u64, max_size: u64) -> Self
    {
        // Since there are no on-disk children, the initial TXG range is empty
        let i: Arc<Inner<A, D, K, V>> = Arc::new(Inner {
            height: Atomic::new(1),
            min_fanout, max_fanout,
            _max_size: max_size,
            root: RwLock::new(IntElem::default()),
            dml
        });
        Tree{ i }
    }

    /// Remove and return the value at key `k`, if any.
    pub fn remove(&self, k: K, txg: TxgT)
        -> impl Future<Item=Option<V>, Error=Error> + Send
    {
        let i2 = self.i.clone();
        self.write()
            .and_then(move |tree_guard| {
                Tree::remove_locked(i2, tree_guard, k, txg)
            })
    }

    /// Remove key `k` from an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn remove_int(inner: Arc<Inner<A, D, K, V>>,
                  parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize, child: TreeWriteGuard<A, K, V>, k: K,
                  txg: TxgT)
        -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>
    {
        // First, fix the node, if necessary.  Merge/steal even if the node
        // currently satifisfies the min_fanout, because we may remove end up
        // removing a child
        if child.underflow(inner.min_fanout + 1) {
            let dml2 = inner.dml.clone();
            Box::new(
                Tree::fix_int(&inner, parent, child_idx, child, txg)
                    .and_then(move |(parent, _, _)| {
                        let child_idx = parent.as_int().position(&k);
                        parent.xlock(&dml2, child_idx, txg)
                    }).and_then(move |(parent, child)| {
                        drop(parent);
                        Tree::remove_no_fix(inner, child, k, txg)
                    })
            )
        } else {
            drop(parent);
            Tree::remove_no_fix(inner, child, k, txg)
        }
    }

    /// Helper for `remove`.  Handles removal once the tree is locked
    fn remove_locked(inner: Arc<Inner<A, D, K, V>>,
                     tree_guard: RwLockWriteGuard<IntElem<A, K, V>>, k: K,
                     txg: TxgT)
        -> impl Future<Item=Option<V>, Error=Error> + Send
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
        -> Box<dyn Future<Item=Option<V>, Error=Error> + Send>
    {

        if node.is_leaf() {
            let old_v = node.as_leaf_mut().remove(&k);
            Box::new(Ok(old_v).into_future())
        } else {
            let child_idx = node.as_int().position(&k);
            let fut = node.xlock(&inner.dml, child_idx, txg);
            Box::new(fut.and_then(move |(parent, child)| {
                    Tree::remove_int(inner, parent, child_idx, child, k,
                                     txg)
                })
            )
        }
    }

    /// Flush all in-memory Nodes to disk
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
    pub fn flush(&self, txg: TxgT) -> impl Future<Item=(), Error=Error>
    {
        let dml2 = self.i.dml.clone();
        self.write()
            .and_then(move |root_guard| {
            if root_guard.ptr.is_dirty() {
                // If the root is dirty, then we have ownership over it.  But
                // another task may still have a lock on it.  We must acquire
                // then release the lock to ensure that we have the sole
                // reference.
                let fut = Tree::xlock_root(&dml2, root_guard, txg)
                    .and_then(move |(mut root_guard, child_guard)|
                {
                    drop(child_guard);
                    let ptr = mem::replace(&mut root_guard.ptr, TreePtr::None);
                    Tree::flush_r(dml2, *ptr.into_node(), txg)
                        .map(move |(addr, txgs)| {
                            root_guard.ptr = TreePtr::Addr(addr);
                            root_guard.txgs = txgs;
                        })
                });
                boxfut!(fut)
            } else {
                Box::new(future::ok::<_, Error>(()))
            }
        })
    }

    /// Render the Tree into a `TreeOnDisk` object.  Requires that the Tree
    /// already be flushed.  Will fail if the Tree is dirty.
    pub fn serialize(&self) -> Result<TreeOnDisk, Error> {
        self.i.root.try_read()
        .map(|root_guard| {
            let root = IntElem::<A, K, V>{
                key: root_guard.key,
                txgs: root_guard.txgs.clone(),
                ptr: TreePtr::Addr(*root_guard.ptr.as_addr())
            };
            let iod = InnerOnDisk{
                height: self.i.height.load(Ordering::Relaxed),
                min_fanout: self.i.min_fanout,
                max_fanout: self.i.max_fanout,
                _max_size: self.i._max_size,
                root
            };
            TreeOnDisk(bincode::serialize(&iod).unwrap())
        }).or(Err(Error::EDEADLK))
    }

    // Clippy has a false positive on `node`
    #[allow(clippy::needless_pass_by_value)]
    fn write_leaf(dml: Arc<D>, node: Node<A, K, V>, txg: TxgT)
        -> impl Future<Item=A, Error=Error>
    {
        node.0.try_unwrap().unwrap().into_leaf().flush(&*dml, txg)
        .and_then(move |leaf_data| {
            let node = Node::new(NodeData::Leaf(leaf_data));
            let arc: Arc<Node<A, K, V>> = Arc::new(node);
            dml.put(arc, Compression::None, txg)
        })
    }

    fn flush_r(dml: Arc<D>, mut node: Node<A, K, V>, txg: TxgT)
        -> Box<dyn Future<Item=(D::Addr, Range<TxgT>), Error=Error> + Send>
    {
        if node.0.get_mut().expect("node.0.get_mut").is_leaf() {
            let fut = Tree::write_leaf(dml, node, txg)
                .map(move |addr| {
                    (addr, txg..txg + 1)
                });
            return Box::new(fut);
        }
        let mut ndata = node.0.try_write().expect("node.0.try_write");

        // We need to flush each dirty child, rewrite its TreePtr, and update
        // the Node's txg range.  Satisfying the borrow checker requires that
        // xlock's continuation have ownership over the child IntElem.  So we
        // need to deconstruct the entire NodeData.children vector and
        // reassemble it after the join_all
        let dml2 = dml.clone();
        let children_fut = ndata.as_int_mut().children.drain(..)
        .map(move |elem| {
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
                    Tree::flush_r(dml3, *elem.ptr.into_node(), txg)
                }).map(move |(addr, txgs)| {
                    IntElem::new(key, txgs, TreePtr::Addr(addr))
                });
                boxfut!(fut, IntElem<A, K, V>, Error)
            } else { // LCOV_EXCL_LINE kcov false negative
                boxfut!(future::ok(elem), IntElem<A, K, V>, Error)
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
                let arc: Arc<Node<A, K, V>> = Arc::new(node);
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

    fn read_root(inner: &Inner<A, D, K, V>)
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

    fn write_root(inner: &Inner<A, D, K, V>)
        -> impl Future<Item=RwLockWriteGuard<IntElem<A, K, V>>, Error=Error>
    {
        inner.root.write().map_err(|_| Error::EPIPE)
    }

    /// Lock the root `IntElem` exclusively.  If it is not already resident in
    /// memory, then COW it.  If it has an only child, merge the root node with
    /// its child.
    fn xlock_and_merge_root(inner: Arc<Inner<A, D, K, V>>,
                  tree_guard: RwLockWriteGuard<IntElem<A, K, V>>, txg: TxgT)
        -> impl Future<Item=(RwLockWriteGuard<IntElem<A, K, V>>,
                             TreeWriteGuard<A, K, V>),
                       Error=Error> + Send
    {
        type MyFut<A, K, V> =
            Box<dyn Future<Item=(RwLockWriteGuard<IntElem<A, K, V>>,
                                 TreeWriteGuard<A, K, V>),
                           Error=Error> + Send>;
        // First, lock the root IntElem
        // If it's not a leaf and has a single child:
        //     Drop the root's intelem lock
        //     Replace the root's IntElem with the child's
        //     Relock the root's intelem
        // If it's not a leaf and has no children:
        //     Make it a leaf
        //     Fix the tree's height
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
                    let fut = Tree::xlock_root(&inner.dml, tree_guard, txg);
                    Box::new(fut) as MyFut<A, K, V>
                } else if !root_guard.is_leaf() && nchildren == 0 {
                    drop(root_guard);
                    let new_root = IntElem::default();
                    *tree_guard = new_root;
                    // The root's key must always be the absolute minimum
                    tree_guard.key = K::min_value();
                    inner.height.store(1, Ordering::Relaxed);
                    let fut = Tree::xlock_root(&inner.dml, tree_guard, txg);
                    Box::new(fut) as MyFut<A, K, V>
                } else {
                    Box::new(Ok((tree_guard, root_guard)).into_future())
                        as MyFut<A, K, V>
                }
            })
    }

    /// Lock the root `IntElem` exclusively.  If it is not already resident in
    /// memory, then COW it.
    fn xlock_root(dml: &Arc<D>, mut guard: RwLockWriteGuard<IntElem<A, K, V>>,
                  txg: TxgT)
        -> (Box<dyn Future<Item=(RwLockWriteGuard<IntElem<A, K, V>>,
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
        -> impl Future<Item=(), Error=Error> + Send
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
        stream::iter_ok(0..tree_height).for_each(move |echelon| {
            let inner3 = inner2.clone();
            CleanZonePass1::new(inner2.clone(), pbas.clone(),
                                txgs.clone(), echelon)
                .collect()
                .and_then(move |nodes| {
                    stream::iter_ok(nodes.into_iter()).for_each(move |node| {
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
        -> impl Future<Item=(VecDeque<NodeId<K>>, Option<K>), Error=Error>
    {
        Tree::<ddml::DRP, D, K, V>::read_root(&*inner)
            .and_then(move |tree_guard| {
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
                    boxfut!(future::ok((dirty, None)))
                } else {
                    let dml2 = inner.dml.clone();
                    let fut = tree_guard.rlock(&dml2)
                         .and_then(move |guard| {
                             drop(tree_guard);
                             Tree::get_dirty_nodes_r(dml2, guard, h - 1, None,
                                                     params)
                         });
                    boxfut!(fut)
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
        -> Box<dyn Future<Item=(VecDeque<NodeId<K>>, Option<K>),
                          Error=Error> + Send>
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
            return Box::new(future::ok((nodes, next_key)))
        }
        // Find the first child >= key whose txg range overlaps with txgs
        let idx0 = guard.as_int().position(&params.key);
        let idx_in_range = (idx0..guard.as_int().nchildren()).filter(|idx| {
            ranges_overlap(&guard.as_int().children[*idx].txgs, &params.txgs)
        }).nth(0);
        if let Some(idx) = idx_in_range {
            let next_key = if idx < guard.as_int().nchildren() - 1 {
                Some(guard.as_int().children[idx + 1].key)
            } else {
                next_key
            };
            let child_fut = guard.as_int().children[idx].rlock(&dml);
            boxfut!(
                child_fut.and_then(move |child_guard| {
                    drop(guard);
                    Tree::get_dirty_nodes_r(dml, child_guard, height - 1,
                                            next_key, params)
                })
            )
        } else {
            boxfut!(future::ok((VecDeque::new(), next_key)))
        }
    }

    /// Rewrite `node`, without modifying its contents
    fn rewrite_node(inner: Arc<Inner<ddml::DRP, D, K, V>>,
                    node: NodeId<K>, txg: TxgT)
        -> impl Future<Item=(), Error=Error> + Send
    {
        Tree::<ddml::DRP, D, K, V>::write_root(&*inner)
        .and_then(move |mut guard| {
            let h = inner.height.load(Ordering::Relaxed) as u8;
            let dml2 = inner.dml.clone();
            if h == node.height + 1 {
                // Clean the root node
                if guard.ptr.is_mem() {
                    // Another thread has already dirtied the root.  Nothing to
                    // do!
                    return boxfut!(future::ok(()));
                }
                let fut = dml2.pop::<Arc<Node<ddml::DRP, K, V>>,
                                     Arc<Node<ddml::DRP, K, V>>>(
                                        guard.ptr.as_addr(), txg)
                    .and_then(move |arc| {
                        dml2.put(*arc, Compression::None, txg)
                    }).map(move |addr| {
                        let new = TreePtr::Addr(addr);
                        guard.ptr = new;
                    });
                boxfut!(fut)
            } else {
                let fut = Tree::xlock_root(&dml2, guard, txg)
                     .and_then(move |(_root_guard, child_guard)| {
                         Tree::rewrite_node_r(dml2, child_guard, h - 1, node,
                                              txg)
                     });
                boxfut!(fut)
            }
        })
    }

    fn rewrite_node_r(dml: Arc<D>, mut guard: TreeWriteGuard<ddml::DRP, K, V>,
                      height: u8, node: NodeId<K>, txg: TxgT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
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
            Box::new(fut)
        } else {
            let fut = guard.xlock(&dml, child_idx, txg)
                .and_then(move |(parent_guard, child_guard)| {
                    drop(parent_guard);
                    Tree::rewrite_node_r(dml, child_guard, height - 1, node, txg)
                });
            Box::new(fut)
        }
    }
}

/// The serialized, on-disk representation of a `Tree`
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TreeOnDisk(Vec<u8>);

#[cfg(test)]
impl Default for TreeOnDisk {
    fn default() -> Self {
        TreeOnDisk(Vec::new())
    }
}

impl Value for TreeOnDisk {}

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
