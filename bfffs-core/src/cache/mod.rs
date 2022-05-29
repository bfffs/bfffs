// vim: tw=80

// https://github.com/fkoep/downcast-rs/issues/6
#![allow(clippy::missing_safety_doc)]

use crate::types::{PBA, RID};
use divbuf::{DivBuf, DivBufShared};
use downcast::*;
use std::{
    borrow::Borrow,
    fmt::Debug,
};

mod lru;

/// Key types used by `Cache`
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Key {
    /// Immutable Record ID.
    Rid(RID),
    /// Physical Block Address, as returned by `Pool::write`.
    PBA(PBA),
}

/// Types that implement `Cacheable` may be stored in the cache
pub trait Cacheable: Any + Debug + Send + Sync {
    /// Deserialize a buffer into Self.  Will panic if deserialization fails.
    fn deserialize(dbs: DivBufShared) -> Self where Self: Sized;

    /// Returns true if the two `Cacheable`s' contents are equal
    // This doesn't implement PartialEq because the rhs is &Cacheable instead of
    // &Self.
    fn eq(&self, other: &dyn Cacheable) -> bool;

    /// How much space does this object use in the Cache?
    fn cache_space(&self) -> usize;

    /// Return a read-only handle to this object.
    ///
    /// As long as this handle is alive, the object will not be evicted from
    /// cache.
    fn make_ref(&self) -> Box<dyn CacheRef>;

    /// How many bytes of writeback credit should this object use?
    //
    // It may not be equal to cache_space, because the wb_space needs to be
    // extend()able and split()able.
    fn wb_space(&self) -> usize;
}

downcast!(dyn Cacheable);

/// Types that implement `CacheRef` are read-only handles to cached objects.
pub trait CacheRef: Any + Send {
    /// Deserialize a buffer into the kind of `Cacheable` that's associated with
    /// this `CacheRef`.  Will panic if deserialization fails.
    fn deserialize(dbs: DivBufShared) -> Box<dyn Cacheable> where Self: Sized;

    /// Serialize to a `DivBuf`.
    fn serialize(&self) -> DivBuf;

    /// Convert this shared `CacheRef` into an owned `Cacheable`, which may or
    /// may not involve copying
    fn into_owned(self) -> Box<dyn Cacheable>;
}

downcast!(dyn CacheRef);

impl Cacheable for DivBufShared {
    fn deserialize(dbs: DivBufShared) -> Self where Self: Sized {
        dbs
    }

    fn eq(&self, other: &dyn Cacheable) -> bool {
        if let Ok(other_dbs) = other.downcast_ref::<DivBufShared>() {
            self.try_const().unwrap()[..] == other_dbs.try_const().unwrap()[..]
        } else {
            // other isn't even the same concrete type
            false
        }
    }

    fn cache_space(&self) -> usize {
        // TODO: add the const overhead, but adjust the code in examples/fanout
        // not to count it for purposes of computing the metadata fraction.
        self.len()
    }

    fn make_ref(&self) -> Box<dyn CacheRef> {
        Box::new(self.try_const().unwrap())
    }

    fn wb_space(&self) -> usize {
        self.len()
    }
}

impl CacheRef for DivBuf {
    fn deserialize(dbs: DivBufShared) -> Box<dyn Cacheable> where Self: Sized {
        Box::new(dbs)
    }

    fn serialize(&self) -> DivBuf {
        self.clone()
    }

    fn into_owned(self) -> Box<dyn Cacheable> {
        // Data copy
        Box::new(DivBufShared::from(self[..].to_vec()))
    }
}

impl Borrow<dyn CacheRef> for DivBuf {
    fn borrow(&self) -> &dyn CacheRef {
        self as &dyn CacheRef
    }
}

/// Basic read-only block cache.
///
/// Caches on-disk blocks by either their address (cluster and LBA pair), or
/// their Record ID.  The cache is read-only because any attempt to change a
/// block would also require changing either its address or record ID.
#[derive(Debug)]
pub struct Cache(self::lru::LruCache);

impl Cache {
    /// Get the maximum memory consumption of the cache, in bytes.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Drop all data from the cache, for testing or benchmarking purposes
    // NB: this should be called "drop", but that conflicts with
    // "std::Drop::drop"
    pub fn drop_cache(&mut self) {
        self.0.drop_cache()
    }

    /// Get a read-only reference to a cached block.
    ///
    /// The block will be marked as the most recently used.
    pub fn get<T: CacheRef>(&mut self, key: &Key) -> Option<Box<T>> {
        self.0.get(key)
    }

    /// Get a read-only generic reference to a cached block.
    ///
    /// The returned reference will not be downcastted to a concrete type, and
    /// the cache's internal state will not be updated.  That is, this method
    /// does not count as an access for the cache replacement algorithm.
    pub fn get_ref(&self, key: &Key) -> Option<Box<dyn CacheRef>> {
        self.0.get_ref(key)
    }

    /// Add a new block to the cache.
    ///
    /// The block will be marked as the most recently used.
    #[tracing::instrument(skip(self, buf))]
    pub fn insert(&mut self, key: Key, buf: Box<dyn Cacheable>) {
        self.0.insert(key, buf)
    }

    /// Remove a block from the cache.
    ///
    /// Unlike `get`, the block will be returned in an owned form, if it was
    /// present at all.
    pub fn remove(&mut self, key: &Key) -> Option<Box<dyn Cacheable>> {
        self.0.remove(key)
    }

    /// Get the current memory consumption of the cache, in bytes.
    ///
    /// Only the cached blocks themselves are included, not the overhead of
    /// managing them.
    pub fn size(&self) -> usize {
        self.0.size()
    }

    /// Create a new cache with the given capacity, in bytes.
    pub fn with_capacity(capacity: usize) -> Self {
        Self(self::lru::LruCache::with_capacity(capacity))
    }
}
