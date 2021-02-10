// vim: tw=80

// https://github.com/fkoep/downcast-rs/issues/6
#![allow(clippy::missing_safety_doc)]

use crate::common::*;
use downcast::*;
use mockall_double::*;
use std::{
    borrow::Borrow,
    fmt::Debug,
};

mod cache;

#[double]
pub use self::cache::Cache;

/// Key types used by `Cache`
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Key {
    /// Immutable Record ID.
    Rid(RID),
    /// Physical Block Address, as returned by `Pool::write`.
    PBA(PBA),
}

/// Types that implement `Cacheable` may be stored in the cache
// Things in Cache will never be empty, so they don't need is_empty
#[allow(clippy::len_without_is_empty)]
pub trait Cacheable: Any + Debug + Send + Sync {
    /// Deserialize a buffer into Self.  Will panic if deserialization fails.
    fn deserialize(dbs: DivBufShared) -> Self where Self: Sized;

    /// Returns true if the two `Cacheable`s' contents are equal
    // This doesn't implement PartialEq because the rhs is &Cacheable instead of
    // &Self.
    fn eq(&self, other: &dyn Cacheable) -> bool;

    /// How much space does this object use in the Cache?
    fn len(&self) -> usize;

    /// Return a read-only handle to this object.
    ///
    /// As long as this handle is alive, the object will not be evicted from
    /// cache.
    fn make_ref(&self) -> Box<dyn CacheRef>;
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

    fn len(&self) -> usize {
        self.len()
    }

    fn make_ref(&self) -> Box<dyn CacheRef> {
        Box::new(self.try_const().unwrap())
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
