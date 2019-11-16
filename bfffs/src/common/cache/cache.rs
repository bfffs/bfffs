// vim: tw=80
use metrohash::{MetroBuildHasher, MetroHash64};
#[cfg(test)] use mockall::automock;
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    hash::BuildHasherDefault
};
use super::*;

struct LruEntry {
    buf: Box<dyn Cacheable>,
    // TODO: switch from Keys to pointers for faster, albeit unsafe, access
    /// Pointer to the next less recently used entry
    lru: Option<Key>,
    /// Pointer to the next more recently used entry
    mru: Option<Key>,
}

impl Debug for LruEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "LruEntry {{ lru: {:?}, mru: {:?} }}", self.lru, self.mru)
    }
}

/// Basic read-only block cache.
///
/// Caches on-disk blocks by either their address (cluster and LBA pair), or
/// their Record ID.  The cache is read-only because any attempt to change a
/// block would also require changing either its address or record ID.
#[derive(Debug)]
pub struct Cache {
    /// Capacity of the `Cache` in bytes, not number of entries
    capacity: usize,
    /// Pointer to the least recently used entry
    lru: Option<Key>,
    /// Pointer to the most recently used entry
    mru: Option<Key>,
    /// Current memory consumption of all cache entries, excluding overhead
    size: usize,
    /// Block storage.
    store: HashMap<Key, LruEntry, BuildHasherDefault<MetroHash64>>,
}

#[cfg_attr(test, automock)]
impl Cache {
    /// Drop all data from the cache, for testing or benchmarking purposes
    // NB: this should be called "drop", but that conflicts with
    // "std::Drop::drop"
    pub fn drop_cache(&mut self) {
        self.store = HashMap::with_hasher(MetroBuildHasher::default());
        self.lru = None;
        self.mru = None;
        self.size = 0;
    }

    fn expire(&mut self) {
        let key = self.lru;
        assert!(key.is_some(), "Can't find an entry to expire");
        self.remove(&key.unwrap());
    }

    /// Get a read-only reference to a cached block.
    ///
    /// The block will be marked as the most recently used.
    pub fn get<T: CacheRef>(&mut self, key: &Key) -> Option<Box<T>> {
        if self.mru == Some(*key) {
            Some(self.store[key].buf.make_ref().downcast::<T>().unwrap())
        } else {
            let mru = self.mru;
            let mut v_mru = None;
            let mut v_lru = None;
            self.store.get_mut(key).map(|v| {
                v_mru = v.mru;
                v_lru = v.lru;
                v.mru = None;
                v.lru = mru;
                v.buf.make_ref().downcast::<T>().unwrap()
            }).map(|cacheref| {
                self.store.get_mut(&v_mru.unwrap()).unwrap().lru = v_lru;
                if let Some(lru) = &v_lru {
                    self.store.get_mut(lru).unwrap().mru = v_mru;
                } else {
                    debug_assert_eq!(self.lru, Some(*key));
                    self.lru = v_mru;
                }
                if let Some(mru) = &mru {
                    self.store.get_mut(mru).unwrap().mru = Some(*key);
                }
                self.mru = Some(*key);
                cacheref
            })
        }
    }

    /// Get a read-only generic reference to a cached block.
    ///
    /// The returned reference will not be downcastted to a concrete type, and
    /// the cache's internal state will not be updated.  That is, this method
    /// does not count as an access for the cache replacement algorithm.
    pub fn get_ref(&self, key: &Key) -> Option<Box<dyn CacheRef>> {
        self.store.get(key).map(|v| {
            v.buf.make_ref()
        })
    }

    /// Add a new block to the cache.
    ///
    /// The block will be marked as the most recently used.
    pub fn insert(&mut self, key: Key, buf: Box<dyn Cacheable>) {
        while self.size + buf.len() > self.capacity {
            self.expire();
        }
        self.size += buf.len();
        let entry = LruEntry { buf, mru: None, lru: self.mru};
        if let Some(old_entry) = self.store.insert(key, entry) {
            // Inserting two different values with the same key is a bug, but
            // inserting two identical values is merely bad timing.  We must
            // compare the buffers to verify.
            {
                let new_entry = &self.store[&key];
                assert!(old_entry.buf.eq(&*new_entry.buf),
                    "Conflicting value cached with key={:?}", key);
            }
            // Just put the old entry back so we don't have to fix the linkages
            self.store.insert(key, old_entry);
            return;
        }
        if self.mru.is_some() {
            if let Some(v) = self.store.get_mut(&self.mru.unwrap()) {
                debug_assert!(v.mru.is_none());
                v.mru = Some(key);
            }
        }
        self.mru = Some(key);
        if self.lru.is_none() {
            self.lru = Some(key);
        }
    }

    /// Remove a block from the cache.
    ///
    /// Unlike `get`, the block will be returned in an owned form, if it was
    /// present at all.
    pub fn remove(&mut self, key: &Key) -> Option<Box<dyn Cacheable>> {
        self.store.remove(key).map(|v| {
            self.size -= v.buf.len();
            if v.mru.is_some() {
                self.store.get_mut(&v.mru.unwrap()).unwrap().lru = v.lru;
            } else {
                debug_assert_eq!(self.mru, Some(*key));
                self.mru = v.lru;
            }
            if v.lru.is_some() {
                self.store.get_mut(&v.lru.unwrap()).unwrap().mru = v.mru;
            } else {
                debug_assert_eq!(self.lru, Some(*key));
                self.lru = v.mru;
            }
            v.buf
        })
    }

    /// Get the current memory consumption of the cache, in bytes.
    ///
    /// Only the cached blocks themselves are included, not the overhead of
    /// managing them.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Create a new cache with the given capacity, in bytes.
    pub fn with_capacity(capacity: usize) -> Self {
        let store = HashMap::with_hasher(MetroBuildHasher::default());
        Cache{capacity, lru: None, mru: None, size: 0, store}
    }
}

/// Get the least recently used entry
// LCOV_EXCL_START

// pet kcov
#[test]
fn debug() {
    format!("{:?}", Cache::with_capacity(100));
    let dbs = DivBufShared::from(Vec::new());
    let entry = LruEntry{buf: Box::new(dbs), lru: None, mru: None};
    assert_eq!("LruEntry { lru: None, mru: None }", format!("{:?}", entry));
}

#[test]
fn test_drop_cache() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let key3 = Key::Rid(RID(3));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 5]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 7]));
    cache.insert(key2, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 11]));
    cache.insert(key3, dbs);

    cache.drop_cache();

    assert_eq!(cache.size(), 0);
    assert!(cache.get::<DivBuf>(&key1).is_none());
    assert!(cache.get::<DivBuf>(&key2).is_none());
    assert!(cache.get::<DivBuf>(&key3).is_none());
}

#[test]
fn test_get_lru() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 5]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 7]));
    cache.insert(key2, dbs);

    assert_eq!(cache.get::<DivBuf>(&key1).unwrap().len(), 5);
    assert_eq!(cache.mru, Some(key1));
    assert_eq!(cache.lru, Some(key2));
    {
        let v = &cache.store[&key1];
        assert_eq!(v.mru, None);
        assert_eq!(v.lru, Some(key2));
    }
    {
        let v = &cache.store[&key2];
        assert_eq!(v.lru, None);
        assert_eq!(v.mru, Some(key1));
    }
}

/// Get an entry which is neither the MRU nor LRU
#[test]
fn test_get_middle() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let key3 = Key::Rid(RID(3));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 5]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 7]));
    cache.insert(key2, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 11]));
    cache.insert(key3, dbs);

    assert_eq!(cache.get::<DivBuf>(&key2).unwrap().len(), 7);
    assert_eq!(cache.mru, Some(key2));
    assert_eq!(cache.lru, Some(key1));
    {
        let v = &cache.store[&key1];
        assert_eq!(v.mru, Some(key3));
    }
    {
        let v = &cache.store[&key2];
        assert_eq!(v.lru, Some(key3));
    }
    {
        let v = &cache.store[&key3];
        assert_eq!(v.lru, Some(key1));
        assert_eq!(v.mru, Some(key2));
    }
}

/// On insertion, old entries should be expired to prevent overflow
#[test]
fn test_expire_one() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 53]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 57]));
    cache.insert(key2, dbs);

    assert_eq!(cache.size(), 57);
    assert!(cache.get::<DivBuf>(&key1).is_none());
}

/// expire multiple entries if necessary
#[test]
fn test_expire_two() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let key3 = Key::Rid(RID(3));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 41]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 43]));
    cache.insert(key2, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 61]));
    cache.insert(key3, dbs);

    assert_eq!(cache.size(), 61);
    assert!(cache.get::<DivBuf>(&key1).is_none());
    assert!(cache.get::<DivBuf>(&key2).is_none());
}

/// Get the most recently used entry
#[test]
fn test_get_mru() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 5]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 7]));
    cache.insert(key2, dbs);

    assert_eq!(cache.get::<DivBuf>(&key2).unwrap().len(), 7);
    assert_eq!(cache.lru, Some(key1));
    assert_eq!(cache.mru, Some(key2));
    {
        let v = &cache.store[&key1];
        assert_eq!(v.mru, Some(key2));
    }
    {
        let v = &cache.store[&key2];
        assert_eq!(v.lru, Some(key1));
    }
}

/// Get multiple references to the same entry, which isn't the MRU
#[test]
fn test_get_multiple() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 5]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 7]));
    cache.insert(key2, dbs);

    let ref1 = cache.get::<DivBuf>(&key1).unwrap();
    {
        // Move key2 to the MRU position
        let _ = cache.get::<DivBuf>(&key2).unwrap();
    }
    let ref2 = cache.get::<DivBuf>(&key1).unwrap();
    assert_eq!(ref1.len(), 5);
    assert_eq!(ref2.len(), 5);
}

/// Get a nonexistent key
#[test]
fn test_get_nonexistent() {
    let mut cache = Cache::with_capacity(100);
    let key = Key::Rid(RID(0));
    assert!(cache.get::<DivBuf>(&key).is_none());
}

/// Cache::get_ref on an entry in the middle.  Its position in the list should
/// not be changed.
#[test]
fn test_get_ref_middle() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let key3 = Key::Rid(RID(3));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 5]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 7]));
    cache.insert(key2, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 11]));
    cache.insert(key3, dbs);

    let r = cache.get_ref(&key2).unwrap().downcast::<DivBuf>().unwrap();
    assert_eq!(r.len(), 7);
    assert_eq!(cache.mru, Some(key3));
    assert_eq!(cache.lru, Some(key1));
    {
        let v = &cache.store[&key1];
        assert_eq!(v.mru, Some(key2));
    }
    {
        let v = &cache.store[&key2];
        assert_eq!(v.lru, Some(key1));
        assert_eq!(v.mru, Some(key3));
    }
    {
        let v = &cache.store[&key3];
        assert_eq!(v.lru, Some(key2));
    }
}

#[test]
fn test_get_ref_nonexistent() {
    let cache = Cache::with_capacity(100);
    let key = Key::Rid(RID(0));
    assert!(cache.get_ref(&key).is_none());
}

/// Insert a different value for an existing key
#[test]
#[should_panic(expected = "Conflicting value cached with key=Rid(RID(0))")]
fn test_insert_dup_key() {
    let mut cache = Cache::with_capacity(100);
    let dbs1 = Box::new(DivBufShared::from(vec![0u8; 6]));
    let dbs2 = Box::new(DivBufShared::from(vec![0u8; 11]));
    let key = Key::Rid(RID(0));
    cache.insert(key, dbs1);
    cache.insert(key, dbs2);
}

/// Insert the same key/value pair twice
#[test]
fn test_insert_dup_value() {
    let mut cache = Cache::with_capacity(100);
    let dbs1 = Box::new(DivBufShared::from(vec![0u8; 6]));
    let dbs2 = Box::new(DivBufShared::from(vec![0u8; 6]));
    let db1 = dbs1.try_const().unwrap();
    let key = Key::Rid(RID(0));
    cache.insert(key, dbs1);
    cache.insert(key, dbs2);
    let db3 = cache.get::<DivBuf>(&key).unwrap();
    assert_eq!(&db1[..], &db3[..]);

    // Check that the mru/lru entries are consistent
    assert_eq!(cache.mru, Some(key));
    assert_eq!(cache.lru, Some(key));
    let entry = &cache.store[&key];
    assert_eq!(entry.mru, None);
    assert_eq!(entry.lru, None);
}

/// Insert the first value into an empty cache
#[test]
fn test_insert_empty() {
    let mut cache = Cache::with_capacity(100);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 6]));
    let key = Key::Rid(RID(0));
    cache.insert(key, dbs);
    assert_eq!(cache.size(), 6);
    assert_eq!(cache.lru, Some(key));
    assert_eq!(cache.mru, Some(key));
    {
        let v = &cache.store[&key];
        assert!(v.lru.is_none());
        assert!(v.mru.is_none());
    }
    assert_eq!(cache.get::<DivBuf>(&key).unwrap().len(), 6);
}

/// Insert into a cache that already has 1 item.
#[test]
fn test_insert_one() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 5]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 7]));
    cache.insert(key2, dbs);
    assert_eq!(cache.size(), 12);
    assert_eq!(cache.lru, Some(key1));
    assert_eq!(cache.mru, Some(key2));
    {
        let v = &cache.store[&key2];
        assert_eq!(v.lru, Some(key1));
        assert!(v.mru.is_none());
    }
    assert_eq!(cache.get::<DivBuf>(&key2).unwrap().len(), 7);
}

/// Remove a nonexistent key.  Unlike inserting a dup, this is not an error
#[test]
fn test_remove_nonexistent() {
    let mut cache = Cache::with_capacity(100);
    let key = Key::Rid(RID(0));
    assert!(cache.remove(&key).is_none());
}

/// Remove the last key from a cache
#[test]
fn test_remove_last() {
    let mut cache = Cache::with_capacity(100);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 6]));
    let key = Key::Rid(RID(0));
    cache.insert(key, dbs);
    assert_eq!(cache.remove(&key).unwrap().len(), 6);
    assert_eq!(cache.size(), 0);
    assert!(cache.lru.is_none());
    assert!(cache.mru.is_none());
    assert_eq!(cache.size(), 0);
}

/// Remove the least recently used entry
#[test]
fn test_remove_lru() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 5]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 7]));
    cache.insert(key2, dbs);

    assert_eq!(cache.remove(&key1).unwrap().len(), 5);
    assert!(cache.store.get(&key1).is_none());
    assert_eq!(cache.size(), 7);
    assert_eq!(cache.lru, Some(key2));
    {
        let v = &cache.store[&key2];
        assert!(v.lru.is_none());
    }
}

/// Remove an entry which is neither the MRU nor the LRU
#[test]
fn test_remove_middle() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let key3 = Key::Rid(RID(3));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 5]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 7]));
    cache.insert(key2, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 11]));
    cache.insert(key3, dbs);

    assert_eq!(cache.remove(&key2).unwrap().len(), 7);
    assert!(cache.store.get(&key2).is_none());
    assert_eq!(cache.size(), 16);
    assert_eq!(cache.lru, Some(key1));
    assert_eq!(cache.mru, Some(key3));
    {
        let v = &cache.store[&key1];
        assert_eq!(v.mru, Some(key3));
    }
    {
        let v = &cache.store[&key3];
        assert_eq!(v.lru, Some(key1));
    }
}

/// Remove the most recently used entry
#[test]
fn test_remove_mru() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(RID(1));
    let key2 = Key::Rid(RID(2));
    let dbs = Box::new(DivBufShared::from(vec![0u8; 5]));
    cache.insert(key1, dbs);
    let dbs = Box::new(DivBufShared::from(vec![0u8; 7]));
    cache.insert(key2, dbs);

    assert_eq!(cache.remove(&key2).unwrap().len(), 7);
    assert!(cache.store.get(&key2).is_none());
    assert_eq!(cache.size(), 5);
    assert_eq!(cache.mru, Some(key1));
    {
        let v = &cache.store[&key1];
        assert!(v.mru.is_none());
    }
}
// LCOV_EXCL_STOP
