// vim: tw=80
use common::*;
use metrohash::{MetroBuildHasher, MetroHash64};
use std::collections::HashMap;
use std::hash::BuildHasherDefault;


#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Key {
    /// Immutable Record ID.
    Rid(u64),
    /// Data Physical Address.
    Dpa(ClusterT, LbaT),
}

struct LruEntry {
    buf: DivBufShared,
    // TODO: switch from Keys to pointers for faster, albeit unsafe, access
    /// Pointer to the next less recently used entry
    lru: Option<Key>,
    /// Pointer to the next more recently used entry
    mru: Option<Key>,
}

pub struct Cache {
    /// Capacity of the `Cache` in bytes, not number of entries
    _capacity: usize,
    /// Pointer to the least recently used entry
    lru: Option<Key>,
    /// Pointer to the most recently used entry
    mru: Option<Key>,
    /// Current memory consumption of all cache entries, excluding overhead
    size: usize,
    store: HashMap<Key, LruEntry, BuildHasherDefault<MetroHash64>>,
}

impl Cache {
    pub fn get(&mut self, key: &Key) -> Option<DivBuf> {
        if self.mru == Some(*key) {
            Some(self.store.get(key).unwrap().buf.try().unwrap())
        } else {
                //self.remove(key).map(|dbs| {
                    //let db = dbs.try().unwrap();
                    //self.insert(*key, dbs);
                    //db
                //})
            //}
            let mru = self.mru;
            let mut v_mru = None;
            let mut v_lru = None;
            self.store.get_mut(key).map(|v| {
                v_mru = v.mru;
                v_lru = v.lru;
                v.mru = None;
                v.lru = mru;
                v.buf.try().unwrap()
            }).map(|buf| {
                if v_mru.is_some() {
                    self.store.get_mut(&v_mru.unwrap()).unwrap().lru = v_lru;
                } else {
                    debug_assert_eq!(self.mru, Some(*key));
                }
                if v_lru.is_some() {
                    self.store.get_mut(&v_lru.unwrap()).unwrap().mru = v_mru;
                } else {
                    debug_assert_eq!(self.lru, Some(*key));
                    self.lru = v_mru;
                }
                if mru.is_some() {
                    self.store.get_mut(&mru.unwrap()).unwrap().mru = Some(*key);
                }
                self.mru = Some(*key);
                buf
            })
        }
    }

    pub fn insert(&mut self, key: Key, buf: DivBufShared) {
        self.size += buf.len();
        let entry = LruEntry { buf, mru: None, lru: self.mru};
        assert!(self.store.insert(key, entry).is_none());
        if self.mru.is_some() {
            self.store.get_mut(&self.mru.unwrap()).map(|v| {
                debug_assert!(v.mru.is_none());
                v.mru = Some(key);
            });
        }
        self.mru = Some(key);
        if self.lru.is_none() {
            self.lru = Some(key);
        }
    }

    pub fn remove(&mut self, key: &Key) -> Option<DivBufShared> {
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

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let store = HashMap::with_hasher(MetroBuildHasher::default());
        Cache{_capacity: capacity, lru: None, mru: None, size: 0, store}
    }
}

/// Get the least recently used entry
#[test]
fn test_get_lru() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(1);
    let key2 = Key::Rid(2);
    let dbs = DivBufShared::from(vec![0u8; 5]);
    cache.insert(key1, dbs);
    let dbs = DivBufShared::from(vec![0u8; 7]);
    cache.insert(key2, dbs);

    assert_eq!(cache.get(&key1).unwrap().len(), 5);
    assert_eq!(cache.mru, Some(key1));
    assert_eq!(cache.lru, Some(key2));
    {
        let v = cache.store.get(&key1).unwrap();
        assert_eq!(v.mru, None);
        assert_eq!(v.lru, Some(key2));
    }
    {
        let v = cache.store.get(&key2).unwrap();
        assert_eq!(v.lru, None);
        assert_eq!(v.mru, Some(key1));
    }
}

/// Get an entry which is neither the MRU nor LRU
#[test]
fn test_get_middle() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(1);
    let key2 = Key::Rid(2);
    let key3 = Key::Rid(3);
    let dbs = DivBufShared::from(vec![0u8; 5]);
    cache.insert(key1, dbs);
    let dbs = DivBufShared::from(vec![0u8; 7]);
    cache.insert(key2, dbs);
    let dbs = DivBufShared::from(vec![0u8; 11]);
    cache.insert(key3, dbs);

    assert_eq!(cache.get(&key2).unwrap().len(), 7);
    assert_eq!(cache.mru, Some(key2));
    assert_eq!(cache.lru, Some(key1));
    {
        let v = cache.store.get(&key1).unwrap();
        assert_eq!(v.mru, Some(key3));
    }
    {
        let v = cache.store.get(&key2).unwrap();
        assert_eq!(v.lru, Some(key3));
    }
    {
        let v = cache.store.get(&key3).unwrap();
        assert_eq!(v.lru, Some(key1));
        assert_eq!(v.mru, Some(key2));
    }
}

/// Get the most recently used entry
#[test]
fn test_get_mru() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(1);
    let key2 = Key::Rid(2);
    let dbs = DivBufShared::from(vec![0u8; 5]);
    cache.insert(key1, dbs);
    let dbs = DivBufShared::from(vec![0u8; 7]);
    cache.insert(key2, dbs);

    assert_eq!(cache.get(&key2).unwrap().len(), 7);
    assert_eq!(cache.lru, Some(key1));
    assert_eq!(cache.mru, Some(key2));
    {
        let v = cache.store.get(&key1).unwrap();
        assert_eq!(v.mru, Some(key2));
    }
    {
        let v = cache.store.get(&key2).unwrap();
        assert_eq!(v.lru, Some(key1));
    }
}

/// Get multiple references to the same entry, which isn't the MRU
#[test]
fn test_get_multiple() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(1);
    let key2 = Key::Rid(2);
    let dbs = DivBufShared::from(vec![0u8; 5]);
    cache.insert(key1, dbs);
    let dbs = DivBufShared::from(vec![0u8; 7]);
    cache.insert(key2, dbs);

    let ref1 = cache.get(&key1).unwrap();
    {
        // Move key2 to the MRU position
        let _ = cache.get(&key2).unwrap();
    }
    let ref2 = cache.get(&key1).unwrap();
    assert_eq!(ref1.len(), 5);
    assert_eq!(ref2.len(), 5);
}

/// Get a nonexistent key
#[test]
fn test_get_nonexistent() {
    let mut cache = Cache::with_capacity(100);
    let key = Key::Rid(0);
    assert!(cache.get(&key).is_none());
}

/// Insert a duplicate value
#[test]
#[should_panic]
fn test_insert_dup() {
    let mut cache = Cache::with_capacity(100);
    let dbs1 = DivBufShared::from(vec![0u8; 6]);
    let dbs2 = DivBufShared::from(vec![0u8; 11]);
    let key = Key::Rid(0);
    cache.insert(key, dbs1);
    cache.insert(key, dbs2);
}

/// Insert the first value into an empty cache
#[test]
fn test_insert_empty() {
    let mut cache = Cache::with_capacity(100);
    let dbs = DivBufShared::from(vec![0u8; 6]);
    let key = Key::Rid(0);
    cache.insert(key, dbs);
    assert_eq!(cache.size(), 6);
    assert_eq!(cache.lru, Some(key));
    assert_eq!(cache.mru, Some(key));
    {
        let v = cache.store.get(&key).unwrap();
        assert!(v.lru.is_none());
        assert!(v.mru.is_none());
    }
    assert_eq!(cache.get(&key).unwrap().len(), 6);
}

/// Insert into a cache that already has 1 item.
#[test]
fn test_insert_one() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(1);
    let key2 = Key::Rid(2);
    let dbs = DivBufShared::from(vec![0u8; 5]);
    cache.insert(key1, dbs);
    let dbs = DivBufShared::from(vec![0u8; 7]);
    cache.insert(key2, dbs);
    assert_eq!(cache.size(), 12);
    assert_eq!(cache.lru, Some(key1));
    assert_eq!(cache.mru, Some(key2));
    {
        let v = cache.store.get(&key2).unwrap();
        assert_eq!(v.lru, Some(key1));
        assert!(v.mru.is_none());
    }
    assert_eq!(cache.get(&key2).unwrap().len(), 7);
}

/// Remove a nonexistent key.  Unlike inserting a dup, this is not an error
#[test]
fn test_remove_nonexistent() {
    let mut cache = Cache::with_capacity(100);
    let key = Key::Rid(0);
    assert!(cache.remove(&key).is_none());
}

/// Remove the last key from a cache
#[test]
fn test_remove_last() {
    let mut cache = Cache::with_capacity(100);
    let dbs = DivBufShared::from(vec![0u8; 6]);
    let key = Key::Rid(0);
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
    let key1 = Key::Rid(1);
    let key2 = Key::Rid(2);
    let dbs = DivBufShared::from(vec![0u8; 5]);
    cache.insert(key1, dbs);
    let dbs = DivBufShared::from(vec![0u8; 7]);
    cache.insert(key2, dbs);

    assert_eq!(cache.remove(&key1).unwrap().len(), 5);
    assert!(cache.store.get(&key1).is_none());
    assert_eq!(cache.size(), 7);
    assert_eq!(cache.lru, Some(key2));
    {
        let v = cache.store.get(&key2).unwrap();
        assert!(v.lru.is_none());
    }
}

/// Remove an entry which is neither the MRU nor the LRU
#[test]
fn test_remove_middle() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(1);
    let key2 = Key::Rid(2);
    let key3 = Key::Rid(3);
    let dbs = DivBufShared::from(vec![0u8; 5]);
    cache.insert(key1, dbs);
    let dbs = DivBufShared::from(vec![0u8; 7]);
    cache.insert(key2, dbs);
    let dbs = DivBufShared::from(vec![0u8; 11]);
    cache.insert(key3, dbs);

    assert_eq!(cache.remove(&key2).unwrap().len(), 7);
    assert!(cache.store.get(&key2).is_none());
    assert_eq!(cache.size(), 16);
    assert_eq!(cache.lru, Some(key1));
    assert_eq!(cache.mru, Some(key3));
    {
        let v = cache.store.get(&key1).unwrap();
        assert_eq!(v.mru, Some(key3));
    }
    {
        let v = cache.store.get(&key3).unwrap();
        assert_eq!(v.lru, Some(key1));
    }
}

/// Remove the most recently used entry
#[test]
fn test_remove_mru() {
    let mut cache = Cache::with_capacity(100);
    let key1 = Key::Rid(1);
    let key2 = Key::Rid(2);
    let dbs = DivBufShared::from(vec![0u8; 5]);
    cache.insert(key1, dbs);
    let dbs = DivBufShared::from(vec![0u8; 7]);
    cache.insert(key2, dbs);

    assert_eq!(cache.remove(&key2).unwrap().len(), 7);
    assert!(cache.store.get(&key2).is_none());
    assert_eq!(cache.size(), 5);
    assert_eq!(cache.mru, Some(key1));
    {
        let v = cache.store.get(&key1).unwrap();
        assert!(v.mru.is_none());
    }
}
