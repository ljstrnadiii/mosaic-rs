use std::borrow::Borrow;
use std::hash::Hash;

use lru::LruCache;

#[derive(Clone)]
struct WeightedValue<V> {
    value: V,
    bytes: usize,
}

/// LRU cache bounded by a byte budget.
///
/// The cache evicts least-recently-used entries until `current_bytes <= max_bytes`.
pub struct ByteLruCache<K, V> {
    lru: LruCache<K, WeightedValue<V>>,
    max_bytes: usize,
    current_bytes: usize,
}

impl<K: Hash + Eq, V> ByteLruCache<K, V> {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            lru: LruCache::unbounded(),
            max_bytes: max_bytes.max(1),
            current_bytes: 0,
        }
    }

    pub fn get_cloned<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        V: Clone,
    {
        self.lru.get(key).map(|v| v.value.clone())
    }

    pub fn put(&mut self, key: K, value: V, bytes: usize) {
        let bytes = bytes.max(1);
        if bytes > self.max_bytes {
            return;
        }

        if let Some(old) = self.lru.pop(&key) {
            self.current_bytes = self.current_bytes.saturating_sub(old.bytes);
        }

        self.lru.put(key, WeightedValue { value, bytes });
        self.current_bytes = self.current_bytes.saturating_add(bytes);

        while self.current_bytes > self.max_bytes {
            if let Some((_k, evicted)) = self.lru.pop_lru() {
                self.current_bytes = self.current_bytes.saturating_sub(evicted.bytes);
            } else {
                break;
            }
        }
    }
}
