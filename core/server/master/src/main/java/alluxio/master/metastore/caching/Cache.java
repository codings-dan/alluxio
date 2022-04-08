/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.metastore.caching;

import alluxio.master.file.meta.CacheEvictType;
import alluxio.master.metastore.ReadOption;
import alluxio.metrics.MetricKey;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Base class for write-back caches which asynchronously evict entries to backing stores.
 *
 * The cache uses water mark based eviction. A dedicated thread waits for the cache to reach its
 * high water mark, then evicts entries until the cache size reaches the low water mark. All backing
 * store write operations are performed asynchronously in the eviction thread, unless the cache hits
 * maximum capacity. At maximum capacity, methods interact synchronously with the backing store. For
 * best performance, maximum capacity should never be reached. This requires that the eviction
 * thread can keep up cache writes.
 *
 * Cache hit reads are served without any locking. Writes and cache miss reads take locks on their
 * cache key.
 *
 * This class leverages the entry-level locks of ConcurrentHashMap to synchronize operations on the
 * same key.
 *
 * @param <K> the cache key type
 * @param <V> the cache value type
 */
@ThreadSafe
public abstract class Cache<K, V> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Cache.class);

  @VisibleForTesting
  final BaseCache mBaseCache;
  // TODO(andrew): Support using multiple threads to speed up backing store writes.
  // Thread for performing eviction to the backing store.

  /**
   * @param conf cache configuration
   * @param name a name for the cache
   * @param evictionsKey the cache evictions metric key
   * @param hitsKey the cache hits metrics key
   * @param loadTimesKey the load times metrics key
   * @param missesKey the misses metrics key
   * @param sizeKey the size metrics key
   */
  public Cache(CacheConfiguration conf, String name, MetricKey evictionsKey, MetricKey hitsKey,
               MetricKey loadTimesKey, MetricKey missesKey, MetricKey sizeKey) {
    this(conf, name, evictionsKey, hitsKey, loadTimesKey, missesKey, sizeKey,
        CacheEvictType.RANDOM);
  }

  public Cache(CacheConfiguration conf, String name, MetricKey evictionsKey, MetricKey hitsKey,
               MetricKey loadTimesKey, MetricKey missesKey,
               MetricKey sizeKey, CacheEvictType evictType) {
    switch (evictType) {
      case RANDOM:
        mBaseCache = new RandomCache(conf, name, evictionsKey, hitsKey,
          loadTimesKey, missesKey, sizeKey, this);
        break;
      case LRU:
        mBaseCache = new MultiShardLRUCache(conf, name, evictionsKey, hitsKey,
          loadTimesKey, missesKey, sizeKey, this);
        break;
      default:
        throw new RuntimeException("cannot support evictType:" + evictType.getEvictName());
    }
  }

  public void setSkipCache(boolean skipCache) { mBaseCache.setSkipCache(skipCache); }

  /**
   * Retrieves a value from the cache, loading it from the backing store if necessary.
   *
   * If the value needs to be loaded, concurrent calls to get(key) will block while waiting for the
   * first call to finish loading the value.
   *
   * If option.shouldSkipCache() is true, then the value loaded from the backing store will not be
   * cached and the eviction thread will not be woken up.
   *
   * @param key the key to get the value for
   * @param option the read options
   * @return the value, or empty if the key doesn't exist in the cache or in the backing store
   */
  public Optional<V> get(K key, ReadOption option) {
    Optional<Object> ret =  mBaseCache.get(key, option);
    if (!ret.isPresent()) {
      return Optional.empty();
    }
    return Optional.of((V) ret.get());
  }

  /**
   * @param key the key to get the value for
   * @return the result of {@link #get(Object, ReadOption)} with default option
   */
  public Optional<V> get(K key) {
    return get(key, ReadOption.defaults());
  }

  /**
   * Writes a key/value pair to the cache.
   *
   * @param key the key
   * @param value the value
   */
  public void put(K key, V value) {
    mBaseCache.put(key, value);
  }

  /**
   * Removes a key from the cache.
   *
   * The key is not immediately removed from the backing store. Instead, we set the entry's value to
   * null to indicate to the eviction thread that to evict the entry, it must first remove the key
   * from the backing store. However, if the cache is full we must synchronously write to the
   * backing store instead.
   *
   * @param key the key to remove
   */
  public void remove(K key) {
    mBaseCache.remove(key);
  }

  /**
   * Flushes all data to the backing store.
   */
  public void flush() throws InterruptedException {
    mBaseCache.flush();
  }

  /**
   * Clears all entries from the map. This is not threadsafe, and requires external synchronization
   * to prevent concurrent modifications to the cache.
   */
  public void clear() {
    mBaseCache.clear();
  }

  @Override
  public void close() {
    mBaseCache.close();
  }

  @VisibleForTesting
  protected Map<Object, Entry> getCacheMap() {
    Map<Object, Entry> ret = new HashMap<>();
    mBaseCache.mMap.forEach((k, entry) -> {
      ret.put(k, entry);
    });
    return ret;
  }

  //
  // Callbacks so that sub-classes can listen for cache changes. All callbacks on the same key
  // happen atomically with respect to each other and other cache operations.
  //

  /**
   * Callback triggered when an update is made to a key/value pair in the cache. For removals, value
   * will be null.
   *
   * @param key the updated key
   * @param value the updated value, or null if the key is being removed
   */
  protected void onCacheUpdate(K key, @Nullable V value) {}

  /**
   * Callback triggered when a key is removed from the cache.
   *
   * This may be used in conjunction with onCacheUpdate to keep track of all changes to the cache.
   *
   * @param key the removed key
   */
  protected void onCacheRemove(K key) {}

  /**
   * Callback triggered whenever a new key/value pair is added by put(key, value).
   *
   * @param key the added key
   * @param value the added value
   */
  protected void onPut(K key, V value) {}

  /**
   * Callback triggered whenever a key is removed by remove(key).
   *
   * @param key the removed key
   */
  protected void onRemove(K key) {}

  /**
   * Loads a key from the backing store.
   *
   * @param key the key to load
   * @return the value for the key, or empty if the key doesn't exist in the backing store
   */
  protected abstract Optional<V> load(K key);

  /**
   * Writes a key/value pair to the backing store.
   *
   * @param key the key
   * @param value the value
   */
  protected abstract void writeToBackingStore(K key, V value);

  /**
   * Removes a key from the backing store.
   *
   * @param key the key
   */
  protected abstract void removeFromBackingStore(K key);

  /**
   * Attempts to flush the given entries to the backing store.
   *
   * The subclass is responsible for setting each candidate's mDirty field to false on success.
   *
   * @param candidates the candidate entries to flush
   */
  protected abstract void flushEntries(List<Entry> candidates);

  public static class Entry {
    public Object mKey;
    // null value means that the key has been removed from the cache, but still needs to be removed
    // from the backing store.
    @Nullable
    public Object mValue;

    // Whether the entry is out of sync with the backing store. If mDirty is true, the entry must be
    // flushed to the backing store before it can be evicted.
    public volatile boolean mDirty = true;

    // Whether the entry has been recently accessed. Accesses set the bit to true, while the
    // eviction thread sets it to false. This is the same as the "referenced" bit described in the
    // CLOCK algorithm.
    public volatile boolean mReferenced = true;

    public Entry(Object key, Object value) {
      mKey = key;
      mValue = value;
    }
  }
}
