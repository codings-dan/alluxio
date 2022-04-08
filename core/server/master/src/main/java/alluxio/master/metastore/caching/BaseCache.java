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

import alluxio.Constants;
import alluxio.master.metastore.ReadOption;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.logging.SamplingLogger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 */
@ThreadSafe
public abstract class BaseCache implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Cache.class);
  protected final int mMaxSize;
  private final int mHighWaterMark;
  private final int mLowWaterMark;
  protected final int mEvictBatchSize;
  private final String mName;
  @VisibleForTesting
  protected final DataMap<Object, Cache.Entry> mMap;
  // TODO(andrew): Support using multiple threads to speed up backing store writes.
  // Thread for performing eviction to the backing store.
  @VisibleForTesting
  protected final EvictionThread mEvictionThread;
  protected final StatsCounter mStatsCounter;
  protected Cache mCache;
  protected volatile boolean mSkipCache = false;

  /**
   * @param conf cache configuration
   * @param name a name for the cache
   * @param evictionsKey the cache evictions metric key
   * @param hitsKey the cache hits metrics key
   * @param loadTimesKey the load times metrics key
   * @param missesKey the misses metrics key
   * @param sizeKey the size metrics key
   * @param cache the cache
   */
  public BaseCache(CacheConfiguration conf, String name, MetricKey evictionsKey, MetricKey hitsKey,
                   MetricKey loadTimesKey, MetricKey missesKey, MetricKey sizeKey, Cache cache) {
    mMaxSize = conf.getMaxSize();
    mHighWaterMark = conf.getHighWaterMark();
    mLowWaterMark = conf.getLowWaterMark();
    mEvictBatchSize = conf.getEvictBatchSize();
    mName = name;
    mMap = getDataMap();
    mEvictionThread = new EvictionThread();
    mEvictionThread.setDaemon(true);
    // The eviction thread is started lazily when we first reach the high water mark.
    mStatsCounter = new StatsCounter();
    mCache = cache;
    MetricsSystem.registerGaugeIfAbsent(evictionsKey.getName(), mStatsCounter.mEvictionCount::get);
    MetricsSystem.registerGaugeIfAbsent(hitsKey.getName(), mStatsCounter.mHitCount::get);
    MetricsSystem.registerGaugeIfAbsent(loadTimesKey.getName(), mStatsCounter.mTotalLoadTime::get);
    MetricsSystem.registerGaugeIfAbsent(missesKey.getName(), mStatsCounter.mMissCount::get);
    MetricsSystem.registerGaugeIfAbsent(sizeKey.getName(), mMap::size);
  }

  public void setSkipCache(boolean skipCache) {
    mSkipCache = skipCache;
  }

  abstract DataMap<Object, Cache.Entry> getDataMap();

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
  public Optional<Object> get(Object key, ReadOption option) {
    if (option.shouldSkipCache() || cacheIsFull() || mSkipCache) {
      return getSkipCache(key);
    }
    Cache.Entry result = mMap.compute(key, (k, entry) -> {
      if (entry != null) {
        mStatsCounter.recordHit();
        entry.mReferenced = true;
        return entry;
      }
      mStatsCounter.recordMiss();
      final Stopwatch stopwatch = Stopwatch.createStarted();
      Optional<Object> value = mCache.load(key);
      mStatsCounter.recordLoad(stopwatch.elapsed(TimeUnit.NANOSECONDS));
      if (value.isPresent()) {
        mCache.onCacheUpdate(key, value.get());
        Cache.Entry newEntry = new Cache.Entry(key, value.get());
        newEntry.mDirty = false;
        return newEntry;
      }
      return null;
    });
    if (result == null || result.mValue == null) {
      return Optional.empty();
    }
    wakeEvictionThreadIfNecessary();
    return Optional.of(result.mValue);
  }

  /**
   * @param key the key to get the value for
   * @return the result of {@link #get(Object, ReadOption)} with default option
   */
  public Optional<Object> get(Object key) {
    return get(key, ReadOption.defaults());
  }

  /**
   * Retrieves a value from the cache if already cached, otherwise, loads from the backing store
   * without caching the value. Eviction is not triggered.
   *
   * @param key the key to get the value for
   * @return the value, or empty if the key doesn't exist in the cache or in the backing store
   */
  protected Optional<Object> getSkipCache(Object key) {
    Cache.Entry entry = mMap.get(key);
    if (entry == null) {
      mStatsCounter.recordMiss();
      final Stopwatch stopwatch = Stopwatch.createStarted();
      final Optional<Object> result = mCache.load(key);
      mStatsCounter.recordLoad(stopwatch.elapsed(TimeUnit.NANOSECONDS));
      return result;
    }
    mStatsCounter.recordHit();
    return Optional.ofNullable(entry.mValue);
  }

  /**
   * Writes a key/value pair to the cache.
   *
   * @param key the key
   * @param value the value
   */
  public void put(Object key, Object value) {
    mMap.compute(key, (k, entry) -> {
      mCache.onPut(key, value);
      if (entry == null && cacheIsFull()) {
        mCache.writeToBackingStore(key, value);
        return null;
      }
      if (entry == null || entry.mValue == null) {
        mCache.onCacheUpdate(key, value);
        return new Cache.Entry(key, value);
      }
      entry.mValue = value;
      entry.mReferenced = true;
      entry.mDirty = true;
      return entry;
    });
    wakeEvictionThreadIfNecessary();
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
  public void remove(Object key) {
    // Set the entry so that it will be removed from the backing store when it is encountered by
    // the eviction thread.
    mMap.compute(key, (k, entry) -> {
      mCache.onRemove(key);
      if (entry == null && cacheIsFull()) {
        mCache.removeFromBackingStore(k);
        return null;
      }
      mCache.onCacheUpdate(key, null);
      if (entry == null) {
        entry = new Cache.Entry(key, null);
      } else {
        entry.mValue = null;
      }
      entry.mReferenced = false;
      entry.mDirty = true;
      return entry;
    });
    wakeEvictionThreadIfNecessary();
  }

  /**
   * Flushes all data to the backing store.note.
   * that flush only write dirty data to backingStore,can not remove any cached data
   */
  public void flush() throws InterruptedException {
    List<Cache.Entry> toFlush = new ArrayList<>(mEvictBatchSize);
    Iterator<Cache.Entry> it = getEvictIterator(null);
    while (it.hasNext()) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      while (toFlush.size() < mEvictBatchSize && it.hasNext()) {
        Cache.Entry candidate = it.next();
        if (candidate.mDirty) {
          toFlush.add(candidate);
        }
      }
      mCache.flushEntries(toFlush);
      toFlush.clear();
    }
  }

  /**
   * Clears all entries from the map. This is not threadsafe, and requires external synchronization
   * to prevent concurrent modifications to the cache.
   */
  public void clear() {
    mMap.forEach((key, value) -> {
      mCache.onCacheUpdate(key, value.mValue);
      mCache.onRemove(key);
    });
    mMap.clear();
  }

  private boolean overHighWaterMark() {
    return mMap.size() >= mHighWaterMark;
  }

  protected boolean cacheIsFull() {
    return mMap.size() >= mMaxSize;
  }

  protected void wakeEvictionThreadIfNecessary() {
    if (mEvictionThread.mIsSleeping && mMap.size() >= mHighWaterMark) {
      kickEvictionThread();
    }
  }

  private void kickEvictionThread() {
    synchronized (mEvictionThread) {
      if (mEvictionThread.getState() == State.NEW) {
        mEvictionThread.start();
      }
      mEvictionThread.notifyAll();
    }
  }

  @Override
  public void close() {
    mEvictionThread.interrupt();
    try {
      mEvictionThread.join(10L * Constants.SECOND_MS);
      if (mEvictionThread.isAlive()) {
        LOG.warn("Failed to stop eviction thread");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  protected Iterator<Cache.Entry> getEvictIterator(Iterator<Cache.Entry> currentIterator) {
    if (currentIterator != null && currentIterator.hasNext()) {
      return currentIterator;
    } else {
      return getEvictIterator();
    }
  }

  protected Iterator<Cache.Entry> getEvictIterator() {
    return mMap.values().iterator();
  }

  @VisibleForTesting
  class EvictionThread extends Thread {
    @VisibleForTesting
    volatile boolean mIsSleeping = true;

    // Populated with #fillBatch, cleared with #evictBatch. We keep it around so that we don't need
    // to keep re-allocating the list.
    private final List<Cache.Entry> mEvictionCandidates = new ArrayList<>(mEvictBatchSize);
    private final List<Cache.Entry> mDirtyEvictionCandidates = new ArrayList<>(mEvictBatchSize);
    private final Logger mCacheFullLogger = new SamplingLogger(LOG, 10L * Constants.SECOND_MS);

    private Iterator<Cache.Entry> mEvictionHead = Collections.emptyIterator();

    private EvictionThread() {
      super(mName + "-eviction-thread");
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        // Wait for the cache to get over the high water mark.
        while (!overHighWaterMark()) {
          synchronized (mEvictionThread) {
            if (!overHighWaterMark()) {
              try {
                mIsSleeping = true;
                mEvictionThread.wait();
                mIsSleeping = false;
              } catch (InterruptedException e) {
                return;
              }
            }
          }
        }
        if (cacheIsFull()) {
          mCacheFullLogger.warn(
              "Metastore {} cache is full. Consider increasing the cache size or lowering the "
                  + "high water mark. size:{} lowWaterMark:{} highWaterMark:{} maxSize:{}",
              mName, mMap.size(), mLowWaterMark, mHighWaterMark, mMaxSize);
        }
        evictToLowWaterMark();
      }
    }

    private void evictToLowWaterMark() {
      long evictionStart = System.nanoTime();
      int toEvict = mMap.size() - mLowWaterMark;
      int evictionCount = 0;
      while (evictionCount < toEvict) {
//                if (!mEvictionHead.hasNext()) {
//                    mEvictionHead = mMap.values().iterator();
//                }
        mEvictionHead = getEvictIterator(mEvictionHead);
        fillBatch(toEvict - evictionCount);
        evictionCount += evictBatch();
      }
      if (evictionCount > 0) {
        mStatsCounter.recordEvictions(evictionCount);
        LOG.debug("{}: Evicted {} entries in {}ms", mName, evictionCount,
            (System.nanoTime() - evictionStart) / Constants.MS_NANO);
      }
    }

    /**
     * Attempts to fill mEvictionCandidates with up to min(count, mEvictBatchSize) candidates for
     * eviction.
     *
     * @param count maximum number of entries to store in the batch
     */
    private void fillBatch(int count) {
      int targetSize = Math.min(count, mEvictBatchSize);
      while (mEvictionCandidates.size() < targetSize && mEvictionHead.hasNext()) {
        Cache.Entry candidate = mEvictionHead.next();
        if (candidate.mReferenced) {
          candidate.mReferenced = false;
          continue;
        }
        mEvictionCandidates.add(candidate);
        if (candidate.mDirty) {
          mDirtyEvictionCandidates.add(candidate);
        }
      }
    }

    /**
     * Attempts to evict all entries in mEvictionCandidates.
     *
     * @return the number of candidates actually evicted
     */
    private int evictBatch() {
      int evicted = 0;
      if (mEvictionCandidates.isEmpty()) {
        return evicted;
      }
      //System.err.println("dirtyLeng:"+mDirtyEvictionCandidates.size() +
      // " evictLength:"+mEvictionCandidates.size());
      mCache.flushEntries(mDirtyEvictionCandidates);
      for (Cache.Entry entry : mEvictionCandidates) {
        if (evictIfClean(entry)) {
          evicted++;
        }
      }
      mEvictionCandidates.clear();
      mDirtyEvictionCandidates.clear();
      return evicted;
    }

    /**
     * @param entry the entry to try to evict
     * @return whether the entry was successfully evicted
     */
    private boolean evictIfClean(Cache.Entry entry) {
      return null == mMap.updateIfPresent(entry.mKey, (key, e) -> {
        if (entry.mDirty) {
          //System.err.println("key_is_not_null:"+key);
          return entry; // entry must have been written since we evicted.
        }
        mCache.onCacheRemove(entry.mKey);
        //System.err.println("key_null:"+key);
        return null;
      });
    }
  }

  /**
   * Implementation of StatsCounter similar to the one in
   * {@link com.google.common.cache.AbstractCache}.
   */
  protected static final class StatsCounter {
    private final AtomicLong mHitCount;
    private final AtomicLong mMissCount;
    private final AtomicLong mTotalLoadTime;
    private final AtomicLong mEvictionCount;

    public StatsCounter() {
      mHitCount = new AtomicLong();
      mMissCount = new AtomicLong();
      mTotalLoadTime = new AtomicLong();
      mEvictionCount = new AtomicLong();
    }

    public void recordHit() {
      mHitCount.getAndIncrement();
    }

    public void recordMiss() {
      mMissCount.getAndIncrement();
    }

    public void recordLoad(long loadTime) {
      mTotalLoadTime.getAndAdd(loadTime);
    }

    public void recordEvictions(long evictionCount) {
      mEvictionCount.getAndAdd(evictionCount);
    }
  }
}
