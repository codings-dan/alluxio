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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.master.metastore.ReadOption;
import alluxio.metrics.MetricKey;

import com.google.common.base.Stopwatch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class MultiShardLRUCache extends BaseCache {
  public MultiShardLRUCache(CacheConfiguration conf, String name, MetricKey evictionsKey,
                            MetricKey hitsKey, MetricKey loadTimesKey, MetricKey missesKey,
                            MetricKey sizeKey, Cache cache) {
    super(conf, name, evictionsKey, hitsKey, loadTimesKey, missesKey, sizeKey, cache);
  }

  @Override
  DataMap<Object, Cache.Entry> getDataMap() {
    int shardNum = Runtime.getRuntime().availableProcessors();
    int shardEvictBatchSize = mEvictBatchSize / shardNum;
    if (shardEvictBatchSize == 0) {
      shardEvictBatchSize = 1;
    }
    return new MultiShardLRUDataMap<>(mMaxSize,
      Runtime.getRuntime().availableProcessors(), shardEvictBatchSize);
    //return new MultiShardLRUDataMap<>(mMaxSize,1,shardEvictBatchSize);
  }

  @Override
  @SuppressFBWarnings
  public Optional<Object> get(Object key, ReadOption option) {
    if (option.shouldSkipCache() || cacheIsFull() || mSkipCache) {
      return getSkipCache(key);
    }
    MultiShardLRUDataMap<Object, Cache.Entry> datamap =
        (MultiShardLRUDataMap<Object, Cache.Entry>) mMap;
    Cache.Entry result = datamap.getAndUpsert(key, (k, entry) -> {
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

  @Override
  public void put(Object key, Object value) {
    mMap.getAndUpsert(key, (k, entry) -> {
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

  @Override
  public void remove(Object key) {
    // Set the entry so that it will be removed from the backing store when it is encountered by
    // the eviction thread.
    mMap.upsertUnLRU(key, (k, entry) -> {
      mCache.onRemove(key);
      if (entry == null && cacheIsFull()) {
        mCache.removeFromBackingStore(key);
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

  @Override
  protected Iterator<Cache.Entry> getEvictIterator(Iterator<Cache.Entry> currentIterator) {
    return getEvictIterator();
  }

  @Override
  @SuppressFBWarnings
  protected Iterator<Cache.Entry> getEvictIterator() {
    return ((MultiShardLRUDataMap) mMap).evictIterator();
  }

  @Override
  public void flush() throws InterruptedException {
    List<Cache.Entry> toFlush = new ArrayList<>(mEvictBatchSize);
    Iterator<Cache.Entry> it = mMap.iterator();
    while (it.hasNext()) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      while (toFlush.size() < mEvictBatchSize && it.hasNext()) {
        Cache.Entry candidate = it.next();
        //System.err.println("key:"+candidate.mKey+" value:"+candidate.mValue);
        if (candidate.mDirty) {
          toFlush.add(candidate);
        }
      }
      mCache.flushEntries(toFlush);
      toFlush.clear();
    }
  }
}
