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

import com.google.common.base.Equivalence;

import java.util.Iterator;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class MultiShardLRUDataMap<K, V extends Cache.Entry> extends DataMap<K, V> {
  private Segment[] mSegments;
  private int mSegmentShift;
  private int mSegmentMask;
  private Equivalence mEquivalence;
  private int mEvictBatch = 10;

  public MultiShardLRUDataMap(int maxSize, int concurrentLevel, int evictBatch) {
    int segmentShift = 0;
    int segmentCount = 1;
    while (segmentCount < concurrentLevel) {
      ++segmentShift;
      segmentCount <<= 1;
    }
    mSegmentShift = 32 - segmentShift;
    mSegmentMask = segmentCount - 1;

    mSegments = new Segment[segmentCount];
    int segmentCapacity = maxSize / segmentCount;
    if (segmentCapacity * segmentCount < maxSize) {
      ++segmentCapacity;
    }
    int segmentSize = 1;
    while (segmentSize < segmentCapacity) {
      segmentSize <<= 1;
    }
    for (int i = 0; i < mSegments.length; i++) {
      mSegments[i] = new Segment<>(new LRUDataMap(segmentSize));
    }
    mEquivalence = Equivalence.equals();
    mEvictBatch = evictBatch;
  }

  @Override
  public int size() {
    int size = 0;
    for (int i = 0; i < mSegments.length; i++) {
      size += mSegments[i].size();
    }
    return size;
  }

  @Override
  public V get(Object key) {
    if (key == null) {
      return null;
    }
    int hash = hash(key);
    return (V) segmentFor(hash).getDataMap().get(key);
  }

  @Override
  public V getAndUpsert(K key, BiFunction<? super K, ? super V, ? extends V> mappinfgFunction) {
    if (key == null) {
      return null;
    }
    int hash = hash(key);
    return (V) segmentFor(hash).getDataMap().getAndUpsert(key, mappinfgFunction);
  }

  @Override
  public void upsertUnLRU(K key, BiFunction<? super K, ? super V, ? extends V> mappinfgFunction) {
    if (key == null) {
      return;
    }
    int hash = hash(key);
    segmentFor(hash).getDataMap().upsertUnLRU(key, mappinfgFunction);
  }

  @Override
  public V updateIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    if (key == null) {
      return null;
    }
    int hash = hash(key);
    return (V) segmentFor(hash).getDataMap().updateIfPresent(key, remappingFunction);
  }

  @Override
  public V put(K key, V value) {
    if (key != null) {
      int hash = hash(key);
      segmentFor(hash).getDataMap().put(key, value);
    }
    return null;
  }

  @Override
  @SuppressFBWarnings
  public V remove(Object key) {
    if (key == null) {
      int hash = hash(null);
      segmentFor(hash).getDataMap().remove(null);
    }
    return null;
  }

  private Segment<K, V> segmentFor(int hash) {
    int index = (hash >>> mSegmentShift) & mSegmentMask;
    return mSegments[index];
  }

  private int hash(Object key) {
    int h = mEquivalence.hash(key);
    return rehash(h);
  }

  private int rehash(int h) {
    h += (h << 15) ^ 0xffffcd7d;
    h ^= (h >>> 10);
    h += (h << 3);
    h ^= (h >>> 6);
    h += (h << 2) + (h << 14);
    return h ^ (h >>> 16);
  }

  @Override
  public Iterator<V> iterator() {
    return new Iterator<V>() {
      private int mCurrentSegment = 0;
      private Iterator<V> mCurrentSegmentIterator = mSegments[mCurrentSegment].mDataMap.iterator();

      private boolean nextSegment() {
        if (mCurrentSegment >= mSegments.length - 1) {
          return false;
        } else {
          mCurrentSegment++;
          mCurrentSegmentIterator = mSegments[mCurrentSegment].mDataMap.iterator();
          return true;
        }
      }

      @Override
      public boolean hasNext() {
          if (mCurrentSegmentIterator.hasNext()) {
            return true;
          } else {
            while (nextSegment()) {
              if (mCurrentSegmentIterator.hasNext()) {
                return true;
              }
            }
          }
          return false;
      }

      @Override
      public V next() {
          return mCurrentSegmentIterator.next();
      }
    };
  }

  public Iterator<V> evictIterator() {
    return new Iterator<V>() {
      private int mEvictSegment = 0;
      private Queue<V> mEvictQueue = mSegments[mEvictSegment].getDataMap().pullBatch(mEvictBatch);

      private boolean nextBatch() {
        while (mEvictSegment < mSegments.length - 1) {
          int nextEvictSegment = mEvictSegment + 1;
          Queue currentEvictQueue = mSegments[nextEvictSegment].getDataMap().pullBatch(mEvictBatch);
          mEvictSegment = nextEvictSegment;
          mEvictQueue = currentEvictQueue;
          if (mEvictQueue.size() > 0) {
            //System.err.println("size:"+evictQueue.size());
            return true;
          }
        }
        return false;
      }

      @Override
      public boolean hasNext() {
        if (mEvictQueue.size() > 0) {
          return true;
        } else {
          return nextBatch();
        }
      }

      @Override
      @SuppressFBWarnings
      public V next() {
        V ret = mEvictQueue.poll();
        return ret;
      }
    };
  }

  @Override
  public void forEach(BiConsumer<? super K, ? super V> action) {
    for (int i = 0; i < mSegments.length; i++) {
      mSegments[i].mDataMap.forEach(action);
    }
  }

  @Override
  public void clear() {
    for (int i = 0; i < mSegments.length; i++) {
      mSegments[i].mDataMap.clear();
    }
  }

  static class Segment<K, V> {
    private LRUDataMap mDataMap;

    public Segment(LRUDataMap dataMap) {
      mDataMap = dataMap;
    }

    private LRUDataMap getDataMap() {
      return mDataMap;
    }

    public int size() {
      return mDataMap.size();
    }
  }
}
