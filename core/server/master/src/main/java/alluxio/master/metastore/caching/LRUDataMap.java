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

import javax.ws.rs.NotSupportedException;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class LRUDataMap<K, V extends Cache.Entry> extends DataMap<K, V> {
  private Node mFirst;
  private Node mLast;
  private ReentrantReadWriteLock mReadWriteLock = new ReentrantReadWriteLock();
  private Map<K, Node<K, V>> mMap;
  private int mMaxSize;

  public LRUDataMap(int maxSize) {
    mInternalMap = null;
    mMaxSize = maxSize;
    mMap = new ConcurrentHashMap<>(mMaxSize);
  }

  @Override
  public void setInternalMap(Map internalMap) {
    throw new NotSupportedException("LRUCache not surport setInternalMap");
  }

  @Override
  public V put(K key, V value) {
    try {
      mReadWriteLock.writeLock().lock();
      Node node = getNode(key);
      putUnlock(key, value, null);
      return null;
    } finally {
      mReadWriteLock.writeLock().unlock();
    }
  }

  public V putUnlock(K key, V value, Node node) {
    if (node == null) {
      node = new Node();
      node.mKey = key;
    }
    node.mValue = value;
    moveToFirst(node);
    mMap.put(key, node);
    return null;
  }

  public V putToRemoveUnlock(K key, V value, Node node) {
    if (node == null) {
      node = new Node();
      node.mKey = key;
    }
    node.mValue = value;
    moveToLast(node);
    mMap.put(key, node);
    return null;
  }

  @Override
  public V get(Object key) {
    try {
      mReadWriteLock.writeLock().lock();
      Node<K, V> node = getNode(key);
      if (node == null) { return null; }
      moveToFirst(node);
      return node.mValue;
    } finally {
      mReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public V getAndUpsert(K key, BiFunction<? super K, ? super V, ? extends V> mappinfgFunction) {
    Objects.requireNonNull(mappinfgFunction);
    try {
      mReadWriteLock.writeLock().lock();
      LRUDataMap.Node oldValue = mMap.get(key);
      V newValue = mappinfgFunction.apply(key, oldValue == null ? null : (V) oldValue.mValue);
      if (newValue == null) {
        // delete mapping
        if (oldValue != null || mMap.containsKey(key)) {
          // something to remove
          removeUnlock(key, oldValue);
          return null;
        } else {
            // nothing to do. Leave things as they were.
          return null;
        }
      } else {
        if (oldValue != null) {
          if (newValue == oldValue.mValue) {
            moveToFirst(oldValue);
          } else {
            putUnlock(key, newValue, oldValue);
          }
        } else {
          putUnlock(key, newValue, null);
        }
        return newValue;
      }
    } finally {
      mReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void upsertUnLRU(K key, BiFunction<? super K, ? super V, ? extends V> mappinfgFunction) {
    Objects.requireNonNull(mappinfgFunction);
    try {
      mReadWriteLock.writeLock().lock();
      LRUDataMap.Node oldValue = mMap.get(key);
      V newValue = mappinfgFunction.apply(key, oldValue == null ? null : (V) oldValue.mValue);
      if (newValue == null) {
        // delete mapping
        if (oldValue != null || mMap.containsKey(key)) {
          // something to remove
          removeUnlock(key, oldValue);
          return;
        } else {
          // nothing to do. Leave things as they were.
          return;
        }
      } else {
        if (oldValue != null) {
          if (newValue == oldValue.mValue) {
            moveToLast(oldValue);
          } else {
            putToRemoveUnlock(key, newValue, oldValue);
          }
        } else {
          putToRemoveUnlock(key, newValue, null);
        }
        return;
      }
    } finally {
      mReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public V remove(Object key) {
    try {
      mReadWriteLock.writeLock().lock();
      removeUnlock(key, null);
      return null;
    } finally {
      mReadWriteLock.writeLock().unlock();
    }
  }

  public V removeUnlock(Object key, Node node) {
    if (node == null) {
      node = getNode(key);
    }
    removeNode(node);
    mMap.remove(key);
    return null;
  }

  private Node<K, V> getNode(Object key) {
    return mMap.get(key);
  }

  private void removeNode(Node node) {
    if (node != null) {
      if (node.mPre != null) { node.mPre.mNext = node.mNext; }
      if (node.mNext != null) { node.mNext.mPre = node.mPre; }
      if (node == mFirst) { mFirst = node.mNext; }
      if (node == mLast) { mLast = node.mPre; }
    }
  }

  public int size() {
    return mMap.size();
  }

  private void moveToFirst(Node node) {
    if (node == mFirst) { return; }
    if (node.mPre != null) { node.mPre.mNext = node.mNext; }
    if (node.mNext != null) { node.mNext.mPre = node.mPre; }
    if (node == mLast) { mLast = mLast.mPre; }

    if (mFirst == null || mLast == null) {
      mFirst = mLast = node;
      return;
    }
    node.mNext = mFirst;
    mFirst.mPre = node;
    mFirst = node;
    node.mPre = null;
  }

  private void moveToLast(Node node) {
    if (node == mLast) { return; }
    if (node.mPre != null) { node.mPre.mNext = node.mNext; }
    if (node.mNext != null) { node.mNext.mPre = node.mPre; }
    if (node == mFirst) { mFirst = mFirst.mNext; }

    if (mFirst == null || mLast == null) {
      mFirst = mLast = node;
      return;
    }

    node.mPre = mLast;
    mLast.mNext = node;
    mLast = node;
    node.mNext = null;
  }

  public Queue<V> pullBatch(int num) {
    int leftBatchSize = num;
    try {
      mReadWriteLock.writeLock().lock();
      Queue<V> queue = new LinkedList<>();
      Node current = mLast;
      while (leftBatchSize > 0 && current != null) {
        //System.err.println("pull key:"+current.mKey+" mLast pret:"+current.mPre.mKey
        // + " equals:"+(current == current.mPre));
        queue.offer((V) current.mValue);
        leftBatchSize--;
        current = current.mPre;
      }
      return queue;
    } finally {
      mReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void forEach(BiConsumer<? super K, ? super V> action) {
    Objects.requireNonNull(action);
    for (Map.Entry<K, Node<K, V>> entry : mMap.entrySet()) {
      K k;
      V v;
      try {
        k = entry.getKey();
        v = entry.getValue().mValue;
      } catch (IllegalStateException ise) {
        // this usually means the entry is no longer in the map.
        throw new ConcurrentModificationException(ise);
      }
      action.accept(k, v);
    }
  }

  class Node<K, V> {
    public Node mPre;
    public Node mNext;
    public K mKey;
    public V mValue;
  }

  @Override
  public void clear() {
    try {
      mReadWriteLock.writeLock().lock();
      mInternalMap = null;
      mMap.clear();
      mFirst = null;
      mLast = null;
      mMap = new ConcurrentHashMap<>(mMaxSize);
    } finally {
      mReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public V updateIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    Objects.requireNonNull(remappingFunction);
    try {
      mReadWriteLock.writeLock().lock();
      LRUDataMap.Node oldValue = mMap.get(key);
      if (oldValue != null) {
        V newValue = remappingFunction.apply(key, (V) oldValue.mValue);
        if (newValue != null) {
          oldValue.mValue = newValue;
          return newValue;
        } else {
          removeUnlock(key, oldValue);
          return null;
        }
      } else {
        return null;
      }
    } finally {
      mReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public Iterator<V> iterator() {
    return new Iterator<V>() {
      private Iterator<Node<K, V>> mInternalIterator = mMap.values().iterator();

      @Override
      public boolean hasNext() {
          return mInternalIterator.hasNext();
      }

      @Override
      public V next() {
          return mInternalIterator.next().mValue;
      }
    };
  }
}
