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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class DataMap<K, V extends Cache.Entry> {
  protected Map<K, V> mInternalMap;

  public void setInternalMap(Map<K, V> internalMap) {
    mInternalMap = internalMap;
  }

  public int size() {
    return mInternalMap.size();
  }

  public boolean isEmpty() {
    return mInternalMap.isEmpty();
  }

  public boolean containsKey(Object key) {
    return mInternalMap.containsKey(key);
  }

  public boolean containsValue(Object value) {
    throw new NotSupportedException("dataMap cannot support containsValue");
  }

  public V get(Object key) {
    return mInternalMap.get(key);
  }

  public V put(K key, V value) {
    return mInternalMap.put(key, value);
  }

  public V remove(Object key) {
    return mInternalMap.remove(key);
  }

  public void putAll(Map<? extends K, ? extends V> m) {
    throw new NotSupportedException("dataMap cannot support putAll");
  }

  public void clear() {
    mInternalMap.clear();
  }

  public Set<K> keySet() {
    return mInternalMap.keySet();
  }

  public Collection<V> values() {
    return mInternalMap.values();
  }

  public Set<Map.Entry<K, V>> entrySet() {
    return mInternalMap.entrySet();
  }

  public V getAndUpsert(K key, BiFunction<? super K, ? super V, ? extends V> mappinfgFunction) {
    throw new NotSupportedException("not support getAndUpsert");
  }

  public void upsertUnLRU(K key, BiFunction<? super K, ? super V, ? extends V> mappinfgFunction) {
    throw new NotSupportedException("not support upsertUnLRU");
  }

  public void forEach(BiConsumer<? super K, ? super V> action) {
    mInternalMap.forEach(action);
  }

  public V updateIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return mInternalMap.computeIfPresent(key, remappingFunction);
  }

  public Iterator<V> iterator() {
    return mInternalMap.values().iterator();
  }

  public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return mInternalMap.compute(key, remappingFunction);
  }
}
