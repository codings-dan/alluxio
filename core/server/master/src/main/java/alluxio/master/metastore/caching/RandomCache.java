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

import alluxio.metrics.MetricKey;

import java.util.concurrent.ConcurrentHashMap;

public class RandomCache extends BaseCache {
  public RandomCache(CacheConfiguration conf, String name, MetricKey evictionsKey,
                     MetricKey hitsKey, MetricKey loadTimesKey, MetricKey missesKey,
                     MetricKey sizeKey, Cache cache) {
    super(conf, name, evictionsKey, hitsKey, loadTimesKey, missesKey, sizeKey, cache);
  }

  @Override
  DataMap<Object, Cache.Entry> getDataMap() {
    DataMap<Object, Cache.Entry> ret = new DataMap<>();
    ret.setInternalMap(new ConcurrentHashMap<>(mMaxSize));
    return ret;
  }
}
