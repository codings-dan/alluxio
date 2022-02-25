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

package alluxio.client.block.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;

import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Randomly distribute workload based on the worker capacities so bigger workers get more requests.
 * The randomness is based on the capacity instead of availability because in the long run,
 * all workers will be filled up and have availability close to 0.
 * We do not want the policy to degenerate to all workers having the same chance.
 */
@ThreadSafe
public class CapacityBaseRandomPolicy implements BlockLocationPolicy {

  /**
   * Constructs a new {@link CapacityBaseRandomPolicy}.
   *
   * @param conf Alluxio configuration
   */
  public CapacityBaseRandomPolicy(AlluxioConfiguration conf) {}

  @Nullable
  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    Iterable<BlockWorkerInfo> blockWorkerInfos = options.getBlockWorkerInfos();
    // The key is the floor of capacity range the worker cover
    // For example if worker1 has 100B and worker2 has 100B
    // The map will have {0L -> worker1, 100L -> worker2}
    TreeMap<Long, BlockWorkerInfo> capacityFloorMap = new TreeMap<>();
    AtomicLong totalCapacity = new AtomicLong(0L);
    blockWorkerInfos.forEach(workerInfo -> {
      if (workerInfo.getCapacityBytes() > 0) {
        long capacityRangeFloor = totalCapacity.getAndAdd(workerInfo.getCapacityBytes());
        capacityFloorMap.put(capacityRangeFloor, workerInfo);
      }
    });
    if (totalCapacity.get() == 0L) {
      return null;
    }
    long randomLong = randomInCapacity(totalCapacity.get());
    return capacityFloorMap.floorEntry(randomLong).getValue().getNetAddress();
  }

  @Override
  public boolean equals(Object o) {
    return this == o || o instanceof CapacityBaseRandomPolicy;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

  protected long randomInCapacity(long totalCapacity) {
    return ThreadLocalRandom.current().nextLong(totalCapacity);
  }
}
