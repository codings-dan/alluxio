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

package alluxio.metrics;

import alluxio.grpc.MetricType;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Tencent defined Metric keys. This class provides a set of pre-defined Alluxio metric keys.
 */
@ThreadSafe
public class TxMetricKey {

  // Master metrics
  public static final MetricKey MASTER_TO_REMOVE_BLOCK_COUNT =
      new MetricKey.Builder("Master.ToRemoveBlockCount")
          .setDescription("Count of blocks to remove")
          .setMetricType(MetricType.GAUGE)
          .build();

  // Cluster metrics
  public static final MetricKey CLUSTER_METADATA_CACHE_REFRESH_COUNT =
      new MetricKey.Builder("Cluster.MetadataCacheRefreshCount")
          .setDescription("Total number of refreshing client metadata cache by all clients")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_METADATA_CACHE_DROP_COUNT =
      new MetricKey.Builder("Cluster.MetadataCacheDropCount")
          .setDescription("Total number of dropping client metadata cache by all clients")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_METADATA_CACHE_REFRESH_TIME =
      new MetricKey.Builder("Cluster.MetadataCacheRefreshTime")
          .setDescription("Total time of refreshing client metadata cache by all clients")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_REGISTER_CLIENTS =
      new MetricKey.Builder("Cluster.RegisterClients")
          .setDescription("Total number of client register in master inside the cluster")
          .setMetricType(MetricType.GAUGE)
          .build();

  // Client metrics
  public static final MetricKey CLIENT_METADATA_CACHE_REFRESH_COUNT =
      new MetricKey.Builder("Client.MetadataCacheRefreshCount")
          .setDescription("Number of refreshing client metadata cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey CLIENT_METADATA_CACHE_DROP_COUNT =
      new MetricKey.Builder("Client.MetadataCacheDropCount")
          .setDescription("Number of dropping client metadata cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey CLIENT_METADATA_CACHE_REFRESH_TIME =
      new MetricKey.Builder("Client.MetadataCacheRefreshTime")
          .setDescription("The total time of refreshing client metadata cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
}
