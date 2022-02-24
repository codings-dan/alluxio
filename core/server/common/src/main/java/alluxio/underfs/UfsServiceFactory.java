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

package alluxio.underfs;

/**
 * A factory that creates UFS services.
 */
public interface UfsServiceFactory {
  /**
   * Creates a UFS service.
   *
   * @param path path to create UFS service for
   * @param ufsConf configuration for the UFS
   * @param serviceType type of the UFS service
   * @param <T> type of the UFS service
   * @return the UFS service created
   */
  <T extends UfsService> T createUfsService(String path, UnderFileSystemConfiguration ufsConf,
      Class<T> serviceType);
}
