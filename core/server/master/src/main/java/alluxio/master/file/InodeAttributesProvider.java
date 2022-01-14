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

package alluxio.master.file;

import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeAttributes;
import alluxio.underfs.UfsService;

/**
 * An interface to provide attributes override and permission checker for an inode.
 */
public interface InodeAttributesProvider extends UfsService {

  /**
   * Initializes the provider.
   */
  void start();

  /**
   * Stops the provider.
   */
  void stop();

  /**
   * Gets Attributes for the given {@link Inode}.
   * @param pathElements path to the Inode
   * @param attributes existing Inode attributes
   * @return the new set of attributes to use to override the default values
   */
  InodeAttributes getAttributes(String[] pathElements, InodeAttributes attributes);

  /**
   * Gets the {@link AccessControlEnforcer} to be used to override default permission enforcement.
   * @param defaultEnforcer default AccessControlEnforcer
   * @return The AccessControlEnforcer to use to override permission checking
   */
  default AccessControlEnforcer getExternalAccessControlEnforcer(
      AccessControlEnforcer defaultEnforcer) {
    return defaultEnforcer;
  }

  @Override
  default void close() {
    stop();
  }
}
