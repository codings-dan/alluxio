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

package alluxio.master.file.meta;

import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;

/**
 * Interface representing attributes of an {@link Inode}.
 */
public interface InodeAttributes {
  /** @return whether the inode is a directory */
  boolean isDirectory();

  /** @return the file name */
  String getName();

  /** @return the user name */
  String getOwner();

  /** @return the group name */
  String getGroup();

  /**
   * @return the ACL
   */
  AccessControlList getACL();

  /**
   * @return the default ACL
   */
  DefaultAccessControlList getDefaultACL() throws UnsupportedOperationException;

  /**
   * @return the permission
   */
  short getMode();

  /** @return the modification time */
  long getLastModificationTimeMs();
}
