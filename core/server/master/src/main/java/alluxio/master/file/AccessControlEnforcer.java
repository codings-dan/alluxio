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

import alluxio.exception.AccessControlException;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.InodeAttributes;
import alluxio.security.authorization.Mode;

import java.util.List;

/**
 * An interface allows implementation to override permission checking for an inode.
 */
public interface AccessControlEnforcer {
  /**
   * Checks permission on a file system object. It will return if check passed, or throw exception
   * if check failed.
   * @param user who requests access permission
   * @param groups the user belongs to
   * @param bits bits that capture the action {@link Mode.Bits} by user
   * @param path the path to check permission on
   * @param inodeList file info list of all the inodes retrieved by traversing the path
   * @param attributes list of inode attributes for each inode in the path
   * @param checkIsOwner indicates whether to check the user is the owner of the path
   * @throws AccessControlException if permission checking fails
   */
  void checkPermission(String user, List<String> groups, Mode.Bits bits,
      String path, List<InodeView> inodeList, List<InodeAttributes> attributes,
      boolean checkIsOwner) throws AccessControlException;
}
