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

package org.apache.hadoop.hdfs.server.namenode;

import alluxio.master.file.meta.InodeView;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;

/**
 * A class extends HDFS {@link INodeDirectory} with Alluxio Inode information.
 */
public class AlluxioHdfsINodeDirectory extends INodeDirectory implements AlluxioHdfsINode {
  private final InodeView mInode;
  private final String mPath;

  /**
   * @param inode an Alluxio Inode
   * @param path a path corresponding to the Inode
   */
  public AlluxioHdfsINodeDirectory(InodeView inode, String path) {
    super(inode.getId(), inode.getName().getBytes(),
        PermissionStatus.createImmutable(
            inode.getOwner(), inode.getGroup(), new FsPermission(inode.getMode())),
        inode.getLastModificationTimeMs());
    mInode = inode;
    mPath = path;
  }

  @Override
  public String getFullPathName() {
    return mPath;
  }

  @Override
  public InodeView toAlluxioInode() {
    return mInode;
  }
}
