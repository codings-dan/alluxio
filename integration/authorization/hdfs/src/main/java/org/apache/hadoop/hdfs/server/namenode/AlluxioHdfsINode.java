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

import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeView;

import com.google.common.base.Preconditions;

/**
 * Interface for Alluxio version of HDFS {@link INode}.
 * Used by Alluxio permission checker to pass Alluxio inode information to HDFS authorization
 * plugins.
 * To derive from HDFS INode classes, Alluxio INode wrappers need to be in
 * org.apache.hadoop.hdfs.server.namenode package to gain access to the package-local constructors.
 * This allows Alluxio to pass inodes to HDFS authorization plugins without having to construct a
 * HDFS inode tree.
 */
public interface AlluxioHdfsINode {

  /**
   * @return the underlying Alluxio {@link Inode}
   */
  InodeView toAlluxioInode();

  /**
   * Creates a HDFS {@link INode} wrapper of an Alluxio {@link Inode}.
   *
   * @param alluxioInode the Alluxio {@link Inode}
   * @param path path of the inode
   * @return the HDFS {@link INode}
   */
  static INode create(InodeView alluxioInode, String path) {
    Preconditions.checkNotNull(alluxioInode, "alluxioInode");
    return alluxioInode.isDirectory() ? new AlluxioHdfsINodeDirectory(alluxioInode, path) :
        new AlluxioHdfsINodeFile(alluxioInode, path);
  }
}
