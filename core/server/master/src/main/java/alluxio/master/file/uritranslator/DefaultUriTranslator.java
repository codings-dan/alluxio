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

package alluxio.master.file.uritranslator;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.MountTable;

/**
 * A default implementation of UriTranslator.
 */
public class DefaultUriTranslator implements UriTranslator {

  protected FileSystemMaster mMaster;
  protected MountTable mMountTable;
  protected InodeTree mInodeTree;

  public DefaultUriTranslator(FileSystemMaster master,
      MountTable mountTable, InodeTree inodeTree) {
    mMaster = master;
    mMountTable = mountTable;
    mInodeTree = inodeTree;
  }

  @Override
  public AlluxioURI translateUri(String uriStr) throws InvalidPathException {
    return new AlluxioURI(uriStr);
  }
}
