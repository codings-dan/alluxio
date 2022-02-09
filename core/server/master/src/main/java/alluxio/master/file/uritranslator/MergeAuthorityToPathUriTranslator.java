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
import alluxio.Constants;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.MountTable;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Auto mount if the resolve unsuccessfully.
 */
public class MergeAuthorityToPathUriTranslator implements UriTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(
      MergeAuthorityToPathUriTranslator.class);

  public MergeAuthorityToPathUriTranslator(FileSystemMaster master,
      MountTable mountTable, InodeTree inodeTree) {
  }

  @Override
  public AlluxioURI translateUri(String uriStr) throws InvalidPathException {
    AlluxioURI uri = new AlluxioURI(uriStr);
    // Scheme-less URIs are regarded as Alluxio URI.
    if (uri.getScheme() == null || uri.getScheme().equals(Constants.SCHEME)) {
      return uri;
    }

    URI uriFromClient = URI.create(uriStr);
    String authority = uriFromClient.getAuthority();
    if (!Strings.isNullOrEmpty(authority)) {
      return new AlluxioURI("/" + authority + uriFromClient.getPath());
    }
    return uri;
  }
}
