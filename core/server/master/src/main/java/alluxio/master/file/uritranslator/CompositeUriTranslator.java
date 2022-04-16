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
import alluxio.conf.TxPropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.MountTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Auto mount if the resolve unsuccessfully.
 */
public class CompositeUriTranslator extends DefaultUriTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(
      CompositeUriTranslator.class);

  private final Map<String, UriTranslator> mUriTranslatorMap;

  public CompositeUriTranslator(FileSystemMaster master,
      MountTable mountTable, InodeTree inodeTree) {
    super(master, mountTable, inodeTree);
    mUriTranslatorMap = new HashMap<>();
    List<String> list =
        ServerConfiguration.getList(TxPropertyKey.MASTER_COMPOSITE_URI_TRANSLATOR_IMPL);
    for (String prefixImplStr : list) {
      String[] prefixImplPair = prefixImplStr.split("=");
      if (prefixImplPair.length == 2) {
        if (!mUriTranslatorMap.containsKey(prefixImplPair[0])) {
          UriTranslator uriTranslator =
              Factory.create(master, mountTable, inodeTree, prefixImplPair[1]);
          mUriTranslatorMap.put(prefixImplPair[0], uriTranslator);
        }
      }
    }
  }

  @Override
  public AlluxioURI translateUri(String uriStr) throws InvalidPathException {
    for (Map.Entry<String, UriTranslator> entry : mUriTranslatorMap.entrySet()) {
      if (uriStr.startsWith(entry.getKey())) {
        return entry.getValue().translateUri(uriStr);
      }
    }
    return super.translateUri(uriStr);
  }
}
