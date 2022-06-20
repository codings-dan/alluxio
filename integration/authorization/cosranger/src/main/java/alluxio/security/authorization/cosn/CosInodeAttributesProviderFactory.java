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

package alluxio.security.authorization.cosn;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.TxPropertyKey;
import alluxio.master.file.InodeAttributesProvider;
import alluxio.master.file.InodeAttributesProviderFactory;
import alluxio.security.authorization.AuthorizationPluginConstants;
import alluxio.underfs.UnderFileSystemConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * An {@link InodeAttributesProvider} that allows Alluxio to retrieve inode attributes from
 * COS {@link CosInodeAttributesProvider}.
 */
public final class CosInodeAttributesProviderFactory implements InodeAttributesProviderFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(CosInodeAttributesProviderFactory.class);
  private URI mUri;

  @Override
  public InodeAttributesProvider create(String path, @Nullable UnderFileSystemConfiguration conf) {
    try {
      mUri = new URI(path);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return new CosInodeAttributesProvider(mUri);
  }

  @Override
  public boolean supportsPath(String path, @Nullable UnderFileSystemConfiguration conf) {
    LOG.info("CosInodeAttributesProviderFactory.supportsPath for path: {}", path);
    boolean isCosn = ServerConfiguration.getList(TxPropertyKey.UNDERFS_COS_PREFIXES).stream()
        .anyMatch(path::startsWith);
    return conf != null && AuthorizationPluginConstants.AUTH_VERSION
        .equalsIgnoreCase(conf.getString(TxPropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME))
        && isCosn;
  }
}
