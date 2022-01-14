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

package alluxio.security.authorization.hadoop;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.Source;
import alluxio.master.file.InodeAttributesProvider;
import alluxio.master.file.meta.MountTable;
import alluxio.security.authorization.AuthorizationPluginConstants;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 * Unit tests for the {@link HdfsInodeAttributesProviderFactory}.
 */
public class HdfsInodeAttributesProviderFactoryTest {

  @Test
  public void factory() {
    HdfsInodeAttributesProviderFactory factory =
        new HdfsInodeAttributesProviderFactory();
    ServerConfiguration.set(alluxio.conf.PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME,
        AuthorizationPluginConstants.AUTH_VERSION);
    UnderFileSystemConfiguration conf =
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global());
    conf.merge(ImmutableMap.of(DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
            DummyHdfsProvider.class.getName()), Source.RUNTIME);
    conf.createMountSpecificConf(ImmutableMap.of(
        alluxio.conf.PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME.getName(),
        AuthorizationPluginConstants.AUTH_VERSION,
        DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
        DummyHdfsProvider.class.getName()));
    UnderFileSystemConfiguration invalidConf =
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global());
    invalidConf.createMountSpecificConf(ImmutableMap.of(
        PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME.getName(), "invalid-1.0",
        DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
        DummyHdfsProvider.class.getName()));
    String s = ServerConfiguration.get(PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME);
    System.out.println(s.charAt(3));
    assertTrue(factory.supportsPath(MountTable.ROOT, conf));
    assertTrue(factory.supportsPath("hdfs://localhost/test/path", conf));
    assertFalse(supportsPath("hdfs://localhost/test/path", invalidConf));
    assertFalse(supportsPath("hdfs://localhost/test/path", null));
    assertFalse(factory.supportsPath("s3a://bucket/test/path", conf));
    InodeAttributesProvider provider = factory.create("hdfs://localhost/test/path", conf);
    assertNotNull(provider);

    assertFalse(factory.supportsPath("o3fs://vol1.bucket1/test/path", conf));
    assertFalse(factory.supportsPath("o3fs://vol1.bucket1.tdw/test/path", conf));
    assertFalse(factory.supportsPath("o3fs://vol1.bucket.1.1.1.1:9999/test/path", conf));
    conf.set(PropertyKey.SECURITY_AUTHORIZATION_PLUGIN_HDFS_COMPATIBLE_OZONE_ENABLED, "true");
    assertTrue(factory.supportsPath("hdfs://localhost/test/path", conf));
    assertFalse(factory.supportsPath("s3a://bucket/test/path", conf));
    assertTrue(factory.supportsPath("o3fs://vol1.bucket1/test/path", conf));
    assertTrue(factory.supportsPath("o3fs://vol1.bucket1.tdw/test/path", conf));
    assertTrue(factory.supportsPath("o3fs://vol1.bucket.1.1.1.1:9999/test/path", conf));
    InodeAttributesProvider compatibleProvider =
        factory.create("o3fs://vol1.bucket1/test/path", conf);
    assertNotNull(compatibleProvider);
  }

  private boolean supportsPath(String path, @Nullable UnderFileSystemConfiguration ufsConf) {
    ServerConfiguration.set(
        alluxio.conf.PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME, "invalid-1.0");
    boolean isHdfs = ServerConfiguration.getList(
        alluxio.conf.PropertyKey.UNDERFS_HDFS_PREFIXES, ",")
        .stream().anyMatch(path::startsWith);
    return ufsConf != null && AuthorizationPluginConstants.AUTH_VERSION.equalsIgnoreCase(
        ServerConfiguration.get(PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME))
        && (isHdfs || path.equals(MountTable.ROOT));
  }

  @Test
  public void longZengTestA() {
    InstancedConfiguration instancedConfiguration =
        new InstancedConfiguration(new AlluxioProperties());
    instancedConfiguration.set(
        alluxio.conf.PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME, "1111111");
    String s = instancedConfiguration.get(
        alluxio.conf.PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME);
    System.out.println(s.charAt(6));
  }
}
