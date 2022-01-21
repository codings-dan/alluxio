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

package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.TxPropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.WritePType;
import alluxio.master.file.uritranslator.MergeAuthorityToPathUriTranslator;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.List;

public class ShimFsMergeAuthorityAlluxioIntegrationTest {

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setNumWorkers(0)
          .setIncludeProxy(false)
          .setProperty(TxPropertyKey.MASTER_URI_TRANSLATOR_IMPL,
              MergeAuthorityToPathUriTranslator.class.getName())
          .build();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  private FileSystem mFileSystem = null;
  private FileSystem mShimFileSystem = null;

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    // Create a BaseFileSystem that has URI validation disabled.
    // Hadoop fs implementation, {@link ShimFileSystem} will also set this flag.
    mShimFileSystem = FileSystem.Factory.create(FileSystemContext
        .create(ClientContext.create(ServerConfiguration.global()).setUriValidationEnabled(false)));
  }

  @Test
  public void listFiles() throws Exception {
    int testFileCount = 10;
    String alluxioRoot = "/bucket/rootdir";
    CreateDirectoryPOptions createDirectoryPOptions =
        CreateDirectoryPOptions.newBuilder()
            .setWriteType(WritePType.MUST_CACHE)
            .setRecursive(true)
            .build();
    mShimFileSystem.createDirectory(new AlluxioURI(alluxioRoot), createDirectoryPOptions);
    for (int i = 0; i < testFileCount; i++) {
      AlluxioURI alluxioUri =
          new AlluxioURI(PathUtils.concatPath(alluxioRoot, Integer.toString(i)));
      mShimFileSystem.createDirectory(alluxioUri, createDirectoryPOptions);
    }

    // List files via shim-fs.
    List<URIStatus> shimStatusList = mShimFileSystem.listStatus(
        new AlluxioURI("s3a://bucket/rootdir"),
        ListStatusPOptions.newBuilder().setRecursive(true).build());
    Assert.assertEquals(testFileCount, shimStatusList.size());
    // List directories via Alluxio-fs.
    // Get foreign root status to get Alluxio path.
    URIStatus dirStatus = mShimFileSystem.getStatus(new AlluxioURI(alluxioRoot));
    List<URIStatus> statusList = mFileSystem.listStatus(new AlluxioURI(dirStatus.getPath()));
    Assert.assertEquals(testFileCount, statusList.size());
  }
}
