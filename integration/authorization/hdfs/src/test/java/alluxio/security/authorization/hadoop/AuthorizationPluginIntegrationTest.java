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

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.Source;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.StorageList;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.metrics.Metric;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.user.TestUserState;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Unit tests for {@link FileSystemMaster}.
 */

public final class AuthorizationPluginIntegrationTest {

  private static final String TEST_USER = "test";

  private MasterRegistry mRegistry;
  private JournalSystem mJournalSystem;
  private BlockMaster mBlockMaster;
  private DefaultFileSystemMaster mFileSystemMaster;
  private MetricsMaster mMetricsMaster;
  private long mWorkerId1;
  private long mWorkerId2;

  private String mJournalFolder;
  private String mUnderFS;
  private List<Metric> mMetrics;
  private ExecutorService mExecutorService;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER,
      new InstancedConfiguration(new AlluxioProperties()));

//  @Rule
//  public LoginUserRule mLoginUser = new LoginUserRule(TEST_USER);

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(new HashMap() {
    {
      put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "000");
      put(PropertyKey.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS, "20");
      put(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, "0");
      put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, AlluxioTestDirectory
          .createTemporaryDirectory("FileSystemMasterTest").getAbsolutePath());
      put(TxPropertyKey.SECURITY_AUTHORIZATION_PLUGINS_ENABLED, "true");
    }
  }, ServerConfiguration.global());

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_TTL_CHECK, HeartbeatContext.MASTER_LOST_FILES_DETECTION);

  // Set ttl interval to 0 so that there is no delay in detecting expired files.
  @ClassRule
  public static TtlIntervalRule sTtlIntervalRule = new TtlIntervalRule(0);

  @Before
  public void before() throws Exception {
    UnderFileSystemConfiguration conf =
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global());
    conf.merge(ImmutableMap.of(DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
        DummyHdfsProvider.class.getName()), Source.RUNTIME);
    GroupMappingServiceTestUtils.resetCache();
    mUnderFS = ServerConfiguration.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    mJournalFolder = mTestFolder.newFolder().getAbsolutePath();
    startServices();
  }

  @After
  public void after() throws Exception {
    stopServices();
  }

  @Test
  public void checkMasterFallbackPermissionPass() throws Exception {
    mFileSystemMaster.listStatus(new AlluxioURI("/"),
            ListStatusContext.defaults());
  }

  @Test
  public void checkMasterFallbackPermissionFail() throws Exception {
    String noAccessDir = "no_access";
    String ufsSubdir = PathUtils.concatPath(mUnderFS, noAccessDir);
    FileUtils.createDir(ufsSubdir);
    FileUtils.changeLocalFilePermission(ufsSubdir, "---------");
    // load metadata to Alluxio
    mFileSystemMaster.listStatus(new AlluxioURI("/" + noAccessDir),
            ListStatusContext.defaults());
    mThrown.expect(AccessControlException.class);
    try (Closeable r = new AuthenticatedUserRule("test_guest",
        new InstancedConfiguration(new AlluxioProperties())).toResource()) {
      mFileSystemMaster.listStatus(new AlluxioURI("/" + noAccessDir),
              ListStatusContext.defaults());
    }
  }

  @Test
  public void checkMasterFallbackParentPermissionFail() throws Exception {
    String noAccessDir = "no_access";
    String childDir = "no_access/child";
    String ufsNoAccessDir = PathUtils.concatPath(mUnderFS, noAccessDir);
    String ufsChildDir = PathUtils.concatPath(mUnderFS, childDir);
    FileUtils.createDir(ufsChildDir);
    FileUtils.changeLocalFilePermission(ufsNoAccessDir, "---------");
    // load metadata to Alluxio
    mFileSystemMaster.listStatus(new AlluxioURI("/" + noAccessDir),
            ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
                    .setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));
    mThrown.expect(AccessControlException.class);
    try (Closeable r = new AuthenticatedUserRule("test_guest",
        new InstancedConfiguration(new AlluxioProperties())).toResource()) {
      mFileSystemMaster.listStatus(new AlluxioURI("/" + childDir),
              ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
                      .setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));
    }
  }

  private void startServices() throws Exception {
    mRegistry = new MasterRegistry();
    mJournalSystem = JournalTestUtils.createJournalSystem(mJournalFolder);
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext(mJournalSystem,
            new TestUserState(TEST_USER, ServerConfiguration.global()));
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mRegistry.add(MetricsMaster.class, mMetricsMaster);
    mMetrics = Lists.newArrayList();
    mBlockMaster = new BlockMasterFactory().create(mRegistry, masterContext);
    mExecutorService =
        Executors.newFixedThreadPool(
            4, ThreadFactoryUtils.build("DefaultFileSystemMasterTest-%d", true));
    mFileSystemMaster = new DefaultFileSystemMaster(mBlockMaster, masterContext,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(FileSystemMaster.class, mFileSystemMaster);
    mJournalSystem.start();
    mJournalSystem.gainPrimacy();
    mRegistry.start(true);

    // set up workers
    mWorkerId1 = mBlockMaster.getWorkerId(
            new WorkerNetAddress().setHost("localhost")
                .setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId1, Arrays.asList("MEM", "SSD"),
            ImmutableMap.of("MEM", (long) Constants.MB, "SSD", (long) Constants.MB),
            ImmutableMap.of("MEM", (long) Constants.KB, "SSD", (long) Constants.KB),
            ImmutableMap.of(),
            new HashMap<String, StorageList>(), RegisterWorkerPOptions.getDefaultInstance());
    mWorkerId2 = mBlockMaster.getWorkerId(
            new WorkerNetAddress().setHost("remote").setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId2, Arrays.asList("MEM", "SSD"),
            ImmutableMap.of("MEM", (long) Constants.MB, "SSD", (long) Constants.MB),
            ImmutableMap.of("MEM", (long) Constants.KB, "SSD", (long) Constants.KB),
            ImmutableMap.of(),
            new HashMap<String, StorageList>(), RegisterWorkerPOptions.getDefaultInstance());
  }

  private void stopServices() throws Exception {
    mRegistry.stop();
    mJournalSystem.stop();
  }
}
