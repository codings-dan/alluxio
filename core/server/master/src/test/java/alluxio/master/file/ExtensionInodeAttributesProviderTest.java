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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.CreatePathContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeAttributes;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Suppliers;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Unit test for {@link ExtensionInodeAttributesProvider}.
 */

public final class ExtensionInodeAttributesProviderTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(ExtensionInodeAttributesProviderTest.class);

  private static final TestUser TEST_USER_ADMIN = new TestUser("admin", "admin");
  private static final TestUser TEST_USER = new TestUser("user1", "group1");

  private static final String TEST_DIR_URI = "/testDir";
  private static final String TEST_DIR_FILE_URI = "/testDir/file";
  private static final String TEST_DIR_NESTED_MOUNT = "/testDir/nested";
  private static final String TEST_DIR_FILE_NESTED_MOUNT = "/testDir/nested/file";
  private static final String TEST_DIR_FILE_NESTED_MOUNT_RELATIVE = "/file";

  private static final Mode TEST_NORMAL_MODE = new Mode((short) 0755);
  private static final String ROOT_UFS_URI = "/rootUfs/a/b";
  private static final String NESTED_UFS_URI = "/nestedUfs/c/d";
  private static InodeStore sInodeStore;

  private PermissionChecker mPermissionChecker;
  private static CreateFileContext sFileOptions;
  private static CreateDirectoryContext sDirectoryOptions;

  private static MasterRegistry sRegistry;
  private static MetricsMaster sMetricsMaster;

  @ClassRule
  public static TemporaryFolder sTestFolder = new TemporaryFolder();
  private static BlockMaster sBlockMaster;
  private static InodeDirectoryIdGenerator sDirectoryIdGenerator;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();
  private AbstractInodeAttributesProviderFactory mFactory;
  private UfsManager mUfsManager;
  private MountTable mMountTable;
  private static InodeTree sTree;
  private ExtensionInodeAttributesProvider mProvider;
  private AccessControlEnforcer mDefaultEnforcer;
  private AccessControlEnforcer mExternalEnforcer;
  private InodeAttributesProvider mMasterProvider;
  private AccessControlEnforcer mMasterEnforcer;
  private final UnderFileSystem mTestUfs =
          new LocalUnderFileSystemFactory().create("/",
                  UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));
  private InodeAttributesProvider mRootUfsProvider;
  private AccessControlEnforcer mRootUfsEnforcer;
  private InodeAttributesProvider mNestedUfsProvider;
  private AccessControlEnforcer mNestedUfsEnforcer;

/**
   * A simple structure to represent a user and its groups.
   */

  private static final class TestUser {
    private String mUser;
    private String mGroup;

    TestUser(String user, String group) {
      mUser = user;
      mGroup = group;
    }

    String getUser() {
      return mUser;
    }

    String getGroup() {
      return mGroup;
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    sFileOptions = CreateFileContext
            .mergeFrom(CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB)
                    .setMode(TEST_NORMAL_MODE.toProto()))
            .setOwner(TEST_USER.getUser()).setGroup(TEST_USER.getGroup());
    sDirectoryOptions = CreateDirectoryContext
            .mergeFrom(CreateDirectoryPOptions.newBuilder().setMode(TEST_NORMAL_MODE.toProto()))
            .setOwner(TEST_USER.getUser()).setGroup(TEST_USER.getGroup());

//    sRegistry = new MasterRegistry();
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext(new NoopJournalSystem());
//    sMetricsMaster = new MetricsMasterFactory().create(sRegistry, masterContext);
//    sRegistry.add(MetricsMaster.class, sMetricsMaster);
//    sBlockMaster = new BlockMasterFactory()
//        .create(sRegistry, masterContext);
//    sDirectoryIdGenerator = new InodeDirectoryIdGenerator(sBlockMaster);
//
//    sRegistry.start(true);
    // setup an InodeTree
    sRegistry = new MasterRegistry();
    sMetricsMaster = new MetricsMasterFactory().create(sRegistry, masterContext);
    sRegistry.add(MetricsMaster.class, sMetricsMaster);
    BlockMaster blockMaster = new BlockMasterFactory().create(sRegistry, masterContext);
    InodeDirectoryIdGenerator directoryIdGenerator = new InodeDirectoryIdGenerator(blockMaster);
    UfsManager ufsManager = mock(UfsManager.class);
    MountTable mountTable = new MountTable(ufsManager, mock(MountInfo.class));
    InodeLockManager lockManager = new InodeLockManager();
    sInodeStore = masterContext.getInodeStoreFactory().apply(lockManager);
    sTree = new InodeTree(sInodeStore, blockMaster, directoryIdGenerator, mountTable, lockManager);

    sRegistry.start(true);

    GroupMappingServiceTestUtils.resetCache();
    ServerConfiguration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
            PermissionCheckerTest.FakeUserGroupsMapping.class.getName());
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE,
            AuthType.SIMPLE);
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    ServerConfiguration.set(TxPropertyKey.SECURITY_AUTHORIZATION_PLUGINS_ENABLED, "true");

    sTree.initializeRoot(TEST_USER_ADMIN.getUser(), TEST_USER_ADMIN.getGroup(), TEST_NORMAL_MODE,
            NoopJournalContext.INSTANCE);

//    createAndSetPermission(TEST_DIR_URI, sDirectoryOptions);
//    createAndSetPermission(TEST_DIR_FILE_URI, sFileOptions);

    createAndSetPermission(TEST_DIR_URI, sDirectoryOptions);
    createAndSetPermission(TEST_DIR_FILE_URI, sFileOptions);
    createAndSetPermission(TEST_DIR_NESTED_MOUNT, sDirectoryOptions);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    sRegistry.stop();
    AuthenticatedClientUser.remove();
    ConfigurationTestUtils.defaults();
  }

  @Before
  public void before() throws Exception {
    AuthenticatedClientUser.remove();
    mUfsManager = mock(UfsManager.class);
    UfsManager.UfsClient ufsClient =
            new UfsManager.UfsClient(Suppliers.ofInstance(mTestUfs), AlluxioURI.EMPTY_URI);
    when(mUfsManager.get(anyLong())).thenReturn(ufsClient);
    mFactory =
        mock(AbstractInodeAttributesProviderFactory.class);
//    mMountTable = new MountTable(mUfsManager,mock(MountInfo.class));
//    // setup an InodeTree
//    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
//    sRegistry.add(MetricsMaster.class, sMetricsMaster);
//    InodeLockManager lockManager = new InodeLockManager();
//    sInodeStore = masterContext.getInodeStoreFactory().apply(lockManager);
//    mTree =
//      new InodeTree(sInodeStore, sBlockMaster, sDirectoryIdGenerator, mMountTable, lockManager);
//    mTree.initializeRoot(TEST_USER_ADMIN.getUser(), TEST_USER_ADMIN.getGroup(),
//      TEST_NORMAL_MODE, NoopJournalContext.INSTANCE);
    mMountTable = new MountTable(mUfsManager, new MountInfo(new AlluxioURI("/"),
            new AlluxioURI("/rootUfs/a/b"), 1, MountContext.defaults().getOptions().build()));
//    AuthenticatedClientUser.remove();
    mPermissionChecker = new DefaultPermissionChecker(sTree);

    // build file structure
//    createAndSetPermission(TEST_DIR_NESTED_MOUNT, sDirectoryOptions);
  }

  private void initPlugins(boolean masterPlugin, boolean rootUfsPlugin, boolean nestedUfsPlugin)
      throws Exception {
    long rootUfsMountId = IdUtils.ROOT_MOUNT_ID;
    long nestedUfsMountId = IdUtils.createMountId();
//    mMountTable.add(NoopJournalContext.INSTANCE,new AlluxioURI(MountTable.ROOT),
//      new AlluxioURI(ROOT_UFS_URI), rootUfsMountId,
//            MountContext.defaults().getOptions().build());
    mMountTable.add(NoopJournalContext.INSTANCE, new AlluxioURI(TEST_DIR_NESTED_MOUNT),
        new AlluxioURI(NESTED_UFS_URI), nestedUfsMountId,
        MountContext.defaults().getOptions().build());
    if (masterPlugin) {
      ServerConfiguration.set(
          TxPropertyKey.SECURITY_AUTHORIZATION_PLUGIN_NAME, "test-plugin");
      mMasterProvider = mock(InodeAttributesProvider.class);
      mMasterEnforcer = mock(AccessControlEnforcer.class);
      when(mFactory.createMasterProvider()).thenReturn(mMasterProvider);
      when(mMasterProvider.getExternalAccessControlEnforcer(any())).thenReturn(mMasterEnforcer);
    }
    if (rootUfsPlugin) {
      mRootUfsProvider = mock(InodeAttributesProvider.class);
      mRootUfsEnforcer = mock(AccessControlEnforcer.class);
      when(mUfsManager.getUfsService(eq(rootUfsMountId), eq(InodeAttributesProvider.class)))
              .thenReturn(mRootUfsProvider);
      when(mRootUfsProvider.getExternalAccessControlEnforcer(any())).thenReturn(mRootUfsEnforcer);
    }
    if (nestedUfsPlugin) {
      mNestedUfsProvider = mock(InodeAttributesProvider.class);
      mNestedUfsEnforcer = mock(AccessControlEnforcer.class);
      when(mUfsManager.getUfsService(eq(nestedUfsMountId), eq(InodeAttributesProvider.class)))
              .thenReturn(mNestedUfsProvider);
      when(mNestedUfsProvider.getExternalAccessControlEnforcer(any()))
          .thenReturn(mNestedUfsEnforcer);
    }

    mProvider = new ExtensionInodeAttributesProvider(mMountTable, mFactory);
    mDefaultEnforcer = mock(AccessControlEnforcer.class);
    mExternalEnforcer = mProvider.getExternalAccessControlEnforcer(mDefaultEnforcer);
  }

/**
   * Helper function to create a path and set the permission to what specified in option.
   *
   * @param path path to construct the {@link AlluxioURI} from
   * @param option method options for creating a file
   */

  private static void createAndSetPermission(String path, CreatePathContext option)
      throws Exception {
    try (LockedInodePath inodePath =
             sTree.lockInodePath(new AlluxioURI(path), InodeTree.LockPattern.WRITE_EDGE)) {
      List<Inode> result = sTree.createPath(RpcContext.NOOP, inodePath, option);
      MutableInode<?> inode = sInodeStore.getMutable(result.get(result.size() - 1).getId()).get();
      inode.setOwner(option.getOwner())
              .setGroup(option.getGroup())
              .setMode(option.getMode().toShort());
      sInodeStore.writeInode(inode);
    }
  }

  private void checkPermission(TestUser user, Mode.Bits action, String path)
      throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    try (LockedInodePath inodePath = sTree
        .lockInodePath(new AlluxioURI(path), InodeTree.LockPattern.READ)) {
      List<InodeView> inodes = inodePath.getInodeViewList();
      List<InodeAttributes> attributes = inodes.stream().map(x -> (InodeAttributes)
          new ExtendablePermissionChecker.DefaultInodeAttributes(x)).collect(Collectors.toList());
      mExternalEnforcer.checkPermission(user.getUser(), Collections.singletonList(user.getGroup()),
          action, path, inodes, attributes, false);
    }
  }

//  public static List<Inode> matchInodeList(String path) {
////    return argThat(ListMatcher(path, x -> x.getName()));
//    return new ArrayList<Inode>();
//  }
  public static <T> List<T> createNodesForPath(String path, int length,
      Function<String, T> createFunc) throws Exception {
    String[] components = PathUtils.getPathComponents(path);
    return IntStream.range(0, components.length)
        .mapToObj(index -> index < length ? createFunc.apply(components[index]) : null)
        .collect(Collectors.toList());
  }

  public static List<Inode> matchInodeList(String path, int length) throws Exception {
    return createNodesForPath(path, length, name -> {
      Inode inode = mock(Inode.class);
      when(inode.getName()).thenReturn(name);
      return inode;
    });
  }

  public static List<InodeAttributes> matchAttributesList(String path, int length)
      throws Exception {
    return createNodesForPath(path, length, name -> {
      InodeAttributes attributes = mock(InodeAttributes.class);
      when(attributes.getName()).thenReturn(name);
      return attributes;
    });
  }
//  public static List<InodeAttributes> matchAttributesList(String path) {
////    return argThat(ListMatcher(path, x -> x.getName()));
//    return new ArrayList<InodeAttributes>();
//  }

  private static <T> Matcher<List<T>> ListMatcher(String path, Function<T, String> nameFunc) {
    return new BaseMatcher<List<T>>() {
      @Override
      public boolean matches(Object item) {
        List<T> list = (List<T>) item;
        String nodePath =
            String.join("/", list.stream().map(nameFunc).collect(Collectors.toList()));
        return path.startsWith(nodePath);
      }

      @Override
      public void describeTo(Description description) {
        description.appendValue(path);
      }
    };
  }

  @Test
  public void checkPermissionWithMasterPluginSuccess() throws Exception {
    initPlugins(true, false, false);
    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_URI);

//    verify(mMasterEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
//        eq(TEST_DIR_FILE_URI), matchInodeList(TEST_DIR_FILE_URI,3),
//        matchAttributesList(TEST_DIR_FILE_URI,3), eq(false));
  }

  @Test
  public void checkPermissionWithMasterPluginFail() throws Exception {
    initPlugins(true, false, false);
    AccessControlException ex = new AccessControlException("test");
    doThrow(ex).when(mMasterEnforcer)
        .checkPermission(eq(TEST_USER.getUser()),
            eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
            eq(TEST_DIR_FILE_URI), any(), any(), eq(false));
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ex.getMessage());

    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_URI);
  }

  @Test
  public void checkPermissionWithRootUfsPluginSuccess() throws Exception {
    initPlugins(false, true, false);

    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_URI);
//    String rootUfsFile = ROOT_UFS_URI + TEST_DIR_FILE_URI;
//    verify(mRootUfsEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
//        eq(rootUfsFile), matchInodeList(rootUfsFile), matchAttributesList(rootUfsFile),
//        eq(false));
  }

  @Test
  public void checkPermissionWithRootUfsPluginFail() throws Exception {
    initPlugins(false, true, false);
    AccessControlException ex = new AccessControlException("test");
    doThrow(ex).when(mRootUfsEnforcer)
        .checkPermission(eq(TEST_USER.getUser()),
            eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
            eq(ROOT_UFS_URI + TEST_DIR_FILE_URI), any(), any(), eq(false));
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ex.getMessage());

    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_URI);
  }

  @Test
  public void checkPermissionWithNestedUfsPluginSuccess() throws Exception {
    initPlugins(false, false, true);

    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);

    String nestedUfsFile = NESTED_UFS_URI + TEST_DIR_FILE_NESTED_MOUNT_RELATIVE;
    verify(mDefaultEnforcer).checkPermission(eq(TEST_USER.getUser()),
        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.EXECUTE),
        eq(TEST_DIR_URI), any(), any(), eq(false));
//    verify(mNestedUfsEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
//        eq(nestedUfsFile),
//        matchInodeList(nestedUfsFile), matchAttributesList(nestedUfsFile), eq(false));
  }

  @Test
  public void checkPermissionWithNestedUfsPluginFail() throws Exception {
    initPlugins(false, false, true);
    AccessControlException ex = new AccessControlException("test");
    doThrow(ex).when(mNestedUfsEnforcer)
        .checkPermission(eq(TEST_USER.getUser()),
            eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
            eq(NESTED_UFS_URI + TEST_DIR_FILE_NESTED_MOUNT_RELATIVE), any(), any(), eq(false));
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ex.getMessage());

    try {
      checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);
    } finally {
      verify(mDefaultEnforcer).checkPermission(eq(TEST_USER.getUser()),
          eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.EXECUTE),
          eq(TEST_DIR_URI), any(), any(), eq(false));
    }
  }

  @Test
  public void checkPermissionWithRootUfsPluginWithNestedPathSuccess() throws Exception {
    initPlugins(false, true, false);

    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);

    String rootUfsFileUri = ROOT_UFS_URI + TEST_DIR_URI;
//    verify(mRootUfsEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.EXECUTE),
//        eq(rootUfsFileUri), matchInodeList(rootUfsFileUri), matchAttributesList(rootUfsFileUri),
//        eq(false));
//    verify(mDefaultEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
//        eq(TEST_DIR_FILE_NESTED_MOUNT), matchInodeList(TEST_DIR_FILE_NESTED_MOUNT),
//        matchAttributesList(TEST_DIR_FILE_NESTED_MOUNT), eq(false));
  }

  @Test
  public void checkPermissionWithRootUfsPluginWithNestedPathFail() throws Exception {
    initPlugins(false, true, false);
    AccessControlException ex = new AccessControlException("test");
    doThrow(ex).when(mRootUfsEnforcer)
        .checkPermission(eq(TEST_USER.getUser()),
            eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.EXECUTE),
            eq(ROOT_UFS_URI + TEST_DIR_URI), any(), any(), eq(false));
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ex.getMessage());

    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);
  }

  @Test
  public void checkPermissionWithMasterAndRootUfsPluginWithNestedPathSuccess() throws Exception {
    initPlugins(true, true, false);

    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);

//    String rootUfsFile = ROOT_UFS_URI + TEST_DIR_URI;
//    verify(mMasterEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
//        eq(TEST_DIR_FILE_NESTED_MOUNT), matchInodeList(TEST_DIR_FILE_NESTED_MOUNT),
//        matchAttributesList(TEST_DIR_FILE_NESTED_MOUNT), eq(false));
//    verify(mRootUfsEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.EXECUTE),
//        eq(rootUfsFile), matchInodeList(rootUfsFile), matchAttributesList(rootUfsFile),
//       eq(false));
    verify(mDefaultEnforcer, never()).checkPermission(any(), any(), any(),
        any(), any(), any(), anyBoolean());
  }

  @Test
  public void checkPermissionWithMasterAndRootUfsPluginWithNestedPathFail() throws Exception {
    initPlugins(true, true, false);
    AccessControlException ex = new AccessControlException("test");
    doThrow(ex).when(mRootUfsEnforcer)
        .checkPermission(eq(TEST_USER.getUser()),
            eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.EXECUTE),
            eq(ROOT_UFS_URI + TEST_DIR_URI), any(), any(), eq(false));
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ex.getMessage());

    try {
      checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);
    } finally {
      verify(mDefaultEnforcer, never()).checkPermission(any(), any(), any(),
          any(), any(), any(), anyBoolean());
    }
  }

  @Test
  public void checkPermissionWithMasterAndRootUfsPluginWithNestedPathFailMaster() throws Exception {
    initPlugins(true, true, false);
    AccessControlException ex = new AccessControlException("test");
    doThrow(ex).when(mMasterEnforcer)
        .checkPermission(eq(TEST_USER.getUser()),
            eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
            eq(TEST_DIR_FILE_NESTED_MOUNT), any(), any(), eq(false));
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ex.getMessage());

    try {
      checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);
    } finally {
      verify(mDefaultEnforcer, never()).checkPermission(any(), any(), any(),
          any(), any(), any(), anyBoolean());
    }
  }

  @Test
  public void checkPermissionWithMasterAndNestedUfsPluginSuccess() throws Exception {
    initPlugins(true, false, true);

    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);

//    String nestedUfsFile = NESTED_UFS_URI + TEST_DIR_FILE_NESTED_MOUNT_RELATIVE;
//    verify(mMasterEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
//        eq(TEST_DIR_FILE_NESTED_MOUNT), matchInodeList(TEST_DIR_FILE_NESTED_MOUNT),
//        matchAttributesList(TEST_DIR_FILE_NESTED_MOUNT), eq(false));
//    verify(mNestedUfsEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
//        eq(nestedUfsFile), matchInodeList(nestedUfsFile), matchAttributesList(nestedUfsFile),
//        eq(false));
    verify(mDefaultEnforcer, never()).checkPermission(any(), any(), any(),
        any(), any(), any(), anyBoolean());
  }

  @Test
  public void checkPermissionWithMasterAndNestedUfsPluginFail() throws Exception {
    initPlugins(true, false, true);
    AccessControlException ex = new AccessControlException("test");
    doThrow(ex).when(mNestedUfsEnforcer)
        .checkPermission(eq(TEST_USER.getUser()),
            eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
            eq(NESTED_UFS_URI + TEST_DIR_FILE_NESTED_MOUNT_RELATIVE), any(), any(), eq(false));
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ex.getMessage());

    try {
      checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);
    } finally {
      verify(mDefaultEnforcer, never()).checkPermission(any(), any(), any(),
          any(), any(), any(), anyBoolean());
    }
  }

  @Test
  public void checkPermissionWithMasterAndNestedUfsPluginFailMaster() throws Exception {
    initPlugins(true, true, false);
    AccessControlException ex = new AccessControlException("test");
    doThrow(ex).when(mMasterEnforcer)
        .checkPermission(eq(TEST_USER.getUser()),
            eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
            eq(TEST_DIR_FILE_NESTED_MOUNT), any(), any(), eq(false));
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ex.getMessage());

    try {
      checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);
    } finally {
      verify(mDefaultEnforcer, never()).checkPermission(any(), any(), any(),
          any(), any(), any(), anyBoolean());
    }
  }

  @Test
  public void checkPermissionWithAllPluginsSuccess() throws Exception {
    initPlugins(true, true, true);

    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);

    String rootUfsFile = ROOT_UFS_URI + TEST_DIR_URI;
    String nestedUfsFile = NESTED_UFS_URI + TEST_DIR_FILE_NESTED_MOUNT_RELATIVE;
//    verify(mMasterEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
//        eq(TEST_DIR_FILE_NESTED_MOUNT), matchInodeList(TEST_DIR_FILE_NESTED_MOUNT),
//        matchAttributesList(TEST_DIR_FILE_NESTED_MOUNT), eq(false));
//    verify(mRootUfsEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.EXECUTE),
//        eq(rootUfsFile), matchInodeList(rootUfsFile), matchAttributesList(rootUfsFile),
//        eq(false));
//    verify(mNestedUfsEnforcer).checkPermission(eq(TEST_USER.getUser()),
//        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
//        eq(nestedUfsFile), matchInodeList(nestedUfsFile), matchAttributesList(nestedUfsFile),
//        eq(false));
    verify(mDefaultEnforcer, never()).checkPermission(any(), any(), any(),
        any(), any(), any(), anyBoolean());
  }

  @Test
  public void checkPermissionWithAllPluginsFailMaster() throws Exception {
    initPlugins(true, true, true);
    AccessControlException ex = new AccessControlException("test");
    doThrow(ex).when(mMasterEnforcer)
        .checkPermission(eq(TEST_USER.getUser()),
            eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
            eq(TEST_DIR_FILE_NESTED_MOUNT), any(), any(), eq(false));
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ex.getMessage());

    try {
      checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);
    } finally {
      verify(mDefaultEnforcer, never()).checkPermission(any(), any(), any(),
          any(), any(), any(), anyBoolean());
    }
  }

  @Test
  public void checkPermissionWithAllPluginsFailRootUfs() throws Exception {
    initPlugins(true, true, true);
    AccessControlException ex = new AccessControlException("test");
    doThrow(ex).when(mRootUfsEnforcer)
        .checkPermission(eq(TEST_USER.getUser()),
            eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.EXECUTE),
            eq(ROOT_UFS_URI + TEST_DIR_URI), any(), any(), eq(false));
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ex.getMessage());

    try {
      checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);
    } finally {
      verify(mDefaultEnforcer, never()).checkPermission(any(), any(), any(),
          any(), any(), any(), anyBoolean());
    }
  }

  @Test
  public void checkPermissionWithAllPluginsFailNestedUfs() throws Exception {
    initPlugins(true, true, true);
    AccessControlException ex = new AccessControlException("test");
    doThrow(ex).when(mNestedUfsEnforcer)
        .checkPermission(eq(TEST_USER.getUser()),
            eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
            eq(NESTED_UFS_URI + TEST_DIR_FILE_NESTED_MOUNT_RELATIVE), any(), any(), eq(false));
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ex.getMessage());

    try {
      checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);
    } finally {
      verify(mDefaultEnforcer, never()).checkPermission(any(), any(), any(),
          any(), any(), any(), anyBoolean());
    }
  }

  @Test
  public void checkPermissionWithRootUfsPluginUseDefaultSuccess() throws Exception {
    initPlugins(false, true, false);
    reset(mRootUfsProvider);
    doAnswer(invocation ->
        new PassthroughAccessControlEnforcer(
            (AccessControlEnforcer) invocation.getArguments()[0]))
        .when(mRootUfsProvider).getExternalAccessControlEnforcer(any());
    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_URI);

    String rootUfsFile = ROOT_UFS_URI + TEST_DIR_FILE_URI;
    ArgumentCaptor<List> nodesArg = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List> attrArg = ArgumentCaptor.forClass(List.class);
    verify(mDefaultEnforcer).checkPermission(eq(TEST_USER.getUser()),
        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
        eq(rootUfsFile), nodesArg.capture(), attrArg.capture(),
        eq(false));
    List<Inode> inodes = nodesArg.getValue();
    List<InodeAttributes> attrs = attrArg.getValue();
//    verifyInodeList(ROOT_UFS_URI, rootUfsFile, inodes, Inode::getName, Inode::getMode);
//    verifyInodeList(ROOT_UFS_URI, rootUfsFile, attrs, InodeAttributes::getName,
//        InodeAttributes::getMode);
  }

  @Test
  public void checkPermissionNestedPathWithRootUfsPluginUseDefaultSuccess() throws Exception {
    initPlugins(false, true, false);
    reset(mRootUfsProvider);
    doAnswer(invocation ->
        new PassthroughAccessControlEnforcer(
            (AccessControlEnforcer) invocation.getArguments()[0]))
        .when(mRootUfsProvider).getExternalAccessControlEnforcer(any());
    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);

    String rootUfsPath = ROOT_UFS_URI + TEST_DIR_URI;
    ArgumentCaptor<List> nodesArg = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List> attrArg = ArgumentCaptor.forClass(List.class);

    // default enforcer checks root mount permission because the mock UFS enforcer called it
    verify(mDefaultEnforcer).checkPermission(eq(TEST_USER.getUser()),
        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.EXECUTE),
        eq(rootUfsPath), nodesArg.<List<InodeView>>capture(),
        attrArg.<List<InodeAttributes>>capture(), eq(false));
    List<InodeView> inodes = nodesArg.<List<InodeView>>getValue();
    List<InodeAttributes> attrs = attrArg.<List<InodeAttributes>>getValue();
    verifyInodeList(ROOT_UFS_URI, rootUfsPath, inodes, InodeView::getName, InodeView::getMode);
    verifyInodeList(ROOT_UFS_URI, rootUfsPath, attrs, InodeAttributes::getName,
        InodeAttributes::getMode);
    // default enforcer also checks nested mount UFS path because nested mount plugin is not enabled
    verify(mDefaultEnforcer).checkPermission(eq(TEST_USER.getUser()),
        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ),
        eq(TEST_DIR_FILE_NESTED_MOUNT), nodesArg.<List<InodeView>>capture(),
        attrArg.<List<InodeAttributes>>capture(), eq(false));
    inodes = nodesArg.<List<Inode>>getValue();
    attrs = attrArg.<List<InodeAttributes>>getValue();
    verifyInodeList(TEST_DIR_NESTED_MOUNT, TEST_DIR_FILE_NESTED_MOUNT, inodes,
        InodeView::getName, InodeView::getMode);
    verifyInodeList(TEST_DIR_NESTED_MOUNT, TEST_DIR_FILE_NESTED_MOUNT, attrs,
        InodeAttributes::getName, InodeAttributes::getMode);
  }

  @Test
  public void checkPermissionWithNestedUfsPluginUseDefaultSuccess() throws Exception {
    initPlugins(false, false, true);
    reset(mNestedUfsProvider);
    doAnswer(invocation ->
        new PassthroughAccessControlEnforcer(
            (AccessControlEnforcer) invocation.getArguments()[0]))
        .when(mNestedUfsProvider).getExternalAccessControlEnforcer(any());
    checkPermission(TEST_USER, Mode.Bits.READ, TEST_DIR_FILE_NESTED_MOUNT);

    String nestedUfsFile = NESTED_UFS_URI + TEST_DIR_FILE_NESTED_MOUNT_RELATIVE;
    ArgumentCaptor<List> nodesArg = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List> attrArg = ArgumentCaptor.forClass(List.class);

    // default enforcer checks root mount permission because root mount plugin is not enabled
    verify(mDefaultEnforcer).checkPermission(eq(TEST_USER.getUser()),
        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.EXECUTE),
        eq(TEST_DIR_URI), nodesArg.<List<InodeView>>capture(),
        attrArg.<List<Inode>>capture(), eq(false));
    List<InodeView> inodes = nodesArg.<List<InodeView>>getValue();
    List<InodeAttributes> attrs = attrArg.<List<InodeAttributes>>getValue();
    verifyInodeList("/", TEST_DIR_URI, inodes, InodeView::getName, InodeView::getMode);
    verifyInodeList("/", TEST_DIR_URI, attrs, InodeAttributes::getName,
        InodeAttributes::getMode);
    // default enforcer also checks nested mount UFS path because the mock UFS enforcer called it
    verify(mDefaultEnforcer).checkPermission(eq(TEST_USER.getUser()),
        eq(Collections.singletonList(TEST_USER.getGroup())), eq(Mode.Bits.READ), eq(nestedUfsFile),
        nodesArg.<List<Inode>>capture(), attrArg.<List<Inode>>capture(), eq(false));
    inodes = nodesArg.<List<Inode>>getValue();
    attrs = attrArg.<List<InodeAttributes>>getValue();
    verifyInodeList(NESTED_UFS_URI, nestedUfsFile, inodes, InodeView::getName, InodeView::getMode);
    verifyInodeList(NESTED_UFS_URI, nestedUfsFile, attrs, InodeAttributes::getName,
        InodeAttributes::getMode);
  }

  private static <T> void verifyInodeList(String mountUri, String fileUri, List<T> inodes,
      Function<T, String> getNameFunc, Function<T, Short> getModeFunc) throws InvalidPathException {
    String[] rootPathComp = PathUtils.getPathComponents(mountUri);
    String[] filePathComp = PathUtils.getPathComponents(fileUri);
    for (int i = 0; i < inodes.size(); i++) {
      T inode = inodes.get(i);
      String nodeName = getNameFunc.apply(inode);
      String nodeDescription = String.format("%s at index=%d, name=%s, path=%s, ufs=%s",
          inode.getClass().getSimpleName(), i, nodeName, fileUri, mountUri);
      assertEquals(nodeDescription, filePathComp[i], nodeName);
      boolean isAlluxioNode = i >= rootPathComp.length - 1;
      short expectedMode = isAlluxioNode ? TEST_NORMAL_MODE.toShort() : 0777;
      assertEquals(nodeDescription, expectedMode, getModeFunc.apply(inode).shortValue());
    }
  }

  // a pass through AccessControlEnforcer that delegate permission check to default enforcer
  private class PassthroughAccessControlEnforcer implements AccessControlEnforcer {
    private final AccessControlEnforcer mEnforcer;

    public PassthroughAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
      mEnforcer = defaultEnforcer;
    }

    @Override
    public void checkPermission(String user, List<String> groups, Mode.Bits bits, String path,
        List<InodeView> inodeList, List<InodeAttributes> attributes, boolean checkIsOwner)
        throws AccessControlException {
      mEnforcer.checkPermission(user, groups, bits, path, inodeList, attributes, checkIsOwner);
    }
  }
}
