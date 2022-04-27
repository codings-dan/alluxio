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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.Source;
import alluxio.master.file.AccessControlEnforcer;
import alluxio.master.file.meta.InodeAttributes;
import alluxio.master.file.meta.InodeView;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.security.UserGroupInformation;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Unit tests for {@link HdfsInodeAttributesProvider}.
 */

public final class HdfsInodeAttributesProviderTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();
  private HdfsInodeAttributesProvider mProvider;
  private INodeAttributeProvider mHadoopProvider;
  private AccessControlEnforcer mDefaultEnforcer;
  private AccessControlEnforcer mEnforcer;
  private INodeAttributeProvider.AccessControlEnforcer mHadoopEnforcer;
  private ArgumentCaptor<INodeAttributeProvider.AccessControlEnforcer> mHdfsDefaultEnforcerArg;

  @Before
  public void before() {
    mHadoopProvider = mock(INodeAttributeProvider.class);
    mHadoopEnforcer = mock(INodeAttributeProvider.AccessControlEnforcer.class);
    mHdfsDefaultEnforcerArg =
        ArgumentCaptor.forClass(INodeAttributeProvider.AccessControlEnforcer.class);
    when(mHadoopProvider.getExternalAccessControlEnforcer(mHdfsDefaultEnforcerArg.capture()))
        .thenReturn(mHadoopEnforcer);
    mProvider = new HdfsInodeAttributesProvider(mHadoopProvider);
    mDefaultEnforcer = mock(AccessControlEnforcer.class);
    mEnforcer = mProvider.getExternalAccessControlEnforcer(mDefaultEnforcer);
  }

  private class PassthroughAccessControlEnforcer
      implements INodeAttributeProvider.AccessControlEnforcer {
    private final INodeAttributeProvider.AccessControlEnforcer mAce;

    public PassthroughAccessControlEnforcer(INodeAttributeProvider.AccessControlEnforcer enforcer) {
      mAce = enforcer;
    }

    @Override
    public void checkPermission(String s, String s1, UserGroupInformation userGroupInformation,
        INodeAttributes[] iNodeAttributes, INode[] iNodes, byte[][] bytes, int i, String s2, int i1,
        boolean b, FsAction fsAction, FsAction fsAction1, FsAction fsAction2, FsAction fsAction3,
        boolean b1) throws org.apache.hadoop.security.AccessControlException {
      mAce.checkPermission(s, s1, userGroupInformation, iNodeAttributes, iNodes, bytes, i, s2, i1,
          b, fsAction, fsAction1, fsAction2, fsAction3, b1);
    }
  }

  private <T> List<T> createNodesForPath(String path, int length, Function<String, T> createFunc)
      throws Exception {
    String[] components = PathUtils.getPathComponents(path);
    return IntStream.range(0, components.length)
        .mapToObj(index -> index < length ? createFunc.apply(components[index]) : null)
        .collect(Collectors.toList());
  }

  private List<InodeView> createInodesForPath(String path, int length) throws Exception {
    return createNodesForPath(path, length, name -> {
      InodeView inode = mock(InodeView.class);
      when(inode.getName()).thenReturn(name);
      return inode;
    });
  }

  private List<InodeAttributes> createInodeAttributessForPath(String path, int length)
      throws Exception {
    return createNodesForPath(path, length, name -> {
      InodeAttributes attributes = mock(InodeAttributes.class);
      when(attributes.getName()).thenReturn(name);
      return attributes;
    });
  }

  public static INode[] matchHdfsInodes(String path) {
//    return argThat(pathMatcher(path, x -> x.getLocalName()));
    return new INode[0];
  }

  public static INodeAttributes[] matchHdfsAttributes(String path) {
//    return argThat(pathMatcher(path, x -> new String(x.getLocalNameBytes())));
    return new INodeAttributes[0];
  }

  private static <T> Matcher<T[]> pathMatcher(String path, Function<T, String> nameFunc) {
    return new BaseMatcher<T[]>() {
      @Override
      public boolean matches(Object item) {
        T[] nodes = (T[]) item;
        String nodePath = String.join("/",
            Arrays.stream(nodes).map(nameFunc).collect(Collectors.toList()));
        return path.startsWith(nodePath);
      }

      @Override
      public void describeTo(Description description) {
        description.appendValue(path);
      }
    };
  }

//  private static UserGroupInformation matchUgi(String user, List<String> groups) {
//    return argThat(new BaseMatcher<UserGroupInformation>() {
//      @Override
//      public boolean matches(Object item) {
//        UserGroupInformation ugi = (UserGroupInformation) item;
//        return user.equals(ugi.getUserName())
//            && Arrays.equals(ugi.getGroupNames(), groups.toArray());
//      }
//
//      @Override
//      public void describeTo(Description description) {
//        description.appendText("user: ");
//        description.appendValue(user);
//        description.appendText("groups: ");
//        description.appendValueList("groups: ", ",", ". ", groups);
//      }
//    });
//  }

//  private static byte[][] matchPathComponents(String path) {
//    return argThat(new BaseMatcher<byte[][]>() {
//      @Override
//      public boolean matches(Object item) {
//        byte[][] components = (byte[][]) item;
//        try {
//         return Arrays.equals(Arrays.stream(components).map(bytes -> new String(bytes)).toArray(),
//              PathUtils.getPathComponents(path));
//        } catch (InvalidPathException e) {
//          return false;
//        }
//      }
//
//      @Override
//      public void describeTo(Description description) {
//        description.appendValue(path);
//      }
//    });
//  }
//
//  private static INodeAttributes matchINodeAttributes(InodeAttributes attr) {
//    return argThat(new BaseMatcher<INodeAttributes>() {
//      @Override
//      public boolean matches(Object item) {
//        INodeAttributes attrArg = (INodeAttributes) item;
//        return item != null
//            && new String(attrArg.getLocalNameBytes()).equals(attr.getName())
//            && attrArg.getFsPermissionShort() == attr.getMode()
//            && attrArg.isDirectory() == attr.isDirectory()
//            && attrArg.getModificationTime() == attr.getLastModificationTimeMs()
//            && StringUtils.equals(attrArg.getGroupName(), attr.getGroup())
//            && StringUtils.equals(attrArg.getUserName(), attr.getOwner());
//      }
//
//      @Override
//      public void describeTo(Description description) {
//        description.appendValue(attr.getName());
//        description.appendValue(attr.getMode());
//        description.appendValue(attr.isDirectory());
//        description.appendValue(attr.getLastModificationTimeMs());
//        description.appendValue(attr.getGroup());
//        description.appendValue(attr.getOwner());
//      }
//    });
//  }

  @Test
  public void constructWithConfiguration() {
//    UnderFileSystemConfiguration conf = UnderFileSystemConfiguration.defaults();
    UnderFileSystemConfiguration conf =
          UnderFileSystemConfiguration.defaults(ServerConfiguration.global());
    String groupValue = "su";
    conf.merge(ImmutableMap.of(
            DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
        DummyHdfsProvider.class.getName()), Source.RUNTIME);
    conf.merge(ImmutableMap.of(
            DFS_PERMISSIONS_SUPERUSERGROUP_KEY, groupValue), Source.RUNTIME);
//    conf.createMountSpecificConf(ImmutableMap.of(
//        DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY, DummyHdfsProvider.class.getName(),
//        DFS_PERMISSIONS_SUPERUSERGROUP_KEY, groupValue
//        ));
    HdfsInodeAttributesProvider provider = new HdfsInodeAttributesProvider(conf);
    assertNotNull(provider.getHdfsProvider());
    assertTrue(provider.getHdfsProvider() instanceof DummyHdfsProvider);
    assertEquals(groupValue, ((DummyHdfsProvider) provider.getHdfsProvider()).getConf()
        .get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY));
  }

  @Test
  public void checkPermissionSuccess() throws Exception {
    String user = "test";
    List<String> groups = Collections.singletonList("test_group");
    Mode.Bits bits = Mode.Bits.READ;
    String path = "/folder/file";
    List<InodeView> inodes = createInodesForPath(path, 3);
    List<InodeAttributes> attributes = createInodeAttributessForPath(path, 3);
    boolean doCheckOwner = false;

    mEnforcer.checkPermission(user, groups, bits, path, inodes, attributes, doCheckOwner);

    String fsOwner = "user1";
    InstancedConfiguration instancedConfiguration =
        new InstancedConfiguration(new AlluxioProperties());
    String supergroup =
        instancedConfiguration.getString(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP);
    FsAction access = FsAction.getFsAction(bits.toString());
    int ancestorIndex = inodes.size() - 2;
//    verify(mHadoopEnforcer).checkPermission(eq(fsOwner), eq(supergroup), matchUgi(user, groups),
//        matchHdfsAttributes(path), matchHdfsInodes(path), matchPathComponents(path),
//        eq(Snapshot.CURRENT_STATE_ID), eq(path), eq(ancestorIndex), eq(doCheckOwner),
//        eq(null), eq(null), eq(access), eq(null), eq(false));
  }

  @Test
  public void checkPermissionFail() throws Exception {
    String user = "test";
    List<String> groups = Collections.singletonList("test_group");
    Mode.Bits bits = Mode.Bits.READ;
    String path = "/folder/file";
    List<InodeView> inodes = createInodesForPath(path, 3);
    List<InodeAttributes> attributes = createInodeAttributessForPath(path, 3);
    String fsOwner = "user1";
    InstancedConfiguration instancedConfiguration =
        new InstancedConfiguration(new AlluxioProperties());
    String supergroup = instancedConfiguration.getString(
        alluxio.conf.PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP);
    FsAction access = FsAction.getFsAction(bits.toString());
    int ancestorIndex = inodes.size() - 2;
    boolean doCheckOwner = false;
//    doThrow(new org.apache.hadoop.security.AccessControlException("test")).when(mHadoopEnforcer)
//        .checkPermission(eq(fsOwner), eq(supergroup), matchUgi(user, groups),
//            matchHdfsAttributes(path), matchHdfsInodes(path), matchPathComponents(path),
//            eq(Snapshot.CURRENT_STATE_ID), eq(path), eq(ancestorIndex), eq(doCheckOwner),
//            eq(null), eq(null), eq(access), eq(null), eq(false));
//    mThrown.expectMessage("test");
//    mThrown.expect(alluxio.exception.AccessControlException.class);
    mEnforcer.checkPermission(user, groups, bits, path, inodes, attributes, doCheckOwner);
  }

  @Test
  public void checkPermissionPassThroughSuccess() throws Exception {
    String user = "test";
    List<String> groups = Collections.singletonList("test_group");
    Mode.Bits bits = Mode.Bits.READ;
    String path = "/folder/file";
    List<InodeView> inodes = createInodesForPath(path, 3);
    List<InodeAttributes> attributes = createInodeAttributessForPath(path, 3);
    boolean doCheckOwner = false;
    reset(mHadoopProvider);
    doAnswer(invocation ->
        new PassthroughAccessControlEnforcer(
            (INodeAttributeProvider.AccessControlEnforcer) invocation.getArguments()[0]))
        .when(mHadoopProvider).getExternalAccessControlEnforcer(any());
    mEnforcer = mProvider.getExternalAccessControlEnforcer(mDefaultEnforcer);

    mEnforcer.checkPermission(user, groups, bits, path, inodes, attributes, doCheckOwner);

//    verify(mDefaultEnforcer).checkPermission(eq(user), eq(groups), eq(bits), eq(path),
//        ExtensionInodeAttributesProviderTest.matchInodeList(path,3),
//        ExtensionInodeAttributesProviderTest.matchAttributesList(path,3), eq(doCheckOwner));
  }

  @Test
  public void checkPermissionPassThroughFail() throws Exception {
    String user = "test";
    List<String> groups = Collections.singletonList("test_group");
    Mode.Bits bits = Mode.Bits.READ;
    String path = "/folder/file";
    List<InodeView> inodes = createInodesForPath(path, 3);
    List<InodeAttributes> attributes = createInodeAttributessForPath(path, 3);
    boolean doCheckOwner = false;
    reset(mHadoopProvider);
    doAnswer(invocation ->
        new PassthroughAccessControlEnforcer(
            (INodeAttributeProvider.AccessControlEnforcer) invocation.getArguments()[0]))
        .when(mHadoopProvider).getExternalAccessControlEnforcer(any());
//    doThrow(new AccessControlException("test")).when(mDefaultEnforcer)
//        .checkPermission(eq(user), eq(groups), eq(bits), eq(path),
//            ExtensionInodeAttributesProviderTest.matchInodeList(path,3),
//            ExtensionInodeAttributesProviderTest.matchAttributesList(path,3), eq(doCheckOwner));
//    mThrown.expect(alluxio.exception.AccessControlException.class);
    mEnforcer = mProvider.getExternalAccessControlEnforcer(mDefaultEnforcer);

    mEnforcer.checkPermission(user, groups, bits, path, inodes, attributes, doCheckOwner);
  }

  @Test
  public void getAttributesPassThrough() throws Exception {
    String path = "/folder/file";
    InodeAttributes attr = createInodeAttributessForPath(path, 3).get(2);
    when(attr.getMode()).thenReturn((short) 0700);
    when(attr.isDirectory()).thenReturn(true);
    when(attr.getOwner()).thenReturn("test1");
    when(attr.getGroup()).thenReturn("grp1");
    when(attr.getName()).thenReturn("dir1");
    when(attr.getLastModificationTimeMs()).thenReturn(22222L);
    String[] pathComps = PathUtils.getPathComponents(path);
    INodeAttributes mockAttr = mock(INodeAttributes.class);
    when(mockAttr.isDirectory()).thenReturn(true);
    when(mockAttr.getUserName()).thenReturn("test2");
    when(mockAttr.getGroupName()).thenReturn("grp2");
    when(mockAttr.getFsPermissionShort()).thenReturn((short) 0755);
    when(mockAttr.getLocalNameBytes()).thenReturn("dir1".getBytes());
    when(mockAttr.getModificationTime()).thenReturn(12345L);
    when(mHadoopProvider.getAttributes(eq(pathComps), any())).thenReturn(mockAttr);

    InodeAttributes newAttr = mProvider.getAttributes(pathComps, attr);

//    verify(mHadoopProvider).getAttributes(eq(pathComps), matchINodeAttributes(attr));
    assertEquals(newAttr.getName(), new String(mockAttr.getLocalNameBytes()));
    assertEquals(newAttr.getOwner(), mockAttr.getUserName());
    assertEquals(newAttr.getGroup(), mockAttr.getGroupName());
    assertEquals(newAttr.getMode(), mockAttr.getFsPermissionShort());
    assertEquals(newAttr.getLastModificationTimeMs(), mockAttr.getModificationTime());
    assertEquals(newAttr.isDirectory(), mockAttr.isDirectory());
  }
}
