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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidPathException;
import alluxio.extensions.ClassLoaderContext;
import alluxio.master.file.meta.InodeAttributes;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.proto.journal.Journal;
import alluxio.proto.meta.InodeMeta;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * An {@link InodeAttributesProvider} that checks permission with external plugins
 * for Alluxio path and UFS paths.
 */
public final class ExtensionInodeAttributesProvider implements InodeAttributesProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(ExtensionInodeAttributesProvider.class);
  private InodeAttributesProvider mMasterProvider;
  private MountTable mMountTable;

  /**
   * @param table master mount table
   * @param factory factory for loading authorization plugins
   */
  public ExtensionInodeAttributesProvider(MountTable table,
      AbstractInodeAttributesProviderFactory factory) {
    mMountTable = table;
    mMasterProvider = factory.createMasterProvider();
  }

  @Override
  public void start() {
    // Only the master plugin is started here. UFS plugin life cycle is managed by UfsManager.
    if (mMasterProvider != null) {
      try (ClassLoaderContext c = ClassLoaderContext.useClassLoaderFrom(mMasterProvider)) {
        mMasterProvider.start();
      }
    }
  }

  @Override
  public void stop() {
    if (mMasterProvider != null) {
      try (ClassLoaderContext c = ClassLoaderContext.useClassLoaderFrom(mMasterProvider)) {
        mMasterProvider.stop();
      }
    }
  }

  @Override
  public InodeAttributes getAttributes(String[] pathElements, InodeAttributes inode) {
    if (mMasterProvider == null) {
      return inode;
    }
    // We only check master plugin for overridden attributes. Attributes overridden by UFS
    // should be retrieved through UFS sync.
    try (ClassLoaderContext c = ClassLoaderContext.useClassLoaderFrom(mMasterProvider)) {
      return mMasterProvider.getAttributes(pathElements, inode);
    }
  }

  @Override
  public AccessControlEnforcer getExternalAccessControlEnforcer(
      AccessControlEnforcer defaultEnforcer) {
    return new ExtensionAccessControlEnforcer(defaultEnforcer);
  }

  private final class ExtensionAccessControlEnforcer implements AccessControlEnforcer {
    private final AccessControlEnforcer mDefaultEnforcer;
    private final AccessControlEnforcer mExternalMasterEnforcer;

    /**
     * @param defaultEnforcer original access control enforcer
     */
    public ExtensionAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
      mDefaultEnforcer = defaultEnforcer;
      mExternalMasterEnforcer = mMasterProvider == null ? null :
          mMasterProvider.getExternalAccessControlEnforcer(defaultEnforcer);
    }

    /**
     * There are 3 types of AccessControlEnforcer involved in checking permissions on a path:
     *
     * 1. Alluxio default permission checker based on POSIX/ACL(Assuming UFS metadata are in sync).
     * 2. plugin for Alluxio URLs, if enabled.
     * 3. plugin for UFS URLs, if enabled.
     *
     * The workflow for checking permissions would be roughly as follows:
     *
     * 1. If authorization plugin is enabled on Alluxio master, then first check permission with the
     *    master plugin, throw AccessControlException if the access is denied.
     * 2. Else if authorization plugin is enabled on UFS, then check permission with the UFS plugin,
     *    throw AccessControlException if the access is denied.
     * 3. If permission is not determined by either plugin, the built-in permission checker is used.
     *
     * There are some extra workflow for a permission check on a file in a nested mount. If a path
     * is under a nested mount, after checking the plugin for Alluxio master, the UFS plugin
     * for the top most mount point(root mount, if configured) is checked for execution permission
     * on the parent of the nested mount point, and if passed, the check will move to the next mount
     * point in path, until the target path is reached. For any mount point, if the plugins for
     * both Alluxio master and UFS are not enabled, the execution permission is checked on the
     * default Alluxio permission checker.
     *
     * For example,
     *
     * If Alluxio root UFS address is xxx://host1/u/v, there is a second mount point at "/a/b/c" for
     * UFS path "xxx://host2/x/y", to check write permission for "/a/b/c/d/e":
     *
     * - If Alluxio master plugin is configured, then master plugin will be checked for write
     *   permission on /a/b/c/d/e.
     * - If Alluxio master plugin and root UFS plugin are configured, then master plugin will be
     *   checked first, then root UFS plugin is checked for execute permission on "/u/v/a/b".
     * - If Alluxio master plugin and both UFS plugins are configured, then master plugin will be
     *   checked first, then root UFS plugin is checked for execute permission on "/u/v/a/b",
     *   and then secondary UFS plugin is checked for write permission on "/x/y/d/e".
     * - If only secondary UFS plugins is configured, then default(POSIX/ACL) permission is checked
     *   for execute permission on "/a/b", and then secondary UFS plugin is checked for write
     *   permission on "/x/y/d/e".
     * - If only root UFS plugins is configured, then root UFS plugin is checked for execute
     *   permission on "/u/v/a/b", and then default(POSIX/ACL) permission is checked
     *   for write permission on "/a/b/c/d/e" with "/a/b" bypassed.
     */
    @Override
    public void checkPermission(String user, List<String> groups, Mode.Bits bits, String path,
        List<InodeView> inodeList, List<InodeAttributes> attributes, boolean checkIsOwner)
        throws AccessControlException {
      LOG.debug("Checking external permission for ExtensionAccessControlEnforcer");
      // checks permission with master plugin
      if (mExternalMasterEnforcer != null) {
        try (ClassLoaderContext c = ClassLoaderContext.useClassLoaderFrom(mMasterProvider)) {
          LOG.debug("Checking external permission for Alluxio location {}, bits {}", path, bits);
          mExternalMasterEnforcer.checkPermission(user, groups, bits, path, inodeList, attributes,
              checkIsOwner);
          LOG.debug("Passed external permission check for Alluxio location {}, bits {}",
              path, bits);
        } catch (AccessControlException e) {
          throw new AccessControlException("Permission denied by authorization plugin: "
              + e.getMessage(), e);
        }
      }
      // checks permission with UFS plugins if available
      AlluxioURI uri = new AlluxioURI(path);
      try {
        List<AlluxioURI> parentsUris = findMountPointParentUris(uri);
        // check permission for all nested UFS mount on the path.
        for (AlluxioURI parentUri : parentsUris) {
          checkUfsPermission(user, groups, Mode.Bits.EXECUTE, parentUri.getPath(), inodeList,
              attributes, false);
        }
        checkUfsPermission(user, groups, bits, path, inodeList, attributes, checkIsOwner);
      } catch (InvalidPathException e) {
        LOG.error("Cannot resolve UFS path: {}", e.getMessage());
      }
    }

    private void checkUfsPermission(String user, List<String> groups, Mode.Bits bits, String path,
        List<InodeView> inodeList, List<InodeAttributes> attributes, boolean checkIsOwner)
        throws InvalidPathException, AccessControlException {
      AlluxioURI alluxioUri = new AlluxioURI(path);
      AlluxioURI mountPoint = new AlluxioURI(mMountTable.getMountPoint(alluxioUri));
      MountTable.Resolution resolution = mMountTable.resolve(alluxioUri);
      AlluxioURI ufsUri = resolution.getUri();
      InodeAttributesProvider ufsAuthProvider = getUfsProvider(mMountTable, alluxioUri);
      if (ufsAuthProvider != null) {
        if (isSetMountOrGlobalConf(
            resolution, TxPropertyKey.SECURITY_AUTHORIZATION_PLUGINS_EXTERNAL_UFS_NAMESPACE_ENABLED)
            && getMountOrGlobalConfBoolean(resolution,
            TxPropertyKey.SECURITY_AUTHORIZATION_PLUGINS_EXTERNAL_UFS_NAMESPACE_ENABLED)) {
          ufsUri = convertToExternalUfsNameSpace(ufsUri);
        }
        // converts inodes and inode attributes to match ufs paths
        List<InodeView> ufsNodes = convertToUfsInodeList(alluxioUri, mountPoint, inodeList,
            PassThroughInode::new, resolution);
        List<InodeAttributes> ufsAttributes = convertToUfsInodeList(alluxioUri, mountPoint,
            attributes, PassThroughInode::new, resolution);
        LOG.debug("Checking external permission for UFS location {}, bits {}", ufsUri, bits);
        try (ClassLoaderContext c = ClassLoaderContext.useClassLoaderFrom(ufsAuthProvider)) {
          AccessControlEnforcer ufsEnforcer = ufsAuthProvider.getExternalAccessControlEnforcer(
              mDefaultEnforcer);
          ufsEnforcer.checkPermission(user, groups, bits, ufsUri.getPath(), ufsNodes, ufsAttributes,
              checkIsOwner);
        } catch (AccessControlException e) {
          throw new AccessControlException(String.format("Permission denied on %s(%s) by under "
              + "filesystem authorization plugin: %s", path, ufsUri.toString(), e.getMessage()), e);
        }
        LOG.debug("Passed external permission check for UFS location {}, bits {}", ufsUri, bits);
      } else if (mExternalMasterEnforcer == null) {
        // checks permission with default enforcer if master plugin is not available
        List<InodeView> partialNodes = convertToMountPointInodeList(alluxioUri, mountPoint,
            inodeList, PassThroughInode::new);
        List<InodeAttributes> partialAttrs = convertToMountPointInodeList(alluxioUri, mountPoint,
            attributes, PassThroughInode::new);
        LOG.debug("Checking default permission for {}, bits {}", ufsUri, bits);
        mDefaultEnforcer.checkPermission(user, groups, bits, path, partialNodes, partialAttrs,
            checkIsOwner);
        LOG.debug("Passed default permission check for {}, bits {}", ufsUri, bits);
      }
    }

    /**
     * Checks if the mount properties or global configuration contains a value for the given key.
     * @param resolution the mount table resolution of the path
     * @param key the key to check
     * @return true if there is value for the key, false otherwise
     */
    private boolean isSetMountOrGlobalConf(MountTable.Resolution resolution, PropertyKey key) {
      MountInfo mountInfo = mMountTable.getMountInfo(resolution.getMountId());
      Preconditions.checkNotNull(mountInfo, "checkUfsPermission");
      return mountInfo.getOptions().getPropertiesMap().containsKey(key.toString())
          || ServerConfiguration.isSet(key);
    }

    /**
     * Get the value of the value specified by Mount, if you don't exist, get value in Global.
     * @param resolution the mount table resolution of the path
     * @return the properties backing this configuration
     * @throws RuntimeException if value is not set in both properties and
     *  global configuration for the given key
     */
    private boolean getMountOrGlobalConfBoolean(MountTable.Resolution resolution, PropertyKey key) {
      MountInfo mountInfo = mMountTable.getMountInfo(resolution.getMountId());
      Preconditions.checkNotNull(mountInfo, "checkUfsPermission");
      if (mountInfo.getOptions().getPropertiesMap().containsKey(key.toString())) {
        return FormatUtils.parseBoolean(
            mountInfo.getOptions().getPropertiesMap().get(key.toString()));
      } else {
        return ServerConfiguration.global().getBoolean(key);
      }
    }

    /**
     * Add external namespaces for the AlluxioURI.
     * @param alluxioURI an Alluxio path contain authority
     * @return a new Alluxio path containing UFS external namespace
     * @throws InvalidPathException if the given path is invalid
     */
    private AlluxioURI convertToExternalUfsNameSpace(AlluxioURI alluxioURI)
        throws InvalidPathException {
      Preconditions.checkArgument(alluxioURI.getScheme() != null,
          "convertToExternalUfsUri Scheme is null");
      LOG.debug("convertToExternalUfsNameSpace before {}", alluxioURI);
      String ozonePrefixes =
          ServerConfiguration.global().getString(TxPropertyKey.UNDERFS_OZONE_PREFIXES);
      if (ozonePrefixes.startsWith(alluxioURI.getScheme())) {
        // o3fs://bucket.volume/key convert to o3fs://bucket.volume/volume/bucket/key.
        // o3fs://bucket.volume.tdw/key convert to o3fs://bucket.volume.tdw/volume/bucket/key.
        // o3fs://bucket.volume.1.1.1.1:9999/key convert to
        //     o3fs://bucket.volume.1.1.1.1:9999/volume/bucket/key.
        String authority = alluxioURI.getAuthority().toString();
        String[] authorities = authority.split("\\.");
        if (authorities.length < 2) {
          throw new InvalidPathException(String
              .format("The given authority %s doesn't have two or more components", authority));
        }
        String bucket = authorities[0];
        String volume = authorities[1];
        String basePath = alluxioURI.getPath();
        AlluxioURI newAlluxioURI = new AlluxioURI(alluxioURI, PathUtils.concatPath(
            Constants.ROOT_PATH, volume, bucket, basePath), false);
        LOG.debug("convertToExternalUfsNameSpace after {}", newAlluxioURI);
        return newAlluxioURI;
      }

      return alluxioURI;
    }

    /**
     * Converts Alluxio inode list to corresponding UFS inode list to use with UFS
     * AccessControlEnforcer. The returned list will include existing Alluxio inodes for path
     * components that exist in Alluxio, and PassThroughInode for path components that are only
     * in UFS.
     *
     * @param alluxioUri an Alluxio path
     * @param mountPoint the Alluxio path of the corresponding mount point
     * @param inodes a list of Inode corresponding to the Alluxio path
     * @param createPassThroughFunc a function that creates a stub inode given name and base inode
     * @param resolution the mount table resolution of the path
     * @return a new list of inodes corresponding to the UFS path
     * @throws InvalidPathException if the given path is invalid
     */
    private <T> List<T> convertToUfsInodeList(AlluxioURI alluxioUri, AlluxioURI mountPoint,
        List<T> inodes, BiFunction<String, T, T> createPassThroughFunc,
        MountTable.Resolution resolution) throws InvalidPathException {
      AlluxioURI ufsUrl = resolution.getUri();
      if (isSetMountOrGlobalConf(
          resolution, TxPropertyKey.SECURITY_AUTHORIZATION_PLUGINS_EXTERNAL_UFS_NAMESPACE_ENABLED)
          && getMountOrGlobalConfBoolean(resolution,
          TxPropertyKey.SECURITY_AUTHORIZATION_PLUGINS_EXTERNAL_UFS_NAMESPACE_ENABLED)) {
        ufsUrl = convertToExternalUfsNameSpace(ufsUrl);
      }
      String[] ufsPathParts = PathUtils.getPathComponents(ufsUrl.getPath());
      String[] alluxioMountPathParts = PathUtils.getPathComponents(mountPoint.getPath());
      String[] alluxioPathParts = PathUtils.getPathComponents(alluxioUri.getPath());
      int alluxioMountPointIndex = alluxioMountPathParts.length - 1;
      int alluxioPathOffset = alluxioPathParts.length - ufsPathParts.length;
      List<T> newList = new ArrayList<>();
      for (int i = 0; i < ufsPathParts.length; i++) {
        int alluxioPathIndex = i + alluxioPathOffset;
        if (alluxioPathIndex >= inodes.size()) {
          break;
        }
        // Leverages Alluxio inode attributes for part of the path that exists in Alluxio.
        // This includes mount point inode which inherits attributes from under file system.
        T alluxioNode = (alluxioPathIndex >= alluxioMountPointIndex)
            ? inodes.get(alluxioPathIndex) : null;
        // Creates stub inodes for part of the path that differs from Alluxio mount.
        // This includes the mount point inode which gets its name from under file system path
        // but also has attributes come from Alluxio inode
        T ufsNode = (alluxioPathIndex <= alluxioMountPointIndex)
            ? createPassThroughFunc.apply(ufsPathParts[i], alluxioNode) : alluxioNode;
        newList.add(ufsNode);
      }
      return newList;
    }

    /**
     * Converts Alluxio full inode list to inode list for permission checking with a particular
     * mount point. The returned list will consist of existing Alluxio inodes for
     * path components that are part of the mount point, and PassThroughInode for other path
     * components.
     *
     * @param alluxioUri an Alluixo path
     * @param inodes a list of Inode corresponding to the path
     * @return a new list of inodes
     * @throws InvalidPathException if the given path is invalid
     */
    private <T> List<T> convertToMountPointInodeList(AlluxioURI alluxioUri, AlluxioURI mountPoint,
        List<T> inodes, Function<String, T> createPassThroughFunc) throws InvalidPathException {
      String[] alluxioUriParts = PathUtils.getPathComponents(alluxioUri.getPath());
      int mountDepth = mountPoint.getDepth();
      int targetDepth = alluxioUri.getDepth();
      List<T> newList = new ArrayList<>();
      for (int i = 0; i <= targetDepth; i++) {
        if (i < mountDepth) {
          newList.add(createPassThroughFunc.apply(alluxioUriParts[i]));
        } else if (i < inodes.size()) {
          newList.add(inodes.get(i));
        }
      }
      return newList;
    }

    /**
     * Finds parent path of each nested mount point in given path. For example, if
     * Alluxio has nested mount points /a/b and /a/b/c/d, findMountPointParentUris("/a/b/c/d/e")
     * returns ["/a", "/a/b/c"].
     *
     * @param alluxioPathUri an Alluxio path
     * @return a list of parent URIs for the nested mount points in the given path
     * @throws InvalidPathException if the given path is invalid
     */
    private List<AlluxioURI> findMountPointParentUris(AlluxioURI alluxioPathUri)
        throws InvalidPathException {
      AlluxioURI mountPoint = new AlluxioURI(mMountTable.getMountPoint(alluxioPathUri));
      List<AlluxioURI> parents = new ArrayList<>();
      while (!mountPoint.isRoot()) {
        AlluxioURI parent = mountPoint.getParent();
        parents.add(0, parent);
        mountPoint = new AlluxioURI(mMountTable.getMountPoint(parent));
      }
      return parents;
    }

    private InodeAttributesProvider getUfsProvider(MountTable table, AlluxioURI path)
        throws InvalidPathException {
      Closeable ufsAuthProvider = table.getService(path, InodeAttributesProvider.class);
      LOG.debug("external inode attribute provider getUfsProvider for mountTable getService the "
          + "param path is {} , InodeAttributesProvider class is {}",
          path, InodeAttributesProvider.class);
      if (ufsAuthProvider != null) {
        try (ClassLoaderContext c = ClassLoaderContext.useClassLoaderFrom(ufsAuthProvider)) {
          return (InodeAttributesProvider) ufsAuthProvider;
        }
      }
      return null;
    }
  }

  // Represents an Inode not in Alluxio with some attributes populated from Alluxio Inode.
  // It's permission will be 777 if a base inode is not given.
  private class PassThroughInode extends MutableInode<PassThroughInode> implements InodeAttributes {
    private static final long STUB_INODE_ID = -1;

    public PassThroughInode(String name) {
      super(STUB_INODE_ID, true);
      setName(name);
      setMode((short) 0777);
    }

    public PassThroughInode(String name, @Nullable InodeView baseNode) {
      this(name);
      if (baseNode != null) {
        setOwner(baseNode.getOwner());
        setGroup(baseNode.getGroup());
        setMode(baseNode.getMode());
        setLastModificationTimeMs(baseNode.getLastModificationTimeMs());
      }
    }

    public PassThroughInode(String name, @Nullable InodeAttributes baseAttr) {
      this(name);
      if (baseAttr != null) {
        setOwner(baseAttr.getOwner());
        setGroup(baseAttr.getGroup());
        setMode(baseAttr.getMode());
        setLastModificationTimeMs(baseAttr.getLastModificationTimeMs());
      }
    }

    @Override
    public DefaultAccessControlList getDefaultACL() throws UnsupportedOperationException {
      // TODO(feng): implement ACL support for plugin interface
      return new DefaultAccessControlList();
    }

    @Override
    public PassThroughInode setDefaultACL(DefaultAccessControlList acl)
        throws UnsupportedOperationException {
      throw new UnsupportedOperationException(
          "Setting default ACL on inodes not managed by Alluxio is not supported!");
    }

    @Override
    public FileInfo generateClientFileInfo(String path) {
      return null;
    }

    @Override
    public InodeMeta.Inode toProto() {
      return super.toProtoBuilder().build();
    }

    @Override
    protected PassThroughInode getThis() {
      return this;
    }

    @Override
    public Journal.JournalEntry toJournalEntry(String path) {
      throw new UnsupportedOperationException("PassThroughInode should not be journaled.");
    }

    @Override
    public Journal.JournalEntry toJournalEntry() {
      throw new UnsupportedOperationException("PassThroughInode should not be journaled.");
    }

    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("name", getName())
          .add("lastModificationTimeMs", getLastModificationTimeMs())
          .add("owner", getOwner())
          .add("group", getGroup())
          .add("permission", getMode())
          .toString();
    }
  }
}
