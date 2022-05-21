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

package alluxio.security.authorization.chdfs;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.master.file.AccessControlEnforcer;
import alluxio.master.file.InodeAttributesProvider;
import alluxio.master.file.meta.InodeAttributes;
import alluxio.master.file.meta.InodeView;
import alluxio.security.authorization.AuthorizationPluginConstants;
import alluxio.security.authorization.Mode;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.cosn.ranger.auth.DefaultStsUserAuth;
import org.apache.hadoop.fs.cosn.ranger.auth.StsUserAuth;
import org.apache.hadoop.fs.cosn.ranger.ranger.RangerAuthorizer;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.AccessType;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.PermissionRequest;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.ServiceType;
import org.apache.hadoop.fs.cosn.ranger.status.StatusExporter;
import org.apache.hadoop.fs.cosranger.common.CosRangerUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.security.AlluxioUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ChdfsInodeAttributesProvider implements InodeAttributesProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ChdfsInodeAttributesProvider.class);
  private final RangerAuthorizer mChdfsAuthorizer;
  private final String mOfsMountPoint;
  private final StsUserAuth mStsUserAuth = new DefaultStsUserAuth();
  private final int mChdfsPermissionMaxRetry;
  private final Path mWorkingDir;

  /**
   * Default constructor for Alluxio master to create {@link ChdfsInodeAttributesProvider} instance.
   * @param uri Default constructor for Alluxio master to get chdfs fileSystem mountPoint
   */
  public ChdfsInodeAttributesProvider(URI uri) {
    mOfsMountPoint = uri.getHost().substring(0, uri.getHost().indexOf("."));
    mWorkingDir = new Path("/user", System.getProperty("user.name")).makeQualified(uri,
            getWorkingDirectory());
    mChdfsPermissionMaxRetry = ServerConfiguration.getInt(
              TxPropertyKey.ALLUXIO_COS_QCLOUD_OBJECT_STORAGE_PERMISSION_CHECK_MAX_RETRY_KEY);

    mChdfsAuthorizer = CosRangerUtils.buildAndInitCosRanger();
  }

  @Override
  public void start() {
    LOG.info("Starting CHDFS INodeAttributesProvider: {}, version {}",
            mChdfsAuthorizer.getClass().getName(), AuthorizationPluginConstants.AUTH_VERSION);
    startChdfsRangerAuthorizer();
  }

  @Override
  public void stop() {
    LOG.info("Stopping CHDFS INodeAttributesProvider: {}, version {}",
            mChdfsAuthorizer.getClass().getName(), AuthorizationPluginConstants.AUTH_VERSION);
    stopChdfsRangerAuthorizer();
  }

  @Override
  public InodeAttributes getAttributes(String[] pathElements, InodeAttributes inode) {
    return null;
  }

  @Override
  public AccessControlEnforcer getExternalAccessControlEnforcer(
            AccessControlEnforcer defaultEnforcer) {
    return new ChdfsAccessControlEnforcer(defaultEnforcer);
  }

  private class ChdfsAccessControlEnforcer implements AccessControlEnforcer {
    private final INodeAttributeProvider.AccessControlEnforcer mChdfsAccessControlEnforcer;

    public ChdfsAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
      mChdfsAccessControlEnforcer = new AlluxioChdfsAccessControlEnforcer(defaultEnforcer);
    }

    @Override
    public void checkPermission(String user, List<String> groups, Mode.Bits bits,
                                String path, List<InodeView> inodeList,
                                List<InodeAttributes> attributes,
                                boolean checkIsOwner) throws AccessControlException {
      FsAction access = bits == null ? FsAction.WRITE : FsAction.getFsAction(bits.toString());
      if (access.toString().equals("EXECUTE")) {
        return;
      }
      Path mRequestPath = new Path(path);
      Path absolutePath = makeAbsolute(mRequestPath);
      String allowKey = CosRangerUtils.pathToKey(absolutePath);
      AccessType mChdfsRangerAccessType = CosRangerUtils.convertAction(access);
      LOG.debug("mChdfsAccessControlEnforcer: {} Checking external permission for "
                      + "PermissionRequest user: {}, groups: {}, accessType: {}"
                      + ", path: {}, serviceType: {}, fsMountPoint: {}",
              mChdfsAccessControlEnforcer, user, groups, mChdfsRangerAccessType, allowKey,
              ServiceType.CHDFS, mOfsMountPoint);
      PermissionRequest permissionRequest = new PermissionRequest(
              ServiceType.CHDFS, mChdfsRangerAccessType, "", "", mOfsMountPoint, allowKey);
      try {
        boolean checkPermissionRes = chdfsCheckPermission(
                permissionRequest, user, groups);
        if (!checkPermissionRes) {
          throw new AccessControlException(String.format("[key: %s], [user: %s], [operation: %s]",
                  allowKey, user, mChdfsRangerAccessType.name()));
        }
      } catch (AccessControlException e) {
        throw new AccessControlException(e.getMessage(), e);
      }
    }
  }

  public Path getWorkingDirectory() {
    return mWorkingDir;
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(mWorkingDir, path);
  }

  private boolean chdfsCheckPermission(
          PermissionRequest permissionRequest, String user, List<String> groups) {
    UserGroupInformation callerUgi = new AlluxioUserGroupInformation(
            user, groups, ServerConfiguration.getString(PropertyKey.SECURITY_AUTHENTICATION_TYPE));
    boolean allow = false;
    for (int retryIndex = 1; retryIndex <= mChdfsPermissionMaxRetry; ++retryIndex) {
      allow = mChdfsAuthorizer.checkPermission(permissionRequest, callerUgi,
              mStsUserAuth, retryIndex, mChdfsPermissionMaxRetry);
      if (allow) {
        break;
      }
      backOff(retryIndex);
    }
    if (allow) {
      StatusExporter.INSTANCE.increasePermissionAllowCnt();
    } else {
      StatusExporter.INSTANCE.increasePermissionDenyCnt();
    }
    return allow;
  }

  private void backOff(int retryIndex) {
    if (retryIndex < 0) {
      return;
    }

    int leastRetryTime = Math.min(retryIndex * 50, 1500);
    int sleepMs = ThreadLocalRandom.current().nextInt(leastRetryTime, 2000);
    try {
      Thread.sleep(sleepMs);
    } catch (InterruptedException e) {
      LOG.debug("backoff is interrupted!", e);
    }
  }

  private void startChdfsRangerAuthorizer() {
    synchronized (mChdfsAuthorizer) {
      mChdfsAuthorizer.init();
    }
  }

  private void stopChdfsRangerAuthorizer() {
    synchronized (mChdfsAuthorizer) {
      mChdfsAuthorizer.stop();
    }
  }

  private class AlluxioChdfsAccessControlEnforcer
            implements INodeAttributeProvider.AccessControlEnforcer {
    private final AccessControlEnforcer mAccessPermissionEnforcer;

    public AlluxioChdfsAccessControlEnforcer(AccessControlEnforcer ace) {
      mAccessPermissionEnforcer = ace;
    }

    @Override
    public void checkPermission(String fsOwner, String superGroup, UserGroupInformation callerUgi,
                                INodeAttributes[] inodeAttrs, INode[] inodes,
                                byte[][] pathByNameArr, int snapshotId, String path,
                                int ancestorIndex, boolean doCheckOwner,
                                FsAction ancestorAccess, FsAction parentAccess, FsAction access,
                                FsAction subAccess, boolean ignoreEmptyDir) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(new StringBuilder().append("AlluxioChdfsAccessControlEnforcer.checkPermission(")
                .append(" mAccessPermissionEnforcer=").append(mAccessPermissionEnforcer)
                .append(" fsOwner=").append(fsOwner)
                .append(" superGroup=").append(superGroup)
                .append(" inodesCount=").append(inodes != null ? inodes.length : 0)
                .append(" snapshotId=").append(snapshotId)
                .append(" user=").append(callerUgi.getUserName())
                .append(" path=").append(path)
                .append(" ancestorIndex=").append(ancestorIndex)
                .append(" doCheckOwner=").append(doCheckOwner)
                .append(" ancestorAccess=").append(ancestorAccess)
                .append(" parentAccess=").append(parentAccess)
                .append(" access=").append(access)
                .append(" subAccess=").append(subAccess)
                .append(" ignoreEmptyDir=").append(ignoreEmptyDir)
                .append(")").toString());
      }
    }
  }
}
