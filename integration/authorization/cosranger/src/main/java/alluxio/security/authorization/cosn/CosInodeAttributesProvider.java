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

public class CosInodeAttributesProvider implements InodeAttributesProvider {
  private static final Logger LOG = LoggerFactory.getLogger(CosInodeAttributesProvider.class);
  private final RangerAuthorizer mCosAuthorizer;
  private final String mCosBucket;
  private final StsUserAuth mStsUserAuth = new DefaultStsUserAuth();
  private final int mCosPermissionMaxRetry;

  /**
   * Default constructor for Alluxio master to create {@link CosInodeAttributesProvider} instance.
   * @param uri Default constructor for Alluxio master to get cosBucket
   */
  public CosInodeAttributesProvider(URI uri) {
    mCosAuthorizer = CosRangerUtils.buildAndInitCosRanger();
    mCosBucket = uri.getHost();
    mCosPermissionMaxRetry = Integer.parseInt(ServerConfiguration
        .getString(TxPropertyKey.ALLUXIO_COS_QCLOUD_OBJECT_STORAGE_PERMISSION_CHECK_MAX_RETRY_KEY));
  }

  @Override
  public void start() {
    LOG.info("Starting Cos INodeAttributesProvider: {}, version {}",
        mCosAuthorizer.getClass().getName(), AuthorizationPluginConstants.AUTH_VERSION);
    startCosAuthorizer();
  }

  @Override
  public void stop() {
    LOG.info("Stopping Cos INodeAttributesProvider: {}, version {}",
        mCosAuthorizer.getClass().getName(), AuthorizationPluginConstants.AUTH_VERSION);
    stopCosAuthorizer();
  }

  @Override
  public InodeAttributes getAttributes(String[] pathElements, InodeAttributes inode) {
    return null;
  }

  @Override
  public AccessControlEnforcer getExternalAccessControlEnforcer(
      AccessControlEnforcer defaultEnforcer) {
    return new CosAccessControlEnforcer(defaultEnforcer);
  }

  private class CosAccessControlEnforcer implements AccessControlEnforcer {
    private final INodeAttributeProvider.AccessControlEnforcer mCosAccessControlEnforcer;

    public CosAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
      mCosAccessControlEnforcer = new AlluxioCosAccessControlEnforcer(defaultEnforcer);
    }

    @Override
    public void checkPermission(String user, List<String> groups, Mode.Bits bits, String path,
        List<InodeView> inodeList, List<InodeAttributes> attributes, boolean checkIsOwner)
        throws AccessControlException {
      LOG.debug("Checking external permission for CosInodeAttributesProvider.checkPermission");
      FsAction access = bits == null ? FsAction.WRITE : FsAction.getFsAction(bits.toString());
      if (access.toString().equals("EXECUTE")) {
        return;
      }
      String allowKey = path.substring(1);
      AccessType mCosRangerAccessType = CosRangerUtils.convertAction(access);
      LOG.debug(
          "mCosAccessControlEnforcer:{} Checking external permission for"
              + "CosInodeAttributesProvider.checkPermission "
              + "for PermissionRequest user: {}, groups: {}, accessType: {}, allowKey: {}, "
              + "serviceType: {}, bucket: {}",
          mCosAccessControlEnforcer, user, groups, mCosRangerAccessType, allowKey, ServiceType.COS,
          mCosBucket);
      PermissionRequest permissionRequest = new PermissionRequest(ServiceType.COS,
          mCosRangerAccessType, mCosBucket, allowKey, "", "");
      try {
        boolean checkPermissionRes = cosCheckPermission(permissionRequest, user, groups);
        if (!checkPermissionRes) {
          throw new AccessControlException(String.format("[key: %s], [user: %s], [operation: %s]",
              allowKey, user, mCosRangerAccessType.name()));
        }
      } catch (AccessControlException e) {
        throw new AccessControlException(e.getMessage(), e);
      }
    }
  }

  private boolean cosCheckPermission(PermissionRequest permissionRequest, String user,
      List<String> groups) {
    UserGroupInformation callerUgi = new AlluxioUserGroupInformation(user, groups,
        ServerConfiguration.getString(PropertyKey.SECURITY_AUTHENTICATION_TYPE));
    boolean allow = false;
    for (int retryIndex = 1; retryIndex <= mCosPermissionMaxRetry; ++retryIndex) {
      allow = mCosAuthorizer.checkPermission(permissionRequest, callerUgi, mStsUserAuth, retryIndex,
          mCosPermissionMaxRetry);
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
      LOG.info("backoff is interrupted!", e);
    }
  }

  private void startCosAuthorizer() {
    synchronized (mCosAuthorizer) {
      mCosAuthorizer.init();
    }
  }

  private void stopCosAuthorizer() {
    synchronized (mCosAuthorizer) {
      mCosAuthorizer.stop();
    }
  }

  private class AlluxioCosAccessControlEnforcer
      implements INodeAttributeProvider.AccessControlEnforcer {
    private final AccessControlEnforcer mAccessPermissionEnforcer;

    public AlluxioCosAccessControlEnforcer(AccessControlEnforcer ace) {
      mAccessPermissionEnforcer = ace;
    }

    @Override
    public void checkPermission(String fsOwner, String superGroup, UserGroupInformation callerUgi,
        INodeAttributes[] inodeAttrs, INode[] inodes, byte[][] pathByNameArr, int snapshotId,
        String path, int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
        FsAction parentAccess, FsAction access, FsAction subAccess, boolean ignoreEmptyDir) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(new StringBuilder().append("AlluxioCosAccessControlEnforcer.checkPermission(")
            .append(" mAccessPermissionEnforcer=").append(mAccessPermissionEnforcer)
            .append(" fsOwner=").append(fsOwner).append(" superGroup=").append(superGroup)
            .append(" inodesCount=").append(inodes != null ? inodes.length : 0)
            .append(" snapshotId=").append(snapshotId).append(" user=")
            .append(callerUgi.getUserName()).append(" path=").append(path).append(" ancestorIndex=")
            .append(ancestorIndex).append(" doCheckOwner=").append(doCheckOwner)
            .append(" ancestorAccess=").append(ancestorAccess).append(" parentAccess=")
            .append(parentAccess).append(" access=").append(access).append(" subAccess=")
            .append(subAccess).append(" ignoreEmptyDir=").append(ignoreEmptyDir).append(")")
            .toString());
      }
    }
  }
}
