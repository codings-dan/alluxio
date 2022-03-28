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
import alluxio.Constants;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.MountPOptions;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.MountTable;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Auto mount if the resolve unsuccessfully.
 */
public class AutoMountUriTranslator implements UriTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(
      AutoMountUriTranslator.class);

  private FileSystemMaster mMaster;
  private MountTable mMountTable;
  private InodeTree mInodeTree;

  public AutoMountUriTranslator(FileSystemMaster master,
      MountTable mountTable, InodeTree inodeTree) {
    mMaster = master;
    mMountTable = mountTable;
    mInodeTree = inodeTree;
  }

  @Override
  public AlluxioURI translateUri(String uriStr) throws InvalidPathException {
    AlluxioURI uri = new AlluxioURI(uriStr);
    // Scheme-less URIs are regarded as Alluxio URI.
    if (uri.getScheme() == null || uri.getScheme().equals(Constants.SCHEME)) {
      return uri;
    }

    // Reverse lookup mount table to find mount point that contains the path.
    MountTable.ReverseResolution resolution = mMountTable.reverseResolve(uri);
    if (resolution != null) {
      // Before returning the resolved path, make sure mount point does not contain
      // nested mounts as translation on one mount should not give visibility to another.
      if (mMountTable.containsMountPoint(resolution.getMountInfo().getAlluxioUri(), false)
          && !mMountTable.getMountPoint(resolution.getUri()).equals(
          resolution.getMountInfo().getAlluxioUri().getPath())) {
        throw new InvalidPathException(
            String.format(
                "Foreign URI: %s is reverse resolved to a mount: %s which has nested mounts.",
                uriStr, resolution.getMountInfo().getAlluxioUri()));
      }
      return resolution.getUri();
    }

    // Resolve unsuccessful
    if (!ServerConfiguration.getBoolean(TxPropertyKey.MASTER_SHIMFS_AUTO_MOUNT_ENABLED)) {
      throw new InvalidPathException(
          String.format("Could not reverse resolve ufs path: %s", uriStr));
    }

    // Attempt to auto-mount.
    try {
      autoMountUfsForUri(uri);
    } catch (Exception e) {
      throw new InvalidPathException(
          String.format("Failed to translate and auto-mount path: %s", uriStr), e);
    }
    // Try translating the URI after auto-mount.
    resolution = mMountTable.reverseResolve(uri);
    if (resolution == null) {
      throw new InvalidPathException(
          String.format("Could not reverse resolve path: %s after auto-mount.", uriStr));
    }
    return resolution.getUri();
  }

  @SuppressFBWarnings({"RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"})
  private synchronized void autoMountUfsForUri(AlluxioURI ufsUri)
      throws AlluxioException, IOException {
    // Check if an auto-mount has been already made that resolves this URI.
    try {
      mMountTable.reverseResolve(ufsUri).getUri();
      // reverse lookup succeeded. No need to auto-mount.
      LOG.debug("No need to do auto-mount for: {}", ufsUri);
      return;
    } catch (Exception e) {
      // Continue auto-mounting.
    }
    // Generate highest Alluxio mount root for given path.
    // The highest mount root will be URIs "scheme/authority" under the configured auto-mount root.
    // This allows same authorities under different schemes to be auto-mounted correctly.
    String alluxioMountRoot =
        PathUtils.concatPath(ServerConfiguration.get(TxPropertyKey.MASTER_SHIMFS_AUTO_MOUNT_ROOT),
            ufsUri.getScheme(), ufsUri.getAuthority());
    // Read default options for auto-mounted UFSes.
    boolean mountReadonly =
        ServerConfiguration.getBoolean(TxPropertyKey.MASTER_SHIMFS_AUTO_MOUNT_READONLY);
    boolean mountShared =
        ServerConfiguration.getBoolean(TxPropertyKey.MASTER_SHIMFS_AUTO_MOUNT_SHARED);
    Map<String, String> mountConf =
        ServerConfiguration.getNestedProperties(TxPropertyKey.MASTER_SHIMFS_AUTO_MOUNT_OPTION);
    // Try mounting UFS to Alluxio starting from the ufs root.
    int pathComponentIndex = 0;
    String currentPathComponent;
    // Used to keep track of first directory that is created for this auto-mount call.
    // It'll be deleted recursively if this mount fails.
    AlluxioURI firstCreatedParent = null;
    try {
      MountContext mountCtx = MountContext
          .mergeFrom(MountPOptions.newBuilder().setReadOnly(mountReadonly)
              .setShared(mountShared).putAllProperties(mountConf));
      while ((currentPathComponent = ufsUri.getLeadingPath(pathComponentIndex++)) != null) {
        // Generate current Alluxio mount root.
        AlluxioURI currentAlluxioRootUri =
            new AlluxioURI(
                PathUtils.concatPath(alluxioMountRoot, currentPathComponent));
        // Generate current UFS mount root.
        AlluxioURI currentUfsRootUri =
            new AlluxioURI(ufsUri.getScheme(), ufsUri.getAuthority(), currentPathComponent);
        try {
          // Ensure Alluxio parent path exist before mounting.
          AlluxioURI currentRootParentUri = currentAlluxioRootUri.getParent();
          if (!mInodeTree.inodePathExists(currentRootParentUri)) {
            mMaster.createDirectory(currentRootParentUri, CreateDirectoryContext
                .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
            if (firstCreatedParent == null) {
              // Track first created dir while preparing to mount.
              firstCreatedParent = currentRootParentUri;
            }
          }
          // Mount current highest path of UFS to Alluxio.
          mMaster.mount(currentAlluxioRootUri, currentUfsRootUri, mountCtx);
          LOG.info("Auto-mounted UFS path: {} to Alluxio path: {}", currentUfsRootUri,
              currentAlluxioRootUri);
          return;
        } catch (AccessControlException | InvalidPathException e) {
          LOG.warn("Failed to auto-mount UFS path: {} to Alluxio path: {}.",
              currentAlluxioRootUri, currentUfsRootUri, e);
          continue;
        }
      }
      // Could not mount any sub-path.
      throw new IOException(String.format("Failed to auto-mount path: %s", ufsUri));
    } catch (AlluxioException | IOException e) {
      // Cleanup before failing.
      if (firstCreatedParent != null) {
        try {
          mMaster.delete(firstCreatedParent,
              DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
        } catch (Exception e1) {
          LOG.warn("Failed to clean-up created directory root: {}", firstCreatedParent, e1);
        }
      }
      throw e;
    }
  }
}
