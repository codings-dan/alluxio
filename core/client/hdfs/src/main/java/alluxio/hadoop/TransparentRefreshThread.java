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

package alluxio.hadoop;

import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.wire.MountPointInfo;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class TransparentRefreshThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(TransparentRefreshThread.class);
  private volatile boolean mShutdown;
  private final long mFreshCacheMS;
  private final FileSystem mFileSystem;
  private AtomicReference<Map<String, String>> mMountTableCache;

  public TransparentRefreshThread(FileSystem fileSystem, long refreshIntervalMS) {
    mShutdown = false;
    mFreshCacheMS = refreshIntervalMS;
    mFileSystem = fileSystem;
    mMountTableCache = new AtomicReference<>();
  }

  public boolean checkUriNeedTransparent(String path) {
    if (mShutdown) {
      LOG.debug("check with refresh mount table cache thread is down");
      return true;
    }
    Map<String, String> tmpMountPointToUfsUri = mMountTableCache.get();
    if (tmpMountPointToUfsUri == null) {
      return true;
    }
    for (Map.Entry<String, String> entry : tmpMountPointToUfsUri.entrySet()) {
      String uri = entry.getValue();
      if (path.startsWith(uri)) {
        LOG.debug("fit alluxioPath:{} mountPoint:{}", path, uri);
        return false;
      }
    }
    return true;
  }

  public void refreshMountTableCache() throws IOException, AlluxioException {
    Map<String, MountPointInfo> mountTable = mFileSystem.getMountTable();
    Map<String, String> tmpMountPointToUfsUri = new HashMap<>();

    for (Map.Entry<String, MountPointInfo> entry : mountTable.entrySet()) {
      String mMountPoint = entry.getKey();
      String ufsUri = entry.getValue().getUfsUri();
      if (mMountPoint.equals("/")) {
        continue;
      }
      if (!mMountPoint.endsWith("/")) {
        mMountPoint += "/";
      }
      if (!ufsUri.endsWith("/")) {
        ufsUri += "/";
      }
      LOG.debug("Add mountPoint:{} ufsUri:{}", mMountPoint, ufsUri);
      Preconditions.checkArgument(!tmpMountPointToUfsUri.containsKey(mMountPoint),
          String.format("mountPoint %s already exist, mountTable:%s", mMountPoint, mountTable));
      tmpMountPointToUfsUri.put(mMountPoint, ufsUri);
    }
    LOG.debug("Found {} alluxio mountPoint", tmpMountPointToUfsUri.size());
    mMountTableCache.set(tmpMountPointToUfsUri);
  }

  @Override
  public void run() {
    try {
      while (!mShutdown) {
        synchronized (this) {
          try {
            refreshMountTableCache();
          } catch (Exception e) {
            LOG.error("Failed to refresh MountTableCache info", e);
          }
          wait(mFreshCacheMS);
        }
      }
    } catch (InterruptedException e) {
      LOG.error("MountTableCache refresh thread was interrupted", e);
    }
  }

  /**
   * close the thread.
   * */
  public void shutdown() {
    synchronized (this) {
      mShutdown = true;
      notifyAll();
    }
  }
}
