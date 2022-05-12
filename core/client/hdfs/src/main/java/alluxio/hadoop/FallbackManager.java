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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ServiceType;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.user.UserState;
import alluxio.security.user.UserState.Factory;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FallbackManager {
  private static final Logger LOG = LoggerFactory.getLogger(FallbackManager.class);

  private ReentrantReadWriteLock mStatusLock = new ReentrantReadWriteLock();

  private boolean mNeedFallback;

  private static final int FALLBACK_EXECUTOR_THREAD_DEFAULT_POOL_SIZE = 0;
  private final ScheduledThreadPoolExecutor mExecutorService =
      new ScheduledThreadPoolExecutor(FALLBACK_EXECUTOR_THREAD_DEFAULT_POOL_SIZE,
          ThreadFactoryUtils.build("ShimFs-Fallback-Probe-%d",
              false));

  private long mLazyTimeout = 0L;

  private AlluxioConfiguration mConf = null;

  public FallbackManager(AlluxioConfiguration alluxioConf) {
    mNeedFallback = false;
    mConf = alluxioConf;
    mLazyTimeout = mConf.getMs(TxPropertyKey.USER_LAZY_FALLBACK_TIMEOUT);
    mExecutorService.setKeepAliveTime(mLazyTimeout * 100, TimeUnit.MILLISECONDS);
    mExecutorService.allowCoreThreadTimeOut(true);
  }

  public void setFallbackStatus(boolean needFallback) {
    try {
      mStatusLock.writeLock().lock();
      mNeedFallback = needFallback;
    } finally {
      mStatusLock.writeLock().unlock();
    }
  }

  public boolean needFallbackStatus() {
    try {
      mStatusLock.readLock().lock();
      return mNeedFallback;
    } finally {
      mStatusLock.readLock().unlock();
    }
  }

  public void markFallBack(ShimFileSystem fs) {
    LOG.debug("it will markFallBack, mLazyTimeout:{}", mLazyTimeout);
    setFallbackStatus(true);
    probe(fs);
  }

  //In default Impl, it use Action to trigger timing to probe
  // if need, it can be designed interface to probe
  // it can use thread heartbeat to master, if need more fast to probe.
  public void probe(ShimFileSystem fs) {
    if (!mExecutorService.getQueue().isEmpty()) {
      return;
    }
    mExecutorService.schedule(new Runnable() {
      @Override
      public void run() {
        ServiceType mServiceType = ServiceType.FILE_SYSTEM_MASTER_CLIENT_SERVICE;
        InetSocketAddress connectAddr = NetworkAddressUtils
            .getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC, mConf);
        UserState mUserState = Factory.create(mConf);
        RetryPolicy retry = new ExponentialBackoffRetry(
            (int) mConf.getMs(TxPropertyKey.USER_FALLBACK_RETRY_BASE_SLEEP_MS),
            (int) mConf.getMs(TxPropertyKey.USER_FALLBACK_RETRY_MAX_SLEEP_MS),
            mConf.getInt(TxPropertyKey.USER_FALLBACK_RETRY_MAX_TIMES));
        while (retry.attempt()) {
          try {
            LOG.debug("Checking whether {} is listening for RPCs", connectAddr);
            NetworkAddressUtils.pingService(connectAddr, mServiceType, mConf, mUserState);
            LOG.debug("Successfully connected to {}", connectAddr);
            fs.reInitAlluxioFs(fs.getUri(), fs.getConf());
            setFallbackStatus(false);
          } catch (UnavailableException e) {
            LOG.debug("Failed to connect to {} on attempt #{}", connectAddr,
                retry.getAttemptCount());
            setFallbackStatus(true);
          } catch (IOException e) {
            LOG.warn("Failed to probe to {} on attempt #{}, e:{}", connectAddr,
                retry.getAttemptCount(), e.getStackTrace());
          }
        }
      }
    }, mLazyTimeout, TimeUnit.MILLISECONDS);
  }
}
