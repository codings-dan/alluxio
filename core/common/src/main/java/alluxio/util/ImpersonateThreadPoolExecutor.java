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

package alluxio.util;

import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This executor passes impersonation information to the real worker thread.
 * The proxy user is tracked by {@link AuthenticatedClientUser#sUserThreadLocal}.
 * This executor delegates operations to the underlying executor while setting the
 * ThreadLocal context for execution.
 * */
public class ImpersonateThreadPoolExecutor extends AbstractExecutorService {
  private final ThreadPoolExecutor mDelegate;

  public ImpersonateThreadPoolExecutor(ThreadPoolExecutor service) {
    mDelegate = service;
  }

  @Override
  public void execute(final Runnable command) {
    // If there's no impersonation, proxyUser is just null
    User proxyUser = AuthenticatedClientUser.getOrNull();
    mDelegate.execute(() -> {
      try {
        AuthenticatedClientUser.set(proxyUser);
        command.run();
      } finally {
        AuthenticatedClientUser.remove();
      }
    });
  }

  public ExecutorService getExecutor() {
    return mDelegate;
  }

  @Override
  public void shutdown() {
    mDelegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return mDelegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return mDelegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return mDelegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return mDelegate.awaitTermination(timeout, unit);
  }

  public void allowCoreThreadTimeOut(boolean value) {
    mDelegate.allowCoreThreadTimeOut(value);
  }

  public BlockingQueue<Runnable> getQueue() {
    return mDelegate.getQueue();
  }
}
