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

import alluxio.Constants;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.file.options.DescendantType;
import alluxio.master.CoreMasterContext;
import alluxio.master.audit.AuditContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import com.codahale.metrics.Counter;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class TxFileSystemMaster extends DefaultFileSystemMaster {
  private ThreadPoolExecutor mListStautsExecutor;

  private final Queue<Future<Boolean>> mListInodeJobs;

  public TxFileSystemMaster(BlockMaster blockMaster, CoreMasterContext masterContext) {
    this(blockMaster, masterContext,
        ExecutorServiceFactories.cachedThreadPool(Constants.FILE_SYSTEM_MASTER_NAME));
  }

  public TxFileSystemMaster(BlockMaster blockMaster, CoreMasterContext masterContext,
      ExecutorServiceFactory executorServiceFactory) {
    super(blockMaster, masterContext, executorServiceFactory);
    mListInodeJobs = new LinkedList<>();

    if (ServerConfiguration.getBoolean(TxPropertyKey.MASTER_LIST_CONCURRENT_ENABLED)) {
      mListStautsExecutor = new ThreadPoolExecutor(
          ServerConfiguration.getInt(TxPropertyKey.MASTER_LIST_STATUS_EXECUTOR_POOL_SIZE),
          ServerConfiguration.getInt(TxPropertyKey.MASTER_LIST_STATUS_EXECUTOR_POOL_SIZE),
          1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
          ThreadFactoryUtils.build("alluxio-list-status-%d", false));
      mListStautsExecutor.allowCoreThreadTimeOut(true);
    }
  }

  @Override
  protected void listStatusInternal(ListStatusContext context, RpcContext rpcContext,
      LockedInodePath currInodePath, AuditContext auditContext, DescendantType descendantType,
      ResultStream<FileInfo> resultStream, int depth, Counter counter)
      throws FileDoesNotExistException, UnavailableException,
      AccessControlException, InvalidPathException {
    if (!ServerConfiguration.getBoolean(TxPropertyKey.MASTER_LIST_CONCURRENT_ENABLED)) {
      super.listStatusInternal(context, rpcContext, currInodePath, auditContext, descendantType,
          resultStream, depth, counter);
      return;
    }
    rpcContext.throwIfCancelled();
    Inode inode = currInodePath.getInode();
    // TODO(baoloongmao): impl the filter loading file.
    //    if (context.getOptions().getFilterLoadingFile() &&
    //        currInodePath.getUri().getPath().endsWith(AlluxioURI.TMP_IN_LOADING)) {
    //      return;
    //    }
    if (inode.isDirectory() && descendantType != DescendantType.NONE) {
      try {
        // TODO(david): Return the error message when we do not have permission
        getPermissionChecker().checkPermission(Mode.Bits.EXECUTE, currInodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        if (descendantType == DescendantType.ALL) {
          return;
        } else {
          throw e;
        }
      }

      DescendantType nextDescendantType = (descendantType == DescendantType.ALL)
          ? DescendantType.ALL : DescendantType.NONE;
      String[] parentComponents = null;
      for (Inode child : getInodeStore().getChildren(inode.asDirectory())) {
        if (parentComponents == null) {
          parentComponents = PathUtils.getPathComponents(currInodePath.getUri().getPath());
        }
        String[] childComponentsHint = new String[parentComponents.length + 1];
        System.arraycopy(parentComponents, 0, childComponentsHint, 0, parentComponents.length);
        // TODO(david): Make extending InodePath more efficient
        childComponentsHint[childComponentsHint.length - 1] = child.getName();

        User proxyUser = AuthenticatedClientUser.getOrNull();
        Future<Boolean> job = mListStautsExecutor.submit(() -> {
          AuthenticatedClientUser.set(proxyUser);
          try (LockedInodePath tmp =
                   getInodeTree().lockInodePath(currInodePath.getUri(),
                       currInodePath.getLockPattern())) {
            try (LockedInodePath childInodePath =
                     tmp.lockChild(child, InodeTree.LockPattern.READ, childComponentsHint)) {
              listStatusInternal(context, rpcContext, childInodePath, auditContext,
                  nextDescendantType, resultStream, depth + 1, counter);
              return true;
            }
          }
        });
        mListInodeJobs.offer(job);
      }
    }
    Future<Boolean> job;
    while ((job = mListInodeJobs.poll()) != null) {
      try {
        job.get();
      } catch (InterruptedException | ExecutionException e) {
        mListInodeJobs.forEach(f -> f.cancel(true));
        mListInodeJobs.clear();
        throw new UnavailableException("Concurrent list encountered exception", e);
      }
    }

    if (inode.isFile()) {
      resultStream.submit(getFileInfoInternal(currInodePath));
    }
  }
}
