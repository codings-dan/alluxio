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
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.ClientCommand;
import alluxio.grpc.ClientCommandType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.CoreMasterContext;
import alluxio.master.audit.AuditContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.TxMetricKey;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.io.PathUtils;
import alluxio.wire.ClientIdentifier;
import alluxio.wire.ClientInfo;
import alluxio.wire.FileInfo;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public final class TxFileSystemMaster extends DefaultFileSystemMaster {
  private static final Logger LOG = LoggerFactory.getLogger(TxFileSystemMaster.class);

  private ThreadPoolExecutor mListStautsExecutor;

  private final Queue<Future<Boolean>> mListInodeJobs;

  private static final IndexDefinition<ClientInfo, Long> ID_INDEX =
      new IndexDefinition<ClientInfo, Long>(true) {
        @Override
        public Long getFieldValue(ClientInfo o) {
          return o.getId();
        }
      };

  private static final IndexDefinition<ClientInfo, ClientIdentifier> IDENTIFIER_INDEX =
      new IndexDefinition<ClientInfo, ClientIdentifier>(true) {
        @Override
        public ClientIdentifier getFieldValue(ClientInfo o) {
          return o.getClientIdentifier();
        }
      };

  private final IndexedSet<ClientInfo> mClients =
      new IndexedSet<>(ID_INDEX, IDENTIFIER_INDEX);
  private final IndexedSet<ClientInfo> mLostClients =
      new IndexedSet<>(ID_INDEX, IDENTIFIER_INDEX);
  private final IndexedSet<ClientInfo> mTempClients =
      new IndexedSet<>(ID_INDEX, IDENTIFIER_INDEX);

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
    MetricsSystem.registerGaugeIfAbsent(TxMetricKey.CLUSTER_REGISTER_CLIENTS.getName(),
        mClients::size);
  }

  @Override
  public void start(Boolean isPrimary) throws IOException {
    super.start(isPrimary);
    getExecutorService().submit(new HeartbeatThread(
        HeartbeatContext.MASTER_LOST_CLIENT_DETECTION,
        new LostClientDetectionHeartbeatExecutor(),
        (int) ServerConfiguration.getMs(TxPropertyKey.MASTER_LOST_CLIENT_DETECTION_INTERVAL),
        ServerConfiguration.global(), mMasterContext.getUserState()));
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
              super.listStatusInternal(context, rpcContext, childInodePath, auditContext,
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

  @Override
  public ClientCommand commandHeartbeat(long clientId, long metadataCacheSize, long journalId)  {
    ClientInfo client = mClients.getFirstByField(ID_INDEX, clientId);
    ClientCommand.Builder builder = ClientCommand.newBuilder();
    if (client == null) {
      LOG.warn("Master Can't find the client with id {} ", clientId);
      return builder.setClientCommandType(ClientCommandType.CLIENT_REGISTER).build();
    }
    client.setMetadataCacheSize(metadataCacheSize);
    client.setLastContactMs(System.currentTimeMillis());
    // Get the journal index of the leader node. In most circumstances,
    // the index of each node is same. If they are inconsistent, the largest
    // journal Index belongs to the leader node.
    long id = mMasterContext.getJournalSystem().getCurrentSequenceNumbers()
        .values().stream().max(Long::compare).orElse(0L);
    if (journalId != id) {
      return builder.setClientCommandType(ClientCommandType.CLIENT_CLEAR).setJournalId(id).build();
    } else {
      return builder.setClientCommandType(ClientCommandType.CLIENT_NOTHING).build();
    }
  }

  @Override
  public long getClientId(ClientIdentifier clientIdentifier) {
    ClientInfo existingClients = mClients.getFirstByField(IDENTIFIER_INDEX, clientIdentifier);
    if (existingClients != null) {
      // This client address is already mapped to a client id.
      long oldClientId = existingClients.getId();
      LOG.warn("The client {} already exists as id {}.", clientIdentifier, oldClientId);
      return oldClientId;
    }

    existingClients = findUnregisteredClient(clientIdentifier);
    if (existingClients != null) {
      return existingClients.getId();
    }

    // Generate a new client id.
    long clientId = IdUtils.getRandomNonNegativeLong();
    while (!mTempClients.add(new ClientInfo(clientId, clientIdentifier))) {
      clientId = IdUtils.getRandomNonNegativeLong();
    }

    LOG.info("getClientId(): ClientIdentifier: {} id: {}", clientIdentifier, clientId);
    return clientId;
  }

  /**
   * Find a client which is considered lost or just gets its id.
   * @param clientIdentifier the address used to find a client
   * @return a {@link ClientInfo} which is presented in master but not registered,
   *         or null if not client is found.
   */
  @Nullable
  private ClientInfo findUnregisteredClient(ClientIdentifier clientIdentifier) {
    for (IndexedSet<ClientInfo> clients: Arrays.asList(mTempClients, mLostClients)) {
      ClientInfo client = clients.getFirstByField(IDENTIFIER_INDEX, clientIdentifier);
      if (client != null) {
        return client;
      }
    }
    return null;
  }

  /**
   * Find a client which is considered lost or just gets its id.
   * @param clientId the id used to find a client
   * @return a {@link ClientInfo} which is presented in master but not registered,
   *         or null if not client is found.
   */
  @Nullable
  private ClientInfo findUnregisteredClient(long clientId) {
    for (IndexedSet<ClientInfo> clients: Arrays.asList(mTempClients, mLostClients)) {
      ClientInfo client = clients.getFirstByField(ID_INDEX, clientId);
      if (client != null) {
        return client;
      }
    }
    return null;
  }

  /**
   * Re-register a lost client or complete registration after getting a client id.
   * This method requires no locking on {@link ClientInfo} because it is only
   * reading final fields.
   *
   * @param clientId the client id to register
   */
  @Nullable
  private ClientInfo recordClientRegistration(long clientId) {
    for (IndexedSet<ClientInfo> clients: Arrays.asList(mTempClients, mLostClients)) {
      ClientInfo client = clients.getFirstByField(ID_INDEX, clientId);
      if (client == null) {
        continue;
      }
      mClients.add(client);
      clients.remove(client);
      LOG.warn("A lost client {} has requested its old id {}.",
          client.getClientIdentifier(), client.getId());
      return client;
    }
    return null;
  }

  @Override
  public void clientRegister(long clientId, long startTime)
      throws NotFoundException {
    ClientInfo client = mClients.getFirstByField(ID_INDEX, clientId);
    if (client == null) {
      client = findUnregisteredClient(clientId);
    }
    if (client == null) {
      throw new NotFoundException(ExceptionMessage.NO_CLIENT_FOUND.getMessage(clientId));
    }
    recordClientRegistration(clientId);
    // Update the TS at the end of the process
    client.setLastContactMs(System.currentTimeMillis());
    client.setStartTimeMs(startTime);

    LOG.info("registerClient(): {}", client);
  }

  @Override
  public List<ClientInfo> getNormalClientInfoList()  {
    return new ArrayList<>(mClients);
  }

  @Override
  public List<ClientInfo> getLostClientInfoList() {
    return new ArrayList<>(mLostClients);
  }

  /**
   * Lost client periodic check.
   */
  public final class LostClientDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostClientDetectionHeartbeatExecutor}.
     */
    public LostClientDetectionHeartbeatExecutor() {}

    @Override
    public void heartbeat() {
      long masterClientTimeoutMs =
          ServerConfiguration.getMs(TxPropertyKey.MASTER_CLIENT_TIMEOUT_MS);
      for (ClientInfo clientInfo : mClients) {
        // This is not locking because the field is atomic
        final long lastUpdate = mClock.millis() - clientInfo.getLastContactMs();
        if (lastUpdate > masterClientTimeoutMs) {
          LOG.error("The client {}({}) timed out after {}ms without a heartbeat!",
              clientInfo.getId(), clientInfo.getClientIdentifier(), lastUpdate);
          mLostClients.add(clientInfo);
          mClients.remove(clientInfo);
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }
}
