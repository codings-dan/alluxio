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

package alluxio.client.file;

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.TxPropertyKey;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterInquireClient;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.ClientIdentifier;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A class used to track command heartbeats to a master.
 *
 * The class should be instantiated when a new FileSystemContext is created with a
 * configuration that points to a given master. As new FileSystemContexts are created, if they
 * utilize the same connection details, then they can simply be added to this context so
 * that their information is included in the command heartbeat. To add them, one should simply
 * call {@link #addHeartbeat(ClientContext, MasterInquireClient, FileSystem)}
 * with the necessary arguments.
 * For each separate set of connection details, a new instance of this class is created. As
 * FileSystemContexts are closed, they remove themselves from the internal command heartbeat.
 *
 * When the final FileSystemContext closes and removes its heartbeat from command it will also
 * shutdown and close the executor service until a new {@link alluxio.client.file.FileSystemContext}
 * is created.
 */
public class CommandHeartbeatContext {
  private static final Logger LOG = LoggerFactory.getLogger(CommandHeartbeatContext.class);

  /** A map from master connection details to heartbeat context instances. */
  private static final Map<MasterInquireClient.ConnectDetails, CommandHeartbeatContext>
      COMMAND_HEARTBEAT = new ConcurrentHashMap<>(2);

  private static final long ERROR_CODE = -1L;
  private static final long INITIAL_CODE = -1L;
  private static final long INVALID_CODE = -1L;

  /** The service which executes command heartbeat RPCs. */
  private static ScheduledExecutorService sExecutorService;
  private final MasterInquireClient.ConnectDetails mConnectDetails;
  private final CommandClientMasterSync mCommandClientMasterSync;
  private final AlluxioConfiguration mConf;
  private long mFailHeartbeatTime;
  private long mJournalId;
  private final FileSystem mFs;
  private final long mStartTime;
  private final int mPid;

  // This can only be a primitive if all accesses are synchronized
  private int mCtxCount;
  private ScheduledFuture<?> mCommandMasterHeartbeatTask;
  private long mClientId;
  private boolean mHaveHeartbeatSuccessful;

  private CommandHeartbeatContext(ClientContext ctx,
      MasterInquireClient inquireClient, FileSystem fs) {
    mCtxCount = 0;
    mConnectDetails = inquireClient.getConnectDetails();
    mConf = ctx.getClusterConf();
    mCommandClientMasterSync = new CommandClientMasterSync(ctx, inquireClient);
    mJournalId = 0;
    mFailHeartbeatTime = INITIAL_CODE;
    mFs = fs;
    mClientId = 0L;
    mHaveHeartbeatSuccessful = false;
    mStartTime = System.currentTimeMillis();
    mPid = Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
  }

  private synchronized void addContext() {
    // increment and lazily schedule the new heartbeat task if it is the first one
    if (mCtxCount++ == 0) {
      mCommandMasterHeartbeatTask =
          sExecutorService.scheduleWithFixedDelay(this::heartbeat,
              mConf.getMs(TxPropertyKey.USER_COMMAND_HEARTBEAT_INTERVAL_MS),
              mConf.getMs(TxPropertyKey.USER_COMMAND_HEARTBEAT_INTERVAL_MS),
              TimeUnit.MILLISECONDS);
    }
  }

  private synchronized void heartbeat() {
    if (registerClient() < 0) {
      return;
    }
    long journalId;
    if (mFs instanceof MetadataCachingBaseFileSystem) {
      journalId = mCommandClientMasterSync.heartbeat(mClientId,
          ((MetadataCachingBaseFileSystem) mFs).getMetadataCacheSize());
      handleClearClientMetadataCache(journalId);
    } else {
      journalId = mCommandClientMasterSync.heartbeat(mClientId, INVALID_CODE);
      if (journalId == ERROR_CODE) {
        LOG.warn("client {} failed to heartbeat with master", mClientId);
      }
    }
    if (journalId != ERROR_CODE) {
      mHaveHeartbeatSuccessful = true;
    }
  }

  private void handleClearClientMetadataCache(long journalId) {
    if (journalId == ERROR_CODE) {
      if (mFailHeartbeatTime == INITIAL_CODE) {
        mFailHeartbeatTime = System.currentTimeMillis();
        return;
      }
      long time = System.currentTimeMillis() - mFailHeartbeatTime;
      long expirationTimeMs = mConf.getMs(TxPropertyKey.USER_COMMAND_HEARTBEAT_INTERVAL_MS);
      if (time >= expirationTimeMs) {
        LOG.info("Failed heartbeat in the past {} s , clear all metadata cache", time / 1000.0);
        if (mFs instanceof MetadataCachingBaseFileSystem) {
          ((MetadataCachingBaseFileSystem) mFs).dropMetadataCacheAll();
        }
      }
      return;
    }
    mFailHeartbeatTime = INITIAL_CODE;
    if (journalId != mJournalId) {
      mJournalId = journalId;
      // TODO(dragonyliu): clear metadata cache according to journal
      LOG.info("Journal journalId change, clear all metadata cache");
      ((MetadataCachingBaseFileSystem) mFs).dropMetadataCacheAll();
    } else {
      LOG.debug("The journal journalId has not changed, refresh the metadata cache,"
          + " and extend the expiration time.");
      ((MetadataCachingBaseFileSystem) mFs).updateMetadataCacheAll();
    }
  }

  private long registerClient() {
    boolean needRegister = mHaveHeartbeatSuccessful && mClientId == 0;
    if (needRegister) {
      String host = getHost(mConf);
      String containerHost =
          mConf.getOrDefault(TxPropertyKey.USER_CONTAINER_HOSTNAME, "");
      ClientIdentifier clientIdentifier = new ClientIdentifier(host, containerHost, mPid);
      mClientId = mCommandClientMasterSync.getClientId(clientIdentifier);
      long code =
          mCommandClientMasterSync.register(mClientId, mStartTime);
      if (code == ERROR_CODE) {
        LOG.info("Failed register client to master.");
        return ERROR_CODE;
      } else {
        LOG.info("Register successful, and the clientId is {}", mClientId);
        return mClientId;
      }
    }
    return 0;
  }

  private String getHost(AlluxioConfiguration conf) {
    String host = mConf.getOrDefault(PropertyKey.USER_HOSTNAME, "");
    if (!Objects.equals(host, "")) {
      return host;
    } else {
      return NetworkAddressUtils.getLocalHostName(
          (int) conf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    }
  }

  /**
   * When closed, this method will remove its task from the scheduled executor.
   *
   * It will also remove itself from being tracked in the COMMAND_HEARTBEAT. It should
   * only ever be called in {@link #removeContext()} when the context count reaches 0. Afterwards,
   * this reference should be discarded.
   */
  private synchronized void close() {
    if (mCommandMasterHeartbeatTask != null) {
      mCommandMasterHeartbeatTask.cancel(false);
    }
    COMMAND_HEARTBEAT.remove(mConnectDetails);
    mCommandClientMasterSync.close();
  }

  /**
   * Sets up a new command heartbeat with the given client information.
   *
   * This will instantiate a new executor service if it is the first heartbeat to be added,
   * his helps to consolidate RPCs and utilize less resources on the client.
   * @param ctx The application's client context
   * @param inquireClient the master inquire client used to connect to the master
   * @param fs the metadata filesystem object
   */
  public static synchronized void addHeartbeat(ClientContext ctx,
      MasterInquireClient inquireClient, FileSystem fs) {
    Preconditions.checkNotNull(ctx);
    Preconditions.checkNotNull(inquireClient);

    // Lazily initializing the executor service for first heartbeat
    // Relies on the method being synchronized
    if (sExecutorService == null) {
      sExecutorService = Executors.newSingleThreadScheduledExecutor(
          ThreadFactoryUtils.build("command-master-heartbeat-%d", true));
    }
    CommandHeartbeatContext heartbeatCtx = COMMAND_HEARTBEAT.computeIfAbsent(
        inquireClient.getConnectDetails(),
        (addr) -> new CommandHeartbeatContext(ctx, inquireClient, fs));
    heartbeatCtx.addContext();
    LOG.debug("Registered command heartbeat");
  }

  /**
   * Removes an application from the command heartbeat.
   *
   * If this is the last application to be removed for a given master then it will cancel the
   * execution of the command RPC for that master.
   * @param ctx The client context used to register the heartbeat
   */
  public static synchronized void removeHeartbeat(ClientContext ctx) {
    MasterInquireClient.ConnectDetails connectDetails =
        MasterInquireClient.Factory.getConnectDetails(ctx.getClusterConf());
    CommandHeartbeatContext heartbeatCtx = COMMAND_HEARTBEAT.get(connectDetails);
    if (heartbeatCtx != null) {
      heartbeatCtx.removeContext();
    }

    if (COMMAND_HEARTBEAT.isEmpty()) {
      sExecutorService.shutdown();
      try {
        sExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Command heartbeat executor did not shut down in a timely manner: {}",
            e.toString());
      }
      sExecutorService = null;
    }
  }

  /**
   * Remove an application from this command heartbeat.
   *
   * A user who calls this method should assume the reference to this context is invalid
   * afterwards. It will automatically close and remove itself from all tracking if the number
   * of open contexts for this heartbeat reaches 0. Never attempt to add another context with
   * the same reference after removing.
   */
  private synchronized void removeContext() {
    if (--mCtxCount <= 0) {
      close();
    }
  }
}