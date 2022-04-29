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
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.ClientCommand;
import alluxio.grpc.ClientCommandType;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;
import alluxio.wire.ClientIdentifier;

import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.net.InetSocketAddress;

/**
 * Task that get journal id information from master through heartbeat. This class manages
 * its own {@link FileSystemMasterClient}.
 *
 * If the task fails to heartbeat to the master, it will destroy its old master client and recreate
 * it before retrying.
 */
@ThreadSafe
public final class CommandClientMasterSync {
  private static final Logger LOG = LoggerFactory.getLogger(CommandClientMasterSync.class);

  private final MasterInquireClient mInquireClient;
  private final ClientContext mContext;
  private long mJournalId = -1;
  private static final long ERROR_CODE = -1;

  /**
   * Client for communicating to fileSystem master.
   */
  private RetryHandlingFileSystemMasterClient mMasterClient;

  /**
   * Constructs a new {@link CommandClientMasterSync}.
   *
   * @param ctx client context
   * @param inquireClient the master inquire client
   */
  public CommandClientMasterSync(ClientContext ctx, MasterInquireClient inquireClient) {
    mInquireClient = inquireClient;
    mContext = ctx;
  }

  /**
   * Transport client information and get journal id information from the master.
   * @param clientId the client id
   * @param metadataSize the metadata cache size of client
   * @param journalId the journal id of the client
   * @return the client command result
   */
  public synchronized ClientCommand heartbeat(long clientId, long metadataSize, long journalId)
      throws AlluxioStatusException {
    loadConfiguration();
    ClientCommand cmd;
    try {
      cmd = mMasterClient.heartbeat(clientId, metadataSize, journalId);
    } catch (AlluxioStatusException e) {
      cmd = handleUnimplementedException(e);
    }
    return cmd;
  }

  public synchronized long getClientId(ClientIdentifier clientIdentifier)
      throws AlluxioStatusException {
    loadConfiguration();
    return mMasterClient.getClientId(clientIdentifier);
  }

  /**
   * Register the client to the master.
   * @param startTime the client start time
   * @param clientId the client id
   */
  public synchronized void register(long clientId, long startTime) throws AlluxioStatusException {
    loadConfiguration();
    mMasterClient.register(clientId, startTime);
  }

  private void loadConfiguration() throws AlluxioStatusException {
    if (mMasterClient == null) {
      loadConf();
      mMasterClient = new RetryHandlingFileSystemMasterClient(MasterClientContext
          .newBuilder(mContext)
          .setMasterInquireClient(mInquireClient)
          .build());
    }
  }

  /**
   * Close the command master client.
   */
  public synchronized void close() {
    if (mMasterClient != null) {
      mMasterClient.close();
    }
  }

  /**
   * Loads configuration.
   *
   * @return true if successfully loaded configuration
   */
  private void loadConf() throws AlluxioStatusException {
    InetSocketAddress masterAddr = mInquireClient.getPrimaryRpcAddress();
    mContext.loadConf(masterAddr, true, false);
  }

  private ClientCommand handleUnimplementedException(AlluxioStatusException e)
      throws AlluxioStatusException {
    if (e.getStatus().getCode() == Status.UNIMPLEMENTED.getCode()) {
      LOG.debug("The master don't have the client command heartbeat interface, please change"
          + " the client or master code.");
      return ClientCommand.newBuilder()
          .setClientCommandType(ClientCommandType.CLIENT_NOTHING).build();
    } else {
      throw e;
    }
  }
}

