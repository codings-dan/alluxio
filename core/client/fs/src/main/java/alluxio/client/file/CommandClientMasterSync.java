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
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
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
   * Get journal id information from the master.
   * @return journal id
   */
  public synchronized long heartbeat() {
    if (mMasterClient == null) {
      if (loadConf()) {
        mMasterClient = new RetryHandlingFileSystemMasterClient(MasterClientContext
            .newBuilder(mContext)
            .setMasterInquireClient(mInquireClient)
            .build());
      } else {
        LOG.error("Failed to load conf and can't heartbeat");
        return ERROR_CODE; // not heartbeat when failed to load conf
      }
    }
    try {
      mJournalId = mMasterClient.heartbeat();
    } catch (IOException e) {
      LOG.warn("Failed to get journal id from master: {}", e.toString());
      return ERROR_CODE;
    }
    return mJournalId;
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
  private boolean loadConf() {
    try {
      InetSocketAddress masterAddr = mInquireClient.getPrimaryRpcAddress();
      mContext.loadConf(masterAddr, true, false);
    } catch (UnavailableException e) {
      LOG.error("Failed to get master address during initialization", e);
      return false;
    } catch (AlluxioStatusException ae) {
      LOG.error("Failed to load configuration from meta master during initialization", ae);
      return false;
    }
    return true;
  }
}

