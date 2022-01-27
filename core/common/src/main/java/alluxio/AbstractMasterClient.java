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

package alluxio;

import alluxio.conf.PropertyKey;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;

import java.net.InetSocketAddress;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for master clients.
 */
@ThreadSafe
public abstract class AbstractMasterClient extends AbstractClient {
  /** Client for determining the master RPC address. */
  private final MasterInquireClient mMasterInquireClient;

  /**
   * Creates a new master client base.
   *
   * @param clientConf master client configuration
   */
  public AbstractMasterClient(MasterClientContext clientConf) {
    super(clientConf, null,
        clientConf.getClusterConf().getBoolean(TxPropertyKey.USER_FALLBACK_ENABLED)
            ? () -> new ExponentialBackoffRetry(
                clientConf.getClusterConf()
                    .getInt(TxPropertyKey.USER_FALLBACK_RETRY_BASE_SLEEP_MS),
                clientConf.getClusterConf().getInt(TxPropertyKey.USER_FALLBACK_RETRY_MAX_SLEEP_MS),
                clientConf.getClusterConf().getInt(TxPropertyKey.USER_FALLBACK_RETRY_MAX_TIMES))
            : () -> RetryUtils.defaultClientRetry(
                clientConf.getClusterConf().getDuration(PropertyKey.USER_RPC_RETRY_MAX_DURATION),
                clientConf.getClusterConf().getDuration(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS),
                clientConf.getClusterConf().getDuration(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS)));
    mMasterInquireClient = clientConf.getMasterInquireClient();
  }

  /**
   * Creates a new master client base.
   *
   * @param clientConf master client configuration
   * @param address address to connect to
   * @param retryPolicySupplier retry policy to use
   */
  public AbstractMasterClient(MasterClientContext clientConf, InetSocketAddress address,
      Supplier<RetryPolicy> retryPolicySupplier) {
    super(clientConf, address, retryPolicySupplier);
    mMasterInquireClient = clientConf.getMasterInquireClient();
  }

  @Override
  public synchronized InetSocketAddress getAddress() throws UnavailableException {
    return mMasterInquireClient.getPrimaryRpcAddress();
  }

  @Override
  public synchronized InetSocketAddress getConfAddress() throws UnavailableException {
    if (mAddress != null) {
      return mAddress;
    }

    return mMasterInquireClient.getPrimaryRpcAddress();
  }
}
