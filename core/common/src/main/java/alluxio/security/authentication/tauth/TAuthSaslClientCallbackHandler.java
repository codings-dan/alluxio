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

package alluxio.security.authentication.tauth;

import com.tencent.tdw.security.Tuple;
import com.tencent.tdw.security.authentication.LocalKeyManager;
import com.tencent.tdw.security.authentication.SecurityCenterProvider;
import com.tencent.tdw.security.authentication.ServiceTarget;
import com.tencent.tdw.security.authentication.TAuthAuthentication;
import com.tencent.tdw.security.authentication.client.SecureClient;
import org.apache.hadoop.security.sasl.TqClientCallbackHandler;
import org.apache.hadoop.security.sasl.TqSaslServer;
import org.apache.hadoop.security.sasl.TqTicketResponseToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class TAuthSaslClientCallbackHandler extends TqClientCallbackHandler {
  public static final Logger LOG = LoggerFactory.getLogger(TAuthSaslClientCallbackHandler.class);
  private final String mUser;
  private final String mRealUser;
  private final String mProtocolName;
  private final SecureClient mSecureClient;
  private final InetSocketAddress mServerAddr;

  public TAuthSaslClientCallbackHandler(InetSocketAddress serverAddr, String user, String realUser,
                                        LocalKeyManager localKeyManager, String protocolName) {
    mServerAddr = serverAddr;
    mUser = user;
    mRealUser = realUser;
    mProtocolName = protocolName;
    mSecureClient = SecurityCenterProvider.createTauthSecureClient(
      realUser != null ? realUser : user, localKeyManager, true);
  }

  @Override
  protected Tuple<byte[], byte[]> processChallenge(String target) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("user {}, realUser {}, target: {}", mUser, mRealUser, target);
    }
    ServiceTarget serviceTarget = ServiceTarget.valueOf(target);
    if (target == null || TqSaslServer.DEFAULT_SERVER_NAME.equals(target)) {
      // TODO(bingbzheng): handle this.
      serviceTarget =
          ServiceTarget.valueOf(mServerAddr.getAddress().getHostAddress(), mServerAddr.getPort(),
              mProtocolName);
    }
    TAuthAuthentication tAuthAuthentication =
        (TAuthAuthentication) mSecureClient.getAuthentication(serviceTarget,
            mRealUser != null ? mUser : null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("session ticket: {}", tAuthAuthentication.encode());
    }
    return Tuple.of(tAuthAuthentication.getSessionKey(),
      new TqTicketResponseToken(tAuthAuthentication.getAuthenticator(),
        tAuthAuthentication.getServiceTicket(), tAuthAuthentication.getSmkEpoch()).toBytes());
  }

  @Override
  protected String getUserName() {
    // using auth user instead.
    // TODO(bingbzheng): fallback to auth user
    return mUser;
  }

  @Override
  protected byte[] getExtraId() {
    return mProtocolName.getBytes();
  }
}
