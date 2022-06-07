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

package alluxio.security.authentication.plain.tbds;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.TxPropertyKey;
import alluxio.security.authentication.AbstractSaslServerHandler;
import alluxio.security.authentication.ImpersonationAuthenticator;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.authentication.SaslServerHandler;

import com.tencent.tbds.sdk.plugin.TbdsAuthenticator;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import java.security.Security;
import java.util.HashMap;

/**
 * {@link SaslServerHandler} implementation for Plain/Custom schemes.
 */
public class TbdsServerHandlerPlain extends AbstractSaslServerHandler {
  private static volatile boolean sInitialized = false;

  static {
    Security.addProvider(new PlainTbdsServerProvider());
  }

  /**
   * Creates {@link SaslServerHandler} for Plain/Custom.
   *
   * @param serverName server name
   * @param conf Alluxio configuration
   * @param authenticator the impersonation authenticator
   * @throws SaslException
   */
  public TbdsServerHandlerPlain(String serverName, AlluxioConfiguration conf,
      ImpersonationAuthenticator authenticator) throws SaslException {
    if (!isInitialized()) {
      synchronized (TbdsServerHandlerPlain.class) {
        if (!isInitialized()) {
          TbdsAuthenticator.initialize(
              conf.getString(TxPropertyKey.SECURITY_AUTHENTICATION_TBDS_PORTAL_RPC_IP),
              conf.getInt(TxPropertyKey.SECURITY_AUTHENTICATION_TBDS_PORTAL_RPC_PORT),
              conf.getString(TxPropertyKey.SECURITY_AUTHENTICATION_TBDS_TABLE));
          markInitialized();
        }
      }
    }
    mSaslServer = Sasl.createSaslServer(PlainTbdsServerProvider.MECHANISM, null, serverName,
        new HashMap<String, String>(), null);
  }

  @Override
  public void setAuthenticatedUserInfo(AuthenticatedUserInfo userinfo) {
    // Plain authentication only needs authorized user name which is available in completed
    // SaslServer instance.
  }

  @Override
  public AuthenticatedUserInfo getAuthenticatedUserInfo() {
    return new AuthenticatedUserInfo(mSaslServer.getAuthorizationID(),
        null, PlainTbdsServerProvider.NAME);
  }

  private static boolean isInitialized() {
    return sInitialized;
  }

  private static void markInitialized() {
    sInitialized = true;
  }
}
