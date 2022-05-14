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

package alluxio.security.authentication.plain.tbds.mock;

import alluxio.conf.AlluxioConfiguration;
import alluxio.security.authentication.AbstractSaslServerHandler;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.authentication.ImpersonationAuthenticator;
import alluxio.security.authentication.SaslServerHandler;
import alluxio.security.authentication.plain.tbds.mock.MockPlainTbdsServerProvider;

import java.security.Security;
import java.util.HashMap;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

/**
 * {@link SaslServerHandler} implementation for Plain/Custom schemes.
 */
public class MockTbdsServerHandlerPlain extends AbstractSaslServerHandler {
  static {
    Security.addProvider(new MockPlainTbdsServerProvider());
  }

  /**
   * Creates {@link SaslServerHandler} for Plain/Custom.
   *
   * @param serverName server name
   * @param conf Alluxio configuration
   * @param authenticator the impersonation authenticator
   * @throws SaslException
   */
  public MockTbdsServerHandlerPlain(String serverName, AlluxioConfiguration conf,
                          ImpersonationAuthenticator authenticator) throws SaslException {
    mSaslServer = Sasl.createSaslServer(MockPlainTbdsServerProvider.MECHANISM,
        null, serverName,
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
        null, MockPlainTbdsServerProvider.NAME);
  }
}
