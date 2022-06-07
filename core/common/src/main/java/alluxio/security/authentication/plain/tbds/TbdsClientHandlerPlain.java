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
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.security.User;
import alluxio.security.authentication.AbstractSaslClientHandler;
import alluxio.security.authentication.AuthenticationUserUtils;
import alluxio.security.authentication.SaslClientHandler;
import alluxio.security.authentication.plain.PlainSaslServerProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Set;

/**
 * {@link TbdsClientHandlerPlain} implementation for TBDS/Plain schemes.
 */
public class TbdsClientHandlerPlain extends AbstractSaslClientHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TbdsClientHandlerPlain.class);

  /**
   * Creates {@link SaslClientHandler} instance for Plain/Custom.
   *
   * @param subject client subject
   * @param conf Alluxio configuration
   * @param serverAddress Alluxio server address
   * @throws UnauthenticatedException excepiton
   */
  public TbdsClientHandlerPlain(Subject subject, AlluxioConfiguration conf,
      InetSocketAddress serverAddress)
      throws UnauthenticatedException {
    super(ChannelAuthenticationScheme.CUSTOM);
    if (subject == null) {
      throw new UnauthenticatedException("client subject not provided");
    }
    String connectionUser = null;
    String password = "noPassword";

    Set<User> users = subject.getPrincipals(User.class);
    if (!users.isEmpty()) {
      connectionUser = users.iterator().next().getName();
    }

    Set<String> credentials = subject.getPrivateCredentials(String.class);
    if (!credentials.isEmpty()) {
      password = credentials.iterator().next();
    }

    String impersonationUser = AuthenticationUserUtils.getImpersonationUser(subject, conf);
    String secureId = conf.getString(TxPropertyKey.SECURITY_AUTHENTICATION_TBDS_SECURE_ID);
    String secureKey = conf.getString(TxPropertyKey.SECURITY_AUTHENTICATION_TBDS_SECURE_KEY);
    String userName = conf.getString(TxPropertyKey.SECURITY_AUTHENTICATION_TBDS_SECURE_USER_NAME);

    mSaslClient = createSaslClient(
        connectionUser, password, impersonationUser, secureId, secureKey);
  }

  private SaslClient createSaslClient(String username, String password, String impersonationUser,
                                      String secureId, String secureKey)
      throws UnauthenticatedException {
    try {
      return Sasl.createSaslClient(new String[] {PlainSaslServerProvider.MECHANISM},
          impersonationUser, null, null, new HashMap<String, String>(),
          new PlainTbdsClientCallbackHandler(username, password, secureId, secureKey));
    } catch (SaslException e) {
      throw new UnauthenticatedException(e.getMessage(), e);
    }
  }
}
