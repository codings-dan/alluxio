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

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.security.User;
import alluxio.security.authentication.AbstractSaslClientHandler;
import alluxio.security.authentication.AuthenticationUserUtils;
import alluxio.security.authentication.SaslClientHandler;

import com.tencent.tdw.security.authentication.LocalKeyManager;
import org.apache.hadoop.security.sasl.TqAuthConst;
import org.apache.hadoop.security.sasl.TqClientSecurityProvider;
import org.apache.hadoop.security.tauth.TAuthLoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.net.InetSocketAddress;
import java.security.Security;
import java.util.HashMap;
import java.util.Set;

/**
 * {@link SaslClientHandler} implementation for TAuth.
 */
public class TAuthSaslClientHandler extends AbstractSaslClientHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TAuthSaslClientHandler.class);

  static {
    Security.addProvider(new TqClientSecurityProvider());
  }

  /**
   * Creates {@link SaslClientHandler} instance for TAuth.
   *
   * @param subject client subject
   * @param conf Alluxio configuration
   * @param serverAddress Alluxio server address
   * @throws UnauthenticatedException
   */
  public TAuthSaslClientHandler(Subject subject, AlluxioConfiguration conf,
      InetSocketAddress serverAddress)
      throws UnauthenticatedException {
    super(ChannelAuthenticationScheme.CUSTOM);
    if (subject == null) {
      throw new UnauthenticatedException("client subject not provided");
    }
    String realUser = null;
    String user = null;
    Set<User> users = subject.getPrincipals(User.class);
    if (users != null && !users.isEmpty()) {
      user = users.iterator().next().getName();
    }
    TAuthLoginModule.TAuthCredential credential = TAuthLoginModule
        .TAuthCredential.getFromSubject(subject);
    if ((credential == null || !credential.getLocalKeyManager().hasAnyKey())) {
      throw new UnauthenticatedException("Not found any key!");
    }
    // Determine the impersonation user
    String impersonationUser = AuthenticationUserUtils.getImpersonationUser(subject, conf);
    if (impersonationUser != null && !impersonationUser.equals(user)) {
      realUser = user;
      user = impersonationUser;
    }

    mSaslClient = createSaslClient(serverAddress,
        user, realUser, null, credential.getLocalKeyManager(), "");
    LOG.debug("Success create tauth client handler!");
  }

  private SaslClient createSaslClient(InetSocketAddress serverAddress,
       String user, String realUser, String impersonationUser,
       LocalKeyManager localKeyManager, String protocolName)
      throws UnauthenticatedException {
    try {
      //FIXME(baoloongmao)
      return Sasl.createSaslClient(new String[] {TqAuthConst.TAUTH},
        impersonationUser, null, "", new HashMap<String, String>(),
        new TAuthSaslClientCallbackHandler(serverAddress, user, realUser,
          localKeyManager, protocolName));
    } catch (SaslException e) {
      throw new UnauthenticatedException(e.getMessage(), e);
    }
  }
}
