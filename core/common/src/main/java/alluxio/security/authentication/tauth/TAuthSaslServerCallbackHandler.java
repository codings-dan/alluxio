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

import alluxio.security.authentication.ImpersonationAuthenticator;

import com.tencent.tdw.security.Tuple;
import com.tencent.tdw.security.authentication.Authenticator;
import com.tencent.tdw.security.authentication.TAuthAuthentication;
import com.tencent.tdw.security.authentication.v2.SecureServiceV2;
import org.apache.hadoop.security.AuthConfigureHolder;
import org.apache.hadoop.security.sasl.TqServerCallbackHandler;
import org.apache.hadoop.security.sasl.TqTicketResponseToken;

public class TAuthSaslServerCallbackHandler extends TqServerCallbackHandler {
  private final String mServerName;
  private final SecureServiceV2<TAuthAuthentication> mSecureService;
  private final ImpersonationAuthenticator mAuthenticator;
  private final boolean mNeedImpersonateCheck;

  public TAuthSaslServerCallbackHandler(String serverName,
      SecureServiceV2<TAuthAuthentication> secureService,
      ImpersonationAuthenticator authenticator, boolean impersonateCheckEnable) {
    mServerName = serverName;
    mSecureService = secureService;
    mAuthenticator = authenticator;
    mNeedImpersonateCheck = impersonateCheckEnable;
  }

  @Override
  protected boolean isNeedAuth(byte[] extraId) {
    return AuthConfigureHolder.isAuthEnable()
      && (extraId == null
      || AuthConfigureHolder.getProtocolPolicyManagement().isNeedAuth(new String(extraId)));
  }

  @Override
  protected boolean isForbidden(String userName) {
    return AuthConfigureHolder.isNotAllow(userName);
  }

  @Override
  protected Tuple<byte[], String> processResponse(byte[] response) throws Exception {
    TqTicketResponseToken token = TqTicketResponseToken.valueOf(response);
    TAuthAuthentication tAuthAuthentication = new TAuthAuthentication(token.getSmkId(),
        token.getServiceTicket(), token.getAuthenticator());
    Authenticator authenticator = mSecureService.authenticate(tAuthAuthentication);
    // Alluxio impersonate auth
    if (mNeedImpersonateCheck) {
      mAuthenticator.authenticate(authenticator.getAuthUser(), authenticator.getUser());
    }
    return Tuple.of(authenticator.getSessionKey(), authenticator.getUser());
  }

  @Override
  protected String getServiceName() {
    return mServerName;
  }
}
