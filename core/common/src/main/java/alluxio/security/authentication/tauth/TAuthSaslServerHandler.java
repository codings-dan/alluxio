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
import alluxio.conf.TxPropertyKey;
import alluxio.security.authentication.AbstractSaslServerHandler;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.authentication.ImpersonationAuthenticator;
import alluxio.security.authentication.SaslServerHandler;

import com.tencent.tdw.security.authentication.LocalKeyManager;
import com.tencent.tdw.security.authentication.SecurityCenterProvider;
import com.tencent.tdw.security.authentication.v2.SecureServiceV2;
import com.tencent.tdw.security.utils.ENVUtils;
import com.tencent.tdw.security.utils.StringUtils;
import org.apache.hadoop.security.sasl.TqAuthConst;
import org.apache.hadoop.security.sasl.TqServerSecurityProvider;
import org.apache.hadoop.security.tauth.TAuthLoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Security;
import java.util.HashMap;

/**
 * {@link SaslServerHandler} implementation for TAuth.
 */
public class TAuthSaslServerHandler extends AbstractSaslServerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TAuthSaslServerHandler.class);
  public static final String SASL_DEFAULT_REALM = "default";
  private static String sServiceName;
  private static volatile SecureServiceV2 sSecureServiceV2;

  static {
    Security.addProvider(new TqServerSecurityProvider());
  }

  /**
   * Creates {@link SaslServerHandler} for TAuth.
   *
   * @param serverName server name
   * @param conf Alluxio configuration
   * @param authenticator the impersonation authenticator
   */
  public TAuthSaslServerHandler(String serverName, AlluxioConfiguration conf,
      ImpersonationAuthenticator authenticator) throws SaslException {
    if (sSecureServiceV2 == null) {
      synchronized (TAuthSaslServerHandler.class) {
        if (sSecureServiceV2 == null) {
          initTAuthSecureService();
        }
      }
    }
    String serverId = SASL_DEFAULT_REALM;
    if (!StringUtils.isBlank(sServiceName)) {
      serverId = sServiceName;
    }
    boolean needImpersonationCheck = conf.getBoolean(TxPropertyKey
        .SECURITY_TAUTH_IMPERSONATION_CHECK_ENABLE);
    mSaslServer = Sasl.createSaslServer(TqAuthConst.TAUTH, null, sServiceName,
      new HashMap<String, String>(),
      new TAuthSaslServerCallbackHandler(serverId, sSecureServiceV2, authenticator,
          needImpersonationCheck));
  }

  @Override
  public void setAuthenticatedUserInfo(AuthenticatedUserInfo userinfo) {
  }

  @Override
  public AuthenticatedUserInfo getAuthenticatedUserInfo() {
    return new AuthenticatedUserInfo(mSaslServer.getAuthorizationID());
  }

  private static void initTAuthSecureService() throws SaslException {
    //Load smk first, otherwise using current login user
    LocalKeyManager localKeyManager = LocalKeyManager.generateByService(ENVUtils.CURUSER);
    sServiceName = localKeyManager.getKeyLoader().getSubject();
    if (!localKeyManager.hasAnyKey()) {
      AccessControlContext context = AccessController.getContext();
      Subject subject = Subject.getSubject(context);
      TAuthLoginModule.TAuthPrincipal principal  = TAuthLoginModule.TAuthPrincipal
          .getFromSubject(subject);
      TAuthLoginModule.TAuthCredential credential = TAuthLoginModule.TAuthCredential
          .getFromSubject(subject);
      if ((credential != null && credential.getLocalKeyManager().hasAnyKey())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using local key manager from current login user: {}", principal);
        }
        sServiceName = principal.getName();
        localKeyManager = credential.getLocalKeyManager();
      }
    }
    if (localKeyManager.hasAnyKey()) {
      LOG.info("Secure service initialized with user {}", sServiceName);
      sSecureServiceV2 = SecurityCenterProvider.createTauthSecureService(sServiceName,
          null, localKeyManager, true);
    } else {
      throw new SaslException("No credential found for tauth user " + ENVUtils.CURUSER);
    }
  }
}
