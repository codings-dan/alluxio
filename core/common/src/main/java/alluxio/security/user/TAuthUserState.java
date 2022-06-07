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

package alluxio.security.user;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.tauth.TAuthSaslClientHandler;
import alluxio.security.login.AppLoginModule;
import alluxio.security.login.TAuthLoginModuleConfiguration;
import alluxio.util.SecurityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * UserState implementation for the TAuth authentication schemes.
 */
public class TAuthUserState extends BaseUserState {
  private static final Logger LOG = LoggerFactory.getLogger(TAuthUserState.class);

  /**
   * Factory class to create the user state.
   */
  public static class Factory implements UserStateFactory {
    @Override
    public UserState create(Subject subject, AlluxioConfiguration conf, boolean isServer) {
      AuthType authType = conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
      if (authType == AuthType.CUSTOM) {
        if (conf.isSet(
            TxPropertyKey.SECURITY_AUTHENTICATION_CUSTOM_SASL_CLIENT_CLASS)) {
          Class clazz = conf.getClass(
              TxPropertyKey.SECURITY_AUTHENTICATION_CUSTOM_SASL_CLIENT_CLASS);
          if (clazz == TAuthSaslClientHandler.class) {
            return new TAuthUserState(subject, conf);
          }
        }
      }
      LOG.debug("N/A: auth type is not CUSTOM authType: {}, or {} is not configured to {}",
          authType, TxPropertyKey.SECURITY_AUTHENTICATION_CUSTOM_SASL_CLIENT_CLASS.getName(),
          TAuthSaslClientHandler.class);
      return null;
    }
  }

  private TAuthUserState(Subject subject, AlluxioConfiguration conf) {
    super(subject, conf);
  }

  @Override
  public User login() throws UnauthenticatedException {
    String username = "";
    if (mConf.isSet(PropertyKey.SECURITY_LOGIN_USERNAME)) {
      username = mConf.getString(PropertyKey.SECURITY_LOGIN_USERNAME);
    }
    try {
      // Use the class loader of User.class to construct the LoginContext. LoginContext uses this
      // class loader to dynamically instantiate login modules. This enables
      // Subject#getPrincipals to use reflection to search for User.class instances.
      LoginContext loginContext =
          SecurityUtils.createLoginContext(AuthType.CUSTOM, mSubject, User.class.getClassLoader(),
              new TAuthLoginModuleConfiguration(username),
              new AppLoginModule.AppCallbackHandler(username));
      loginContext.login();
    } catch (LoginException e) {
      throw new UnauthenticatedException("Failed to login: " + e.getMessage(), e);
    }

    LOG.debug("login subject: {}", mSubject);
    Set<User> userSet = mSubject.getPrincipals(User.class);
    if (userSet.isEmpty()) {
      throw new UnauthenticatedException("Failed to login: No Alluxio User is found.");
    }
    if (userSet.size() > 1) {
      StringBuilder msg = new StringBuilder(
          "Failed to login: More than one Alluxio Users are found:");
      for (User user : userSet) {
        msg.append(" ").append(user.toString());
      }
      throw new UnauthenticatedException(msg.toString());
    }
    return userSet.iterator().next();
  }
}
