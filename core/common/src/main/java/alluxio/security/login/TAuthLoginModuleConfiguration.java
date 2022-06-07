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

package alluxio.security.login;

import alluxio.security.authentication.AuthType;

import org.apache.hadoop.security.tauth.TAuthLoginModule;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

/**
 * A JAAS configuration that defines the login modules, by which JAAS uses to login.
 *
 * In implementation, we define several modes (Simple, Kerberos, ...) by constructing different
 * arrays of AppConfigurationEntry, and select the proper array based on the configured mode.
 *
 * Then JAAS login framework use the selected array of AppConfigurationEntry to determine the login
 * modules to be used.
 */
@ThreadSafe
public final class TAuthLoginModuleConfiguration extends LoginModuleConfiguration {
  private final String mPropertyUserName;

  /** Login module that allows a user name provided by an Alluxio specific login module. */
  private static final AppConfigurationEntry TAUTH_EXT_LOGIN = new AppConfigurationEntry(
      TAuthExtLoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, EMPTY_JAAS_OPTIONS);

  /**
   * Constructs a new {@link TAuthLoginModuleConfiguration}.
   * @param userName the username configured through property
   */
  public TAuthLoginModuleConfiguration(String userName) {
    mPropertyUserName = userName;
  }

  @Override
  @Nullable
  public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
    AppConfigurationEntry tauthLogin = getTAuthConfigurationEntry();
    AppConfigurationEntry[] tauthEntry =
        new AppConfigurationEntry[] {APP_LOGIN, OS_SPECIFIC_LOGIN,
          tauthLogin, TAUTH_EXT_LOGIN};
    if (appName.equalsIgnoreCase(AuthType.CUSTOM.name())) {
      return tauthEntry;
    } else if (appName.equalsIgnoreCase(AuthType.KERBEROS.name())) {
      throw new UnsupportedOperationException("Kerberos is not supported currently.");
    }
    return null;
  }

  private AppConfigurationEntry getTAuthConfigurationEntry() {
    String principle = System.getenv("TAUTHPRINCIPAL");
    String tauthKey = System.getenv("TAUTHKEY");
    String tauthKeyPath = System.getenv("TAUTHKEYPATH");
    LoginModuleControlFlag controlFlag = LoginModuleControlFlag.REQUIRED;
    final Map<String, Object> options = new HashMap<>(EMPTY_JAAS_OPTIONS);
    if (principle != null) {
      options.put(TAuthLoginModule.TAUTH_PRINCIPAL, principle);
    }

    if (!mPropertyUserName.equals("")) {
      options.put(TAuthLoginModule.TAUTH_PRINCIPAL, mPropertyUserName);
    }

    if (tauthKey != null) {
      options.put(TAuthLoginModule.TAUTH_KEY, tauthKey);
    }
    if (tauthKeyPath != null) {
      options.put(TAuthLoginModule.TAUTH_KEY_PATH, tauthKeyPath);
    }
    options.put(TAuthLoginModule.OS_PRINCIPAL_CLASS,
        LoginModuleConfigurationUtils.OS_PRINCIPAL_CLASS);
    return new AppConfigurationEntry(TAuthLoginModule.class.getName(),
        controlFlag, options);
  }
}
