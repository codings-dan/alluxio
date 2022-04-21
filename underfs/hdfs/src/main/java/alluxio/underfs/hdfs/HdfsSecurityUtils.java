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

package alluxio.underfs.hdfs;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public final class HdfsSecurityUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsSecurityUtils.class);

  private static boolean isHdfsSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }

  public static <T> T runAsCurrentUser(SecuredRunner<T> runner, boolean securityCheckEnabled)
      throws IOException {
    if (securityCheckEnabled && !isHdfsSecurityEnabled()) {
      LOG.warn("security is not enabled");
      return runner.run();
    }
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return runAs(ugi, runner, securityCheckEnabled);
  }

  public static <T> T runAs(UserGroupInformation ugi, final SecuredRunner<T> runner,
      boolean securityCheckEnabled) throws IOException {
    if (securityCheckEnabled && !isHdfsSecurityEnabled()) {
      LOG.warn("security is not enabled");
      return runner.run();
    }
    LOG.debug("UGI: {}", ugi.toString());
    LOG.debug("UGI login user {}", UserGroupInformation.getLoginUser());
    LOG.debug("UGI current user {}", UserGroupInformation.getCurrentUser());
    if (ugi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.KERBEROS
        && !ugi.hasKerberosCredentials()) {
      LOG.error(
          "UFS Kerberos security is enabled but UGI has no Kerberos credentials. Please check "
              + "Alluxio configurations for Kerberos principal and keytab file.");
    }
    try {
      return ugi.doAs((PrivilegedExceptionAction<T>) () -> runner.run());
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public interface SecuredRunner<T> {
    T run() throws IOException;
  }
}
