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

import alluxio.Constants;
import alluxio.security.authentication.plain.tbds.PlainTbdsServer;

import com.google.common.base.Preconditions;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

/**
 * This class provides Mock PLAIN TBDS authentication.
 * <p/>
 * NOTE: When this SaslServer works on authentication (i.e., in the method
 * {@link #evaluateResponse(byte[])}, it always assigns authentication ID to authorization ID
 * currently.
 */
@NotThreadSafe
public final class MockPlainTbdsServer extends PlainTbdsServer {

  MockPlainTbdsServer() {}

  @Override
  @Nullable
  public byte[] evaluateResponse(byte[] response) throws SaslException {

    String[] tokens;
    try {
      LOG.info("start authenticate user, input info:" + new String(response, "UTF-8"));
      tokens = new String(response, "UTF-8").split("\u0000");
    } catch (UnsupportedEncodingException e) {
      throw new SaslException("UTF-8 encoding not supported", e);
    }
    if (tokens.length != 3) {
      throw new SaslException(
          "Invalid SASL/TBDS response: expected 3 tokens, got " + tokens.length);
    }

    String authcInfo = tokens[1];
    String[] authcInfoparts = authcInfo.split(Constants.PLAIN_TBDS_AUTHCINFO_SEP);

    if (authcInfo.isEmpty() || !(authcInfoparts.length == 4)) {
      throw new SaslException(
          "Authentication for tbds mechanism failed: tbds auth params not specified!!");
    }
    mAuthorizationId = authcInfoparts[0];
    LOG.info("successfully authenticated user " + mAuthorizationId + " with tbds auth.");

    mCompleted = true;
    return null;
  }

  /**
   * This class is used to create an instances of {@link PlainTbdsServer}. The parameter mechanism
   * must be "PLAIN" when this Factory is called, or null will be returned.
   */
  @ThreadSafe
  public static class Factory implements SaslServerFactory {
    /**
     * Constructs a new {@link PlainTbdsServer.Factory} for the {@link PlainTbdsServer}.
     */
    public Factory() {}

    /**
     * Creates a {@link SaslServer} using the parameters supplied. It returns null if no SaslServer
     * can be created using the parameters supplied. Throws {@link SaslException} if it cannot
     * create a SaslServer because of an error.
     *
     * @param mechanism the name of a SASL mechanism. (e.g. "PLAIN")
     * @param protocol the non-null string name of the protocol for which the authentication is
     *        being performed
     * @param serverName the non-null fully qualified host name of the server to authenticate to
     * @param props the possibly null set of properties used to select the SASL mechanism and to
     *        configure the authentication exchange of the selected mechanism
     * @param callbackHandler the possibly null callback handler to used by the SASL mechanisms to
     *        do further operation
     * @return A possibly null SaslServer created using the parameters supplied. If null, this
     *         factory cannot produce a SaslServer using the parameters supplied.
     * @exception SaslException If it cannot create a SaslServer because of an error.
     */
    @Override
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName,
        Map<String, ?> props, CallbackHandler callbackHandler) throws SaslException {
      Preconditions.checkArgument(MockPlainTbdsServerProvider.MECHANISM.equals(mechanism));
      return new MockPlainTbdsServer();
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      return new String[] {MockPlainTbdsServerProvider.MECHANISM};
    }
  }
}
