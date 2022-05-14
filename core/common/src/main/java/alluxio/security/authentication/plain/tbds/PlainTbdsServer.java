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

import alluxio.Constants;
import alluxio.security.authentication.AuthenticatedClientUser;

import com.tencent.tbds.rpc.portal.UserDocument;
import com.tencent.tbds.sdk.plugin.AuthenticationException;
import com.tencent.tbds.sdk.plugin.TbdsAuthenticator;
import com.google.common.base.Preconditions;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServerFactory;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;

/**
 * This class provides PLAIN TBDS authentication.
 * <p/>
 * NOTE: When this SaslServer works on authentication (i.e., in the method
 * {@link #evaluateResponse(byte[])}, it always assigns authentication ID to authorization ID
 * currently.
 */
@NotThreadSafe
public class PlainTbdsServer implements SaslServer {
  protected static final Logger LOG = LoggerFactory.getLogger(PlainTbdsServer.class);
  /**
   * This ID represent the authorized client user, who has been authenticated successfully. It is
   * associated with the client connection thread for following action authorization usage.
   */
  protected String mAuthorizationId;
  /** Whether an authentication is complete or not. */
  protected boolean mCompleted = false;

  protected PlainTbdsServer() {}

  @Override
  public String getMechanismName() {
    return PlainTbdsServerProvider.MECHANISM;
  }

  @Override
  @Nullable
  public byte[] evaluateResponse(byte[] response) throws SaslException {
    /*
     * Message format (from https://tools.ietf.org/html/rfc4616):
     *
     * message   = [authzid] UTF8NUL authcid UTF8NUL passwd
     * authcid   = 1*SAFE ; MUST accept up to 255 octets
     * authzid   = 1*SAFE ; MUST accept up to 255 octets
     * passwd    = 1*SAFE ; MUST accept up to 255 octets
     * UTF8NUL   = %x00 ; UTF-8 encoded NUL character
     *
     * SAFE      = UTF1 / UTF2 / UTF3 / UTF4
     * ;; any UTF-8 encoded Unicode character except NUL
     */

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
    try {
      UserDocument userDoc = TbdsAuthenticator.getInstance().authenticate(
          authcInfoparts[0], Long.parseLong(authcInfoparts[1]),
          Integer.parseInt(authcInfoparts[2]), authcInfoparts[3]);
      mAuthorizationId = userDoc.getName();
    } catch (AuthenticationException e) {
      LOG.warn("authentication failed with tbds mechanism, client params received:" + authcInfo);
      throw new SaslException(e.getMessage());
    }
    LOG.info("successfully authenticated user " + mAuthorizationId + " with tbds auth.");

    mCompleted = true;
    return null;
  }

  @Override
  public boolean isComplete() {
    return mCompleted;
  }

  @Override
  public String getAuthorizationID() {
    checkNotComplete();
    return mAuthorizationId;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
    checkNotComplete();
    return Arrays.copyOfRange(incoming, offset, offset + len);
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
    checkNotComplete();
    return Arrays.copyOfRange(outgoing, offset, offset + len);
  }

  @Override
  @Nullable
  public Object getNegotiatedProperty(String propName) {
    checkNotComplete();
    return Sasl.QOP.equals(propName) ? "auth" : null;
  }

  @Override
  public void dispose() {
    if (mCompleted) {
      // clean up the user in threadlocal, when client connection is closed.
      AuthenticatedClientUser.remove();
    }

    mCompleted = false;
    mAuthorizationId = null;
  }

  private void checkNotComplete() {
    if (!mCompleted) {
      throw new IllegalStateException("PLAIN TBDS authentication not completed");
    }
  }

  /**
   * This class is used to create an instances of {@link PlainTbdsServer}. The parameter mechanism
   * must be "PLAIN" when this Factory is called, or null will be returned.
   */
  @ThreadSafe
  public static class Factory implements SaslServerFactory {
    /**
     * Constructs a new {@link Factory} for the {@link PlainTbdsServer}.
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
      Preconditions.checkArgument(PlainTbdsServerProvider.MECHANISM.equals(mechanism));
      return new PlainTbdsServer();
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      return new String[] {PlainTbdsServerProvider.MECHANISM};
    }
  }
}
