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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.HmacUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.io.UnsupportedEncodingException;
import java.security.SecureRandom;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

/**
 * A client side callback to put application provided
 * username/password/secureid/securekey into TBDS SASL transport.
 */
public final class PlainTbdsClientCallbackHandler implements CallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PlainTbdsClientCallbackHandler.class);

  private final String mUserName;
  private final String mPassword;
  private final String mSecureId;
  private final String mSecureKey;
  private static final String DPASSWORD = "PLACEHOLDER";
  private SecureRandom mRandom;

  /**
   * Constructs a new client side callback.
   *
   * @param userName the name of the user
   * @param password the password
   * @param secureId the secureid
   * @param secureKey the securekey
   */
  public PlainTbdsClientCallbackHandler(
      String userName, String password, String secureId, String secureKey) {
    mUserName = userName;
    mPassword = password;
    mSecureId = secureId;
    mSecureKey = secureKey;
    mRandom = new SecureRandom();
  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    for (Callback callback : callbacks) {
      if (callback instanceof NameCallback) {
        long curTime = System.currentTimeMillis();
        int random = mRandom.nextInt();
        String signature = generateSignature(mSecureId, curTime, random, mSecureKey);
        NameCallback nameCallback = (NameCallback) callback;
        nameCallback.setName(mSecureId + Constants.PLAIN_TBDS_AUTHCINFO_SEP
            + curTime + Constants.PLAIN_TBDS_AUTHCINFO_SEP + random
            + Constants.PLAIN_TBDS_AUTHCINFO_SEP + signature);
      } else if (callback instanceof PasswordCallback) {
        PasswordCallback passCallback = (PasswordCallback) callback;
        passCallback.setPassword(mPassword == null
            ? DPASSWORD.toCharArray() : mPassword.toCharArray());
      } else {
        Class<?> callbackClass = (callback == null) ? null : callback.getClass();
        throw new UnsupportedCallbackException(callback, callbackClass + " is unsupported.");
      }
    }
  }

  private String generateSignature(
      String secureId, long timestamp, int randomValue, String secureKey) {
    Base64 base64 = new Base64();
    byte[] baseStr = base64.encode(
        HmacUtils.hmacSha1(secureKey, secureId + timestamp + randomValue));

    String result = "";
    try {
      result = URLEncoder.encode(new String(baseStr), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.error("Failed to encode.", e);
    }

    return  result;
  }
}
