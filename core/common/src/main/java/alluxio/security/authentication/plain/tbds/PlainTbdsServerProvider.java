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

import javax.annotation.concurrent.ThreadSafe;
import java.security.Provider;

/**
 * The Java SunSASL provider supports CRAM-MD5, DIGEST-MD5 and GSSAPI mechanisms on the server side.
 * When the SASL is using PLAIN mechanism, there is no support the SASL server. So there is a new
 * provider needed to register to support server-side PLAIN mechanism.
 * <p/>
 * Three basic steps to complete a SASL security provider:
 * <ol>
 * <li>Implements {@link PlainTbdsServer} class which extends {@link javax.security.sasl.SaslServer}
 * interface</li>
 * <li>Provides {@link PlainTbdsServer.Factory} class that implements
 * {@link javax.security.sasl.SaslServerFactory} interface</li>
 * <li>Provides a JCA provider that registers the factory</li>
 * </ol>
 */
@ThreadSafe
public final class PlainTbdsServerProvider extends Provider {
  private static final long serialVersionUID = 4583558117355348639L;

  public static final String NAME = "PlainTbds";
  public static final String MECHANISM = "TBDS";
  public static final double VERSION = 1.0;

  /**
   * Constructs a new provider for the SASL server when using the PLAIN TBDS mechanism.
   */
  public PlainTbdsServerProvider() {
    super(NAME, VERSION, "Plain Tbds server provider");
    put("SaslServerFactory." + MECHANISM, PlainTbdsServer.Factory.class.getName());
  }
}
