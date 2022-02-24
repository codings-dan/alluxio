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

package alluxio.wire;

import alluxio.util.webui.ClientNodeInfo;

import java.io.Serializable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI clients information.
 */
@NotThreadSafe
public class MasterWebUIClients implements Serializable {

  private static final long serialVersionUID = 3508915032997968110L;
  private ClientNodeInfo[] mNormalClientNodeInfos;
  private ClientNodeInfo[] mFailedClientNodeInfos;

  /**
   * Creates a new instance of {@link MasterWebUIClients}.
   */
  public MasterWebUIClients() {
  }

  /**
   * Gets the client infos.
   * @return the client info []
   */
  public ClientNodeInfo[] getNormalClientNodeInfos() {
    return mNormalClientNodeInfos;
  }

  /**
   * Gets the lost client infos.
   * @return the lost client info []
   */
  public ClientNodeInfo[] getFailedClientNodeInfos() {
    return mFailedClientNodeInfos;
  }

  /**
   * Sets the client infos.
   * @param normalClientNodeInfos the client infos
   */
  public void setNormalClientNodeInfos(ClientNodeInfo[] normalClientNodeInfos) {
    mNormalClientNodeInfos = normalClientNodeInfos.clone();
  }

  /**
   * Sets the lost client infos.
   * @param failedClientNodeInfos the lost client infos
   */
  public void setFailedClientNodeInfos(ClientNodeInfo[] failedClientNodeInfos) {
    mFailedClientNodeInfos = failedClientNodeInfos.clone();
  }
}
