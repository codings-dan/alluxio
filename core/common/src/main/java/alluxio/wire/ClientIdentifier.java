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

import java.io.Serializable;
import java.util.Objects;

public class ClientIdentifier implements Serializable {
  private static final long serialVersionUID = -1068844806269634958L;
  private String mHost = "";
  private String mContainerHost = "";
  private int mPid;

  public ClientIdentifier() {
    this("", "");
  }

  /**
   * create a new instance of {@link ClientIdentifier }.
   * @param host the host of client
   * @param containerHost the container host of client, default to empty string if the client
   * is not in a container
   */
  public ClientIdentifier(String host, String containerHost) {
    mHost = host;
    mContainerHost = containerHost;
    mPid = -1;
  }

  /**
   * create a new instance of {@link ClientIdentifier }.
   * @param host the host of client
   * @param containerHost the container host of client, default to empty string if the client
   * is not in a container
   * @param pid the pid of client
   */
  public ClientIdentifier(String host, String containerHost, int pid) {
    mHost = host;
    mContainerHost = containerHost;
    mPid = pid;
  }

  /**
   * Gets the host of the client.
   * @return the host of client
   */
  public String getHost() {
    return mHost;
  }

  /**
   * Gets the container host of the client.
   * @return the container host of the client, default to empty string if the client
   * is not in a container
   */
  public String getContainerHost() {
    return mContainerHost;
  }

  /**
   * Gets the pid of the client.
   * @return the pid of the client
   */
  public int getPid() {
    return mPid;
  }

  /**
   * Sets the host of the client.
   * @param host the host of the client
   */
  public void setHost(String host) {
    mHost = host;
  }

  /**
   * Sets the container host of the client.
   * @param containerHost the container host of the client
   */
  public void setContainerHost(String containerHost) {
    mContainerHost = containerHost;
  }

  /**
   * Sets the pid of client.
   * @param pid the pid of client
   */
  public void setPid(int pid) {
    mPid = pid;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClientIdentifier)) {
      return false;
    }
    ClientIdentifier that = (ClientIdentifier) o;
    return mPid == that.mPid && mHost.equals(that.mHost) && Objects.equals(mContainerHost,
        that.mContainerHost);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mHost, mContainerHost, mPid);
  }
}
