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

package alluxio.util.webui;

import alluxio.wire.ClientInfo;

import java.util.Objects;

/**
 * Class to make referencing client nodes more intuitive. Mainly to avoid implicit association by
 * array indexes.
 */

public final class ClientNodeInfo implements Comparable<ClientNodeInfo> {
  private final long mClientId;
  private final long mMetadataCacheSize;
  private final String mHost;
  private final String mLastContact;
  private final String mStartTimeMs;
  private final int mPid;

  /**
   * Instantiates a new ClientNode info.
   * @param clientInfo the client info
   */
  public ClientNodeInfo(ClientInfo clientInfo) {
    if (!clientInfo.getClientIdentifier().getContainerHost().equals("")) {
      mHost = String.format("%s (%s)", clientInfo.getClientIdentifier().getHost(),
          clientInfo.getClientIdentifier().getContainerHost());
    } else {
      mHost = clientInfo.getClientIdentifier().getHost();
    }
    mPid = clientInfo.getClientIdentifier().getPid();
    mLastContact = Long.toString(
        (System.currentTimeMillis() - clientInfo.getLastContactMs()) / 1000);
    mStartTimeMs = clientInfo.getStartTimeMs().toString();
    mClientId = clientInfo.getId();
    mMetadataCacheSize = clientInfo.getMetadataCacheSize();
  }

  /**
   * Gets client id.
   * @return the client id
   */
  public long getClientId() {
    return mClientId;
  }

  /**
   * Gets client host information.
   * @return client host information
   */
  public String getHost() {
    return mHost;
  }

  /**
   * Gets last heartbeat.
   * @return the time of the last client heartbeat
   */
  public String getLastHeartbeat() {
    return mLastContact;
  }

  /**
   * Gets register time.
   * @return the time of client register to master
   */
  public String getRegisterTime() {
    return mStartTimeMs;
  }

  /**
   * Gets client metadata cache size.
   * @return the size of client metadata cache
   */
  public long getMetadataCacheSize() {
    return mMetadataCacheSize;
  }

  /**
   * Gets client pid.
   * @return the pid of client
   */
  public int getPid() {
    return mPid;
  }

  /**
   * Compare {@link ClientNodeInfo} by lexicographical order of their associated host.
   *
   * @param o the comparison term
   * @return a positive value if {@code this.getHost} is lexicographically "bigger" than
   *         {@code o.getHost}, 0 if the hosts are equal, a negative value otherwise.
   */
  @Override
  public int compareTo(ClientNodeInfo o) {
    if (o == null) {
      return 1;
    } else {
      return this.getHost().compareTo(o.getHost());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ClientNodeInfo)) {
      return false;
    }
    return this.getHost().equals(((ClientNodeInfo) o).getHost());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.getHost());
  }
}
