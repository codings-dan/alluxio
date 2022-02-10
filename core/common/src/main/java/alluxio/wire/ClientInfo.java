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
import java.util.Date;

public class ClientInfo implements Serializable {

  private static final long serialVersionUID = -4781730459655926089L;
  private long mId;
  private ClientIdentifier mClientIdentifier = new ClientIdentifier();
  private long mLastContactMs;
  private Date mStartTimeMs;
  private long mMetadataCacheSize;
  private int mPid;

  /**
   * Creates a new instance of {@link ClientInfo}.
   */
  public ClientInfo() {
  }

  public ClientInfo(long clientId, ClientIdentifier clientIdentifier) {
    mId = clientId;
    mClientIdentifier = clientIdentifier;
    mPid = clientIdentifier.getPid();
  }

  /**
   * Gets client id.
   * @return client id
   */
  public long getId() {
    return mId;
  }

  /**
   * Sets client id.
   * @param id the client id
   */
  public void setId(long id) {
    mId = id;
  }

  /**
   * Gets last time client connect to master.
   * @return last time client connect to master in seconds
   */
  public long getLastContactMs() {
    return mLastContactMs;
  }

  /**
   * Sets last time client connect to master.
   * @param lastContactMs last time client connect to master in seconds
   */
  public void setLastContactMs(long lastContactMs) {
    mLastContactMs = lastContactMs;
  }

  /**
   * Gets the client start time.
   * @return the client start time
   */
  public Date getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * Sets the client start time.
   * @param startTimeMs the client start time
   */
  public void setStartTimeMs(long startTimeMs) {
    mStartTimeMs = new Date(startTimeMs);
  }

  /**
   * Gets the client host information.
   * @return client host information
   */
  public ClientIdentifier getClientIdentifier() {
    return mClientIdentifier;
  }

  /**
   * Sets the client host information.
   * @param clientIdentifier the client host information
   */
  public void setClientIdentifier(ClientIdentifier clientIdentifier) {
    mClientIdentifier = clientIdentifier;
  }

  /**
   * Gets the client metadata cache size.
   * @return the client metadata cache size
   */
  public long getMetadataCacheSize() {
    return mMetadataCacheSize;
  }

  /**
   * Sets the client metadata cache size.
   * @param metadataCacheSize the client metadata cache size
   */
  public void setMetadataCacheSize(long metadataCacheSize) {
    mMetadataCacheSize = metadataCacheSize;
  }

  /**
   * Gets the pid of client.
   * @return the client pid
   */
  public int getPid() {
    return mPid;
  }

  /**
   * Sets the pid of client.
   * @param pid the client pid
   */
  public void setPid(int pid) {
    mPid = pid;
  }
}
