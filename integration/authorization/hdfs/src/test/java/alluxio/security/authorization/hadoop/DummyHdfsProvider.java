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

package alluxio.security.authorization.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;

/**
 * A test implementation of HDFS {@link INodeAttributeProvider} which allows testing configuration
 * passing and default {@link AccessControlEnforcer}.
 */
public class DummyHdfsProvider extends INodeAttributeProvider implements Configurable {
  private org.apache.hadoop.conf.Configuration mConf;

  @Override
  public void setConf(org.apache.hadoop.conf.Configuration conf) {
    mConf = conf;
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public INodeAttributes getAttributes(String[] strings, INodeAttributes iNodeAttributes) {
    return iNodeAttributes;
  }

  @Override
  public org.apache.hadoop.conf.Configuration getConf() {
    return mConf;
  }
}
