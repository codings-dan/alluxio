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

package org.apache.hadoop.fs.cosranger.common;

import alluxio.Constants;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.TxPropertyKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.cosn.ranger.constants.ObjectStorageConstants;
import org.apache.hadoop.fs.cosn.ranger.ranger.RangerAuthorizer;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.AccessType;
import org.apache.hadoop.fs.permission.FsAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * cosranger utils methods.
 */
public final class CosRangerUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CosRangerUtils.class);

  public static RangerAuthorizer buildAndInitCosRanger() {
    Configuration conf = new Configuration();
    String confDirPath = ServerConfiguration.getString(
            TxPropertyKey.ALLUXIO_COS_QCLOUD_OBJECT_STORAGE_CONFIG_DIR_KEY);
    if (confDirPath == null) {
      throw new RuntimeException(String.format(
              "conf %s is missing!", ObjectStorageConstants.QCLOUD_OBJECT_STORAGE_CONFIG_DIR_KEY));
    }
    File confDir = new File(confDirPath);
    if (!confDir.isDirectory()) {
      throw new RuntimeException(String.format("conf dir %s not exist!", confDirPath));
    }

    String[] confNameArray = {"cos-ranger.xml", "ranger-cos-audit.xml",
        "ranger-cos-security.xml", "ranger-chdfs-audit.xml", "ranger-chdfs-security.xml"};
    for (String confName : confNameArray) {
      File confFile = new File(confDir, confName);
      if (confFile.exists() && confFile.canRead()) {
        conf.addResource(new Path(confFile.getAbsolutePath()));
      }
    }
    conf.addResource("hadoop-policy.xml");
    conf.addResource("hdfs-site.xml");
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    conf.reloadConfiguration();
    for (String propName : System.getProperties().stringPropertyNames()) {
      conf.set(propName, System.getProperty(propName));
    }
    boolean enableCosRanger = conf.getBoolean(
            ObjectStorageConstants.QCLOUD_OBJECT_STORAGE_ENABLE_COS_RANGER, false);
    boolean enableChdfsRanger = conf.getBoolean(
            ObjectStorageConstants.QCLOUD_OBJECT_STORAGE_ENABLE_CHDFS_RANGER, false);
    String cosServiceName = conf.get(
            ObjectStorageConstants.QCLOUD_OBJECT_STORAGE_RANGER_COS_SERVICE_NAME);
    String chdfsServiceName = conf.get(
            ObjectStorageConstants.QCLOUD_OBJECT_STORAGE_RANGER_CHDFS_SERVICE_NAME);
    boolean cosBucketAutoRemoveEnable = conf.getBoolean(
            ObjectStorageConstants.QCLOUD_OBJECT_BUCKET_NAME_AUTO_REMOVE_ENABLED, false);
    String appId = conf.get(
            ObjectStorageConstants.QCLOUD_OBJECT_STORAGE_APPID);
    if (enableCosRanger && enableChdfsRanger) {
      if (cosServiceName == null || chdfsServiceName == null
              || chdfsServiceName.trim().isEmpty() || cosServiceName.trim().isEmpty()) {
        try {
          throw new IOException(String.format("cos config %s and chdfs config %s is missing",
                  ObjectStorageConstants.QCLOUD_OBJECT_STORAGE_RANGER_COS_SERVICE_NAME,
                  ObjectStorageConstants.QCLOUD_OBJECT_STORAGE_RANGER_CHDFS_SERVICE_NAME));
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      }
    }
    if (cosBucketAutoRemoveEnable) {
      if (appId == null || appId.trim().isEmpty()) {
        try {
          throw new IOException(String.format("cos bucket auto remove enable is true but"
                  + " cos-ranger.xml config %s is missing or empty",
                  ObjectStorageConstants.QCLOUD_OBJECT_STORAGE_APPID));
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      }
    }
    LOG.info("build RangerAuthorizer for enableCosRanger: {}, enableChdfsRanger: {},"
                    + " cosServiceName: {}, chdfsServiceName: {}, cosBucketAutoRemoveEnable: {},"
                    + " appId: {}", enableCosRanger, enableChdfsRanger, cosServiceName,
            chdfsServiceName, cosBucketAutoRemoveEnable, appId);
    return new RangerAuthorizer(
            enableCosRanger, enableChdfsRanger, cosServiceName,
            chdfsServiceName, cosBucketAutoRemoveEnable, appId);
  }

  public static String pathToKey(Path path) {
    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      // allow uris without trailing slash after bucket to refer to root
      // like ofs://
      return "";
    }
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    String ret = path.toUri().getPath();
    if (ret.endsWith(Constants.ROOT_PATH)
            && (ret.indexOf(Constants.ROOT_PATH) != ret.length() - 1)) {
      ret = ret.substring(0, ret.length() - 1);
    }
    return ret;
  }

  public static AccessType convertAction(FsAction alluxioFsAction) {
    String cosRangerAction = alluxioFsAction.toString();
    switch (cosRangerAction) {
      case "LIST" : {
        return AccessType.LIST;
      }
      case "WRITE" : {
        return AccessType.WRITE;
      }
      case "READ" : {
        return AccessType.READ;
      }
      default: {
        throw new UnsupportedOperationException(
                "Unsupported COSRANGER ACL permission: " + (alluxioFsAction.toString()));
      }
    }
  }
}
