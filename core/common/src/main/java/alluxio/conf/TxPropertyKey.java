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

package alluxio.conf;

import alluxio.annotation.PublicApi;
import alluxio.conf.PropertyKey.Builder;
import alluxio.conf.PropertyKey.ConsistencyCheckLevel;
import alluxio.conf.PropertyKey.DisplayType;
import alluxio.grpc.Scope;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration property keys. This class provides a set of pre-defined property keys.
 */
@ThreadSafe
@PublicApi
public final class TxPropertyKey {

  /**
   * Master related properties.
   */
  public static final PropertyKey MASTER_FILE_METADATA_SYNC_INTERVAL =
      new Builder(Name.MASTER_FILE_METADATA_SYNC_INTERVAL)
          .setDefaultValue("-1")
          .setDescription("The interval for syncing UFS metadata before invoking an "
              + "operation on a path. -1 means no sync will occur. 0 means Alluxio will "
              + "always sync the metadata of the path before an operation. If you specify a time "
              + "interval, Alluxio will (best effort) not re-sync a path within that time "
              + "interval. Syncing the metadata for a path must interact with the UFS, so it is "
              + "an expensive operation. If a sync is performed for an operation, the "
              + "configuration of \"alluxio.master.file.metadata.sync.list\" should be set.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_FILE_METADATA_SYNC_LIST =
      new Builder(Name.MASTER_FILE_METADATA_SYNC_LIST)
          .setDefaultValue("")
          .setDescription("A comma-separated list of the paths which are "
              + "configured to be synced, separated by semi-colons.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_LOST_CLIENT_DETECTION_INTERVAL =
      new Builder(Name.MASTER_LOST_CLIENT_DETECTION_INTERVAL)
          .setDefaultValue("20sec")
          .setAlias("alluxio.master.worker.heartbeat.interval")
          .setDescription("The interval between Alluxio master detections to find lost clients "
              + "based on updates from Alluxio workers.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.SERVER)
          .build();
  public static final PropertyKey MASTER_CLIENT_TIMEOUT_MS =
      new Builder(Name.MASTER_CLIENT_TIMEOUT_MS)
          .setAlias("alluxio.master.client.timeout.ms")
          .setDefaultValue("5min")
          .setDescription("Timeout between master and client indicating a lost client.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_COUNT_TO_REMOVE_BLOCKS_ENABLE =
      new Builder(Name.MASTER_COUNT_TO_REMOVE_BLOCKS_ENABLE)
          .setAlias("alluxio.master.count.to.remove.blocks.enable")
          .setDefaultValue(false)
          .setDescription("Whether enable master collect the value of blocks to remove,"
              + "which is use in the metric MASTER_TO_REMOVE_BLOCK_COUNT.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey  MASTER_LIST_CONCURRENT_ENABLED =
      new Builder(Name.MASTER_LIST_CONCURRENT_ENABLED)
          .setDefaultValue(false)
          .setDescription("Whether to enable the concurrent list.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();
  public static final PropertyKey MASTER_LIST_STATUS_EXECUTOR_POOL_SIZE =
      new Builder(Name.MASTER_LIST_STATUS_EXECUTOR_POOL_SIZE)
          .setDefaultSupplier(() -> Runtime.getRuntime().availableProcessors(),
              "The total number of threads which can concurrently execute list status "
                  + "operations.")
          .setDescription("The number of threads used to execute list status "
              + "operations")
          .setScope(Scope.MASTER)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey MASTER_SLOW_LIST_OPERATION_THRESHOLD =
      new Builder(Name.MASTER_SLOW_LIST_OPERATION_THRESHOLD)
          .setDefaultValue(10000)
          .setDescription("The threshold for slow list operation")
          .setScope(Scope.MASTER)
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .build();
  public static final PropertyKey MASTER_METASTORE_BLOCK_STORE_DIR =
      new Builder(Name.MASTER_METASTORE_BLOCK_STORE_DIR)
          .setDefaultValue(String.format("${%s}", PropertyKey.Name.MASTER_METASTORE_DIR))
          .setDescription("The block store metastore work directory. "
              + "Only some metastores need disk.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();

  //
  // Shimfs  related properties
  //
  public static final PropertyKey MASTER_SHIMFS_AUTO_MOUNT_ENABLED =
      new Builder(Name.MASTER_SHIMFS_AUTO_MOUNT_ENABLED)
          .setDescription("If enabled, Alluxio will attempt to mount UFS for foreign URIs.")
          .setDefaultValue(Boolean.valueOf(false))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_SHIMFS_AUTO_MOUNT_ROOT =
      new Builder(Name.MASTER_SHIMFS_AUTO_MOUNT_ROOT)
          .setDescription("Alluxio root path for auto-mounted UFSes. "
              + "This directory should already exist in Alluxio.")
          .setDefaultValue("/auto-mount")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_SHIMFS_AUTO_MOUNT_READONLY =
      new Builder(Name.MASTER_SHIMFS_AUTO_MOUNT_READONLY)
          .setDescription("If true, UFSes are auto-mounted as read-only.")
          .setDefaultValue(Boolean.valueOf(true))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_SHIMFS_AUTO_MOUNT_SHARED =
      new Builder(Name.MASTER_SHIMFS_AUTO_MOUNT_SHARED)
          .setDescription("If true, UFSes are auto-mounted as shared.")
          .setDefaultValue(Boolean.valueOf(false))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey USER_SHIMFS_BYPASS_PREFIX_LIST =
      new Builder(Name.USER_SHIMFS_BYPASS_PREFIX_LIST)
          .setDescription("A comma-separated list of prefix paths to by-pass. "
              + "User classpath should contain a native hadoop FileSystem implementation"
              + " for target scheme. \n"
              + String.format("For example: \"%s=s3://bucket1/foo, s3://bucket1/bar\"",
                  Name.USER_SHIMFS_BYPASS_PREFIX_LIST))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_SHIMFS_BYPASS_UFS_IMPL_LIST =
      new Builder(Name.USER_SHIMFS_BYPASS_UFS_IMPL_LIST)
          .setDescription("A Set of Hadoop FileSystem implementation lists for ufs by-pass. "
              + "User use ':' separate different FileSystem for target scheme. \n"
              + String.format("For example:fs.s3n.impl:com.amazon.ws.emr.hadoop.fs.EmrFileSystem,"
                  + "fs.ofs.impl:com.qcloud.chdfs.fs.CHDFSHadoopFileSystemAdapter",
              Name.USER_SHIMFS_BYPASS_UFS_IMPL_LIST))
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey MASTER_URI_TRANSLATOR_IMPL =
      new Builder(Name.MASTER_URI_TRANSLATOR_IMPL)
          .setDefaultValue("alluxio.master.file.uritranslator.DefaultUriTranslator")
          .setDescription("The class of uri translator implementation.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey MASTER_COMPOSITE_URI_TRANSLATOR_IMPL =
      new Builder(Name.MASTER_COMPOSITE_URI_TRANSLATOR_IMPL)
          .setDefaultValue("/=alluxio.master.file.uritranslator.DefaultUriTranslator")
          .setDescription("The class of uri translator implementations, comma separated.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.MASTER)
          .build();
  //
  // Shimfs  fallback related properties
  //
  public static final PropertyKey USER_FALLBACK_ENABLED =
      new Builder(Name.USER_FALLBACK_ENABLED)
          .setDefaultValue(false)
          .setDescription("Shimfs support fallback enabled.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_LAZY_FALLBACK_TIMEOUT =
      new Builder(Name.USER_LAZY_FALLBACK_TIMEOUT)
          .setDefaultValue(60000)
          .setDescription("Shimfs lazy fallback timeout.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FALLBACK_RETRY_BASE_SLEEP_MS =
      new Builder(Name.USER_FALLBACK_RETRY_BASE_SLEEP_MS)
          .setDefaultValue(50)
          .setDescription("Shimfs fallback retry base sleep time.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FALLBACK_RETRY_MAX_SLEEP_MS =
      new Builder(Name.USER_FALLBACK_RETRY_MAX_SLEEP_MS)
          .setDefaultValue(100)
          .setDescription("Shimfs fallback retry MAX sleep times.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_FALLBACK_RETRY_MAX_TIMES =
      new Builder(Name.USER_FALLBACK_RETRY_MAX_TIMES)
          .setDefaultValue(2)
          .setDescription("Shimfs fallback MAX retry times.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();

  //
  // Security related properties
  //
  public static final PropertyKey UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME =
      new Builder(Name.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME)
          .setDescription("Name of the authorization plugin for the under filesystem.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_PATHS =
      new PropertyKey.Builder(Name.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_PATHS)
          .setDescription("Classpaths for the under filesystem authorization plugin,"
              + " separated by colons.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PLUGIN_NAME =
      new Builder(Name.SECURITY_AUTHORIZATION_PLUGIN_NAME)
          .setDescription("Plugin for master authorization.")
          .setConsistencyCheckLevel(PropertyKey.ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PLUGIN_PATHS =
      new Builder(Name.SECURITY_AUTHORIZATION_PLUGIN_PATHS)
          .setDescription("Classpath for master authorization plugin, separated by colons.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PLUGINS_ENABLED =
      new Builder(Name.SECURITY_AUTHORIZATION_PLUGINS_ENABLED)
          .setDefaultValue(false)
          .setDescription("Enable plugins for authorization.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PLUGINS_EXTERNAL_UFS_NAMESPACE_ENABLED =
      new Builder(Name.SECURITY_AUTHORIZATION_PLUGINS_EXTERNAL_UFS_NAMESPACE_ENABLED)
          .setDefaultValue(false)
          .setDescription("Enable convert to external ufs namespace Uri.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey SECURITY_AUTHORIZATION_PLUGIN_HDFS_COMPATIBLE_OZONE_ENABLED =
      new Builder(Name.SECURITY_AUTHORIZATION_PLUGIN_HDFS_COMPATIBLE_OZONE_ENABLED)
          .setDefaultValue(false)
          .setDescription("enable HDFS authorization plugin compatible with ozone")
          .setConsistencyCheckLevel(PropertyKey.ConsistencyCheckLevel.ENFORCE)
          .setScope(Scope.MASTER)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_URL =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_URL)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_SSL =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_SSL)
          .setDefaultValue(false)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD)
          .setDisplayType(DisplayType.CREDENTIALS)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD_FILE =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD_FILE)
          .setDisplayType(DisplayType.CREDENTIALS)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_BIND_USER =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_BIND_USER)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD)
          .setDisplayType(DisplayType.CREDENTIALS)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD_FILE =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD_FILE)
          .setDisplayType(DisplayType.CREDENTIALS)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_BASE =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_BASE)
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_SEARCH_FILTER_USER =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_SEARCH_FILTER_USER)
          .setDefaultValue("(&(objectClass=user)(sAMAccountName={0}))")
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_SEARCH_FILTER_GROUP =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_SEARCH_FILTER_GROUP)
          .setDefaultValue("(objectClass=group)")
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_SEARCH_TIMEOUT =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_SEARCH_TIMEOUT)
          .setDefaultValue("10000")
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_ATTR_MEMBER =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_ATTR_MEMBER)
          .setDefaultValue("member")
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_ATTR_GROUP_NAME =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_ATTR_GROUP_NAME)
          .setDefaultValue("cn")
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_ATTR_POSIX_UID =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_ATTR_POSIX_UID)
          .setDefaultValue("uidNumber")
          .build();
  public static final PropertyKey SECURITY_GROUP_MAPPING_LDAP_ATTR_POSIX_GID =
      new Builder(Name.SECURITY_GROUP_MAPPING_LDAP_ATTR_POSIX_GID)
          .setDefaultValue("gidNumber")
          .build();

  //
  // Worker related properties
  //
  public static final PropertyKey WORKER_BLOCK_ANNOTATOR_ENABLED =
      new Builder(Name.WORKER_BLOCK_ANNOTATOR_ENABLED)
          .setDefaultValue(true)
          .setDescription("If false, the worker will not evict when insufficient space for "
              + "worker.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.WORKER)
          .build();

  //
  // User related properties
  //
  public static final PropertyKey USER_COMMAND_HEARTBEAT_ENABLED =
      new Builder(Name.USER_COMMAND_HEARTBEAT_ENABLED)
          .setDefaultValue(false)
          .setDescription("Enable client get journal index from master")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_COMMAND_HEARTBEAT_INTERVAL_MS =
      new Builder(Name.USER_COMMAND_HEARTBEAT_INTERVAL_MS)
          .setAlias("alluxio.user.command.heartbeat.interval.ms")
          .setDefaultValue("5min")
          .setDescription("The time period of client master heartbeat to "
              + "get the journal index from master.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();
  public static final PropertyKey USER_CONTAINER_HOSTNAME =
      new Builder(Name.USER_CONTAINER_HOSTNAME)
          .setDescription("The container hostname if client is running in a container.")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.IGNORE)
          .setScope(Scope.WORKER)
          .build();

  //
  // Fuse related properties
  //
  public static final PropertyKey FUSE_WORKAROUND_LIST =
      new Builder(Name.FUSE_WORKAROUND_LIST)
          .setDefaultValue("")
          .setConsistencyCheckLevel(ConsistencyCheckLevel.WARN)
          .setScope(Scope.CLIENT)
          .build();

  /**
   * A nested class to hold named string constants for their corresponding properties.
   * Used for setting configuration in integration tests.
   */
  @ThreadSafe
  public static final class Name {
    //
    // Master related properties
    //
    public static final String MASTER_FILE_METADATA_SYNC_INTERVAL =
        "alluxio.master.file.metadata.sync.interval";
    public static final String MASTER_FILE_METADATA_SYNC_LIST =
        "alluxio.master.file.metadata.sync.list";
    public static final String MASTER_LOST_CLIENT_DETECTION_INTERVAL =
        "alluxio.master.lost.client.detection.interval";
    public static final String MASTER_CLIENT_TIMEOUT_MS = "alluxio.master.client.timeout";
    public static final String MASTER_COUNT_TO_REMOVE_BLOCKS_ENABLE =
        "alluxio.master.count.to.remove.blocks.enable";
    public static final String MASTER_LIST_CONCURRENT_ENABLED =
        "alluxio.master.list.concurrent.enabled";
    public static final String MASTER_LIST_STATUS_EXECUTOR_POOL_SIZE =
        "alluxio.master.list.status.executor.pool.size";
    public static final String MASTER_SLOW_LIST_OPERATION_THRESHOLD =
        "alluxio.master.slow.list.operation.threshold";

    public static final String MASTER_METASTORE_BLOCK_STORE_DIR =
        "alluxio.master.metastore.block.store.dir";
    //
    // Worker related properties
    //
    public static final String WORKER_BLOCK_ANNOTATOR_ENABLED =
        "alluxio.worker.block.annotator.enabled";

    //
    // User related properties
    //
    public static final String USER_COMMAND_HEARTBEAT_ENABLED =
        "alluxio.user.command.heartbeat.enabled";
    public static final String USER_COMMAND_HEARTBEAT_INTERVAL_MS =
        "alluxio.user.command.heartbeat.interval.ms";
    public static final String USER_CONTAINER_HOSTNAME =
        "alluxio.user.container.hostname";

    //
    // Fuse related properties
    //
    public static final String FUSE_WORKAROUND_LIST =
        "alluxio.fuse.workaround.list";

    //
    // Shimfs related properties
    //
    public static final String MASTER_SHIMFS_AUTO_MOUNT_ENABLED =
        "alluxio.master.shimfs.auto.mount.enabled";
    public static final String MASTER_SHIMFS_AUTO_MOUNT_ROOT =
        "alluxio.master.shimfs.auto.mount.root";
    public static final String MASTER_SHIMFS_AUTO_MOUNT_READONLY =
        "alluxio.master.shimfs.auto.mount.readonly";
    public static final String MASTER_SHIMFS_AUTO_MOUNT_SHARED =
        "alluxio.master.shimfs.auto.mount.shared";
    public static final String USER_SHIMFS_BYPASS_PREFIX_LIST =
        "alluxio.user.shimfs.bypass.prefix.list";
    public static final String USER_SHIMFS_BYPASS_UFS_IMPL_LIST =
        "alluxio.user.shimfs.bypass.ufs.impl.list";

    // Shimfs extended properties
    public static final String MASTER_URI_TRANSLATOR_IMPL =
        "alluxio.master.uri.translator.impl";
    public static final String MASTER_COMPOSITE_URI_TRANSLATOR_IMPL =
        "alluxio.master.composite.uri.translator.impl";

    //Shimfs fall-back  properties
    public static final String USER_FALLBACK_ENABLED =
        "alluxio.user.shimfs.fallback.enabled";
    public static final String USER_LAZY_FALLBACK_TIMEOUT =
        "alluxio.user.lazy.fallback.timeout";
    public static final String USER_FALLBACK_RETRY_BASE_SLEEP_MS =
        "alluxio.user.fallback.retry.base.sleep.ms";
    public static final String USER_FALLBACK_RETRY_MAX_SLEEP_MS =
        "alluxio.user.fallback.retry.max.sleep.ms";
    public static final String USER_FALLBACK_RETRY_MAX_TIMES =
        "alluxio.user.fallback.retry.max.times";

    //
    // Security related properties
    //
    public static final String SECURITY_AUTHORIZATION_PLUGIN_NAME =
        "alluxio.security.authorization.plugin.name";
    public static final String SECURITY_AUTHORIZATION_PLUGIN_PATHS =
        "alluxio.security.authorization.plugin.paths";
    public static final String SECURITY_AUTHORIZATION_PLUGINS_ENABLED =
        "alluxio.security.authorization.plugins.enabled";
    public static final String SECURITY_AUTHENTICATION_TYPE =
        "alluxio.security.authentication.type";
    public static final String SECURITY_AUTHORIZATION_PERMISSION_ENABLED =
        "alluxio.security.authorization.permission.enabled";
    public static final String SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP =
        "alluxio.security.authorization.permission.supergroup";
    public static final String SECURITY_AUTHORIZATION_PERMISSION_UMASK =
        "alluxio.security.authorization.permission.umask";
    public static final String SECURITY_GROUP_MAPPING_CLASS =
        "alluxio.security.group.mapping.class";
    public static final String SECURITY_LOGIN_USERNAME = "alluxio.security.login.username";

    // ldap group mapping related properties
    public static final String SECURITY_GROUP_MAPPING_LDAP_URL =
        "alluxio.security.group.mapping.ldap.url";
    public static final String SECURITY_GROUP_MAPPING_LDAP_SSL =
        "alluxio.security.group.mapping.ldap.ssl";
    public static final String SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE =
        "alluxio.security.group.mapping.ldap.ssl.keystore";
    public static final String SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD =
        "alluxio.security.group.mapping.ldap.ssl.keystore.password";
    public static final String SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD_FILE =
        "alluxio.security.group.mapping.ldap.ssl.keystore.password.file";
    public static final String SECURITY_GROUP_MAPPING_LDAP_BIND_USER =
        "alluxio.security.group.mapping.ldap.bind.user";
    public static final String SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD =
        "alluxio.security.group.mapping.ldap.bind.password";
    public static final String SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD_FILE =
        "alluxio.security.group.mapping.ldap.bind.password.file";
    public static final String SECURITY_GROUP_MAPPING_LDAP_BASE =
        "alluxio.security.group.mapping.ldap.base";
    public static final String SECURITY_GROUP_MAPPING_LDAP_SEARCH_FILTER_USER =
        "alluxio.security.group.mapping.ldap.search.filter.user";
    public static final String SECURITY_GROUP_MAPPING_LDAP_SEARCH_FILTER_GROUP =
        "alluxio.security.group.mapping.ldap.search.filter.group";
    public static final String SECURITY_GROUP_MAPPING_LDAP_SEARCH_TIMEOUT =
        "alluxio.security.group.mapping.ldap.search.timeout";
    public static final String SECURITY_GROUP_MAPPING_LDAP_ATTR_MEMBER =
        "alluxio.security.group.mapping.ldap.attr.member";
    public static final String SECURITY_GROUP_MAPPING_LDAP_ATTR_GROUP_NAME =
        "alluxio.security.group.mapping.ldap.attr.group.name";
    public static final String SECURITY_GROUP_MAPPING_LDAP_ATTR_POSIX_UID =
        "alluxio.security.group.mapping.ldap.attr.posix.uid";
    public static final String SECURITY_GROUP_MAPPING_LDAP_ATTR_POSIX_GID =
        "alluxio.security.group.mapping.ldap.attr.posix.gid";

    // Security extended properties
    public static final String SECURITY_AUTHORIZATION_PLUGINS_EXTERNAL_UFS_NAMESPACE_ENABLED =
        "alluxio.security.authorization.plugins.external.ufs.namespace.enabled";
    public static final String SECURITY_AUTHORIZATION_PLUGIN_HDFS_COMPATIBLE_OZONE_ENABLED =
        "alluxio.security.authorization.plugin.hdfs.compatible.ozone.enabled";

    // Ufs security properties
    public static final String UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME =
        "alluxio.underfs.security.authorization.plugin.name";
    public static final String UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_PATHS =
        "alluxio.underfs.security.authorization.plugin.paths";

    private Name() {} // prevent instantiation
  }
}
