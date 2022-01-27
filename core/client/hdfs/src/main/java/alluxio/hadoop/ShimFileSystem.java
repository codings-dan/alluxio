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

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.DeletePOptions;
import alluxio.util.io.PathUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An Alluxio client API compatible with Apache Hadoop {@link org.apache.hadoop.fs.FileSystem}
 * interface. Any program working with Hadoop HDFS can work with Alluxio transparently. Note that
 * the performance of using this API may not be as efficient as the performance of using the Alluxio
 * native API defined in {@link alluxio.client.file.FileSystem}, which this API is built on top of.
 *
 * ShimFileSystem supports working with arbitrary schemes that are supported by Alluxio.
 */
@PublicApi
@NotThreadSafe
public class ShimFileSystem extends AbstractFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(ShimFileSystem.class);

  private Set<URI> mByPassedPrefixSet = new HashSet<>();

  private org.apache.hadoop.fs.FileSystem mByPassFs = null;

  private boolean mFallbackEnabled;

  private FallbackManager mFallbackManager = null;

  private ReentrantReadWriteLock mHadInitLock = new ReentrantReadWriteLock();

  private boolean mAlluxioFsHadInited = false;

  /**
   * Constructs a new {@link ShimFileSystem}.
   */
  public ShimFileSystem() {
    super();
  }

  /**
   * Constructs a new {@link ShimFileSystem} instance with a specified
   * {@link alluxio.client.file.FileSystem} handler for tests.
   *
   * @param fileSystem handler to file system
   */
  public ShimFileSystem(alluxio.client.file.FileSystem fileSystem) {
    super(fileSystem);
  }

  public synchronized void initialize(URI uri, Configuration conf) throws IOException {
    try {
      super.initialize(uri, conf);
    } catch (UnavailableException e) {
      LOG.warn("Failed to initialize mAlluxioFileSystem to({},{}) e:{}", uri, conf, e);
      setAlluxioFsHadInited(false);
    }
    setAlluxioFsHadInited(true);
    LOG.debug("Successfully to mAlluxioFileSystem to {}", mFileSystem);

    mFallbackEnabled = mAlluxioConf.getBoolean(TxPropertyKey.USER_FALLBACK_ENABLED);
    if (mFallbackEnabled) {
      mFallbackManager = new FallbackManager(mAlluxioConf);
    }
    if (mAlluxioConf.isSet(TxPropertyKey.USER_SHIMFS_BYPASS_PREFIX_LIST)) {
      List<String> prefixes =
          mAlluxioConf.getList(TxPropertyKey.USER_SHIMFS_BYPASS_PREFIX_LIST, ",");
      try {
        for (String prefix : prefixes) {
          prefix = prefix.trim();
          if (!prefix.isEmpty()) {
            mByPassedPrefixSet.add(new URI(prefix));
          }
        }
      } catch (URISyntaxException e) {
        throw new IOException(String.format("By-pass configuration not correct: %s",
            mAlluxioConf.get(TxPropertyKey.USER_SHIMFS_BYPASS_PREFIX_LIST)), e);
      }
    }
    if (!mByPassedPrefixSet.isEmpty() || mFallbackEnabled) {
      try {
        Class<? extends org.apache.hadoop.fs.FileSystem> clazz =
            org.apache.hadoop.fs.FileSystem.getFileSystemClass(uri.getScheme(),
                getByPassFsImplConf(mAlluxioConf));
        mByPassFs = clazz.newInstance();
        LOG.debug("Successfully load native fs ({},{}) fro {}", uri, conf, mByPassFs);
      } catch (Exception e) {
        LOG.error("Failed to load native fs for bypassing scheme: {}. Error: {}", uri.getScheme(),
            e.getStackTrace());
        throw new IOException(String.format(
            "Failed to load native fs for bypassing scheme: %s. Error: %s", uri.getScheme()), e);
      }
      try {
        mByPassFs.initialize(uri, conf);
        LOG.debug("Successfully initialize ({},{}) fro {}", uri, conf, mByPassFs);
      } catch (Exception e) {
        LOG.error("Failed to initialize bypass fs for scheme: {}. Error: {}", uri.getScheme(),
            e.getStackTrace());
        throw new IOException(
            String.format("Failed to initialize bypass fs for scheme: %s.", uri.getScheme()), e);
      }
    }
  }

  private Configuration getByPassFsImplConf(AlluxioConfiguration alluxioConf) throws IOException {
    if (alluxioConf.isSet(TxPropertyKey.USER_SHIMFS_BYPASS_UFS_IMPL_LIST)) {
      List<String> implList =
          mAlluxioConf.getList(TxPropertyKey.USER_SHIMFS_BYPASS_UFS_IMPL_LIST, ",");
      Configuration conf = new Configuration();
      for (String impl : implList) {
        List<String> fsImpl = Lists.newArrayList(Splitter.on(":").trimResults().omitEmptyStrings()
            .split(impl));
        if (fsImpl.size() != 2) {
          throw new IOException(String.format("bad Property:%s" ,
              TxPropertyKey.USER_SHIMFS_BYPASS_UFS_IMPL_LIST));
        }
        conf.set(fsImpl.get(0), fsImpl.get(1));
        LOG.debug("Use {} item conf [{}={}] to init ByPassFs.",
            TxPropertyKey.USER_SHIMFS_BYPASS_UFS_IMPL_LIST, fsImpl.get(0), fsImpl.get(1));
      }
      return conf;
    }
    return null;
  }

  public void setAlluxioFsHadInited(boolean hadInited) {
    try {
      mHadInitLock.writeLock().lock();
      mAlluxioFsHadInited = hadInited;
    } finally {
      mHadInitLock.writeLock().unlock();
    }
  }

  public boolean alluxioFsHadInited() {
    try {
      mHadInitLock.readLock().lock();
      return mAlluxioFsHadInited;
    } finally {
      mHadInitLock.readLock().unlock();
    }
  }

  public void reInitAlluxioFs(URI uri, Configuration conf) throws IOException {
    if (alluxioFsHadInited()) {
      return;
    }
    super.initialize(uri, conf, null);
    setAlluxioFsHadInited(true);
  }

  public String getScheme() {
    //
    // {@link #getScheme()} will be used in hadoop 2.x for dynamically loading
    // filesystems based on scheme. This limits capability of ShimFileSystem
    // as it's intended to be a forwarder for arbitrary schemes.
    //
    // Hadoop currently gives configuration priority over dynamic loading, so
    // whatever scheme is configured for ShimFileSystem will be attached with a shim.
    // Below constant will basically hide ShimFileSystem from dynamic loading as
    // it maps to a bogus scheme.
    //
    return Constants.NO_SCHEME;
  }

  @Override
  protected boolean isZookeeperMode() {
    return mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
  }

  protected Map<String, Object> getConfigurationFromUri(URI uri, Configuration conf) {
    return Collections.emptyMap();
  }

  @Override
  protected void validateFsUri(URI fsUri) throws IOException {
    // No validation for ShimFS.
  }

  @Override
  protected String getFsScheme(URI fsUri) {
    // ShimFS does not know its scheme until FS URI is supplied.
    // Use base URI's scheme.
    return fsUri.getScheme();
  }

  @Override
  protected AlluxioURI getAlluxioPath(Path path) {
    // Sends the full path to Alluxio for master side resolution.
    return new AlluxioURI(path.toString());
  }

  @Override
  protected Path getFsPath(String fsUri, URIStatus fileStatus) {
    // ShimFS doesn't expose internal Alluxio path.
    return new Path(fileStatus.getUfsPath());
  }

  private boolean pathByPassed(Path path) throws IOException {
    if (mFallbackEnabled && mFallbackManager.needFallbackStatus()) {
      LOG.debug("it will fallback for path:{}", path);
      return true;
    }

    if (!alluxioFsHadInited()) {
      LOG.debug("AlluxioFileSystem had not be inited, will pass for path:{}", path);
      return true;
    }

    if (mByPassedPrefixSet.isEmpty()) {
      return false;
    }
    URI pathUri = path.toUri();
    for (URI prefixUri : mByPassedPrefixSet) {
      if (!Objects.equals(prefixUri.getScheme(), pathUri.getScheme())) {
        continue;
      }
      if (!Objects.equals(prefixUri.getAuthority(), pathUri.getAuthority())) {
        continue;
      }
      try {
        if (PathUtils.hasPrefix(pathUri.getPath(), prefixUri.getPath())) {
          return true;
        }
      } catch (InvalidPathException e) {
        throw new IOException(
            String.format("Failed to check path against by-pass prefixes. Path: %s", path), e);
      }
    }
    return false;
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.create(path, permission, overwrite, bufferSize, replication, blockSize,
            progress);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.create(path, permission, overwrite, bufferSize, replication, blockSize,
        progress);
  }

  public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.createNonRecursive(path, permission, overwrite, bufferSize, replication,
            blockSize, progress);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.createNonRecursive(path, permission, overwrite, bufferSize, replication,
        blockSize, progress);
  }

  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.append(path, bufferSize, progress);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.append(path, bufferSize, progress);
  }

  public boolean delete(Path path, boolean recursive) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.delete(path, recursive);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.delete(path, recursive);
  }

  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
      throws IOException {
    Path path = getFullPath(getUri(), file.getPath());
    if (!pathByPassed(path)) {
      try {
        return super.getFileBlockLocations(path, start, len);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.getFileBlockLocations(path, start, len);
  }

  public boolean setReplication(Path path, short replication) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.setReplication(path, replication);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.setReplication(path, replication);
  }

  public FileStatus getFileStatus(Path path) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.getFileStatus(path);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.getFileStatus(path);
  }

  public void setOwner(Path path, String username, String groupname) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        super.setOwner(path, username, groupname);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    } else {
      mByPassFs.setOwner(path, username, groupname);
    }
  }

  public void setPermission(Path path, FsPermission permission) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        super.setPermission(path, permission);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    } else {
      mByPassFs.setPermission(path, permission);
    }
  }

  public FileStatus[] listStatus(Path path) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.listStatus(path);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.listStatus(path);
  }

  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.mkdirs(path, permission);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.mkdirs(path, permission);
  }

  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.open(path, bufferSize);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.open(path, bufferSize);
  }

  public boolean rename(Path src, Path dst) throws IOException {
    src = getFullPath(getUri(), src);
    dst = getFullPath(getUri(), dst);
    boolean srcBypassed = pathByPassed(src);
    boolean dstBypassed = pathByPassed(dst);

    if (srcBypassed && dstBypassed) {
      return mByPassFs.rename(src, dst);
    }
    if (!srcBypassed && !dstBypassed) {
      return super.rename(src, dst);
    }
    // here only one is by-pass
    // FIXME: Across schema cannot support currently, we will fix it.
    if (srcBypassed) {
      try {
        mFileSystem.loadMetadata(getAlluxioPath(dst));
      } catch (AlluxioException e) {
        LOG.warn("rename failed: {}", e.getMessage());
        return false;
      }
    }
    if (dstBypassed) {
      try {
        mFileSystem.delete(getAlluxioPath(src),
            DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(true).build());
      } catch (AlluxioException e) {
        LOG.warn("rename failed: {}", e.getMessage());
        return false;
      }
    }
    return mByPassFs.rename(src, dst);
  }

  public void setWorkingDirectory(Path path) {
    path = getFullPath(getUri(), path);
    try {
      if (!pathByPassed(path)) {
        super.setWorkingDirectory(path);
        return;
      }
    } catch (UnavailableException e) {
      mFallbackManager.markFallBack(this);
    } catch (IOException e) {
      LOG.error("path:{} setWorkingDirectory appear exception {}", path, e);
    }
    mByPassFs.setWorkingDirectory(path);
  }

  public FileChecksum getFileChecksum(Path path) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.getFileChecksum(path);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.getFileChecksum(path);
  }

  public void setXAttr(Path path, String name, byte[] value) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        super.setXAttr(path, name, value);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    } else {
      mByPassFs.setXAttr(path, name, value);
    }
  }

  public byte[] getXAttr(Path path, String name) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.getXAttr(path, name);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.getXAttr(path, name);
  }

  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.getXAttrs(path);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.getXAttrs(path);
  }

  public void removeXAttr(Path path, String name) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        super.removeXAttr(path, name);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    } else {
      mByPassFs.removeXAttr(path, name);
    }
  }

  public List<String> listXAttrs(Path path) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.listXAttrs(path);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.listXAttrs(path);
  }

  public ContentSummary getContentSummary(Path path) throws IOException {
    path = getFullPath(getUri(), path);
    if (!pathByPassed(path)) {
      try {
        return super.getContentSummary(path);
      } catch (UnavailableException e) {
        mFallbackManager.markFallBack(this);
      }
    }
    return mByPassFs.getContentSummary(path);
  }

  @Override
  public void close() throws IOException {
    if (mByPassFs != null) {
      mByPassFs.close();
    }
    super.close();
  }

  @VisibleForTesting
  protected static Path getFullPath(URI uri, Path path) {
    URI pathUri = path.toUri();
    if (pathUri.getAuthority() == null || pathUri.getScheme() == null) {
      return new Path(uri.getScheme(), uri.getAuthority(), pathUri.getPath());
    }
    return path;
  }
}
