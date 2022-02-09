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
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.URI;

/**
 * A flexible Alluxio client API compatible with Apache Hadoop
 * {@link org.apache.hadoop.fs.FileSystem} interface.
 * The authority of uri can be treated as a parent folder of the following path.
 */
@NotThreadSafe
public class FileSystemFlexible extends FileSystem {

  private String mAlluxioHeader;

  /**
   * Constructs a new {@link FileSystem}.
   */
  public FileSystemFlexible() {
    super();
  }

  /**
   * Constructs a new {@link FileSystem} instance with a
   * specified {@link alluxio.client.file.FileSystem} handler for tests.
   *
   * @param fileSystem handler to file system
   */
  public FileSystemFlexible(alluxio.client.file.FileSystem fileSystem) {
    super(fileSystem);
  }

  @Override
  protected void validateFsUri(URI fsUri) throws IOException, IllegalArgumentException {
  }

  @Override
  protected String getFsScheme(URI fsUri) {
    return fsUri.getScheme();
  }

  @Override
  protected AlluxioURI getAlluxioPath(Path path) {
    // Sends the full path to Alluxio for master side resolution.
    String authority = path.toUri().getAuthority();
    if (!Strings.isNullOrEmpty(authority)) {
      return new AlluxioURI(
          "/" + authority + HadoopUtils.getPathWithoutScheme(path));
    }
    return super.getAlluxioPath(path);
  }

  @Override
  public synchronized void initialize(URI uri, Configuration conf,
      @Nullable AlluxioConfiguration alluxioConfiguration) throws IOException {
    super.initialize(uri, conf, alluxioConfiguration);
    mAlluxioHeader = getFsScheme(uri) + ":/";
  }

  @Override
  protected Path getFsPath(String fsUriHeader, URIStatus fileStatus) {
    return new Path(mAlluxioHeader + fileStatus.getPath());
  }
}
