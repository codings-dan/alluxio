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

package alluxio.master.file;

import alluxio.extensions.ExtensionFactory;
import alluxio.underfs.UnderFileSystemConfiguration;

import javax.annotation.Nullable;

/**
 * Interface for {@link InodeAttributesProvider} factories.
 */
public interface InodeAttributesProviderFactory
    extends ExtensionFactory<InodeAttributesProvider, UnderFileSystemConfiguration> {

  /**
   * Creates a new {@link InodeAttributesProvider} for providing authorization on the given path.
   * An {@link IllegalArgumentException} is thrown if this factory does not support the given path
   * or if the configuration provided is insufficient to create a provider.
   *
   * @param path file path
   * @param conf optional configuration object for the provider, may be null
   * @return the provider
   */
  InodeAttributesProvider create(String path, @Nullable UnderFileSystemConfiguration conf);

  /**
   * Gets whether this factory supports the given path and thus whether calling the
   * {@link #create(String, UnderFileSystemConfiguration)} can succeed for this path.
   *
   * @param path file path
   * @param conf optional configuration object for the provider, may be null
   * @return true if the path is supported, false otherwise
   */
  boolean supportsPath(String path, @Nullable UnderFileSystemConfiguration conf);
}
