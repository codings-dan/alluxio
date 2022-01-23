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

import alluxio.conf.ServerConfiguration;
import alluxio.conf.TxPropertyKey;
import alluxio.extensions.ClassLoaderContext;
import alluxio.extensions.ExtensionFactoryRegistry;
import alluxio.extensions.ExtensionsClassLoader;
import alluxio.underfs.UfsService;
import alluxio.underfs.UfsServiceFactory;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A factory that creates {@link InodeAttributesProvider} based on configuration.
 */
public class AbstractInodeAttributesProviderFactory implements UfsServiceFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractInodeAttributesProviderFactory.class);

  private static final String EXTENSION_PATTERN = "alluxio-authorization-*.jar";

  private final ExtensionFactoryRegistry<
      InodeAttributesProviderFactory,
      UnderFileSystemConfiguration> mExtensionFactory;

  /**
   * Default constructor.
   */
  public AbstractInodeAttributesProviderFactory() {
    mExtensionFactory = new ExtensionFactoryRegistry<>(InodeAttributesProviderFactory.class,
        EXTENSION_PATTERN);
  }

  /**
   * Creates a new {@link InodeAttributesProvider} for Alluxio master.
   *
   * @return the provider
   */
  public InodeAttributesProvider createMasterProvider() {
    if (ServerConfiguration.isSet(TxPropertyKey.SECURITY_AUTHORIZATION_PLUGIN_NAME)) {
      try {
        String pluginName =
                ServerConfiguration.get(TxPropertyKey.SECURITY_AUTHORIZATION_PLUGIN_NAME);
        String pluginPaths =
                ServerConfiguration.get(TxPropertyKey.SECURITY_AUTHORIZATION_PLUGIN_PATHS);
        LOG.info("Initializing Alluxio master authorization for pluginName: {} ,pluginPaths: {} ",
            pluginName, pluginPaths);
        UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(
            ServerConfiguration.global());
        ufsConf = ufsConf.createMountSpecificConf(ImmutableMap.of(
            TxPropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME.getName(), pluginName,
            TxPropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_PATHS.getName(), pluginPaths
        ));
        return create("/", ufsConf);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  /**
   * Creates a new {@link InodeAttributesProvider} for the given path and configuration.
   *
   * @param path a path which the provider is associate with
   * @param ufsConf configuration for the provider
   * @return the provider
   */
  public InodeAttributesProvider create(String path, UnderFileSystemConfiguration ufsConf) {
    List<InodeAttributesProviderFactory> factories =
        mExtensionFactory.findAll(path, ufsConf);
    if (factories.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("No InodeAttributesProviderFactory found for: %s", path));
    }

    List<Throwable> errors = new ArrayList<>();
    for (InodeAttributesProviderFactory factory : factories) {
      try (ClassLoaderContext context = ClassLoaderContext.useClassLoaderFrom(factory)) {
        ClassLoader pluginClassLoader = context.getClassLoader();
        if (pluginClassLoader instanceof ExtensionsClassLoader) {
          ExtensionsClassLoader extLoader = (ExtensionsClassLoader) pluginClassLoader;
          String pluginPaths =
              ufsConf.get(TxPropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_PATHS);
          Arrays.stream(pluginPaths.split(":")).forEachOrdered(pluginPath -> {
            try {
              LOG.info("Adding plugin path {}", pluginPath);
              extLoader.addPath(pluginPath);
            } catch (MalformedURLException e) {
              LOG.warn("Failed to add path {}: {}", pluginPath, e.getMessage());
            }
          });
        } else {
          // We should never reach here. We should always use ExtensionsClassloader for factories.
          LOG.error("Cannot add path - factory is not loaded by an ExtensionsClassloader.");
        }
        LOG.info("Attempt to create InodeAttributesProvider for path {} by factory {}", path,
            factory);
        InodeAttributesProvider provider = factory.create(path, ufsConf);
        provider.start();
        return provider;
      } catch (Throwable e) {
        // Catching Throwable rather than Exception to catch service loading errors
        errors.add(e);
        LOG.warn("Failed to create InodeAttributesProvider by factory {}: {}", factory,
            e.getMessage());
      }
    }
    if (!errors.isEmpty()) {
      IllegalArgumentException e = new IllegalArgumentException(
          String.format("Unable to create an InodeAttributesProvider instance for path: %s", path));
      for (Throwable t : errors) {
        e.addSuppressed(t);
      }
      throw e;
    }
    return null;
  }

  //serverConfiguration.get(alluxio.conf.PropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_NAME)
  @Override
  public <T extends UfsService> T createUfsService(String path,
      UnderFileSystemConfiguration ufsConf, Class<T> serviceType) {
    if (serviceType != InodeAttributesProvider.class
        || !ufsConf.isSet(TxPropertyKey.UNDERFS_SECURITY_AUTHORIZATION_PLUGIN_PATHS)) {
      return null;
    }
    return (T) create(path, ufsConf);
  }
}
