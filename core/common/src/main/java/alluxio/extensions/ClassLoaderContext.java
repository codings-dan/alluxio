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

package alluxio.extensions;

import com.google.common.base.Preconditions;

/**
 * An {@link AutoCloseable} tool to help switching thread context class loader.
 */
public final class ClassLoaderContext implements AutoCloseable {
  private final ClassLoader mPreviousClassLoader;
  private final ClassLoader mClassLoader;

  /**
   * Use the class loader of the given object for class loading in the current thread.
   * @param object the object whose class loader should be used
   * @return a class loader context that auto restore the previous thread class loader when closed
   */
  public static ClassLoaderContext useClassLoaderFrom(Object object) {
    return new ClassLoaderContext(Preconditions.checkNotNull(object, "object")
        .getClass().getClassLoader());
  }

  private ClassLoaderContext(ClassLoader classLoader) {
    mClassLoader = classLoader;
    mPreviousClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
  }

  /**
   * @return the class loader
   */
  public ClassLoader getClassLoader() {
    return mClassLoader;
  }

  @Override
  public void close() {
    Thread.currentThread().setContextClassLoader(mPreviousClassLoader);
  }
}
