/*
 * Copyright (c) 2017 Ampool, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

package io.ampool.monarch.table;

import io.ampool.store.ExternalStoreWriter;
import org.apache.geode.internal.ClassPathLoader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Properties;

/**
 * This configuration is used for configuring the details of the external store. Properties has all
 * the file system related configurations based on the type of the file system. Base URI is the
 * starting location where archived data to be written.
 */
public class ArchiveConfiguration {
  public static final String BASE_DIR_PATH = "base.dir.path";
  private static final String DEFAULT_EXT_STORE_WRITER =
      "io.ampool.tierstore.writers.orc.ORCExternalStoreWriter";
  private static final int DEFAULT_BATCH_SIZE = 1000;

  private Properties fsProperties;
  private int batchSize = DEFAULT_BATCH_SIZE;
  private ExternalStoreWriter externalStoreWriter;

  public Properties getFsProperties() {
    return fsProperties;
  }

  public void setFsProperties(Properties fsProperties) {
    this.fsProperties = fsProperties;
  }

  public ExternalStoreWriter getExternalStoreWriter() {
    if (externalStoreWriter == null) {
      try {
        Class<?> extStoreWriter = ClassPathLoader.getLatest().forName(DEFAULT_EXT_STORE_WRITER);
        Constructor<?> constructor = extStoreWriter.getConstructors()[0];
        externalStoreWriter = (ExternalStoreWriter) constructor.newInstance();
        return externalStoreWriter;
      } catch (ClassNotFoundException | IllegalAccessException | InstantiationException
          | InvocationTargetException e) {
        return externalStoreWriter;
      }
    } else {
      return externalStoreWriter;
    }
  }

  public void setExternalStoreWriter(ExternalStoreWriter externalStoreWriter) {
    this.externalStoreWriter = externalStoreWriter;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }
}
