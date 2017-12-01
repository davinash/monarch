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
package io.ampool.tierstore.internal;

import java.util.Properties;

public class DefaultStore {

  public static final String STORE_NAME = "DefaultLocalORCStore";
  public static final String STORE_CLASS = "io.ampool.tierstore.stores.LocalTierStore";
  public static final String ORC_READER_CLASS =
      "io.ampool.tierstore.readers.orc.TierStoreORCReader";
  public static final String ORC_WRITER_CLASS =
      "io.ampool.tierstore.writers.orc.TierStoreORCWriter";

  public static Properties getDefaultStoreProperties() {
    Properties properties = new Properties();
    // add base dir path
    properties.put("base.dir.path", "./ORC");
    properties.put("time.based.partitioning.enabled", "true");
    // 600 seconds
    properties.put("partitioning.interval.ns", "600000000000");
    // for testing
    // properties.put("partitioning.interval.ns", "1000000000");
    return properties;
  }

  public static Properties getDefaultStoreORCReaderProperties() {
    Properties properties = new Properties();
    // add base dir path
    return properties;
  }

  public static Properties getDefaultStoreORCWriterProperties() {
    Properties properties = new Properties();
    properties.put("compression.kind", "NONE");
    properties.put("stripe.size", "1000");
    properties.put("buffer.size", "1000");
    properties.put("new.index.stride", "1000");
    return properties;
  }
}
