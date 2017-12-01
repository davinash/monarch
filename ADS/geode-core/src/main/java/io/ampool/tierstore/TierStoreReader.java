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
package io.ampool.tierstore;

import java.util.Properties;

import io.ampool.monarch.table.ftable.internal.ReaderOptions;
import io.ampool.store.StoreRecord;
import io.ampool.store.StoreScan;

/**
 * Tier Store reader
 */
public interface TierStoreReader extends Iterable<StoreRecord> {

  /**
   * Initialize the reader with properties
   * 
   * @param properties
   */
  void init(Properties properties);

  Properties getProperties();

  void setFilter(StoreScan scan);

  /**
   * set the converter descriptor used for converting store data types to ampool data types
   * 
   * @param converterDescriptor
   */
  void setConverterDescriptor(final ConverterDescriptor converterDescriptor);

  void setReaderOptions(ReaderOptions readerOptions);
}
