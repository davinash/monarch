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

import java.util.Iterator;
import java.util.Properties;

import io.ampool.monarch.table.ftable.internal.ReaderOptions;
import io.ampool.store.StoreRecord;
import io.ampool.store.StoreScan;

public class SampleTierStoreReaderImpl implements TierStoreReader {
  Properties properties;

  @Override
  public void init(Properties properties) {
    this.properties = properties;
  }

  @Override
  public void setFilter(final StoreScan scan) {

  }

  @Override
  public void setConverterDescriptor(final ConverterDescriptor converterDescriptor) {

  }

  @Override
  public void setReaderOptions(ReaderOptions readerOptions) {
    // empty stub..
  }

  @Override
  public Iterator<StoreRecord> iterator() {
    return null;
  }

  public Properties getProperties() {
    return properties;
  }
}
