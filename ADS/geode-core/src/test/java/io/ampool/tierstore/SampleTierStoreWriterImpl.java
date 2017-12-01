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

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import io.ampool.store.StoreRecord;

public class SampleTierStoreWriterImpl implements TierStoreWriter {
  Properties properties;

  @Override
  public void init(Properties properties) {
    this.properties = properties;
  }

  @Override
  public int write(final Properties writerProperties, StoreRecord... rows) throws IOException {
    return 0;
  }

  @Override
  public int write(final Properties writerProperties, List<StoreRecord> rows) throws IOException {
    return 0;
  }

  @Override
  public void setConverterDescriptor(final ConverterDescriptor converterDescriptor) {

  }

  @Override
  public void openChunkWriter(Properties props) throws IOException {

  }

  @Override
  public int writeChunk(List<StoreRecord> records) throws IOException {
    return 0;
  }

  @Override
  public void closeChunkWriter() throws IOException {

  }

  public Properties getProperties() {
    return properties;
  }
}
