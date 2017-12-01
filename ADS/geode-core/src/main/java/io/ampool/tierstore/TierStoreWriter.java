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

/**
 * Tier Store writer
 */
public interface TierStoreWriter {

  /**
   * Initialize the writer with properties
   * 
   * @param properties
   */
  void init(Properties properties);

  Properties getProperties();

  /**
   * Write multiple rows to file
   *
   * @param writerProperties - Per table per tier config
   * @param rows
   * @return number of rows written
   */
  int write(final Properties writerProperties, StoreRecord... rows) throws IOException;

  /**
   * Write multiple rows to file
   *
   * @param writerProperties - Per table per tier config
   * @param rows
   * @return number of rows written
   */
  int write(final Properties writerProperties, List<StoreRecord> rows) throws IOException;


  /**
   * set the converter descriptor used for converting ampool data types to store data types
   * 
   * @param converterDescriptor
   */
  void setConverterDescriptor(final ConverterDescriptor converterDescriptor);

  /**
   * Create the tier-store-writer to write the data/records as a single unit in multiple chunks. The
   * writer must be closed once all the data is written.
   *
   * @param props the writer properties
   * @throws IOException in case of an error while creating the writer
   */
  void openChunkWriter(final Properties props) throws IOException;

  /**
   * Write a single chunk to an already opened writer. It does not open a new writer and in case it
   * is not yet opened it fails. The records written do not get reflected until the writer is
   * closed.
   *
   * @param records the records to be written
   * @return the number of records written
   * @throws IOException in case of an error while writing
   * @see TierStoreWriter#openChunkWriter(Properties)
   * @see TierStoreWriter#closeChunkWriter()
   */
  int writeChunk(final List<StoreRecord> records) throws IOException;

  /**
   * Close the already opened writer.
   *
   * @throws IOException in case of an error while closing the writer
   * @see TierStoreWriter#openChunkWriter(Properties)
   */
  void closeChunkWriter() throws IOException;
}
