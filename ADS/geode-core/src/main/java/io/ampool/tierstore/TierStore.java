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
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.filter.Filter;
import org.apache.geode.internal.cache.TierStoreStats;


public interface TierStore {
  /**
   * initalize the store
   * 
   * @param storeProperties
   */
  void init(Properties storeProperties, TierStoreWriter writer, TierStoreReader reader)
      throws IOException;

  /**
   * Get writer for Store
   *
   * @param tableName
   * @param partitionId
   * @param writerProps
   * @return writer instance for scanning
   */
  TierStoreWriter getWriter(String tableName, int partitionId, Properties writerProps);

  /**
   * Get writer for Store
   *
   * @param tableName
   * @param partitionId
   * @return writer instance for scanning
   */
  TierStoreWriter getWriter(String tableName, int partitionId);

  /**
   * Get reader for Store
   *
   * @param tableName
   * @param partitionId
   * @param readerProps
   * @return reader instance for scanning
   */
  TierStoreReader getReader(String tableName, int partitionId, Properties readerProps);

  /**
   * Get reader for Store
   *
   * @param tableName
   * @param partitionId
   * @return reader instance for scanning
   */
  TierStoreReader getReader(String tableName, int partitionId);

  /**
   * Delete a table from store
   */
  void deleteTable(String tableName);

  /**
   * Delete bucket data from the store
   * 
   * @param tableName
   * @param partitionId
   */
  void deletePartition(String tableName, int partitionId);

  /**
   * Stop tier evictor threads
   */
  void stopMonitoring();

  /**
   * Delete the content at the specified path from the tier-store. It should delete the file or the
   * complete directory, recursively.
   *
   * @param unitPath the unit-path to be deleted
   */
  void delete(final String... unitPath);

  /**
   * Return base URI
   */
  URI getBaseURI();

  long getBytesRead();

  /**
   * Get the common properties of the tier-store.
   *
   * @return the store properties
   */
  Properties getProperties();

  /**
   * Get name of the tier-store.
   *
   * @return name of the tier-store
   */
  String getName();

  /**
   * Get the tier-store-reader for reading specific portion of the table. It can get the reader to
   * read full table or a bucket from a table or a specific time-partition for a bucket of a table.
   *
   * @param props the reader properties
   * @param args the table part paths
   * @return the reader for the specific unit
   */
  TierStoreReader getReader(final Properties props, final String... args);

  /**
   * Get the tier-store-writer to write a specific chunk/set of records to the table as a single
   * unit.
   *
   * @param props the writer properties
   * @param args the table part paths
   * @return the writer
   */
  TierStoreWriter getWriter(final Properties props, final String... args);

  /**
   * Destroy the tier-store and cleanup any resources it may have been managing.
   */
  default void destroy() {
    //// do nothing..
  }

  /**
   * Truncate records from a tier store belonging to the specified table and partition which match
   * the specified filter.
   * 
   * @param tableName Name of the table from which records are to be deleted
   * @param partitionId Partition to be used for deleting records
   * @param filter Filter criteria
   * @param td Table descriptor of the table.
   * @param tierProperties Table specific tier properties to be used.
   */
  void truncateBucket(String tableName, int partitionId, Filter filter, TableDescriptor td,
      Properties tierProperties) throws IOException;

  /**
   * Update records in a tier store belonging to the specified table and partition which match the
   * specified filter.
   *
   * @param tableName Name of the table from which records are to be deleted
   * @param partitionId Partition to be used for deleting records
   * @param filter Filter criteria
   * @param colValues New column values map.
   * @param td Table descriptor of the table.
   * @param tierProperties Table specific tier properties to be used.
   */
  void updateBucket(String tableName, int partitionId, Filter filter,
      Map<Integer, Object> colValues, TableDescriptor td, Properties tierProperties)
      throws IOException;

  /**
   * Get the converter descriptor for the specified table. The converter descriptor can convert the
   * values that to/from ORC into respective Java objects when reading/writing the data from/to the
   * respective tier-store and Ampool.
   *
   * @param tableName the table name
   * @return the converter descriptor
   */
  ConverterDescriptor getConverterDescriptor(String tableName);

  TierStoreStats getStats();


  long getBytesWritten();
}
