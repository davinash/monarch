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

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.exceptions.MCacheInternalErrorException;
import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.exceptions.MTableNotExistsException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.exceptions.FTableExistsException;
import io.ampool.monarch.table.ftable.exceptions.FTableNotExistsException;
import io.ampool.monarch.table.ftable.exceptions.TierStoreNotAvailableException;

import java.util.Map;

/**
 * The administrative API for MTable. Obtain an instance from an {@link MonarchCache#getAdmin()}.
 * Admin can be used to create, list and check table exists and other administrative operations.
 * 
 * @since 0.3.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Admin {

  /**
   * Creates Monarch table using the given tablename and description.
   * 
   * @param tableName name for the table
   * @param tableDescriptor table descriptor for the table
   * @return Handle to the table if created otherwise null
   * @throws IllegalArgumentException If table name is not valid
   * @throws MTableExistsException If table with the given name already exists
   * @throws IllegalArgumentException If No Column Definition is found
   * @throws IllegalArgumentException If MAsyncEventListener's full class path name is incorrect.
   * @throws MCacheInternalErrorException Internal Error
   * @deprecated use {@link #createMTable(String, MTableDescriptor)} or
   *             {@link #createFTable(String, FTableDescriptor)}
   */
  @Deprecated
  MTable createTable(String tableName, MTableDescriptor tableDescriptor)
      throws MTableExistsException;

  /**
   * Creates Monarch table using the given tablename and description.
   * 
   * @param tableName name for the table
   * @param tableDescriptor table descriptor for the table
   * @return Handle to the table if created otherwise null
   * @throws IllegalArgumentException If table name is not valid
   * @throws MTableExistsException If table with the given name already exists
   * @throws IllegalArgumentException If No Column Definition is found
   * @throws MCacheInternalErrorException Internal Error
   */
  MTable createMTable(String tableName, MTableDescriptor tableDescriptor)
      throws MTableExistsException;

  /**
   * Creates Fact table using the given tablename and description. User must have to set a
   * partitioningColumn using {@link FTableDescriptor#setPartitioningColumn(byte[])} while creating
   * the {@link FTableDescriptor}.
   * 
   * @param tableName name for the table
   * @param tableDescriptor table descriptor for the table
   * @return Handle to the table if created otherwise null
   * @throws IllegalArgumentException If table name is not valid
   * @throws MTableExistsException If table with the given name already exists
   * @throws IllegalArgumentException If No Column Definition is found
   * @throws TierStoreNotAvailableException If not available store is provided
   * @throws MCacheInternalErrorException Internal Error
   */
  FTable createFTable(String tableName, FTableDescriptor tableDescriptor)
      throws FTableExistsException;

  /**
   * Checks if the table with the given tableName exists or not
   * 
   * @param tableName tablename to check
   * @return true if table exists else false
   * @throws MCacheInternalErrorException Internal Error
   * @deprecated use {@link #existsMTable(String)} or {@link #existsFTable(String)}
   */
  @Deprecated
  boolean tableExists(String tableName);

  /**
   * Checks if the fact table with the given tableName exists or not
   * 
   * @param tableName tablename to check
   * @return true if table exists else false
   * @throws MCacheInternalErrorException Internal Error
   * @deprecated use {@link #existsMTable(String)}
   */
  @Deprecated
  boolean mTableExists(String tableName);

  /**
   * Checks if the MTable with the given tableName exists or not
   *
   * @param tableName tableName to check
   * @return true if table exists else false
   * @throws MCacheInternalErrorException Internal Error
   */
  boolean existsMTable(String tableName);

  /**
   * Checks if the fact table with the given tableName exists or not
   * 
   * @param tableName tablename to check
   * @return true if table exists else false
   * @throws MCacheInternalErrorException Internal Error
   * @deprecated use {@link #existsFTable(String)}
   */
  @Deprecated
  boolean fTableExists(String tableName);

  /**
   * Checks if the fact table with the given tableName exists or not
   *
   * @param tableName tableName to check
   * @return true if table exists else false
   * @throws MCacheInternalErrorException Internal Error
   */
  boolean existsFTable(String tableName);

  /**
   * List of all table descriptors.
   *
   * @throws MCacheInternalErrorException Internal Error
   * @return returns map of tableName vs descriptors of all existing MTables
   * @deprecated use {@link #listMTables()} or {@link #listFTables()}
   */
  @Deprecated
  Map<String, MTableDescriptor> listTables();

  /**
   * List of all mTables.
   *
   * @throws MCacheInternalErrorException Internal Error
   * @return returns map of tableName vs descriptors of all existing MTables
   */
  Map<String, MTableDescriptor> listMTables();

  /**
   * List of all fTables.
   *
   * @throws MCacheInternalErrorException Internal Error
   * @return returns map of tableName vs descriptors of all existing FTables
   */
  Map<String, FTableDescriptor> listFTables();

  /**
   * List of all tables names
   * 
   * @return returns array of table names of all existing MTables
   * @throws MCacheInternalErrorException Internal Error
   * @deprecated use {@link #listMTableNames()} or {@link #listFTableNames()}
   */
  @Deprecated
  String[] listTableNames();

  /**
   * List of all tables names
   * 
   * @return returns array of table names of all existing MTables
   * @throws MCacheInternalErrorException Internal Error
   */
  String[] listMTableNames();

  /**
   * List of all tables names
   * 
   * @return returns array of table names of all existing MTables
   * @throws MCacheInternalErrorException Internal Error
   */
  String[] listFTableNames();

  /**
   * Delete the table with specified tableName
   * 
   * @param tableName tablename to delete
   * @throws MTableNotExistsException if table with given tablename does not exists
   * @throws MCacheInternalErrorException Internal Error
   * @deprecated use {@link #deleteMTable(String)} or {@link #deleteFTable(String)}
   */
  @Deprecated
  void deleteTable(String tableName);

  /**
   * Delete the table with this tableName
   * 
   * @param tableName tablename to delete
   * @throws MTableNotExistsException if table with given tablename does not exists
   * @throws MCacheInternalErrorException Internal Error
   */
  void deleteMTable(String tableName);

  /**
   * Delete the table with this tableName
   * 
   * @param tableName tablename to delete
   * @throws MTableNotExistsException if table with given tablename does not exists
   * @throws MCacheInternalErrorException Internal Error
   */
  void deleteFTable(String tableName);


  /**
   * Truncate a table with specified tableName.
   * 
   * @param tableName tablename to truncate
   * @throws MTableNotExistsException if table with given tablename does not exists
   * @throws MCacheInternalErrorException Internal Error
   * @deprecated use {@link #truncateMTable(String)}
   */
  @Deprecated
  void truncateTable(String tableName) throws Exception;

  /**
   * Truncate MTable.
   * 
   * @param tableName tablename to truncate
   * @throws MTableNotExistsException if table with given tablename does not exists
   * @throws IllegalArgumentException if MTable with given tablename does not exists
   * @throws MCacheInternalErrorException Internal Error
   */
  void truncateMTable(String tableName) throws Exception;

  /**
   * Truncate a FTable based on the filter provider
   *
   * @param tableName Name of the FTable to be truncated
   * @param filter Criteria for selecting records to be removed
   * @throws IllegalArgumentException if FTable with given tablename does not exists
   */
  void truncateFTable(String tableName, Filter filter) throws Exception;


  /**
   * Archive Table data to an external store.
   *
   * @param tableName tablename to archive
   * @param filter Criteria for selecting records
   * @param configuration archive configuration including file path etc.
   * @throws MTableNotExistsException if MTable with given tablename does not exists
   * @throws FTableNotExistsException if FTable with given tablename does not exists
   * @throws MCacheInternalErrorException Internal Error
   */
  void archiveTable(String tableName, Filter filter, ArchiveConfiguration configuration);

  /**
   * Truncate a MTable based on the filter provider
   *
   * @param tableName Name of the MTable to be truncated
   * @param filter Criteria for selecting records to be removed
   * @param preserveOlderVersions Set it to true only if the version matching the filter is to be
   *        deleted If this is set to false then the versions prior to the one matching the filter
   *        will also be deleted.
   * @throws IllegalArgumentException if MTable with given tablename does not exists
   */
  void truncateMTable(String tableName, Filter filter, boolean preserveOlderVersions)
      throws Exception;

  /**
   * Update rows in table which match the filter. All the column values specified in colValue will
   * be set corresponding to new value for that column specified in the map. The updated rows will
   * have same values for the specified columns. i.e the effect will be same as SQL update: UPDATE
   * table-Name SET column-Name = Value, column-Name = Value, ... [WHERE clause] Column name to
   * value mapping is provided in the value map and the clause for WHERE is specified as the filter.
   * DATA::MANAGE permissions are required for this operation.
   * 
   * @param tableName Name of the table to be updated.
   * @param filter Row selection filter
   * @param colValues Map of column names and new values.
   */
  void updateFTable(String tableName, Filter filter, Map<byte[], Object> colValues);


  /**
   * Flush all the entries of a FTable on the subsequent tier through WAL.
   *
   * @param tableName Name of the FTable to be truncated
   */

  void forceFTableEviction(String tableName);
}
