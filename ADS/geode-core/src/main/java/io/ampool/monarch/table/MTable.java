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

import java.util.List;
import java.util.Map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.IllegalColumnNameException;
import io.ampool.monarch.table.exceptions.MCacheInternalErrorException;
import io.ampool.monarch.table.exceptions.MCacheLowMemoryException;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.exceptions.MException;
import io.ampool.monarch.table.exceptions.TableInvalidConfiguration;
import io.ampool.monarch.table.exceptions.RowKeyDoesNotExistException;
import io.ampool.monarch.table.exceptions.RowKeyOutOfRangeException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.internal.MTableLocationInfo;
import io.ampool.monarch.table.internal.Table;

/**
 * Reference to a single MTable table. Obtain an instance from a {@link MCacheFactory}.
 * <p>
 * Table reference can be used to get, put, delete data from a table.
 * 
 * @see Put
 * @see Get
 * @see Delete
 * @see MTableDescriptor
 * @since 0.2.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MTable extends Table {

  /**
   * Put or update a row into the table.
   *
   * Doing a put always creates a new version of a row, at a certain timestamp. Version are
   * maintained upto {@link MTableDescriptor#setMaxVersions(int)} (default 1), after which oldest
   * version is removed. By default the system uses the server's currentTimeMillis, but you can
   * specify the timestamp using {@link Put#setTimeStamp(long)}.
   *
   * To overwrite an existing row, do a put with exactly the same rowkey, columns, and timestamp. If
   * column/columns are different with same rowkey and timestamp, then the row at that timestamp
   * will be updated with the new column values (non-updated columns will be carry-overed from older
   * versions).
   * 
   * @param put defines the row to insert in a table. See {@link Put}
   * @throws IllegalArgumentException if the put is null
   * @throws IllegalColumnNameException if the columnName is not defined in the schema
   * @throws RowKeyOutOfRangeException if keyspace is define and key is out of range
   * @throws MCacheLowMemoryException if Servers are running with low memory
   * @throws MCacheInternalErrorException Internal Error
   * @since 0.2.0
   */
  void put(Put put) throws IllegalArgumentException, IllegalColumnNameException,
      MCacheInternalErrorException, RowKeyOutOfRangeException, MCacheLowMemoryException;

  /**
   * Puts or updates multiple rows into the table as a batch Merging behavior is similar to
   * {@link #put(Put)}
   * 
   * @param puts records to put, in a form of see {@link Put}
   * @throws IllegalArgumentException if the puts is null or list contains multiple put's with same
   *         rowkey
   * @throws IllegalColumnNameException if the columnName is not defined in the schema
   * @throws RowKeyOutOfRangeException if keyspace is define and key is out of range
   * @throws MCacheLowMemoryException if Servers are running with low memory
   * @throws MCacheInternalErrorException Internal Error
   * @since 0.2.0
   */
  void put(List<Put> puts) throws IllegalArgumentException, IllegalColumnNameException,
      MCacheInternalErrorException, RowKeyOutOfRangeException, MCacheLowMemoryException;

  void put(List<Put> puts, Object callbackArg)
      throws IllegalArgumentException, IllegalColumnNameException, MCacheInternalErrorException,
      RowKeyOutOfRangeException, MCacheLowMemoryException;

  /**
   * Extracts row data for a given row key from the table.
   * 
   * @param get The object that specifies what row to fetch and what columns to fetch. See
   *        {@link Get} for details.
   * @return data for the specified row, if it exists. If the row specified doesn't exist, the
   *         {@link Row} instance returned won't contain any {@link Cell}, as indicated by
   *         {@link Row#isEmpty()}.
   * @throws IllegalArgumentException if the gets is null or maxVersion set in MGet exceeds defined
   *         tables max version.
   * @throws IllegalColumnNameException if the specified columnName is not defined in the schema
   * @throws RowKeyOutOfRangeException if keyspace is defined and key is out of range
   * @throws MCacheInternalErrorException Internal Error
   */
  Row get(Get get) throws IllegalArgumentException, IllegalColumnNameException,
      MCacheInternalErrorException, RowKeyOutOfRangeException;

  /**
   * Extracts row data from the specified rows as a batch.
   *
   * If the value for the key is not found then this api will return null for that particular Get
   * request.
   * 
   * @param gets row to be inserted in a table.
   * @return data for the specified rows, if it exists. If the row specified doesn't exist, the
   *         {@link Row} instance returned won't contain any {@link Cell}, as indicated by
   *         {@link Row#isEmpty()}.
   * @throws IllegalArgumentException if the gets is null or list contains multiple get's with same
   *         rowkey
   * @throws IllegalColumnNameException if the specified columnName is not defined in the schema
   * @throws RowKeyOutOfRangeException if keyspace is defined and key is out of range
   * @throws MCacheInternalErrorException Internal Error
   */
  Row[] get(List<Get> gets) throws IllegalArgumentException, IllegalColumnNameException,
      MCacheInternalErrorException, RowKeyOutOfRangeException;

  /**
   * Deletes the specified entire row or selected columns of the row.
   * 
   * @param delete The object that specifies what to delete. See{@link Delete}
   * @throws IllegalArgumentException if the delete is null
   * @throws IllegalColumnNameException if the columnName is not defined in the schema
   * @throws TableInvalidConfiguration if timestamp setis invalid.
   * @throws RowKeyOutOfRangeException if keyspace is define and key is out of range
   * @throws MCacheLowMemoryException if Servers are running with low memory
   * @throws RowKeyDoesNotExistException if Row key does not exists
   * @throws MCacheInternalErrorException Internal Error
   * @since 0.2.0
   */
  void delete(Delete delete) throws IllegalArgumentException, IllegalColumnNameException,
      MCacheInternalErrorException, RowKeyOutOfRangeException, MCacheLowMemoryException;

  /**
   * Not Yet Implemented 0.2.0 Deletes the specified entire rows or selected columns of the rows as
   * a batch.
   * 
   * @param deletes delete object to be deleted
   * @throws IllegalArgumentException if the deletes is null
   * @throws IllegalColumnNameException if the columnName is not defined in the schema
   * @throws MCacheLowMemoryException if Servers are running with low memory
   * @throws MCacheInternalErrorException Internal Error
   **/
  void delete(List<Delete> deletes) throws IllegalArgumentException, IllegalColumnNameException,
      MCacheInternalErrorException, MCacheLowMemoryException;


  /**
   * Delete the specified rows and all the versions if these are matched to the corresponding
   * filter.
   * 
   * @param deleteFilterMap map of #Delete object and corresponding #Filter object
   * @throws IllegalArgumentException if the deleteFilterMap is null
   * @throws IllegalColumnNameException if the columnName is not defined in the schema
   * @throws MCacheLowMemoryException if Servers are running with low memory
   * @throws MCacheInternalErrorException Internal Error
   */
  void delete(Map<Delete, Filter> deleteFilterMap) throws IllegalArgumentException,
      IllegalColumnNameException, MCacheInternalErrorException, MCacheLowMemoryException;

  /**
   * Return the MTableDescriptor object associated with this table.
   * 
   * @return MTableDescriptor
   */
  MTableDescriptor getTableDescriptor();

  /**
   * Get the count of keys of this table.
   * 
   * @return count of keys of table
   */
  int getKeyCount();

  /**
   * Atomically checks if a row/column value matches the expected value. If it does match the put
   * operation is executed, if not the put is ignored. Notes: post event observers will not get
   * events for check and delete if the check fails as no state was changed in this case. If a check
   * passes the performance of checkAndPut is comparable to put, but for failed checks performance
   * may be degraded due to the need to pass the failure notice back to the caller.
   * 
   * @param row to check
   * @param column column family to check
   * @param value the expected value
   * @param put data to put if check succeeds
   * @return true if put completed; false if check failed and put was not executed
   * @throws RowKeyDoesNotExistException if Row key does not exists
   */
  boolean checkAndPut(byte[] row, byte[] column, byte[] value, Put put);

  /**
   * Atomically checks if a row/column value matches the expected value. If it does match the delete
   * operations is executed, if not it is ignored. Notes: post event observers will not get events
   * for check and delete if the check fails as no state was changed in this case. If a check passes
   * the performance of checkAndDelete is comparable to delete, but for failed checks performance
   * may be degraded due to the need to pass the failure notice back to the caller.
   * 
   * @param row to check
   * @param column column family to check
   * @param value the expected value
   * @param delete delete operation to execute if check succeeds
   * @return true if delete completed; false if check failed and delete was not executed
   * @throws RowKeyDoesNotExistException if Row key does not exists
   * @throws TableInvalidConfiguration if timestamp set invalid.
   */
  boolean checkAndDelete(byte[] row, byte[] column, byte[] value, Delete delete);

  /**
   * Get a scanner instance for a table
   * 
   * @param scan see {@link Scan}
   * @return instance of a scanner
   */

  Scanner getScanner(final Scan scan) throws MException;

  /**
   * A parallel scanner may improve performance but does require more threads (depending on number
   * of splits for this method) so is best on client machines with higher core counts. This method
   * starts a scanner on each split (every region bucket) and combines the results. Batched scans
   * are not supported by parallel scanners.
   *
   * <b>NOTE: Parallel scans are always unordered for {@link MTableType#ORDERED_VERSIONED}</b>, as
   * this type is range partitioned. In order to preserve order and obtain superior performance
   * table rows must be randomly distributed across buckets by key, and this is not the case for
   * range partitioned tables.
   * 
   * @param scan the {@link Scan} object that defined the scan.
   * @return the parallel scanner
   */
  Scanner getParallelScanner(final Scan scan) throws MException;

  /**
   * A parallel scanner may improve performance but does require more threads (depending on number
   * of splits and group size) so is best on client machines with higher core counts. This method
   * starts a scanner on each group of groupSize buckets and combines the results. Batched scans are
   * not supported by parallel scanners.
   *
   * <b>NOTE: Parallel scans are always unordered for {@link MTableType#ORDERED_VERSIONED}</b>, as
   * this type is range partitioned. In order to preserve order and obtain superior performance
   * table rows must be randomly distributed across buckets by key, and this is not the case for
   * range partitioned tables.
   * 
   * @param scan the {@link Scan} object that defined the scan.
   * @param groupSize the number of buckets for each sub-scan in the parallel scan.
   * @return the parallel scanner
   */
  Scanner getParallelScanner(final Scan scan, int groupSize) throws MException;

  int getTotalNumOfColumns();

  /**
   * Creates an instance of the given co-processor class and invokes the specified
   * {@code methodName} on a associated table split having {@code row}.
   * <p>
   * For Endpoint coprocessor execution on Unordered table, Use
   * {@link MTable#coprocessorService(String, String, byte[], byte[], MExecutionRequest)} with both
   * start and stop key as NULL
   * </p>
   * <p>
   * Endpoint coprocessor execution is not supported on empty table, To check for empty table use,
   * {@link MTable#isEmpty()}
   * </p>
   * <p>
   * In case of server restart or upgrade, user has to make sure that jar containing coprocessor
   * class is in server classpath
   * </p>
   * 
   * @param methodName - method to be executed in a co-processor
   * @return Returns the results against each TableSplit. The result can be a valid result or an
   *         exception thrown from the coprocessor.
   * @throws MCoprocessorException If coprocessor execution is failed at server
   */
  List<Object> coprocessorService(String className, String methodName, byte[] row,
      MExecutionRequest request);

  /**
   * Creates an instance of the given co-processor class and invokes the specified
   * {@code methodName} on a associated table splits having keys in the range {@code startKey} and
   * {@code stopKey}.
   * <p>
   * For endpoint coprocessor execution on Unordered table, both {@code startKey} and
   * {@code stopKey} must have to be null
   * </p>
   * <p>
   * Endpoint coprocessor execution is not supported on empty table, To check for empty table use,
   * {@link MTable#isEmpty()}
   * </p>
   * <p>
   * In case of server restart or upgrade, user has to make sure that jar containing coprocessor
   * class is in server classpath
   * </p>
   * 
   * @param className - Class of co-processor
   * @param methodName - method to be executed in a co-processor
   * @param startKey - startKey. It can be {@code null}, in such case it means first key of table.
   * @param stopKey - stopKey. It can be {@code null}, in such case it means last key of table.
   * @param request - {@link MExecutionRequest}
   * @return Returns the results against each TableSplit. The result can be a valid result or an
   *         exception thrown from the coprocessor.
   * @throws MCoprocessorException If coprocessor execution is failed at server.
   */
  Map<Integer, List<Object>> coprocessorService(String className, String methodName,
      byte[] startKey, byte[] stopKey, MExecutionRequest request);

  /**
   * Returns data structure holding location information for this table.
   * 
   * @return MTableLocationInfo
   */
  MTableLocationInfo getMTableLocationInfo();

  /**
   * Check if the table is empty or not.
   * 
   * @return true if empty.
   */
  boolean isEmpty();
}
