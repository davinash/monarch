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
package io.ampool.monarch.table.internal;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.internal.ServerTableRegionProxy;
import io.ampool.internal.functions.DeleteWithFilterFunction;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.coprocessor.internal.MCoprocessorService;
import io.ampool.monarch.table.coprocessor.internal.MCoprocessorUtils;
import io.ampool.monarch.table.coprocessor.internal.MResultCollector;
import io.ampool.monarch.table.coprocessor.internal.MResultCollectorImpl;
import io.ampool.monarch.table.exceptions.*;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.results.FormatAwareRow;
import io.ampool.monarch.types.MPredicateHolder;
import org.apache.geode.CopyHelper;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.*;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.execute.DefaultResultCollector;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.function.Predicate;

import static io.ampool.monarch.table.internal.MTableUtils.checkSecurityException;

/**
 * Created by nilkanth on 4/11/15.
 */

// TODO:: APIs should throws IOException or MException??

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MTableImpl implements MTable, InternalTable {
  private static final Logger logger = LogService.getLogger();

  // TODO: This should be a instance of ServerTableRegionProxy which will support scan
  ServerTableRegionProxy srp;
  private final Region<Object, Object> tableRegion;
  private final MTableDescriptor tableDescriptor;
  private final Set<ByteArrayKey> columnNames;
  private final MonarchCacheImpl amplCache;
  private final MTableLocationInfo mTableLocationInfo;

  public MTableImpl(Region<Object, Object> tableRegion, MTableDescriptor tableDescriptor,
                    MonarchCacheImpl cache) {
    this.tableRegion = tableRegion;
    this.tableDescriptor = tableDescriptor;
    this.columnNames = new HashSet<>();

    for (Map.Entry<MColumnDescriptor, Integer> column : tableDescriptor.getColumnDescriptorsMap()
        .entrySet()) {
      this.columnNames.add(new ByteArrayKey(column.getKey().getColumnName()));
    }
    if (cache.isClient()) {
      srp = new ServerTableRegionProxy(tableRegion);
    }
    this.amplCache = cache;
    this.mTableLocationInfo = new MTableLocationInfoImpl(tableRegion.getName(), this);
  }

  public Region<Object, Object> getTableRegion() {
    return tableRegion;
  }

  public MonarchCacheImpl getAmplCache() {
    return amplCache;
  }

  private void verifyAllColumnNames(Put put) {
    Map<ByteArrayKey, Object> columnValueMap = put.getColumnValueMap();
    for (Map.Entry<ByteArrayKey, Object> column : columnValueMap.entrySet()) {
      if (!tableDescriptor.getColumnsByName().containsKey(column.getKey())) {
        throw new IllegalColumnNameException("Column is not defined in schema");
      }
    }
  }

  private void verifyColumnForNoValues(Put put) {
    Map<ByteArrayKey, Object> columnValueMap = put.getColumnValueMap();
    if (columnValueMap.size() == 0) {
      throw new IllegalArgumentException("At-least one Column value should be added!");
    }
  }

  private void verifyAllColumnNames(List<byte[]> columnsInRequest) {
    for (byte[] columns : columnsInRequest) {
      if (!tableDescriptor.getColumnsByName().containsKey(new ByteArrayKey(columns))) {
        throw new IllegalColumnNameException("Column is not defined in schema");
      }
    }
  }

  private void setColumnPositions(MTableKey key, Put put) {
    // Add column position for those whose value user has set
    // This will be used to distinguish if user has set explicit value or it is set from MTable put
    // impl
    // BUG GEN-485
    if (put.getColumnValueMap().size() > 0) {
      try {
        Map<MColumnDescriptor, Integer> listOfColumns =
            this.tableDescriptor.getColumnDescriptorsMap();
        MColumnDescriptor current = new MColumnDescriptor();
        put.getColumnValueMap().forEach((colName, colValue) -> {
          current.setColumnName(colName.getByteArray());
          int position = listOfColumns.get(current);
          key.addColumnPosition(position);
        });
      } catch (CacheClosedException cce) {
        throw new MException(cce.getMessage());
      }
    }
  }

  // GEN-899
  private void verifyKeyForRange(final byte[] rowKey) {

    Map<Integer, Pair<byte[], byte[]>> keySpace = this.tableDescriptor.getKeySpace();
    boolean keyOutOfRangeFlag = true;

    if (keySpace != null) {
      for (Map.Entry<Integer, Pair<byte[], byte[]>> entry : keySpace.entrySet()) {
        if ((Bytes.compareToPrefix(rowKey, entry.getValue().getFirst()) >= 0)
            && (Bytes.compareToPrefix(rowKey, entry.getValue().getSecond()) <= 0)) {
          keyOutOfRangeFlag = false;
          break;
        }
      }
    }
    if (keyOutOfRangeFlag == true) {
      throw new RowKeyOutOfRangeException(
          "Row Key " + Arrays.toString(rowKey) + " out of defined Range");
    }
  }

  @Override
  public void put(Put put) throws IllegalArgumentException, IllegalColumnNameException,
          MCacheInternalErrorException, RowKeyOutOfRangeException, MCacheLowMemoryException {
    nullCheck(put, "Invalid Put. operation with null values is not allowed!");
    verifyAllColumnNames(put);
    verifyColumnForNoValues(put);
    verifyForTimeStamp(put);
    verifyKeyForRange(put.getRowKey());

    Object key = new MKeyBase(put.getRowKey());
    MValue value = MValue.fromPut(tableDescriptor, put, MOperation.PUT);

    // for debugging purpose only.
    put.setSerializedSize(value.length);
    try {
      this.tableRegion.put(key, value);
    } catch (GemFireException ge) {
      checkSecurityException(ge);
      // GEN-983
      // when put fails with error LowMemoryException then just
      // throw this exception, It is expected that client will catch
      // the exception and try again, letting eviction to happen
      if (ge.getRootCause() instanceof LowMemoryException) {
        handleLowMemoryException(ge);
      }

      logger.error("MTableImpl::put::Internal Error ", ge);
      throw new MCacheInternalErrorException("Put operation failed", ge);
    }
  }

  private byte[] getByteArray(MTableDescriptor tableDescriptor, Put put) {
    ByteArrayKey columnName = new ByteArrayKey();

    Map<MColumnDescriptor, Integer> colToPositionMap = tableDescriptor.getColumnDescriptorsMap();
    int numOfColumns = tableDescriptor.getNumOfColumns();

    /* Construct the MTableRow, which is serialized version of MPut */
    MTableRow row = new MTableRow(numOfColumns, true, put.getTimeStamp());

    Map<ByteArrayKey, Object> columnValueMap = put.getColumnValueMap();

    /*
     * Iterate over all the column in the schema and writes its serialized byte[] into the MTableRow
     */
    for (Map.Entry<MColumnDescriptor, Integer> column : colToPositionMap.entrySet()) {
      columnName.setData(column.getKey().getColumnName());
      Object columnValue = columnValueMap.get(columnName);
      row.writeColumn(column.getKey(), columnValue);
    }
    return row.getByteArray();
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] columnName, byte[] value, Put put) {
    nullCheck(put, "Invalid Put. operation with null values is not allowed!");
    verifyAllColumnNames(put);
    verifyForTimeStamp(put);
    Map<MColumnDescriptor, Integer> colPositionMap = this.tableDescriptor.getColumnDescriptorsMap();
    MColumnDescriptor current = new MColumnDescriptor(columnName);
    Integer position = colPositionMap.get(current);
    if (position == null) {
      throw new IllegalColumnNameException("Column is not defined in schema");
    }

    if (!Bytes.equals(row, put.getRowKey())) {
      throw new IllegalStateException(
          "For checkAndPut operation rowKey and Put Object key should be same.");
    }

    Object key = new MKeyBase(put.getRowKey());

    // TODO: over-loading the meaning of columnPositions like this is dangerous; separate out check
    MValue mValue = MValue.fromPut(tableDescriptor, put, MOperation.CHECK_AND_PUT);
    mValue.setCondition(position, value);
    try {
      this.tableRegion.put(key, mValue);
    } catch (GemFireException ge) {
      checkSecurityException(ge);
      if (ge.getRootCause() instanceof MCheckOperationFailException) {
        return false;
      }
      if (ge.getRootCause() instanceof RowKeyDoesNotExistException) {
        throw (RowKeyDoesNotExistException) ge.getRootCause();
      }
      logger.error("MTableImpl::put::Internal Error ", ge);
      throw new MCacheInternalErrorException("Put operation failed", ge);
    }
    return true;
  }

  /**
   * Checks if the time-stamp is provided by user TimeStamp should be allowed only for table type
   * MTableType.ORDERED_VERSIONED
   */
  private void verifyForTimeStamp(Put put) {}

  private void nullCheck(Object object, String message) {
    if (object == null) {
      throw new IllegalArgumentException(message);
    }
  }

  private void zeroSizeCheck(Collection<?> collection, String message) {
    if (collection.size() == 0) {
      throw new IllegalArgumentException(message);
    }
  }


  /**
   * only for debugging purpose
   **/
  private long byteArraySize = 0;

  public long getByteArraySizeBatchPut() {
    return this.byteArraySize;
  }

  public void resetByteArraySizeBatchPut() {
    this.byteArraySize = 0;
  }

  @Override
  public void put(List<Put> puts, final Object callbackArg)
      throws IllegalArgumentException, IllegalColumnNameException, MCacheInternalErrorException,
          RowKeyOutOfRangeException, MCacheLowMemoryException {
    // long l1 = System.nanoTime();
    nullCheck(puts, "Invalid Puts. operation with null values is not allowed!");
    zeroSizeCheck(puts, "Invalid Puts. Operation with 0 size keys is not allowed");

    Map<Object, Object> putAllMap = new LinkedHashMap<>(puts.size());

    for (Put put : puts) {
      nullCheck(put, "Invalid Put. operation with null values is not allowed!");
      verifyAllColumnNames(put);
      verifyColumnForNoValues(put);
      verifyForTimeStamp(put);
      verifyKeyForRange(put.getRowKey());
      Object key = new MKeyBase(put.getRowKey());
      /*
       * GEN-1554: Disable the check for existence of same key within a batch. Disabled the check
       * for now.. need to review it later.
       */
      MValue value = MValue.fromPut(tableDescriptor, put, MOperation.PUT);
      this.byteArraySize += value.length;
      if (putAllMap.containsKey(key) && tableDescriptor.getMaxVersions() > 1) {
        // GEN-2011 : If multiple keys are present, then send values as list
        Object valueList = putAllMap.get(key);
        if (valueList instanceof List) {
          ((List) valueList).add(value);
        } else {
          Object temp = valueList;
          valueList = new LinkedList<Object>();
          ((List) valueList).add(temp);
          ((List) valueList).add(value);
        }
        putAllMap.put(key, valueList);
      } else {
        putAllMap.put(key, value);
      }
      // key.setIsPutOp();
      // byte[] value = getByteArray(tableDescriptor, put);
    }

    try {
      /** -- PutAll profiling code -- **/
      // long l2 = System.nanoTime();
      this.tableRegion.putAll(putAllMap, callbackArg);
      // if (callbackArg instanceof Object2LongOpenHashMap) {
      // ((Object2LongOpenHashMap) callbackArg).addTo("clientPutOverhead", (l2 - l1));
      // ((Object2LongOpenHashMap) callbackArg).addTo("clientOnlyPutAll", (System.nanoTime() - l2));
      // }
    } catch (GemFireException ge) {
      checkSecurityException(ge);
      // GEN-983
      // when put fails with error LowMemoryException then just
      // throw this exception, It is expected that client will catch
      // the exception and try again, letting eviction to happen

      if (ge.getRootCause() instanceof LowMemoryException) {
        handleLowMemoryException(ge);
      }
      logger.error("MTableImpl::BatchPut::Internal Error ", ge);
      throw new MCacheInternalErrorException("BatchPut operation failed", ge);
    }
  }

  @Override
  public void put(List<Put> puts) throws IllegalArgumentException, IllegalColumnNameException,
          MCacheInternalErrorException, RowKeyOutOfRangeException, MCacheLowMemoryException {
    put(puts, null);
  }

  /**
   * populates the number of columns in terms of positon and also validate the columns. also sets
   * the timestamp of the key
   */
  private MTableKey setupKeyWithColumnsAndTimeStamp(Get get) {
    MTableKey key = new MTableKey(get.getRowKey());
    if (get.getColumnNameList().size() > 0) {
      try {
        List<byte[]> columnNameList = get.getColumnNameList();
        verifyAllColumnNames(columnNameList);
        Map<MColumnDescriptor, Integer> listOfColumns =
            this.tableDescriptor.getColumnDescriptorsMap();
        MColumnDescriptor current = new MColumnDescriptor();
        for (byte[] columnName : columnNameList) {
          current.setColumnName(columnName);
          int position = listOfColumns.get(current);
          key.addColumnPosition(position);
        }
      } catch (CacheClosedException cce) {
        throw new MException(cce.getMessage());
      }
    }
    key.setTimeStamp(get.getTimeStamp());
    // set operation as get
    key.setIsGetOp();
    key.setMaxVersions(getMaxVersions(get));

    if (get.getMaxVersionsToFetch() > tableDescriptor.getMaxVersions()) {
      throw new IllegalArgumentException(
          "You cannot configure MaxVersions greater than MaxVersions for table. Defined MaxVersions for table "
              + tableRegion.getName() + " is " + tableDescriptor.getMaxVersions());
    }

    return key;
  }

  @Override
  public Row get(Get get) throws IllegalArgumentException, IllegalColumnNameException,
          MCacheInternalErrorException, RowKeyOutOfRangeException {
    nullCheck(get, "Invalid Get. operation with null values is not allowed!");
    verifyKeyForRange(get.getRowKey());

    MTableKey key = setupKeyWithColumnsAndTimeStamp(get);

    try {
      Object result = this.tableRegion.get(key);
      if (!tableDescriptor.getUserTable()) {
        // for now keeping as it is for userTable false
        // Used by hive
        return new RowImpl(key.getBytes(), (byte[]) result, this.tableDescriptor,
            get.getColumnNameList());
      }
      // special case when multiversion get is done
      if (result instanceof MultiVersionValueWrapper) {
        result = ((MultiVersionValueWrapper) result).getVal();
      }

      Row newRes =
          new FormatAwareRow(key.getBytes(), result, tableDescriptor, get.getColumnNameList());
      return newRes;

    } catch (GemFireException ge) {
      checkSecurityException(ge);
      logger.error("MTableImpl::get::Internal Error ", ge);
      throw new MCacheInternalErrorException("Get operation failed", ge);
    }
  }

  private int getMaxVersions(Get get) {
    int maxVersionsToFetch = 1;
    if (get.isOlderValueFirst()) {
      // postive value
      if (get.getMaxVersionsToFetch() == 0) {
        maxVersionsToFetch = tableDescriptor.getMaxVersions();
      } else {
        maxVersionsToFetch = get.getMaxVersionsToFetch();
      }
    } else {
      // negative value
      if (get.getMaxVersionsToFetch() == 0) {
        maxVersionsToFetch = tableDescriptor.getMaxVersions() * (-1);
      } else {
        maxVersionsToFetch = get.getMaxVersionsToFetch() * (-1);
      }
    }
    return maxVersionsToFetch;
  }

  @Override
  public Row[] get(List<Get> gets) throws IllegalArgumentException, IllegalColumnNameException,
          MCacheInternalErrorException, RowKeyOutOfRangeException {
    nullCheck(gets, "Invalid Get. operation with null values is not allowed");
    zeroSizeCheck(gets, "Invalid Gets. Operation with 0 size keys is not allowed");
    Row[] results = null;

    /* Create the keys list */
    Set<Object> getAllKeys = new LinkedHashSet<>(gets.size());
    for (Get get : gets) {
      nullCheck(get, "Invalid Get operation with null value found in list of gets");
      verifyKeyForRange(get.getRowKey());
      MTableKey key = setupKeyWithColumnsAndTimeStamp(get);
      if (getAllKeys.contains(key)) {
        throw new IllegalArgumentException("Cannot add get with same rowkey twice in batch get");
      }
      getAllKeys.add(key);
    }

    try {
      /* Make the call the Apache Geode */
      Map<Object, Object> resultMap = this.tableRegion.getAll(getAllKeys);
      if (resultMap == null) {
        return results;
      }
      results = new Row[getAllKeys.size()];
      int index = 0;
      /**
       * KEY list has the same order as that of input Gets
       */
      for (Object key : getAllKeys) {
        Object v = resultMap.get(key);
        /* Result should be always of type byte [] */
        if (v == null || v instanceof byte[] || v instanceof byte[][]
            || v instanceof MultiVersionValueWrapper) {
          /* Make the Result ready for consumption */
          if (v instanceof MultiVersionValueWrapper) {
            v = ((MultiVersionValueWrapper) v).getVal();
          }
          results[index] = new FormatAwareRow(((MTableKey) key).getBytes(), v, tableDescriptor,
              gets.get(index).getColumnNameList());
          index += 1;
        } else {
          throw new MCacheInternalErrorException("Internal Error, Value Type Does not Match " + v);
        }
      }

    } catch (GemFireException ge) {
      logger.error("MTableImpl::batchGet::Internal Error ", ge);
      throw new MCacheInternalErrorException("BatchGet operation failed", ge);
    }

    return results;
  }

  private boolean isRowMatchPredicates(final MPredicateHolder[] predicates,
      final List<Cell> cells) {
    boolean ret = true;
    Predicate predicate;
    for (final MPredicateHolder ph : predicates) {
      predicate = ph.getPredicate();
      if (!predicate.test(cells.get(ph.columnIdx).getColumnValue())) {
        ret = false;
        break;
      }
    }
    return ret;
  }

  @Override
  public void delete(Delete delete) throws IllegalArgumentException, IllegalColumnNameException,
          MCacheInternalErrorException, RowKeyOutOfRangeException {
    nullCheck(delete, "Invalid Delete. operation with null values is not allowed!");
    verifyKeyForRange(delete.getRowKey());
    _delete(delete, false, null, null);
  }


  private void _delete(Map<Delete, Filter> deleteFilterMap) {
    if (deleteFilterMap == null) {
      throw new IllegalArgumentException("Map is null");
    }
    boolean result = true;
    Exception ex = null;

    // execute the function on all servers
    try {
      Function atcf = new DeleteWithFilterFunction();
      FunctionService.registerFunction(atcf);
      List<Object> inputList = new ArrayList<Object>();
      inputList.add(getName());
      inputList.add(deleteFilterMap);

      Execution members;
      if (amplCache.isClient()) {
        members = FunctionService.onServers(amplCache.getDefaultPool()).withArgs(inputList);
      } else {
        members = FunctionService.onMembers(MTableUtils.getAllDataMembers(this.amplCache))
            .withArgs(inputList);
      }

      List functionResult = (List) members.execute(atcf.getId()).getResult();
      for (int i = 0; i < functionResult.size(); i++) {
        Object receivedObj = functionResult.get(i);
        if (receivedObj instanceof Boolean) {
          result = result && (Boolean) receivedObj;
        }
        if (receivedObj instanceof Exception) {
          result = false;
          ex = (Exception) receivedObj;
          break;
        }
      }
    } catch (Exception e) {
      result = false;
      ex = e;
    }

    if (!result) {
      logger.debug("Deletion failed failed with exception " + ex);
      if (ex instanceof MCacheInternalErrorException) {
        throw (MCacheInternalErrorException) ex;
      }
      checkSecurityException(ex);
      if (ex instanceof GemFireException) {
        throw (GemFireException) ex;
      }
      if (ex instanceof RuntimeException) {
        throw (RuntimeException) ex;
      }
    }
  }

  /**
   * Private method for delete and checkAndDelete
   */
  private boolean _delete(Delete delete, boolean isCheckAndDelete, byte[] value,
                          byte[] checkColumnName) {
    MTableKey key = new MTableKey(delete.getRowKey());
    int noColsTobeDeleted = delete.getColumnNameList().size();
    int totalNumOfCols = getTotalNumOfColumns();
    // Verify for valid column names
    if (noColsTobeDeleted > 0) {
      verifyAllColumnNames(delete.getColumnNameList());
    }

    try {
      if (isCheckAndDelete) {
        // if operation is check and Delete
        key.setIsCheckAndDeleteOp();
        key.setColumnValue(value);
        Map<MColumnDescriptor, Integer> colPositionMap =
            this.tableDescriptor.getColumnDescriptorsMap();

        Iterator<Map.Entry<MColumnDescriptor, Integer>> colPosItr =
            colPositionMap.entrySet().iterator();
        MColumnDescriptor current = new MColumnDescriptor(checkColumnName);
        Integer position = colPositionMap.get(current);
        if (position == null) {
          throw new IllegalColumnNameException("Column is not defined in schema");
        }
        key.addColumnPosition(position);
        // specially required for checkAndDelete in LocalRegion
        key.setNumberofColumns(colPositionMap.size());
      } else {
        key.setIsDeleteOp();
      }

      // adding extra condition, as if table is single version and timestamp is provided
      // then if timestamp matches then we should delete the entry rather than keeping empty entry
      if ((delete.getTimestamp() != 0 && tableDescriptor.getMaxVersions() > 1)
          || (noColsTobeDeleted > 0 && noColsTobeDeleted + 1 != totalNumOfCols)) {
        /*
         * RLG: we should be able to just construct a put operation in the case where only some
         * column values are being deleted. Though this approach works it adds a lot of code that
         * must be maintained to do what a put or checkAndPut could accomplish.
         */
        Map<MColumnDescriptor, Integer> listOfColumns =
            this.tableDescriptor.getColumnDescriptorsMap();
        MColumnDescriptor current = new MColumnDescriptor();
        if (noColsTobeDeleted > 0) {
          for (byte[] columnName : delete.getColumnNameList()) {
            current.setColumnName(columnName);
            int position = listOfColumns.get(current);
            key.addColumnPosition(position);
          }
        }
        if (delete.getTimestamp() != 0) {
          key.setTimeStamp(delete.getTimestamp());
        }
        try {
          this.tableRegion.put(key, new byte[0]);
        } catch (RowKeyDoesNotExistException re) {
          throw re;
        } catch (GemFireException ge) {
          if (ge.getRootCause() instanceof MCheckOperationFailException) {
            return false;
          }
          if (ge.getRootCause() instanceof MCacheInternalErrorException) {
            return false;
          }
          if (ge instanceof EntryNotFoundException
              || ge.getRootCause() instanceof RowKeyDoesNotExistException) {
            throw (RowKeyDoesNotExistException) ge.getRootCause();
          }
          // GEN-983
          // when put fails with error LowMemoryException then just
          // throw this exception, It is expected that client will catch
          // the exception and try again, letting eviction to happen
          if (ge.getRootCause() instanceof LowMemoryException) {
            handleLowMemoryException(ge);
          }

          logger.error("MTableImpl::delete::Internal Error ", ge);
          throw new MCacheInternalErrorException("Delete operation failed", ge);
        }
        return true;
      }
      key.setTimeStamp(delete.getTimestamp());
      try {
        this.tableRegion.destroy(key);
      } catch (GemFireException ge) {
        checkSecurityException(ge);
        if (ge.getRootCause() instanceof RegionDestroyedException) {
          logger.error("MTableImpl::delete::Internal Error ", ge);
          throw new MCacheInternalErrorException("Delete operation failed",
              (GemFireException) ge.getRootCause());
        }
        if (ge instanceof EntryNotFoundException
            || ge.getRootCause() instanceof RowKeyDoesNotExistException) {
          throw new RowKeyDoesNotExistException("Row ID does not exist");
        }
        if (ge.getRootCause() instanceof MCheckOperationFailException) {
          return false;
        }
        return false;
        // throw new MCacheDeleteFailedException("Delete operation has failed!");
      }
      return true;
    } catch (CacheClosedException cce) {
      throw new MException(cce.getMessage());
    }
  }

  public boolean checkAndDelete(byte[] row, byte[] columnName, byte[] value, Delete delete)
      throws IllegalArgumentException, IllegalColumnNameException, MCacheInternalErrorException {
    /*
     * checkAndDelete is divided into 2 parts 1) checkAndDelete with certain column deletion - This
     * is nothing but checkAndPut 2) checkAndDelete with rowKey (This scenario is check for certain
     * column value if true then delete total row
     */
    nullCheck(delete, "Invalid Delete. operation with null values is not allowed!");
    if (!Bytes.equals(row, delete.getRowKey())) {
      throw new IllegalStateException(
          "For checkAndDelete operation rowKey and delete Object key should be same.");
    }
    return _delete(delete, true, value, columnName);
  }

  @Override
  public void delete(List<Delete> deletes)
      throws IllegalArgumentException, IllegalColumnNameException, MCacheInternalErrorException,
          MCacheLowMemoryException, MCacheLowMemoryException {
    throw new UnsupportedOperationException("Bulk delete is not supoorted");
  }

  @Override
  public void delete(Map<Delete, Filter> deleteFilterMap)
      throws IllegalArgumentException, IllegalColumnNameException, MCacheInternalErrorException,
          MCacheLowMemoryException, MCacheLowMemoryException {
    _delete(deleteFilterMap);
  }


  @Override
  public int getKeyCount() {
    return this.tableRegion.keySetOnServer().size();
  }

  @Override
  public MTableDescriptor getTableDescriptor() {
    return CopyHelper.copy(this.tableDescriptor);
  }

  @Override
  public String getName() {
    return this.getTableRegion().getName();
  }

  private byte[][] processResponseFromSetofKeys(List<List<byte[]>> listOfResponseFromAllServers) {
    Set<ByteArrayKey> setOfStartKeys = new HashSet<>();

    int index = 0;

    listOfResponseFromAllServers.forEach((R) -> {
      R.forEach((S) -> {
        setOfStartKeys.add(new ByteArrayKey(S));
      });
    });

    byte[][] result = new byte[setOfStartKeys.size()][];
    for (ByteArrayKey startKey : setOfStartKeys) {
      result[index] = new byte[startKey.getByteArray().length];
      result[index++] = startKey.getByteArray();
    }

    return result;
  }

  @Override
  public Scanner getScanner(Scan scan) throws MException {
    if (scan == null) {
      throw new IllegalArgumentException("Scan Object cannot be null");
    }
    if (this.amplCache.isClient()) {
      return new MClientScannerUsingGetAll(scan, this);
    } else {
      return new ScannerServerImpl(scan, this);
    }
  }

  /**
   * A parallel scanner may improve performance but does require
   * <p>
   * more threads (depending on number of splits for this method)
   * <p>
   * so is best on client machines with higher core counts. This
   * <p>
   * method starts a scanner on each split (every region bucket) and combines the results. Batched
   * scans are not supported by
   * <p>
   * parallel scanners.
   * <p>
   * <p>
   * <p>
   * <b>NOTE: Parallel scans are always unordered for
   * <p>
   * {@link MTableType#ORDERED_VERSIONED}</b>, as this type is
   * <p>
   * range partitioned. In order to preserve order and obtain
   * <p>
   * superior performance table rows must be randomly distributed
   * <p>
   * across buckets by key, and this is not the case for range
   * <p>
   * partitioned tables.
   * 
   * @param scan the {@link Scan} object that defined the scan.
   * @return the parallel scanner
   */
  @Override
  public Scanner getParallelScanner(Scan scan) {
    return new MClientParallelScanner(scan, this, 1);
  }

  /**
   * A parallel scanner may improve performance but does require
   * <p>
   * more threads (depending on number of splits for this method)
   * <p>
   * so is best on client machines with higher core counts. This
   * <p>
   * method starts a scanner on each split (every region bucket) and combines the results. Batched
   * scans are not supported by
   * <p>
   * parallel scanners.
   * <p>
   * <p>
   * <p>
   * <b>NOTE: Parallel scans are always unordered for
   * <p>
   * {@link MTableType#ORDERED_VERSIONED}</b>, as this type is
   * <p>
   * range partitioned. In order to preserve order and obtain
   * <p>
   * superior performance table rows must be randomly distributed
   * <p>
   * across buckets by key, and this is not the case for range
   * <p>
   * partitioned tables.
   * 
   * @param scan the {@link Scan} object that defined the scan.
   * @return the parallel scanner
   */
  @Override
  public Scanner getParallelScanner(Scan scan, int groupSize) {
    return new MClientParallelScanner(scan, this, groupSize);
  }

  /**
   * Retrieves Client Scanner instance for Mash cli
   * 
   * @return returns the client side scanner
   */
  public Scanner getClientScanner(Scan scan) throws MException {
    return new MClientScannerUsingGetAll(scan, this);
  }

  @Override
  public int getTotalNumOfColumns() {
    return this.tableDescriptor.getColumnDescriptorsMap().size();
  }

  private MResultCollector coprocessorExecutionOnBucketId(MCoprocessor endpoint, int bucketId,
                                                          ResultCollector rc, MExecutionRequest request) {
    Execution members = null;

    members = FunctionService.onMTable(this).withTableSplitId(bucketId).withCollector(rc)
        .withArgs(request).forCoProcessor();
    members.execute(endpoint);

    return new MResultCollectorImpl(rc);
  }

  @Override
  public List<Object> coprocessorService(String className, String methodName, byte[] row,
      MExecutionRequest request) {
    nullCheck(row, "rowKey found null while coprocessor execution!");
    nullCheck(className, "className is null while coprocessor execution!");

    int bucketId = -1;
    // For both ORDERED and UNORDERED Table, given a key, find a corresponding bucket and execute a
    // co-processor on it.

    if (this.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
      bucketId = PartitionedRegionHelper.getHashKey(new MKeyBase(row),
          this.getTableDescriptor().getTotalNumOfSplits());
    } else {
      bucketId = MTableUtils.getBucketId(row, this.getTableDescriptor());
    }

    if (bucketId != -1) {
      try {
        MCoprocessor function = (MCoprocessor) MCoprocessorUtils.createInstance(className);
        request.setMethodName(methodName);
        String coprocessorName =
            this.getName() + "_" + function.getClass().getName() + "_" + bucketId;
        function.setId(coprocessorName);
        // MCoprocessorService.registerCoprocessor(function.getId(), function);

        DefaultResultCollector rc = new DefaultResultCollector();

        MResultCollector mResultCollector =
            coprocessorExecutionOnBucketId(function, bucketId, rc, request);
        return (ArrayList<Object>) mResultCollector.getResult();
      } catch (GemFireException fe) {
        logger.error("MTableImpl::coprocessorService::Internal Error ", fe);
        throw new MCoprocessorException("coprocessor execution failed!");
      } catch (MCoprocessorException mce) {
        logger.error("MTableImpl::coprocessorService::Internal Error ", mce);
        throw new MCoprocessorException("coprocessor execution failed!" + mce.getMessage());
      }
    } else {
      throw new IllegalArgumentException(
          "[Error] Could not find a valid bucketID for a specified Key!");
    }
  }

  @Override
  public Map<Integer, List<Object>> coprocessorService(String className, String methodName,
      byte[] startKey, byte[] endKey, MExecutionRequest request) {

    if (this == null) {
      throw new IllegalStateException(
          "table must not be null while using coprocessorService execution!");
    }
    try {
      // Register coprocessor/function on client.
      if (request == null) {
        request = new MExecutionRequest();
      }
      request.setMethodName(methodName);

      // Per-Bucket Execution
      Map<Integer, List<Object>> bucketsToResult = new TreeMap();
      Set<Integer> bucketIdSet = null;
      if (this.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        bucketIdSet = MTableUtils.getBucketIdSet(startKey, endKey, this.tableDescriptor);
      } else if (this.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
        bucketIdSet = MTableUtils.getPrimaryBucketSet(this.getName());
      } else {
        throw new MCoprocessorException("Invalid table type!");
      }

      for (Integer bucketId : bucketIdSet) {
        MCoprocessor function = (MCoprocessor) MCoprocessorUtils.createInstance(className);
        String coprocessorName =
            this.getName() + "_" + function.getClass().getName() + "_" + bucketId;
        function.setId(coprocessorName);
        // It seems no need to register CP at client
        MCoprocessorService.registerCoprocessor(coprocessorName, function);
        DefaultResultCollector rc = new DefaultResultCollector();
        MResultCollector mResultCollector =
            coprocessorExecutionOnBucketId(function, bucketId, rc, request);

        bucketsToResult.put(bucketId, (List<Object>) mResultCollector.getResult());
      }
      return bucketsToResult;

    } catch (GemFireException fe) {
      checkSecurityException(fe);
      logger.error("MTableImpl::coprocessorService::Internal Error ", fe);
      throw new MCoprocessorException("coprocessor execution failed!");
    } catch (MCoprocessorException mce) {
      logger.error("MTableImpl::coprocessorService::Internal Error ", mce);
      // throw new MCoprocessorException("coprocessor execution failed!");
      throw mce;
    }
  }

  @Override
  public MTableLocationInfo getMTableLocationInfo() {
    return mTableLocationInfo;
  }

  @Override
  public boolean isEmpty() {
    return MTableUtils.isTableEmpty(this);
  }

  /**
   * Does get operation using the Scan Object
   * 
   * @return Number of results returned
   */
  @Override
  public RowEndMarker scan(Scan scan, ServerLocation serverLocation,
                           BlockingQueue<Object> resultQueue) {
    return srp.scan(scan, resultQueue, tableDescriptor, serverLocation);
  }

  // GEN-983
  private void handleLowMemoryException(final GemFireException ge) {
    throw new MCacheLowMemoryException(ge.getRootCause());
  }

  /**
   * Provide the internal region.
   * 
   * @return the internal Geode region
   */
  @Override
  public Region<Object, Object> getInternalRegion() {
    return tableRegion;
  }

  public void addGatewaySenderId(String senderId) {
    ((PartitionedRegion) this.tableRegion).addGatewaySenderId(senderId);
  }

  public Set<String> getGatewaySenderId() {
    return ((PartitionedRegion) this.tableRegion).getGatewaySenderIds();
  }
}
