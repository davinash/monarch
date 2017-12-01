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

package io.ampool.monarch.table.ftable.internal;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.internal.ServerTableRegionProxy;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.exceptions.IllegalColumnNameException;
import io.ampool.monarch.table.exceptions.MCacheInternalErrorException;
import io.ampool.monarch.table.exceptions.MCacheLowMemoryException;
import io.ampool.monarch.table.exceptions.MException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.KeyOnlyFilter;
import io.ampool.monarch.table.filter.RowFilter;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.InternalTable;
import io.ampool.monarch.table.internal.MClientScannerUsingGetAll;
import io.ampool.monarch.table.internal.MTableLocationInfo;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.RowEndMarker;
import io.ampool.monarch.table.internal.ScannerServerImpl;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.CopyHelper;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class ProxyFTableRegion implements FTable, InternalTable {

  private static final Logger logger = LogService.getLogger();

  ServerTableRegionProxy srp;
  private final Region<Object, Object> tableRegion;
  private final FTableDescriptor tableDescriptor;
  private final MonarchCacheImpl cache;
  private final MTableLocationInfo mTableLocationInfo;


  private static Set<Class<? extends Filter>> unsupportedFilters = new LinkedHashSet<>();
  static {
    unsupportedFilters.add(RowFilter.class);
    unsupportedFilters.add(KeyOnlyFilter.class);
  }

  public ProxyFTableRegion(Region<Object, Object> tableRegion, FTableDescriptor tableDescriptor,
      MonarchCacheImpl cache) {
    this.tableRegion = tableRegion;
    this.tableDescriptor = tableDescriptor;

    this.cache = cache;
    // TODO Change following function to accept ProxyFTableRegion also
    this.mTableLocationInfo = null;
    if (cache.isClient()) {
      srp = new ServerTableRegionProxy(tableRegion);
    }
  }

  @Override
  public FTableDescriptor getTableDescriptor() {
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
  public int getTotalNumOfColumns() {
    return this.tableDescriptor.getColumnDescriptorsMap().size();
  }

  @Override
  public void append(final Record... records) {
    nullCheck(records, "Invalid append. operation with null values is not allowed!");
    emptyCheck(records, "Invalid number of records...");
    if (records.length == 1) {
      append(records[0]);
    } else {
      batchAppend(records);
    }
  }

  private byte[] serializeRecord(final Record record) {
    return (byte[]) tableDescriptor.getEncoding().serializeValue(tableDescriptor, record);
  }

  private void batchAppend(final Record[] records) {
    Map<Object, BlockValue> putAllMap = new LinkedHashMap<>(records.length);
    BlockValue bv;
    for (int i = 0; i < records.length; i++) {
      final Record record = records[i];
      nullCheck(record, "Invalid append. operation with null values is not allowed!");
      verifyAllColumnNames(record);
      FTableKey key = new FTableKey();
      key.setDistributionHash(getDistributionHash(record) % tableDescriptor.getTotalNumOfSplits());

      bv = putAllMap.computeIfAbsent(key, k -> new BlockValue(records.length));
      bv.checkAndAddRecord(serializeRecord(records[i]));
    }

    try {
      this.tableRegion.putAll(putAllMap);
    } catch (GemFireException ge) {
      MTableUtils.checkSecurityException(ge);
      // GEN-983
      // when put fails with error LowMemoryException then just
      // throw this exception, It is expected that client will catch
      // the exception and try again, letting eviction to happen
      if (ge.getRootCause() instanceof LowMemoryException) {
        handleLowMemoryException(ge);
      }
      logger.error("ProxyMTableRegion::BatchAppend::Internal Error ", ge);
      throw new MCacheInternalErrorException("BatchAppend operation failed", ge);
    }
  }

  /**
   * Return the distribution hash required for distributing this row in the underlying partitioned
   * region. In case the table is configured with partitioning column the hash-code of the
   * respective column value is returned else the hash-code of the row (underlying map of
   * column-name/column-value) is returned.
   *
   * @param row the input record/row
   * @return the distribution hash-code for the row
   */
  private int getDistributionHash(final Record row) {
    Map<ByteArrayKey, Object> valueMap = row.getValueMap();
    ByteArrayKey partitioningColumn = tableDescriptor.getPartitioningColumn();
    Object o =
        valueMap.containsKey(partitioningColumn) ? valueMap.get(partitioningColumn) : valueMap;
    MColumnDescriptor mColumnDescriptor =
        this.tableDescriptor.getColumnsByName().get(partitioningColumn);

    if (o instanceof byte[] && mColumnDescriptor != null
        && mColumnDescriptor.getColumnType() != BasicTypes.BINARY) {
      o = mColumnDescriptor.getColumnType().deserialize((byte[]) o);
    }
    return MTableUtils.getDeepHashCode(o);
  }

  private void append(Record record) {
    verifyAllColumnNames(record);
    FTableKey key = new FTableKey();
    key.setDistributionHash(getDistributionHash(record));

    try {
      this.tableRegion.put(key, serializeRecord(record));
    } catch (GemFireException ge) {
      MTableUtils.checkSecurityException(ge);
      // GEN-983
      // when put fails with error LowMemoryException then just
      // throw this exception, It is expected that client will catch
      // the exception and try again, letting eviction to happen
      if (ge.getRootCause() instanceof LowMemoryException) {
        handleLowMemoryException(ge);
      }
      logger.error("ProxyMTableRegion::Append::Internal Error ", ge);
      throw new MCacheInternalErrorException("Append operation failed", ge);
    }
  }

  @Override
  public Scanner getScanner(final Scan scan) throws MException {
    if (scan == null) {
      throw new IllegalArgumentException("Scan Object cannot be null");
    }
    handleScanOptionsChecks(scan);
    // Scan iScan = new Scan(scan);

    if (this.cache.isClient()) {
      return new MClientScannerUsingGetAll(scan, this);
    } else {
      return new ScannerServerImpl(scan, this);
    }
  }



  public boolean validateFilterSupportedTypes(final Scan iScan) {
    boolean filtersSupported = true;
    final Filter filter = iScan.getFilter();
    if (filter instanceof FilterList) {
      final FilterList filterList = (FilterList) iScan.getFilter();
      for (final Filter mFilter : filterList.getFilters()) {
        if (unsupportedFilters.contains(mFilter.getClass())) {
          filtersSupported = false;
          break;
        }
      }
    } else {
      if (unsupportedFilters.contains(filter.getClass())) {
        filtersSupported = false;
      }
    }
    return filtersSupported;
  }

  private void handleScanOptionsChecks(final Scan scan) {
    if (scan.batchModeEnabled()) {
      throw new MException("Batch mode is not supported for immutable tables");
    }

    if (scan.hasFilter() && !validateFilterSupportedTypes(scan)) {
      throw new MException("Unsupported filters : " + unsupportedFilters.toString());
    }
  }

  public Region<Object, Object> getTableRegion() {
    return tableRegion;
  }

  public MonarchCacheImpl getCache() {
    return cache;
  }

  private void nullCheck(Object object, String message) {
    if (object == null) {
      throw new IllegalArgumentException(message);
    }
  }

  private void emptyCheck(Record[] records, String message) {
    if (records.length == 0) {
      throw new IllegalArgumentException(message);
    }
  }

  private void verifyAllColumnNames(Record[] records) {
    for (int i = 0; i < records.length; i++) {
      Record record = records[i];
      Map<ByteArrayKey, Object> columnValueMap = record.getValueMap();
      for (Map.Entry<ByteArrayKey, Object> column : columnValueMap.entrySet()) {
        if (!tableDescriptor.getColumnsByName().containsKey(column.getKey())) {
          throw new IllegalColumnNameException("Column is not defined in schema");
        }
      }
    }
  }

  private void verifyAllColumnNames(Record record) {
    Map<ByteArrayKey, Object> columnValueMap = record.getValueMap();
    for (Map.Entry<ByteArrayKey, Object> column : columnValueMap.entrySet()) {
      if (!tableDescriptor.getColumnsByName().containsKey(column.getKey())) {
        throw new IllegalColumnNameException(
            "Column is not defined in schema: " + Bytes.toString(column.getKey().getByteArray()));
      }
    }

  }

  // GEN-983
  private void handleLowMemoryException(final GemFireException ge) {
    Set<DistributedMember> criticalMembers =
        ((LowMemoryException) ge.getRootCause()).getCriticalMembers();
    ArrayList<String> criticalMembersList = new ArrayList<>();
    criticalMembers.forEach(DM -> {
      criticalMembersList.add(DM.getName());
    });
    String errorMessage = "Members on Low Memory are " + criticalMembersList.toString();
    throw new MCacheLowMemoryException(errorMessage);
  }

  @Override
  public RowEndMarker scan(final Scan scan, final ServerLocation serverLocation,
      final BlockingQueue<Object> resultQueue) {
    return srp.scan(scan, resultQueue, tableDescriptor, serverLocation);
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
