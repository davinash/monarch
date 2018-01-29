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

import java.util.Arrays;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.exceptions.MWrongTableRegionException;
import org.apache.geode.internal.cache.PartitionedRegion;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MTableRegionImpl implements MTableRegion {

  private byte[] startKey = null;
  private byte[] endKey = null;
  private final String name;
  private int targetBucketId = -1;
  private final MTableImpl table;


  public MTableRegionImpl(String name, int targetBucketId) {
    this.name = name;
    this.targetBucketId = targetBucketId;

    this.table = (MTableImpl) MCacheFactory.getAnyInstance().getTable(name);
    if (table == null) {
      throw new MCoprocessorException("Table " + table + " Not Found");
    }
    MTableDescriptor tableDescriptor = table.getTableDescriptor();

    if (tableDescriptor.getTableType() == MTableType.ORDERED_VERSIONED) {
      Pair<byte[], byte[]> startEndKey = tableDescriptor.getKeySpace().get(targetBucketId);
      if (startEndKey == null) {
        throw new MCoprocessorException("Table " + table + " is not configured properly");
      }
      this.startKey = startEndKey.getFirst();
      this.endKey = startEndKey.getSecond();
    }
  }

  @Override
  public Scanner getScanner(Scan scan) throws Exception {
    MTableImpl table = (MTableImpl) MCacheFactory.getAnyInstance().getTable(this.name);
    scan.setBucketId(this.targetBucketId);
    return new ScannerServerImpl(scan, table);
  }

  @Override
  public long size() {
    PartitionedRegion pr = (PartitionedRegion) table.getTableRegion();
    return pr.getDataStore().getLocalBucketById(targetBucketId).size();
  }

  @Override
  public void put(Put put) throws MWrongTableRegionException {
    if (this.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
      checkRow(put.getRowKey(), "Put");
    }
    this.table.put(put);
  }

  @Override
  public Row get(Get get) throws MWrongTableRegionException {
    if (this.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
      checkRow(get.getRowKey(), "Get");
    }
    return this.table.get(get);
  }

  @Override
  public void delete(Delete delete) throws MWrongTableRegionException {
    if (this.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
      checkRow(delete.getRowKey(), "Delete");
    }
    this.table.delete(delete);
  }

  @Override
  public MTableDescriptor getTableDescriptor() {
    return this.table.getTableDescriptor();
  }

  @Override
  public byte[] getStartKey() {
    return this.startKey;
  }

  @Override
  public byte[] getEndKey() {
    return this.endKey;
  }

  /**
   * Make sure this is a valid row for the MTbleRegion
   */
  private void checkRow(final byte[] row, String op) throws MWrongTableRegionException {
    if (!rowIsInRange(row)) {
      throw new MWrongTableRegionException(
          "Requested row out of range for " + op + " on MTableRegion " + this + ", startKey='"
              + Bytes.toStringBinary(this.startKey) + "', getEndKey()='"
              + Bytes.toStringBinary(this.endKey) + "', row='" + Bytes.toStringBinary(row) + "'");
    }
  }

  /**
   * Determines if the specified row is within the row range specified by the specified HRegionInfo
   *
   * @param row row to be checked
   * @return true if the row is within the range specified by the MTableDesriptor
   */
  private boolean rowIsInRange(final byte[] row) {
    return ((this.startKey.length == 0) || (Bytes.compareTo(this.startKey, row) <= 0))
        && ((this.endKey.length == 0) || (Bytes.compareTo(this.endKey, row) >= 0));
  }

  @Override
  public String toString() {
    return "MTableRegionImpl{" + "startKey=" + Arrays.toString(startKey) + ", endKey="
        + Arrays.toString(endKey) + ", name='" + name + '\'' + ", targetBucketId=" + targetBucketId
        + ", table=" + table + '}';
  }
}
