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

package io.ampool.monarch.table.results;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.SingleVersionRow;

/**
 * This class is only used while passing to the filter working on row key. This is to avoid every
 * time creation of row implementation which only works with Row key.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class KeyOnlyRow implements Row {

  private static final long serialVersionUID = 6612148950456791256L;
  private final byte[] keyBytes;
  private final List<Cell> cells;
  private final Long timestamp;
  private final TableDescriptor tableDescriptor;

  public KeyOnlyRow(byte[] keyBytes, TableDescriptor tableDescriptor) {
    this.keyBytes = keyBytes;
    this.cells = Collections.EMPTY_LIST;
    this.timestamp = null;
    this.tableDescriptor = tableDescriptor;
  }

  public KeyOnlyRow(byte[] keyBytes, long timestamp, int numberOfCells,
      TableDescriptor tableDescriptor) {
    this.keyBytes = keyBytes;
    this.timestamp = new Long(timestamp);
    this.cells = Collections.EMPTY_LIST;
    this.tableDescriptor = tableDescriptor;
  }

  @Override
  public List<Cell> getCells() {
    return this.cells;
  }

  @Override
  public int size() {
    return this.cells.size();
  }

  @Override
  public boolean isEmpty() {
    return this.cells.size() == 0;
  }

  @Override
  public Long getRowTimeStamp() {
    return timestamp;
  }

  @Override
  public byte[] getRowId() {
    return this.keyBytes;
  }

  @Override
  public int compareTo(Row item) {
    return Bytes.compareTo(this.keyBytes, item.getRowId());
  }

  @Override
  public Map<Long, SingleVersionRow> getAllVersions() {
    return Collections.EMPTY_MAP;
  }

  @Override
  public SingleVersionRow getVersion(Long timestamp) {
    return null;
  }

  @Override
  public SingleVersionRow getLatestRow() {
    return null;
  }

  /**
   * Get the raw value respective to this row.
   *
   * @return the raw value
   */
  @Override
  public Object getRawValue() {
    return null;
  }

  public byte[] getRawByteArray() {
    if (tableDescriptor instanceof FTableDescriptor) {

    }
    return new byte[] {1, 1, 1, 0};
  }

  public TableDescriptor getTableDescriptor() {
    return this.tableDescriptor;
  }
}
