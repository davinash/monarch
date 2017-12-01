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
package io.ampool.monarch.table.filter.internal;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.filter.RowFilter;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.internal.VersionedRow;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.BasicTypes;

/**
 * This filter is used to filter based on the key. It takes an operator (equal, greater, not equal,
 * etc) and a byte [] comparator for the row,
 * <P>
 * </P>
 * Example: using a filter to select matching row keys:
 * <P>
 * </P>
 * MScan scan = new MScan();<br>
 * MFilter filter = new RowFilter(CompareOp.EQUAL, rowId);<br>
 * scan.setFilter(filter);<br>
 * MResultScanner scanner = table.getScanner(scan);<br>
 * <P>
 * </P>
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class BlockKeyFilter extends RowFilter {

  private boolean filterOutRow = false;

  /**
   * Constructor that takes string regex as input value This is use for REGEX comparison.
   *
   * @param compareOp the compare op for row matching
   * @param value value to compare with
   */
  public BlockKeyFilter(CompareOp compareOp, String value) {
    super(compareOp, value);
  }

  /**
   * Constructor which takes bytearray as input value
   *
   * @param compareOp
   * @param value
   */
  public BlockKeyFilter(CompareOp compareOp, byte[] value) {
    super(compareOp, value);
  }


  @Override
  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public boolean hasFilterCell() {
    return false;
  }

  @Override
  public void reset() {
    this.filterOutRow = false;
  }

  @Override
  public boolean filterRow() {
    return this.filterOutRow;
  }

  @Override
  public boolean filterRowKey(Row result) {
    Object targetValue = result.getRowId();
    BasicTypes objectType = BasicTypes.BINARY;
    this.filterOutRow = false;
    if (initialValue instanceof String) {
      targetValue = Bytes.toString(result.getRowId());
      objectType = BasicTypes.STRING;
    }
    if (!doCompare(this.compareOp, targetValue, objectType, Bytes.toBytes("dummy"))) {
      this.filterOutRow = true;
    }
    return this.filterOutRow;
  }

  public boolean filterBlockKey(BlockKey blockKey) {
    return filterRowKey(new VersionedRow(Bytes.toBytes(blockKey.getStartTimeStamp())))
        && filterRowKey(new VersionedRow(Bytes.toBytes(blockKey.getEndTimeStamp())));
  }

  @Override
  public String toString() {
    return String.format("%s (%s, %s)", this.getClass().getSimpleName(), this.compareOp.name(),
        Bytes.toLong((byte[]) this.initialValue));
  }
}


