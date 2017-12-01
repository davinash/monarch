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
package io.ampool.monarch.table.filter;

import java.util.function.Predicate;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.types.CompareOp;

/**
 * This filter is used to filters in cells based on cell values. It takes columnName,
 * {@link CompareOp} operator, and a byte[] value. It uses lexicographical comparator to compares
 * values.
 *
 * For example, if passed value is 'b' and cell has 'a' and the compare operator is LESS, then we
 * will filter out this cell (return true).
 * <P>
 * </P>
 * Example:
 * <P>
 * </P>
 * MScan scan = new MScan();<br>
 * MFilter filter = new SingleColumnValueFilter("mycol1", CompareOp.LESS_OR_EQUAL, VALUE);<br>
 * scan.setFilter(filter);<br>
 * MResultScanner scanner = table.getScanner(scan);<br>
 * <P>
 * </P>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SingleColumnValueFilter extends CompareFilter {

  private static final long serialVersionUID = -1994909225088018193L;
  private byte[] columnName;
  private String columnNameStr;
  private Predicate predicate = null;

  private boolean foundColumn = false;
  private boolean foundMatchingColumnValue = false;

  /**
   * Constructor with string columname, compare operator and value of type same as column type
   *
   * @param columnName
   * @param compareOp
   * @param value
   */
  public SingleColumnValueFilter(String columnName, CompareOp compareOp, Object value) {
    super(compareOp, value);
    this.columnNameStr = columnName;
    this.columnName = columnName.getBytes();
  }

  /**
   * Constructor with columname as byte[], compare operator and value of type same as column type
   *
   * @param columnName
   * @param compareOp
   * @param value
   */
  public SingleColumnValueFilter(byte[] columnName, CompareOp compareOp, Object value) {
    super(compareOp, value);
    this.columnName = columnName;
    this.columnNameStr = Bytes.toString(columnName);
  }

  public byte[] getColumnName() {
    return columnName;
  }

  public String getColumnNameString() {
    return this.columnNameStr;
  }

  public void setCompareOp(CompareOp compareOp) {
    this.compareOp = compareOp;
  }

  @Override
  public void reset() {
    foundColumn = false;
    foundMatchingColumnValue = false;
  }

  @Override
  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public boolean hasFilterCell() {
    return true;
  }

  @Override
  public ReturnCode filterCell(Cell cell, byte[] rowKey) {
    if (this.foundMatchingColumnValue) {
      // We already found and matched the single column, all keys now pass
      return ReturnCode.INCLUDE;
    } else if (this.foundColumn) {
      // We found but did not match the single column, skip to next row
      return ReturnCode.NEXT_ROW;
    }
    if (!(Bytes.compareTo(columnName, cell.getColumnName()) == 0)) {
      return ReturnCode.INCLUDE;
    }
    foundColumn = true;
    if (filterColumnValue(cell)) {
      return ReturnCode.NEXT_ROW;
    }
    this.foundMatchingColumnValue = true;
    return ReturnCode.INCLUDE;
  }

  private boolean filterColumnValue(Cell cell) {
    boolean doFilter = false;
    if (!doCompare(compareOp, cell.getColumnValue(), cell.getColumnType(), columnName)) {
      doFilter = true;
    }
    return doFilter;
  }

  @Override
  public boolean filterRow() {
    // If column was found, return false if it was matched, true if it was not
    // If column not found, return true if we filter if missing, false if not
    return !this.foundMatchingColumnValue;
  }
}
