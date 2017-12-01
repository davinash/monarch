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

import java.io.Serializable;
import java.util.List;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.exceptions.MFilterException;

/**
 * Interface for row and column filters directly applied to server.
 *
 * A filter can expect the following call sequence:
 * <ul>
 * <li>{@link #reset()} : reset the filter state before filtering a new row.</li>
 * <li>{@link #filterAllRemaining()}: true means row scan is over; false means keep going.</li>
 * <li>{@link #filterRowKey(Row)}: true means drop this row; false means include.</li>
 * <li>{@link #filterCell(Cell, byte[])} : decides whether to include or exclude this Cell with
 * rowkey for reference. See {@link ReturnCode}.</li>
 * <li>{@link #transformCell(Cell, byte[])} : if the Cell is included, let the filter transform the
 * Cell with rowkey for reference.</li>
 * <li>{@link #transformResult(Row)} : last chance to transform entire row. Eg: Transform a row and
 * return only rowkey {@link KeyOnlyFilter}.</li>
 * <li>{@link #filterRowCells(List)}: allows direct modification of the final list to be submitted
 * <li>{@link #filterRow()}: last chance to drop entire row based on the sequence of filter calls.
 * Eg: filter a row if it doesn't contain a specified column.</li>
 * </ul>
 *
 * Filter instances are created one per region/scan. This abstract class replaces the old
 * RowFilterInterface.
 *
 * When implementing your own filters, consider inheriting {@link FilterBase} to help you reduce
 * boilerplate.
 *
 * @see FilterBase
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class Filter implements Serializable {

  private static final long serialVersionUID = -8970754071524386662L;
  /**
   * Variable indicating if the filter is configured for reverse scan
   */
  protected transient boolean reversed;

  /**
   * Reset the state of the filter between rows.
   * 
   * Concrete implementers can signal a failure condition in their code by throwing an
   * {@link MFilterException}.
   * 
   * @throws MFilterException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public void reset();

  /**
   * If this returns true, the scan will terminate.
   *
   * Concrete implementers can signal a failure condition in their code by throwing an
   * {@link MFilterException}.
   *
   * @return true to end scan, false to continue.
   * @throws MFilterException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public boolean filterAllRemaining();

  /**
   * Filters a row based on the row key. If this returns true, the entire row will be excluded. If
   * false, each KeyValue in the row will be passed to {@link #filterCell(Cell, byte[])} below.
   *
   * Concrete implementers can signal a failure condition in their code by throwing an
   * {@link MFilterException}.
   *
   * @param result entire row
   * @return true, remove entire row, false, include the row (maybe).
   * @throws MFilterException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public boolean filterRowKey(Row result);

  /**
   * A way to filter based on the column and/or the column value. Return code is described below.
   * This allows filters to filter only certain number of columns, then terminate without matching
   * ever column.
   * 
   * If your filter returns <code>ReturnCode.NEXT_ROW</code>, it should return
   * <code>ReturnCode.NEXT_ROW</code> until {@link #reset()} is called just in case the caller calls
   * for the next row.
   * 
   * Concrete implementers can signal a failure condition in their code by throwing an
   * {@link MFilterException}.
   * 
   * @param cell the MCell in question
   * @return code as described below
   * @throws MFilterException in case an I/O or an filter specific failure needs to be signaled.
   * @see Filter.ReturnCode
   */
  abstract public ReturnCode filterCell(final Cell cell, byte[] rowKey);

  /**
   * Give the filter a chance to transform the passed KeyValue. If the MCell is changed a new MCell
   * object must be returned.
   *
   * Concrete implementers can signal a failure condition in their code by throwing an
   * {@link MFilterException}.
   *
   * @param cell the MCell in question
   * @return the changed MCell
   * @throws MFilterException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public Cell transformCell(final Cell cell, byte[] rowKey);

  /**
   * Give the filter a chance to transform the passed KeyValue. If the MResult is changed a new
   * MResult object must be returned.
   *
   * Concrete implementers can signal a failure condition in their code by throwing an
   * {@link MFilterException}.
   *
   * @param result the MResult in question
   * @return the changed MResult
   * @throws MFilterException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public Row transformResult(final Row result);

  /**
   * Chance to alter the list of Cells to be submitted. Modifications to the list will carry on
   *
   * Concrete implementers can signal a failure condition in their code by throwing an
   * {@link MFilterException}.
   *
   * @param kvs the list of Cells to be filtered
   * @throws MFilterException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public List<Cell> filterRowCells(List<Cell> kvs);

  /**
   * Primarily used to avoid unnecessary checks for row level filters at server side time).
   * <p>
   * Note: Concrete implementations must return true for this method or {@link #hasFilterCell()} or
   * for both as per implementation.
   *
   * @return True if this filter actively uses {@link #filterRowKey(Row)}, {@link #filterRow()}, or
   *         {@link #transformResult(Row)}.
   */
  abstract public boolean hasFilterRow();

  /**
   * Primarily used to avoid unnecessary checks for cell level filters at server side
   * <p>
   * Note: Concrete implementations must return true for this method or {@link #hasFilterRow()} or
   * for both as per implementation.
   *
   * @return True if this filter actively uses {@link #filterCell(Cell, byte[])},
   *         {@link #filterRowCells(List)} or {@link #transformCell(Cell, byte[])}.
   */
  abstract public boolean hasFilterCell();

  /**
   * Last chance to veto row based on previous {@link #filterCell(Cell, byte[])} calls. The filter
   * needs to retain state then return a particular value for this call if they wish to exclude a
   * row if a certain column is missing (for example).
   *
   * Concrete implementers can signal a failure condition in their code by throwing an
   * {@link MFilterException}.
   *
   * @return true to exclude row, false to include row.
   * @throws MFilterException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public boolean filterRow();

  /**
   * Return codes for filterCell().
   */
  public enum ReturnCode {
    /**
     * Include the MCell
     */
    INCLUDE,
    /**
     * Include the MCell and seek to the next column skipping older versions.
     */
    INCLUDE_AND_NEXT_COL,
    /**
     * Skip this MCell
     */
    SKIP,
    /**
     * Skip this column. Go to the next column in this row.
     */
    NEXT_COL,
    /**
     * Done with columns, skip to next row. Note that filterRow() will still be called.
     */
    NEXT_ROW,
    /**
     * Seek to next key which is given as hint by the filter.
     */
    SEEK_NEXT_USING_HINT,
  }

  /**
   * alter the reversed scan flag
   * 
   * @param reversed flag
   */
  public void setReversed(boolean reversed) {
    this.reversed = reversed;
  }

  public boolean isReversed() {
    return this.reversed;
  }
}
