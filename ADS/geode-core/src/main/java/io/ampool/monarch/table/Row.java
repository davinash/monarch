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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.internal.SingleVersionRow;

/**
 * Single row result of a {@link Get}.
 * <p>
 *
 * Convenience methods are available that return various {@link Cell} structures and values
 * directly.
 * <p>
 * To get a complete mapping of all cells in the Result, {@link #getCells()}.
 * <p>
 * The underlying {@link Cell} objects can be accessed through the method {@link #getCells()}. This
 * will create a List from the internal Cell [].
 *
 * @since 0.2.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Row extends Comparable<Row>, Serializable {
  /**
   * Create a list of the Cell's in this result.
   * 
   * @return List of Cells; Cell value can be null if its value is deleted.
   */
  List<Cell> getCells();

  /**
   * Get the size of the underlying MCell []
   * 
   * @return size of MCell
   */
  int size();

  /**
   * Check if the underlying MCell [] is empty or not
   * 
   * @return true if empty
   */
  boolean isEmpty();

  /**
   * Gets the timestamp associated with the row value.
   * 
   * @return timestamp for the row
   */
  Long getRowTimeStamp();

  /**
   * Gets the row Id for this instance of result
   * 
   * @return rowid of this result
   */
  byte[] getRowId();

  public int compareTo(Row item);


  /**
   * Get all versions.
   * 
   * @return Map of timestamp (version) to {@link SingleVersionRow}
   */
  Map<Long, SingleVersionRow> getAllVersions();

  /**
   * Get particular version. If not found returns null.
   *
   * @param timestamp - Version identifier
   * @return {@link SingleVersionRow}
   */
  SingleVersionRow getVersion(Long timestamp);

  /**
   * Gives latest version.
   * 
   * @return Latest version
   */
  SingleVersionRow getLatestRow();

  /**
   * Get the raw value respective to this row.
   *
   * @return the raw value
   */
  Object getRawValue();

  /**
   * Get the de-serialized value of the column specified by the index.
   *
   * @param columnIdx the index of a column within row
   * @return the de-serialized value
   */
  default Object getValue(final int columnIdx) {
    return getCells().get(columnIdx).getColumnValue();
  }
}
