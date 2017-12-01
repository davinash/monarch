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
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.types.TypeHelper;


@InterfaceAudience.Public
@InterfaceStability.Stable
public class DeSerializedRow implements Row {
  private static final byte[] EMPTY = new byte[0];
  private byte[] key = EMPTY;
  protected Object[] values = null;

  public DeSerializedRow(final int size) {
    this.values = new Object[size];
  }

  public DeSerializedRow(final Object[] values) {
    this.values = values;
  }

  public void setValue(final int columnIdx, final Object value) {
    this.values[columnIdx] = value;
  }

  /**
   * Create a list of the Cell's in this result.
   *
   * @return List of Cells; Cell value can be null if its value is deleted.
   */
  @Override
  public List<Cell> getCells() {
    return null;
  }

  /**
   * Get the size of the underlying MCell []
   *
   * @return size of MCell
   */
  @Override
  public int size() {
    return this.values == null ? 0 : this.values.length;
  }

  /**
   * Check if the underlying MCell [] is empty or not
   *
   * @return true if empty
   */
  @Override
  public boolean isEmpty() {
    return this.values == null || this.values.length == 0;
  }

  /**
   * Gets the timestamp associated with the row value.
   *
   * @return timestamp for the row
   */
  @Override
  public Long getRowTimeStamp() {
    return 0L;
  }

  /**
   * Gets the row Id for this instance of result
   *
   * @return row-id of this result
   */
  @Override
  public byte[] getRowId() {
    return this.key;
  }

  @Override
  public int compareTo(Row item) {
    return 0;
  }

  /**
   * Get all versions.
   *
   * @return Map of timestamp (version) to {@link SingleVersionRow}
   */
  @Override
  public Map<Long, SingleVersionRow> getAllVersions() {
    return null;
  }

  /**
   * Get particular version. If not found returns null.
   *
   * @param timestamp - Version identifier
   * @return {@link SingleVersionRow}
   */
  @Override
  public SingleVersionRow getVersion(Long timestamp) {
    return null;
  }

  /**
   * Gives latest version.
   *
   * @return Latest version
   */
  @Override
  public SingleVersionRow getLatestRow() {
    return null;
  }

  /**
   * Get the de-serialized value of the column specified by the index.
   *
   * @param columnIdx the index of a column within row
   * @return the de-serialized value
   */
  @Override
  public Object getValue(int columnIdx) {
    return this.values[columnIdx];
  }

  /**
   * Get the raw value respective to this row.
   *
   * @return the raw value
   */
  @Override
  public Object getRawValue() {
    return this.values;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (final Object value : values) {
      TypeHelper.deepToString(value, sb);
      sb.append(',');
    }
    return sb.toString();
  }

}
