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
import io.ampool.monarch.table.filter.CompareFilter;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;

/**
 * This filter is used to filter based on the timestamp of the version.
 * <P>
 * </P>
 * Example: using a filter to select matching row versions:
 * <P>
 * </P>
 * Scan scan = new Scan();<br>
 * Filter filter = new TimestampFilter(CompareOp.EQUAL, long_timestamp);<br>
 * scan.setFilter(filter);<br>
 * Scanner scanner = table.getScanner(scan);<br>
 * <P>
 * </P>
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TimestampFilter extends CompareFilter {


  private boolean filterOutRow = false;

  /**
   * Constructor that takes string regex as input value This is use for REGEX comparison.
   *
   * @param compareOp the compare op for row matching
   * @param value value to compare with
   */
  public TimestampFilter(CompareOp compareOp, Long value) {
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
    Object targetValue = result.getRowTimeStamp();
    BasicTypes objectType = BasicTypes.LONG;
    this.filterOutRow = false;

    if (!doCompare(this.compareOp, targetValue, objectType, Bytes.toBytes("dummy"))) {
      this.filterOutRow = true;
    }
    return this.filterOutRow;
  }
}
