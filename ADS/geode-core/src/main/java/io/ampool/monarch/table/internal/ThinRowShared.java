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

import java.io.Serializable;
import java.util.List;

import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.TableDescriptor;

public class ThinRowShared implements Serializable {
  private List<Cell> cells;
  private TableDescriptor descriptor;
  private List<Integer> columnIds;
  private BitMap columnBitMap;
  private boolean isFullRow = false;

  public ThinRowShared(List<Cell> cells, TableDescriptor descriptor, List<Integer> columnIds) {
    this.cells = cells;
    this.descriptor = descriptor;
    this.columnIds = columnIds;
    this.columnBitMap = new BitMap(descriptor.getNumOfColumns());
    this.isFullRow = columnIds.isEmpty() || columnIds.size() == descriptor.getNumOfColumns();
  }

  public List<Cell> getCells() {
    return cells;
  }

  public TableDescriptor getDescriptor() {
    return descriptor;
  }

  public List<Integer> getColumnIds() {
    return columnIds;
  }

  public BitMap getColumnBitMap() {
    return this.columnBitMap;
  }

  public boolean isFullRow() {
    return isFullRow;
  }
}
