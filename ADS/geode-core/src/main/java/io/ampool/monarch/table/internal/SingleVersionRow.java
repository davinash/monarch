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

/**
 *
 * Class representing row having cells
 *
 */
public interface SingleVersionRow extends Comparable<SingleVersionRow>, Serializable {

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
   * Gets the row Id for this instance of result
   * 
   * @return rowid of this result
   */
  byte[] getRowId();

  /**
   * Get timestamp
   * 
   * @return Timestamp of current row
   */
  Long getTimestamp();

  int compareTo(SingleVersionRow item);

}
