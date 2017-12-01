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

package io.ampool

import io.ampool.monarch.table.Cell
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
  * The wrapper class to hold the current Row. This eliminates the need to create a
  * new Row object for each result. It reuses the mutable-row object to hold each
  * result.
  *
  * Created on: 2016-04-11
  * Since version: 0.3.2.0
  */
class MMutableRow(schema: StructType) extends Row {
  private var cells: java.util.List[Cell] = new java.util.ArrayList[Cell](0)

  /**
    * Get the number of cells/columns in this row.
    *
    * @return the number of columns
    */
  override def length: Int = cells.size()

  /**
    * Get de-serialized column value for the specified column.
    *
    * @param i the column-index
    * @return the de-serialized column value
    */
  override def get(i: Int): Any = {
    DataConverter.convertRead(schema(i).dataType, cells.get(i).getColumnValue)
  }

  /**
    * Set the cells for this row.
    *
    * @param currentCells the list of cells
    * @return this object
    */
  def setCells(currentCells: java.util.List[Cell]): MMutableRow = {
    cells = currentCells
    this
  }

  override def copy(): Row = this
}
