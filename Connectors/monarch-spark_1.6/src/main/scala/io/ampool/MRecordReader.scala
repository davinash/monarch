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

import java.util.Collections

import io.ampool.monarch.table.{Cell, MTableDescriptor}
import io.ampool.monarch.table.internal.{MTableUtils, SingleVersionRow, VersionedRow}
import io.ampool.monarch.table.results.FormatAwareRow
import org.apache.spark.Partition
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._

/**
  * A wrapper class for reading (using Scan) the data from Ampool into Spark. The
  * underlying scanner/iterator is used to retrieve the results (from Ampool) and
  * then converted to the required format i.e. from Row to Row. The mutable
  * row is used to hold the required cells and lazily de-serialize when required.
  *
  * Created on: 2016-04-11
  * Since version: 0.3.2.0
  */
class MRecordReader(mTable: MTableWrapper, split: Partition,
                    actualColumnIds: List[Integer], outputColumnIds: List[Int]) {
  val currentRow = new MMutableRow(outputColumnIds)
  def getIterator: Iterator[Row] = {
    /** return a dummy iterator for simpler count queries without actually fetching data..
      * this is a very simple case where no columns/no filters were provided but count
      * operation was executed..
      * only to compare against meta-data based count queries like parquet.
      */
    val tableDescriptor = mTable.getTable.getTableDescriptor
    val optimization = tableDescriptor.isInstanceOf[MTableDescriptor] &&
      tableDescriptor.asInstanceOf[MTableDescriptor].getMaxVersions() > 1
    if (outputColumnIds.isEmpty && !optimization) {
      val ms = split.asInstanceOf[MResultPartition]
      val totalCount = MTableUtils.getTotalCount(mTable.getTable, ms.bucketIds, ms.predicates, true)
      new Iterator[Row] {
        var currentCount: Long = 0
        override def size: Int = totalCount.toInt
        override def hasNext: Boolean = currentCount < totalCount
        override def next(): Row = {
          currentCount += 1
          currentRow.setCells(MRecordReader.EMPTY_CELLS)
        }
      }
    } else {
      val resultIterator = mTable.getScanner(actualColumnIds, split.asInstanceOf[MResultPartition])
      var versionIterator: Iterator[Any] = Collections.emptyListIterator().asInstanceOf[java.util.Iterator[Object]];

      /** record the time spent in scan operations.. **/
      val stats = new MStats(split.index, "Scan")

      /** return the iterator to iterate Row and convert it to the required Row **/
      new Iterator[Row] {
        override def hasNext: Boolean = {
          val isNext = resultIterator.hasNext || versionIterator.hasNext
          if (!isNext) stats.printTime()
          isNext
        }

        override def next(): Row = {
          stats.resetBegTime()
          var result: Any = null
          val isNext = versionIterator.hasNext
          if (isNext) {
            result = versionIterator.next
          }
          else {
            result = resultIterator.next
            if (result.isInstanceOf[FormatAwareRow] || result.isInstanceOf[VersionedRow]) {
              versionIterator = result.asInstanceOf[io.ampool.monarch.table.Row].getAllVersions.values.iterator
              result = versionIterator.next
            }
          }

          val cells = if (result.isInstanceOf[SingleVersionRow]) result.asInstanceOf[SingleVersionRow].getCells
          else result.asInstanceOf[io.ampool.monarch.table.Row].getCells

          currentRow.setCells(cells)
          stats.computeStats(cells.map(_.getValueLength).sum)
          currentRow

        }
      }
    }
  }
}

/** static member for empty cells.. **/
object MRecordReader {
  final val EMPTY_CELLS = List[Cell]()
}
