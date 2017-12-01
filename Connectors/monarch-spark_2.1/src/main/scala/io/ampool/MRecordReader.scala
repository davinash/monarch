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

import io.ampool.monarch.table.filter.FilterList
import io.ampool.monarch.table.{Cell, MTableDescriptor, Scan}
import io.ampool.monarch.table.internal._
import io.ampool.monarch.table.results.FormatAwareRow
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

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
class MRecordReader(tableName: String,
                    split: MResultPartition,
                    parameters: Map[String, String],
                    schema: StructType) {
  private val logger = Logger.getLogger(classOf[MRecordReader])

  val currentRow = new MMutableRow(schema)
  def getIterator: Iterator[Row] = {
    /** return a dummy iterator for simpler count queries without actually fetching data..
      * this is a very simple case where no columns/no filters were provided but count
      * operation was executed..
      * only to compare against meta-data based count queries like parquet.
      */
    val table = Utils.getInternalTable(tableName, Utils.getClientCache(parameters))
    val tableDescriptor = table.getTableDescriptor
    val optimization = tableDescriptor.isInstanceOf[MTableDescriptor] &&
      tableDescriptor.asInstanceOf[MTableDescriptor].getMaxVersions > 1
    val expectedColumnIds = schema.map(e=>Integer.valueOf(tableDescriptor.getColumnByName(e.name).getIndex)).toList

    if (expectedColumnIds.isEmpty && !optimization) {
      logger.debug(s"Optimizing the count query; fetching only count.")
      val ms = split
      val totalCount = MTableUtils.getTotalCount(table, ms.bucketIds, ms.predicates, true)
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
      val resultIterator = getScanner(table, expectedColumnIds, split)
      var versionIterator: Iterator[Any] = Collections.emptyListIterator().asInstanceOf[java.util.Iterator[Object]]

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

          val cells = result match {
            case row: SingleVersionRow => row.getCells
            case _ => result.asInstanceOf[io.ampool.monarch.table.Row].getCells
          }

          currentRow.setCells(cells)
          stats.computeStats(cells.map(_.getValueLength).sum)
          currentRow
        }
      }
    }
  }

  /**
    * Instantiate the underlying Ampool table scanner with appropriate parameters.
    *
    * @param columns the list of columns to be retrieved
    * @param split the split
    * @return the instance of Ampool scanner
    */
  def getScanner(table: InternalTable, columns: List[Integer], split: MResultPartition): Iterator[io.ampool.monarch.table.Row] = {
    logger.info(s"PartitionScanner: Table= $tableName, Split= ${split.toString}")
    val scan: Scan = new Scan
    scan.setReturnKeysFlag(false)
    scan.setFilter(new FilterList(split.predicates))
    scan.setBucketToServerMap(split.bucketToServerMap)
    scan.setBucketIds(split.bucketIds)
    scan.setColumns(columns)

    // Setting scan parameters for multi versioned scan.
    val str = parameters.getOrElse(Constants.AmpoolReadFilterOnLatestVersion, "false")
    scan.setFilterOnLatestVersionOnly(str.toBoolean)
    val maxVersions = parameters.getOrElse(Constants.AmpoolMaxVersions, Constants.AmpoolDefaultMaxVersions).toInt
    val isOldestFirst = parameters.getOrElse(Constants.AmpoolReadOldestFirst, "false").toBoolean
    scan.setMaxVersions(maxVersions, isOldestFirst)

    table.getScanner(scan).iterator
  }
}

/** static member for empty cells.. **/
object MRecordReader {
  final val EMPTY_CELLS = List[Cell]()
}
