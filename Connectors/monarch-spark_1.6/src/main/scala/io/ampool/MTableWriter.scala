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

import io.ampool.monarch.table.ftable.{FTable, Record}
import io.ampool.monarch.table.internal.ByteArrayKey
import io.ampool.monarch.table.{Get, Put, MTable}
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._

/**
  * The class to save data-frame as MTable in Ampool/Geode.
  *
  * Created on: 2016-02-29
  * Since version: 0.3.2.0
  */
class MTableWriter(val schema: StructType, val table: MTableWrapper,
                   val overwrite: Boolean = false) extends Serializable {
  /**
    * API to actually save the DataFrame as table using MTable APIs.
    *
    * @param dataFrame the data frame to be saved to MTable
    * @param overwrite whether or not to overwrite the existing entries in the table
    */
  def write(dataFrame: DataFrame, overwrite: Boolean = false): Unit = {
    /** may be we do not need this.. add API to add columns with index **/
    val columns: Array[String] = schema.map(_.name).toArray

    /** delete the table if already exists and overwrite is true **/
    if (table.tableExists && overwrite) table.deleteTable()

    /** create the table, if not yet created, avoiding race-condition.
      * cannot pass it to below block since it is not Serializable..
      */
    table.createAndGet(schema, overwrite)

    if(table.isFtable) {
      append(dataFrame, overwrite, columns)
    } else {
      putAll(dataFrame, overwrite, columns)
    }
  }

  /**
    * Put the data from the respective data-frame to Ampool MTable via putAll API.
    *
    * @param dataFrame the data frame to be saved to MTable
    * @param overwrite whether or not to overwrite the existing entries in the table
    * @param columns an array of column names
    */
  def putAll(dataFrame: DataFrame, overwrite: Boolean, columns: Array[String]): Unit = {
    /** the prefix to be used for batch **/
    val rowKeyPrefix = table.getOption(Constants.AmpoolBatchPrefix, "").toString

    /** get columns to be used as row-key, if specified **/
    val rowKeyColumns = table.getOption(Constants.AmpoolKeyColumns, null) match {
      case v: String => v.split(",").map(x => new ByteArrayKey(x.getBytes("UTF-8")))
      case _ => Array[ByteArrayKey]()
    }
    val rowKeySeparator = table.getOption(Constants.AmpoolKeySeparator, ":").toString

    dataFrame.foreachPartition { rdd =>
      val mTable = table.createAndGet(schema, overwrite).asInstanceOf[MTable]
      val pid = TaskContext.getPartitionId()
      val batchSize = table.getBatchSize
      /** record the time spent during put operations.. **/
      val stats = new MStats(pid, "Put_")

      /** pre-allocate and reuse the Put objects for each batch.. **/
      val putList = List.fill(batchSize) { new Put("dummy") }
      val metaTable = table.getMetaTable
      var recCount: Long = 0
      if (metaTable != null) {
        val rc = metaTable.get(new Get(Constants.MetaKeyPrefix + pid))
        recCount = if (rc == null || rc.isEmpty) 0 else rc.getCells.get(0).getColumnValue.asInstanceOf[Long]
      }

      /** put all rows in chunks of size batchSize **/
      rdd.zipWithIndex.grouped(batchSize).foreach { chunk =>
        val puts = chunk.map { pair =>
          val put = putList(pair._2 % batchSize)
          pair._1.toSeq.zipWithIndex.foreach(x => put.addColumn(columns(x._2), x._1))
          setRowKey(rowKeyPrefix, rowKeyColumns, rowKeySeparator, pid, recCount, put)
          recCount += 1
          put
        }.toList
        stats.resetBegTime()
        mTable.put(puts)
        stats.computeStats(puts.map(_.getSerializedSize).sum)
      }

      /** update the count **/
      if (table.isMetaTableRequired && metaTable != null) {
        val countPut = new Put(Constants.MetaKeyPrefix + pid)
        countPut.addColumn(Constants.MetaColumn, recCount)
        metaTable.put(countPut)
      }
      stats.printTime()
    }
  }

  /**
    * Setup the row-key for each put.
    *
    * @param rowKeyPrefix the key prefix
    * @param rowKeyColumns the columns to be used as key
    * @param rowKeySeparator key separator
    * @param pid the partition id
    * @param recCount count
    * @param put actual put object
    */
  def setRowKey(rowKeyPrefix: String, rowKeyColumns: Array[ByteArrayKey], rowKeySeparator: String, pid: Int, recCount: Long, put: Put): Unit = {
    val rowKey = if (rowKeyColumns.isEmpty) rowKeyPrefix + pid + "-" + recCount
    else rowKeyColumns.map(put.getColumnValueMap.get(_)).mkString(rowKeySeparator)
    put.setRowKey(rowKey.getBytes("UTF-8"))
  }

  /**
    * Append the data from the respective data-frame to Ampool FTable via append API.
    *
    * @param dataFrame the data frame to be saved to MTable
    * @param overwrite whether or not to overwrite the existing entries in the table
    * @param columns an array of column names
    */
  def append(dataFrame: DataFrame, overwrite: Boolean, columns: Array[String]): Unit = {
    dataFrame.foreachPartition { rdd =>
      val fTable = table.createAndGet(schema, overwrite).asInstanceOf[FTable]
      val pid = TaskContext.getPartitionId()
      val batchSize = table.getBatchSize
      //        logger.info(s"PartitionId= $pid")


      /** record the time spent during put operations.. **/
      val stats = new MStats(pid, "Append_")

      /** pre-allocate and reuse the Put objects for each batch.. **/
      val putList = List.fill(batchSize) {
        new Record()
      }

      val metaTable = table.getMetaTable
      var recCount: Long = 0
      if (metaTable != null) {
        val rc = metaTable.get(new Get(Constants.MetaKeyPrefix + pid))
        recCount = if (rc == null || rc.isEmpty) 0 else rc.getCells.get(0).getColumnValue.asInstanceOf[Long]
      }

      /** append all rows in chunks of size batchSize **/
      rdd.zipWithIndex.grouped(batchSize).foreach { chunk =>
        val puts = chunk.map { pair =>
          val put = putList(pair._2 % batchSize)
          pair._1.toSeq.zipWithIndex.foreach(x => put.add(columns(x._2), x._1))
          recCount += 1
          put
        }.toList

        stats.resetBegTime()
        fTable.append(puts.toArray: _*)
      }
      stats.printTime()
    }
  }
}
