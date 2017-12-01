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

import io.ampool.monarch.table._
import io.ampool.monarch.table.client.MClientCache
import io.ampool.monarch.table.filter.FilterList
import io.ampool.monarch.table.internal._
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created on: 2016-02-29
  * Since version: 0.2.0
  */
class MTableWrapper(var tableName: String = null,
                    val parameters: Map[String, String]) extends Serializable {
  //  val tableName: String = parameters.getOrElse("path", null)
  if (tableName == null) tableName = parameters.getOrElse("path", null)

  def getCache: MClientCache = Utils.getClientCache(parameters)

  def getBatchSize: Int = parameters.getOrElse(Constants.AmpoolBatchSize, "1000").toInt

  def getTableName: String = tableName

  def getTable: Table = Utils.getInternalTable(getTableName, getCache);

  def getOption(option: String, defaultValue: String = null): AnyRef =
    parameters.getOrElse(option, defaultValue)

  def getScanner(columns: List[Integer], split: MResultPartition): Iterator[Row] = {
    val mTable: InternalTable = getTable.asInstanceOf[InternalTable]
    if (mTable == null) {
      return Iterator.empty
    }
    Logger.getLogger(classOf[MTableWriter]).info(s"PartitionScanner: Table= $tableName, Id= ${split.toString}")
    val scan: Scan = new Scan
    scan.setBatchSize(parameters.getOrElse(Constants.AmpoolBatchSize, "50000").toInt)
    scan.setReturnKeysFlag(false)
    /*scan.setPredicates(split.predicates)*/
    scan.setFilter(new FilterList(split.predicates))
    scan.setBucketToServerMap(split.bucketToServerMap)
    scan.setBucketIds(split.bucketIds)
    /** add selected columns, if any **/
    scan.setColumns(columns)

    // Setting scan parameters for multi versioned scan.
    val str = parameters.getOrElse(Constants.AmpoolReadFilterOnLatestVersion, "false")
    scan.setFilterOnLatestVersionOnly((str.toBoolean))
    val maxVersions = parameters.getOrElse(Constants.AmpoolMaxVersions, Constants.AmpoolDefaultMaxVersions).toInt
    val isOldestFirst = parameters.getOrElse(Constants.AmpoolReadOldestFirst, "false").toBoolean
    scan.setMaxVersions(maxVersions, isOldestFirst)

    mTable.getScanner(scan).iterator
  }

  /**
    * Get the MTable reference. If the table is not created yet,
    * create and return the newly created reference.
    * In case overwrite is set to true, delete the table and create
    * new so that old data is removed.
    *
    * @param schema the table schema
    * @param overwrite true if old data is to be removed before write; false otherwise
    * @return the reference to the table (MTable)
    */
  def createAndGet(schema: StructType, overwrite: Boolean): InternalTable = {
    val tableExists: Boolean = Utils.tableExists(tableName, getCache)
    if (tableExists) {
      Utils.getInternalTable(getTableName, getCache)
    } else {
      //get the tableType, Default tabletype is FTable, i.e immutable.
      val tableType = parameters.getOrElse(Constants.AmpoolTableType, "immutable")
      if("immutable".equalsIgnoreCase(tableType)){
        //create FTable
        /** create the actual table containing data **/
        val td = Utils.toAmpoolFTableSchema(schema, parameters)
        val redundancy = parameters.getOrElse(Constants.AmpoolRedundancy, "0").toInt
        td.setRedundantCopies(redundancy)
        val pc = parameters.getOrElse(Constants.AmpoolPartitioningColumn, null)
        if(pc != null){
          td.setPartitioningColumn(Bytes.toBytes(pc.toString))
        }
        //getCache.getAdmin.createFTable(tableName, td).asInstanceOf[InternalTable]
        Utils.createTable(tableName, td, getCache, true)
      }else {
        createAndGetMTable(schema, tableType)
      }
    }
  }

  /**
    * Create and get the MTable from the provided configuration.
    *
    * @param schema the table schema
    * @param tableType the table type (Ordered or Unordered)
    * @return the instance of MTable
    */
  def createAndGetMTable(schema: StructType, tableType: String): InternalTable = {
    //create MTable
    /** create a meta-table with single column to hold the count **/
    try {
      if (isMetaTableRequired) {
        val st = new StructType(Array(new StructField(Constants.MetaColumn, DataTypes.LongType)))
        val td = Utils.toAmpoolMTableSchema(st,MTableType.UNORDERED)
        td.setUserTable(true)
        td.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)

        getCache.getAdmin.createTable(tableName + Constants.MetaTableSuffix, td)
      }
    } catch {
      case e: Throwable => Logger.getLogger(classOf[MTableWriter])
        .error(s"Error creating meta-table for: $tableName", e)
    }

    /** create the actual table containing data **/
    val td = Utils.toAmpoolMTableSchema(schema, MTableType.valueOf(tableType))
    val redundancy = parameters.getOrElse(Constants.AmpoolRedundancy, "0").toInt
    td.setRedundantCopies(redundancy)

    // setting max-versions for mtable
    val maxVersions = parameters.getOrElse(Constants.AmpoolMaxVersions, Constants.AmpoolDefaultMaxVersions).toInt
    td.setMaxVersions(maxVersions)

    if (tableType != null && !tableType.equalsIgnoreCase(MTableType.UNORDERED.name())) {
      setupOrderedTable(tableType, td)
    }
    parameters.getOrElse(Constants.AmpoolEnablePersistence, null) match {
      case "sync" => td.enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
      case "async" => td.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
      case _ =>
    }
    Utils.createTable(tableName, td, getCache, false)
    //getCache.getAdmin.createTable(tableName, td).asInstanceOf[InternalTable]
  }

  /**
    * Setup the ordered table from the provided configuration. It includes the key
    * ranges, if specified.
    *
    * @param tableType the table type
    * @param td the instance of table descriptor with appropriate configuration
    * @return the table descriptor
    */
  def setupOrderedTable(tableType: String, td: MTableDescriptor): Any = {
    /** set start-stop key range if specified **/
    val keyRange = getOption("ampool.key.range", null)
    if (keyRange != null) {
      try {
        val keys = keyRange.toString.split(",")
        if (keys.length > 1) {
          getOption("ampool.key.range.type", null) match {
            case "int" =>
              td.setStartStopRangeKey(keys(0).toInt, keys(1).toInt)
            case "long" =>
              td.setStartStopRangeKey(keys(0).toLong, keys(1).toLong)
            case _ =>
              if (keys(0).length > 0 && keys(1).length > 0)
                td.setStartStopRangeKey(keys(0).getBytes("UTF-8"), keys(1).getBytes("UTF-8"))
          }
        }
      } catch {
        case e: Throwable => Logger.getLogger(classOf[MTableWriter])
          .error(s"Using default key-range. Failed to set key range from: $keyRange", e)
      }
    }


  }

  def tableExists: Boolean = Utils.tableExists(tableName, getCache)

  /**
    * Return if the meta-table is required. In case user has specified the `batch-prefix`
    * or one of the columns as row-key then we do not need to maintain the counter of
    * the id as the uniqueness of the key is explicitly taken care of by the user.
    * Else we have to make sure that the unique row-keys are assigned and that is done
    * by maintaining the counter.
    * NOTE: This is not going to work if save is done in parallel as the atomicity is
    * not guaranteed -- causing data loss.
    *
    * @return true if the meta-table is required; false otherwise
    */
  def isMetaTableRequired: Boolean = {
    "true".equals(getOption(Constants.AmpoolTableAppend, "false"))
    /** for now let user explicitly say append is needed **/
//    if ((getOption(Constants.AmpoolBatchPrefix, null) == null)
//    && (getOption(Constants.AmpoolKeyColumns, null) == null)) true
//    else false
  }

  def isFtable: Boolean = {
    "immutable".equals(getOption(Constants.AmpoolTableType, "immutable"))
  }

  /**
    * Get the meta-table maintaining the counters.
    *
    * @return the table that maintains the counter information of the last data written
    */
  def getMetaTable: MTable = getCache.getTable(tableName + Constants.MetaTableSuffix)

  def deleteTable(): Unit = {
    Utils.deleteTable(tableName, getCache)
    try {
      if (isMetaTableRequired) {
        Utils.deleteTable(tableName + Constants.MetaTableSuffix, getCache)
      }
    } catch {
      case _: Throwable => Logger.getLogger(classOf[MTableWriter])
        .error(s"Error deleting table: $tableName${Constants.MetaTableSuffix}")
    }
  }
}