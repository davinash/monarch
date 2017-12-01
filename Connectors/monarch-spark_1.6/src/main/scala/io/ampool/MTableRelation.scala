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

import io.ampool.monarch.table.filter.{SingleColumnValueFilter}
import io.ampool.monarch.types.CompareOp
import io.ampool.monarch.types.interfaces.{DataType, TypePredicateOp}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConversions._

/**
  * Created on: 2016-02-26
  * Since version: 0.3.2.0
  */
class MTableRelation(@transient val sqlContext: SQLContext,
                     parameters: Map[String, String],
                     val schema: StructType,
                     @transient val data: DataFrame)
  extends BaseRelation with InsertableRelation with PrunedFilteredScan {

  private val conn: MTableWrapper = new MTableWrapper(null, parameters)
  val tableName: String = conn.tableName
  private val logger = Logger.getLogger(classOf[MTableRelation])

  override def toString: String = {
    this.getClass.getSimpleName + ": [table= " + tableName + "]"
  }

  /**
    * Return whether or not the table exists.
    *
    * @return true if table already exists; false otherwise
    */
  def tableExists: Boolean = conn.tableExists

  def getTableSchema(tableName: String): StructType = {
    val td = conn.getCache.getTable(tableName).getTableDescriptor
    logger.debug(s"Retrieving schema: table= $tableName; schema= ${td.getAllColumnDescriptors.mkString(",")}")
    Utils.toSparkSchema(td)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.saveToAmpool(tableName, parameters, overwrite)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    /** collect the required details from configuration/input and then create the required RDD **/
    val mySchema = if (schema == null) getTableSchema(tableName) else schema
    logger.debug(s"Using Spark schema= $mySchema")
    logger.debug(s"Required columns= ${requiredColumns.mkString(", ")}")
    val numPartitions = parameters.getOrElse("partitions", "4").toInt
    val requiredSchema = pruneSchema(mySchema, requiredColumns)
    logger.debug(s"Pruned Spark schema= $requiredSchema")
    val columnIdMap = conn.getTable.getTableDescriptor.getColumnDescriptorsMap
      .map(e => (e._1.getColumnNameAsString, (e._2, e._1.getColumnType))).toMap
    val predicates =
    if ("true".equals(conn.getOption("push.filters", "true"))) {
      try {
        filters.map(convertFilter(_, columnIdMap))
      } catch {
        case _: Throwable => new Array[io.ampool.monarch.table.filter.Filter](0)
      }
    }
    else new Array[io.ampool.monarch.table.filter.Filter](0)
    logger.debug(s"Pushed filters= ${predicates.mkString(",")}")
    val expectedColumnIds = requiredSchema.map(e=>columnIdMap.get(e.name).get._1).toList
    val predicateColumnIds = predicates.map(e=>columnIdMap.get(e.asInstanceOf[SingleColumnValueFilter].getColumnNameString).get._1).toList
    val actualColumnIds: List[Integer] = (expectedColumnIds ++ predicateColumnIds).distinct.sorted
    val outputColumnIds: List[Int] = expectedColumnIds.map(e=>actualColumnIds.indexOf(e))

    /** create the required RDD **/
    logger.debug(s"Creating MResultRDD for table= ${conn.getTableName}, partitions= $numPartitions, " +
      s"actualColumnIds= $actualColumnIds, outputColumnIds= $outputColumnIds")
    new MResultRDD(sqlContext, conn, numPartitions, predicates, actualColumnIds, outputColumnIds)
  }

  def pruneSchema(schema: StructType, requiredColumns: Array[String]): StructType = {
    val schemaMap = schema.map(e=>(e.name, e)).toMap
    val newSchema: List[StructField] = requiredColumns.map(e=> schemaMap.get(e).get).toList
    DataTypes.createStructType(newSchema)
  }

  /**
    * Helper method to create new Ampool predicate.
    *
    * @param index the index of column used in predicate
    * @param objectType the type of the column used in predicate
    * @param op the predicate operation
    * @param value the value to be compared against respective column-value for each row
    * @return the Ampool predicate
    */
  def newPredicate(colName: String,
                   op: TypePredicateOp, value: Any): io.ampool.monarch.table.filter.Filter = {
    new SingleColumnValueFilter(colName, op.asInstanceOf[CompareOp], value)
  }

  /**
    * Convert Spark Filters to respective Ampool predicates. Only filters
    * that have equivalent predicate supported are only translated to. Others
    * may still be executed within Spark query engine. Applying the filters/
    * predicates in Ampool would filter out unwanted data at source improving
    * the overall throughput.
    *
    * @param filter the Spark filter
    * @return the Ampool predicate for the respective Spark Filter
    */
  def convertFilter(filter: Filter, columnIdMap: Map[String,(Integer, DataType)]):io.ampool.monarch.table.filter.Filter = {
    filter match {
      case _: EqualTo =>
        val eqFilter = filter.asInstanceOf[EqualTo]
        val tuple = columnIdMap.get(eqFilter.attribute).get
        newPredicate(eqFilter.attribute, CompareOp.EQUAL, eqFilter.value)

      case _: LessThan =>
        val ltFilter = filter.asInstanceOf[LessThan]
        val tuple = columnIdMap.get(ltFilter.attribute).get
        newPredicate(ltFilter.attribute, CompareOp.LESS, ltFilter.value)

      case _: LessThanOrEqual =>
        val leFilter = filter.asInstanceOf[LessThanOrEqual]
        val tuple = columnIdMap.get(leFilter.attribute).get
        newPredicate(leFilter.attribute, CompareOp.LESS_OR_EQUAL, leFilter.value)

      case _: GreaterThan =>
        val gtFilter = filter.asInstanceOf[GreaterThan]
        val tuple = columnIdMap.get(gtFilter.attribute).get
        newPredicate(gtFilter.attribute, CompareOp.GREATER, gtFilter.value)

      case _: GreaterThanOrEqual =>
        val geFilter = filter.asInstanceOf[GreaterThanOrEqual]
        val tuple = columnIdMap.get(geFilter.attribute).get
        newPredicate(geFilter.attribute, CompareOp.GREATER_OR_EQUAL, geFilter.value)

      case _: Not =>
        val p = convertFilter(filter.asInstanceOf[Not].child, columnIdMap).asInstanceOf[SingleColumnValueFilter]
        val newOp = p.getOperator.toString match {
          case "EQ" => CompareOp.NOT_EQUAL
          case "LT" => CompareOp.GREATER
          case "GT" => CompareOp.LESS
          case "LE" => CompareOp.GREATER
          case "GE" => CompareOp.LESS
        }
        newPredicate(p.getColumnNameString, newOp, p.getValue)
    }
  }
}
