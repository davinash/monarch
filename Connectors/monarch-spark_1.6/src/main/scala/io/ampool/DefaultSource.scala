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

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * The data-source API's to be exposed for end-users to save and load data
  * from Ampool MTable from/to DataFrame.
  *
  * Created on: 2016-02-26
  * Since version: 0.3.2.0
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {
  /**
    * Create the relation to interpret MTable as DataFrame when no schema was provided.
    * When the schema was not provided, we should load schema of the respective MTable
    * and load the DataFrame with that schema.
    *
    * @param sqlContext the SQLContext
    * @param parameters the configuration parameters, if any
    * @return the relation interpreting DataFrame
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]):
  BaseRelation = {
    createRelation(sqlContext, parameters, getSchema(sqlContext, parameters))
  }
  def getSchema(sqlContext: SQLContext, parameters: Map[String,String]): StructType = {
    val conn: MTableWrapper = new MTableWrapper(null, parameters)
    val table = conn.getTable
    if (table == null) throw new IllegalArgumentException("Ampool table does not exist: " + conn.getTableName)
    Utils.toSparkSchema(table.getTableDescriptor)
  }

  /**
    * Create the relation to interpret MTable as DataFrame when user has provided the schema
    * explicitly.
    *
    * @param sqlContext the SQLContext
    * @param parameters the configuration parameters, if any
    * @param schema the schema to be used to load DataFrame
    * @return the relation interpreting DataFrame
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    new MTableRelation(sqlContext, parameters, schema, null)
  }

  /**
    * Create the relation for writing DataFrame to MTable.
    *
    * @param sqlContext the SQLCOntext
    * @param mode the save-mode
    * @param parameters the configuration parameters, if any
    * @param data the data-frame to be saved to MTable
    * @return the relation translating schema and data from DataFrame to MTable
    */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val relation: MTableRelation = new MTableRelation(sqlContext, parameters, null, data)
    /** TODO: implement the following mode correctly.. **/
    mode match {
      case SaveMode.Append => relation.insert(data, overwrite = false)
      case SaveMode.Overwrite => relation.insert(data, overwrite = true)
      case SaveMode.ErrorIfExists =>
        if (relation.tableExists) throw new Exception("Table already exists: table= " + relation.tableName)
        else relation.insert(data, overwrite = false)
      case SaveMode.Ignore =>
        if (!relation.tableExists) relation.insert(data, overwrite = false)
    }
    relation
  }
}
