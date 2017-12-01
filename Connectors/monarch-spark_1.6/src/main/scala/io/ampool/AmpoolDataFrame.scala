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

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
 * The entry point for DataFrame API's into Ampool.
 *
 * Created on: 2016-02-26
 * Since version: 0.3.2.0
 */
class AmpoolDataFrame(@transient val dataFrame: DataFrame) extends Serializable {
  private val logger = Logger.getLogger(classOf[AmpoolDataFrame])

  /**
    * Get the parameters/options from spark-context configuration, if specified there.
    * The specified options must override the context configuration, if any.
    *
    * @param options the specified options
    * @param conf the spark configuration
    * @return the options to be consumed
    */
  def getOptions(options: Map[String, String], conf: SparkConf): Map[String, String] = {
    /** copy configuration, for following keys, from spark-context if present **/
    val keys = Constants.ContextDefaults

    val map = mutable.Map[String,String]()
    keys.foreach(key=> {
      val confValue = conf.get(key, null)
      val optValue = options.getOrElse(key, null)
      if (confValue != null && optValue == null) map.put(key, confValue)
      else if (optValue != null) map.put(key, optValue)
    })
    if (map.nonEmpty) (map ++ options.filterKeys(k => !keys.contains(k))).toMap else options
  }

  /**
    * An entry point to save DataFrame as Ampool MTable.
    *
    * @param tableName the table name
    * @param parameters the configuration parameters, if any
    * @param overwrite whether or not to overwrite existing data in the table
    */
  def saveToAmpool(tableName: String,
                   parameters: Map[String,String] = Map.empty,
                   overwrite: Boolean = false): Unit = {
    val params = getOptions(parameters, dataFrame.sqlContext.sparkContext.getConf)
    logger.info(s"SaveToAmpool :: table= $tableName, parameters= $params")
    val table: MTableWrapper = new MTableWrapper(tableName, params)
    val writer: MTableWriter = new MTableWriter(dataFrame.schema, table)
    writer.write(dataFrame, overwrite)
  }
}