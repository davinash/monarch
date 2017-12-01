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

import io.ampool.monarch.table.Scan
import io.ampool.monarch.table.ftable.FTableDescriptor
import io.ampool.monarch.types.TypeHelper
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._
import scala.runtime.ScalaRunTime.stringOf

/**
  * Created on: 2017-09-27
  * Since version: 1.5.0
  */
object TestHelper {
  def dataFrameToStringComplexTypes(dataFrame: DataFrame): Array[String] = {
    dataFrame.drop(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME).collect()
      .map(e =>  TypeHelper.deepToString(DataConverter.convertWrite(e.schema, e))).sorted
  }
  def mTableToStringComplexTypes(tableName: String, params: Map[String,String]): Array[String] = {
    val cache = getAmpoolCache(params)
    val scan = new Scan()
    val buckets = new java.util.HashSet[Integer]
    (0 to 112).foreach(e=>buckets.add(e.asInstanceOf[java.lang.Integer]))
    scan.setBucketIds(buckets)
    val iterator =
      if (cache.getAdmin.existsFTable(tableName)) cache.getFTable(tableName).getScanner(scan).iterator()
      else cache.getMTable(tableName).getScanner(scan).iterator()
    val scanner = iterator
    val builder = Array.newBuilder[String]

    while (scanner.hasNext) {
      val nxt = scanner.next
      builder += ("[" + nxt.getCells.dropRight(1)
        .map(e => TypeHelper.deepToString(e.getColumnValue)).mkString(", ") + "]")
    }
    builder.result.sorted
  }
}
