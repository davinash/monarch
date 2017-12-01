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
package io

import io.ampool.monarch.table.client.MClientCache
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._
import scala.language.implicitConversions


package object ampool {
  implicit def toDataFrameFunctions(dataFrame: DataFrame): AmpoolDataFrame = new AmpoolDataFrame(dataFrame)

  def getAmpoolCache(parameters: Map[String,String]): MClientCache = {
    Utils.getClientCache(parameters)
  }

  /**
    * Measure the time taken for the executed task/code.
    *
    * @param code code to be executed
    */
  def timeTask(code: => Unit) = {
    val t1 = System.nanoTime
    code
    println("Time= " + (System.nanoTime-t1)/1000000/1000.0)
  }
}
