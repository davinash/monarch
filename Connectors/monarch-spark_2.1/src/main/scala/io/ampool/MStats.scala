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

import scala.annotation.elidable

/**
  * A class that holds statistics like number of operations, total time taken
  * for the execution, total time taken by internal execution.
  *
  * The methods are defined at INFO level so these become available only
  * when compilation level set to INFO (via -Xelide-below). Otherwise,
  * these methods will be ignored by the compiler itself.
  *
  * When compiling with maven this can be enabled by specifying the
  * value property 'spark.ampool.elide' as 'INFO' or higher. The generated
  * byte-code will have the instrumented code to display statistics
  * at the end-of-execution. Following is an example to turn this on:
  * ``
  * mvn clean install -Dspark.ampool.elide=INFO
  * ``
  *
  * Created on: 2016-04-20
  * Since version: 0.3.2.0
  */
class MStats(val id: Int, val opType: String) {
  val startTime = System.nanoTime
  var totalOps: Long = 0
  var opBegTime = startTime
  var opTotalTime: Long = 0
  var opTotalBytes: Long = 0
  val formatter = java.text.NumberFormat.getNumberInstance

  /**
    * Reset the beginning time used to measure internal execution time.
    */
  @elidable(elidable.INFO)
  def resetBegTime() : Unit = opBegTime = System.nanoTime

  /**
    * Compute stats time taken for internal operation (like internal put/next), also
    * increment number of operations and number of bytes.
    *
    * @param bytes total number of bytes transferred
    */
  @elidable(elidable.INFO)
  def computeStats(bytes: Long = 0): Unit = {
    opTotalTime += (System.nanoTime - opBegTime)
    opTotalBytes += bytes
    totalOps += 1
  }

  /**
    * Print the statistics summary..
    */
  @elidable(elidable.INFO)
  def printTime(): Unit = {
    println(f"# TimeIn${opType}_$id%d::: " +
      f"OpsCount= $totalOps, " +
      f"TotalTime= ${((System.nanoTime - startTime) / 1000000) / 1000.0}%.3f, " +
      f"InternalTime= ${(opTotalTime / 1000000) / 1000.0}%.3f, " +
      f"TotalBytes= ${formatter.format(opTotalBytes / 1024.0)} KB")
  }
}
