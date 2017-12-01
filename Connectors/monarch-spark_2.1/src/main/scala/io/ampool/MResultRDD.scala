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

import java.util

import io.ampool.monarch.table.filter.Filter
import io.ampool.monarch.table.internal.MTableUtils
import org.apache.geode.distributed.internal.ServerLocation
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Partition, TaskContext}

import scala.collection.JavaConversions._

/**
  * Created on: 2016-02-28
  * Since version: 0.3.2.0
  */
class MResultRDD(@transient val sc: SQLContext,
                 tableName: String,
                 parameters: Map[String, String],
                 predicates: Array[Filter],
                 schema: StructType)
  extends RDD[Row](sc.sparkContext, deps = Nil) {

  var totalSizeInBytes: Long = 0

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    new MRecordReader(tableName, split.asInstanceOf[MResultPartition], parameters, schema).getIterator
  }

  /**
    * Return the required partitions; divide buckets into separate partitions to help
    * parallel execution of partitions better.
    * Also, the partition contains the respective predicates to be executed to filter
    * out unwanted data during the scan. The Spark filters are translated to respective
    * (supported) predicates and executed during the scan.
    *
    * @return an array of mutually exclusive partitions
    */
  override protected def getPartitions: Array[Partition] = {
    val table = Utils.getInternalTable(tableName, Utils.getClientCache(parameters))
    val bucketCount = table.getTableDescriptor.getTotalNumOfSplits
    val bucketMap = new util.HashMap[Integer, util.Set[ServerLocation]]()
    val numPartitions = parameters.getOrElse("partitions", "4").toInt
    MTableUtils.getSplits(table.getName, numPartitions, bucketCount, bucketMap)
      .zipWithIndex
      .map { case (split, i) =>
        totalSizeInBytes += split.getSize
        val bMap = bucketMap.filter(x => split.getBuckets.contains(x._1))
        MResultPartition(i, split.getBuckets, bMap, split.getSize, split.getServersArray, predicates)
      }.toArray
  }

  /**
    * Provide the preferred worker locations where the split should be executed. We already
    * have the buckets and respective server-locations so returning the server locations
    * would eventually help in achieving data-locality, if possible, (NODE_LOCAL) as the
    * respective worker would process the buckets on respective local servers.
    *
    * @param split the input split
    * @return an array of servers (host-name) where the buckets (primary) of this split reside
    */
  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[MResultPartition].hosts
  }

  /**
    * String representation of this RDD.
    *
    * @return the string representing this RDD
    */
  override def toString: String = {
    "[Table= " + tableName + ", Schema= " + schema + ", Predicates= " + predicates + "]"
  }
}