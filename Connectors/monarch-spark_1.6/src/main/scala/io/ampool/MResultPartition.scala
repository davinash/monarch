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

import org.apache.geode.distributed.internal.ServerLocation
import io.ampool.monarch.table.filter.Filter
import org.apache.spark.Partition

/**
  * The class for handling partitions.
  *
  * Created on: 2016-02-28
  * Since version: 0.2.0
  */
case class PartitionRange[T](minKey: Option[T], maxKey: Option[T]) extends Serializable

object PartitionRange {
  def DUMMY: PartitionRange[Array[Byte]] = new PartitionRange[Array[Byte]](null, null)
}

case class MResultPartition(index: Int, bucketIds: java.util.Set[Integer],
                            bucketToServerMap: java.util.Map[Integer, java.util.Set[ServerLocation]],
                            hosts: Seq[String], predicates: Array[Filter],
                            range: PartitionRange[Array[Byte]] = PartitionRange.DUMMY)
  extends Partition with Serializable {
  override def toString: String = {
    "[Index= " + index + ", BucketIds= " + bucketIds + ", Servers= " + hosts.mkString(",") + "]"
  }
}