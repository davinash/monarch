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

import org.apache.spark.Partition

/**
  * The class providing the required partitions.
  *
  * TODO: Implement correct partitioning strategy..
  *
  * Created on: 2016-02-28
  * Since version: 0.3.2.0
  */
class MResultPartitionProvider(parameters: Map[String,String]) extends Serializable {
  def getPartitions: Array[Partition] = {
    Array()
  }
}
