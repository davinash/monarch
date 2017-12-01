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

package io.ampool.monarch.table.region;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import org.apache.geode.cache.PartitionResolver;

/**
 * Helper interface which extends PartitionResolver. Since Apache Geode Client side code call
 * default constructor only We have no way to pass the numberOfbuckets, which is numberOfSplits for
 * our Range-Partitioning Scheme.
 * 
 * @param <K>
 * @param <V>
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public interface RowTuplePartitionResolver<K, V> extends PartitionResolver<K, V> {
  /**
   * Sets the total number of buckets.
   * 
   * @param numberOfBuckets
   */
  void setTotalNumberOfBuckets(int numberOfBuckets);

  /**
   * Set the start and stop range for the region
   *
   * @param startRangeKey
   * @param stopRangeKey
   */
  void setStartStopRangeKey(byte[] startRangeKey, byte[] stopRangeKey);
}
