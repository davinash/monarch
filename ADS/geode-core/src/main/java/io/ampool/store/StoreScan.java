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
package io.ampool.store;

import java.util.List;

import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.ftable.internal.BlockKey;

/**
 * Used to perform Scan operations on Store
 */
public class StoreScan extends Scan {
  private BlockKey blockKey;
  private List<Pair<Long, Long>> ranges;

  public StoreScan(Scan scan) {
    super(scan);
  }

  public StoreScan(Scan scan, BlockKey blockKey) {
    super(scan);
    this.blockKey = blockKey;
  }

  public StoreScan() {

  }

  public BlockKey getBlockKey() {
    return blockKey;
  }

  public void setBlockKey(BlockKey blockKey) {
    this.blockKey = blockKey;
  }

  /**
   * Set the time partition ranges to be utilized for optimal querying based on insertion-time.
   *
   * @param ranges the time partition ranges
   */
  public void setTimePartitionRanges(final List<Pair<Long, Long>> ranges) {
    this.ranges = ranges;
  }

  /**
   * Get the time partition ranges for the scan.
   *
   * @return the time partition ranges
   */
  public List<Pair<Long, Long>> getRanges() {
    return ranges;
  }
}
