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
package org.apache.geode.internal.cache;

import java.io.Serializable;
import java.util.Arrays;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/*
 * Hold status bucket execution
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MBucketScanResult implements Serializable {

  private static final long serialVersionUID = -6900482249846649906L;
  private int buckedId;
  private boolean bucketScanCompleted;
  private byte[] lastScanKey;
  private int totalResults;

  public MBucketScanResult(int bucketId, byte[] lastScanKey, boolean bucketCompleted,
      int resultsCount) {
    this.buckedId = bucketId;
    this.lastScanKey = lastScanKey;
    this.bucketScanCompleted = bucketCompleted;
    this.totalResults = resultsCount;
  }

  public int getBuckedId() {
    return buckedId;
  }

  public boolean isBucketScanCompleted() {
    return bucketScanCompleted;
  }

  public byte[] getLastScanKey() {
    return lastScanKey;
  }

  public int getTotalResults() {
    return totalResults;
  }

  @Override
  public String toString() {
    return "MBucketScanResult{" + "buckedId=" + buckedId + ", bucketScanCompleted="
        + bucketScanCompleted + ", lastScanKey=" + Arrays.toString(lastScanKey) + ", totalResults="
        + totalResults + '}';
  }
}
