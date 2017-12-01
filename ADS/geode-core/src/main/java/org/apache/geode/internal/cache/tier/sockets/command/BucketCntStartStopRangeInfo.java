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

package org.apache.geode.internal.cache.tier.sockets.command;

import io.ampool.monarch.table.Bytes;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BucketCntStartStopRangeInfo implements DataSerializable {

  private int bucketCount;
  private byte[] startRangeKey;
  private byte[] stopRangeKey;

  public BucketCntStartStopRangeInfo() {
    this.bucketCount = -1;
    this.startRangeKey = Bytes.EMPTY_BYTE_ARRAY;
    this.stopRangeKey = Bytes.EMPTY_BYTE_ARRAY;
  }

  public int getBucketCount() {
    return bucketCount;
  }

  public void setBucketCount(int bucketCount) {
    this.bucketCount = bucketCount;
  }

  public byte[] getStartRangeKey() {
    return startRangeKey;
  }

  public void setStartRangeKey(byte[] startRangeKey) {
    this.startRangeKey = startRangeKey;
  }

  public byte[] getStopRangeKey() {
    return stopRangeKey;
  }

  public void setStopRangeKey(byte[] stopRangeKey) {
    this.stopRangeKey = stopRangeKey;
  }


  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(this.bucketCount, out);
    DataSerializer.writeByteArray(this.startRangeKey, out);
    DataSerializer.writeByteArray(this.stopRangeKey, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.bucketCount = DataSerializer.readInteger(in);
    this.startRangeKey = DataSerializer.readByteArray(in);
    this.stopRangeKey = DataSerializer.readByteArray(in);
  }
}
