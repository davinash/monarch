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

package io.ampool.monarch.table.ftable.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.internal.IMKey;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;



/**
 * Block key is the key maintained for each block of FTable Each block consists mulitple records of
 * FTable store in single RegionEntry
 */
public class BlockKey implements IMKey, DataSerializableFixedID, Serializable {
  static public final int length = (Bytes.SIZEOF_LONG * 3) + Bytes.SIZEOF_INT;
  private long startTimeStamp = -1;
  private long endTimeStamp = -1;
  private long blockSequenceID = -1;
  private int bucketID = -1;

  public int getBucketID() {
    return bucketID;
  }

  public void setBucketID(final int bucketID) {
    this.bucketID = bucketID;
  }



  public BlockKey(final long startTimeStamp) {
    this.startTimeStamp = startTimeStamp;
  }

  public BlockKey(final long startTimeStamp, final long blockSequenceID, final int bucketID) {
    this.startTimeStamp = startTimeStamp;
    this.blockSequenceID = blockSequenceID;
    this.bucketID = bucketID;
  }


  public BlockKey() {

  }

  public BlockKey(byte[] bytes) {
    int offset = 0;

    startTimeStamp = Bytes.toLong(bytes, offset);
    offset += Bytes.SIZEOF_LONG;

    endTimeStamp = Bytes.toLong(bytes, offset);
    offset += Bytes.SIZEOF_LONG;

    blockSequenceID = Bytes.toLong(bytes, offset);
    offset += Bytes.SIZEOF_LONG;

    bucketID = Bytes.toInt(bytes, offset);
  }

  @Override
  public byte[] getBytes() {
    byte[] bytes = new byte[Bytes.SIZEOF_LONG * 3 + Bytes.SIZEOF_INT];
    int offset = 0;
    byte[] startTimeStampBytes = Bytes.toBytes(startTimeStamp);
    byte[] endTimeStampBytes = Bytes.toBytes(endTimeStamp);
    byte[] blockSequenceIDBytes = Bytes.toBytes(blockSequenceID);
    byte[] bucketIDBytes = Bytes.toBytes(bucketID);
    offset = Bytes.putBytes(bytes, offset, startTimeStampBytes, 0, startTimeStampBytes.length);
    offset = Bytes.putBytes(bytes, offset, endTimeStampBytes, 0, endTimeStampBytes.length);
    offset = Bytes.putBytes(bytes, offset, blockSequenceIDBytes, 0, blockSequenceIDBytes.length);
    offset = Bytes.putBytes(bytes, offset, bucketIDBytes, 0, bucketIDBytes.length);
    return bytes;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return AMPL_FTABLE_BLOCKKEY;
  }

  @Override
  public void toData(final DataOutput out) throws IOException {
    out.writeLong(startTimeStamp);
    out.writeLong(endTimeStamp);
    out.writeLong(blockSequenceID);
    out.writeInt(bucketID);
  }

  @Override
  public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
    this.startTimeStamp = in.readLong();
    this.endTimeStamp = in.readLong();
    this.blockSequenceID = in.readLong();
    this.bucketID = in.readInt();
  }

  @Override
  public int compareTo(final IMKey o) {
    int result = Long.compare(this.startTimeStamp, ((BlockKey) o).getStartTimeStamp());
    if (result == 0) {
      return Long.compare(this.blockSequenceID, ((BlockKey) o).getBlockSequenceID());
    }
    return result;
  }

  @Override
  public int hashCode() {
    return bucketID;
  }

  /**
   * Equality check.. all objects of IMKey type and same byte-array are equal.
   *
   * @param other the other object
   * @return true if other object is of IMKey type and has same bytes; false otherwise
   */
  @Override
  public boolean equals(Object other) {
    return this == other || (other instanceof BlockKey
        && this.blockSequenceID == ((BlockKey) other).getBlockSequenceID()
        && this.bucketID == ((BlockKey) other).getBucketID());
  }

  @Override
  public String toString() {
    return "Blockey:" + bucketID + "_" + blockSequenceID + "_" + startTimeStamp + "_"
        + endTimeStamp;
  }

  public long getStartTimeStamp() {
    return startTimeStamp;
  }

  public void setStartTimeStamp(final long startTimeStamp) {
    this.startTimeStamp = startTimeStamp;
  }

  public long getEndTimeStamp() {
    return endTimeStamp;
  }

  public void setEndTimeStamp(final long endTimeStamp) {
    this.endTimeStamp = endTimeStamp;
  }

  public long getBlockSequenceID() {
    return blockSequenceID;
  }

  public void setBlockSequenceID(final long blockSequenceID) {
    this.blockSequenceID = blockSequenceID;
  }
}
