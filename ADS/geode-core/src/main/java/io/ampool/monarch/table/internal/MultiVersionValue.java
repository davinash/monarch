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

package io.ampool.monarch.table.internal;

import org.apache.geode.CopyHelper;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.BytesAndBitsForCompactor;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.RegionEntry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class MultiVersionValue implements AmpoolValue {
  private int valueSize;
  private byte[] rowHeader;
  private byte[][] records;

  public MultiVersionValue() {}

  public MultiVersionValue(byte[] headerBytes, int versions) {
    rowHeader = headerBytes;
    records = new byte[versions][];
  }

  synchronized public boolean setVersions(byte[][] record, int valueSize) {
    this.records = record;
    this.valueSize = valueSize;
    return true;
  }


  public byte[][] getVersions() {
    return records;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return AMPL_MTABLE_MULTIVERSION_VALUE;
  }

  @Override
  public void toData(final DataOutput out) throws IOException {
    DataSerializer.writeArrayOfByteArrays(records, out);
    DataSerializer.writeInteger(valueSize, out);
    DataSerializer.writeByteArray(rowHeader, out);
  }

  @Override
  public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
    records = DataSerializer.readArrayOfByteArrays(in);
    valueSize = DataSerializer.readInteger(in);
    rowHeader = DataSerializer.readByteArray(in);
  }

  public byte[] getBytes() {
    return EntryEventImpl.serialize(this);
  }

  static MultiVersionValue fromBytes(final byte[] bytes) {
    return (MultiVersionValue) EntryEventImpl.deserialize(bytes);
  }

  @Override
  public byte[] getSerializedValue() {
    return getBytes();
  }

  @Override
  public Object getDeserializedForReading() {
    return this;
  }

  @Override
  public String getStringForm() {
    return "MultiVersionValue";
  }

  @Override
  public Object getDeserializedWritableCopy(final Region r, final RegionEntry re) {
    return CopyHelper.deepCopy(this);
  }

  @Override
  public Object getDeserializedValue(final Region r, final RegionEntry re) {
    return this;
  }

  @Override
  public Object getValue() {
    return this;
  }

  @Override
  public void writeValueAsByteArray(final DataOutput out) throws IOException {
    DataSerializer.writeByteArray(getBytes(), out);
  }

  @Override
  public void fillSerializedValue(final BytesAndBitsForCompactor wrapper, final byte userBits) {
    EntryEventImpl.fillSerializedValue(wrapper, this, userBits);
  }

  @Override
  public int getValueSizeInBytes() {
    return valueSize;
  }

  @Override
  public boolean isSerialized() {
    return true;
  }

  @Override
  public boolean usesHeapForStorage() {
    return true;
  }

  @Override
  public int getSizeInBytes() {
    return valueSize;
  }

  public byte[] getVersionByTimeStamp(long timestamp) {
    if (records != null) {
      for (int i = 0; i < records.length; i++) {
        long readTimeStamp = MTableStorageFormatter.readTimeStamp(records[i]);
        if (readTimeStamp == timestamp) {
          return records[i];
        }
      }
    }
    return null;
  }

  public byte[][] getVersionsByOrder(int maxVersionsToRead) {
    /** return null if current value is null **/
    if (records == null || maxVersionsToRead == 0) {
      return null;
    }

    int numOfVersions;
    int dataReadPosition = 0;
    boolean isLatestFirst = false;

    if (maxVersionsToRead < 0) {
      isLatestFirst = true;
    }
    maxVersionsToRead = Math.abs(maxVersionsToRead);
    byte[][] retArr =
        new byte[maxVersionsToRead > records.length ? records.length : maxVersionsToRead][];
    int index = 0;
    if (isLatestFirst) {
      // iterate in reverse way
      for (int i = records.length; i > 0 && index < maxVersionsToRead; i--) {
        retArr[index++] = records[i - 1];
      }
    } else {
      for (int i = 0; i < records.length && index < maxVersionsToRead; i++) {
        retArr[index++] = records[i];
      }
    }
    return retArr;
  }

  public byte[] getLatestVersion() {
    byte[][] versionsByOrder = getVersionsByOrder(-1);
    if (versionsByOrder != null) {
      return versionsByOrder[0];
    }
    return null;
  }

  public long getLatestVersionTimeStamp() {
    byte[][] versionsByOrder = getVersionsByOrder(-1);
    if (versionsByOrder != null) {
      return MTableStorageFormatter.readTimeStamp(versionsByOrder[0]);
    }
    return 0;
  }
}
