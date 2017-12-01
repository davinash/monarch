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

import java.io.Serializable;

import io.ampool.monarch.table.Pair;

public class BitMap implements IBitMap, Serializable {

  private int SET_VALUE = 1;

  private int capacity;
  private byte[] backedBytes = null;
  private boolean isOffsetBasedBytes = false;
  private int offset = 0;
  private int length;

  private static final int SET_BIT = 1;
  private static final int UNSET_BIT = 0;

  /**
   * Constructs new bit map of given capacity
   */
  public BitMap(int capacity) {
    // convert into number of bytes
    this.capacity = capacity;
    int bytesSize = toNumberOfBytes(capacity);
    this.backedBytes = new byte[bytesSize];
  }

  public BitMap(byte[] byteArray) {
    this.backedBytes = byteArray;
    this.capacity = this.backedBytes.length * Byte.SIZE;
    this.isOffsetBasedBytes = false;
  }

  /*
   * Impl is not correct till now.
   */
  public BitMap(byte[] byteArray, int offset, int length) {
    if (offset < 0 || length <= 0) {
      throw new RuntimeException(
          "Value of length is zero or less than zero. OR Value of offset is less than zero.");
    }

    if (byteArray.length < offset + length) {
      throw new RuntimeException("Offset + Length exceeding the length of byte array");
    }

    this.backedBytes = byteArray;
    this.offset = offset;
    this.length = length;
    this.isOffsetBasedBytes = true;
    this.capacity = length * Byte.SIZE;
  }

  public void reset(byte[] byteArray, int offset, int length) {
    if (offset < 0 || length <= 0) {
      throw new RuntimeException(
          "Value of length is zero or less than zero. OR Value of offset is less than zero.");
    }

    if (byteArray.length < offset + length) {
      throw new RuntimeException("Offset + Length exceeding the length of byte array");
    }

    this.backedBytes = byteArray;
    this.offset = offset;
    this.length = length;
    this.isOffsetBasedBytes = true;
    this.capacity = length * Byte.SIZE;
  }

  @Override
  public void set(int index) {
    int base = this.offset + (index / Byte.SIZE);
    byte off = (byte) (index % Byte.SIZE);
    backedBytes[base] |= (SET_BIT << off);
  }

  @Override
  public void set(int index, boolean value) {
    if (value) {
      set(index);
    } else {
      unSet(index);
    }
  }

  @Override
  public void unSet(int index) {
    int base = this.offset + (index / Byte.SIZE);
    byte off = (byte) (index % Byte.SIZE);
    backedBytes[base] &= ~(SET_BIT << off);
  }

  @Override
  public boolean get(int index) {
    int base = this.offset + (index / Byte.SIZE);
    byte off = (byte) (index % Byte.SIZE);
    // if index is out of bounds return false
    if (base >= backedBytes.length)
      return false;
    if (((backedBytes[base]) & (SET_BIT << off)) != UNSET_BIT) {
      return true;
    }
    return false;
  }

  @Override
  public void or(IBitMap bitMap) {
    if (this == bitMap) {
      return;
    }

    expandIfRequired(bitMap);

    // now compare bit by bit and set accordingly
    int totalBits = this.sizeOfByteArray() * Byte.SIZE;
    for (int i = 0; i < totalBits; i++) {
      if (this.get(i) || bitMap.get(i)) {
        this.set(i, true);
      } else {
        this.set(i, false);
      }
    }
  }


  @Override
  public void and(IBitMap bitMap) {
    if (this == bitMap) {
      return;
    }

    expandIfRequired(bitMap);

    // now compare bit by bit and set accordingly
    int totalBits = this.sizeOfByteArray() * Byte.SIZE;
    for (int i = 0; i < totalBits; i++) {
      if (this.get(i) && bitMap.get(i)) {
        this.set(i, true);
      } else {
        this.set(i, false);
      }
    }
  }

  @Override
  public void andNot(IBitMap bitMap) {
    if (this == bitMap) {
      return;
    }

    expandIfRequired(bitMap);

    // now compare bit by bit and set accordingly
    int totalBits = this.sizeOfByteArray() * Byte.SIZE;
    for (int i = 0; i < totalBits; i++) {
      if (this.get(i) && !bitMap.get(i)) {
        this.set(i, true);
      } else {
        this.set(i, false);
      }
    }
  }

  @Override
  public int sizeOfByteArray() {
    if (this.backedBytes != null) {
      if (isOffsetBasedBytes) {
        return this.length;
      }
      return this.backedBytes.length;
    }
    return -1;
  }

  @Override
  public byte[] toByteArray() {
    return backedBytes;
  }

  @Override
  public void clear() {
    for (int i = 0; i < sizeOfByteArray(); i++) {
      this.backedBytes[i] = (byte) UNSET_BIT;
    }
  }

  @Override
  public int capacity() {
    return this.capacity;
  }

  @Override
  public int cardinality() {
    int cardinality = 0;
    for (int i = 0; i < backedBytes.length; i++) {
      cardinality += countBitsInByte(backedBytes[i]);
    }
    return cardinality;

  }

  /**
   * Counts each set bit in a byte. This is the Kernighan bit counting method. It works by
   * subtracting one from the test number, and then ANDing it. This clears the last bit. Each time
   * we clear the last set bit, we increment a counter. When the number is zero, we stop. By testing
   * only the least significant set bit, this algorithm offers improvement over testing every bit.
   * 
   * @param test the byte to test
   * @return the number of set bits in the byte
   */
  public int countBitsInByte(byte test) {
    byte count;
    for (count = 0; test != 0; count++) {
      test &= (test - 1);
    }
    return count;
  }


  private int toNumberOfBytes(int capacity) {
    return (int) Math.ceil(((double) capacity) / Byte.SIZE);
  }

  private Pair<Integer, Byte> getByteIndexToModify(int index) {
    // if (index >= capacity) {
    // // should we throw exception for now yes
    // throw new RuntimeException("Index out of capacity");
    // }
    Pair<Integer, Byte> byteIndexToRelativeIndex = new Pair<>();
    byteIndexToRelativeIndex.setFirst((int) Math.floor(((double) index) / Byte.SIZE));
    byteIndexToRelativeIndex.setSecond((byte) (index % Byte.SIZE));
    return byteIndexToRelativeIndex;
  }

  private void expandIfRequired(IBitMap bitMap) {
    if (this.sizeOfByteArray() < bitMap.sizeOfByteArray()) {
      // expand current byte array
      if (isOffsetBasedBytes) {
        // we dont support this type of operation
        throw new RuntimeException("Offset based BitMap doesnt support expansion");
      } else {
        // expand
        byte[] newBytes = new byte[bitMap.sizeOfByteArray()];
        System.arraycopy(this.backedBytes, 0, newBytes, 0, this.sizeOfByteArray());
        this.backedBytes = newBytes;
      }
    }
  }
}
