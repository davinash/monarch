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
package io.ampool.tierstore.wal;

import io.ampool.monarch.table.ftable.internal.BlockKey;


public class WALRecordHeader {
  private BlockKey blockKey;
  public static final int SIZE = BlockKey.length;


  public WALRecordHeader(BlockKey blockKey) {
    this.blockKey = blockKey;
  }


  /**
   * Gets the record's block key from header.
   * 
   * @return Block key
   */
  public BlockKey getBlockKey() {
    return blockKey;
  }

  /**
   * Get the byte array corresponding to the record header
   * 
   * @return byte[] representation of record header.
   */
  public byte[] getBytes() {
    byte[] bytes = new byte[SIZE];
    System.arraycopy(blockKey.getBytes(), 0, bytes, 0, SIZE);
    return bytes;
  }

  public static WALRecordHeader getWALRecordHeader(byte[] headerBytes) {
    if (headerBytes.length != SIZE) {
      return null;
    }
    BlockKey blockKey = new BlockKey(headerBytes);
    return new WALRecordHeader(blockKey);
  }

}
