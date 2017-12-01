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

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.IMKey;

public class WALRecord {

  private WALRecordHeader header;
  private final BlockValue blockValue;


  public WALRecord(final IMKey blockKey, final BlockValue blockValue) {
    this.blockValue = blockValue;
    this.header = new WALRecordHeader((BlockKey) blockKey);
  }

  /**
   * @return byte array representation of the record
   */
  public byte[] getWALBytes() {
    byte[] blockValueBytes = blockValue.getBytes();
    // [Total record length ] + [ WAL record header length] + [block value length]
    int totalRecordSize = Bytes.SIZEOF_INT + WALRecordHeader.SIZE + blockValueBytes.length;
    byte[] bytes = new byte[totalRecordSize];
    int offset = 0;
    System.arraycopy(Bytes.toBytes(totalRecordSize), 0, bytes, offset, Bytes.SIZEOF_INT);
    offset += Bytes.SIZEOF_INT;
    System.arraycopy(header.getBytes(), 0, bytes, offset, WALRecordHeader.SIZE);
    offset += WALRecordHeader.SIZE;
    System.arraycopy(blockValueBytes, 0, bytes, offset, blockValueBytes.length);
    return bytes;
  }

  /**
   * Get the WAL header from record.
   * 
   * @return WALHeader
   */
  public WALRecordHeader getHeader() {
    return header;
  }

  public byte[][] getRecords() {
    // create store records from Block
    byte[][] records = new byte[size()][];
    for (int i = 0; i < size(); i++) {
      records[i] = blockValue.getRecord(i);
    }
    return records;
  }

  /**
   * Return the stream of records from the underlying block-value.
   *
   * @return the stream of records/rows
   */
  public Stream<Object> getRecordStream() {
    return StreamSupport.stream(
        Spliterators.spliterator(blockValue.iterator(), blockValue.size(), Spliterator.ORDERED),
        false);
  }

  public int size() {
    return blockValue.getCurrentIndex();
  }

  public BlockKey getBlockKey() {
    return this.header.getBlockKey();
  }

  public BlockValue getBlockValue() {
    return this.blockValue;
  }

  public Encoding getEncoding() {
    return Encoding.getEncoding(this.blockValue.getRowHeader().getEncoding());
  }

  public byte[] getStoredRecordFormatIdentifiers() {
    return this.blockValue.getRowHeader().getHeaderBytes();
  }
}
