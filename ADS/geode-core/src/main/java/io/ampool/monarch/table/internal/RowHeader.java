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

import io.ampool.monarch.table.Bytes;

/*
 * Header for the byte array format used for ftable header format is
 * ----------------------------------------------------------------------- | Magic Number | Encoding
 * | Schema version | Reserved for future use |
 * ----------------------------------------------------------------------- |-- 1 byte -----|-- 1
 * byte --|--- 1 byte -----|-----1 byte ------------|
 * -----------------------------------------------------------------------
 */
public class RowHeader {
  private byte MAGIC_NUMBER = 3;
  private byte ENCODING = 2;
  private byte SCHEMA_VERSION = 1;
  private byte RESERVED = 0;

  private static final int headerLength = 4;
  private static final int magicNumberOffset = 0;
  public static final int OFFSET_ENCODING = 1;
  private static final int schemaVersionOffset = 2;
  private static final int reservedOffset = 3;

  /**
   * Prevent instantiation of this class.
   */
  public RowHeader() {}

  public RowHeader(final byte MAGIC_NUMBER, final byte ENCODING, final byte SCHEMA_VERSION,
      final byte RESERVED) {
    this.MAGIC_NUMBER = MAGIC_NUMBER;
    this.ENCODING = ENCODING;
    this.SCHEMA_VERSION = SCHEMA_VERSION;
    this.RESERVED = RESERVED;
  }

  public RowHeader(final byte[] headerBytes) {
    int offset = Bytes.SIZEOF_INT;

    MAGIC_NUMBER = headerBytes[magicNumberOffset];
    ENCODING = headerBytes[OFFSET_ENCODING];
    SCHEMA_VERSION = headerBytes[schemaVersionOffset];
    RESERVED = headerBytes[reservedOffset];

  }


  public RowHeader(byte schemaVersion) {
    this.SCHEMA_VERSION = schemaVersion;
  }


  public byte getMagicNumber() {
    return MAGIC_NUMBER;
  }

  public static byte getMagicNumber(byte[] header) {
    return header[magicNumberOffset];
  }

  public byte getEncoding() {
    return ENCODING;
  }

  public static byte getEncoding(byte[] header) {
    return header[OFFSET_ENCODING];
  }

  public byte getSchemaVersion() {
    return SCHEMA_VERSION;
  }

  public static byte getSchemaVersion(byte[] header) {
    return header[OFFSET_ENCODING];
  }

  public byte setSchemaVersion(byte schemaVersion) {
    return this.SCHEMA_VERSION = schemaVersion;
  }

  public byte getReserved() {
    return RESERVED;
  }

  public static byte getReserved(byte[] header) {
    return header[reservedOffset];
  }

  public static int getHeaderLength() {
    return headerLength;
  }

  public byte[] getHeaderBytes() {
    final byte[] bytes = new byte[headerLength];
    Bytes.putByte(bytes, magicNumberOffset, MAGIC_NUMBER);
    Bytes.putByte(bytes, OFFSET_ENCODING, ENCODING);
    Bytes.putByte(bytes, schemaVersionOffset, SCHEMA_VERSION);
    Bytes.putByte(bytes, reservedOffset, RESERVED);
    return bytes;
  }
}
