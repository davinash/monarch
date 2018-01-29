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

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import org.apache.geode.internal.cache.EntryEventImpl;

public class WALReader implements AutoCloseable {
  private Path file;
  private DataInputStream fis = null;

  public WALReader(Path file) throws IOException {
    this.file = file;
    final FileInputStream fs = new FileInputStream(file.toFile());
    fis = new DataInputStream(fs);
    WALUtils.verifyIntegrity(file, fs, true);
  }

  public WALReader(Path file, Boolean verifyIntegrity) throws IOException {
    this.file = file;
    final FileInputStream fs = new FileInputStream(file.toFile());
    fis = new DataInputStream(fs);
    WALUtils.verifyIntegrity(file, fs, verifyIntegrity);
  }

  public Path getFile() {
    return file;
  }

  /**
   * Read the next WAL record from a WAL file
   *
   * @return WALRecord or null if there are no more records
   */
  public WALRecord readNext() throws IOException {
    return WALRecord.fromData(fis);
  }

  /**
   * Read next N WAL record from a WAL file
   *
   * @param numRecords
   * @return Number of records read, null if there are no more records
   */
  public WALRecord[] readNext(int numRecords) throws IOException {
    /*
     * TODO: Can this be made faster by changing the amount of data we read from file in a single
     * read(block reads) and maintaining the buffer
     */
    WALRecord[] walRecords = new WALRecord[numRecords];
    int i = 0;
    for (i = 0; i < numRecords; i++) {
      WALRecord record = readNext();
      if (record == null) {
        break;
      }
      walRecords[i] = record;
    }
    if (i == 0) {
      return null;
    } else {
      return (i >= 1 && i < numRecords) ? Arrays.copyOfRange(walRecords, 0, i) : walRecords;
    }
  }

  /**
   * Read the previous WAL record from a WAL file
   *
   * @return WALRecord or null if there are no more records
   */
  public WALRecord readPrev() {
    /* TODO: Need a RandomFileReader here */
    return null;
  }

  /**
   * Read previous N WAL record from a WAL file
   *
   * @param numRecords
   * @return Number of records read, null if there are no more records
   */
  public WALRecord[] readPrev(int numRecords) {
    /* TODO: Need a RandomFileReader here */
    return null;
  }

  /**
   * Close the WAL file associated with this writer
   */
  public void close() throws IOException {
    fis.close();
  }
}
