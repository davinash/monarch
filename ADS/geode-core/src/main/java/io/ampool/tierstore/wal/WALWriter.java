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

import static io.ampool.tierstore.wal.WriteAheadLog.*;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.IMKey;
import org.apache.geode.internal.GemFireVersion;


public class WALWriter {
  private Path file;
  private DataOutputStream fos;
  private AtomicInteger numRecords;
  private int recordLimit;
  private final String tableName;
  private final int partitionId;

  public WALWriter(String tableName, int partitionId, Path file, int recordLimit)
      throws IOException {
    this.tableName = tableName;
    this.file = file;
    this.partitionId = partitionId;
    numRecords = new AtomicInteger(0);
    initWriter(file, recordLimit);
  }

  private void initWriter(Path file, int recordLimit) throws IOException {
    if (file == null) {
      this.file = Paths.get(WriteAheadLog.getInstance().getWalDirectory() + "/" + tableName + "_"
          + partitionId + "_" + WriteAheadLog.getInstance().getNextSeqNo(tableName, partitionId)
          + WAL_INPROGRESS_SUFFIX);
    }

    this.recordLimit = recordLimit;
    numRecords.set(0);
    fos = new DataOutputStream(new FileOutputStream(this.file.toFile()));
    fos.write(WriteAheadLog.getNodeId());
    String ampoolVersion = GemFireVersion.getAmpoolVersion();
    fos.write(new byte[4]); /* reserved */
    fos.write(Bytes.toBytes(String.format("%16s", ampoolVersion)));
  }

  public Path getFile() {
    return file;
  }

  /**
   * Return the path associted with the opened file.
   *
   * @return path
   */
  public Path getPath() {
    return file;
  }

  /**
   * Write one Row to WAL file
   *
   * @return number of rows written
   */
  public int write(IMKey blockKey, BlockValue blockValue) throws IOException {
    if (this.numRecords.get() >= this.recordLimit) {
      close();
      WriteAheadLog.getInstance().markFileDone(this.file.toString(), false);
      synchronized (this) {
        /* make sure that only one threads creates new file */
        if (this.fos == null) {
          initWriter(null, recordLimit);
        }
      }
    }
    WALRecord record = new WALRecord(blockKey, blockValue);
    writeOneRecord(record);
    return 0;
  }

  /**
   * Write one WALRecord to WAL file
   *
   * @param record
   * @throws IOException
   */
  private void writeOneRecord(WALRecord record) throws IOException {
    synchronized (this) {
      if (this.fos == null) {
        initWriter(null, recordLimit);
      }
      record.toData(fos);
      numRecords.incrementAndGet();
      fos.flush();
    }
  }

  /**
   * Close a writer
   */
  public void close() throws IOException {
    if (fos == null) {
      return;
    } else {
      synchronized (this) {
        if (fos != null) {
          fos.close();
          fos = null;
        }
      }
    }
  }

  /**
   * Get number of records written by current WAL writer.
   *
   * @return get number of records written by this writer.
   */
  public int getNumRecords() {
    return numRecords.get();
  }
}
