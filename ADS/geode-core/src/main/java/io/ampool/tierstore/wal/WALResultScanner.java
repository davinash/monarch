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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;

import io.ampool.monarch.table.exceptions.StoreInternalException;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

public class WALResultScanner implements Iterable<WALRecord> {

  protected static final Logger logger = LogService.getLogger();

  private String tableName = null;
  private int partitionId = -1;
  private WALReader reader;
  /* TODO: The files created after scan starts will be ignored. */
  private String[] fileList;
  private int fileListIndex = 0;

  public WALResultScanner(String tableName, int partitionId) {
    this.tableName = tableName;
    this.partitionId = partitionId;
    fileList = WriteAheadLog.getInstance().getAllfilesForBucket(tableName, partitionId);
    reader = updateFileInputStream();
  }

  public WALRecord next() {
    WALRecord record = null;

    if (reader != null) {
      try {
        record = reader.readNext();
      } catch (IOException e) {
        String message = "IO Error while reading from WAL file: " + fileList[fileListIndex];
        logger.error(message);
        throw new StoreInternalException(message);
      }
      if (null == record) {
        moveToNextFile();
        reader = updateFileInputStream();
        return next();
      } else {
        return record;
      }
    } else {
      if (fileList == null || fileListIndex >= fileList.length) {
        // we are done with iteration
        return null;
      } else {
        moveToNextFile();
        reader = updateFileInputStream();
        return next();
      }
    }
  }

  private WALReader updateFileInputStream() {
    WALReader reader = null;
    if (fileList == null || fileListIndex >= fileList.length)
      return reader;

    String walDir = WriteAheadLog.getInstance().getWalDirectory().toString();
    do {
      try {
        reader =
            new WALReader(Paths.get(new File(WriteAheadLog.getInstance().getWalDirectory().toFile(),
                fileList[fileListIndex]).toURI()));
      } catch (FileNotFoundException e) {
        // If WAL monitor thread moved old file to next tier,
        // those records should be return by next tier's scan so log and move
        if (logger.isDebugEnabled())
          logger.debug(e);
        moveToNextFile();
      } catch (IOException e) {
        // error in reading file
        // log and move to the next file
        logger.error(e);
        // if any reader is open close it
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e1) {
            logger.error(e);
          } finally {
            reader = null;
          }
        }
        moveToNextFile();
      }
    } while (reader == null && fileListIndex < fileList.length);

    return reader;
  }

  private void moveToNextFile() {
    if (null != reader) {
      try {
        reader.close();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
      reader = null;
    }
    fileListIndex++;
  }

  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        /* TODO */
        e.printStackTrace();
      }
    }
  }

  @Override
  public Iterator<WALRecord> iterator() {
    return new DefaultWALIterator(this);
  }

  class DefaultWALIterator implements Iterator<WALRecord> {

    private final WALResultScanner scanner;

    // The next RowMResult, possibly pre-read
    DefaultWALIterator(WALResultScanner scanner) {
      this.scanner = scanner;
    }

    WALRecord next = null;

    // return true if there is another item pending, false if there isn't.
    // this method is where the actual advancing takes place, but you need
    // to call next() to consume it. hasNext() will only advance if there
    // isn't a pending next().
    @Override
    public boolean hasNext() {
      if (this.next == null) {
        this.next = scanner.next();
      }
      return this.next != null;
    }

    // get the pending next item and advance the iterator. returns null if
    // there is no next item.
    @Override
    public WALRecord next() {
      // since hasNext() does the real advancing, we call this to determine
      // if there is a next before proceeding.
      // if (!hasNext()) {
      // return null;
      // }

      // if we get to here, then hasNext() has given us an item to return.
      // we want to return the item and then null out the next pointer, so
      // we use a temporary variable.
      WALRecord temp = this.next;
      this.next = null;
      return temp;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  };



}
