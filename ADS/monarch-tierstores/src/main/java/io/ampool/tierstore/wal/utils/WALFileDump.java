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

package io.ampool.tierstore.wal.utils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.tierstore.wal.WALReader;
import io.ampool.tierstore.wal.WALRecord;
import io.ampool.tierstore.wal.WALRecordHeader;

public class WALFileDump {
  private static Boolean dumpData = false;
  private static Set<String> ignore = null;

  static {
    ignore = new HashSet<>();
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      usage();
    }

    if (args.length == 2) {
      dumpData = Boolean.parseBoolean(args[1]);
      if (dumpData) {
        System.out.println("Data will be dumped");
      }
    }



    File fsObject = new File(args[0]);
    dumpWalRecordsFromObject(fsObject);

  }

  private static void dumpWalRecordsFromObject(File fsObject) throws IOException {
    if (fsObject.isDirectory()) {
      String[] objs = fsObject.list();
      Arrays.sort(objs);
      for (String file : objs) {
        dumpWalRecordsFromObject(new File(fsObject.getAbsolutePath() + "/" + file));
      }
    } else {
      dumpWalRecordsFromFile(fsObject);
    }
  }

  private static void dumpWalRecordsFromFile(File file) throws IOException {
    StringBuilder metaInfo = new StringBuilder(1024);

    if (ignore.contains(file.getName())) {
      return;
    }

    metaInfo.append("File: " + file.getAbsolutePath());
    WALReader reader = null;
    try {
      reader = new WALReader(file.toPath(), false);
    } catch (Exception e) {
      System.err.println("Exception caught for file: " + file);
      e.printStackTrace();
    }
    WALRecord record = null;
    WALRecord prevRecord = null;
    int numRecords = 0;
    BlockKey firstBlkKey = null;
    BlockKey lastBlkKey = null;
    do {
      prevRecord = record;
      record = reader.readNext();
      if (record == null) {
        break;
      }
      numRecords++;
      if (firstBlkKey == null) {
        firstBlkKey = record.getBlockKey();
      }
      if (dumpData) {
        StringBuilder dataInfo = new StringBuilder(1024000);
        dataInfo.append("\nBlock Key: " + record.getBlockKey());
        dataInfo.append("\nHeader->size " + WALRecordHeader.SIZE);
        dataInfo.append("\nHeader->blockKey " + record.getHeader().getBlockKey());
        // dataInfo.append("\nRow: " + Arrays.toString(record.getRow().getValue()));
        BlockValue blockValue = record.getBlockValue();
        dataInfo.append("\nNum rows in block: " + blockValue.getCurrentIndex());
        int index = 0;
        for (byte[] value : blockValue.getRecords()) {
          if (value != null) {
            dataInfo.append("\nRecord " + index + ":" + Arrays.toString(value));
          }
          index++;
        }
        System.out.println(dataInfo.toString());
      }
    } while (record != null);
    if (prevRecord != null) {
      lastBlkKey = prevRecord.getBlockKey();
    }

    metaInfo.append(",Number of blocks:" + numRecords);
    metaInfo.append(",First block key: " + firstBlkKey);
    metaInfo.append(",Last block key: " + lastBlkKey);
    System.out.println(metaInfo.toString());

  }

  private static void usage() {
    System.err.println("Usage WALFileDump <wal dir|wal file> <1|0>(for data)");
  }
}
