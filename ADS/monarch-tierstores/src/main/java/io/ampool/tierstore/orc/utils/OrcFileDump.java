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
package io.ampool.tierstore.orc.utils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.logging.log4j.Logger;

import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.store.StoreRecord;

public class OrcFileDump {

  private static final Logger logger = LogService.getLogger();

  private static Boolean dumpData = false;

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
    dumpOrcRecordsFromObject(fsObject);
  }

  private static void dumpOrcRecordsFromObject(File fsObject) throws IOException {
    if (fsObject.isDirectory()) {
      String[] objs = fsObject.list();
      Arrays.sort(objs);
      for (String file : objs) {
        dumpOrcRecordsFromObject(new File(fsObject.getAbsolutePath() + "/" + file));
      }
    } else {
      dumpOrcRecordsFromFile(fsObject);
    }
  }

  private static void dumpOrcRecordsFromFile(File file) throws IOException {
    StringBuilder metaInfo = new StringBuilder(1024);

    metaInfo.append("File: " + file.getAbsolutePath());
    Reader reader = null;
    try {
      reader = OrcFile.createReader(new Path(file.toPath().toString()),
          OrcFile.readerOptions(new Configuration()));
    } catch (Exception e) {
      System.err.println("Exception caught for file: " + file);
      e.printStackTrace();
    }
    StoreRecord record = null;
    int numRecords = 0;
    BlockKey firstBlkKey = null;
    BlockKey lastBlkKey = null;
    RecordReader rows = reader.rows();
    Object row = null;
    while (rows.hasNext()) {
      numRecords++;
      record = (StoreRecord) rows.next(row);
      if (dumpData) {
        StringBuilder dataInfo = new StringBuilder(1024000);
        dataInfo.append("\nRecord " + numRecords + ":" + record);
        System.out.println(dataInfo.toString());
      }
    }
  }

  private static void usage() {
    System.err.println("Usage OrcFileDump <wal dir|wal file> <1|0>(for data)");
  }
}
