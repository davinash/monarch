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
package io.ampool.monarch.table.quickstart;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import java.util.List;
import java.util.Map;


public class MTableCoprocessorExample {

  private static final String TABLE_NAME = "EmployeeTable";
  private static final String COL1 = "NAME";
  private static final String COL2 = "ID";
  private static final String COL3 = "AGE";
  private static final String COL4 = "SALARY";

  final static int numBuckets = 113;
  final static int numOfEntries = 1000;

  private static String ROW_COUNT_COPROCESSOR_CLASS =
      "io.ampool.monarch.table.quickstart.SampleRowCountCoprocessor";

  public static void main(String args[]) {

    // create a configuration and connect to monarch locator.
    MClientCache clientCache = new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
        .addPoolLocator("127.0.0.1", 10335).create();
    System.out.println("Connection to monarch distributed system is successfully done!");

    long rows = 0L;
    try {
      MTable mtable = createTable(TABLE_NAME);
      insertRows(mtable);

      // execute coprocessor to get row count
      Scan scan = new Scan();
      String startKey = "rowkey-0";
      String stopKey = "rowkey-" + (numOfEntries - 1);
      scan.setStartRow(Bytes.toBytes(startKey));
      scan.setStopRow(Bytes.toBytes(stopKey));
      MExecutionRequest request = new MExecutionRequest();
      request.setScanner(scan);

      Map<Integer, List<Object>> collector = mtable.coprocessorService(ROW_COUNT_COPROCESSOR_CLASS,
          "rowCount", scan.getStartRow(), scan.getStopRow(), request);

      rows = collector.values().stream().mapToLong(value -> value.stream().map(val -> (Long) val)
          .reduce(0L, (prev, current) -> prev + current)).sum();
      System.out.println("Row count: " + rows);

    } catch (MCoprocessorException cce) {

    }
  }

  private static MTable createTable(String tableName) {
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    // Add Coprocessor
    tableDescriptor.addCoprocessor(ROW_COUNT_COPROCESSOR_CLASS);

    tableDescriptor.addColumn(Bytes.toBytes(COL1)).addColumn(Bytes.toBytes(COL2))
        .addColumn(Bytes.toBytes(COL3)).addColumn(Bytes.toBytes(COL4));

    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setTotalNumOfSplits(numBuckets);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable(tableName, tableDescriptor);
    // assertEquals(mtable.getName(), tableName);
    return mtable;
  }

  private static void insertRows(MTable mtable) {
    for (int keyIndex = 0; keyIndex < numOfEntries; keyIndex++) {
      Put myput1 = new Put(Bytes.toBytes("rowkey-" + keyIndex));
      myput1.addColumn(Bytes.toBytes(COL1), Bytes.toBytes("col" + keyIndex));
      myput1.addColumn(Bytes.toBytes(COL2), Bytes.toBytes(keyIndex + 10));
      myput1.addColumn(Bytes.toBytes(COL3), Bytes.toBytes(keyIndex + 10));
      myput1.addColumn(Bytes.toBytes(COL4), Bytes.toBytes(keyIndex + 10));
      mtable.put(myput1);
    }
  }

}
