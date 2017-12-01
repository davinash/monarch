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
package io.ampool.monarch.table.perf;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MTableBatchPutPerf {
  /**
   * Helper method which generate the data and keep in a file
   *
   * @param numOfRecords
   * @param numOfColumns
   * @param lenOfColumn
   * @param lenOfRowKey
   */
  private void generateInputData(final int numOfRecords, final int numOfColumns,
      final int lenOfColumn, final int lenOfRowKey) throws IOException {
    MTablePerfUtils.generateDataIfRequired(numOfRecords, numOfColumns, lenOfColumn, lenOfRowKey,
        this.getClass().getCanonicalName() + ".simple.put", false);
  }

  private void putSomeRecords(final int numOfColumns) throws IOException {
    MClientCache clientCache = new MClientCacheFactory().set("log-file", "/tmp/MTableClient.log")
        .addPoolLocator("127.0.0.1", 10334).create();

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int i = 0; i < numOfColumns; i++) {
      tableDescriptor.addColumn(Bytes.toBytes("COLUMN" + i));
    }
    tableDescriptor.setRedundantCopies(1);

    Admin admin = clientCache.getAdmin();
    String tableName = "Performance";
    MTable table = admin.createTable(tableName, tableDescriptor);

    BufferedReader in = new BufferedReader(
        new FileReader("/tmp/" + this.getClass().getCanonicalName() + ".simple.put"));
    String str;
    str = in.readLine();


    long totalTime = 0;
    int size = 0;
    List<Put> listOfPuts = new ArrayList<>();
    long start = System.currentTimeMillis();
    while ((str = in.readLine()) != null) {
      String[] ar = str.split(",");
      byte[] rowKey = Bytes.toBytes(ar[0]);
      Put singlePut = new Put(rowKey);
      for (int i = 1; i <= numOfColumns; i++) {
        singlePut.addColumn(Bytes.toBytes("COLUMN" + (i - 1)), Bytes.toBytes(ar[i]));
      }
      size += singlePut.getSerializedSize();
      listOfPuts.add(singlePut);

      if (listOfPuts.size() == 10) {
        table.put(listOfPuts);
        listOfPuts.clear();
      }
    }
    System.out.println("Put finished in " + (System.currentTimeMillis() - start)
        + " MilliSeconds for Size = " + size + "  Bytes ");
    in.close();
    clientCache.getAdmin().deleteTable(tableName);
    clientCache.close();

  }


  public static void main(String args[]) throws IOException {
    MTableBatchPutPerf mspp = new MTableBatchPutPerf();
    int numOfColumns = 10;
    mspp.generateInputData(100000, numOfColumns, 300, 20);
    System.out.println("Data Generation is complete ...");
    mspp.putSomeRecords(numOfColumns);
    mspp.putSomeRecords(numOfColumns);
  }


}
