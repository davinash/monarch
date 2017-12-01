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
package io.ampool.monarch.table.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;


@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class MultiThreadedScanDUnitTest extends MTableDUnitHelper {
  private static final String[] COLUMN_NAMES =
      new String[] {"NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ"};

  class ScanThread implements Runnable {
    private String tableName;
    private int expectedRecords;

    ScanThread(final String tableName, final int expectedRecords) {
      this.tableName = tableName;
      this.expectedRecords = expectedRecords;
    }

    @Override
    public void run() {
      MTable table = MClientCacheFactory.getAnyInstance().getMTable(tableName);
      Iterator<Row> itr = table.getScanner(new Scan()).iterator();
      int count = 0;
      while (itr.hasNext()) {
        count++;
        assertNotNull(itr.next());
      }
      System.out.println(Thread.currentThread().getId() + ": Scanned records: " + count);
      assertEquals(expectedRecords, count);
    }
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MCacheFactory.getAnyInstance().close();
            return null;
          }
        }));
    super.tearDown2();
  }

  private void createTable(final String tableName, final boolean ordered) {
    final Schema schema = new Schema(COLUMN_NAMES);
    MTableDescriptor tableDescriptor =
        new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
    tableDescriptor.setSchema(schema);
    MClientCacheFactory.getAnyInstance().getAdmin().createMTable(tableName, tableDescriptor);
  }

  private void deleteTable(final String tableName) {
    MClientCacheFactory.getAnyInstance().getAdmin().deleteMTable(tableName);
  }

  private void ingestData(final String tableName, final int totalRecords) {
    MTable table = MClientCacheFactory.getAnyInstance().getMTable(tableName);
    for (int j = 0; j < totalRecords; j++) {
      Put put = new Put(Integer.toString(j));
      for (String colName : COLUMN_NAMES) {
        put.addColumn(colName, Bytes.toBytes(colName + "_" + (j * 1000)));
      }
      table.put(put);
    }
  }

  private void runMultiThreadedScan(final String tableName, final int numThreads,
      final int expectedRecords) throws InterruptedException {
    Thread[] scanThreads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      scanThreads[i] = new Thread(new ScanThread(tableName, expectedRecords));
      scanThreads[i].start();
    }
    for (int i = 0; i < numThreads; i++) {
      scanThreads[i].join();
    }
  }


  public static Object[] getTestData() {
    return new Object[][] {{"MTScanTestOrdered2", true, 2, 100000},
        {"MTScanTestOrdered4", true, 4, 100000}, {"MTScanTestOrdered8", true, 8, 100000},
        {"MTScanTestOrdered16", true, 16, 100000}, {"MTScanTestOrdered32", true, 32, 100000},
        {"MTScanTestUnOrdered2", false, 2, 100000}, {"MTScanTestUnOrdered4", false, 4, 100000},
        {"MTScanTestUnOrdered8", false, 8, 100000}, {"MTScanTestUnOrdered16", false, 16, 100000},
        {"MTScanTestUnOrdered32", false, 32, 100000}};
  }

  @Test
  @Parameters(method = "getTestData")
  public void testMultiThreadedScan(final String tableName, final boolean ordered,
      final int numThreads, final int totalRecords) throws InterruptedException {
    createTable(tableName, ordered);
    ingestData(tableName, totalRecords);
    runMultiThreadedScan(tableName, numThreads, totalRecords);
    deleteTable(tableName);
  }


}


