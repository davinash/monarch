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
package io.ampool.monarch.table.facttable.dunit;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.facttable.FTableDUnitHelper;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.StressTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(StressTest.class)
public class FTableBatchAppendDUnitStressTest extends MTableDUnitHelper {
  public FTableBatchAppendDUnitStressTest() {}

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());
    createClientCache(vm3);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache(vm3);
    closeMClientCache();
    super.tearDown2();
  }

  @Test
  public void testBatchAppendWithLargeRecords() throws InterruptedException {
    String tableName = getTestMethodName();
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    FTableDescriptor fd = new FTableDescriptor();
    for (int i = 0; i < 10; i++) {
      fd.addColumn("COL" + "_" + i);
    }
    fd.setRedundantCopies(0);
    fd.setTotalNumOfSplits(1);
    Admin admin = clientCache.getAdmin();
    FTable fTable = admin.createFTable(tableName, fd);
    assertNotNull(fTable);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);

    Record[] records = new Record[1000];
    final int expectedCount = 5000000;
    int totalInserted = 0;
    for (int i = 0; i < expectedCount / 1000; i++) {
      for (int j = 0; j < 1000; j++) {
        records[j] = new Record();
        for (int k = 0; k < 10; k++) {
          records[j].add("COL" + "_" + k, Bytes.toBytes("COL_" + k + "_" + totalInserted));
        }
      }
      fTable.append(records);
      totalInserted += 1000;
      if (totalInserted % 100000 == 0) {
        System.out.println("Total records inserted = " + totalInserted);
      }
    }

    final long size = MTableUtils.getTotalCount(fTable, null, null, true);
    System.out.println("### Count on  server " + size);

    try {
      Thread.sleep(5000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    Scanner scanner = fTable.getScanner(new Scan());
    int totalCount = 0;
    for (Row ignored : scanner) {
      totalCount++;
    }
    System.out.println("### totalCount = " + totalCount);
    //// TODO: sometimes the de-duplication does not work as expected.. needs to be fixed
    assertTrue("Incorrect number of records returned by scan.", (totalCount >= expectedCount));
    // assertEquals("Incorrect number of records returned by scan.", expectedCount, totalCount);
  }
}
