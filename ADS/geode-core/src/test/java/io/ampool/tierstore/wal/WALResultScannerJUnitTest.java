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

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.FTableTest;

@Category(FTableTest.class)
public class WALResultScannerJUnitTest {
  private static final String TABLE_NAME = "testTable";
  WriteAheadLog writeAheadLog = null;

  private void cleanDir(String dirName) {
    File dir = new File(dirName);
    String[] files = dir.list();
    if (files != null) {
      for (String f : files) {
        File file = new File(dirName + "/" + f);
        file.delete();
      }
    }
    dir.delete();
  }

  @Before
  public void setup() throws IOException {
    cleanDir("/tmp/WALDIR");
    WriteAheadLog.getInstance().deinit();
    Exception e = null;
    try {
      writeAheadLog = WriteAheadLog.getInstance();
      writeAheadLog.init(Paths.get("/tmp/WALDIR"), 10, 10);
    } catch (Exception e1) {
      e = e1;
    }
    assertNull(e);
  }

  @After
  public void cleanUp() {
    Exception e = null;
    try {
      writeAheadLog.deinit();
    } catch (IOException e1) {
      e = e1;
    }
    assertNull(e);
    cleanDir("/tmp/WALDIR");
  }

  private byte[] incrementByteArray(byte[] arr) {
    int a = Bytes.toInt(arr);
    a++;
    return Bytes.toBytes(a);
  }

  @Test
  public void testScannerMultiBucket() {
    singleBucketTest(TABLE_NAME, 1, 1001, 1001, 1, 100);
    singleBucketTest(TABLE_NAME, 2, 2003, 2003, 2, 300);
  }

  @Test
  public void testScanner() {
    singleBucketTest(TABLE_NAME, 1, 1001, 1001, 1, 100);
  }

  private void singleBucketTest(String tableName, int partitionId, int numRecords, int expRecords,
      int expInProgress, int expDone) {
    byte[] data = {0, 0, 0, 0};
    byte[] data1 = data;
    Exception e = null;

    for (int i = 0; i < numRecords; i++) {
      try {
        BlockValue bv = new BlockValue(1000);
        bv.checkAndAddRecord(data1);
        writeAheadLog.append(tableName, partitionId, new BlockKey(1l), bv);
        // writeAheadLog.append(new FTableRowInfo(tableName, partitionId, new BlockKey(1l), data1));
        data1 = incrementByteArray(data1);
      } catch (IOException e1) {
        e = e1;
      }
      assertNull(e);
    }

    String[] inprogressFiles = writeAheadLog.getInprogressFiles();
    assertEquals(expInProgress, inprogressFiles.length);
    String[] completedFiles = writeAheadLog.getCompletedFiles();
    assertEquals(expDone, completedFiles.length);

    WALResultScanner scanner = writeAheadLog.getScanner("testTable", partitionId);
    assertNotNull(scanner);
    data1 = data;
    WALRecord result = null;
    int counter = 0;
    do {
      result = scanner.next();
      if (result != null) {
        counter++;
        assertEquals(1, result.getBlockKey().getStartTimeStamp());
        assertEquals(0, Bytes.compareTo(result.getRecords()[0], data1));
        // System.out.println("Key =" + Arrays.toString(result.getRowValue()));
        // System.out.println("Data = " + Arrays.toString(result.getRowValue()));
      }
      data1 = incrementByteArray(data1);
    } while (result != null);
    System.out.println("Counter = " + counter);

    assertEquals(expRecords, counter);
  }
}
