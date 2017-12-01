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
package io.ampool.monarch.table;

import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTableExpirationDUnitTest extends MTableDUnitHelper {

  protected static final Logger logger = LogService.getLogger();

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "MTableAllOpsDUnitTest";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected int TIMEOUT = 6;

  public MTableExpirationDUnitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createClientCache();
  }

  protected void createTable() {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }

    tableDescriptor
        .setExpirationAttributes(new MExpirationAttributes(TIMEOUT, MExpirationAction.DESTROY));
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  private void doPuts(int startIndex, int stopIndex) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = startIndex; rowIndex <= stopIndex; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  private void doGets(int startIndex, int stopIndex) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = startIndex; rowIndex <= stopIndex; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      verifyResult(rowIndex, result);
    }
  }

  private void verifyResult(int rowIndex, Row result) {
    assertEquals(NUM_OF_COLUMNS + 1 /* Key Column , GEN-1696 */, result.size());
    assertFalse(result.isEmpty());
    int columnIndex = 0;
    List<Cell> row = result.getCells();

    /* Verify the row Id features */
    if (!Bytes.equals(result.getRowId(), Bytes.toBytes(KEY_PREFIX + rowIndex))) {
      System.out.println(
          "expectedColumnName => " + Arrays.toString(Bytes.toBytes(KEY_PREFIX + rowIndex)));
      System.out.println("actualColumnName   => " + Arrays.toString(result.getRowId()));
      Assert.fail("Invalid Row Id");
    }

    for (Cell cell : row) {
      Assert.assertNotEquals(NUM_OF_COLUMNS + 1 /* Key Column , GEN-1696 */, columnIndex);
      byte[] expectedColumnName = null;
      byte[] exptectedValue = null;

      // GEN-1696 Changes starts
      if (columnIndex == NUM_OF_COLUMNS) {
        expectedColumnName = Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME);
        exptectedValue = result.getRowId();
      } else {
        expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
      }
      // GEN-1696 Changes ends

      if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
        System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
        System.out.println("actualColumnName   => " + Arrays.toString(cell.getColumnName()));
        Assert.fail("Invalid Values for Column Name");
      }
      if (!Bytes.equals(exptectedValue, (byte[]) cell.getColumnValue())) {
        System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
        System.out.println("actualValue    => " + Arrays.toString((byte[]) cell.getColumnValue()));
        Assert.fail("Invalid Values for Column Value");
      }
      columnIndex++;
    }
  }

  @Test
  public void testExpiration() throws InterruptedException {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    createTable();
    // 1. adding records 10 to 20
    doPuts(10, 20);
    doGets(10, 20);
    doScan(10, 20);

    // Sleeping for timeout/2 seconds
    Thread.sleep((TIMEOUT / 2) * 1000 + 2000);

    // 2. adding records 21 to 30
    doPuts(21, 30);

    // Should get all records from 10 to 30
    doGets(10, 30);
    doScan(10, 30);

    // Sleeping for timeout/2 seconds
    Thread.sleep((TIMEOUT / 2) * 1000 + 2000);

    // 3. adding records 31 to 40
    doPuts(31, 40);

    // Should get all records from 21 to 40
    // Records 10 -20 must have expired
    doGets(21, 40);
    doScan(21, 40);
    doGetExpectedNull(10, 20);

    // Sleeping for timeout/2 seconds
    Thread.sleep((TIMEOUT / 2) * 1000 + 2000);

    // Should get all records from 31 to 40
    // Records 20 - 30 must have expired
    doGets(31, 40);
    doScan(31, 40);
    doGetExpectedNull(10, 30);

    // Sleeping for timeout/2 seconds
    Thread.sleep((TIMEOUT / 2) * 1000 + 2000);

    // All records should have expired
    doGetExpectedNull(10, 40);
    doScanNull();

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  private void doScan(int startIndex, int stopIndex) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    final Scanner scanner = table.getScanner(new Scan());
    int rowIndex = startIndex;
    Row result = null;
    do {
      result = scanner.next();
      if (result != null) {
        verifyResult(rowIndex, result);
        rowIndex++;
        if (rowIndex == stopIndex)
          break;
      }
    } while (result != null);
    scanner.close();
  }

  private void doScanNull() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    final Scanner scanner = table.getScanner(new Scan());
    assertNull(scanner.next());
    scanner.close();
  }

  private void doGetExpectedNull(int startIndex, int stopIndex) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = startIndex; rowIndex <= stopIndex; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertTrue(result.isEmpty());
    }
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    closeMClientCache();
    closeMClientCache(client1);

    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MCacheFactory.getAnyInstance().close();
            return null;
          }
        }));
  }
}
