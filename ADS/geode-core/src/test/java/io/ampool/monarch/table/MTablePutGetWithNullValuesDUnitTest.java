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

import java.util.Arrays;
import java.util.List;

import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.junit.Test;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTablePutGetWithNullValuesDUnitTest extends MTableDUnitHelper {
  public MTablePutGetWithNullValuesDUnitTest() {
    super();
  }

  private static final Logger logger = LogService.getLogger();

  private final int NUM_OF_COLUMNS = 4;
  private final byte[][] COLUMN_VALUES = new byte[][] {null, new byte[0], null, new byte[0]};
  private final String TABLE_NAME = "ALLOPERATION";
  private final String KEY_PREFIX = "KEY";
  private final int NUM_OF_ROWS = 10;
  private final String COLUMN_NAME_PREFIX = "COLUMN";


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    super.tearDown2();
  }


  private void doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(COLUMN_NAME_PREFIX + columnIndex, COLUMN_VALUES[columnIndex]);
      }
      table.put(record);
    }
  }

  private void doPutFrom(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPuts();
        return null;
      }
    });
  }

  public void createTable(final boolean ordered) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  private void doGets() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int k = 0; k < row.size() - 1; k++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] expectedValue = COLUMN_VALUES[columnIndex];

        if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out
              .println("actualColumnName   => " + Arrays.toString(row.get(k).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(expectedValue, (byte[]) row.get(k).getColumnValue())) {
          System.out.println("expectedValue => " + Arrays.toString(expectedValue));
          System.out.println(
              "actualValue    => " + Arrays.toString((byte[]) row.get(k).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
    }
  }

  private void createTableOn(VM vm, final boolean ordered) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(ordered);
        return null;
      }
    });
  }

  @Test
  public void testPutGetsWithNullAndEmptyValues_Ordered() {
    createTableOn(this.client1, true);
    doPutFrom(this.client1);
    doGets();
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testPutGetsWithNullAndEmptyValues_UnOrdered() {
    createTableOn(this.client1, false);
    doPutFrom(this.client1);
    doGets();
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }
}
