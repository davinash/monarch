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

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@Category(MonarchTest.class)
@RunWith(JUnitParamsRunner.class)
public class MTableWithVirtualColumnDUnitTest extends MTableDUnitHelper {

  private final String[] columnNames = {"ID", "NAME", "AGE", "DEPT"};
  private final String[][] columnValues = {{"ID1", "NAME1", "AGE1", "DEPT1"},
      {"ID2", "NAME2", "AGE2", "DEPT2"}, {"ID3", "NAME3", "AGE3", "DEPT3"},
      {"ID4", "NAME4", "AGE4", "DEPT4"}, {"ID5", "NAME5", "AGE5", "DEPT5"}};

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
    closeMClientCache();
    closeMClientCache(this.vm3);
    super.tearDown2();
  }

  @Test
  @Parameters(method = "tableType")
  public void testTableOpsWithVirtualColumn(MTableType tableType) {
    final String TABLE_NAME = "testTableOpsWithVirtualColumn";
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor(tableType);
    for (int i = 0; i < columnNames.length; i++) {
      if (columnNames[i].equals("ID")) {
        tableDescriptor.addColumn(columnNames[i], BasicTypes.BINARY);
      } else {
        tableDescriptor.addColumn(columnNames[i]);
      }
    }
    MTable table = clientCache.getAdmin().createMTable(TABLE_NAME, tableDescriptor);

    for (int i = 0; i < columnValues.length; i++) {
      Put put = new Put(Bytes.toBytes(columnValues[i][0]));
      for (int j = 0; j < columnValues[i].length; j++) {
        put.addColumn(columnNames[j], Bytes.toBytes(columnValues[i][j]));
      }
      table.put(put);
    }
    // Doing GET Operation
    for (int i = 0; i < columnValues.length; i++) {
      Get get = new Get(Bytes.toBytes((columnValues[i][0])));
      Row row = table.get(get);
      List<Cell> cells = row.getCells();
      Assert.assertEquals(columnValues[i].length + 1, cells.size());
      for (int k = 0; k < cells.size() - 1; k++) {
        VerifyCell(columnValues[i][k], cells.get(k), k);
      }
      if (!Bytes.equals(Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME),
          cells.get(cells.size() - 1).getColumnName())) {
        Assert.fail("Last Column Should be Key Column");
      }
      if (!Bytes.equals((byte[]) cells.get(cells.size() - 1).getColumnValue(), row.getRowId())) {
        Assert.fail("Key Column Value should be equal to the row key");
      }
    }
    // Doing Scan Operation
    int i = 0;
    for (Row row : table.getScanner(new Scan())) {
      List<Cell> cells = row.getCells();
      Assert.assertEquals(columnValues[i].length + 1, cells.size());
      for (int k = 0; k < cells.size() - 1; k++) {
        VerifyCell(columnValues[i][k], cells.get(k), k);
      }
      if (!Bytes.equals(Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME),
          cells.get(cells.size() - 1).getColumnName())) {
        Assert.fail("Last Column Should be Key Column");
      }
      if (!Bytes.equals((byte[]) cells.get(cells.size() - 1).getColumnValue(), row.getRowId())) {
        Assert.fail("Key Column Value should be equal to the row key");
      }
      i++;
    }
    clientCache.getAdmin().deleteMTable(TABLE_NAME);
    clientCache.close();
  }

  private void VerifyCell(String s, Cell cell1, int k) {
    Cell cell = cell1;
    if (!Bytes.equals(Bytes.toBytes(columnNames[k]), cell.getColumnName())) {
      System.out.println("Expected Column Name -> " + columnNames[k]);
      System.out.println("Actual   Column Name -> " + Arrays.toString(cell.getColumnName()));
      Assert.fail("Column Names are not equal");
    }
    if (!Bytes.equals(Bytes.toBytes(s), (byte[]) cell.getColumnValue())) {
      System.out.println("Expected Column Value -> " + s);
      System.out.println("Actual   Column Value -> " + cell.getColumnValue());
      Assert.fail("Column Values are not equal");
    }
  }

  private MTableType[] tableType() {
    return new MTableType[] {MTableType.ORDERED_VERSIONED, MTableType.UNORDERED};
  }
}
