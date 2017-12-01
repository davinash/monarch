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

import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.*;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTableAllDeleteDunit extends MTableDUnitConfigFramework {
  public MTableAllDeleteDunit() {
    super();
  }

  public static String KEY_PREFIX = "KEY";

  @Test
  public void testDelete() {

    String[] keys = {"005", "006", "007", "008", "009"};
    String[] keys2 = {"000", "001", "002", "003", "004"};
    runAllConfigs(new FrameworkRunnable() {

      @Override
      public void run() {
        MTable table = getTable();
        doPut(table, keys);
        doDelete(table, keys, keys2);
        doVerifyDeleteFromClient(client1, keys, keys2);
        doBatchDelete(table, keys);
      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        if (table.getTableDescriptor().isDiskPersistenceEnabled()) {
          doDelete(table, keys, keys2);
          doVerifyDeleteFromClient(client1, keys, keys2);
          doBatchDelete(table, keys);
        } else {
          doPut(table, keys);
          doDelete(table, keys, keys2);
          doVerifyDeleteFromClient(client1, keys, keys2);
          doBatchDelete(table, keys);
        }
      }
    });
  }

  @Test
  public void testDeleteWithCoprocessor1() {
    String UPDATE_COPROCESSOR_CLASS = "io.ampool.monarch.table.MTableDeleteCoprocessor";

    String[] keys3 = {"021", "022", "023", "024", "025"};

    runAllConfigs(new FrameworkRunnable() {
      @Override
      public void run() {
        MTable table = getTable();
        System.out.println("MTableAllDeleteDunit.Normal TableType = "
            + table.getTableDescriptor().getTableType().name());
        // Execute for both Ordered and UnOrdered Table
        {
          doPut(table, keys3);
          Map<Integer, List<Object>> collector = table.coprocessorService(UPDATE_COPROCESSOR_CLASS,
              "deleteRows", table.getTableDescriptor().getStartRangeKey(),
              table.getTableDescriptor().getStopRangeKey(), new MExecutionRequest());
          Object[] resultValues = collector.values().toArray();
          for (Object result : resultValues) {
            ArrayList list = (ArrayList) result;
            for (Object res : list) {
              boolean check = (boolean) res;
              assertTrue(check);
            }
          }
          doGet(table, keys3);
        }
      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        // Execute for both Ordered and Unordered Table
        {
          System.out.println("MTableAllDeleteDunit.Restart TableType = "
              + table.getTableDescriptor().getTableType().name());
          doPut(table, keys3);
          Map<Integer, List<Object>> collector = table.coprocessorService(UPDATE_COPROCESSOR_CLASS,
              "deleteRows", table.getTableDescriptor().getStartRangeKey(),
              table.getTableDescriptor().getStopRangeKey(), new MExecutionRequest());
          Object[] resultValues = collector.values().toArray();
          for (Object result : resultValues) {
            ArrayList list = (ArrayList) result;
            for (Object res : list) {
              boolean check = (boolean) res;
              assertTrue(check);
            }
          }
          doGet(table, keys3);
        }
      }
    });
  }


  // TODO For now commenting out. To be fixed ASAP
  @Ignore
  // @Test
  public void testDeleteWithCoprocessor2() {
    String UPDATE_COPROCESSOR_CLASS = "io.ampool.monarch.table.MTableDeleteCoprocessor";

    String[] keys1 = {"005", "006", "007", "008", "009"};

    runAllConfigs(new FrameworkRunnable() {
      @Override
      public void run() {
        MTable table = getTable();
        if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
          doPut(table, keys1);
          Map<Integer, List<Object>> collector = table.coprocessorService(UPDATE_COPROCESSOR_CLASS,
              "doDelete", null, null, new MExecutionRequest());
          Object[] resultValues = collector.values().toArray();
          boolean finalResult = true;
          for (Object result : resultValues) {
            ArrayList list = (ArrayList) result;
            for (Object res : list) {
              System.out.println("res.getClass().getName() = " + res.getClass().getName());
              finalResult &= (boolean) res;
            }
          }
          assertTrue(finalResult);
        }
      }

      @Override
      public void runAfterRestart() {
        MTable table = getTable();
        if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
          doPut(table, keys1);
          Map<Integer, List<Object>> collector = table.coprocessorService(UPDATE_COPROCESSOR_CLASS,
              "doDelete", null, null, new MExecutionRequest());
          Object[] resultValues = collector.values().toArray();
          boolean finalResult = true;
          for (Object result : resultValues) {
            ArrayList list = (ArrayList) result;
            for (Object res : list) {
              finalResult &= (boolean) res;
            }
          }
          assertTrue(finalResult);
        }
      }
    });
  }

  private void doBatchDelete(MTable table, String[] keys) {
    Exception expectedException = null;
    List<Delete> deleteList = new ArrayList<>(keys.length);
    for (int rowId = 0; rowId < keys.length; rowId++) {
      deleteList.add(new Delete(keys[rowId]));
    }
    try {
      table.delete(deleteList);
    } catch (UnsupportedOperationException ex) {
      expectedException = ex;
    }
    assertTrue(expectedException instanceof UnsupportedOperationException);
  }

  private void doVerifyDeleteFromClient(VM vm, String[] keys, String[] keys2) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doDelete(getTableFromClientCache(), keys, keys2);
        return null;
      }
    });
  }

  private void doDelete(MTable table, String[] keys, String[] keys2) {

    // 1. Test Table.delete(null)
    /*
     * Test MTable delete with nullKey Expect IllegalArgumentException.
     */
    Delete delete = null;
    Exception expectedException = null;
    try {
      table.delete(delete);
    } catch (IllegalArgumentException iae) {
      expectedException = iae;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    // 2. Delete Test: Table.delete() with Not existing column name
    delete = new Delete(Bytes.toBytes("001"));
    delete.addColumn(Bytes.toBytes("ID"));
    delete.addColumn(Bytes.toBytes("COLUMN_DOES_NOT_EXISTS"));

    expectedException = null;
    try {
      table.delete(delete);
    } catch (Exception icne) {
      expectedException = icne;
    }
    assertTrue(expectedException instanceof IllegalColumnNameException);

    // 3. Test MTable delete with invalidRowKey
    /*
     * Test fails if some exception occurs. Get same invalidRowKey after get and assert for empty
     * result.
     */
    Delete invalidRowKey = new Delete("invalid row key");
    try {
      table.delete(invalidRowKey);
    } catch (Exception icne) {
      assertTrue(
          icne instanceof RowKeyOutOfRangeException || icne instanceof RowKeyDoesNotExistException);
    }
    Row result = null;
    try {
      result = table.get(new Get("invalid row key"));
    } catch (Exception icne) {
      if (table.getTableDescriptor().getKeySpace() != null) {
        assertTrue(icne instanceof RowKeyOutOfRangeException);
      } else {
        assertTrue(result.isEmpty());
      }
    }

    // 4. Test same row multiple times and verify
    /* No exception, returns silently */
    Delete deleteKeyTwice = new Delete(keys[2]);
    try {
      table.delete(deleteKeyTwice);
      table.delete(deleteKeyTwice);
    } catch (Exception ex) {
      Assert.fail("Deleting same row twice should not throw exception.");
    }

    Get getDeletedKey = new Get(keys[2]);
    try {
      Row row = table.get(getDeletedKey);
      assertTrue(row.isEmpty());
    } catch (Exception ex) {
      Assert.fail("Should not get exception for get after delete.");
    }

    // 5. Test delete selective column from row verify
    /* No exception, returns silently */
    Delete deleteColumn = new Delete(keys[1]);
    deleteColumn.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
    try {
      table.delete(deleteColumn);
    } catch (Exception ex) {
      Assert.fail("Deleting same row twice should not throw exception.");
    }

    // 6. Test delete selective column multiple times from row verify
    /*
     * No exception, returns silently Test fails if exception occurs
     */
    try {
      table.delete(deleteColumn);
    } catch (Exception ex1) {
      Assert.fail("Deleting same row twice should not throw exception.");
    }

    // Insert deleted columns
    Put put = new Put(keys[1]);
    put.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2), Bytes.toBytes(VALUE_PREFIX + 2));
    table.put(put);

    // 7. Test delete selective rows (even) and verify data from odd and even number values.
    for (int rowIndex = 0; rowIndex < 5; rowIndex = rowIndex + 1) {
      if (rowIndex % 2 == 0) {
        Delete delete2 = new Delete(Bytes.toBytes(keys[rowIndex]));
        table.delete(delete2);
      }
    }

    for (int rowIndex = 0; rowIndex < 5; rowIndex = rowIndex + 1) {
      Get get = new Get(Bytes.toBytes(keys[rowIndex]));
      Row result1 = table.get(get);
      if (rowIndex % 2 == 0) {
        assertTrue(result1.isEmpty());
      } else {
        assertFalse(result1.isEmpty());
        int columnIndex = 0;
        List<Cell> row = result1.getCells();
        for (int i = 0; i < row.size() - 1; i++) {
          byte[] expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);
          byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
          if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
            System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
            System.out
                .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
            Assert.fail("Invalid Values for Column Name");
          }
          if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
            System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
            System.out.println(
                "actualValue    => " + Arrays.toString((byte[]) row.get(i).getColumnValue()));
            Assert.fail("Invalid Values for Column Value");
          }
          columnIndex++;
        }
      }
    }

    // 8. Test delete rows with even number of columns on not inserted keys and verify data from
    // rows.
    // Test with random keys Test for GEN-852
    String[] keys3 = {"010", "011", "012", "013", "014"};

    for (int rowIndex = 0; rowIndex < 5; rowIndex = rowIndex + 1) {
      Delete delete2 = new Delete(Bytes.toBytes(keys3[rowIndex]));
      for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex = columnIndex + 1) {
        if (columnIndex % 2 != 0) {
          delete2.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex));
        }
      }
      try {
        table.delete(delete2);
      } catch (Exception ex) {
        assertTrue(ex instanceof RowKeyDoesNotExistException);
      }
    }

    for (int rowIndex = 0; rowIndex < 5; rowIndex = rowIndex + 1) {
      Get get = new Get(Bytes.toBytes(keys3[rowIndex]));
      Row result1 = table.get(get);
      assertTrue(result1.isEmpty());
    }

    for (int rowIndex = 0; rowIndex < 5; rowIndex++) {
      Put record = new Put(Bytes.toBytes(keys2[rowIndex]));
      for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }

    // 9. Test delete selective rows (odd now because those are only left with even number of
    // columns) and verify columns data from odd numbers.
    for (int rowIndex = 0; rowIndex < 5; rowIndex = rowIndex + 1) {
      if (rowIndex % 2 == 0) {
        Delete delete2 = new Delete(Bytes.toBytes(keys2[rowIndex]));
        for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex = columnIndex + 1) {
          if (columnIndex % 2 != 0) {
            delete2.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex));
          }
        }
        try {
          table.delete(delete2);
        } catch (Exception ex2) {
          Assert.fail("Should not throw exception here.");
        }
      }
    }

    for (int rowIndex = 0; rowIndex < 5; rowIndex = rowIndex + 1) {
      Get get = new Get(Bytes.toBytes(keys2[rowIndex]));
      Row result1 = table.get(get);
      if (rowIndex % 2 != 0) {
        assertFalse(result1.isEmpty());
        int columnIndex = 0;
        List<Cell> row = result1.getCells();
        for (int i = 0; i < row.size() - 1; i++) {
          byte[] expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);
          byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
          if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
            System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
            System.out
                .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
            Assert.fail("Invalid Values for Column Name");
          }
          {
            if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
              System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
              System.out.println(
                  "actualValue    => " + Arrays.toString((byte[]) row.get(i).getColumnValue()));
              Assert.fail("Invalid Values for Column Value");
            }
          }
          columnIndex++;
        }
      } else {
        assertFalse(result1.isEmpty());
        int columnIndex = 0;
        List<Cell> row = result1.getCells();
        for (int i = 0; i < row.size() - 1; i++) {
          byte[] expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);
          byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
          if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
            System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
            System.out
                .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
            Assert.fail("Invalid Values for Column Name");
          }
          if (columnIndex % 2 != 0) {
            assertEquals(row.get(i).getColumnValue(), null);
          } else {
            if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
              System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
              System.out.println(
                  "actualValue    => " + Arrays.toString((byte[]) row.get(i).getColumnValue()));
              Assert.fail("Invalid Values for Column Value");
            }
          }
          columnIndex++;
        }
      }
    }

    // 10. Delete selected column in incremental fashion:
    String[] keys_version = {"015", "016", "017", "018", "019", "020"};
    Put put1 = new Put(Bytes.toBytes(keys_version[0]));
    put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 0), Bytes.toBytes(VALUE_PREFIX + 0));
    put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 3), Bytes.toBytes(VALUE_PREFIX + 3));
    table.put(put1);

    Delete delete1 = new Delete(Bytes.toBytes(keys_version[0]));
    delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 0));
    delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 3));
    table.delete(delete1);

    Get get = new Get(Bytes.toBytes(keys_version[0]));
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 0));
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 3));
    Row result1 = table.get(get);
    assertFalse(result1.isEmpty());

    List<Cell> row = result1.getCells();
    int columnIndex = 0;
    assertEquals(row.size(), 2);
    for (int i = 0; i < row.size() - 1; i++) {
      byte[] expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);
      if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
        System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
        System.out.println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
        Assert.fail("Invalid Values for Column Name");
      }
      assertEquals(null, row.get(i).getColumnValue());
      columnIndex = columnIndex + 3;
    }

    put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2), Bytes.toBytes(VALUE_PREFIX + 2));
    table.put(put1);

    delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
    table.delete(delete1);

    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
    result1 = table.get(get);
    assertFalse(result1.isEmpty());

    row = result1.getCells();
    columnIndex = 2;
    assertEquals(null, row.get(columnIndex).getColumnValue());
    assertEquals(row.size(), 3);
    byte[] expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);
    if (!Bytes.equals(expectedColumnName, row.get(columnIndex).getColumnName())) {
      System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
      System.out.println(
          "actualColumnName   => " + Arrays.toString(row.get(columnIndex).getColumnName()));
      Assert.fail("Invalid Values for Column Name");
    }

    // Delete columns with specific timestamp.
    // Test for GEN-875
    long timestamp = 1;
    put1 = new Put(Bytes.toBytes(keys_version[1]));
    put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 0), Bytes.toBytes(VALUE_PREFIX + 0));
    put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 3), Bytes.toBytes(VALUE_PREFIX + 3));
    put1.setTimeStamp(timestamp);
    table.put(put1);

    delete1 = new Delete(Bytes.toBytes(keys_version[1]));
    delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 0));
    delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 3));
    delete1.setTimestamp(timestamp);
    table.delete(delete1);


    get = new Get(Bytes.toBytes(keys_version[1]));
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 0));
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 3));
    get.setTimeStamp(timestamp);
    result1 = table.get(get);
    assertFalse(result1.isEmpty());
    row = result1.getCells();
    columnIndex = 0;
    assertEquals(row.size(), 2);
    for (Cell cell : row) {
      expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);
      if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
        System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
        System.out.println("actualColumnName   => " + Arrays.toString(cell.getColumnName()));
        Assert.fail("Invalid Values for Column Name");
      }
      assertEquals(null, cell.getColumnValue());
      columnIndex = columnIndex + 3;
    }

    if (table.getTableDescriptor().getMaxVersions() > 1) {
      timestamp = 2;
      put1 = new Put(Bytes.toBytes(keys_version[1]));
      put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2), Bytes.toBytes(VALUE_PREFIX + 2));
      put1.setTimeStamp(timestamp);
      table.put(put1);

      delete1 = new Delete(Bytes.toBytes(keys_version[1]));
      delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
      delete1.setTimestamp(timestamp);
      table.delete(delete1);

      get = new Get(Bytes.toBytes(keys_version[1]));
      get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
      get.setTimeStamp(timestamp);
      result1 = table.get(get);
      assertFalse(result1.isEmpty());
      row = result1.getCells();
      assertEquals(row.size(), 1);
      columnIndex = 0;
      assertEquals(null, row.get(columnIndex).getColumnValue());
      expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + 2);
      if (!Bytes.equals(expectedColumnName, row.get(columnIndex).getColumnName())) {
        System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
        System.out.println(
            "actualColumnName   => " + Arrays.toString(row.get(columnIndex).getColumnName()));
        Assert.fail("Invalid Values for Column Name");
      }
    }

    // Test case to delete a row with specific timestamp:
    // It should delete entire row.
    timestamp = 1;
    put1 = new Put(Bytes.toBytes(keys_version[2]));
    for (int i = 0; i < NUM_COLUMNS; i++) {
      put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + i), Bytes.toBytes(VALUE_PREFIX + i));
    }
    put1.setTimeStamp(timestamp);
    table.put(put1);

    delete1 = new Delete(keys_version[2]);
    delete1.setTimestamp(timestamp + 1);
    table.delete(delete1);

    get = new Get(keys_version[2]);
    result1 = table.get(get);
    if (table.getTableDescriptor().getMaxVersions() > 1)
      assertTrue(result1.isEmpty());
    else {
      // after GEN-2113, for single version it will only delete matching versions
      assertFalse(result1.isEmpty());
    }


    // Test with populating all versions until maxVersions not specifying any column explicitly,
    // delete a row with specific version.
    // Test asserts all rows less than equal to given version are deleted.

    int maxVersions = table.getTableDescriptor().getMaxVersions();

    if (maxVersions > 1) {
      for (int i = 0; i < maxVersions; i++) {
        put = new Put(keys_version[3]);
        put.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 0), Bytes.toBytes(VALUE_PREFIX + 0));
        put.setTimeStamp(i + 1);
        table.put(put);
      }

      delete1 = new Delete(keys_version[3]);
      delete1.setTimestamp(3);
      table.delete(delete1);

      for (int i = 0; i < maxVersions; i++) {
        get = new Get(keys_version[3]);
        get.setTimeStamp(i + 1);
        result1 = table.get(get);
        if (i <= 2) {
          assertTrue(result1.isEmpty());
        } else {
          assertFalse(result1.isEmpty());
          assertEquals(result1.size(), 5 + 1);
        }
      }

      // Test rows with columns populating all versions until maxVersions, delete a column with
      // specific version.
      // Test asserts all columns other than specified version deleted are present and that column
      // version is not present.
      for (int i = 0; i < maxVersions; i++) {
        put = new Put(keys_version[4]);
        put.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + i), Bytes.toBytes(VALUE_PREFIX + i));
        put.setTimeStamp(i + 1);
        table.put(put);
      }

      delete1 = new Delete(keys_version[4]);
      delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
      delete1.setTimestamp(2);
      table.delete(delete1);
      get = new Get(keys_version[4]);
      get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
      get.setTimeStamp(2);
      result1 = table.get(get);
      assertEquals(result1.size(), 1);
      assertEquals(result1.getCells().get(0).getColumnValue(), null);
      expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + 2);
      if (!Bytes.equals(expectedColumnName, result1.getCells().get(0).getColumnName())) {
        System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
        System.out.println(
            "actualColumnName   => " + Arrays.toString(result1.getCells().get(0).getColumnName()));
        Assert.fail("Invalid Values for Column Name");
      }


      // Populate a row with all columns with timestamp and test.
      // Test rows with columns populating all versions until maxVersions, delete a column with
      // specific version.
      // Test asserts all columns less than equal to that version are deleted.
      for (int i = 0; i < maxVersions; i++) {
        put = new Put(keys_version[5]);
        for (columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
          put.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        put.setTimeStamp(i + 1);
        table.put(put);
      }

      delete1 = new Delete(keys_version[5]);
      delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 3));
      delete1.setTimestamp(3);
      table.delete(delete1);

      // Assert All of the rows data, only all columns less than equal to timestamp specified are
      // deleted.
      for (int i = 0; i < maxVersions; i++) {
        get = new Get(keys_version[5]);
        get.setTimeStamp(i + 1);
        try {
          result1 = table.get(get);
          assertFalse(result1.isEmpty());
          columnIndex = 0;
          row = result1.getCells();
          for (int k = 0; k < row.size() - 1; k++) {
            expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);
            byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
            if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
              System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
              System.out
                  .println("actualColumnName   => " + Arrays.toString(row.get(k).getColumnName()));
              Assert.fail("Invalid Values for Column Name");
            }
            if (i <= 2) {
              if (columnIndex == 3) {
                assertEquals(row.get(k).getColumnValue(), null);
              }
            } else {
              if (!Bytes.equals(exptectedValue, (byte[]) row.get(k).getColumnValue())) {
                System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
                System.out.println(
                    "actualValue    => " + Arrays.toString((byte[]) row.get(k).getColumnValue()));
                Assert.fail("Invalid Values for Column Value");
              }
            }
            columnIndex++;
          }
        } catch (Exception ex) {
          Assert.fail("Should not get exception here.");
        }
      }
    }
  }

  private void doPut(MTable table, String[] keys) {
    for (int rowIndex = 0; rowIndex < 5; rowIndex++) {
      Put record = new Put(Bytes.toBytes(keys[rowIndex]));
      for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  private void doGet(MTable table, String[] keys) {
    for (int rowIndex = 0; rowIndex < 5; rowIndex++) {
      Get record = new Get(Bytes.toBytes(keys[rowIndex]));
      for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex));
      }
      Row result = table.get(record);
      System.out.println("result = " + result);
      assertTrue(result.isEmpty());
    }
  }
}
