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

import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.*;
import org.junit.Assert;

public class MTableDeleteCoprocessor extends MCoprocessor {
  private static final int NUM_COLUMNS = 5;
  public static String COLUMNNAME_PREFIX = "COL", VALUE_PREFIX = "VAL";

  public MTableDeleteCoprocessor() {}

  public boolean deleteRows(MCoprocessorContext context) {
    System.out.println("MTableDeleteCoprocessor.deleteRows");
    MExecutionRequest request = context.getRequest();
    MTable table = context.getTable();
    String[] keys = {"021", "022", "023", "024", "025"};
    for (int rowIndex = 0; rowIndex < 5; rowIndex++) {
      Delete record = new Delete(Bytes.toBytes(keys[rowIndex]));
      for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex));
      }
      try {
        table.delete(record);
      } catch (Exception e) {
        // e.printStackTrace();
      }
    }
    return true;


  }

  public void doPut(MTable table, String[] keys) {
    for (int rowIndex = 0; rowIndex < 5; rowIndex++) {
      Put record = new Put(Bytes.toBytes(keys[rowIndex]));
      for (int columnIndex = 0; columnIndex < NUM_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  public boolean doDelete(MCoprocessorContext context) {
    System.out.println("MTableDeleteCoprocessor.doDelete");
    MExecutionRequest request = context.getRequest();
    MTable table = context.getTable();
    String[] keys = {"005", "006", "007", "008", "009"};
    String[] keys2 = {"000", "001", "002", "003", "004"};
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
    Assert.assertTrue(expectedException instanceof IllegalArgumentException);

    System.out.println("MTableDeleteCoprocessor.doDelete 1");

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
    Assert.assertTrue(expectedException instanceof IllegalColumnNameException);

    System.out.println("MTableDeleteCoprocessor.doDelete 2");

    // 3. Test MTable delete with invalidRowKey
    /*
     * Test fails if some exception occurs. Get same invalidRowKey after get and assert for empty
     * result.
     */
    Delete invalidRowKey = new Delete("invalid row key");
    try {
      table.delete(invalidRowKey);
    } catch (Exception icne) {
      if (table.getTableDescriptor().getStartRangeKey() != null
          || table.getTableDescriptor().getStopRangeKey() != null
          || table.getTableDescriptor().getKeySpace() != null) {
        /** for universal key-space this key may fall in some valid range **/
        Assert.assertTrue((icne instanceof RowKeyOutOfRangeException
            || icne instanceof RowKeyDoesNotExistException));
      } else {
        Assert.assertTrue(icne instanceof RowKeyDoesNotExistException);
      }
    }
    System.out.println("MTableDeleteCoprocessor.doDelete 3");
    Row result = null;
    try {
      result = table.get(new Get("invalid row key"));
    } catch (Exception icne) {
      if (table.getTableDescriptor().getKeySpace() != null) {
        Assert.assertTrue(icne instanceof RowKeyOutOfRangeException);
      } else {
        Assert.assertTrue(result.isEmpty());
      }
    }

    System.out.println("MTableDeleteCoprocessor.doDelete 3.1");

    // 4. Test same row multiple times and verify
    Delete deleteKeyTwice = new Delete(keys[2]);
    try {
      table.delete(deleteKeyTwice);
      table.delete(deleteKeyTwice);
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof RowKeyDoesNotExistException);
      // Assert.fail("Deleting same row twice should not throw exception.");
    }
    System.out.println("MTableDeleteCoprocessor.doDelete 4");

    Get getDeletedKey = new Get(keys[2]);
    try {
      Row row = table.get(getDeletedKey);
      Assert.assertTrue(row.isEmpty());
    } catch (Exception ex) {
      Assert.fail("Should not get exception for get after delete.");
    }
    System.out.println("MTableDeleteCoprocessor.doDelete 4.1");

    // 5. Test delete selective column from row verify
    Delete deleteColumn = new Delete(keys[1]);
    deleteColumn.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
    try {
      table.delete(deleteColumn);
    } catch (Exception ex) {
      Assert.fail("No exception expected.");
    }
    System.out.println("MTableDeleteCoprocessor.doDelete 5");

    // 6. Test delete selective column multiple times from row verify
    /*
     * No exception, returns silently Test fails if exception occurs
     */
    try {
      table.delete(deleteColumn);
    } catch (Exception ex1) {
      Assert.assertTrue(ex1 instanceof RowKeyDoesNotExistException);
    }
    System.out.println("MTableDeleteCoprocessor.doDelete 6");

    // Insert deleted columns
    Put put = new Put(keys[1]);
    put.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2), Bytes.toBytes(VALUE_PREFIX + 2));
    table.put(put);

    // 7. Test delete selective rows (even) and verify data from odd and even number values.
    for (int rowIndex = 0; rowIndex < 5; rowIndex = rowIndex + 1) {
      if (rowIndex % 2 == 0) {
        Delete delete2 = new Delete(Bytes.toBytes(keys[rowIndex]));
        try {
          table.delete(delete2);
        } catch (Exception e) {
          // e.printStackTrace();
        }
      }
    }

    for (int rowIndex = 0; rowIndex < 5; rowIndex = rowIndex + 1) {
      Get get = new Get(Bytes.toBytes(keys[rowIndex]));
      Row result1 = table.get(get);
      if (rowIndex % 2 == 0) {
        Assert.assertTrue(result1.isEmpty());
      } else {
        Assert.assertFalse(result1.isEmpty());
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

    System.out.println("MTableDeleteCoprocessor.doDelete 7");
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
        Assert.assertTrue(ex instanceof RowKeyDoesNotExistException);
      }
    }

    for (int rowIndex = 0; rowIndex < 5; rowIndex = rowIndex + 1) {
      Get get = new Get(Bytes.toBytes(keys3[rowIndex]));
      Row result1 = table.get(get);
      Assert.assertTrue(result1.isEmpty());
    }
    System.out.println("MTableDeleteCoprocessor.doDelete 8");

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
        Assert.assertFalse(result1.isEmpty());
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
        Assert.assertFalse(result1.isEmpty());
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
            Assert.assertEquals(row.get(i).getColumnValue(), null);
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
    System.out.println("MTableDeleteCoprocessor.doDelete 9");

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
    Assert.assertFalse(result1.isEmpty());

    List<Cell> row = result1.getCells();
    int columnIndex = 0;
    Assert.assertEquals(row.size(), 2);
    for (int i = 0; i < row.size() - 1; i++) {
      byte[] expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);
      if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
        System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
        System.out.println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
        Assert.fail("Invalid Values for Column Name");
      }
      Assert.assertEquals(null, row.get(i).getColumnValue());
      columnIndex = columnIndex + 3;
    }

    put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2), Bytes.toBytes(VALUE_PREFIX + 2));
    table.put(put1);

    delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
    table.delete(delete1);

    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
    result1 = table.get(get);
    Assert.assertFalse(result1.isEmpty());

    row = result1.getCells();
    columnIndex = 2;
    Assert.assertEquals(null, row.get(columnIndex).getColumnValue());
    Assert.assertEquals(row.size(), 3);
    byte[] expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);
    if (!Bytes.equals(expectedColumnName, row.get(columnIndex).getColumnName())) {
      System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
      System.out.println(
          "actualColumnName   => " + Arrays.toString(row.get(columnIndex).getColumnName()));
      Assert.fail("Invalid Values for Column Name");
    }
    System.out.println("MTableDeleteCoprocessor.doDelete 10");

    // Delete columns with specific timestamp.
    // Test for GEN-875
    long timestamp = 1;
    put1 = new Put(Bytes.toBytes(keys_version[1]));
    put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 0), Bytes.toBytes(VALUE_PREFIX + 0));
    put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 3), Bytes.toBytes(VALUE_PREFIX + 3));
    put1.setTimeStamp(timestamp);
    try {
      table.put(put1);
    } catch (Exception ex) {
      expectedException = ex;
      if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
        Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
      } else {
        Assert.fail("Should not get exception for ORDERED Table");
      }
    }

    if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
      delete1 = new Delete(Bytes.toBytes(keys_version[1]));
      delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 0));
      delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 3));
      delete1.setTimestamp(timestamp);
      table.delete(delete1);
    }

    get = new Get(Bytes.toBytes(keys_version[1]));
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 0));
    get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 3));
    get.setTimeStamp(timestamp);
    try {
      result1 = table.get(get);
      Assert.assertFalse(result1.isEmpty());
      row = result1.getCells();
      columnIndex = 0;
      Assert.assertEquals(row.size(), 2);
      for (Cell cell : row) {
        expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);
        if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out.println("actualColumnName   => " + Arrays.toString(cell.getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        Assert.assertEquals(null, cell.getColumnValue());
        columnIndex = columnIndex + 3;
      }
    } catch (Exception ex) {
      expectedException = ex;
      if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
        Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
      } else {
        Assert.fail("Should not get exception for ORDERED Table");
      }
    }
    System.out.println("MTableDeleteCoprocessor.doDelete 11");

    if (table.getTableDescriptor().getMaxVersions() > 1) {
      timestamp = 2;
      put1 = new Put(Bytes.toBytes(keys_version[1]));
      put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2), Bytes.toBytes(VALUE_PREFIX + 2));
      put1.setTimeStamp(timestamp);
      try {
        table.put(put1);
      } catch (Exception ex) {
        expectedException = ex;
        if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
          Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
        } else {
          Assert.fail("Should not get exception for ORDERED Table");
        }
      }

      if (table.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        delete1 = new Delete(Bytes.toBytes(keys_version[1]));
        delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
        delete1.setTimestamp(timestamp);
        table.delete(delete1);
      }

      get = new Get(Bytes.toBytes(keys_version[1]));
      get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
      get.setTimeStamp(timestamp);
      try {
        result1 = table.get(get);
        Assert.assertFalse(result1.isEmpty());
        row = result1.getCells();
        Assert.assertEquals(row.size(), 1);
        columnIndex = 0;
        Assert.assertEquals(null, row.get(columnIndex).getColumnValue());
        expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + 2);
        if (!Bytes.equals(expectedColumnName, row.get(columnIndex).getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out.println(
              "actualColumnName   => " + Arrays.toString(row.get(columnIndex).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
      } catch (Exception ex) {
        expectedException = ex;
        if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
          Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
        } else {
          Assert.fail("Should not get exception for ORDERED Table");
        }
      }
    }
    System.out.println("MTableDeleteCoprocessor.doDelete 12");

    // Test case to delete a row with specific timestamp:
    // It should delete entire row.
    timestamp = 1;
    put1 = new Put(Bytes.toBytes(keys_version[2]));
    for (int i = 0; i < NUM_COLUMNS; i++) {
      put1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + i), Bytes.toBytes(VALUE_PREFIX + i));
    }
    put1.setTimeStamp(timestamp);
    try {
      table.put(put1);
    } catch (Exception ex) {
      expectedException = ex;
      if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
        Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
      } else {
        Assert.fail("Should not get exception here.");
      }
    }

    delete1 = new Delete(keys_version[2]);
    delete1.setTimestamp(timestamp + 1);
    try {
      table.delete(delete1);
    } catch (Exception ex) {
      expectedException = ex;
      if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
        Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
      } else {
        Assert.assertTrue(expectedException instanceof MCheckOperationFailException);
      }
    }

    System.out.println("MTableDeleteCoprocessor.doDelete 13");

    // GEN-872 - Test for checkAndDelete Api with timestamp with UNORDERED table to assert
    // TableInvalidConfiguration.
    if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
      delete1 = new Delete(keys_version[2]);
      delete1.setTimestamp(timestamp + 1);
      try {
        table.checkAndDelete(Bytes.toBytes(keys_version[2]), null, null, delete1);
      } catch (Exception ex) {
        ex.printStackTrace();
        expectedException = ex;
      }
      Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
    }

    System.out.println("MTableDeleteCoprocessor.doDelete 13.1");

    // Test with populating all versions until maxVersions not specifying any column explicitly,
    // delete a row with specific version.
    // Test asserts all rows less than equal to given version are deleted.

    int maxVersions = table.getTableDescriptor().getMaxVersions();

    if (maxVersions > 1) {
      for (int i = 0; i < maxVersions; i++) {
        put = new Put(keys_version[3]);
        put.setTimeStamp(i + 1);
        try {
          table.put(put);
        } catch (Exception ex) {
          Assert.assertTrue(ex instanceof IllegalArgumentException);
        }
      }

      System.out.println("MTableDeleteCoprocessor.doDelete 13.99");
      delete1 = new Delete(keys_version[3]);
      delete1.setTimestamp(3);
      try {
        table.delete(delete1);
      } catch (Exception ex) {
        Assert.assertTrue(ex instanceof RowKeyDoesNotExistException);
      }

      System.out.println("MTableDeleteCoprocessor.doDelete 14");
      // Test rows with columns populating all versions until maxVersions, delete a column with
      // specific version.
      // Test asserts all columns other than specified version deleted are present and that column
      // version is not present.
      for (int i = 0; i < maxVersions; i++) {
        put = new Put(keys_version[4]);
        put.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + i), Bytes.toBytes(VALUE_PREFIX + i));
        put.setTimeStamp(i + 1);
        try {
          table.put(put);
        } catch (Exception ex) {
          expectedException = ex;
          if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
            Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
          } else {
            Assert.fail("Should not get exception here.");
          }
        }
      }

      delete1 = new Delete(keys_version[4]);
      delete1.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
      delete1.setTimestamp(2);
      table.delete(delete1);

      try {
        // Dont set timestamp and column and expect latest get with only last column.
        get = new Get(keys_version[4]);
        get.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + 2));
        get.setTimeStamp(2);
        result1 = table.get(get);
        Assert.assertEquals(result1.size(), 1);
        Assert.assertEquals(result1.getCells().get(0).getColumnValue(), null);
        expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + 2);
        if (!Bytes.equals(expectedColumnName, result1.getCells().get(0).getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out.println("actualColumnName   => "
              + Arrays.toString(result1.getCells().get(0).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
      } catch (Exception ex) {
        expectedException = ex;
        if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
          Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
        } else {
          Assert.fail("Should not get exception here.");
        }
      }
      System.out.println("MTableDeleteCoprocessor.doDelete 15");

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
        try {
          table.put(put);
        } catch (Exception ex) {
          expectedException = ex;
          if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
            Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
          } else {
            Assert.fail("Should not get exception here.");
          }
        }
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
          Assert.assertFalse(result1.isEmpty());
          columnIndex = 0;
          row = result1.getCells();
          for (int j = 0; j < row.size() - 1; j++) {
            expectedColumnName = Bytes.toBytes(COLUMNNAME_PREFIX + columnIndex);
            byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
            if (!Bytes.equals(expectedColumnName, row.get(j).getColumnName())) {
              System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
              System.out
                  .println("actualColumnName   => " + Arrays.toString(row.get(j).getColumnName()));
              Assert.fail("Invalid Values for Column Name");
            }
            if (i <= 2) {
              if (columnIndex == 3) {
                Assert.assertEquals(row.get(j).getColumnValue(), null);
              }
            } else {
              if (!Bytes.equals(exptectedValue, (byte[]) row.get(j).getColumnValue())) {
                System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
                System.out.println(
                    "actualValue    => " + Arrays.toString((byte[]) row.get(j).getColumnValue()));
                Assert.fail("Invalid Values for Column Value");
              }
            }
            columnIndex++;
          }
        } catch (Exception ex) {
          expectedException = ex;
          if (table.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
            Assert.assertTrue(expectedException instanceof TableInvalidConfiguration);
          } else {
            Assert.fail("Should not get exception here.");
          }
        }
      }
    }
    System.out.println("MTableDeleteCoprocessor.doDelete 16");
    return true;
  }

}
