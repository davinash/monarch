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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.IllegalColumnNameException;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

@Category(MonarchTest.class)
public class MTableAllOpsWithColumnsDUnitTest extends MTableDUnitHelper {
  private static final int NUM_OF_RECORDS = 10;
  private static final String KEY_PREFIX = "RowKey";
  private static final String COLUMN_NAME_PREFIX = "COLUMN-";

  public MTableAllOpsWithColumnsDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  private static final String TABLE_NAME = "TABLE_WITH_COLUMN";

  private class ColumnTypesValues implements Serializable {
    public DataType columnType;
    public Object validValue;
    // public Object inValidValue;

    public ColumnTypesValues(DataType type, Object validValue/* , Object inValidValue */) {
      this.columnType = type;
      this.validValue = validValue;
      // this.inValidValue = inValidValue;
    }

  }

  ColumnTypesValues[] testData = new ColumnTypesValues[] {
      new ColumnTypesValues(BasicTypes.INT, 100), new ColumnTypesValues(BasicTypes.LONG, 1000L),
      new ColumnTypesValues(BasicTypes.STRING, "ABC"),};

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

  private void createTable(final boolean ordered) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setRedundantCopies(1);

    for (int i = 0; i < testData.length; i++) {
      System.out.println("Adding data type: " + testData[i].columnType);
      tableDescriptor.addColumn(COLUMN_NAME_PREFIX + testData[i].columnType,
          new MTableColumnType(testData[i].columnType));
    }
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
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
  public void testCreateTableWithEmptyOrNullColumnName() {
    createTableWithEmptyOrNullColumnName();
  }

  private void createTableWithEmptyOrNullColumnName() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    Exception e = null;
    try {
      tableDescriptor.addColumn("", new MTableColumnType(BasicTypes.INT));

    } catch (IllegalColumnNameException icne) {
      e = icne;
    }
    assertTrue(e instanceof IllegalColumnNameException);
    e = null;

    try {
      String columnName = null;
      tableDescriptor.addColumn(columnName, new MTableColumnType(BasicTypes.INT));
    } catch (IllegalColumnNameException icne) {
      e = icne;
    }
    assertTrue(e instanceof IllegalColumnNameException);
  }

  @Test
  public void testCreateTableWithMaximumColumnNameLength() {
    createTableWithMaximumColumnNameLength();
  }

  public void createTableWithMaximumColumnNameLength() {
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    Exception e = null;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      sb.append('X');
    }
    try {
      tableDescriptor.addColumn(sb.toString(), new MTableColumnType(BasicTypes.INT));
    } catch (IllegalColumnNameException icne) {
      e = icne;
    }
    assertTrue(e instanceof IllegalColumnNameException);
  }

  @Test
  public void testTableColumnType() {
    columnTypesTest();
  }

  @Test
  public void testTableColumnTypeWithClass() {
    // columnTypesTest(true);
  }

  private void columnTypesTest() {
    createTableOn(this.client1, true);
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);
    List<MColumnDescriptor> listOfColumns = table.getTableDescriptor().getAllColumnDescriptors();
    int i = 0;
    for (int j = 0; j < listOfColumns.size() - 1; j++) {
      assertEquals(testData[i++].columnType.toString(),
          listOfColumns.get(j).getColumnType().toString());
    }
  }

  @Test
  public void testTableColumnTypeFromMetaDataFromClient() {
    tableColumnTypeFromMetaDataFromClientTest();
  }

  private void tableColumnTypeFromMetaDataFromClientTest() {
    createTableOn(this.client1, true);
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);
    /* Read the MTableDescriptor From Meta Data */
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);

    MTableDescriptor tableDescriptor = clientCache.getMTableDescriptor(TABLE_NAME);
    assertNotNull(tableDescriptor);

    List<MColumnDescriptor> listOfColumns = tableDescriptor.getAllColumnDescriptors();
    int i = 0;
    for (int j = 0; j < listOfColumns.size() - 1; j++) {
      assertEquals(testData[i++].columnType.toString(),
          listOfColumns.get(j).getColumnType().toString());
    }
  }

  public void tableColumnTypeFromMetaDataFromServer(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);

        MTableDescriptor tableDescriptor = cache.getMTableDescriptor(TABLE_NAME);
        assertNotNull(tableDescriptor);

        List<MColumnDescriptor> listOfColumns = tableDescriptor.getAllColumnDescriptors();
        int i = 0;
        for (int j = 0; j < listOfColumns.size() - 1; j++) {
          assertEquals(testData[i++].columnType.toString(),
              listOfColumns.get(j).getColumnType().toString());
        }
        return null;
      }
    });
  }

  @Test
  public void testTableColumnTypeFromMetaDataFromServer() {
    tableColumnTypeFromMetaDataFromServerTest();
  }

  public void tableColumnTypeFromMetaDataFromServerTest() {
    createTableOn(this.client1, true);

    tableColumnTypeFromMetaDataFromServer(this.vm0);
    tableColumnTypeFromMetaDataFromServer(this.vm1);
    tableColumnTypeFromMetaDataFromServer(this.vm2);
  }

  private void verifyGetsOn(final boolean ordered) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int j = 0; j < NUM_OF_RECORDS; j++) {
      Get get = new Get(KEY_PREFIX + j);
      Row result = table.get(get);
      assertNotNull(result);
      List<Cell> row = result.getCells();
      int k = 0;
      for (int i = 0; i < row.size() - 1; i++) {
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + testData[k].columnType);

        byte[] actualColumnName = row.get(i).getColumnName();

        if (!Bytes.equals(expectedColumnName, actualColumnName)) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out
              .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }

        Object exptectedValue = testData[k].validValue;
        if (testData[k].columnType.equals(BasicTypes.LONG)) {
          assertEquals((long) exptectedValue, (long) row.get(i).getColumnValue());
        } else if (testData[k].columnType.equals(BasicTypes.STRING)) {
          assertEquals((String) exptectedValue, (String) row.get(i).getColumnValue());
        } else if (testData[k].columnType.equals(BasicTypes.INT)) {
          assertEquals((int) exptectedValue, (int) row.get(i).getColumnValue());
        } else if (testData[k].columnType.equals(BasicTypes.SHORT)) {
          assertEquals((short) exptectedValue, (short) row.get(i).getColumnValue());
        } else if (testData[k].columnType.equals(BasicTypes.BOOLEAN)) {
          assertEquals((boolean) exptectedValue, (boolean) row.get(i).getColumnValue());
        } else if (testData[k].columnType.equals(BasicTypes.CHAR)) {
          assertEquals((char) exptectedValue, (char) row.get(i).getColumnValue());
        } else if (testData[k].columnType.equals(BasicTypes.DATE)) {
          assertEquals((Date) exptectedValue, (Date) row.get(i).getColumnValue());
        } else if (testData[k].columnType.equals(BasicTypes.TIMESTAMP)) {
          assertEquals((Timestamp) exptectedValue, (Timestamp) row.get(i).getColumnValue());
        }
        k++;
      }
    }
  }

  private void verifyGetsOn(VM vm, final boolean ordered) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyGetsOn(ordered);
        return null;
      }
    });
  }

  @Test
  public void testTablePutGetsWithColumnType() {
    tablePutGetsWithColumnType();
  }


  public void tablePutGetsWithColumnType() {
    createTableOn(this.client1, true);
    /* Create Put Records */
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int j = 0; j < NUM_OF_RECORDS; j++) {
      Put putRecord = new Put(KEY_PREFIX + j);
      for (int i = 0; i < testData.length; i++) {
        putRecord.addColumn(COLUMN_NAME_PREFIX + testData[i].columnType, testData[i].validValue);
      }
      table.put(putRecord);
    }
    verifyGetsOn(this.client1, true);
  }

  @Test
  public void testTableColumnTypeValidInvalidValues() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    for (int i = 0; i < testData.length; i++) {
      MTableDescriptor tableDescriptor = new MTableDescriptor();
      tableDescriptor.addColumn("COLUMN_PREFIX_" + testData[i].columnType,
          new MTableColumnType(testData[i].columnType));

      Admin admin = clientCache.getAdmin();
      MTable table = admin.createTable("TABLE_PREFIX_" + testData[i].columnType, tableDescriptor);

      Put putRecord = new Put("ROW_KEY1");
      putRecord.addColumn("COLUMN_PREFIX_" + testData[i].columnType, testData[i].validValue);

      table.put(putRecord);

      Get getRecord = new Get("ROW_KEY1");
      Row result = table.get(getRecord);
      assertFalse(result.isEmpty());

      List<Cell> cells = result.getCells();
      assertNotNull(cells);
      assertTrue(cells.size() == 1 + 1);

    }
  }
}
