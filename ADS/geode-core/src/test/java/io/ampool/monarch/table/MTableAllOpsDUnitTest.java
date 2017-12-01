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

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.*;
import io.ampool.monarch.table.internal.MTableRow;

import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;
import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTableAllOpsDUnitTest extends MTableDUnitHelper {

  public MTableAllOpsDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "MTableAllOpsDUnitTest";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int LATEST_TIMESTAMP = 300;
  protected final int MAX_VERSIONS = 5;
  protected final int TABLE_MAX_VERSIONS = 7;


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

    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            try {
              if (!MCacheFactory.getAnyInstance().isClosed())
                MCacheFactory.getAnyInstance().close();
            } catch (CacheClosedException exception) {
              logger.warn(exception);
            }
            return null;
          }

        }));

    super.tearDown2();
  }


  private void doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
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

  protected void createTable() {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1).setMaxVersions(5);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  public void createTable(final boolean ordered) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setMaxVersions(5);
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  protected void createTableOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable();
        return null;
      }
    });
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


  private void doGets() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
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

      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10 + 1, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
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

  private void doDeletes() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Delete delete = new Delete(Bytes.toBytes(KEY_PREFIX + rowIndex));
      table.delete(delete);
    }
  }

  private void doDeletesFrom(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doDeletes();
        return null;
      }
    });
  }

  private void verifyDeletes() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertTrue(result.isEmpty());
    }
  }

  private void doPartialGets() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));

      int getResultSize = 0;
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex = columnIndex + 2) {
        get.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex));
        getResultSize++;
      }

      Row result = table.get(get);
      assertFalse(result.isEmpty());
      assertEquals(getResultSize, result.size());

      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex = columnIndex + 2;
      }
    }
  }

  private void doPartialPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex = columnIndex + 2) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  private void doPartialPutFrom(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPartialPuts();
        return null;
      }
    });
  }

  private void verifyAllColumnGetsForRecordsWithPartialPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          if (columnIndex % 2 != 0) {
            assertNull(row.get(i).getColumnValue());
          } else {
            Assert.fail("Invalid Values for Column Value");
          }
        }
        columnIndex = columnIndex + 1;
      }
    }
  }


  private void doPutsWithVersion() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0), Bytes.toBytes(VALUE_PREFIX + 0));
      table.put(record);

      Put record1 = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      record1.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 3), Bytes.toBytes(VALUE_PREFIX + 3));
      table.put(record1);

      Put record2 = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      record2.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 5), Bytes.toBytes(VALUE_PREFIX + 5));
      table.put(record2);
    }
  }

  private void doPutsWithVersionFrom(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPutsWithVersion();
        return null;
      }
    });
  }

  private void verifyVersionedGets() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          if ((columnIndex % 1 != 0) || (columnIndex % 2 != 0) || (columnIndex % 4 != 0)
              || (columnIndex % 6 != 0) || (columnIndex % 7 != 0) || (columnIndex % 8 != 0)
              || (columnIndex % 9 != 0)) {
            assertNull(row.get(i).getColumnValue());
          } else {
            Assert.fail("Invalid Values for Column Value");
          }
        }
        columnIndex = columnIndex + 1;
      }
    }
  }


  private void doPutsWithTimeStampFrom(VM vm, final boolean ordered) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
          {
            Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
            if (ordered) {
              record.setTimeStamp(100L);
            }
            for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
              record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                  Bytes.toBytes(VALUE_PREFIX + 100));
            }
            table.put(record);
          }
          {
            Put record1 = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
            if (ordered) {
              record1.setTimeStamp(200L);
            }
            for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
              record1.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                  Bytes.toBytes(VALUE_PREFIX + 200));
            }
            table.put(record1);
          }
          {
            Put record2 = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
            if (ordered) {
              record2.setTimeStamp(300L);
            }
            for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
              record2.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                  Bytes.toBytes(VALUE_PREFIX + 300));
            }
            table.put(record2);
          }
        }
        return null;
      }
    });
  }

  private void verifyTimeStampGets(int timeStamp, final boolean ordered) {
    timeStamp = timeStamp != 0 ? timeStamp : LATEST_TIMESTAMP;
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      if (timeStamp != LATEST_TIMESTAMP) {
        if (ordered) {
          get.setTimeStamp(timeStamp);
        }
      }
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + timeStamp);
        if (ordered == false) {
          exptectedValue = Bytes.toBytes(VALUE_PREFIX + LATEST_TIMESTAMP);
        }

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
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


  private void doFrequentColumnPuts(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {

          int columnIndex = 0;
          for (int version = 0; version < MAX_VERSIONS; version++) {
            Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
            record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                Bytes.toBytes(VALUE_PREFIX + columnIndex));
            columnIndex = columnIndex + 1;
            record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                Bytes.toBytes(VALUE_PREFIX + columnIndex));
            columnIndex = columnIndex + 1;
            table.put(record);
          }
        }
        return null;
      }
    });
  }

  private void doFrequentColumnPutsWithColumnOverlap(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {

          int columnIndex = 0;
          boolean updateValue = false;
          for (int version = 0; version < MAX_VERSIONS; version++) {
            Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
            {
              if (updateValue) {
                record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                    Bytes.toBytes(VALUE_PREFIX + columnIndex + columnIndex));
                updateValue = false;
              } else {
                record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                    Bytes.toBytes(VALUE_PREFIX + columnIndex));
              }
            }
            {
              columnIndex = columnIndex + 1;
              record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                  Bytes.toBytes(VALUE_PREFIX + columnIndex));
              columnIndex = columnIndex + 1;
            }

            {
              if (columnIndex < 10) {
                record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
                    Bytes.toBytes(VALUE_PREFIX + columnIndex));
              }
            }
            updateValue = true;

            table.put(record);
          }
        }
        return null;
      }
    });
  }

  private void doGetsForFrequentUpdates() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (columnIndex == 2 || columnIndex == 4 || columnIndex == 6 || columnIndex == 8) {
          exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex + columnIndex);
        }
        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
    }
  }


  public void createTableOnWithMaxVerions(final boolean ordered) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setMaxVersions(5);

    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(TABLE_NAME, table.getName());
    assertNotNull(table);
  }

  private void createTableOnWithMaxVerionsOn(VM vm, final boolean ordered) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTableOnWithMaxVerions(ordered);
        return null;
      }
    });
  }

  private void doSomePuts(final boolean ordered) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int versions = 0; versions < TABLE_MAX_VERSIONS; versions++) {
        if (ordered) {
          record.setTimeStamp((versions + 1) * 100);
        }
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + versions));
        }
        table.put(record);
      }
    }
  }

  private void doTestGetColumnValueOnVM(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        byte[] value = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, /* TimeStamp */
            0, 0, 0, 6, /* Column1 Length */
            0, 0, 0, 6, /* Column2 Length */
            0, 0, 0, 6, /* Column3 Length */
            0, 0, 0, 6, /* Column4 Length */
            0, 0, 0, 6, /* Column5 Length */
            0, 0, 0, 6, /* Column6 Length */
            0, 0, 0, 6, /* Column7 Length */
            0, 0, 0, 6, /* Column8 Length */
            0, 0, 0, 6, /* Column9 Length */
            0, 0, 0, 6, /* Column10 Length */
            0, 0, 0, 6, /* KEY */
            86, 65, 76, 85, 69, 48, /* Column1 Value */
            86, 65, 76, 85, 69, 49, /* Column2 Value */
            86, 65, 76, 85, 69, 50, /* Column3 Value */
            86, 65, 76, 85, 69, 51, /* Column4 Value */
            86, 65, 76, 85, 69, 52, /* Column5 Value */
            86, 65, 76, 85, 69, 53, /* Column6 Value */
            86, 65, 76, 85, 69, 54, /* Column7 Value */
            86, 65, 76, 85, 69, 55, /* Column8 Value */
            86, 65, 76, 85, 69, 56, /* Column9 Value */
            86, 65, 76, 85, 69, 57, /* Column10 Value */
            86, 65, 76, 85, 69, 58};

        MTableDescriptor tableDescriptor =
            MCacheFactory.getAnyInstance().getMTableDescriptor(TABLE_NAME);
        assertNotNull(tableDescriptor);

        Map<MColumnDescriptor, byte[]> resultExpected =
            new LinkedHashMap<MColumnDescriptor, byte[]>();
        for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS + 1; colmnIndex++) {
          byte[] expectedValue = new byte[6];
          System.arraycopy(value, 52 + (colmnIndex * 6), expectedValue, 0, 6);

          resultExpected.put(tableDescriptor.getAllColumnDescriptors().get(colmnIndex),
              expectedValue);
        }

        MTableRow mcvo = new MTableRow(value, true, 11);

        Map<MColumnDescriptor, Object> result = mcvo.getColumnValue(tableDescriptor);
        if (result == null) {
          Assert.fail("Result got is null");
        }
        int columnIndex = 0;
        Iterator entries = resultExpected.entrySet().iterator();

        Iterator resultIterator = result.entrySet().iterator();
        for (int i = 0; i < result.entrySet().size() - 1; i++) {
          Map.Entry<MColumnDescriptor, Object> column =
              (Map.Entry<MColumnDescriptor, Object>) resultIterator.next();
          // for (Map.Entry<MColumnDescriptor, Object> column : result.entrySet()) {

          if (columnIndex > 10) {
            Assert.fail("Something is Wrong !!!");
          }
          Map.Entry<MColumnDescriptor, byte[]> thisEntry =
              (Map.Entry<MColumnDescriptor, byte[]>) entries.next();

          byte[] actualColumnName = column.getKey().getColumnName();
          byte[] expectedColumnName = thisEntry.getKey().getColumnName();

          if (!Bytes.equals(actualColumnName, expectedColumnName)) {
            System.out.println("ACTUAL   => " + Arrays.toString(actualColumnName));
            System.out.println("EXPECTED => " + Arrays.toString(expectedColumnName));
            Assert.fail("getColumnAt failed");
          }

          byte[] actualValue = (byte[]) column.getValue();
          byte[] expectedValue = resultExpected.get(column.getKey());

          if (!Bytes.equals(actualValue, expectedValue)) {
            System.out.println("ACTUAL   => " + Arrays.toString(actualValue));
            System.out.println("EXPECTED => " + Arrays.toString(expectedValue));
            Assert.fail("getColumnAt failed");
          }

          columnIndex++;
        }
        return null;
      }
    });
  }

  private void verifyGetsWithMaxVersionsOn(VM vm, final boolean ordered) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {

        MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
          Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
          for (int versions = 0; versions < TABLE_MAX_VERSIONS; versions++) {
            if (ordered) {
              get.setTimeStamp((versions + 1) * 100);
            }
            Row result = table.get(get);
            if ((versions + 1) * 100 == 100 || (versions + 1) * 100 == 200) {
              /* Verifying that old version entries are not fetched */
              if (ordered) {
                assertTrue(result.isEmpty());
              } else {
                assertFalse(result.isEmpty());
              }
              continue;
            }

            assertNotNull(result);
            int columnIndex = 0;
            List<Cell> row = result.getCells();
            for (int i = 0; i < row.size() - 1; i++) {
              Assert.assertNotEquals(10, columnIndex);
              byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
              byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + versions);

              if (ordered == false) {
                exptectedValue = Bytes.toBytes(VALUE_PREFIX + (TABLE_MAX_VERSIONS - 1));
              }


              if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
                System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
                System.out.println(
                    "actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
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
        return null;
      }
    });
  }

  private void putsWithMaxVersionSpecified(final boolean ordered) {

    createTableOnWithMaxVerionsOn(this.client1, ordered);
    doSomePuts(ordered);
    verifyGetsWithMaxVersionsOn(this.client1, ordered);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    clientCache.getAdmin().deleteTable(TABLE_NAME);

  }

  @Test
  public void testPutsWithMaxVersionSpecified() {
    putsWithMaxVersionSpecified(true);
    putsWithMaxVersionSpecified(false);
  }

  private void putFromOneClientAndGetFromAnother(final boolean ordered) {
    createTableOn(this.client1, ordered);
    // doPutFrom(this.client1);
    doPuts();
    doGets();
    doTestGetColumnValueOnVM(this.vm0);
    doTestGetColumnValueOnVM(this.vm1);
    doTestGetColumnValueOnVM(this.vm2);
    doPartialColumnDeletes();
    doPartialColumnGets();
    doPutFrom(this.client1);
    doPartialRowDeletes(this.client1);
    doPartialRowGets(this.vm2);
    doDeletesFrom(this.client1);
    verifyDeletes();
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testPutFromOneClientAndGetFromAnother() {
    putFromOneClientAndGetFromAnother(true);
    putFromOneClientAndGetFromAnother(false);
  }

  private void putAllColumnsGetPartialColumns(final boolean ordered) {
    createTableOn(this.client1, ordered);
    doPutFrom(this.client1);
    doPartialGets();
    doPartialColumnDeletes();
    doPartialColumnGets();
    doPartialColumnDeletes();
    doPartialColumnGets();
    doPutFrom(this.client1);
    doPartialRowDeletes(this.client1);
    doPartialRowGets(this.vm1);
    doDeletesFrom(this.client1);
    verifyDeletes();
    doPutFrom(this.client1);
    doPartialGets();
    doAllColumnDeletesFromSpecificRows(this.client1);
    doPartialRowGets(this.vm2);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  private void doAllColumnDeletesFromSpecificRows(VM client1) {
    client1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex = rowIndex + 3) {
          Delete delete = new Delete(Bytes.toBytes(KEY_PREFIX + rowIndex));
          for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex = columnIndex + 1) {
            delete.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex));
          }
          table.delete(delete);
        }
        return null;
      }
    });
  }

  private void doPartialRowGets(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex = rowIndex + 1) {
          Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
          Row result = table.get(get);
          if (rowIndex % 3 == 0) {
            assertTrue(result.isEmpty());
          } else {
            assertFalse(result.isEmpty());
            int columnIndex = 0;
            List<Cell> row = result.getCells();
            for (int i = 0; i < row.size() - 1; i++) {
              Assert.assertNotEquals(10, columnIndex);
              byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
              byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

              if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
                System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
                System.out.println(
                    "actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
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
        return null;
      }
    });
  }

  private void doPartialRowDeletes(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex = rowIndex + 3) {
          Delete delete = new Delete(Bytes.toBytes(KEY_PREFIX + rowIndex));
          table.delete(delete);
        }
        return null;
      }
    });
  }

  private void doPartialColumnGets() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));

      int getResultSize = 0;
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex = columnIndex + 1) {
        get.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex));
        getResultSize++;
      }

      Row result = table.get(get);
      assertFalse(result.isEmpty());
      assertEquals(getResultSize, result.size());

      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          Assert.fail("Invalid Values for Column Name");
        }
        // Add a comment to this line
        if (columnIndex % 2 == 0) {
          assertEquals(row.get(i).getColumnValue(), null);
        } else {
          if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
            Assert.fail("Invalid Values for Column Value");
          }
        }
        columnIndex = columnIndex + 1;
      }
    }
  }

  private void doPartialColumnDeletes() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Delete delete = new Delete(Bytes.toBytes(KEY_PREFIX + rowIndex));

      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex = columnIndex + 2) {
        delete.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex));
      }
      table.delete(delete);
    }
  }

  @Test
  public void testPutAllColumnsGetPartialColumns() {
    putAllColumnsGetPartialColumns(true);
    putAllColumnsGetPartialColumns(false);
  }

  private void updatePartialColumnGetAllColumns(final boolean ordered) {
    createTableOn(this.client1, ordered);
    doPartialPutFrom(this.client1);
    verifyAllColumnGetsForRecordsWithPartialPuts();

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testUpdatePartialColumnGetAllColumns() {
    updatePartialColumnGetAllColumns(true);
    updatePartialColumnGetAllColumns(false);
  }

  private void putsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClient(
      final boolean ordered) {
    createTableOn(this.client1, ordered);
    doPutsWithVersionFrom(this.client1);
    verifyVersionedGets();

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testPutsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClient() {
    putsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClient(true);
    putsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClient(false);
  }

  private void putsWithTimeStampAndGetWithTimeStampFromOtherClient(final boolean ordered) {
    createTableOn(this.client1, ordered);
    doPutsWithTimeStampFrom(this.client1, ordered);
    verifyTimeStampGets(LATEST_TIMESTAMP, ordered);
    verifyTimeStampGets(200, ordered);
    verifyTimeStampGets(100, ordered);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testPutsWithTimeStampAndGetWithTimeStampFromOtherClient() {
    putsWithTimeStampAndGetWithTimeStampFromOtherClient(true);
    putsWithTimeStampAndGetWithTimeStampFromOtherClient(false);
  }

  private void putsGetsWithFrequentColumnUpdates(final boolean ordered) {
    createTableOn(this.client1, ordered);
    doFrequentColumnPuts(this.client1);
    doGets();
    // System.out.println("====================================================================");
    doFrequentColumnPutsWithColumnOverlap(this.client1);
    doGetsForFrequentUpdates();
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testPutsGetsWithFrequentColumnUpdates() {
    putsGetsWithFrequentColumnUpdates(true);
    putsGetsWithFrequentColumnUpdates(false);
  }

  // 1. Test creates a table
  // 2. Table is empty
  // 3. perform Delete Operation with selected columns.
  // GEN - 852
  @Test
  public void testDeleteSpecificColumnOnEmptyTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);

    Delete deleteRowWithColumn = new Delete(Bytes.toBytes(KEY_PREFIX + 0));
    deleteRowWithColumn.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
    deleteRowWithColumn.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 1));
    try {
      table.delete(deleteRowWithColumn);
    } catch (Exception e) {
      assertTrue(e instanceof RowKeyDoesNotExistException);
    }

    Get getRow = new Get(Bytes.toBytes(KEY_PREFIX + 0));
    Row result = table.get(getRow);

    assertTrue(result.isEmpty());

    clientCache.getAdmin().deleteTable(TABLE_NAME);

  }

  // TEST
  // 1. Create Table with Persist Enaled
  // 2. Do some puts
  // 3. Verify using gets
  // 4. Delete the table
  // 5. Create Table again with same Configuration
  // 6. Try doing gets, It should not get any values
  // Ensure that nothing is getting reloaded and delete
  // is working fine.
  @Test
  public void testTablePerstCreateDeleteCreateAgain() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1).setMaxVersions(5);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);

    doPuts();
    doGets();

    clientCache.getAdmin().deleteTable(TABLE_NAME);

    MTableDescriptor tableDescriptor1 = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor1 = tableDescriptor1.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor1.setRedundantCopies(1).setMaxVersions(5);
    tableDescriptor1.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable table1 = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor1);
    assertEquals(table1.getName(), TABLE_NAME);
    assertNotNull(table1);

    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table1.get(get);
      assertTrue(result.isEmpty());
    }

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  // Test
  // 1. Create Table with Perst Enabled
  // 2. Do some puts
  // 3. Verify values using Get
  // 4. Delete the table
  // 5. create new table with the same name but this time
  // no persistent
  // 6. Do some Puts
  // 7. Do some gets
  // Expectation : No failures, new table should be created
  // and PUT/GET operations should happen properly.

  @Test
  public void testTablePerstCreateDeleteCreateAgainWithDifferentConf() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1).setMaxVersions(5);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);

    doPuts();
    doGets();

    clientCache.getAdmin().deleteTable(TABLE_NAME);

    MTableDescriptor tableDescriptor1 = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor1 = tableDescriptor1.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor1.setRedundantCopies(1).setMaxVersions(5);
    MTable table1 = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor1);
    assertEquals(table1.getName(), TABLE_NAME);
    assertNotNull(table1);

    doPuts();
    doGets();

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testStopServersAndDeleteTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1).setMaxVersions(5);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);

    doPuts();
    doGets();

    stopServerOn(this.vm0);
    stopServerOn(this.vm1);
    stopServerOn(this.vm2);

    try {
      clientCache.getAdmin().deleteTable(TABLE_NAME);
    } catch (Exception e) {
      assertTrue(e instanceof MTableNotExistsException);
    }
  }


  // GEM-873 Test
  private MTable createTestTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();

    tableDescriptor.setMaxVersions(1);

    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    // add the same columns again
    Exception e = null;
    try {
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
    } catch (TableColumnAlreadyExists ex) {
      e = ex;
    }
    assertTrue(e instanceof TableColumnAlreadyExists);
    return clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
  }


  @Test
  public void testDuplicateColAdd() {
    MTable table = createTestTable();

    // Version 1
    MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    List<Put> mputs = new ArrayList<>();

    Put put1 = new Put("1");
    for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex += 2) {
      put1.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
          Bytes.toBytes(VALUE_PREFIX + columnIndex));
    }
    table.put(put1);
    mputs.add(put1);

    Put put2 = new Put("1");
    for (int columnIndex = 1; columnIndex < NUM_OF_COLUMNS; columnIndex += 2) {
      put2.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
          Bytes.toBytes(VALUE_PREFIX + columnIndex));
    }
    mputs.add(put2);
    table.put(put2);
    // table.put(mputs);

    // get the latest (and only version)
    Get get = new Get("1");
    Row result = table.get(get);
    List<Cell> cells = result.getCells();

    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      Cell cell = cells.get(i);
      Object value = cell.getColumnValue();
      System.out.println("testPutGetWithVersion --> " + Bytes.toString(cell.getColumnName()) + ":"
          + Arrays.toString((byte[]) value));
      assertTrue(value != null);
      assertTrue(Bytes.toString((byte[]) value).compareTo(VALUE_PREFIX + i) == 0);

    }
  }

  // GEN-899
  @Test
  public void testOutOfRangeKeys() {
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + i));
    }
    tableDescriptor.setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"));
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);

    Exception e = null;
    try {
      Put put = new Put(Bytes.toBytes("800"));
      put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0), new byte[10]);
      table.put(put);
    } catch (RowKeyOutOfRangeException ex) {
      e = ex;
    }
    assertTrue(e instanceof RowKeyOutOfRangeException);
    e = null;

    try {
      ArrayList<Put> putList = new ArrayList<>();

      Put put = new Put(Bytes.toBytes("800"));
      put.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0), new byte[10]);
      putList.add(put);
      table.put(putList);
    } catch (RowKeyOutOfRangeException ex) {
      e = ex;
    }
    assertTrue(e instanceof RowKeyOutOfRangeException);
    e = null;


    try {
      Get get = new Get(Bytes.toBytes("800"));
      table.get(get);
    } catch (RowKeyOutOfRangeException ex) {
      e = ex;
    }
    assertTrue(e instanceof RowKeyOutOfRangeException);
    e = null;

    try {
      ArrayList<Get> getList = new ArrayList<>();

      Get get = new Get(Bytes.toBytes("800"));
      getList.add(get);
      table.get(getList);
    } catch (RowKeyOutOfRangeException ex) {
      e = ex;
    }
    assertTrue(e instanceof RowKeyOutOfRangeException);
    e = null;



    try {
      Delete delete = new Delete(Bytes.toBytes("800"));
      table.delete(delete);
    } catch (RowKeyOutOfRangeException ex) {
      e = ex;
    }
    assertTrue(e instanceof RowKeyOutOfRangeException);
    e = null;

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }
}
