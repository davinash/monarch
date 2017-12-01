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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.ampool.monarch.table.internal.CellRef;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MResultParser;
import io.ampool.monarch.table.internal.MTableRow;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.MPredicateHolder;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.TypePredicateOp;


public abstract class MTableDiskPersistenceTestBase extends MTableDUnitHelper {
  public MTableDiskPersistenceTestBase() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  Host host = Host.getHost(0);
  protected VM vm0 = host.getVM(0);
  protected VM vm1 = host.getVM(1);
  protected VM vm2 = host.getVM(2);
  protected VM client1 = host.getVM(3);

  protected final int NUM_OF_COLUMNS = 10;
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int LATEST_TIMESTAMP = 300;
  protected final int MAX_VERSIONS = 5;
  protected final int TABLE_MAX_VERSIONS = 7;
  private final int[] columnIds = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  private final int DEFAULT_SPLITS = 113;


  protected void startAllServers() {
    try {
      startServerOn(this.vm0, DUnitLauncher.getLocatorString());
      startServerOn(this.vm1, DUnitLauncher.getLocatorString());
      startServerOn(this.vm2, DUnitLauncher.getLocatorString());
      createClientCache(this.client1);
      createClientCache();
    } catch (Exception ex) {
      fail(ex.getMessage());
    }
  }

  protected void stopAllServers() {
    try {
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
    } catch (Exception ex) {
      fail(ex.getMessage());
    }
  }


  protected void startAllServersAsync() {
    AsyncInvocation asyncInvocation1 =
        (AsyncInvocation) asyncStartServerOn(vm0, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocation2 =
        (AsyncInvocation) asyncStartServerOn(vm1, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocation3 =
        (AsyncInvocation) asyncStartServerOn(vm2, DUnitLauncher.getLocatorString());
    try {
      asyncInvocation1.join();
      asyncInvocation2.join();
      asyncInvocation3.join();
      createClientCache(this.client1);
      createClientCache();
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }
  }

  protected void createDiskStore(final String diskStoreName) {
    try {
      new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
          .forEach((VM) -> VM.invoke(new SerializableCallable() {
            @Override
            public Object call() throws Exception {
              DiskStoreFactory diskStoreFactory =
                  MCacheFactory.getAnyInstance().createDiskStoreFactory();
              diskStoreFactory.create(diskStoreName);
              return null;
            }
          }));
    } catch (Exception ex) {
      fail(ex.getMessage());
    }
  }

  protected void removeDiskStore(final String diskStoreName) {
    try {
      new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
          .forEach((VM) -> VM.invoke(new SerializableCallable() {
            @Override
            public Object call() throws Exception {
              MCacheFactory.getAnyInstance().findDiskStore(diskStoreName).destroy();
              return null;
            }
          }));
    } catch (Exception ex) {
      fail(ex.getMessage());
    }
  }

  protected void doPuts(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  protected void doPutFrom(VM vm, final String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPuts(tableName);
        return null;
      }
    });
  }

  protected abstract void createTable(final String tableName);

  protected abstract void createTable(final String tableName, final boolean ordered, int numSplits);


  protected void createTableOn(VM vm, final String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(tableName);
        return null;
      }
    });
  }

  protected void createTableOn(VM vm, final String tableName, final boolean ordered) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(tableName, ordered);
        return null;
      }
    });
  }

  protected void createTableOn(VM vm, final String tableName, final boolean ordered,
      int numSplits) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(tableName, ordered, numSplits);
        return null;
      }
    });
  }


  protected void doGets(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
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
        Assert.assertNotEquals(10, columnIndex);
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

  protected void doDeletes(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Delete delete = new Delete(Bytes.toBytes(KEY_PREFIX + rowIndex));
      table.delete(delete);
    }
  }

  protected void doDeletesFrom(VM vm, final String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doDeletes(tableName);
        return null;
      }
    });
  }

  protected void verifyDeletes(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertTrue(result.isEmpty());
    }
  }

  protected void doPartialGets(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
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
      for (Cell cell : row) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) cell.getColumnValue())) {
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex = columnIndex + 2;
      }
    }
  }

  protected void doPartialPuts(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex = columnIndex + 2) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  protected void doPartialPutFrom(VM vm, final String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPartialPuts(tableName);
        return null;
      }
    });
  }

  protected void verifyAllColumnGetsForRecordsWithPartialPuts(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (Cell cell : row) {
        Assert.assertNotEquals(10 + 1, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (columnIndex == NUM_OF_COLUMNS) {
          expectedColumnName = Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME);
          exptectedValue = result.getRowId();
        }

        if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) cell.getColumnValue())) {
          if (columnIndex % 2 != 0) {
            assertNull(cell.getColumnValue());
          } else {
            Assert.fail("Invalid Values for Column Value");
          }
        }
        columnIndex = columnIndex + 1;
      }
    }
  }


  protected void doPutsWithVersion(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
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

  protected void doPutsWithVersionFrom(VM vm, final String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPutsWithVersion(tableName);
        return null;
      }
    });
  }

  protected void verifyVersionedGets(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
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


  protected void doPutsWithTimeStampFrom(VM vm, final String tableName, final boolean ordered) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
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

  protected void verifyTimeStampGets(final String tableName, int timeStamp, final boolean ordered) {
    timeStamp = timeStamp != 0 ? timeStamp : LATEST_TIMESTAMP;
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
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
        Assert.assertNotEquals(10 + 1, columnIndex);
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


  protected void doFrequentColumnPuts(VM vm, final String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
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

  protected void doFrequentColumnPutsWithColumnOverlap(VM vm, final String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
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

  protected void doGetsForFrequentUpdates(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10 + 1, columnIndex);
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


  protected void createTableOnWithMaxVerions(final String tableName, final boolean ordered) {
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
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable table = clientCache.getAdmin().createTable(tableName, tableDescriptor);
    assertEquals(tableName, table.getName());
    assertNotNull(table);
  }

  protected void createTableOnWithMaxVerionsOn(VM vm, final String tableName,
      final boolean ordered) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTableOnWithMaxVerions(tableName, ordered);
        return null;
      }
    });
  }

  protected void doSomePuts(final String tableName, final boolean ordered) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
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

  protected void doTestGetColumnValueOnVM(VM vm, final String tableName) {
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
            0, 0, 0, 6, 86, 65, 76, 85, 69, 48, /* Column1 Value */
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

        Region region = MCacheFactory.getAnyInstance().getRegion(MTableUtils.AMPL_META_REGION_NAME);
        assertNotNull(region);
        MTableDescriptor tableDescriptor = (MTableDescriptor) region.get(tableName);
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
            System.out.println("ACTUAL   COLUMN NAME => " + new String(actualColumnName));
            System.out.println("EXPECTED COLUMN NAME => " + new String(expectedColumnName));
            Assert.fail("getColumnAt failed");
          }

          byte[] actualValue = (byte[]) column.getValue();
          byte[] expectedValue = resultExpected.get(column.getKey());

          if (!Bytes.equals(actualValue, expectedValue)) {
            System.out.println("ACTUAL   COLUMN VALUE => " + new String(actualValue));
            System.out.println("EXPECTED COLUMN VALUE => " + new String(expectedValue));
            Assert.fail("getColumnAt failed");
          }

          columnIndex++;
        }
        return null;
      }
    });
  }

  protected void verifyGetsWithMaxVersionsOn(VM vm, final String tableName, final boolean ordered) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {

        MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
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
              Assert.assertNotEquals(10 + 1, columnIndex);
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

  protected void putsWithMaxVersionSpecified(final String tableName, final boolean ordered) {
    createTableOnWithMaxVerionsOn(this.client1, tableName, ordered);
    doSomePuts(tableName, ordered);
    verifyGetsWithMaxVersionsOn(this.client1, tableName, ordered);
  }

  protected void putsWithMaxVersionSpecifiedAfterRestart(final String tableName,
      final boolean ordered) {
    verifyGetsWithMaxVersionsOn(this.client1, tableName, ordered);
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    clientCache.getAdmin().deleteTable(tableName);
  }

  protected void putFromOneClientAndGetFromAnother(final String tableName, final boolean ordered) {
    createTableOn(this.client1, tableName, ordered);
    doPutFrom(this.client1, tableName);
    doGets(tableName);
    doTestGetColumnValueOnVM(this.vm0, tableName);
    doTestGetColumnValueOnVM(this.vm1, tableName);
    doTestGetColumnValueOnVM(this.vm2, tableName);
    // doDeletesFrom(this.client1, tableName);
    // verifyDeletes(tableName);
  }

  protected void putFromOneClientAndGetFromAnotherAfterRestart(final String tableName,
      final boolean ordered) {
    doGets(tableName);
    doTestGetColumnValueOnVM(this.vm0, tableName);
    doTestGetColumnValueOnVM(this.vm1, tableName);
    doTestGetColumnValueOnVM(this.vm2, tableName);
    doDeletesFrom(this.client1, tableName);
    verifyDeletes(tableName);
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    clientCache.getAdmin().deleteTable(tableName);
  }

  protected void putAllColumnsGetPartialColumns(final String tableName, final boolean ordered) {
    createTableOn(this.client1, tableName, ordered);
    doPutFrom(this.client1, tableName);
    doPartialGets(tableName);
    // doDeletesFrom(this.client1, tableName);
  }

  protected void putAllColumnsGetPartialColumnsAfterRestart(final String tableName,
      final boolean ordered) {
    doPartialGets(tableName);
    doDeletesFrom(this.client1, tableName);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }


  protected void updatePartialColumnGetAllColumns(final String tableName, final boolean ordered) {
    createTableOn(this.client1, tableName, ordered);
    doPartialPutFrom(this.client1, tableName);
    verifyAllColumnGetsForRecordsWithPartialPuts(tableName);
  }

  protected void updatePartialColumnGetAllColumnsAfterRestart(final String tableName,
      final boolean ordered) {
    verifyAllColumnGetsForRecordsWithPartialPuts(tableName);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }


  protected void putsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClient(
      final String tableName, final boolean ordered) {
    createTableOn(this.client1, tableName, ordered);
    doPutsWithVersionFrom(this.client1, tableName);
    verifyVersionedGets(tableName);
  }

  protected void putsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClientAfterRestart(
      final String tableName, final boolean ordered) {
    verifyVersionedGets(tableName);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }


  protected void putsWithTimeStampAndGetWithTimeStampFromOtherClient(final String tableName,
      final boolean ordered) {
    createTableOn(this.client1, tableName, ordered);
    doPutsWithTimeStampFrom(this.client1, tableName, ordered);
    verifyTimeStampGets(tableName, LATEST_TIMESTAMP, ordered);
    verifyTimeStampGets(tableName, 200, ordered);
    verifyTimeStampGets(tableName, 100, ordered);
  }

  protected void putsWithTimeStampAndGetWithTimeStampFromOtherClientAfterRestart(
      final String tableName, final boolean ordered) {
    verifyTimeStampGets(tableName, LATEST_TIMESTAMP, ordered);
    verifyTimeStampGets(tableName, 200, ordered);
    verifyTimeStampGets(tableName, 100, ordered);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }


  protected void putsGetsWithFrequentColumnUpdates(final String tableName, final boolean ordered) {
    createTableOn(this.client1, tableName, ordered);
    doFrequentColumnPuts(this.client1, tableName);
    doGets(tableName);
    doFrequentColumnPutsWithColumnOverlap(this.client1, tableName);
    doGetsForFrequentUpdates(tableName);
  }

  protected void putsGetsWithFrequentColumnUpdatesAfterRestart(final String tableName,
      final boolean ordered) {
    doFrequentColumnPutsWithColumnOverlap(this.client1, tableName);
    doGetsForFrequentUpdates(tableName);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }


  /**
   * Helper method to assert on the results..
   *
   * @param numberOfRows the number of rows
   * @param results results retrieved
   * @param columnIds an array of column-ids fetched
   */
  protected void assertResults(int numberOfRows, List<Row> results, final int[] columnIds) {
    assertEquals(numberOfRows, results.size());
    for (Row result : results) {
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        byte[] columnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] columnValueBytes = MResultParser.EMPTY_BYTES;
        if (columnIds.length == 0 || Arrays.binarySearch(columnIds, columnIndex) >= 0) {
          columnValueBytes = Bytes.toBytes(VALUE_PREFIX + columnIndex);
        }
        // if ( columnIndex == row.size()) {
        // columnName = Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME);
        // columnValueBytes = result.getRowId();
        // }
        // System.out.println("columnName --> " + Arrays.toString(columnName));
        // System.out.println("columnName --> " + Arrays.toString(row.get(i).getColumnName()));

        assertArrayEquals("Column-id: " + columnIndex, columnName, row.get(i).getColumnName());
        assertArrayEquals("Column-id: " + columnIndex, columnValueBytes,
            ((CellRef) row.get(i)).getValueArrayCopy());
        columnIndex = columnIndex + 1;
      }
    }
  }

  /** ---- Tests for batchGet (using MBatchGet) ---- **/
  /**
   * helper methods for asserting on results..
   **/
  private void assertResults(int numberOfRows, List<Row> results) {
    assertResults(numberOfRows, results, new int[0]);
  }

  protected void doBatchPut(VM vm, final String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPuts(tableName);
        return null;
      }
    });
  }

  protected void doBatchPut(VM vm, final String tableName, int numThreads) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPuts(tableName, numThreads);
        return null;
      }
    });
  }

  protected void verifyBatchGets(MTable table) {
    /* Bulk GET - Test a zero size input */
    List<Get> getListNull = null;
    Exception expectedException = null;
    try {
      table.get(getListNull);
    } catch (IllegalArgumentException e) {
      expectedException = e;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    /* Bulk PUT - Test a zero size input */
    List<Put> nullPutList = null;
    expectedException = null;
    try {
      table.put(nullPutList);
    } catch (IllegalArgumentException e) {
      expectedException = e;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);


    List<Get> getList = new ArrayList<>();

    /* Bulk GET - Test a zero size input */
    expectedException = null;
    try {
      table.get(getList);
    } catch (IllegalArgumentException e) {
      expectedException = e;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    /* Bulk PUT - Test a zero size input */
    expectedException = null;
    List<Put> emptyPuts = new ArrayList<>();
    try {
      table.put(emptyPuts);
    } catch (IllegalArgumentException e) {
      expectedException = e;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      getList.add(new Get(Bytes.toBytes(KEY_PREFIX + rowIndex)));
    }

    Row[] batchGetResult = table.get(getList);
    assertNotNull(batchGetResult);

    // verify result size
    assertResults(NUM_OF_ROWS, Arrays.asList(batchGetResult));

    getList.clear();
    /* Try getting keys which are alternate */
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex = rowIndex + 2) {
      getList.add(new Get(Bytes.toBytes(KEY_PREFIX + rowIndex)));
    }
    batchGetResult = table.get(getList);
    assertNotNull(batchGetResult);

    for (Row result : batchGetResult) {
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      assertEquals(NUM_OF_COLUMNS + 1, row.size());
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);

        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          System.out.println("Expected Column Name " + Bytes.toString(expectedColumnName));
          System.out.println("Actual   Column Name " + Bytes.toString(row.get(i).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }

        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          System.out.println("Expected Column Value " + Bytes.toString(exptectedValue));
          System.out.println(
              "Actual   Column Value " + Bytes.toString((byte[]) row.get(i).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex = columnIndex + 1;
      }
    }

    getList.clear();
    /* try adding keys which does not exists */
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      getList.add(new Get(Bytes.toBytes(KEY_PREFIX + rowIndex + rowIndex)));
    }
    batchGetResult = table.get(getList);
    assertNotNull(batchGetResult);

    /*
     * Result should be null for the keys as those keys does not exists
     */
    for (Row result : batchGetResult) {
      assertNotNull(result);
      assertTrue(result.isEmpty());
    }
  }

  /**
   * TEST DESCRIPTION 1. Create a Table with 10 columns 2. Insert around 50 rows in the table. 3.
   * Perform batch get ALL THE COLUMNS 4. Verify the Result.
   */

  protected void batchPutGetRows(final String tableName, final boolean order) {
    createTableOn(this.client1, tableName, order);
    doBatchPut(this.client1, tableName);
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable table = clientCache.getTable(tableName);
    verifyBatchGets(table);
  }

  protected void batchPutGetRowsAfterRestart(final String tableName, final boolean order) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable table = clientCache.getTable(tableName);
    verifyBatchGets(table);
    clientCache.getAdmin().deleteTable(tableName);
  }

  protected void batchPutMultiThread(final String tableName) {
    createTableOn(this.client1, tableName, true);
    doBatchPut(this.client1, tableName, 10);
  }

  protected void batchPutWithMaxVersionEqualsToOne(final String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.setMaxVersions(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(tableName, tableDescriptor);
    assertEquals(tableName, table.getName());


    List<Put> puts = new ArrayList<>();
    for (int rowIndex = 0; rowIndex < 1; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      puts.add(record);
    }
    table.put(puts);
    puts.clear();

    for (int rowIndex = 0; rowIndex < 1; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex + "00"));
      }
      puts.add(record);
    }
    table.put(puts);

    Get getRecord = new Get(Bytes.toBytes(KEY_PREFIX + 0));
    Row result = table.get(getRecord);
    assertNotNull(result);

    int columnIndex = 0;
    List<Cell> row = result.getCells();
    assertEquals(NUM_OF_COLUMNS + 1, row.size());
    for (int i = 0; i < row.size() - 1; i++) {
      Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);

      byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
      byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex + "00");

      if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
        System.out.println("Expected Column Name " + Bytes.toString(expectedColumnName));
        System.out.println("Actual   Column Name " + Bytes.toString(row.get(i).getColumnName()));
        Assert.fail("Invalid Values for Column Name");
      }
      if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
        System.out.println("Expected Column Value " + Bytes.toString(exptectedValue));
        System.out.println(
            "Actual   Column Value " + Bytes.toString((byte[]) row.get(i).getColumnValue()));
        Assert.fail("Invalid Values for Column Value");
      }
      columnIndex = columnIndex + 1;
    }
  }

  protected void batchPutWithMaxVersionEqualsToOneAfterRestart(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    Get getRecord = new Get(Bytes.toBytes(KEY_PREFIX + 0));
    Row result = table.get(getRecord);
    assertNotNull(result);

    int columnIndex = 0;
    List<Cell> row = result.getCells();
    assertEquals(NUM_OF_COLUMNS + 1, row.size());
    for (int i = 0; i < row.size() - 1; i++) {
      Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);

      byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
      byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex + "00");

      if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
        System.out.println("Expected Column Name " + Bytes.toString(expectedColumnName));
        System.out.println("Actual   Column Name " + Bytes.toString(row.get(i).getColumnName()));
        Assert.fail("Invalid Values for Column Name");
      }
      if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
        System.out.println("Expected Column Value " + Bytes.toString(exptectedValue));
        System.out.println(
            "Actual   Column Value " + Bytes.toString((byte[]) row.get(i).getColumnValue()));
        Assert.fail("Invalid Values for Column Value");
      }
      columnIndex = columnIndex + 1;
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  class PutterThread implements Runnable {
    private Thread thread;
    private String threadName;
    private MTable table;

    public PutterThread(String threadName, MTable table) {
      this.threadName = threadName;
      this.table = table;
      thread = new Thread(this, threadName);
    }

    @Override
    public void run() {
      int numRows = 1000;
      List<Put> puts = new ArrayList<>();
      for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
        Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        puts.add(record);
      }
      Exception e = null;
      try {
        table.put(puts);
      } catch (Exception ce) {
        e = ce;
      }
      assertNull(e);
    }

    public void start() {
      thread.start();
    }

    public void waitTillDone() {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  protected void doPuts(final String tableName, int numThreads) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    List<MTableDiskPersistenceTestBase.PutterThread> l = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      l.add(new PutterThread("Thread" + i, table));
    }
    for (MTableDiskPersistenceTestBase.PutterThread pt : l) {
      pt.start();
    }
    for (MTableDiskPersistenceTestBase.PutterThread pt : l) {
      pt.waitTillDone();
    }
  }

  ////////////////// SCAN tests ////////////////////////////////////

  private void createTableOnServerVM(VM vm, final String tableName, final boolean ordered,
      final String coProcessClass) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTableOnServer(tableName, ordered, coProcessClass);
        return null;
      }
    });
  }

  protected void createTableOnServer(final String tableName, final boolean ordered,
      final String coProcessClass) {
    MCache cache = MCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    Admin admin = cache.getAdmin();
    MTable table = admin.createTable(tableName, tableDescriptor);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);
  }


  private void createScanwithNullObjectOn(VM vm, final String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        MTable table = cache.getTable(tableName);
        assertNotNull(table);
        Exception expectedException = null;
        Scanner rs = null;
        try {
          rs = table.getScanner(null);
        } catch (Exception e) {
          expectedException = e;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);
        return null;
      }
    });
  }

  /* Scan with null object should not be allowd */
  protected void scanCreateWithNullObject(final String tableName) {
    createTableOnServerVM(this.vm0, tableName, true,
        "io.ampool.monarch.table.MTableServerScanDUnitTest$ExecuteScannerFn");
    createScanwithNullObjectOn(this.vm0, tableName);
    createScanwithNullObjectOn(this.vm1, tableName);
    createScanwithNullObjectOn(this.vm2, tableName);
  }

  /* Scan with null object should not be allowd */
  protected void scanCreateWithNullObjectAfterrestart(final String tableName) {
    createScanwithNullObjectOn(this.vm0, tableName);
    createScanwithNullObjectOn(this.vm1, tableName);
    createScanwithNullObjectOn(this.vm2, tableName);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  protected void scannerOnEmptyTable(final String tableName) {
    createTableOnServerVM(this.vm0, tableName, true,
        "io.ampool.monarch.table.MTableServerScanDUnitTest$ExecuteScannerOnEmptyTable");
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    assertNotNull(table);

    MExecutionRequest execution = new MExecutionRequest();
    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), 10);
    execution.setArguments(keysForAllBuckets);

    for (int i = 0; i < table.getTableDescriptor().getTotalNumOfSplits(); i++) {
      Exception e = null;
      try {
        List<Object> resultCollector = table.coprocessorService(
            "io.ampool.monarch.table.MTableServerScanDUnitTest$ExecuteScannerOnEmptyTable",
            "scanEmptyTable", keysForAllBuckets.get(i).get(0), execution);
      } catch (MCoprocessorException e1) {
        e = e1;
      }
      assertTrue(e instanceof MCoprocessorException);
    }
  }

  protected void scannerOnEmptyTableAfterRestart(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    assertNotNull(table);
    MExecutionRequest execution = new MExecutionRequest();
    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), 10);
    execution.setArguments(keysForAllBuckets);
    for (int i = 0; i < table.getTableDescriptor().getTotalNumOfSplits(); i++) {
      Exception e = null;
      try {
        List<Object> resultCollector = table.coprocessorService(
            "io.ampool.monarch.table.MTableServerScanDUnitTest$ExecuteScannerOnEmptyTable",
            "scanEmptyTable", keysForAllBuckets.get(i).get(0), execution);
      } catch (MCoprocessorException e1) {
        e = e1;
      }
      assertTrue(e instanceof MCoprocessorException);
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  private Map<Integer, List<byte[]>> doPutsWithAllBucketKeys(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    return keysForAllBuckets;
  }


  protected void scannerUsingMCoprocessor(final String tableName) {
    createTableOnServerVM(this.vm0, tableName, true,
        "io.ampool.monarch.table.MTableServerScanDUnitTest$ExecuteScannerFn");
    Map<Integer, List<byte[]>> uniformKeys = doPutsWithAllBucketKeys(getName());
    // Get the table from
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    assertNotNull(table);

    MExecutionRequest execution = new MExecutionRequest();
    execution.setArguments(uniformKeys);

    for (int i = 0; i < table.getTableDescriptor().getTotalNumOfSplits(); i++) {
      List<Object> resultCollector = table.coprocessorService(
          "io.ampool.monarch.table.MTableServerScanDUnitTest$ExecuteScannerFn", "verifyScan",
          uniformKeys.get(i).get(0), execution);
    }
  }

  protected void scannerUsingMCoprocessorAfterRestart(final String tableName) {
    // Get the table from
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    assertNotNull(table);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<byte[]>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> {
      KEY_LIST.forEach((KEY) -> {
        allKeys.add(KEY);
      });
    });

    MExecutionRequest execution = new MExecutionRequest();
    execution.setArguments(keysForAllBuckets);

    for (int i = 0; i < table.getTableDescriptor().getTotalNumOfSplits(); i++) {
      List<Object> resultCollector = table.coprocessorService(
          "io.ampool.monarch.table.MTableServerScanDUnitTest$ExecuteScannerFn", "verifyScan",
          keysForAllBuckets.get(i).get(0), execution);
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  private List<byte[]> doBatchPutsForAllBuckets(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> KEY_LIST.forEach((KEY) -> {
      allKeys.add(KEY);
    }));

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), allKeys.size());

    final List<Put> puts = new ArrayList<>(NUM_OF_ROWS);
    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      puts.add(record);
    });
    table.put(puts);

    return allKeys;
  }

  private void doScan(final String tableName, List<byte[]> listOfKeys, Boolean returnKeys)
      throws IOException {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    assertNotNull(table);
    Scan scan = new Scan();
    scan.setBatchSize(10);
    if (returnKeys != null) {
      scan.setReturnKeysFlag(returnKeys);
    }
    Scanner scanner = table.getScanner(scan);
    Row currentRow = scanner.next();

    int rowIndex = 0;


    while (currentRow != null) {
      byte[] key = currentRow.getRowId();
      List<Cell> cells = currentRow.getCells();
      // System.out.println("Actual : " + Arrays.toString(currentRow.getRowId()) + "
      // -------------------");
      // System.out.println("Expected :" + Arrays.toString(listOfKeys.get(rowIndex)) + "
      // -------------------");

      // if (returnKeys == null || returnKeys == true) {
      // // default is to return keys
      // assertNotNull(key);
      // if (Bytes.compareTo(listOfKeys.get(rowIndex), key) != 0) {
      // Assert.fail("Order Mismatch from the Scanner");
      // }
      // } else {
      // assertNull(key); // keys were explicitly not requested
      // }
      int columnCount = 0;
      for (Cell cell : cells) {
        columnCount++;
        // System.out.println("COLUMN NAME -> " + Arrays.toString(cell.getColumnName()));
        // System.out.println("COLUMN VALUE -> " + Arrays.toString((byte[])cell.getColumnValue()));
      }
      assertEquals(NUM_OF_COLUMNS + 1, columnCount);
      rowIndex++;
      currentRow = scanner.next();
    }
    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), rowIndex);
    scanner.close();
  }

  protected List<byte[]> getAllBucketsKeys(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);
    List<byte[]> allKeys = new ArrayList<>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> KEY_LIST.forEach((KEY) -> {
      allKeys.add(KEY);
    }));
    return allKeys;
  }

  protected List<byte[]> simpleScannerUsingGetAll(final String tableName) throws IOException {
    createTable(tableName, true);
    List<byte[]> listOfKeys = doBatchPutsForAllBuckets(tableName);
    Collections.sort(listOfKeys, Bytes.BYTES_COMPARATOR);
    doScan(tableName, listOfKeys, null);
    return listOfKeys;
  }

  protected void simpleScannerUsingGetAllAfterRestart(final String tableName,
      List<byte[]> listOfKeys) throws IOException {
    doScan(tableName, listOfKeys, null);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  protected List<byte[]> simpleScannerUsingGetAllKeys(final String tableName) throws IOException {
    createTable(tableName, true);
    List<byte[]> listOfKeys = doBatchPutsForAllBuckets(tableName);
    Collections.sort(listOfKeys, Bytes.BYTES_COMPARATOR);
    doScan(tableName, listOfKeys, true);
    return listOfKeys;
  }

  private void createTable(String tableName, boolean b) {
    createTable(tableName, b, DEFAULT_SPLITS);
  }

  protected void simpleScannerUsingGetAllKeysAfterRestart(final String tableName,
      List<byte[]> listOfKeys) throws IOException {
    doScan(tableName, listOfKeys, true);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  protected List<byte[]> simpleScannerUsingGetAllNoKeys(final String tableName) throws IOException {
    createTable(tableName, true);
    List<byte[]> listOfKeys = doBatchPutsForAllBuckets(tableName);
    Collections.sort(listOfKeys, Bytes.BYTES_COMPARATOR);
    doScan(tableName, listOfKeys, false);
    return listOfKeys;
  }

  protected void simpleScannerUsingGetAllNoKeysAfterRestart(final String tableName,
      List<byte[]> listOfKeys) throws IOException {
    doScan(tableName, listOfKeys, false);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  protected void verifyUnorderedScan(final String tableName, Boolean returnKeys, int expectedSize) {
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    MTable table = cache.getTable(tableName);

    int totalCount = 0;
    final int iniBucket = 0;
    final int maxBucket = 113;
    final int bucketBatchSize = 10;
    for (int i = iniBucket; i < maxBucket; i += bucketBatchSize) {
      Scan scan = new Scan();
      Set<Integer> set =
          IntStream.range(i, i + bucketBatchSize).mapToObj(e -> e).collect(Collectors.toSet());
      scan.setBucketIds(set);
      scan.setMessageChunkSize(1);
      scan.setBatchSize(10);
      if (returnKeys != null) {
        scan.setReturnKeysFlag(returnKeys);
      }
      Scanner scanner = table.getScanner(scan);
      long count = 0;
      for (Row result : scanner) {
        byte[] key = result.getRowId();
        if (returnKeys == null || returnKeys == true) {
          // default is to return keys
          assertNotNull(key);
        }
        // else {
        // assertNull(key); // keys were explicitly not requested
        // }
        count++;
      }
      totalCount += count;
    }
    System.out.println(expectedSize + " :: " + totalCount);
    assertEquals(expectedSize, totalCount);
  }

  protected int doScannerUsingGetAll_UnOrdered(final String tableName, Boolean returnKeys) {
    System.out.println("MTableScannerUsingGetAllTest.testScannerUsingGetAll_UnOrdered");
    /** create UNORDERED table and put some data in it.. **/
    createTable(tableName, false);
    final int expectedSize = doBatchPutsForAllBuckets(tableName).size();
    verifyUnorderedScan(tableName, returnKeys, expectedSize);
    return expectedSize;
  }

  protected void doScannerUsingGetAll_UnOrderedAfterRestart(final String tableName,
      Boolean returnKeys, final int expectedSize) {
    verifyUnorderedScan(tableName, returnKeys, expectedSize);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  /**
   * Helper method to create an array with single predicate.
   *
   * @param columnIdx the column index
   * @param type the column type
   * @param op the operation to be performed
   * @param value the value to be compared against
   * @return the new predicate with provided details
   */
  private MPredicateHolder[] newPredicate(final int columnIdx, final DataType type,
      final TypePredicateOp op, final Object value) {
    return new MPredicateHolder[] {new MPredicateHolder(columnIdx, type, op, value)};
  }

  /**
   * Helper method to assert on the results provided by a scan. Make sure that the required number
   * of results are returned by the scanner.
   *
   * @param table the table on which scan is to be performed
   * @param expectedCount the expected count
   * @param predicate the predicates to be executed
   */
  private void assertScanWithPredicates(final MTable table, final int expectedCount,
      final Filter predicate) {
    Scan scan = new Scan();
    scan.setBucketIds(IntStream.range(0, 113).mapToObj(e -> e).collect(Collectors.toSet()));
    scan.setMessageChunkSize(1);
    scan.setBatchSize(2);
    if (predicate != null)
      scan.setFilter(predicate);
    Scanner scanner = table.getScanner(scan);
    System.out.println("Predicates= " + predicate);
    long actualCount = 0;
    for (final Row result : scanner) {
      actualCount++;
      assertTrue(result.getCells().size() > 0);
    }
    System.out.println(expectedCount + " :: " + actualCount);
    assertEquals(expectedCount, actualCount);
  }

  protected void scanWithPredicate(final String tableName, final int dataLength) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    /** create table with following schema **/
    final Object[][] columns = new Object[][] {{"c_1", BasicTypes.LONG}, {"c_2", BasicTypes.STRING},
        {"c_3", BasicTypes.INT}};
    /** execute scan with predicate (single at the moment) and assert as required **/
    /** execute scan with predicate (single at the moment) and assert as required **/
    final Object[][] expectedObjects = new Object[][] {{dataLength, null},
        {4, new SingleColumnValueFilter((String) columns[0][0], CompareOp.GREATER_OR_EQUAL, 2L)},
        {1, new SingleColumnValueFilter((String) columns[1][0], CompareOp.REGEX, ".*5$")},
        {0, new SingleColumnValueFilter((String) columns[1][0], CompareOp.REGEX, ".*ABC.*")},
        {4, new SingleColumnValueFilter((String) columns[1][0], CompareOp.NOT_EQUAL, "String_2")},
        {2, new SingleColumnValueFilter((String) columns[2][0], CompareOp.LESS, 33)},
        {0, new SingleColumnValueFilter((String) columns[2][0], CompareOp.GREATER_OR_EQUAL, 555)},
        {0, new SingleColumnValueFilter((String) columns[0][0], CompareOp.LESS_OR_EQUAL, 0)},};

    for (Object[] expectedObject : expectedObjects) {
      assertScanWithPredicates(table, (int) expectedObject[0],
          (SingleColumnValueFilter) expectedObject[1]);
    }
  }

  protected int scanner_UnOrderedWithPredicates(final String tableName) {
    System.out.println("MTableScannerUsingGetAllTest.testScanner_UnOrderedWithPredicates");
    /** create UNORDERED table and put some data in it.. **/
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    /** create table with following schema **/
    final Object[][] columns = new Object[][] {{"c_1", BasicTypes.LONG}, {"c_2", BasicTypes.STRING},
        {"c_3", BasicTypes.INT}};
    for (final Object[] column : columns) {
      td = td.addColumn(column[0].toString(), new MTableColumnType(column[1].toString()));
    }
    td.setRedundantCopies(1);
    td.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable table = clientCache.getAdmin().createTable(tableName, td);

    /** insert following data in above created table **/
    final Object[][] data = new Object[][] {{1L, "String_1", 11}, {2L, "String_2", 22},
        {3L, "String_3", 33}, {4L, "String_4", 44}, {5L, "String_5", 55},};
    for (int i = 0; i < data.length; i++) {
      Put put = new Put("row_" + i);
      for (int j = 0; j < data[i].length; j++) {
        put.addColumn(columns[j][0].toString(), data[i][j]);
      }
      table.put(put);
    }
    scanWithPredicate(tableName, data.length);
    return data.length;
  }

  protected void scanner_UnOrderedWithPredicatesAfterRestart(final String tableName,
      final int dataLength) {
    scanWithPredicate(tableName, dataLength);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  /**
   * Test to make sure that range cannot be specified for un-ordered tables.
   */
  protected void scanner_UnOrderedWithRange(final String tableName) {
    System.out.println("MTableScannerUsingGetAllTest.testScanner_UnOrderedWithRange");
    /** create UNORDERED table.. **/
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    td.addColumn("c_1", new MTableColumnType(BasicTypes.LONG));
    td.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable table = clientCache.getAdmin().createTable(tableName, td);

    /** execute scan with selected columns and assert as required **/
    Scan scan = new Scan();
    try {
      table.getScanner(scan.setStartRow(new byte[] {0, 1}));
      fail("Should not provide startRow for UnOrdered table.");
    } catch (IllegalArgumentException e) {
      assertEquals("Key range should not be provided for UnOrdered scan.", e.getLocalizedMessage());
    }

    try {
      scan.setStartRow(null);
      table.getScanner(scan.setStopRow(new byte[] {0, 1}));
      fail("Should not provide stopRow for UnOrdered table.");
    } catch (IllegalArgumentException e) {
      assertEquals("Key range should not be provided for UnOrdered scan.", e.getLocalizedMessage());
    }
  }

  protected void scanner_UnOrderedWithRangeAfterRestart(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    /** execute scan with selected columns and assert as required **/
    Scan scan = new Scan();
    try {
      table.getScanner(scan.setStartRow(new byte[] {0, 1}));
      fail("Should not provide startRow for UnOrdered table.");
    } catch (IllegalArgumentException e) {
      assertEquals("Key range should not be provided for UnOrdered scan.", e.getLocalizedMessage());
    }

    try {
      scan.setStartRow(null);
      table.getScanner(scan.setStopRow(new byte[] {0, 1}));
      fail("Should not provide stopRow for UnOrdered table.");
    } catch (IllegalArgumentException e) {
      assertEquals("Key range should not be provided for UnOrdered scan.", e.getLocalizedMessage());
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  /**
   * Test for un-ordered scan with selected columns.
   */
  protected void scanner_UnOrderedWithSelectedColumns(final String tableName) {
    System.out.println("MTableScannerUsingGetAllTest.testScanner_UnOrderedWithSelectedColumns");
    /** create UNORDERED table and put some data in it.. **/
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor td = new MTableDescriptor(MTableType.UNORDERED);
    /** create table with following schema **/
    final Object[][] columns = new Object[][] {{"c_1", BasicTypes.LONG}, {"c_2", BasicTypes.STRING},
        {"c_3", BasicTypes.INT}};
    for (final Object[] column : columns) {
      td = td.addColumn(column[0].toString(), new MTableColumnType(column[1].toString()));
    }
    td.setRedundantCopies(1);
    td.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable table = clientCache.getAdmin().createTable(tableName, td);

    /** insert following data in above created table **/
    final Object[][] data = new Object[][] {{1L, "String_1", 11}, {2L, "String_2", 22},
        {3L, "String_3", 33}, {4L, "String_4", 44}, {5L, "String_5", 55},};
    for (int i = 0; i < data.length; i++) {
      Put put = new Put("row_" + i);
      for (int j = 0; j < data[i].length; j++) {
        put.addColumn(columns[j][0].toString(), data[i][j]);
      }
      table.put(put);
    }

    /** execute scan with selected columns and assert as required **/
    Scan scan = new Scan();
    scan.setBucketIds(IntStream.range(0, 113).mapToObj(e -> e).collect(Collectors.toSet()));
    scan.setBatchSize(2);
    scan.setColumns(Arrays.asList(1, 2));
    Scanner scanner = table.getScanner(scan);
    int actualCount = 0;
    final int expectedCount = data.length;
    for (final Row result : scanner) {
      assertEquals(data[actualCount][1], result.getCells().get(0).getColumnValue());
      assertEquals(data[actualCount][2], result.getCells().get(1).getColumnValue());
      actualCount++;
    }
    System.out.println(expectedCount + " :: " + actualCount);
    assertEquals(expectedCount, actualCount);
  }

  /**
   * Test for un-ordered scan with selected columns.
   */
  protected void scanner_UnOrderedWithSelectedColumnsAfterRestart(final String tableName) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);

    /** insert following data in above created table **/
    final Object[][] data = new Object[][] {{1L, "String_1", 11}, {2L, "String_2", 22},
        {3L, "String_3", 33}, {4L, "String_4", 44}, {5L, "String_5", 55},};

    /** execute scan with selected columns and assert as required **/
    Scan scan = new Scan();
    scan.setBucketIds(IntStream.range(0, 113).mapToObj(e -> e).collect(Collectors.toSet()));
    scan.setBatchSize(2);
    scan.setColumns(Arrays.asList(1, 2));
    Scanner scanner = table.getScanner(scan);
    int actualCount = 0;
    final int expectedCount = data.length;
    for (final Row result : scanner) {
      assertEquals(data[actualCount][1], result.getCells().get(0).getColumnValue());
      assertEquals(data[actualCount][2], result.getCells().get(1).getColumnValue());
      actualCount++;
    }
    System.out.println(expectedCount + " :: " + actualCount);
    assertEquals(expectedCount, actualCount);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }


  protected List<byte[]> scanAfterDelete(final String tableName) throws IOException {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    MTable table = clientCache.getAdmin().createTable(tableName, tableDescriptor);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), 20);

    List<byte[]> allKeys = new ArrayList<>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> KEY_LIST.forEach((KEY) -> {
      allKeys.add(KEY);
    }));

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * 20), allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    Scanner scanner = table.getScanner(new Scan());
    assertNotNull(scanner);
    Row row = scanner.next();
    while (row != null) {
      row = scanner.next();
    }
    scanner.close();
    return allKeys;
  }

  protected void scanAfterDeleteAfterRestart(final String tableName, final List<byte[]> allKeys)
      throws IOException {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    Scanner scanner = table.getScanner(new Scan());
    assertNotNull(scanner);
    Row row = scanner.next();
    while (row != null) {
      row = scanner.next();
    }
    scanner.close();

    for (int i = 0; i < 10; i++) {
      Delete delete = new Delete(allKeys.get(i));
      table.delete(delete);
    }

    Scanner scannerd = table.getScanner(new Scan());
    assertNotNull(scannerd);
    Row rowd = scannerd.next();
    while (rowd != null) {
      rowd = scannerd.next();
    }
    scannerd.close();
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  private List<byte[]> doPutsWithRecordsPerBucket(final String tableName, int numRecordsPerBucket) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), numRecordsPerBucket);

    List<byte[]> allKeys = new ArrayList<>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> KEY_LIST.forEach((KEY) -> {
      allKeys.add(KEY);
    }));

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * numRecordsPerBucket),
        allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    return allKeys;
  }

  protected List<byte[]> clientRefAcrossBatch(final String tableName) {
    createTable(tableName, true);
    int NUM_OF_ROWS_PER_BUCKET = 50;
    List<byte[]> listOfKeys = doPutsWithRecordsPerBucket(tableName, NUM_OF_ROWS_PER_BUCKET);
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    int batchSize = 10;
    Scan scan = new Scan();
    scan.setBatchSize(batchSize);
    Scanner scanner = table.getScanner(scan);
    assertNotNull(scanner);
    Row firstRow = scanner.next();
    byte[] firstRowId = firstRow.getRowId();
    Row row = firstRow;
    int count = 0;
    while (row != null && count++ < batchSize) {
      row = scanner.next();
    }
    assertEquals(new ByteArrayKey(firstRowId), new ByteArrayKey(firstRow.getRowId()));
    scanner.close();
    return listOfKeys;
  }

  protected void clientRefAcrossBatchAfterRestart(final String tableName,
      final List<byte[]> listOfKeys) {
    int NUM_OF_ROWS_PER_BUCKET = 50;
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    int batchSize = 10;
    Scan scan = new Scan();
    scan.setBatchSize(batchSize);
    Scanner scanner = table.getScanner(scan);
    assertNotNull(scanner);
    Row firstRow = scanner.next();
    byte[] firstRowId = firstRow.getRowId();
    Row row = firstRow;
    int count = 0;
    while (row != null && count++ < batchSize) {
      row = scanner.next();
    }
    assertEquals(new ByteArrayKey(firstRowId), new ByteArrayKey(firstRow.getRowId()));
    scanner.close();
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  private Map<Integer, List<byte[]>> doPutsWithRowsPerBucket(final String tableName, int numRows) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);

    Map<Integer, List<byte[]>> allKeys =
        getBucketToKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), numRows);
    int totalKeyCount = 0;
    for (Map.Entry<Integer, List<byte[]>> entry : allKeys.entrySet()) {
      totalKeyCount += entry.getValue().size();
    }
    System.out.println(
        "Generate keys buckets " + allKeys.keySet().size() + " key count  " + totalKeyCount);
    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * numRows), totalKeyCount);
    int totalPutKeys = 0;

    for (Map.Entry<Integer, List<byte[]>> entry : allKeys.entrySet()) {
      for (byte[] key : entry.getValue()) {
        Get get = new Get(key);
        Row result = table.get(get);
        assertTrue(result.isEmpty());
        Put record = new Put(key);
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        table.put(record);
        totalPutKeys++;
      }
    }

    assertEquals(table.getTableDescriptor().getTotalNumOfSplits() * numRows, totalKeyCount);
    return allKeys;
  }


  private void doGetsWithBucketKeys(final String tableName, Map<Integer, List<byte[]>> keys) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    assertNotNull(table);

    keys.forEach((K, V) -> {
      V.forEach((X) -> {
        Get get = new Get(X);
        Row result = table.get(get);
        assertEquals(NUM_OF_COLUMNS + 1, result.size());
        assertFalse(result.isEmpty());
        int columnIndex = 0;
        List<Cell> row = result.getCells();
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

      });

    });
  }


  protected Map<Integer, List<byte[]>> clientScannerTest(final String tableName) {
    int noOfBuckets = 10;
    int numOfRowsInEachBucket = 200;
    createTableOn(this.client1, tableName, true, noOfBuckets);
    Map<Integer, List<byte[]>> bucketWiseKeys =
        new TreeMap<>(doPutsWithRowsPerBucket(tableName, numOfRowsInEachBucket));
    doGetsWithBucketKeys(tableName, bucketWiseKeys);

    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    Scan scan = new Scan();
    Scanner resultScanner = table.getScanner(scan);
    Iterator<Row> itr = resultScanner.iterator();
    int resultCount = 0;
    int nullResulCount = 0;
    while (itr.hasNext()) {
      Row result = itr.next();
      if (result != null) {
        resultCount++;
      } else {
        nullResulCount++;
      }
    }

    assertEquals(0, nullResulCount);
    assertEquals(noOfBuckets * numOfRowsInEachBucket, resultCount);

    // Test next(num records)
    resultScanner = table.getScanner(new Scan());
    itr = resultScanner.iterator();
    resultCount = 0;
    Row[] results = resultScanner.next(numOfRowsInEachBucket);
    assertEquals(numOfRowsInEachBucket, results.length);

    // All keys should be from first bucket
    List<byte[]> keysFrom1stbucket = bucketWiseKeys.get(0);
    Set<ByteBuffer> keys = new HashSet<>();
    keysFrom1stbucket.forEach((K) -> {
      keys.add(ByteBuffer.wrap(K));
    });

    for (Row result : results) {
      assertTrue(keys.contains(ByteBuffer.wrap(result.getRowId())));
    }

    return bucketWiseKeys;
  }


  protected void clientScannerTestAfterRestart(final String tableName,
      Map<Integer, List<byte[]>> bucketWiseKeys) {
    int noOfBuckets = 10;
    int numOfRowsInEachBucket = 200;
    doGetsWithBucketKeys(tableName, bucketWiseKeys);
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    Scan scan = new Scan();
    Scanner resultScanner = table.getScanner(scan);
    Iterator<Row> itr = resultScanner.iterator();
    int resultCount = 0;
    int nullResulCount = 0;
    while (itr.hasNext()) {
      Row result = itr.next();
      if (result != null) {
        resultCount++;
      } else {
        nullResulCount++;
      }
    }

    assertEquals(0, nullResulCount);
    assertEquals(noOfBuckets * numOfRowsInEachBucket, resultCount);

    // Test next(num records)
    resultScanner = table.getScanner(new Scan());
    itr = resultScanner.iterator();
    resultCount = 0;
    Row[] results = resultScanner.next(numOfRowsInEachBucket);
    assertEquals(numOfRowsInEachBucket, results.length);

    // All keys should be from first bucket
    List<byte[]> keysFrom1stbucket = bucketWiseKeys.get(0);
    Set<ByteBuffer> keys = new HashSet<>();
    keysFrom1stbucket.forEach((K) -> {
      keys.add(ByteBuffer.wrap(K));
    });

    for (Row result : results) {
      assertTrue(keys.contains(ByteBuffer.wrap(result.getRowId())));
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  private void doPutsWithKeys(final String tableName, Set<String> keys) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);

    int totalPutKeys = 0;

    for (String key : keys) {
      Get get = new Get(key);
      Row result = table.get(get);
      assertTrue(result.isEmpty());
      Put record = new Put(key);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
      totalPutKeys++;
    }
  }

  private void doGetsWithKeys(final String tableName, Set<String> keys) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    assertNotNull(table);

    keys.forEach((X) -> {
      Get get = new Get(X);
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

    });
  }

  protected Set<String> scanSortOrderTest(final String tableName) {

    int noOfBuckets = 10;
    int numOfRecs = 200;
    Set<String> keys = new HashSet<>();
    Random rd = new Random();
    while (keys.size() != numOfRecs) {
      keys.add(String.valueOf(rd.nextInt(Integer.MAX_VALUE)));
    }

    createTableOn(this.client1, tableName, true, noOfBuckets);
    doPutsWithKeys(tableName, keys);
    doGetsWithKeys(tableName, keys);

    Set<String> clientSortedKeys = new TreeSet(keys);

    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);
    Scanner resultScanner = table.getScanner(new Scan());
    Iterator<Row> resultIterator = resultScanner.iterator();
    Iterator clientKeysIterator = clientSortedKeys.iterator();

    int resultCount = 0;

    List<Row> scanResults = new ArrayList<>();

    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));

      if (result != null) {
        resultCount++;
        scanResults.add(result);
      }
    }

    assertEquals(numOfRecs, resultCount);

    return clientSortedKeys;
  }


  protected void scanSortOrderTestAfterRestart(final String tableName,
      Set<String> clientSortedKeys) {
    int noOfBuckets = 10;
    int numOfRecs = 200;
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);
    Scanner resultScanner = table.getScanner(new Scan());
    Iterator<Row> resultIterator = resultScanner.iterator();
    Iterator clientKeysIterator = clientSortedKeys.iterator();

    int resultCount = 0;

    List<Row> scanResults = new ArrayList<>();

    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));

      if (result != null) {
        resultCount++;
        scanResults.add(result);
      }
    }
    assertEquals(numOfRecs, resultCount);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  protected Set<String> resultBatchingTest(final String tableName) {

    int noOfBuckets = 10;
    createTableOn(this.client1, tableName, true, noOfBuckets);
    int numOfRecs = 200;
    Set<String> keys = new HashSet<>();
    Random rd = new Random();
    while (keys.size() != numOfRecs) {
      keys.add(String.valueOf(rd.nextInt(Integer.MAX_VALUE)));
    }

    doPutsWithKeys(tableName, keys);
    doGetsWithKeys(tableName, keys);

    Set<String> clientSortedKeys = new TreeSet(keys);

    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);
    Scan scan = new Scan();
    scan.setClientQueueSize(20);
    Scanner resultScanner = table.getScanner(scan);
    Iterator<Row> resultIterator = resultScanner.iterator();
    Iterator clientKeysIterator = clientSortedKeys.iterator();

    int resultCount = 0;

    List<Row> scanResults = new ArrayList<>();

    int resultsVerified = 1;
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      System.out.println("results verified " + resultsVerified++);
      if (result != null) {
        resultCount++;
        scanResults.add(result);
      }
    }
    return clientSortedKeys;
  }


  protected void resultBatchingTestAfterRestart(final String tableName,
      Set<String> clientSortedKeys) {

    int numOfRecs = 200;
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);
    Scan scan = new Scan();
    scan.setClientQueueSize(20);
    Scanner resultScanner = table.getScanner(scan);
    Iterator<Row> resultIterator = resultScanner.iterator();
    Iterator clientKeysIterator = clientSortedKeys.iterator();

    int resultCount = 0;

    List<Row> scanResults = new ArrayList<>();

    int resultsVerified = 1;
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      System.out.println("results verified " + resultsVerified++);
      if (result != null) {
        resultCount++;
        scanResults.add(result);
      }
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  protected void doScanWithKeys(final String tableName, Set<String> keys) {
    Iterator clientKeysIterator = keys.iterator();
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);
    Scanner resultScanner = table.getScanner(new Scan());
    Iterator<Row> resultIterator = resultScanner.iterator();

    // Test scan with start & stop row
    // select 2 random keys
    clientKeysIterator = keys.iterator();
    String startKey = null;
    String stopKey = null;
    int itrCnt = 0;
    Set<String> startStopKeys = new TreeSet<>();

    while (clientKeysIterator.hasNext()) {
      String tmp = (String) clientKeysIterator.next();
      if (StringUtils.isEmpty(tmp)) {
        continue;
      }
      if (itrCnt == 400) {
        startKey = tmp;
        startStopKeys.add(tmp);
      }

      if ((itrCnt > 400) && itrCnt < 900) {
        startStopKeys.add(tmp);
      }
      if (itrCnt == 900) {
        stopKey = tmp;
        startStopKeys.add(tmp);
      }
      itrCnt++;
    }

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(startKey));
    scan.setStopRow(Bytes.toBytes(stopKey));
    resultScanner = table.getScanner(scan);
    resultIterator = resultScanner.iterator();
    clientKeysIterator = startStopKeys.iterator();

    int resultCount = 0;
    List<Row> scanStartStopResults = new ArrayList<>();
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        resultCount++;
        scanStartStopResults.add(result);
      }
    }
    assertEquals(500, resultCount);

    // scan with start row & null stop row
    scan = new Scan();
    scan.setStartRow(Bytes.toBytes(startKey));
    scan.setStopRow(null);
    resultScanner = table.getScanner(scan);
    resultIterator = resultScanner.iterator();
    clientKeysIterator = startStopKeys.iterator();

    resultCount = 0;
    scanStartStopResults = new ArrayList<>();
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      if (result != null) {
        resultCount++;
        scanStartStopResults.add(result);
      }
    }
    assertEquals(600, resultCount);
  }

  protected Set<String> scanByStartStopRowTest(final String tableName) {
    int noOfBuckets = 10;
    int numOfRecs = 1000;

    createTableOn(this.client1, tableName, true, noOfBuckets);
    Set<String> keys = new TreeSet<>();
    Random rd = new Random();
    while (keys.size() != numOfRecs) {
      keys.add(String.valueOf(rd.nextInt(Integer.MAX_VALUE)));
    }

    doPutsWithKeys(tableName, keys);
    doScanWithKeys(tableName, keys);
    return keys;
  }

  protected void scanByStartStopRowTestAfterRestart(final String tableName, Set<String> keys) {
    doScanWithKeys(tableName, keys);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  protected Set<String> scanByStopRowTest(final String tableName) {
    int noOfBuckets = 10;
    int numOfRecs = 1000;
    createTableOn(this.client1, tableName, true, noOfBuckets);
    Set<String> keys = new TreeSet<>();
    Random rd = new Random();
    while (keys.size() != numOfRecs) {
      keys.add(String.valueOf(rd.nextInt(Integer.MAX_VALUE)));
    }
    doPutsWithKeys(tableName, keys);
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Scan with stop row
    Scanner resultScanner = table.getScanner(new Scan());
    Iterator<Row> resultIterator = resultScanner.iterator();
    int resultCount = 0;
    Iterator clientKeysIterator = keys.iterator();
    String stopKey = null;
    int itrCnt = 0;
    Set<String> startStopKeys = new TreeSet<>();
    while (clientKeysIterator.hasNext()) {
      String tmp = (String) clientKeysIterator.next();

      if (itrCnt < 60) {
        startStopKeys.add(tmp);
      }
      if (itrCnt == 60) {
        stopKey = tmp;
        startStopKeys.add(tmp);
      }
      itrCnt++;
    }

    Scan scan = new Scan();
    scan.setStopRow(Bytes.toBytes(stopKey));
    resultScanner = table.getScanner(scan);
    resultIterator = resultScanner.iterator();
    clientKeysIterator = startStopKeys.iterator();

    System.out.println("MTableClientScannerDunitTest.testScanByStopRow :: " + "Stop Row: " + stopKey
        + " Bytes:" + Bytes.toBytes(stopKey));

    resultCount = 0;
    List<Row> scanStartStopResults = new ArrayList<>();
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        resultCount++;
        scanStartStopResults.add(result);
      }
    }
    assertEquals(startStopKeys.size() - 1, resultCount);
    return keys;
  }


  protected void scanByStopRowTestAfterRestart(final String tableName, Set<String> keys) {
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Scan with stop row
    Scanner resultScanner = table.getScanner(new Scan());
    Iterator<Row> resultIterator = resultScanner.iterator();
    int resultCount = 0;
    Iterator clientKeysIterator = keys.iterator();
    String stopKey = null;
    int itrCnt = 0;
    Set<String> startStopKeys = new TreeSet<>();
    while (clientKeysIterator.hasNext()) {
      String tmp = (String) clientKeysIterator.next();

      if (itrCnt < 60) {
        startStopKeys.add(tmp);
      }
      if (itrCnt == 60) {
        stopKey = tmp;
        startStopKeys.add(tmp);
      }
      itrCnt++;
    }

    Scan scan = new Scan();
    scan.setStopRow(Bytes.toBytes(stopKey));
    resultScanner = table.getScanner(scan);
    resultIterator = resultScanner.iterator();
    clientKeysIterator = startStopKeys.iterator();

    System.out.println("MTableClientScannerDunitTest.testScanByStopRow :: " + "Stop Row: " + stopKey
        + " Bytes:" + Bytes.toBytes(stopKey));

    resultCount = 0;
    List<Row> scanStartStopResults = new ArrayList<>();
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        resultCount++;
        scanStartStopResults.add(result);
      }
    }
    assertEquals(startStopKeys.size() - 1, resultCount);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  protected Set<String> scanByStartRowTest(final String tableName) {
    int noOfBuckets = 10;
    int numOfRecs = 1000;
    createTableOn(this.client1, tableName, true, noOfBuckets);
    Set<String> keys = new TreeSet<>();
    Random rd = new Random();
    while (keys.size() != numOfRecs) {
      keys.add(String.valueOf(rd.nextInt(Integer.MAX_VALUE)));
    }
    doPutsWithKeys(tableName, keys);

    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Scan with stop row
    Scanner resultScanner = table.getScanner(new Scan());
    Iterator<Row> resultIterator = resultScanner.iterator();

    int resultCount = 0;

    Iterator clientKeysIterator = keys.iterator();
    String startKey = null;
    int itrCnt = 0;
    Set<String> startStopKeys = new TreeSet<>();

    while (clientKeysIterator.hasNext()) {
      String tmp = (String) clientKeysIterator.next();

      if (itrCnt > 60) {
        startStopKeys.add(tmp);
      }
      if (itrCnt == 60) {
        startKey = tmp;
        startStopKeys.add(tmp);
      }
      itrCnt++;
    }

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(startKey));
    resultScanner = table.getScanner(scan);
    resultIterator = resultScanner.iterator();
    clientKeysIterator = startStopKeys.iterator();

    System.out.println("MTableClientScannerDunitTest.testScanByStopRow :: " + "Start Row: "
        + startKey + " Bytes:" + Bytes.toBytes(startKey));

    resultCount = 0;
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        resultCount++;
      }
    }
    assertEquals(startStopKeys.size(), resultCount);
    return keys;
  }

  protected void scanByStartRowTestAfterRestart(String tableName, Set<String> keys) {
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Scan with stop row
    Scanner resultScanner = table.getScanner(new Scan());
    Iterator<Row> resultIterator = resultScanner.iterator();

    int resultCount = 0;

    Iterator clientKeysIterator = keys.iterator();
    String startKey = null;
    int itrCnt = 0;
    Set<String> startStopKeys = new TreeSet<>();

    while (clientKeysIterator.hasNext()) {
      String tmp = (String) clientKeysIterator.next();

      if (itrCnt > 60) {
        startStopKeys.add(tmp);
      }
      if (itrCnt == 60) {
        startKey = tmp;
        startStopKeys.add(tmp);
      }
      itrCnt++;
    }

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(startKey));
    resultScanner = table.getScanner(scan);
    resultIterator = resultScanner.iterator();
    clientKeysIterator = startStopKeys.iterator();

    System.out.println("MTableClientScannerDunitTest.testScanByStopRow :: " + "Start Row: "
        + startKey + " Bytes:" + Bytes.toBytes(startKey));

    resultCount = 0;
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        resultCount++;
      }
    }
    assertEquals(startStopKeys.size(), resultCount);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }


  protected Set<String> fullTableScanTest(final String tableName) {

    int noOfBuckets = 10;

    createTableOn(this.client1, tableName, true, noOfBuckets);

    int numOfRecs = 1000;
    Set<String> keys = new TreeSet<>();
    Random rd = new Random();
    while (keys.size() != numOfRecs) {
      keys.add(String.valueOf(rd.nextInt(Integer.MAX_VALUE)));
    }

    doPutsWithKeys(tableName, keys);
    doGetsWithKeys(tableName, keys);


    Iterator clientKeysIterator = keys.iterator();

    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Full scan without start & stop row
    Scanner resultScanner = table.getScanner(new Scan());
    Iterator<Row> resultIterator = resultScanner.iterator();

    int resultCount = 0;
    List<Row> scanResults = new ArrayList<>();
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        // System.out.println("result no "+ resultCount +" result key"+ result);
        resultCount++;
        scanResults.add(result);
      }
    }
    assertEquals(numOfRecs, resultCount);

    // Test next(n) call
    resultScanner = table.getScanner(new Scan());
    Iterator itr = resultScanner.iterator();
    resultCount = 0;
    Row[] results = resultScanner.next(10);
    assertEquals(10, results.length);
    return keys;
  }


  protected void fullTableScanTestAfterRestart(final String tableName, Set<String> keys) {
    int numOfRecs = 1000;
    Iterator clientKeysIterator = keys.iterator();
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Full scan without start & stop row
    Scanner resultScanner = table.getScanner(new Scan());
    Iterator<Row> resultIterator = resultScanner.iterator();

    int resultCount = 0;
    List<Row> scanResults = new ArrayList<>();
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        // System.out.println("result no "+ resultCount +" result key"+ result);
        resultCount++;
        scanResults.add(result);
      }
    }
    assertEquals(numOfRecs, resultCount);

    // Test next(n) call
    resultScanner = table.getScanner(new Scan());
    Iterator itr = resultScanner.iterator();
    resultCount = 0;
    Row[] results = resultScanner.next(10);
    assertEquals(10, results.length);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  protected Set<String> fullTableScanWithSelColumnTest(final String tableName) {
    int noOfBuckets = 10;
    createTableOn(this.client1, tableName, true, noOfBuckets);
    int numOfRecs = 1000;
    byte[] columnName = Bytes.toBytes(COLUMN_NAME_PREFIX + 2);
    Set<String> keys = new TreeSet<>();
    Random rd = new Random();
    while (keys.size() != numOfRecs) {
      keys.add(String.valueOf(rd.nextInt(Integer.MAX_VALUE)));
    }

    doPutsWithKeys(tableName, keys);
    doGetsWithKeys(tableName, keys);


    Iterator clientKeysIterator = keys.iterator();

    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Full scan without start & stop row
    Scan scan = new Scan();

    scan.addColumn(columnName);
    Scanner resultScanner = table.getScanner(scan);
    Iterator<Row> resultIterator = resultScanner.iterator();

    int resultCount = 0;
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        // System.out.println("result no "+ resultCount +" result key"+ result);
        resultCount++;
        List<Cell> cells = result.getCells();
        assertEquals(1, cells.size());
        Cell cell = cells.get(0);
        assertNotNull(cell.getColumnValue());
        assertEquals(new ByteArrayKey(columnName), new ByteArrayKey(cell.getColumnName()));
      }
    }
    assertEquals(numOfRecs, resultCount);

    // Test next(n) call
    resultScanner = table.getScanner(new Scan());
    Iterator itr = resultScanner.iterator();
    resultCount = 0;
    Row[] results = resultScanner.next(10);
    assertEquals(10, results.length);
    return keys;
  }

  protected void fullTableScanWithSelColumnTestAfterRestart(final String tableName,
      Set<String> keys) {
    int numOfRecs = 1000;
    byte[] columnName = Bytes.toBytes(COLUMN_NAME_PREFIX + 2);
    Iterator clientKeysIterator = keys.iterator();
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Full scan without start & stop row
    Scan scan = new Scan();
    scan.addColumn(columnName);
    Scanner resultScanner = table.getScanner(scan);
    Iterator<Row> resultIterator = resultScanner.iterator();
    int resultCount = 0;
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        // System.out.println("result no "+ resultCount +" result key"+ result);
        resultCount++;
        List<Cell> cells = result.getCells();
        assertEquals(1, cells.size());
        Cell cell = cells.get(0);
        assertNotNull(cell.getColumnValue());
        assertEquals(new ByteArrayKey(columnName), new ByteArrayKey(cell.getColumnName()));
      }
    }
    assertEquals(numOfRecs, resultCount);

    // Test next(n) call
    resultScanner = table.getScanner(new Scan());
    Iterator itr = resultScanner.iterator();
    resultCount = 0;
    Row[] results = resultScanner.next(10);
    assertEquals(10, results.length);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  protected Set<String> fullTableScanWithSelColumnsTest(final String tableName) {

    int noOfBuckets = 10;
    createTableOn(this.client1, tableName, true, noOfBuckets);
    int numOfRecs = 1000;
    byte[][] columnNames =
        {Bytes.toBytes(COLUMN_NAME_PREFIX + 2), Bytes.toBytes(COLUMN_NAME_PREFIX + 9),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 1), Bytes.toBytes(COLUMN_NAME_PREFIX + 5)};

    int[] colPositions = {2, 9, 1, 5};

    Set<String> keys = new TreeSet<>();
    Random rd = new Random();
    while (keys.size() != numOfRecs) {
      keys.add(String.valueOf(rd.nextInt(Integer.MAX_VALUE)));
    }

    doPutsWithKeys(tableName, keys);
    doGetsWithKeys(tableName, keys);

    Iterator clientKeysIterator = keys.iterator();

    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Full scan without start & stop row
    Scan scan = new Scan();
    for (byte[] columnName : columnNames) {
      scan.addColumn(columnName);
    }
    Scanner resultScanner = table.getScanner(scan);
    Iterator<Row> resultIterator = resultScanner.iterator();

    int resultCount = 0;
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        // System.out.println("result no "+ resultCount +" result key"+ result);
        resultCount++;
        List<Cell> cells = result.getCells();
        assertEquals(colPositions.length, cells.size());
        int index = 0;
        for (Cell cell : cells) {
          assertNotNull(cell.getColumnName());
          assertEquals(new ByteArrayKey(Bytes.toBytes(COLUMN_NAME_PREFIX + colPositions[index++])),
              new ByteArrayKey(cell.getColumnName()));
        }
      }
    }
    assertEquals(numOfRecs, resultCount);
    resultScanner.close();

    // Test next(n) call
    resultScanner = table.getScanner(new Scan());
    Iterator itr = resultScanner.iterator();
    resultCount = 0;
    Row[] results = resultScanner.next(10);
    assertEquals(10, results.length);
    return keys;
  }


  protected void fullTableScanWithSelColumnsTestAfterRestart(final String tableName,
      Set<String> keys) {
    int numOfRecs = 1000;
    byte[][] columnNames =
        {Bytes.toBytes(COLUMN_NAME_PREFIX + 2), Bytes.toBytes(COLUMN_NAME_PREFIX + 9),
            Bytes.toBytes(COLUMN_NAME_PREFIX + 1), Bytes.toBytes(COLUMN_NAME_PREFIX + 5)};

    int[] colPositions = {2, 9, 1, 5};

    Iterator clientKeysIterator = keys.iterator();

    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Full scan without start & stop row
    Scan scan = new Scan();
    for (byte[] columnName : columnNames) {
      scan.addColumn(columnName);
    }
    Scanner resultScanner = table.getScanner(scan);
    Iterator<Row> resultIterator = resultScanner.iterator();

    int resultCount = 0;
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        // System.out.println("result no "+ resultCount +" result key"+ result);
        resultCount++;
        List<Cell> cells = result.getCells();
        assertEquals(colPositions.length, cells.size());
        int index = 0;
        for (Cell cell : cells) {
          assertNotNull(cell.getColumnName());
          assertEquals(new ByteArrayKey(Bytes.toBytes(COLUMN_NAME_PREFIX + colPositions[index++])),
              new ByteArrayKey(cell.getColumnName()));
        }
      }
    }
    assertEquals(numOfRecs, resultCount);
    resultScanner.close();

    // Test next(n) call
    resultScanner = table.getScanner(new Scan());
    Iterator itr = resultScanner.iterator();
    resultCount = 0;
    Row[] results = resultScanner.next(10);
    assertEquals(10, results.length);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  protected Set<String> scanForMaxSizeTest(final String tableName) {
    int noOfBuckets = 10;
    int numOfRecs = 100;

    createTableOn(this.client1, tableName, true, noOfBuckets);
    Set<String> keys = new TreeSet<>();
    Random rd = new Random();
    while (keys.size() != numOfRecs) {
      keys.add(String.valueOf(rd.nextInt(Integer.MAX_VALUE)));
    }

    doPutsWithKeys(tableName, keys);
    doGetsWithKeys(tableName, keys);


    Iterator clientKeysIterator = keys.iterator();

    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Full scan without start & stop row
    Scan scan = new Scan();
    scan.setMaxResultLimit(90);
    Scanner resultScanner = table.getScanner(scan);
    Iterator<Row> resultIterator = resultScanner.iterator();

    int resultCount = 0;
    List<Row> scanResults = new ArrayList<>();
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        resultCount++;
        scanResults.add(result);
      }
    }
    assertEquals(90, scanResults.size());
    return keys;
  }

  protected void scanForMaxSizeTestAfterRestart(final String tableName, Set<String> keys) {
    Iterator clientKeysIterator = keys.iterator();
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);

    // Full scan without start & stop row
    Scan scan = new Scan();
    scan.setMaxResultLimit(90);
    Scanner resultScanner = table.getScanner(scan);
    Iterator<Row> resultIterator = resultScanner.iterator();
    int resultCount = 0;
    List<Row> scanResults = new ArrayList<>();
    while (resultIterator.hasNext()) {
      Row result = resultIterator.next();
      assertEquals(clientKeysIterator.next(), Bytes.toString(result.getRowId()));
      if (result != null) {
        resultCount++;
        scanResults.add(result);
      }
    }
    assertEquals(90, scanResults.size());
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }
}


