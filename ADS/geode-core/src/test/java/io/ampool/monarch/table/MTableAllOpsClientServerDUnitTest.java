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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

@Category(MonarchTest.class)
public class MTableAllOpsClientServerDUnitTest extends MTableDUnitHelper {
  public MTableAllOpsClientServerDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "MTableAllOpsClientServerDUnitTest";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 100;
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
    createClientCacheUsingDeprecatedAPI(this.client1);
    createClientCacheUsingDeprecatedAPI();

  }

  private void closeMCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        cache.close();
        return null;
      }
    });
  }

  private void closeMCacheAll() {
    closeMCache(this.vm0);
    closeMCache(this.vm1);
    closeMCache(this.vm2);
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    closeMCacheAll();
    super.tearDown2();
  }


  private void createTableOn() {
    MCache memberCache = MCacheFactory.getAnyInstance();
    assertNotNull(memberCache);

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    Admin admin = memberCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertNotNull(table);
    assertEquals(table.getName(), TABLE_NAME);


  }

  private void createTableOnMember(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTableOn();
        return null;
      }
    });
  }

  private void doGetTableFromServer(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache mCache = MCacheFactory.getAnyInstance();
        MTable table = null;

        table = mCache.getTable(TABLE_NAME);
        assertNotNull(table);
        return null;
      }
    });
  }

  private void doGetTableFromAllVMs() {
    doGetTableFromServer(this.vm0);
    doGetTableFromServer(this.vm1);
    doGetTableFromServer(this.vm2);
  }

  private void doGetTableClient(final boolean tableExists) {
    MClientCache cf = MClientCacheFactory.getAnyInstance();
    assertNotNull(cf);
    if (tableExists) {
      MTable table = cf.getTable(TABLE_NAME);
      assertNotNull(table);
    } else {
      MTable table = cf.getTable(TABLE_NAME);
      assertNull(table);

    }
  }

  private void doGetTableClientFrom(VM vm, final boolean tableExists) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doGetTableClient(tableExists);
        return null;
      }
    });
  }

  private void doDeleteTableFrom(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        cache.getAdmin().deleteTable(TABLE_NAME);
        return null;
      }
    });
  }

  private void doPuts(final int startRow, final int endRow) {
    MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = startRow; rowIndex <= endRow; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  private void doPutsFrom(VM vm, final int startRow, final int endRow) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPuts(startRow, endRow);
        return null;
      }
    });
  }

  private void doVerifyGetsFrom(VM vm, final int startRow, final int endRow) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNotNull(table);
        for (int rowIndex = startRow; rowIndex <= endRow; rowIndex++) {
          Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
          Row result = table.get(get);
          assertFalse(result.isEmpty());
          assertEquals(NUM_OF_COLUMNS + 1, result.size());
          int columnIndex = 0;
          List<Cell> row = result.getCells();
          for (int i = 0; i < row.size() - 1; i++) {
            Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);
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
        return null;
      }
    });

  }

  private void doVerifyClient() {
    MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    for (int rowIndex = 0; rowIndex <= 75; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);
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

  private void doVerifyFromClient(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doVerifyClient();
        return null;
      }
    });
  }

  @Test
  public void testTableCreateGetAndDeleteAllVMs() {
    createTableOnMember(this.vm0);
    doGetTableFromAllVMs();

    doGetTableClientFrom(this.client1, true);
    doGetTableClient(true);

    doDeleteTableFrom(this.vm2);

    doGetTableClientFrom(this.client1, false);
    doGetTableClient(false);
  }


  private void verifyRegionSizeEachVM(VM vm, final String regionName, final int expectedSize) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache c = MCacheFactory.getAnyInstance();
        assertNotNull(c);
        Region r = c.getRegion(regionName);
        PartitionedRegion pr = (PartitionedRegion) r;
        assertNotNull(pr);
        assertEquals(expectedSize, pr.size());
        return null;
      }
    });
  }


  @Override
  public void verifyRegionSizeAllVMs(final String regionName, final int expectedSize) {
    verifyRegionSizeEachVM(this.vm0, regionName, expectedSize);
    verifyRegionSizeEachVM(this.vm1, regionName, expectedSize);
    verifyRegionSizeEachVM(this.vm2, regionName, expectedSize);
  }

  @Test
  public void testPutsGetsAllVms() {
    createTableOnMember(this.vm0);
    doPutsFrom(this.vm0, 0, 25);
    doPutsFrom(this.vm1, 26, 50);
    doPutsFrom(this.vm2, 51, 75);

    verifyRegionSizeAllVMs(TABLE_NAME, 76);
    doVerifyGetsFrom(this.vm0, 0, 25);

    doVerifyGetsFrom(this.vm1, 51, 75);
    doVerifyGetsFrom(this.vm2, 26, 50);

    doVerifyFromClient(this.client1);
    doVerifyClient();

  }

  private void doDeleteFromServerAndAccessFromClientAndOtherVMs() {
    this.vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
        return null;
      }
    });

    this.vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Exception e = null;
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNull(table);
        return null;
      }
    });

    this.vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Exception e = null;
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNull(table);
        return null;
      }
    });

    this.client1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Exception e = null;
        MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNull(table);
        return null;
      }
    });

    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNull(table);



  }


  /**
   * Create a table from a client Make sure we can access the same from All the VMs in the
   * distributed system.
   */
  @Test
  public void testCreateTableFromClientAccessFromServers() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }

    {
      // setting invalid eviction-policy for MTable and validating the exception
      Exception expectedException = null;
      try {
        tableDescriptor.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
      } catch (IllegalArgumentException iae) {
        expectedException = iae;
      }
      assertTrue(expectedException instanceof IllegalArgumentException);
    }

    tableDescriptor.setRedundantCopies(1);
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertNotNull(table);
    assertEquals(table.getName(), TABLE_NAME);

    doGetTableFromServer(this.vm0);
    doGetTableFromServer(this.vm1);
    doGetTableFromServer(this.vm2);

    doDeleteFromServerAndAccessFromClientAndOtherVMs();
  }

  @Test
  public void testPutFromClientAccessFromAllServers() {
    this.client1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        assertNotNull(clientCache);

        MTableDescriptor tableDescriptor = new MTableDescriptor();
        for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
          tableDescriptor =
              tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
        }
        tableDescriptor.setRedundantCopies(1);
        Admin admin = clientCache.getAdmin();
        MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
        assertNotNull(table);
        assertEquals(table.getName(), TABLE_NAME);
        return null;
      }
    });

    doPuts(0, 100);

    this.vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNotNull(table);
        for (int rowIndex = 0; rowIndex <= 100; rowIndex++) {
          Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
          Row result = table.get(get);
          assertFalse(result.isEmpty());
          assertEquals(NUM_OF_COLUMNS + 1, result.size());
          int columnIndex = 0;
          List<Cell> row = result.getCells();
          for (int i = 0; i < row.size() - 1; i++) {
            Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);
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
        return null;
      }
    });

    this.vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNotNull(table);
        for (int rowIndex = 0; rowIndex <= 100; rowIndex++) {
          Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
          Row result = table.get(get);
          assertFalse(result.isEmpty());
          assertEquals(NUM_OF_COLUMNS + 1, result.size());
          int columnIndex = 0;
          List<Cell> row = result.getCells();
          for (int i = 0; i < row.size() - 1; i++) {
            Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);
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
        return null;
      }
    });

    this.vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNotNull(table);
        for (int rowIndex = 0; rowIndex <= 100; rowIndex++) {
          Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
          Row result = table.get(get);
          assertFalse(result.isEmpty());
          assertEquals(NUM_OF_COLUMNS + 1, result.size());
          int columnIndex = 0;
          List<Cell> row = result.getCells();
          for (int i = 0; i < row.size() - 1; i++) {
            Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);
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
        return null;
      }
    });

    this.client1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNotNull(table);
        for (int rowIndex = 0; rowIndex <= 100; rowIndex++) {
          Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
          Row result = table.get(get);
          assertFalse(result.isEmpty());
          assertEquals(NUM_OF_COLUMNS + 1, result.size());
          int columnIndex = 0;
          List<Cell> row = result.getCells();
          for (int i = 0; i < row.size() - 1; i++) {
            Assert.assertNotEquals(NUM_OF_COLUMNS, columnIndex);
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
        return null;
      }
    });


  }

}
