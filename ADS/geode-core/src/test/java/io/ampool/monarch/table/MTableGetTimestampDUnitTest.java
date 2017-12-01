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

import org.apache.geode.cache.Region;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.IllegalColumnNameException;
import io.ampool.monarch.table.internal.ByteArrayKey;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Category(MonarchTest.class)
public class MTableGetTimestampDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();

  final int numOfEntries = 3;

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

  public MTableGetTimestampDUnitTest() {
    super();
  }

  private void verifySizeOfRegionOnServer(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        try {
          MCache c = MCacheFactory.getAnyInstance();
          Region r = c.getRegion("EmployeeTable");
          assertNotNull(r);

          assertEquals("Region Size MisMatch", numOfEntries, r.size());

        } catch (CacheClosedException cce) {
        }
        return null;
      }
    });

  }

  private void doPutOperationFromClient(VM vm, final int locatorPort, final boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          MTableDescriptor tableDescriptor = null;
          if (order == false) {
            tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
          } else {
            tableDescriptor = new MTableDescriptor();
            tableDescriptor.setMaxVersions(5);
          }
          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
          tableDescriptor.setRedundantCopies(1);

          MClientCache clientCache = MClientCacheFactory.getAnyInstance();
          Admin admin = clientCache.getAdmin();
          MTable mtable = admin.createTable("EmployeeTable", tableDescriptor);
          assertEquals(mtable.getName(), "EmployeeTable");

          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

          for (int i = 0; i < numOfEntries; i++) {

            String key1 = "RowKey" + i;
            Put myput = new Put(Bytes.toBytes(key1));
            myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            Get myget = new Get(Bytes.toBytes("RowKey" + i));
            Row result = mtable.get(myget);
            assertFalse(result.isEmpty());
            // check timestamp
            assertTrue(result.getRowTimeStamp().longValue() != 0);
            List<Cell> row = result.getCells();

            Iterator<MColumnDescriptor> iteratorColumnDescriptor =
                mtable.getTableDescriptor().getAllColumnDescriptors().iterator();

            for (int k = 0; k < row.size() - 1; k++) {
              byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
              byte[] expectedColumnValue =
                  (byte[]) myput.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));
              if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
                Assert.fail("Invalid Values for Column Name");
              }
              if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
                System.out
                    .println("expectedColumnValue =>  " + Arrays.toString(expectedColumnValue));
                System.out.println("actuaColumnValue    =>  "
                    + Arrays.toString((byte[]) row.get(k).getColumnValue()));
                Assert.fail("Invalid Values for Column Value");
              }
            }
          }

          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            if (order)
              myput1.setTimeStamp(1000L);
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

          for (int i = 0; i < numOfEntries; i++) {

            String key1 = "RowKey" + i;
            Put myput = new Put(Bytes.toBytes(key1));
            myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            Get myget = new Get(Bytes.toBytes("RowKey" + i));
            if (order) {
              myget.setTimeStamp(1000L);
            }
            Row result = mtable.get(myget);
            assertFalse(result.isEmpty());
            // check timestamp
            if (order)
              assertEquals(1000L, result.getRowTimeStamp().longValue());
            List<Cell> row = result.getCells();

            Iterator<MColumnDescriptor> iteratorColumnDescriptor =
                mtable.getTableDescriptor().getAllColumnDescriptors().iterator();

            for (int k = 0; k < row.size() - 1; k++) {
              byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
              byte[] expectedColumnValue =
                  (byte[]) myput.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));
              if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
                Assert.fail("Invalid Values for Column Name");
              }
              if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
                System.out
                    .println("expectedColumnValue =>  " + Arrays.toString(expectedColumnValue));
                System.out.println("actuaColumnValue    =>  "
                    + Arrays.toString((byte[]) row.get(k).getColumnValue()));
                Assert.fail("Invalid Values for Column Value");
              }
            }
          }

          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            if (order)
              myput1.setTimeStamp(2000L);
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            if (order)
              myput1.setTimeStamp(3000L);
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

          for (int i = 0; i < numOfEntries; i++) {

            String key1 = "RowKey" + i;
            Put myput = new Put(Bytes.toBytes(key1));
            myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            Get myget = new Get(Bytes.toBytes("RowKey" + i));
            if (order)
              myget.setTimeStamp(2000L);
            Row result = mtable.get(myget);
            assertFalse(result.isEmpty());
            // check timestamp
            if (order)
              assertEquals(2000L, result.getRowTimeStamp().longValue());
            List<Cell> row = result.getCells();

            Iterator<MColumnDescriptor> iteratorColumnDescriptor =
                mtable.getTableDescriptor().getAllColumnDescriptors().iterator();

            for (int k = 0; k < row.size() - 1; k++) {
              byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
              byte[] expectedColumnValue =
                  (byte[]) myput.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));
              if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
                Assert.fail("Invalid Values for Column Name");
              }
              if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
                System.out
                    .println("expectedColumnValue =>  " + Arrays.toString(expectedColumnValue));
                System.out.println("actuaColumnValue    =>  "
                    + Arrays.toString((byte[]) row.get(k).getColumnValue()));
                Assert.fail("Invalid Values for Column Value");
              }
            }
          }

        } catch (CacheClosedException cce) {
        }

        return null;
      }
    });

  }

  private void doMTableOpsFromClientWithNullValues(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {

        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        Admin admin = clientCache.getAdmin();

        Exception expectedException = null;
        try {
          MTable invalidTable = admin.createTable("TABLE", null);
        } catch (IllegalArgumentException ncd) {
          expectedException = ncd;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        // create valid table and test with null values of MPut, MGet, MDelete.
        MTableDescriptor tableDescriptor = new MTableDescriptor();
        tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
            .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
        tableDescriptor.setRedundantCopies(1).setMaxVersions(5);
        MTable table = admin.createTable("TABLE", tableDescriptor);
        // Test Table.put(null)
        Put put = null;
        expectedException = null;
        try {
          table.put(put);
        } catch (IllegalArgumentException iae) {
          expectedException = iae;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        // Test Table.get(null)
        Get get = null;
        expectedException = null;
        try {
          table.get(get);
        } catch (IllegalArgumentException iae) {
          expectedException = iae;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        // Test Table.delete(null)
        Delete delete = null;
        expectedException = null;
        try {
          table.delete(delete);
        } catch (IllegalArgumentException iae) {
          expectedException = iae;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        return null;
      }
    });
  }

  public void doMTableOpsFromClientWithInvalidColumn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MClientCache cf = MClientCacheFactory.getAnyInstance();
        Admin admin = cf.getAdmin();

        MTableDescriptor mtd = new MTableDescriptor();
        mtd.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
            .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

        MTable table = admin.createTable("TABLE", mtd);

        Put newRow = new Put(Bytes.toBytes("KEY1"));
        newRow.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("NAME"));
        newRow.addColumn(Bytes.toBytes("ID"), Bytes.toBytes("ID"));
        newRow.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes("AGE"));
        newRow.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes("SALARY"));

        table.put(newRow);

        Put newRow1 = new Put(Bytes.toBytes("KEY1"));
        newRow1.addColumn(Bytes.toBytes("COLUMN_DOES_NOT_EXISTS"), Bytes.toBytes("NAME"));
        newRow1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes("ID"));
        newRow1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes("AGE"));
        newRow1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes("SALARY"));

        Exception expectedException = null;
        try {
          table.put(newRow1);
        } catch (IllegalColumnNameException icne) {
          expectedException = icne;
        }
        assertTrue(expectedException instanceof IllegalColumnNameException);

        // Get Test: Table.get() with Not existing column name
        Get get = new Get(Bytes.toBytes("KEY1"));
        get.addColumn(Bytes.toBytes("ID"));
        get.addColumn(Bytes.toBytes("COLUMN_DOES_NOT_EXISTS"));

        expectedException = null;
        try {
          Row result = table.get(get);
        } catch (IllegalColumnNameException icne) {
          expectedException = icne;
        }
        assertTrue(expectedException instanceof IllegalColumnNameException);

        // Delete Test: Table.delete() with Not existing column name
        Delete delete = new Delete(Bytes.toBytes("KEY1"));
        delete.addColumn(Bytes.toBytes("ID"));
        delete.addColumn(Bytes.toBytes("COLUMN_DOES_NOT_EXISTS"));

        expectedException = null;
        try {
          table.delete(delete);
        } catch (IllegalColumnNameException icne) {
          expectedException = icne;
        }
        assertTrue(expectedException instanceof IllegalColumnNameException);

        return null;
      }
    });
  }

  private void doBasicPutGet(final boolean order) throws Exception {
    doPutOperationFromClient(vm3, getLocatorPort(), order);
    verifySizeOfRegionOnServer(vm0);
    verifySizeOfRegionOnServer(vm1);
    verifySizeOfRegionOnServer(vm2);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    clientCache.getAdmin().deleteTable("EmployeeTable");
  }

  @Test
  public void testDoBasicPutGet() throws Exception {
    doBasicPutGet(true);
    doBasicPutGet(false);
  }

  @Test
  public void testNullValidationTests() {
    doMTableOpsFromClientWithNullValues(vm3);
  }

  @Test
  public void testColumnNameValidationTests() {
    doMTableOpsFromClientWithInvalidColumn(vm3);
  }

}
