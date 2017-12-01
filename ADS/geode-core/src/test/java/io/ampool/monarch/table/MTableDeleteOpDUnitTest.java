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
import io.ampool.monarch.table.exceptions.IllegalColumnNameException;
import io.ampool.monarch.table.exceptions.RowKeyDoesNotExistException;
import io.ampool.monarch.table.exceptions.TableInvalidConfiguration;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Category(MonarchTest.class)
public class MTableDeleteOpDUnitTest extends MTableDUnitHelper {

  public MTableDeleteOpDUnitTest() {
    super();
  }

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
    closeMClientCache(vm3);
    super.tearDown2();
  }

  /*
   * @Override public void tearDown2() throws Exception { closeMClientCache(vm3); cleanUpAllVMs();
   * 
   * super.tearDown2(); }
   */

  final int numOfEntries = 5;

  private void verifySizeOfRegionOnServer(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        try {
          MCache c = MCacheFactory.getAnyInstance();
          Region r = c.getRegion("EmployeeTable");

          assertEquals("Region Size MisMatch", 0, r.size());

        } catch (CacheClosedException cce) {
          cce.printStackTrace();
        }
        return null;
      }
    });

  }

  protected void createTable(final boolean order) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (order == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
    tableDescriptor.setRedundantCopies(1);
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable("EmployeeTable", tableDescriptor);
    assertEquals("EmployeeTable", mtable.getName());

  }

  private void createTableOn(VM vm, final boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(order);
        return null;
      }
    });
  }

  private void doDeleteRowWitAllColumns(VM vm, final int locatorPort, boolean deleteWithTimestamp) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MClientCache clientCache = null;
        try {
          clientCache = MClientCacheFactory.getAnyInstance();
          assertNotNull(clientCache);
          MTableDescriptor tableDescriptor = new MTableDescriptor();

          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

          MTable mtable = clientCache.getTable("EmployeeTable");
          ArrayList<Long> timestamps = new ArrayList<>();
          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            if (deleteWithTimestamp) {
              long ctime = System.currentTimeMillis();
              timestamps.add(ctime);
              myput1.setTimeStamp(ctime);
              System.out.println("PUT -> Key: " + key1 + " : " + ctime);
            }
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

          // mtable.get
          for (int i = 0; i < numOfEntries; i++) {

            String key1 = "RowKey" + i;
            Put myput = new Put(Bytes.toBytes(key1));
            myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            Get myget = new Get(Bytes.toBytes("RowKey" + i));
            Iterator<MColumnDescriptor> iterColDes =
                tableDescriptor.getAllColumnDescriptors().iterator();
            Row result = mtable.get(myget);
            System.out.println("GET -> Key: " + Bytes.toString(result.getRowId()) + " : "
                + result.getRowTimeStamp());
            assertFalse(result.isEmpty());
            assertEquals(4 + 1, result.size());
            List<Cell> row = result.getCells();
            for (int k = 0; k < row.size() - 1; k++) {
              if (row.get(k).getColumnValue() == null) {
                Assert.fail("Not All column updated");
              }

              byte[] expectedColumnName = iterColDes.next().getColumnName();
              byte[] expectedColumnValue =
                  (byte[]) myput.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));

              if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
                System.out.println("expectedColumnName =>  " + Arrays.toString(expectedColumnName));
                System.out.println(
                    "actualColumnName   =>  " + Arrays.toString(row.get(k).getColumnName()));
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

          // mTable.delete()
          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Delete delete = new Delete(Bytes.toBytes(key1));
            if (deleteWithTimestamp) {
              // explicitly give non-matching timestamp (less than actual)
              delete.setTimestamp(timestamps.get(i) - 1);
              System.out.println("DELETE -> Key: " + key1 + " : " + timestamps.get(i));
              mtable.delete(delete);
              // Verify the delete
              Get myget = new Get(Bytes.toBytes("RowKey" + i));
              Row result = mtable.get(myget);
              assertFalse(result.isEmpty());

              // explicitly give non-matching timestamp (greater than actual)
              delete.setTimestamp(timestamps.get(i) + 1);
              mtable.delete(delete);
              // Verify the delete
              myget = new Get(Bytes.toBytes("RowKey" + i));
              result = mtable.get(myget);
              assertFalse(result.isEmpty());

              // try deleting with correct TS
              delete.setTimestamp(timestamps.get(i));
              mtable.delete(delete);
              // Verify the delete
              myget = new Get(Bytes.toBytes("RowKey" + i));
              result = mtable.get(myget);
              assertTrue(result.isEmpty());
            } else {
              mtable.delete(delete);
              // Verify the delete
              Get myget = new Get(Bytes.toBytes("RowKey" + i));
              Row result = mtable.get(myget);
              assertTrue(result.isEmpty());
            }
          }
        } catch (CacheClosedException cce) {
          cce.printStackTrace();
        }
        return null;
      }
    });

  }

  private void deleteRowWitAllColumns(final boolean order, boolean deleteWithTimestamp)
      throws Exception {
    createTableOn(vm3, order);
    doDeleteRowWitAllColumns(vm3, getLocatorPort(), deleteWithTimestamp);

    verifySizeOfRegionOnServer(vm0);
    verifySizeOfRegionOnServer(vm1);
    verifySizeOfRegionOnServer(vm2);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable("EmployeeTable");

  }

  private void doDeleteRowWitSingleColumns(VM vm, final int locatorPort, boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MClientCache clientCache = null;
        try {
          clientCache = MClientCacheFactory.getAnyInstance();
          assertNotNull(clientCache);
          MTableDescriptor tableDescriptor = new MTableDescriptor();

          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

          MTable mtable = clientCache.getTable("EmployeeTable");

          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn("AGE", Bytes.toBytes(i + 10));
            myput1.addColumn("SALARY", Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

          // mtable.get
          for (int i = 0; i < numOfEntries; i++) {

            String key1 = "RowKey" + i;
            Put myput = new Put(Bytes.toBytes(key1));
            myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            Get myget = new Get(Bytes.toBytes("RowKey" + i));
            Iterator<MColumnDescriptor> iterColDes =
                tableDescriptor.getAllColumnDescriptors().iterator();
            Row result = mtable.get(myget);
            assertFalse(result.isEmpty());
            assertEquals(5, result.size());
            List<Cell> row = result.getCells();
            for (int k = 0; k < row.size() - 1; k++) {
              if (row.get(k).getColumnValue() == null) {
                Assert.fail("Not All column updated");
              }

              byte[] expectedColumnName = iterColDes.next().getColumnName();
              byte[] expectedColumnValue =
                  (byte[]) myput.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));

              if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
                System.out.println("expectedColumnName =>  " + Arrays.toString(expectedColumnName));
                System.out.println(
                    "actualColumnName   =>  " + Arrays.toString(row.get(k).getColumnName()));
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

          // mTable.delete()
          for (int rowIndex = 0; rowIndex < numOfEntries; rowIndex++) {
            String key1 = "RowKey" + rowIndex;
            Delete delete = new Delete(Bytes.toBytes(key1));
            delete.addColumn(Bytes.toBytes("NAME"));
            /*
             * if(!order){ // for unordered we should expect exception try { mtable.delete(delete);
             * }catch(Exception e){ if(e instanceof TableInvalidConfiguration){ e.printStackTrace();
             * }else{
             * fail("Expected exception io.ampool.monarch.table.exceptions.TableInvalidConfiguration but got "
             * + e); } } return null; }else
             */
            mtable.delete(delete);

            // Verify the delete
            Get myget = new Get(Bytes.toBytes("RowKey" + rowIndex));
            Row result = mtable.get(myget);
            // since only column is deleted
            assertFalse(result.isEmpty());
            byte[] valueBeforeDel = Bytes.toBytes("Avinash" + rowIndex);
            // check what is value of column "NAME"
            List<Cell> cells = result.getCells();
            for (Cell cell : cells) {
              byte[] colName = cell.getColumnName();
              byte[] colVal = (byte[]) cell.getColumnValue();
              if (Bytes.compareTo(Bytes.toBytes("NAME"), colName) == 0 && rowIndex == 0) {
                assertNull(colVal);
              }
            }
          }

          // Delete non existent key from client, should get exception here.
          Delete delete = new Delete(Bytes.toBytes("Not existent Row Key"));
          try {
            mtable.delete(delete);
          } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("MTableDeleteOpDUnitTest.call -------- " + ex.getMessage());
            assertTrue(ex instanceof RowKeyDoesNotExistException);
          }

          // Delete non existent key from client, with non existent column should get exception
          // here.
          delete = new Delete(Bytes.toBytes("Not existent Row Key"));
          delete.addColumn(Bytes.toBytes("Non existent column"));
          try {
            mtable.delete(delete);
          } catch (Exception ex) {
            assertTrue(ex instanceof IllegalColumnNameException);
          }

          // Delete non existent key from client with existing column
          delete = new Delete(Bytes.toBytes("Not existent Row Key"));
          delete.addColumn(Bytes.toBytes("ID"));
          try {
            mtable.delete(delete);
          } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("MTableDeleteOpDUnitTest.call -------- " + ex.getMessage());
            assertTrue(ex instanceof RowKeyDoesNotExistException);
          }

        } catch (CacheClosedException cce) {
          cce.printStackTrace();
        }
        return null;
      }
    });

  }

  private void doDeleteRowWithAllColumns(VM vm, final int locatorPort, boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = null;
        try {
          cache = MCacheFactory.getAnyInstance();
          assertNotNull(cache);
          MTableDescriptor tableDescriptor = null;
          if (order == false) {
            tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
          } else {
            tableDescriptor = new MTableDescriptor();
          }

          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

          MTable mtable = cache.getAdmin().createTable("EmployeeTable", tableDescriptor);

          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

          // mtable.get
          for (int i = 0; i < numOfEntries; i++) {

            String key1 = "RowKey" + i;
            Put myput = new Put(Bytes.toBytes(key1));
            myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            Get myget = new Get(Bytes.toBytes("RowKey" + i));
            Iterator<MColumnDescriptor> iterColDes =
                tableDescriptor.getAllColumnDescriptors().iterator();
            Row result = mtable.get(myget);
            assertFalse(result.isEmpty());
            assertEquals(5, result.size());
            List<Cell> row = result.getCells();
            for (int k = 0; k < row.size() - 1; k++) {
              if (row.get(k).getColumnValue() == null) {
                Assert.fail("Not All column updated");
              }

              byte[] expectedColumnName = iterColDes.next().getColumnName();
              byte[] expectedColumnValue =
                  (byte[]) myput.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));

              if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
                System.out.println("expectedColumnName =>  " + Arrays.toString(expectedColumnName));
                System.out.println(
                    "actualColumnName   =>  " + Arrays.toString(row.get(k).getColumnName()));
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

          // mTable.delete()
          for (int rowIndex = 0; rowIndex < numOfEntries; rowIndex++) {
            String key1 = "RowKey" + rowIndex;
            Delete delete = new Delete(Bytes.toBytes(key1));
            delete.addColumn(Bytes.toBytes("NAME"));
            delete.addColumn(Bytes.toBytes("ID"));
            delete.addColumn(Bytes.toBytes("AGE"));
            delete.addColumn(Bytes.toBytes("SALARY"));
            /*
             * if(!order){ // for unordered we should expect exception try { mtable.delete(delete);
             * }catch(Exception e){ if(e instanceof TableInvalidConfiguration){ e.printStackTrace();
             * }else{
             * fail("Expected exception io.ampool.monarch.table.exceptions.TableInvalidConfiguration but got "
             * + e); } } return null; }else
             */
            mtable.delete(delete);

            // Verify the delete
            Get myget = new Get(Bytes.toBytes("RowKey" + rowIndex));
            Row result = mtable.get(myget);
            // though it is particular column delete, since all columns are deleted it should behave
            // like
            // complete row delete
            assertTrue(result.isEmpty());

            // Delete non existent key on server, should get exception here.
            delete = new Delete(Bytes.toBytes("Not existent Row Key"));
            try {
              mtable.delete(delete);
            } catch (Exception ex) {
              ex.printStackTrace();
              System.out.println("MTableDeleteOpDUnitTest.call -------- " + ex.getMessage());
              assertTrue(ex instanceof RowKeyDoesNotExistException);
            }

            // Delete non existent key on server, with non existent column should get exception
            // here.
            delete = new Delete(Bytes.toBytes("Not existent Row Key"));
            delete.addColumn(Bytes.toBytes("Non existent column"));
            try {
              mtable.delete(delete);
            } catch (Exception ex) {
              assertTrue(ex instanceof IllegalColumnNameException);
            }

            // Delete non existent key on server, with existing column
            delete = new Delete(Bytes.toBytes("Not existent Row Key"));
            delete.addColumn(Bytes.toBytes("ID"));
            try {
              mtable.delete(delete);
            } catch (Exception ex) {
              ex.printStackTrace();
              System.out.println("MTableDeleteOpDUnitTest.call -------- " + ex.getMessage());
              assertTrue(ex instanceof RowKeyDoesNotExistException);
            }
          }
          cache.getAdmin().deleteTable("EmployeeTable");
        } catch (CacheClosedException cce) {
          cce.printStackTrace();
        }
        return null;
      }
    });

  }

  private void doDeleteRowWithMultipleColumnsWithTimeStamp(boolean order) {
    MClientCache cache = null;
    try {
      cache = MClientCacheFactory.getAnyInstance();
      assertNotNull(cache);
      MTableDescriptor tableDescriptor = null;
      if (order == false) {
        tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
      } else {
        tableDescriptor = new MTableDescriptor();
      }
      tableDescriptor.setMaxVersions(5);

      tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
          .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

      MTable mtable = null;
      if (!cache.getAdmin().tableExists("EmployeeTable")) {
        mtable = cache.getAdmin().createTable("EmployeeTable", tableDescriptor);
      } else {
        mtable = cache.getTable("EmployeeTable");
      }

      Exception expectedException = null;
      long timestamp = 1;
      String key1 = "RowKey" + 0;
      Put put1 = new Put(Bytes.toBytes(key1));
      put1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("ABC"));
      put1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(100));
      put1.setTimeStamp(timestamp);
      try {
        mtable.put(put1);
      } catch (Exception ex) {
        expectedException = ex;
        if (mtable.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
          assertTrue(expectedException instanceof TableInvalidConfiguration);
        } else {
          Assert.fail("Should not get exception for ORDERED Table");
        }
      }

      if (mtable.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
        Delete delete1 = new Delete(Bytes.toBytes(key1));
        delete1.addColumn(Bytes.toBytes("NAME"));
        delete1.addColumn(Bytes.toBytes("SALARY"));
        delete1.setTimestamp(timestamp);
        mtable.delete(delete1);
      }

      Get get = new Get(Bytes.toBytes(key1));
      get.addColumn(Bytes.toBytes("NAME"));
      get.addColumn(Bytes.toBytes("SALARY"));
      get.setTimeStamp(timestamp);
      try {
        Row result1 = mtable.get(get);
        assertFalse(result1.isEmpty());
        List<Cell> row = result1.getCells();
        assertEquals(row.size(), 2);
      } catch (Exception ex) {
        expectedException = ex;
        if (mtable.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
          assertTrue(expectedException instanceof TableInvalidConfiguration);
        } else {
          Assert.fail("Should not get exception for ORDERED Table");
        }
      }

      if (mtable.getTableDescriptor().getMaxVersions() > 1) {
        timestamp = 2;
        put1 = new Put(Bytes.toBytes(key1));
        put1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(10));
        put1.setTimeStamp(timestamp);
        try {
          mtable.put(put1);
        } catch (Exception ex) {
          expectedException = ex;
          if (mtable.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
            assertTrue(expectedException instanceof TableInvalidConfiguration);
          } else {
            Assert.fail("Should not get exception for ORDERED Table");
          }
        }

        if (mtable.getTableDescriptor().getTableType() == MTableType.ORDERED_VERSIONED) {
          Delete delete1 = new Delete(Bytes.toBytes(key1));
          delete1.addColumn(Bytes.toBytes("AGE"));
          delete1.setTimestamp(timestamp);
          mtable.delete(delete1);
        }

        get = new Get(Bytes.toBytes(key1));
        get.addColumn(Bytes.toBytes("AGE"));
        get.setTimeStamp(timestamp);
        try {
          Row result1 = mtable.get(get);
          assertFalse(result1.isEmpty());
          List<Cell> row = result1.getCells();
          assertEquals(row.size(), 1);
        } catch (Exception ex) {
          expectedException = ex;
          if (mtable.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
            assertTrue(expectedException instanceof TableInvalidConfiguration);
          } else {
            Assert.fail("Should not get exception for ORDERED Table");
          }
        }
      }
    } catch (CacheClosedException cce) {
      cce.printStackTrace();
    }
  }

  private void doDeleteRowWithMultipleColumnsWithTimeStamp_2(boolean order) {
    MClientCache cache = null;
    String COLUMN_PREFIX = "COL", VALUE_PREFIX = "VAL";
    String[] keys_version = {"015", "016", "017", "018", "019", "020"};
    try {
      cache = MClientCacheFactory.getAnyInstance();
      assertNotNull(cache);
      MTableDescriptor mTableDescriptor = null;
      if (order == false) {
        mTableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
      } else {
        mTableDescriptor = new MTableDescriptor();
      }
      for (int i = 0; i < 5; i++) {
        mTableDescriptor.addColumn(Bytes.toBytes(COLUMN_PREFIX + i));
      }

      mTableDescriptor.setMaxVersions(5);
      MTable mtable = null;
      boolean tableExists = false;
      if (!cache.getAdmin().tableExists("EmployeeTable")) {
        mtable = cache.getAdmin().createTable("EmployeeTable", mTableDescriptor);
      } else {
        mtable = cache.getTable("EmployeeTable");
        tableExists = true;
      }

      if (tableExists) {
        int maxVersions = mtable.getTableDescriptor().getMaxVersions();
        Exception expectedException = null;
        for (int i = 0; i < maxVersions; i++) {
          Get get = new Get(keys_version[5]);
          get.setTimeStamp(i + 1);
          try {
            Row result1 = mtable.get(get);
            assertFalse(result1.isEmpty());
            int columnIndex = 0;
            List<Cell> row = result1.getCells();
            for (int k = 0; k < row.size() - 1; k++) {
              byte[] expectedColumnName = Bytes.toBytes(COLUMN_PREFIX + columnIndex);
              byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
              if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
                System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
                System.out.println(
                    "actualColumnName   => " + Arrays.toString(row.get(k).getColumnName()));
                Assert.fail("Invalid Values for Column Name");
              }
              if (i <= 2) {
                if (columnIndex == 3) {
                  assertEquals(row.get(k).getColumnValue(), null);
                } else {
                  if (!Bytes.equals(exptectedValue, (byte[]) row.get(k).getColumnValue())) {
                    System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
                    System.out.println("actualValue    => "
                        + Arrays.toString((byte[]) row.get(k).getColumnValue()));
                    Assert.fail("Invalid Values for Column Value");
                  }
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
            expectedException = ex;
            if (mtable.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
              assertTrue(expectedException instanceof TableInvalidConfiguration);
            } else {
              Assert.fail("Should not get exception here.");
            }
          }
        }
      }

      int maxVersions = mtable.getTableDescriptor().getMaxVersions();
      Exception expectedException = null;
      // Populate a row with all columns with timestamp and test.
      // Test rows with columns populating all versions until maxVersions, delete a column with
      // specific version.
      // Test asserts all columns less than equal to that version are deleted.
      if (maxVersions > 1) {
        for (int i = 0; i < maxVersions; i++) {
          Put put = new Put(keys_version[5]);
          for (int columnIndex = 0; columnIndex < 5; columnIndex++) {
            put.addColumn(Bytes.toBytes(COLUMN_PREFIX + columnIndex),
                Bytes.toBytes(VALUE_PREFIX + columnIndex));
          }
          put.setTimeStamp(i + 1);
          try {
            mtable.put(put);
          } catch (Exception ex) {
            expectedException = ex;
            if (mtable.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
              assertTrue(expectedException instanceof TableInvalidConfiguration);
            } else {
              Assert.fail("Should not get exception here.");
            }
          }
        }

        // Test again
        maxVersions = mtable.getTableDescriptor().getMaxVersions();
        expectedException = null;
        for (int i = 0; i < maxVersions; i++) {
          Get get = new Get(keys_version[5]);
          get.setTimeStamp(i + 1);
          try {
            Row result1 = mtable.get(get);
            assertFalse(result1.isEmpty());
            int columnIndex = 0;
            List<Cell> row = result1.getCells();
            for (int j = 0; j < row.size() - 1; j++) {
              byte[] expectedColumnName = Bytes.toBytes(COLUMN_PREFIX + columnIndex);
              byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
              if (!Bytes.equals(expectedColumnName, row.get(j).getColumnName())) {
                System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
                System.out.println(
                    "actualColumnName   => " + Arrays.toString(row.get(j).getColumnName()));
                Assert.fail("Invalid Values for Column Name");
              }
              if (!Bytes.equals(exptectedValue, (byte[]) row.get(j).getColumnValue())) {
                System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
                System.out.println(
                    "actualValue    => " + Arrays.toString((byte[]) row.get(j).getColumnValue()));
                Assert.fail("Invalid Values for Column Value");
              }
              columnIndex++;
            }
          } catch (Exception ex) {
            expectedException = ex;
            if (mtable.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
              assertTrue(expectedException instanceof TableInvalidConfiguration);
            } else {
              Assert.fail("Should not get exception here.");
            }
          }
        }

        Delete delete1 = new Delete(keys_version[5]);
        delete1.addColumn(Bytes.toBytes(COLUMN_PREFIX + 3));
        delete1.setTimestamp(3);
        mtable.delete(delete1);

        // Assert All of the rows data, only all columns less than equal to timestamp specified are
        // deleted.
        for (int i = 0; i < maxVersions; i++) {
          Get get = new Get(keys_version[5]);
          get.setTimeStamp(i + 1);
          try {
            Row result1 = mtable.get(get);
            assertFalse(result1.isEmpty());
            int columnIndex = 0;
            List<Cell> row = result1.getCells();
            for (int k = 0; k < row.size() - 1; k++) {
              byte[] expectedColumnName = Bytes.toBytes(COLUMN_PREFIX + columnIndex);
              byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);
              if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
                System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
                System.out.println(
                    "actualColumnName   => " + Arrays.toString(row.get(k).getColumnName()));
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
            expectedException = ex;
            if (mtable.getTableDescriptor().getTableType() == MTableType.UNORDERED) {
              assertTrue(expectedException instanceof TableInvalidConfiguration);
            } else {
              Assert.fail("Should not get exception here.");
            }
          }
        }
      }
    } catch (CacheClosedException cce) {
      cce.printStackTrace();
    }
  }

  private void deleteRowWitSingleColumns(final boolean order) throws Exception {
    createTableOn(vm3, order);
    doDeleteRowWitSingleColumns(vm3, getLocatorPort(), order);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable("EmployeeTable");

  }

  @Test
  public void testDeleteRowWitAllColumns() throws Exception {
    deleteRowWitAllColumns(true, false);
    deleteRowWitAllColumns(false, false);
  }

  @Test
  public void testDeleteRowWitAllColumnsExplicitlyGiven() throws Exception {
    deleteRowWitSingleColumns(true);
    deleteRowWitSingleColumns(false);

    doDeleteRowWithAllColumns(vm2, 0, false);
    doDeleteRowWithAllColumns(vm2, 0, true);
  }

  /**
   * New test to check delete with few columns
   */
  @Test
  public void testDeleteRowWithSingleColumns() throws Exception {
    deleteRowWitSingleColumns(true);
    deleteRowWitSingleColumns(false);
  }

  /**
   * Test to check delete with particular timestamp for single version table.
   */
  @Test
  public void testDeleteRowWithTimeStamp() throws Exception {
    deleteRowWitAllColumns(true, true);
    deleteRowWitAllColumns(false, true);
  }

  /**
   * New test to check delete with few columns
   */
  // Test for GEN-875
  @Test
  public void testDeleteRowWithMultipleColumnsWithTimeStamp() throws Exception {
    doDeleteRowWithMultipleColumnsWithTimeStampFrom(vm3, 0, true);
    doDeleteRowWithMultipleColumnsWithTimeStamp(true);
    doDeleteRowWithMultipleColumnsWithTimeStampFrom(vm3, 0, false);
    doDeleteRowWithMultipleColumnsWithTimeStamp(false);
  }

  private void doDeleteRowWithMultipleColumnsWithTimeStampFrom(VM vm, int i, boolean b) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doDeleteRowWithMultipleColumnsWithTimeStamp(b);
        return null;
      }
    });
  }


  /**
   * New test to check delete with few columns
   */
  // Test for GEN-889
  @Test
  public void testDeleteRowWithMultipleColumnsWithTimeStamp_2() throws Exception {
    doDeleteRowWithMultipleColumnsWithTimeStampFrom_2(vm3, 0, true);
    doDeleteRowWithMultipleColumnsWithTimeStamp_2(true);
    /*
     * doDeleteRowWithMultipleColumnsWithTimeStampFrom_2(vm3, 0, false);
     * doDeleteRowWithMultipleColumnsWithTimeStamp_2(false);
     */
  }

  private void doDeleteRowWithMultipleColumnsWithTimeStampFrom_2(VM vm, int i, boolean b) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doDeleteRowWithMultipleColumnsWithTimeStamp_2(b);
        return null;
      }
    });
  }

  /**
   * New test to check delete with filter
   */
  @Test
  public void testDeleteWithFilter() throws Exception {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    tableDescriptor.addColumn("NAME", BasicTypes.STRING).addColumn("ID", BasicTypes.INT)
        .addColumn("AGE", BasicTypes.INT).addColumn("SALARY", BasicTypes.LONG);
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setMaxVersions(10);
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createMTable("EmployeeTable", tableDescriptor);
    assertEquals("EmployeeTable", mtable.getName());

    for (int i = 0; i < 1; i++) {
      for (int verison = 0; verison < tableDescriptor.getMaxVersions(); verison++) {
        String key1 = "RowKey" + i;
        Put myput1 = new Put(Bytes.toBytes(key1));
        myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("ABC" + i));
        myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(verison));
        myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
        myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
        mtable.put(myput1);
      }
    }

    Map<Delete, Filter> deleteFilterMap = new HashMap<>();
    for (int i = 0; i < 1; i++) {
      String key1 = "RowKey" + i;
      Delete delete = new Delete(key1);
      deleteFilterMap.put(delete, new SingleColumnValueFilter("ID", CompareOp.LESS, 5));
    }

    mtable.delete(deleteFilterMap);

    Scan scan = new Scan();
    scan.setMaxVersions();
    Iterator<Row> iterator = mtable.getScanner(scan).iterator();
    AtomicInteger rowCount = new AtomicInteger(0);
    while (iterator.hasNext()) {
      Map<Long, SingleVersionRow> allVersions = iterator.next().getAllVersions();
      allVersions.entrySet().forEach((E) -> {
        System.out.println(E.getKey());
        System.out.println(E.getValue());
        if (!E.getValue().isEmpty()) {
          rowCount.incrementAndGet();
          E.getValue().getCells().forEach((C) -> {
            System.out.println(Bytes.toString(C.getColumnName()));
            System.out.println(C.getColumnValue());
          });
        }
      });
    }

    assertEquals(5, rowCount.get());
    admin.deleteMTable("EmployeeTable");
  }
}
