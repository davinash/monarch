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

import org.apache.geode.cache.Region;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.*;
import org.junit.Test;

/**
 * Created by Nilkanth Patel
 */
@Category(MonarchTest.class)
public class MTableMultiClientDeleteOpDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();
  VM server1, server2, server3;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    server1 = vm0;
    server2 = vm1;
    server3 = vm2;
    startServerOn(this.server1, DUnitLauncher.getLocatorString());
    startServerOn(this.server2, DUnitLauncher.getLocatorString());
    startServerOn(this.server3, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(this.client1);
    super.tearDown2();
  }

  public MTableMultiClientDeleteOpDUnitTest() {
    super();
  }

  public MTable createEmployeeTable(MClientCache clientCache) {

    MTableDescriptor tableDescriptor = new MTableDescriptor();

    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"))
        .addColumn(Bytes.toBytes("DEPT")).addColumn(Bytes.toBytes("DOJ"));

    tableDescriptor.setRedundantCopies(1).setMaxVersions(5);
    Admin admin = clientCache.getAdmin();
    return admin.createTable("EmployeeTable", tableDescriptor);
  }

  public MTable createEmployeeTable(MClientCache clientCache, final boolean ordered) {

    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setMaxVersions(5);


    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"))
        .addColumn(Bytes.toBytes("DEPT")).addColumn(Bytes.toBytes("DOJ"));

    tableDescriptor.setRedundantCopies(1);
    Admin admin = clientCache.getAdmin();
    return admin.createTable("EmployeeTable", tableDescriptor);
  }

  public void createTableFromClient1(VM vm, final int locatorPort, final boolean ordered) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        assertNotNull(clientCache);
        MTable table = createEmployeeTable(clientCache, ordered);
        assertEquals("EmployeeTable", table.getName());

        return null;
      }
    });
  }

  public /* Map<String, MPut> */void doInsertRowsFromClient2(final int locatorPort,
      final boolean ordered) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable("EmployeeTable");
    if (table == null) {
      Assert.fail("Client 2 does not have access to Table EmployeeTable");
    }

    Map<String, Put> rowKeyToMPut = new HashMap<>();

    Put putrecord = null;

    String rowKey1 = "RowKey1";
    // NAME, ID, AGE, SALARY, DEPT, DOJ
    {
      putrecord =
          TableHelper.createPutRecord(rowKey1, "Nilkanth11", 001, 11, 1000, 01, "01-01-2011");
      if (ordered) {
        putrecord.setTimeStamp(100L);
      }
      table.put(putrecord);

      putrecord =
          TableHelper.createPutRecord(rowKey1, "Nilkanth12", 002, 12, 2000, 02, "01-01-2012");
      if (ordered) {
        putrecord.setTimeStamp(1000L);
      }
      table.put(putrecord);

      putrecord =
          TableHelper.createPutRecord(rowKey1, "Nilkanth13", 003, 13, 3000, 03, "01-01-2013");
      table.put(putrecord);
    }

    String rowKey2 = "RowKey2";
    {
      putrecord =
          TableHelper.createPutRecord(rowKey2, "Nilkanth21", 021, 21, 5000, 01, "01-01-2021");
      if (ordered) {
        putrecord.setTimeStamp(200L);
      }
      table.put(putrecord);

      putrecord =
          TableHelper.createPutRecord(rowKey2, "Nilkanth22", 022, 22, 6000, 02, "01-01-2022");
      if (ordered) {
        putrecord.setTimeStamp(2000L);
      }
      table.put(putrecord);

      putrecord =
          TableHelper.createPutRecord(rowKey2, "Nilkanth23", 023, 23, 7000, 03, "01-01-2023");
      table.put(putrecord);
    }

    String rowKey3 = "RowKey3";
    {
      putrecord =
          TableHelper.createPutRecord(rowKey3, "Nilkanth31", 031, 31, 10000, 01, "01-01-2031");
      if (ordered) {
        putrecord.setTimeStamp(300L);
      }
      table.put(putrecord);

      putrecord =
          TableHelper.createPutRecord(rowKey3, "Nilkanth32", 032, 32, 12000, 02, "01-01-2032");
      if (ordered) {
        putrecord.setTimeStamp(3000L);
      }
      table.put(putrecord);

      putrecord =
          TableHelper.createPutRecord(rowKey3, "Nilkanth33", 033, 33, 13000, 03, "01-01-2033");
      table.put(putrecord);
    }
    // rowKeyToMPut.put(rowKey, putrecord);

    // return rowKeyToMPut;

  }

  public void doDeleteAndGetRowFromClient2(final int locatorPort, final String rowKey) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    MTable table = clientCache.getTable("EmployeeTable");
    if (table == null) {
      Assert.fail("Client 2 does not have access to Table EmployeeTable");
    }

    //
    // mTable.delete()
    // String rowKey1 = "RowKey1";
    // for ( int i = 0; i < numOfEntries; i++) {
    Delete delete = new Delete(Bytes.toBytes(rowKey));
    table.delete(delete);

    // Verify the delete
    Get myget = new Get(Bytes.toBytes(rowKey));
    Row result = table.get(myget);
    assertTrue(result.isEmpty());
    assertEquals(0, result.size());

    /*
     * if(result == null) System.out.println("Nilkanth:: doDeletesFromClient2 result is NULL!");
     */

  }

  private void doGetDeletedRowFromClient1(VM vm, final int locatorPort,
      final String rowKey/* , final Map<String, MPut> testData */) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        // Verify the delete. Get op on key must return null as client2 has deleted this row before.
        // String rowKey1 = "RowKey1";

        MTable table = MClientCacheFactory.getAnyInstance().getTable("EmployeeTable");
        if (table == null) {
          Assert.fail("Client 1 does not have access to Table EmployeeTable");
        }
        // for ( int i = 0; i < numOfEntries; i++) {

        // Verify the delete by retrieving the data for key
        Get myget = new Get(Bytes.toBytes(rowKey));
        Row result = table.get(myget);
        assertTrue(result.isEmpty());
        assertEquals(0, result.size());

        /*
         * if(result == null) System.out.println("Nilkanth:: doGetFromClient1 result is NULL!");
         */
        return null;
      }
    });
  }


  private void doDeleteRowColumnsFromClient1(VM vm, final int locatorPort, final boolean ordered
  /* , final Map<String, MPut> testData */) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {

        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        assertNotNull(clientCache);
        MTable table = clientCache.getTable("EmployeeTable");
        if (table == null) {
          Assert.fail("Client 2 does not have access to Table EmployeeTable");
        }

        //
        // mTable.delete() for selected columns
        String rowKey2 = "RowKey2";
        // for ( int i = 0; i < numOfEntries; i++) {
        Delete delete = new Delete(Bytes.toBytes(rowKey2));
        if (ordered) {
          delete.addColumn(Bytes.toBytes("AGE"));
          delete.addColumn(Bytes.toBytes("DEPT"));
        }

        table.delete(delete);

        List<ByteArrayKey> deletedColumns = new ArrayList<ByteArrayKey>();

        if (ordered) {
          deletedColumns.add(new ByteArrayKey(Bytes.toBytes("AGE")));
          deletedColumns.add(new ByteArrayKey(Bytes.toBytes("DEPT")));
        }

        Get getRecord = new Get(Bytes.toBytes(rowKey2));
        Row result = table.get(getRecord);
        if (result.isEmpty())
          System.out.println("Nilkanth Result is Null!");
        if (ordered) {
          assertFalse(result.isEmpty());
        } else {
          assertTrue(result.isEmpty());
        }

        if (ordered) {
          MTableDescriptor tableDescriptor = clientCache.getMTableDescriptor("EmployeeTable");

          Iterator<Map.Entry<MColumnDescriptor, Integer>> iterColDes =
              tableDescriptor.getColumnDescriptorsMap().entrySet().iterator();


          // MPut expectedRecord = TableHelper.createPutRecord(rowKey2, "Nilkanth2", 003, 0, 200, 0,
          // "01-01-2001");
          Put expectedRecord =
              TableHelper.createPutRecord(rowKey2, "Nilkanth23", 023, 23, 7000, 03, "01-01-2023");
          List<Cell> row = result.getCells();
          for (int k = 0; k < row.size() - 1; k++) {


            // verify deleted column value is null
            // System.out.println("Nilkanth ColumnName = " + Arrays.toString(cell.getColumnName()));
            // System.out.println("Nilkanth ColumnValue = " +
            // Arrays.toString(cell.getColumnValue()));

            byte[] expectedColumnName = iterColDes.next().getKey().getColumnName();
            byte[] expectedColumnValue = (byte[]) expectedRecord.getColumnValueMap()
                .get(new ByteArrayKey(expectedColumnName));

            if (deletedColumns.contains(new ByteArrayKey(row.get(k).getColumnName()))) {
              Assert.assertNull(row.get(k).getColumnValue());
              continue;
            }

            logger.info("Expected ColumnName  {} Actual ColumnName  {}", expectedColumnName,
                row.get(k).getColumnName());
            logger.info("Expected ColumnValue {} Actual ColumnValue {}", expectedColumnValue,
                row.get(k).getColumnValue());

            if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
              Assert.fail("Invalid Values for Column Name");
            }

            if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
              Assert.fail("Invalid Values for Column Value");
            }

          }
        }
        return null;

      }
    });
  }

  public void doVerifyTimestampVersionEntryFromClient2(final int locatorPort)
      throws IOException, ClassNotFoundException {
    verifyTimestampDelete();
  }

  private void verifyTimestampDelete() throws IOException, ClassNotFoundException {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    MTable table = clientCache.getTable("EmployeeTable");
    if (table == null) {
      Assert.fail("Client 2 does not have access to Table EmployeeTable");
    }

    //
    // mTable.delete() for selected columns
    String rowKey3 = "RowKey3";
    // for ( int i = 0; i < numOfEntries; i++) {
    Delete delete = new Delete(Bytes.toBytes(rowKey3));
    delete.setTimestamp(3000L);

    table.delete(delete);
    // Entry at specified timestamp deleted.

    // Verify delete by retrieving the same entry
    Get getRecord = new Get(Bytes.toBytes(rowKey3));
    getRecord.setTimeStamp(3000L);
    Row result = table.get(getRecord);
    if (result.isEmpty())
      System.out.println("Nilkanth Result is Null!");
    assertTrue(result.isEmpty());

    getRecord.setTimeStamp(300L);
    result = table.get(getRecord);
    if (result.isEmpty())
      System.out.println("Nilkanth Result is Null!");
    assertTrue(result.isEmpty());

    getRecord.setTimeStamp(0L);
    result = table.get(getRecord);
    if (result.isEmpty())
      System.out.println("Nilkanth Result is Null!");
    assertFalse(result.isEmpty());

    MTableDescriptor tableDescriptor = clientCache.getMTableDescriptor("EmployeeTable");

    Iterator<Map.Entry<MColumnDescriptor, Integer>> iterColDes =
        tableDescriptor.getColumnDescriptorsMap().entrySet().iterator();


    // MPut expectedRecord = TableHelper.createPutRecord(rowKey2, "Nilkanth2", 003, 0, 200, 0,
    // "01-01-2001");
    Put expectedRecord =
        TableHelper.createPutRecord(rowKey3, "Nilkanth33", 033, 33, 13000, 03, "01-01-2033");
    List<Cell> row = result.getCells();
    for (int k = 0; k < row.size() - 1; k++) {

      // verify deleted column value is null
      // System.out.println("Nilkanth ColumnName = " + Arrays.toString(cell.getColumnName()));
      // System.out.println("Nilkanth ColumnValue = " + Arrays.toString(cell.getColumnValue()));

      byte[] expectedColumnName = iterColDes.next().getKey().getColumnName();
      byte[] expectedColumnValue =
          (byte[]) expectedRecord.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));

      logger.info("Expected ColumnName  {} Actual ColumnName  {}", expectedColumnName,
          row.get(k).getColumnName());
      logger.info("Expected ColumnValue {} Actual ColumnValue {}", expectedColumnValue,
          row.get(k).getColumnValue());

      if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
        Assert.fail("Invalid Values for Column Name");
      }

      if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
        Assert.fail("Invalid Values for Column Value");
      }
    }
  }

  private void doDeleteTimestampVersionEntryFromClient1(VM vm,
      final int locatorPort/* , final Map<String, MPut> testData */) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyTimestampDelete();
        return null;
      }
    });
  }

  //
  public void doVerifyDeletedColumnsFromClient2(final int locatorPort, final boolean ordered)
      throws IOException, ClassNotFoundException {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable table = clientCache.getMTable("EmployeeTable");
    if (table == null) {
      Assert.fail("Client 2 does not have access to Table EmployeeTable");
    }

    // mTable.delete() for selected columns
    String rowKey2 = "RowKey2";
    // for ( int i = 0; i < numOfEntries; i++) {

    List<ByteArrayKey> deletedColumns = new ArrayList<ByteArrayKey>();
    deletedColumns.add(new ByteArrayKey(Bytes.toBytes("AGE")));
    deletedColumns.add(new ByteArrayKey(Bytes.toBytes("DEPT")));

    Get getRecord = new Get(Bytes.toBytes(rowKey2));
    Row result = table.get(getRecord);
    if (ordered) {
      assertFalse(result.isEmpty());
    } else {
      assertTrue(result.isEmpty());
    }

    if (ordered) {
      MTableDescriptor tableDescriptor = clientCache.getMTableDescriptor("EmployeeTable");

      Iterator<Map.Entry<MColumnDescriptor, Integer>> iterColDes =
          tableDescriptor.getColumnDescriptorsMap().entrySet().iterator();


      // MPut expectedRecord = TableHelper.createPutRecord(rowKey2, "Nilkanth2", 003, 0, 200, 0,
      // "01-01-2001");
      Put expectedRecord =
          TableHelper.createPutRecord(rowKey2, "Nilkanth23", 023, 23, 7000, 03, "01-01-2023");

      List<Cell> row = result.getCells();
      for (int k = 0; k < row.size() - 1; k++) {

        // verify deleted column value is null
        // System.out.println("Nilkanth ColumnName = " + Arrays.toString(cell.getColumnName()));
        // System.out.println("Nilkanth ColumnValue = " + Arrays.toString(cell.getColumnValue()));

        byte[] expectedColumnName = iterColDes.next().getKey().getColumnName();
        byte[] expectedColumnValue =
            (byte[]) expectedRecord.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));

        if (deletedColumns.contains(new ByteArrayKey(row.get(k).getColumnName()))) {
          Assert.assertNull(row.get(k).getColumnValue());
          continue;
        }

        logger.info("Expected ColumnName  {} Actual ColumnName  {}", expectedColumnName,
            row.get(k).getColumnName());
        logger.info("Expected ColumnValue {} Actual ColumnValue {}", expectedColumnValue,
            row.get(k).getColumnValue());

        if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
          Assert.fail("Invalid Values for Column Name");
        }

        if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
          Assert.fail("Invalid Values for Column Value");
        }
      }

      // Verify that a delete has propogated to entries/versions having timestamp smaller then the
      // timestamp specified in delete op.
      // Just above we have performed delete (rowKey2) on latest timestamp.
      // Verify that this delete has propogated to smaller timestamps for same key.

      getRecord = new Get(Bytes.toBytes(rowKey2));
      if (ordered) {
        getRecord.setTimeStamp(200L);
      }
      result = table.get(getRecord);
      Assert.assertNotNull(result);

      expectedRecord =
          TableHelper.createPutRecord(rowKey2, "Nilkanth21", 021, 21, 5000, 01, "01-01-2021");
      iterColDes = tableDescriptor.getColumnDescriptorsMap().entrySet().iterator();

      row = result.getCells();
      for (int k = 0; k < row.size() - 1; k++) {

        byte[] expectedColumnName = iterColDes.next().getKey().getColumnName();
        byte[] expectedColumnValue =
            (byte[]) expectedRecord.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));

        if (deletedColumns.contains(new ByteArrayKey(row.get(k).getColumnName()))) {
          Assert.assertNull(row.get(k).getColumnValue());
          continue;
        }

        /*
         * System.out.println("Expected ColumnName " + Arrays.toString(expectedColumnName) +
         * "  Actual ColumnName " + Arrays.toString(cell.getColumnName()));
         * System.out.println("Expected ColumnValue " + Arrays.toString(expectedColumnValue) +
         * "  Actual ColumnValue " + Arrays.toString(cell.getColumnValue()));
         */

        if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
          Assert.fail("Invalid Values for Column Name");
        }

        if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
          Assert.fail("Invalid Values for Column Value");
        }
      }
    }

  }

  private Map<String, Put> doPutsFromClient2WithTimeStamp(int locatorPort) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable table = clientCache.getTable("EmployeeTable");
    if (table == null) {
      Assert.fail("Client 2 does not have access to Table EmployeeTable");
    }

    Map<String, Put> rowKeyToMPut = new HashMap<>();

    String rowKey = "RowKey1";
    Put putrecord = TableHelper.createPutRecord(rowKey, "Nilkanth", 002, 37, 100, 01, "01-01-2000");
    putrecord.setTimeStamp(100L);
    table.put(putrecord);

    putrecord = TableHelper.createPutRecord(rowKey, "Nilkanth2", 003, 38, 200, 02, "01-01-2001");
    putrecord.setTimeStamp(200L);
    table.put(putrecord);

    putrecord = TableHelper.createPutRecord(rowKey, "Nilkanth3", 004, 39, 300, 03, "01-01-2002");
    table.put(putrecord);

    rowKeyToMPut.put(rowKey, putrecord);

    return rowKeyToMPut;
  }

  private void doGetFromClient1WithTimeStamp(VM vm, final int locatorPort,
      final Map<String, Put> testData) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MClientCacheFactory.getAnyInstance().getTable("EmployeeTable");
        MTableDescriptor tableDescriptor =
            MClientCacheFactory.getAnyInstance().getMTableDescriptor("EmployeeTable");

        if (table == null) {
          Assert.fail("Client 1 does not have access to Table EmployeeTable");
        }

        String rowKey = "RowKey1";
        Put eachRecord =
            TableHelper.createPutRecord(rowKey, "Nilkanth1", 002, 37, 100, 01, "01-01-2000");
        eachRecord.setTimeStamp(100L);

        // for (Map.Entry<String, MPut> eachRecord : testData.entrySet()) {
        Get getRecord = new Get(Bytes.toBytes(rowKey));
        getRecord.setTimeStamp(eachRecord.getTimeStamp());

        Row result = table.get(getRecord);
        assertFalse(result.isEmpty());
        assertEquals(5, result.size());


        Iterator<Map.Entry<MColumnDescriptor, Integer>> iterColDes =
            tableDescriptor.getColumnDescriptorsMap().entrySet().iterator();

        List<Cell> row = result.getCells();
        for (int k = 0; k < row.size() - 1; k++) {

          if (row.get(k).getColumnValue() == null) {
            Assert.fail("Not All column updated");
          }
          byte[] expectedColumnName = iterColDes.next().getKey().getColumnName();
          byte[] expectedColumnValue =
              (byte[]) eachRecord.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));

          logger.info("Expected ColumnName  {} Actual ColumnName  {}", expectedColumnName,
              row.get(k).getColumnName());
          logger.info("Expected ColumnValue {} Actual ColumnValue {}", expectedColumnValue,
              row.get(k).getColumnValue());

          if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
            Assert.fail("Invalid Values for Column Name");
          }

          if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
            Assert.fail("Invalid Values for Column Value");
          }
        }
        return null;
      }
    });
  }

  private void multipleClientDeleteOps(final boolean ordered)
      throws IOException, ClassNotFoundException {

    createTableFromClient1(this.client1, getLocatorPort(), ordered);

    // Testcase: Delete a given row. keys with single (Non-versioned) value
    /* Map<String, MPut> testData = */
    doInsertRowsFromClient2(getLocatorPort(), ordered);

    /* Test same row multiple times and verify */
    doDeleteAndGetRowFromClient2(getLocatorPort(), "RowKey1");
    doGetDeletedRowFromClient1(this.client1, getLocatorPort(), "RowKey1");
    doDeleteAndGetRowFromClient2(getLocatorPort(), "RowKey1");
    doGetDeletedRowFromClient1(this.client1, getLocatorPort(), "RowKey1");

    // TODO: enable this.
    // Testcase: Delete values of specified columns for a given row. keys with single
    // (Non-versioned) value
    doDeleteRowColumnsFromClient1(client1, getLocatorPort(), ordered);
    doVerifyDeletedColumnsFromClient2(getLocatorPort(), ordered);

    // Testcase: Given a timestamp, delete a corresponding version entry for the given key
    if (ordered) {
      doDeleteTimestampVersionEntryFromClient1(client1, getLocatorPort());
      doVerifyTimestampVersionEntryFromClient2(getLocatorPort());
    }
    // doGetFromClient1WithTimeStamp(client1, getLocatorPort(), testData);*/

    // TestCase: Given a timestamp, and set of colums to be deleted, perform delete op.

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable("EmployeeTable");
  }

  @Test
  public void testMultipleClientDeleteOps() throws IOException, ClassNotFoundException {
    multipleClientDeleteOps(true);
    multipleClientDeleteOps(false);
  }
}
