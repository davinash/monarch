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
import io.ampool.monarch.table.internal.ByteArrayKey;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;


@Category(MonarchTest.class)
public class MTableMultiClientVersionedOpDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();
  VM server1, server2, server3;

  public MTableMultiClientVersionedOpDUnitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    server1 = vm0;
    server2 = vm1;
    server3 = vm2;

    startServerOn(server1, DUnitLauncher.getLocatorString());
    startServerOn(server2, DUnitLauncher.getLocatorString());
    startServerOn(server3, DUnitLauncher.getLocatorString());

    createClientCache(client1);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache(client1);
    closeMClientCache();
    super.tearDown2();
  }

  public Map<String, Put> doPutsFromClient2(final int locatorPort) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable("EmployeeTable");
    if (table == null) {
      Assert.fail("Client 2 does not have access to Table EmployeeTable");
    }

    Map<String, Put> rowKeyToMPut = new HashMap<>();

    String rowKey = "RowKey1";
    Put putrecord = TableHelper.createPutRecord(rowKey, "Avinash1", 002, 37, 100, 01, "01-01-2000");
    table.put(putrecord);

    putrecord = TableHelper.createPutRecord(rowKey, "Avinash2", 003, 38, 200, 02, "01-01-2001");
    table.put(putrecord);

    putrecord = TableHelper.createPutRecord(rowKey, "Avinash3", 004, 39, 300, 03, "01-01-2002");
    table.put(putrecord);

    rowKeyToMPut.put(rowKey, putrecord);

    return rowKeyToMPut;

  }

  private void doGetFromClient1(VM vm, final int locatorPort, final Map<String, Put> testData) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MClientCacheFactory.getAnyInstance().getTable("EmployeeTable");
        MTableDescriptor tableDescriptor =
            MClientCacheFactory.getAnyInstance().getMTableDescriptor("EmployeeTable");

        if (table == null) {
          Assert.fail("Client 1 does not have access to Table EmployeeTable");
        }

        for (Map.Entry<String, Put> eachRecord : testData.entrySet()) {
          Get getRecord = new Get(Bytes.toBytes(eachRecord.getKey()));


          Iterator<Map.Entry<MColumnDescriptor, Integer>> iterColDes =
              tableDescriptor.getColumnDescriptorsMap().entrySet().iterator();

          Row result = table.get(getRecord);
          assertFalse(result.isEmpty());
          List<Cell> row = result.getCells();

          for (int k = 0; k < row.size() - 1; k++) {
            if (row.get(k).getColumnValue() == null) {
              Assert.fail("Not All column updated");
            }
            byte[] expectedColumnName = iterColDes.next().getKey().getColumnName();
            byte[] expectedColumnValue = (byte[]) eachRecord.getValue().getColumnValueMap()
                .get(new ByteArrayKey(expectedColumnName));

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

  private Map<String, Put> doPutsFromClient2WithTimeStamp(int locatorPort) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    MTable table = clientCache.getTable("EmployeeTable");
    if (table == null) {
      Assert.fail("Client 2 does not have access to Table EmployeeTable");
    }

    Map<String, Put> rowKeyToMPut = new HashMap<>();

    String rowKey = "RowKey1";
    Put putrecord = TableHelper.createPutRecord(rowKey, "Avinash1", 002, 37, 100, 01, "01-01-2000");
    putrecord.setTimeStamp(100L);
    table.put(putrecord);

    putrecord = TableHelper.createPutRecord(rowKey, "Avinash2", 003, 38, 200, 02, "01-01-2001");
    putrecord.setTimeStamp(200L);
    table.put(putrecord);

    putrecord = TableHelper.createPutRecord(rowKey, "Avinash3", 004, 39, 300, 03, "01-01-2002");
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
        Assert.assertEquals(tableDescriptor.getMaxVersions(), 5);

        if (table == null) {
          Assert.fail("Client 1 does not have access to Table EmployeeTable");
        }

        {
          String rowKey = "RowKey1";
          Put eachRecord =
              TableHelper.createPutRecord(rowKey, "Avinash2", 003, 38, 200, 02, "01-01-2001");
          eachRecord.setTimeStamp(100L);

          Get getRecord = new Get(Bytes.toBytes(rowKey));
          getRecord.setTimeStamp(eachRecord.getTimeStamp());

          Row result = table.get(getRecord);
          if (result.isEmpty() == false) {
            Assert.fail("Result is not empty and should be");
          }
          // assertTrue(result.isEmpty());
        }

        String rowKey = "RowKey1";
        Put eachRecord =
            TableHelper.createPutRecord(rowKey, "Avinash2", 003, 38, 200, 02, "01-01-2001");
        eachRecord.setTimeStamp(200L);

        Get getRecord = new Get(Bytes.toBytes(rowKey));
        getRecord.setTimeStamp(eachRecord.getTimeStamp());

        Iterator<Map.Entry<MColumnDescriptor, Integer>> iterColDes =
            tableDescriptor.getColumnDescriptorsMap().entrySet().iterator();

        Row result = table.get(getRecord);
        assertFalse(result.isEmpty());

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

  public MTable createEmployeeTable(MClientCache clientCache, final boolean ordered) {

    MTableDescriptor tableDescriptor = null;
    if (ordered) {
      tableDescriptor = new MTableDescriptor();
    } else {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    }

    tableDescriptor.setMaxVersions(5);
    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"))
        .addColumn(Bytes.toBytes("DEPT")).addColumn(Bytes.toBytes("DOJ"));

    Admin admin = clientCache.getAdmin();
    return admin.createTable("EmployeeTable", tableDescriptor);

  }


  public void createTableFromClient1(VM vm, final int locatorPort, final boolean ordered) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        MTable table = createEmployeeTable(clientCache, ordered);
        return null;
      }
    });
  }

  @Test
  public void testMultipleClientVersionedOp() {

    createTableFromClient1(client1, getLocatorPort(), true);

    Map<String, Put> testData = doPutsFromClient2(getLocatorPort());
    doGetFromClient1(client1, getLocatorPort(), testData);
    testData = doPutsFromClient2WithTimeStamp(getLocatorPort());
    doGetFromClient1WithTimeStamp(client1, getLocatorPort(), testData);
  }

  @Test
  public void testMultipleClientVersionedOpUnordered() {

    createTableFromClient1(client1, getLocatorPort(), false);

    Map<String, Put> testData = doPutsFromClient2(getLocatorPort());
    doGetFromClient1(client1, getLocatorPort(), testData);
    testData = doPutsFromClient2WithTimeStamp(getLocatorPort());
    doGetFromClient1WithTimeStamp(client1, getLocatorPort(), testData);
  }
}
