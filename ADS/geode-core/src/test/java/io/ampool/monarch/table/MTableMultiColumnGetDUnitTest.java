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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Category(MonarchTest.class)
public class MTableMultiColumnGetDUnitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();

  public MTableMultiColumnGetDUnitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());

    createClientCache(this.vm3);

  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache(this.vm3);
    super.tearDown2();
  }

  private Put createPutRecord(final String rowkey, final String name, final int id, final int age,
      final int salary) {
    Put putRecord = new Put(Bytes.toBytes(rowkey));
    putRecord.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes(name));
    putRecord.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(id));
    putRecord.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(age));
    putRecord.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(salary));

    return putRecord;

  }

  private Map<String, Put> doSomeInitialPuts(final MTable mtable) {

    Map<String, Put> rowKeyToMPut = new HashMap<>();

    String rowKey = "ABCD";
    Put putRecord = createPutRecord(rowKey, "Avinash", 002, 37, 100);
    mtable.put(putRecord);
    rowKeyToMPut.put(rowKey, putRecord);

    rowKey = "RowKey2";
    putRecord = createPutRecord(rowKey, "Nilkanth", 003, 35, 200);
    mtable.put(putRecord);
    rowKeyToMPut.put(rowKey, putRecord);

    rowKey = "RowKey3";
    putRecord = createPutRecord(rowKey, "Manish", 004, 37, 300);
    mtable.put(putRecord);
    rowKeyToMPut.put(rowKey, putRecord);

    rowKey = "RowKey4";
    putRecord = createPutRecord(rowKey, "Amey", 005, 33, 400);
    mtable.put(putRecord);
    rowKeyToMPut.put(rowKey, putRecord);

    return rowKeyToMPut;

  }

  private void doAllColumnGets(MTable mtable, Map<String, Put> rowKeyToMPut)
      throws IOException, ClassNotFoundException {

    for (Map.Entry<String, Put> eachRecord : rowKeyToMPut.entrySet()) {
      Get getRecord = new Get(Bytes.toBytes(eachRecord.getKey()));
      Row result = mtable.get(getRecord);
      assertFalse(result.isEmpty());

      Iterator<MColumnDescriptor> iteratorColumnDescriptor =
          mtable.getTableDescriptor().getAllColumnDescriptors().iterator();
      List<Cell> row = result.getCells();
      for (int k = 0; k < row.size() - 1; k++) {
        if (row.get(k).getColumnValue() == null) {
          Assert.fail("Not All column updated");
        }
        byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
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
  }

  private void doPartialColumnGets(MTable mtable, Map<String, Put> rowKeyToMPut)
      throws IOException, ClassNotFoundException {
    for (Map.Entry<String, Put> eachRecord : rowKeyToMPut.entrySet()) {
      Get getRecord = new Get(Bytes.toBytes(eachRecord.getKey()));

      ArrayList<MColumnDescriptor> partialColumnList = new ArrayList<>();

      Random generator = new Random();
      List<MColumnDescriptor> columnList = mtable.getTableDescriptor().getAllColumnDescriptors();

      MColumnDescriptor eachMcds = columnList.get(0);
      getRecord.addColumn(eachMcds.getColumnName());
      logger.info("Requesting Get for ColumnName {}", eachMcds.getColumnName());
      partialColumnList.add(eachMcds);

      eachMcds = columnList.get(1);
      getRecord.addColumn(eachMcds.getColumnName());
      logger.info("Requesting Gets for ColumnName {}", eachMcds.getColumnName());
      partialColumnList.add(eachMcds);

      logger.info("Number Of Records {}", getRecord.getColumnNameList().size());

      Row result = mtable.get(getRecord);
      assertFalse(result.isEmpty());



      Iterator<MColumnDescriptor> iteratorColumnDescriptor = partialColumnList.iterator();
      List<Cell> row = result.getCells();
      for (int k = 0; k < row.size() - 1; k++) {

        logger.info("Got Tuple ColumnName {} and ColumnValue {}", row.get(k).getColumnName(),
            row.get(k).getColumnValue());
        byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
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
  }

  public void createTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    MTableDescriptor tableDescriptor = new MTableDescriptor();

    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

    tableDescriptor.setRedundantCopies(1);

    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable("EmployeeTable", tableDescriptor);
    assertEquals("EmployeeTable", mtable.getName());

  }

  private void doMultiColumnGetOp(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        createTable();
        MTable mtable = MClientCacheFactory.getAnyInstance().getTable("EmployeeTable");
        Map<String, Put> rowKeyToMPut = doSomeInitialPuts(mtable);

        doAllColumnGets(mtable, rowKeyToMPut);

        doPartialColumnGets(mtable, rowKeyToMPut);


        return null;
      }
    });
  }


  @Test
  public void testMultiColumnGetOp() {
    doMultiColumnGetOp(vm3);

  }

}
