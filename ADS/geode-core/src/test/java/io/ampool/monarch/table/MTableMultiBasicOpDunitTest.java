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

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MTableKey;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;
import java.io.IOException;
import java.util.*;

@Category(MonarchTest.class)
public class MTableMultiBasicOpDunitTest extends MTableDUnitHelper {
  private static final Logger logger = LogService.getLogger();


  public MTableMultiBasicOpDunitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.vm3);

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(this.vm3);

    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MCacheFactory.getAnyInstance().close();
            return null;
          }
        }));

    super.tearDown2();
  }

  /* following is the test data will be used for verification */
  private Map<MTableKey, Put> testRowKeyToMPut = new HashMap<>();
  private Random random = new Random(System.currentTimeMillis());

  private void restTestData() {
    testRowKeyToMPut.clear();
  }

  private int getRandomInRange(int min, int max) {
    return random.nextInt((max - min) + 1) + min;
  }

  private Put createPutRecord(MTableDescriptor tableDescriptor, byte[] rowKey, int valueLengh) {
    Put putRecord = new Put(rowKey);

    for (MColumnDescriptor columnDescriptor : tableDescriptor.getAllColumnDescriptors()) {
      byte[] value = new byte[valueLengh];
      new Random().nextBytes(value);
      putRecord.addColumn(columnDescriptor.getColumnName(), value);
    }
    return putRecord;
  }

  private byte[] generateRowKey(int maxLength) {
    int rowKeyLength = getRandomInRange(1, maxLength);
    byte[] value = new byte[rowKeyLength];
    new Random().nextBytes(value);
    return value;
  }

  public void doPuts(final MTable table, int numOfRecord, int maxRowKeyLength, int maxValueLength)
      throws IOException, ClassNotFoundException {
    logger.info("Inserting {} of Records", numOfRecord);

    for (int i = 0; i < numOfRecord; i++) {
      byte[] rowKey = generateRowKey(maxRowKeyLength);
      Put putRecord =
          createPutRecord(table.getTableDescriptor(), rowKey, getRandomInRange(1, maxValueLength));
      if (i % 100 == 0) {
        logger.info("Inserted Records {}", i);
      }
      table.put(putRecord);

      testRowKeyToMPut.put(new MTableKey(rowKey), putRecord);
    }
  }

  private void doUpdates(MTable table, int numOfRecord, int maxRowKeyLen, int maxValueLen)
      throws IOException, ClassNotFoundException {
    byte[] rowKey = generateRowKey(maxRowKeyLen);
    for (int i = 0; i < numOfRecord; i++) {
      Put putRecord =
          createPutRecord(table.getTableDescriptor(), rowKey, getRandomInRange(1, maxValueLen));
      if (i % 100 == 0) {
        logger.info("Update Records {}", i);
      }
      table.put(putRecord);
    }
  }


  public MTable createTable(final String tableName, int numOfColumns, int maxColumnNameLength,
      final boolean order) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);

    MTableDescriptor tableDescriptor = null;
    if (order == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    for (int i = 0; i < numOfColumns; i++) {
      byte[] columnName = new byte[maxColumnNameLength];
      new Random().nextBytes(columnName);
      tableDescriptor = tableDescriptor.addColumn(columnName);
    }
    tableDescriptor.setRedundantCopies(1);
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(tableName, tableDescriptor);
    assertEquals(tableName, table.getName());
    return table;
  }

  private void doRowTupleBasicPutOperation(VM vm, final boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        int numOfColumns = 10;
        int maxColumnNameLength = 128;

        MTable table = createTable("TestTable", numOfColumns, maxColumnNameLength, order);
        int numOfRecord = 100;
        int maxRowKeyLen = 128;
        int maxValueLen = 1024;
        doPuts(table, numOfRecord, maxRowKeyLen, maxValueLen);
        return null;
      }
    });
  }

  private void doRowTupleBasicUpdateOperation(VM vm, final boolean order) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        int numOfColumns = 10;
        int maxColumnNameLength = 128;

        MTable table = createTable("TestTable1", numOfColumns, maxColumnNameLength, order);
        int numOfRecord = 100;
        int maxRowKeyLen = 128;
        int maxValueLen = 1024;
        doUpdates(table, numOfRecord, maxRowKeyLen, maxValueLen);
        return null;
      }
    });
  }

  private void rowtupleBasicOp(final boolean order) {
    doRowTupleBasicPutOperation(vm3, order);
    doRowTupleBasicUpdateOperation(vm3, order);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable("TestTable");
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable("TestTable1");

  }

  @Test
  public void testRowtupleBasicOp() {
    rowtupleBasicOp(true);
    rowtupleBasicOp(false);
  }


}
