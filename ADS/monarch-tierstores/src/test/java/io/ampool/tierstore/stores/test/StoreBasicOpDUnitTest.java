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

package io.ampool.tierstore.stores.test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.store.DefaultConstructorMissingException;
import io.ampool.store.StoreCreateException;
import io.ampool.tierstore.stores.store.StoreDunitHelper;
import io.ampool.store.StoreHandler;
import io.ampool.store.StoreRecord;
import io.ampool.store.StoreScan;
import io.ampool.tierstore.wal.WALRecord;
import io.ampool.tierstore.wal.WALResultScanner;
import io.ampool.tierstore.internal.ConverterUtils;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.utils.TimestampUtil;

@Category(FTableTest.class)
public class StoreBasicOpDUnitTest extends StoreDunitHelper {

  public StoreBasicOpDUnitTest() {
    super();
  }

  Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);
  private VM vm2 = host.getVM(2);
  private VM client1 = host.getVM(3);

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "StoreBasicOpDUnitTest";
  private final String KEY_PREFIX = "KEY";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 100;
  private final String COLUMN_NAME_PREFIX = "COLUMN";
  private final byte[] byteData = new byte[] {12, 45, 65, 66, 67, 75, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
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
            MCacheFactory.getAnyInstance().close();
            return null;
          }
        }));

    super.tearDown2();
  }

  private void createTable()
      throws ClassNotFoundException, DefaultConstructorMissingException, StoreCreateException {

    MCache cache = MCacheFactory.getAnyInstance();
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex),
          BasicTypes.BINARY);
    }
    tableDescriptor.setRedundantCopies(1);
    Admin admin = cache.getAdmin();
    FTable table = admin.createFTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  private void appendRecordsToStoreHandler() {
    MCache cache = MCacheFactory.getAnyInstance();
    StoreHandler storeHandler = cache.getStoreHandler();
    assertNotNull(storeHandler);

    for (int i = 0; i < 113; i++) {
      try {
        final BlockValue bv = new BlockValue(1000);
        bv.checkAndAddRecord(byteData);
        storeHandler.append(TABLE_NAME, i, new BlockKey(byteData), bv);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void scanStoreHandler() { // scan from storeHandler
    MCache cache = MCacheFactory.getAnyInstance();
    StoreHandler storeHandler = cache.getStoreHandler();
    for (int i = 0; i < 113; i++) {
      WALResultScanner scanner = storeHandler.getWALScanner(TABLE_NAME, i);
      WALRecord next = scanner.next();
      assertNotNull(next);
    }
  }

  private byte[] incrementByteArray(byte[] arr) {
    int a = Bytes.toInt(arr);
    a++;
    return Bytes.toBytes(a);
  }

  private void createAndAppendRecordsToLocalStore() throws StoreCreateException,
      DefaultConstructorMissingException, ClassNotFoundException, IOException {

    FTableDescriptor ftd = new FTableDescriptor();
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      ftd.addColumn("COL" + i, BasicTypes.BINARY);
    }
    // for test purpose we need to add this
    ftd.addColumn(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME, BasicTypes.LONG);

    TierStore localStore = StoreHandler.getInstance().getTierStore(TABLE_NAME, 1);
    Integer data1 = 1;
    int numRecords = 5001;

    StoreRecord[] storeRecords = new StoreRecord[numRecords];
    for (int i = 0; i < storeRecords.length; i++) {
      StoreRecord storeRecord = new StoreRecord(NUM_OF_COLUMNS + 1);
      for (int j = 0; j < NUM_OF_COLUMNS; j++) {
        storeRecord.addValue(Bytes.toBytes(data1));
      }
      storeRecord.addValue(TimestampUtil.getCurrentTime());
      storeRecords[i] = storeRecord;
    }

    TierStoreWriter localStoreWriter = localStore.getWriter(TABLE_NAME, 0);
    localStoreWriter.setConverterDescriptor(localStore.getConverterDescriptor(TABLE_NAME));
    int append = localStoreWriter.write(new Properties(), storeRecords);
    assertEquals(storeRecords.length, append);


    int count = 0;

    TierStoreReader reader = localStore.getReader(TABLE_NAME, 0);
    reader.setConverterDescriptor(localStore.getConverterDescriptor(TABLE_NAME));
    reader.setFilter(new StoreScan(new Scan()));
    Iterator<StoreRecord> iterator = reader.iterator();
    assertNotNull(iterator);
    StoreRecord result = null;
    while (iterator.hasNext()) {
      result = iterator.next();
      if (result != null) {
        // System.out.println("StoreBasicOpDUnitTest.createTable " + count + " nextkey = " +
        // Arrays.toString(result.getRowKey()));
        // System.out.println("StoreBasicOpDUnitTest.createTable " + count + " nextvalue = " +
        // Arrays.toString((result.getRowValue())));
        final Object[] values = result.getValues();
        for (int j = 0; j < NUM_OF_COLUMNS; j++) {
          // System.out.println("StoreBasicOpDUnitTest.createAndAppendRecordsToLocalStore :: value
          // class: "+values[j].getClass());
          assertTrue(values[j] instanceof byte[]);
          assertEquals(data1.intValue(), Bytes.toInt((byte[]) values[j]));
        }
        count++;
      }
    }
    assertEquals(numRecords, count);


    for (int i = 1; i < 10; i++) {
      final TierStoreWriter lw = localStore.getWriter(TABLE_NAME, i);
      lw.setConverterDescriptor(localStore.getConverterDescriptor(TABLE_NAME));
      append = lw.write(new Properties(), storeRecords);
      assertEquals(storeRecords.length, append);
    }


    for (int i = 1; i < 10; i++) {
      count = 0;
      reader = localStore.getReader(TABLE_NAME, i);
      reader.setConverterDescriptor(localStore.getConverterDescriptor(TABLE_NAME));
      reader.setFilter(new StoreScan(new Scan()));
      iterator = reader.iterator();
      assertNotNull(iterator);
      result = null;
      while (iterator.hasNext()) {
        result = iterator.next();
        if (result != null) {
          final Object[] values = result.getValues();
          for (int j = 0; j < NUM_OF_COLUMNS; j++) {
            assertTrue(values[j] instanceof byte[]);
            assertEquals(data1.intValue(), Bytes.toInt((byte[]) values[j]));
          }
          count++;
        }
      }
      assertEquals(numRecords, count);
    }

    TierStoreWriter lw = localStore.getWriter(TABLE_NAME, 11);
    lw.setConverterDescriptor(localStore.getConverterDescriptor(TABLE_NAME));
    append = lw.write(new Properties(), storeRecords);
    assertEquals(storeRecords.length, append);

    count = 0;
    reader = localStore.getReader(TABLE_NAME, 11);
    reader.setConverterDescriptor(localStore.getConverterDescriptor(TABLE_NAME));
    reader.setFilter(new StoreScan(new Scan()));
    iterator = reader.iterator();
    assertNotNull(iterator);
    result = null;
    while (iterator.hasNext()) {
      result = iterator.next();
      if (result != null) {
        final Object[] values = result.getValues();
        for (int j = 0; j < NUM_OF_COLUMNS; j++) {
          assertTrue(values[j] instanceof byte[]);
          assertEquals(data1.intValue(), Bytes.toInt((byte[]) values[j]));
        }
        count++;
      }
    }
    assertEquals(numRecords, count);

    localStore.deleteTable(TABLE_NAME);
  }

  private void doCreateTable(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable();
        return null;
      }
    });
  }

  private void doAppendRecordsToStoreHandler(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        appendRecordsToStoreHandler();
        return null;
      }
    });
  }

  private void doScanStoreHandler(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        scanStoreHandler();
        return null;
      }
    });
  }

  private void doCreateAndAppendRecordsToLocalStore(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createAndAppendRecordsToLocalStore();
        return null;
      }
    });
  }

  @Test
  public void testBasicOpWithStore()
      throws DefaultConstructorMissingException, ClassNotFoundException, StoreCreateException {
    doCreateTable(client1);
    doAppendRecordsToStoreHandler(vm0);
    doScanStoreHandler(vm0);
    doCreateAndAppendRecordsToLocalStore(vm0);
    doCreateAndAppendRecordsToLocalStore(vm1);
    doCreateAndAppendRecordsToLocalStore(vm2);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }
}
