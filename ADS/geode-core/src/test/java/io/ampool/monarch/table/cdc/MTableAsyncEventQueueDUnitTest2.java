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
package io.ampool.monarch.table.cdc;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.CDCInformation;
import junit.framework.Assert;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

@Category(MonarchTest.class)
public class MTableAsyncEventQueueDUnitTest2 extends MTableDUnitHelper {
  public String OP_FULL_PUT = "FULL_PUT";
  public String OP_FULL_UPDATE = "FULL_UPDATE";
  public String OP_PARTIAL_PUT = "PARTIAL_PUT";
  public String OP_PARTIAL_UPDATE = "PARTIAL_UPDATE";

  public MTableAsyncEventQueueDUnitTest2() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  Host host = Host.getHost(0);
  private VM vm0 = host.getVM(0);
  private VM vm1 = host.getVM(1);
  private VM vm2 = host.getVM(2);
  private VM client1 = host.getVM(3);

  private final int NUM_OF_COLUMNS = 3;
  private final String KEY_PREFIX = "KEY";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 1;
  private final String COLUMN_NAME_PREFIX = "COLUMN";
  private final int LATEST_TIMESTAMP = 300;
  private final int MAX_VERSIONS = 5;
  private final int TABLE_MAX_VERSIONS = 7;


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
    super.tearDown2();
  }

  public void createTable(final boolean ordered, String tName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    String tableName = tName;
    if (ordered) {
      tableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
      tableName = tableName + "_ORDERED";
      CDCConfig cdcConfig = tableDescriptor.createCDCConfig();
      cdcConfig.setBatchSize(500).setBatchConflationEnabled(false).setBatchTimeInterval(5)
          .setDispatcherThreads(10).setMaximumQueueMemory(500).setPersistent(true)
          .setDiskSynchronous(true);
      tableDescriptor.addCDCStream(tableName + "1",
          "io.ampool.monarch.table.cdc.MTableEventListener2", cdcConfig); // io.ampool.monarch.table.cdc.MTableEventListener2
      tableDescriptor.addCDCStream(tableName + "2",
          "io.ampool.monarch.table.cdc.MTableEventListener2", null);

    } else {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
      tableName = tableName + "_UNORDERED";
      CDCConfig cdcConfig = tableDescriptor.createCDCConfig();
      cdcConfig.setBatchSize(500).setBatchConflationEnabled(false).setBatchTimeInterval(5)
          .setDispatcherThreads(10).setMaximumQueueMemory(500).setPersistent(true)
          .setDiskSynchronous(true);
      tableDescriptor.addCDCStream(tableName + "1",
          "io.ampool.monarch.table.cdc.MTableEventListener2", cdcConfig);
      tableDescriptor.addCDCStream(tableName + "2",
          "io.ampool.monarch.table.cdc.MTableEventListener2", null);
    }
    tableDescriptor.setMaxVersions(5);
    tableDescriptor.addColumn(Bytes.toBytes("OPERATION"));

    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1);
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(tableName, tableDescriptor);
    assertNotNull(table);
    assertEquals(table.getName(), tableName);
  }

  protected void createTableOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(true, null);
        return null;
      }
    });
  }

  public void deleteTable(boolean ordered, String tName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    Admin admin = clientCache.getAdmin();

    String tableName = tName;
    if (ordered) {
      tableName = tableName + "_ORDERED";
    } else {
      tableName = tableName + "_UNORDERED";
    }
    admin.deleteTable(tableName);
  }

  protected void deleteTableOn(VM vm, boolean ordered, String tName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        deleteTable(ordered, tName);
        return null;
      }
    });
  }

  private void createTableOn(VM vm, final boolean ordered, String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(ordered, tableName);
        return null;
      }
    });
  }

  private void doFullPuts(boolean ordered, String tName) {
    String tableName = tName;
    if (ordered) {
      tableName = tableName + "_ORDERED";
    } else {
      tableName = tableName + "_UNORDERED";
    }
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      // record.addColumn(Bytes.toBytes("OPERATION"), Bytes.toBytes(OP_FULL_PUT));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      System.out.println("Doing PUTS ");
      table.put(record);
    }
  }

  private void doFullUpdates(boolean ordered, String tName) {
    String tableName = tName;
    if (ordered) {
      tableName = tableName + "_ORDERED";
    } else {
      tableName = tableName + "_UNORDERED";
    }
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      // record.addColumn(Bytes.toBytes("OPERATION"), Bytes.toBytes(OP_FULL_UPDATE));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes("U_" + VALUE_PREFIX + columnIndex));
      }
      System.out.println("Doing Full updates ");
      table.put(record);
    }
  }

  private void doFullPutFrom(VM vm, boolean ordered, String tName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doFullPuts(ordered, tName);
        return null;
      }
    });
  }

  private void doFullUpdateFrom(VM vm, boolean ordered, String tName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doFullUpdates(ordered, tName);
        return null;
      }
    });
  }

  private void doPartialPuts(boolean ordered, String tName) {
    String tableName = tName;
    if (ordered) {
      tableName = tableName + "_ORDERED";
    } else {
      tableName = tableName + "_UNORDERED";
    }
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      // record.addColumn(Bytes.toBytes("OPERATION"), Bytes.toBytes(OP_PARTIAL_PUT));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex += 2) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      System.out.println("Doing Partial PUTS ");
      table.put(record);
    }
  }

  private void doPartialUpdates(boolean ordered, String tName) {
    String tableName = tName;
    if (ordered) {
      tableName = tableName + "_ORDERED";
    } else {
      tableName = tableName + "_UNORDERED";
    }
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      // record.addColumn(Bytes.toBytes("OPERATION"), Bytes.toBytes(OP_PARTIAL_UPDATE));
      for (int columnIndex = 1; columnIndex < NUM_OF_COLUMNS; columnIndex += 2) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes("U_" + VALUE_PREFIX + columnIndex));
      }
      System.out.println("Doing Partial UPDATES");
      table.put(record);
    }
  }


  private void doPartialPutFrom(VM vm, boolean ordered, String tName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPartialPuts(ordered, tName);
        return null;
      }
    });
  }

  private void doPartialUpdateFrom(VM vm, boolean ordered, String tName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPartialUpdates(ordered, tName);
        return null;
      }
    });
  }

  private void doDeleteFullRows(boolean ordered, String tName) {
    String tableName = tName;
    if (ordered) {
      tableName = tableName + "_ORDERED";
    } else {
      tableName = tableName + "_UNORDERED";
    }
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Delete record = new Delete(Bytes.toBytes(KEY_PREFIX + rowIndex));
      System.out.println("Doing Full row DELETE");
      table.delete(record);
    }
  }

  private void doDeleteFullRowOn(VM vm, boolean ordered, String tName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doDeleteFullRows(ordered, tName);
        return null;
      }
    });
  }


  private void doDeletePartialRows(boolean ordered, String tName) {
    String tableName = tName;
    if (ordered) {
      tableName = tableName + "_ORDERED";
    } else {
      tableName = tableName + "_UNORDERED";
    }
    MTable table = MClientCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Delete record = new Delete(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 1; columnIndex < NUM_OF_COLUMNS; columnIndex += 2) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex));
      }
      System.out.println("Doing Partial row DELETE");
      table.delete(record);
    }
  }

  private void doDeletePartialRowOn(VM vm, boolean ordered, String tName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doDeletePartialRows(ordered, tName);
        return null;
      }
    });
  }

  /**
   * 1. Create Table from client 1 2. Create AsyncEventQueue From Client 2
   */
  @Test
  public void testAsyncEventQueue() {
    AsyncEventQueue(true);
    AsyncEventQueue(false);
  }

  public void AsyncEventQueue(boolean ordered) {
    createTableOn(this.client1, ordered, "AsyncEventQueue_A");
    verifyCDCStreamConfig(this.vm1, ordered, "AsyncEventQueue_A");
    createTableOn(this.client1, ordered, "AsyncEventQueue_B");
    verifyCDCStreamConfig(this.vm2, ordered, "AsyncEventQueue_B");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueue_A");
    verifyCDCData(this.client1, ordered, "AsyncEventQueue_A");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueue_B");
    doFullUpdateFrom(this.client1, ordered, "AsyncEventQueue_A");
    doFullUpdateFrom(this.client1, ordered, "AsyncEventQueue_B");
    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    deleteTableOn(this.client1, ordered, "AsyncEventQueue_A");
    deleteTableOn(this.client1, ordered, "AsyncEventQueue_B");
  }

  private void verifyCDCData(VM vm, boolean ordered, String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyCDCStreamData(ordered, tableName);
        return null;
      }
    });
  }

  private void verifyCDCStreamData(boolean ordered, String tableName) {

  }

  private void verifyCDCStreamConfig(VM vm, boolean ordered, String tableName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyCDCStream(ordered, tableName);
        return null;
      }
    });
  }

  private void verifyCDCStream(boolean ordered, String tableName) {

    String listenerClassPath = "io.ampool.monarch.table.cdc.MTableEventListener2";

    if (ordered) {
      tableName = tableName + "_ORDERED";
    } else {
      tableName = tableName + "_UNORDERED";
    }
    String[] queueIds = {tableName + "1", tableName + "2"};
    String[] unqueueIds = {tableName + "1", tableName + "2"};
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);
    CDCConfig cdcConfig = table.getTableDescriptor().createCDCConfig();
    cdcConfig.setBatchSize(500).setBatchConflationEnabled(false).setBatchTimeInterval(5)
        .setDispatcherThreads(10).setMaximumQueueMemory(500).setPersistent(true)
        .setDiskSynchronous(true);
    Assert.assertNotNull(table);
    ArrayList<CDCInformation> cdcInformations = table.getTableDescriptor().getCdcInformations();
    Assert.assertEquals(2, cdcInformations.size());
    for (int i = 0; i < cdcInformations.size(); i++) {
      CDCInformation cdcInformation = cdcInformations.get(i);
      Assert.assertNotNull(cdcInformation);
      if (ordered) {
        Assert.assertEquals(cdcInformation.getQueueId(), queueIds[i]);
        Assert.assertEquals(cdcInformation.getListenerClassPath(), listenerClassPath);
        if (i == 0) {
          Assert.assertEquals(cdcInformation.getCDCConfig(), cdcConfig);
        } else {
          Assert.assertEquals(cdcInformation.getCDCConfig(), null);
        }
      } else {
        Assert.assertEquals(cdcInformation.getQueueId(), unqueueIds[i]);
        Assert.assertEquals(cdcInformation.getListenerClassPath(), listenerClassPath);
        if (i == 0) {
          Assert.assertEquals(cdcInformation.getCDCConfig(), cdcConfig);
        } else {
          Assert.assertEquals(cdcInformation.getCDCConfig(), null);
        }
      }
    }
  }

  @Test
  public void testAsyncEventQueueFullPuts() {
    AsyncEventQueueFullPuts(true);
    AsyncEventQueueFullPuts(false);
  }

  public void AsyncEventQueueFullPuts(boolean ordered) {
    createTableOn(this.client1, ordered, "AsyncEventQueueFullPuts_A");
    verifyCDCStreamConfig(this.vm1, ordered, "AsyncEventQueueFullPuts_A");
    createTableOn(this.client1, ordered, "AsyncEventQueueFullPuts_B");
    verifyCDCStreamConfig(this.vm2, ordered, "AsyncEventQueueFullPuts_B");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueueFullPuts_A");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueueFullPuts_B");
    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    deleteTableOn(this.client1, ordered, "AsyncEventQueueFullPuts_A");
    deleteTableOn(this.client1, ordered, "AsyncEventQueueFullPuts_B");
  }

  @Test
  public void testAsyncEventQueueFullPutFullUpdates() {
    AsyncEventQueueFullPutFullUpdates(true);
    AsyncEventQueueFullPutFullUpdates(false);
  }

  public void AsyncEventQueueFullPutFullUpdates(boolean ordered) {
    createTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdates_A");
    verifyCDCStreamConfig(this.vm2, ordered, "AsyncEventQueueFullPutFullUpdates_A");
    createTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdates_B");
    verifyCDCStreamConfig(this.vm1, ordered, "AsyncEventQueueFullPutFullUpdates_B");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdates_A");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdates_B");
    doFullUpdateFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdates_A");
    doFullUpdateFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdates_B");

    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    deleteTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdates_A");
    deleteTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdates_B");
  }

  @Test
  public void testAsyncEventQueuePartialPutFullUpdates() {
    AsyncEventQueuePartialPutFullUpdates(true);
    AsyncEventQueuePartialPutFullUpdates(false);
  }

  public void AsyncEventQueuePartialPutFullUpdates(boolean ordered) {
    createTableOn(this.client1, ordered, "AsyncEventQueuePartialPutFullUpdates_A");
    verifyCDCStreamConfig(this.vm1, ordered, "AsyncEventQueuePartialPutFullUpdates_A");
    createTableOn(this.client1, ordered, "AsyncEventQueuePartialPutFullUpdates_B");
    verifyCDCStreamConfig(this.vm2, ordered, "AsyncEventQueuePartialPutFullUpdates_B");
    doPartialPutFrom(this.client1, ordered, "AsyncEventQueuePartialPutFullUpdates_A");
    doPartialPutFrom(this.client1, ordered, "AsyncEventQueuePartialPutFullUpdates_B");
    doFullUpdateFrom(this.client1, ordered, "AsyncEventQueuePartialPutFullUpdates_A");
    doFullUpdateFrom(this.client1, ordered, "AsyncEventQueuePartialPutFullUpdates_B");

    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    deleteTableOn(this.client1, ordered, "AsyncEventQueuePartialPutFullUpdates_A");
    deleteTableOn(this.client1, ordered, "AsyncEventQueuePartialPutFullUpdates_B");
  }

  @Test
  public void testAsyncEventQueuePartialPuts() {
    AsyncEventQueuePartialPuts(true);
    AsyncEventQueuePartialPuts(false);
  }

  public void AsyncEventQueuePartialPuts(boolean ordered) {
    createTableOn(this.client1, ordered, "AsyncEventQueuePartialPuts_A");
    verifyCDCStreamConfig(this.vm1, ordered, "AsyncEventQueuePartialPuts_A");
    createTableOn(this.client1, ordered, "AsyncEventQueuePartialPuts_B");
    verifyCDCStreamConfig(this.vm2, ordered, "AsyncEventQueuePartialPuts_B");
    doPartialPutFrom(this.client1, ordered, "AsyncEventQueuePartialPuts_A");
    doPartialPutFrom(this.client1, ordered, "AsyncEventQueuePartialPuts_B");
    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    deleteTableOn(this.client1, ordered, "AsyncEventQueuePartialPuts_A");
    deleteTableOn(this.client1, ordered, "AsyncEventQueuePartialPuts_B");
  }

  @Test
  public void testAsyncEventQueueFullPutPartialUpdates() {
    AsyncEventQueueFullPutPartialUpdates(true);
    AsyncEventQueueFullPutPartialUpdates(false);
  }

  public void AsyncEventQueueFullPutPartialUpdates(boolean ordered) {
    createTableOn(this.client1, ordered, "AsyncEventQueueFullPutPartialUpdates_A");
    verifyCDCStreamConfig(this.vm2, ordered, "AsyncEventQueueFullPutPartialUpdates_A");
    createTableOn(this.client1, ordered, "AsyncEventQueueFullPutPartialUpdates_B");
    verifyCDCStreamConfig(this.vm1, ordered, "AsyncEventQueueFullPutPartialUpdates_B");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueueFullPutPartialUpdates_A");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueueFullPutPartialUpdates_B");
    doPartialUpdateFrom(this.client1, ordered, "AsyncEventQueueFullPutPartialUpdates_A");
    doPartialUpdateFrom(this.client1, ordered, "AsyncEventQueueFullPutPartialUpdates_B");
    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    deleteTableOn(this.client1, ordered, "AsyncEventQueueFullPutPartialUpdates_A");
    deleteTableOn(this.client1, ordered, "AsyncEventQueueFullPutPartialUpdates_B");
  }

  @Test
  public void testAsyncEventQueuePartialPutPartialUpdates() {
    AsyncEventQueuePartialPutPartialUpdates(true);
    AsyncEventQueuePartialPutPartialUpdates(false);
  }

  public void AsyncEventQueuePartialPutPartialUpdates(boolean ordered) {
    createTableOn(this.client1, ordered, "AsyncEventQueuePartialPutPartialUpdates_A");
    verifyCDCStreamConfig(this.vm1, ordered, "AsyncEventQueuePartialPutPartialUpdates_A");
    createTableOn(this.client1, ordered, "AsyncEventQueuePartialPutPartialUpdates_B");
    verifyCDCStreamConfig(this.vm2, ordered, "AsyncEventQueuePartialPutPartialUpdates_B");
    doPartialPutFrom(this.client1, ordered, "AsyncEventQueuePartialPutPartialUpdates_A");
    doPartialPutFrom(this.client1, ordered, "AsyncEventQueuePartialPutPartialUpdates_B");
    doPartialUpdateFrom(this.client1, ordered, "AsyncEventQueuePartialPutPartialUpdates_A");
    doPartialUpdateFrom(this.client1, ordered, "AsyncEventQueuePartialPutPartialUpdates_B");
    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    deleteTableOn(this.client1, ordered, "AsyncEventQueuePartialPutPartialUpdates_A");
    deleteTableOn(this.client1, ordered, "AsyncEventQueuePartialPutPartialUpdates_B");
  }

  @Test
  public void testAsyncEventQueueFullPutFullUpdatesFullDelete() {
    AsyncEventQueueFullPutFullUpdatesFullDelete(true);
    AsyncEventQueueFullPutFullUpdatesFullDelete(false);
  }

  public void AsyncEventQueueFullPutFullUpdatesFullDelete(boolean ordered) {
    createTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_A");
    verifyCDCStreamConfig(this.vm2, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_A");
    createTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_B");
    verifyCDCStreamConfig(this.vm1, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_B");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_A");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_B");
    doFullUpdateFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_A");
    doFullUpdateFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_B");
    doDeleteFullRowOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_A");
    doDeleteFullRowOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_B");
    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    deleteTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_A");
    deleteTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesFullDelete_B");
  }

  @Test
  public void testAsyncEventQueueFullPutFullUpdatesPartialDelete() {
    AsyncEventQueueFullPutFullUpdatesPartialDelete(true);
    AsyncEventQueueFullPutFullUpdatesPartialDelete(false);
  }

  public void AsyncEventQueueFullPutFullUpdatesPartialDelete(boolean ordered) {
    createTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_A");
    verifyCDCStreamConfig(this.vm1, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_A");
    createTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_B");
    verifyCDCStreamConfig(this.vm2, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_B");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_A");
    doFullPutFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_B");
    doFullUpdateFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_A");
    doFullUpdateFrom(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_B");
    doDeletePartialRowOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_A");
    doDeletePartialRowOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_B");
    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    deleteTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_A");
    deleteTableOn(this.client1, ordered, "AsyncEventQueueFullPutFullUpdatesPartialDelete_B");
  }
}
