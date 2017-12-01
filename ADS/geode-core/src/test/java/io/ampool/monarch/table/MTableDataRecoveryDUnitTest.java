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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.ProxyMTableRegion;
import io.ampool.monarch.types.DataTypeFactory;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

@Category(MonarchTest.class)
public class MTableDataRecoveryDUnitTest extends MTableDUnitHelper {
  public MTableDataRecoveryDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();
  private static final String MTABLE_ORDERED = "MTABLE_ORDERED";
  private static final String MTABLE_UNORDERED = "MTABLE_UNORDERED";


  private static final String mtableOrderedSnaphost = "mtable/v1/mtable_ordered.gfd";
  private static final String mtableUnOrderedSnaphost = "mtable/v1/mtable_unordered.gfd";
  private static final String mtableOrderedSnaphost_v11 = "mtable/v11/mtable_ordered.gfd";
  private static final String mtableUnOrderedSnaphost_v11 = "mtable/v11/mtable_unordered.gfd";


  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  private void closeAllMCache() {
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
  }

  private void verifyMTableDataOnAllClientVM(final VM clientVM) {
    clientVM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyMTableData(true);
        return null;
      }
    });
  }

  private void verifyMTableData(boolean isClient) {
    ProxyMTableRegion mTable_ordered = null;
    ProxyMTableRegion mTable_unordered = null;

    if (!isClient) {
      mTable_ordered = (ProxyMTableRegion) MCacheFactory.getAnyInstance().getTable(MTABLE_ORDERED);
      mTable_unordered =
          (ProxyMTableRegion) MCacheFactory.getAnyInstance().getTable(MTABLE_UNORDERED);
    } else {
      mTable_ordered =
          (ProxyMTableRegion) MClientCacheFactory.getAnyInstance().getTable(MTABLE_ORDERED);
      mTable_unordered =
          (ProxyMTableRegion) MClientCacheFactory.getAnyInstance().getTable(MTABLE_UNORDERED);
    }

    final Iterator<Row> mResultIterator = mTable_ordered.getScanner(new Scan()).iterator();
    int numRows = 0;
    while (mResultIterator.hasNext()) {
      final Row row = mResultIterator.next();
      assertNotNull(row);
      System.out.println(Bytes.toString(row.getRowId()));
      assertEquals(2, row.getCells().size());
      final Iterator<Cell> mCellIterator = row.getCells().iterator();
      while (mCellIterator.hasNext()) {
        final Cell cell = mCellIterator.next();
        System.out.println(Bytes.toString(cell.getColumnName()));
        System.out.println(Bytes.toString((byte[]) cell.getColumnValue()));
      }
      numRows++;
    }
    assertEquals(10, numRows);

    for (int i = 1; i <= 10; i++) {
      Get mget = new Get(Bytes.toBytes(i + ""));
      final Row row = mTable_ordered.get(mget);
      assertEquals(i + "", Bytes.toString(row.getRowId()));
      assertEquals(2, row.getCells().size());
      Cell cell1 = row.getCells().get(0);
      Cell cell2 = row.getCells().get(1);
      assertEquals("ID", Bytes.toString(cell1.getColumnName()));
      assertEquals(i + "", Bytes.toString((byte[]) cell1.getColumnValue()));
      assertEquals("NAME", Bytes.toString(cell2.getColumnName()));
      assertEquals("A" + i, Bytes.toString((byte[]) cell2.getColumnValue()));
    }

    final Iterator<Row> mResultIteratorUnOrdered =
        mTable_unordered.getScanner(new Scan()).iterator();
    numRows = 0;
    while (mResultIteratorUnOrdered.hasNext()) {
      final Row row = mResultIteratorUnOrdered.next();
      System.out.println(Bytes.toString(row.getRowId()));
      assertNotNull(row);
      assertEquals(2, row.getCells().size());
      final Iterator<Cell> mCellIterator = row.getCells().iterator();
      while (mCellIterator.hasNext()) {
        final Cell cell = mCellIterator.next();
        System.out.println(Bytes.toString(cell.getColumnName()));
        System.out.println(Bytes.toString((byte[]) cell.getColumnValue()));
      }
      numRows++;
    }
    assertEquals(10, numRows);

    for (int i = 1; i <= 10; i++) {
      Get mget = new Get(Bytes.toBytes(i + ""));
      final Row row = mTable_unordered.get(mget);
      assertEquals(i + "", Bytes.toString(row.getRowId()));
      assertEquals(2, row.getCells().size());
      Cell cell1 = row.getCells().get(0);
      Cell cell2 = row.getCells().get(1);
      assertEquals("ID", Bytes.toString(cell1.getColumnName()));
      assertEquals(i + "", Bytes.toString((byte[]) cell1.getColumnValue()));
      assertEquals("NAME", Bytes.toString(cell2.getColumnName()));
      assertEquals("A" + i, Bytes.toString((byte[]) cell2.getColumnValue()));
    }
  }

  private void verifyMTableDataOnAllClient() {
    verifyMTableData(true);
  }

  private void verifyMTableDataOnAllServers() {
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            verifyMTableData(false);
            return null;
          }
        }));
  }

  private void loadSnapshotsData() throws IOException, ClassNotFoundException {
    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        final ProxyMTableRegion mTable_ordered =
            (ProxyMTableRegion) MCacheFactory.getAnyInstance().getTable(MTABLE_ORDERED);
        File snpOrdered = new File(this.getClass().getResource(mtableOrderedSnaphost).getPath());
        mTable_ordered.getTableRegion().getSnapshotService().load(snpOrdered,
            SnapshotFormat.GEMFIRE);

        final ProxyMTableRegion mTable_unordered =
            (ProxyMTableRegion) MCacheFactory.getAnyInstance().getTable(MTABLE_UNORDERED);
        File snpUnOrdered =
            new File(this.getClass().getResource(mtableUnOrderedSnaphost).getPath());
        mTable_unordered.getTableRegion().getSnapshotService().load(snpUnOrdered,
            SnapshotFormat.GEMFIRE);
        return null;
      }
    });
  }

  private void loadSnapshotsData_V11ToV111() throws IOException, ClassNotFoundException {
    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        final ProxyMTableRegion mTable_ordered =
            (ProxyMTableRegion) MCacheFactory.getAnyInstance().getTable(MTABLE_ORDERED);
        File snpOrdered =
            new File(this.getClass().getResource(mtableOrderedSnaphost_v11).getPath());
        mTable_ordered.getTableRegion().getSnapshotService().load(snpOrdered,
            SnapshotFormat.GEMFIRE);

        final ProxyMTableRegion mTable_unordered =
            (ProxyMTableRegion) MCacheFactory.getAnyInstance().getTable(MTABLE_UNORDERED);
        File snpUnOrdered =
            new File(this.getClass().getResource(mtableUnOrderedSnaphost_v11).getPath());
        mTable_unordered.getTableRegion().getSnapshotService().load(snpUnOrdered,
            SnapshotFormat.GEMFIRE);
        return null;
      }
    });
  }

  public MTableColumnType getType(final String typeStr) {
    return new MTableColumnType(DataTypeFactory.getTypeFromString(typeStr));
  }

  private void createTables() {
    // ordered table
    MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    tableDescriptor.addColumn("ID");
    tableDescriptor.addColumn("NAME");
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS);
    MCacheFactory.getAnyInstance().getAdmin().createTable(MTABLE_ORDERED, tableDescriptor);

    // unordered table
    MTableDescriptor tableDescriptor1 = new MTableDescriptor(MTableType.UNORDERED);
    tableDescriptor1.addColumn("ID");
    tableDescriptor1.addColumn("NAME");
    tableDescriptor1.enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS);
    MCacheFactory.getAnyInstance().getAdmin().createTable(MTABLE_UNORDERED, tableDescriptor1);
  }

  private void startServersAndClients() {
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createClientCache();
  }

  // @Test
  // public void testMTableRecoveryFromV1toV11() throws IOException, ClassNotFoundException {
  // startServersAndClients();
  // createTables();
  // loadSnapshotsData();
  // verifyMTableDataOnAllClient();
  // // verifyMTableDataOnAllServers();
  // verifyMTableDataOnAllClientVM(this.client1);
  // closeAllMCache();
  // }
  //
  // /**
  // * upgrade test (MTables) for MTable (Version 1.1 to 1.1.1)
  // *
  // * @throws IOException
  // * @throws ClassNotFoundException
  // */
  // @Test
  // public void testMTableRecoveryFromV11toV111() throws IOException, ClassNotFoundException {
  // startServersAndClients();
  // createTables();
  // loadSnapshotsData_V11ToV111();
  // verifyMTableDataOnAllClient();
  // // verifyMTableDataOnAllServers();
  // verifyMTableDataOnAllClientVM(this.client1);
  // closeAllMCache();
  // }

  @Test
  public void dummyTest() {

  }

}
