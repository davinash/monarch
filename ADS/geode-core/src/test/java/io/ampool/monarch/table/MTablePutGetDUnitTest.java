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

import io.ampool.internal.RegionDataOrder;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.IllegalColumnNameException;
import io.ampool.monarch.table.exceptions.RowKeyDoesNotExistException;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import io.ampool.monarch.table.internal.MultiVersionValue;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(MonarchTest.class)
public class MTablePutGetDUnitTest extends MTableDUnitHelper {
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


  public MTablePutGetDUnitTest() {
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
          }
          tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
              .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
          tableDescriptor.setRedundantCopies(1);

          MClientCache clientCache = MClientCacheFactory.getAnyInstance();
          Admin admin = clientCache.getAdmin();
          MTable mtable = admin.createTable("EmployeeTable", tableDescriptor);
          assertEquals(mtable.getName(), "EmployeeTable");
          assertTrue(mtable.isEmpty());

          for (int i = 0; i < numOfEntries; i++) {
            String key1 = "RowKey" + i;
            Put myput1 = new Put(Bytes.toBytes(key1));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }
          assertFalse(mtable.isEmpty());

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

        // create valid table and test with null values of Put, MGet, MDelete.
        MTableDescriptor tableDescriptor = new MTableDescriptor();
        tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
            .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
        tableDescriptor.setRedundantCopies(1);
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
        /*
         * Test MTable delete with nullKey Expect IllegalArgumentException.
         */

        Delete delete = null;
        expectedException = null;
        try {
          table.delete(delete);
        } catch (IllegalArgumentException iae) {
          expectedException = iae;
        }
        assertTrue(expectedException instanceof IllegalArgumentException);

        /*
         * Test MTable delete with invalidRowKey Test fails if some exception occurs. Get same
         * invalidRowKey after get and assert for empty result.
         */
        Delete invalidRowKey = new Delete("invalid row key");
        try {
          table.delete(invalidRowKey);
        } catch (Exception icne) {
          // Expecting RowKeyDoesNotExistException
          Assert.assertTrue(icne instanceof RowKeyDoesNotExistException);
        }
        Row result = table.get(new Get("invalid row key"));
        assertTrue(result.isEmpty());

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

  private void doMultiVersionedPutAndObjectTest(boolean ordered) {
    try {
      int versions = 5;
      final MTableDescriptor tableDescriptor =
          new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
      tableDescriptor.setTotalNumOfSplits(1);
      tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
          .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
      tableDescriptor.setRedundantCopies(1);

      tableDescriptor.setMaxVersions(versions);

      MClientCache clientCache = MClientCacheFactory.getAnyInstance();
      Admin admin = clientCache.getAdmin();
      String tableName = "EmployeeTable";
      MTable mtable = admin.createTable(tableName, tableDescriptor);
      assertEquals(mtable.getName(), tableName);
      assertTrue(mtable.isEmpty());

      for (int i = 0; i < numOfEntries; i++) {
        versions = 1;
        String key1 = "RowKey" + i;
        for (int j = 0; j < versions; j++) {
          Put myput1 = new Put(Bytes.toBytes(key1));
          myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
          myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
          myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
          myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
          mtable.put(myput1);
        }
      }
      assertFalse(mtable.isEmpty());

      vm0.invoke(() -> {
        return checkForValueType(tableName, tableDescriptor.getMaxVersions() > 1);
      });

      vm1.invoke(() -> {
        return checkForValueType(tableName, tableDescriptor.getMaxVersions() > 1);
      });

      vm2.invoke(() -> {
        return checkForValueType(tableName, tableDescriptor.getMaxVersions() > 1);
      });

      admin.deleteMTable(tableName);
    } catch (CacheClosedException cce) {
    }

  }

  private void doSingleVersionedPutAndObjectTest(boolean ordered) {
    try {
      final MTableDescriptor tableDescriptor =
          new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
      tableDescriptor.setTotalNumOfSplits(1);
      tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
          .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
      tableDescriptor.setRedundantCopies(1);

      // tableDescriptor.setMaxVersions(5);

      MClientCache clientCache = MClientCacheFactory.getAnyInstance();
      Admin admin = clientCache.getAdmin();
      String tableName = "EmployeeTable";
      MTable mtable = admin.createTable(tableName, tableDescriptor);
      assertEquals(mtable.getName(), tableName);
      assertTrue(mtable.isEmpty());

      for (int i = 0; i < numOfEntries; i++) {
        String key1 = "RowKey" + i;
        Put myput1 = new Put(Bytes.toBytes(key1));
        myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
        myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
        myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
        myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

        mtable.put(myput1);
      }
      assertFalse(mtable.isEmpty());

      vm0.invoke(() -> {
        return checkForValueType(tableName, tableDescriptor.getMaxVersions() > 1);
      });

      vm1.invoke(() -> {
        return checkForValueType(tableName, tableDescriptor.getMaxVersions() > 1);
      });

      vm2.invoke(() -> {
        return checkForValueType(tableName, tableDescriptor.getMaxVersions() > 1);
      });
      admin.deleteMTable(tableName);

    } catch (CacheClosedException cce) {
    }

  }

  private void doMaxVersionTestAfterMetaregionDelete(boolean ordered) {
    MTableDescriptor tableDescriptor =
        new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
    tableDescriptor.setRedundantCopies(1);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable("EmployeeTable", tableDescriptor);
    assertEquals(mtable.getName(), "EmployeeTable");
    admin.deleteMTable("EmployeeTable");

    ClientCacheFactory.getAnyInstance().getRegion(MTableUtils.AMPL_META_REGION_NAME)
        .destroyRegion();

    Exception e = null;
    try {
      getMaxVersions("EmployeeTable1", (MonarchCacheImpl) MClientCacheFactory.getAnyInstance());
    } catch (IllegalStateException ise) {
      e = ise;
    }
    assertTrue(e instanceof IllegalStateException);
  }

  private int getMaxVersions(final String name, final MonarchCacheImpl monarchCacheImpl) {
    Region metaRegion = (Region) monarchCacheImpl.getRegion(MTableUtils.AMPL_META_REGION_NAME);
    if (metaRegion == null) {
      throw new IllegalStateException("Meta Region is not created !!!");
    }
    MTableDescriptor tableDescriptor = (MTableDescriptor) metaRegion.get(name);
    if (tableDescriptor == null) {
      throw new IllegalStateException("Table " + name + " Not found in MetaData");
    }
    return tableDescriptor.getMaxVersions();
  }



  private void doMaxVersionTestAfterMetaRegionEntryDelete(boolean ordered) {
    MTableDescriptor tableDescriptor =
        new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));
    tableDescriptor.setRedundantCopies(1);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable("EmployeeTable", tableDescriptor);
    assertEquals(mtable.getName(), "EmployeeTable");

    ClientCacheFactory.getAnyInstance().getRegion(MTableUtils.AMPL_META_REGION_NAME)
        .destroy("EmployeeTable");
    ClientCacheFactory.getAnyInstance().getRegion("EmployeeTable").destroyRegion();

    Exception e = null;
    try {
      getMaxVersions("EmployeeTable1", (MonarchCacheImpl) MClientCacheFactory.getAnyInstance());
    } catch (IllegalStateException ise) {
      e = ise;
    }
    assertTrue(e instanceof IllegalStateException);
  }

  private Boolean checkForValueType(String tableName, boolean multiVersion) {
    MCache anyInstance = MCacheFactory.getAnyInstance();
    Region<Object, Object> region = anyInstance.getRegion(tableName);

    Collection<Object> values = region.values();
    PartitionedRegion tableRegionPR = (PartitionedRegion) region;
    Set<Integer> allLocalBucketIds = tableRegionPR.getDataStore().getAllLocalBucketIds();
    allLocalBucketIds.forEach(bucketid -> {
      BucketRegion bucket = tableRegionPR.getDataStore().getLocalBucketById(bucketid);
      if (((AmpoolTableRegionAttributes) bucket.getCustomAttributes())
          .getRegionDataOrder() == RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED) {
        // if (bucket.getRegionDataOrder() == RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED) {
        SortedMap iMap = ((RowTupleConcurrentSkipListMap) bucket.getRegionMap().getInternalMap())
            .getInternalMap();
        Set<Map.Entry<IMKey, RegionEntry>> entries = iMap.entrySet();
        System.out.println("Operating for bucket " + bucketid + " isPrimary: "
            + bucket.getBucketAdvisor().isPrimary() + " bucketSize: " + entries.size());
        for (Map.Entry<IMKey, RegionEntry> entry : entries) {
          RegionEntry value1 = entry.getValue();
          Object value = value1._getValue();
          if (value != null) {
            System.out.println("FTableAppendDUnitTest.call :: " + "val class: " + value.getClass());
            if (multiVersion) {
              System.out.println("FTableAppendDUnitTest.call :: " + "val class: "
                  + ((CachedDeserializable) value).getDeserializedForReading().getClass());
              assertEquals(MultiVersionValue.class, value.getClass());
            } else {
              System.out
                  .println("FTableAppendDUnitTest.call :: " + "val class: " + value.getClass());
              assertTrue(value instanceof byte[]);
            }
          } else {
            System.out.println("FTableAppendDUnitTest.call :: " + "val class: NULL");
            fail("Value should not be null");
          }
        }
      } else {
        Set<Map.Entry<IMKey, RegionEntry>> entrySet =
            ((CustomEntryConcurrentHashMap) bucket.getRegionMap().getInternalMap()).entrySet();
        System.out.println("Operating for bucket " + bucketid + " isPrimary: "
            + bucket.getBucketAdvisor().isPrimary() + " bucketSize: " + entrySet.size());
        for (Map.Entry<IMKey, RegionEntry> entry : entrySet) {
          RegionEntry value1 = entry.getValue();
          Object value = value1._getValue();
          if (value != null) {
            System.out.println("FTableAppendDUnitTest.call :: " + "val class: " + value.getClass());
            if (multiVersion) {
              System.out.println("FTableAppendDUnitTest.call :: " + "val class: "
                  + ((CachedDeserializable) value).getDeserializedForReading().getClass());
              assertEquals(MultiVersionValue.class, value.getClass());
            } else {
              System.out
                  .println("FTableAppendDUnitTest.call :: " + "val class: " + value.getClass());
              assertTrue(value instanceof byte[]);
            }
          } else {
            System.out.println("FTableAppendDUnitTest.call :: " + "val class: NULL");
            fail("Value should not be null");
          }
        }
      }
    });
    return true;
  }

  private void doSimplePutGet(boolean ordered) {
    MTableDescriptor tableDescriptor =
        new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

    tableDescriptor.setRedundantCopies(1);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable("EmployeeTable", tableDescriptor);
    assertEquals(mtable.getName(), "EmployeeTable");

    assertTrue(mtable.isEmpty());

    for (int i = 0; i < numOfEntries; i++) {
      String key1 = "RowKey" + i;
      Put myput1 = new Put(Bytes.toBytes(key1));
      myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
      myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
      myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
      myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

      mtable.put(myput1);
    }
    assertFalse(mtable.isEmpty());

    for (int i = 0; i < numOfEntries; i++) {

      String key1 = "RowKey" + i;
      Put myput = new Put(Bytes.toBytes(key1));
      myput.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
      myput.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
      myput.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
      myput.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
      myput.addColumn(Bytes.toBytes(MTableUtils.KEY_COLUMN_NAME), Bytes.toBytes(key1));

      Get myget = new Get(Bytes.toBytes("RowKey" + i));
      Row result = mtable.get(myget);
      assertFalse(result.isEmpty());

      List<Cell> row = result.getCells();

      Iterator<MColumnDescriptor> iteratorColumnDescriptor =
          mtable.getTableDescriptor().getAllColumnDescriptors().iterator();

      for (int k = 0; k < row.size(); k++) {
        byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
        byte[] expectedColumnValue =
            (byte[]) myput.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));
        if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
          System.out.println("expectedColumnValue =>  " + Arrays.toString(expectedColumnValue));
          System.out.println(
              "actuaColumnValue    =>  " + Arrays.toString((byte[]) row.get(k).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
      }
    }
    admin.deleteMTable("EmployeeTable");
  }

  private void doPutGetMultiVersioned(boolean ordererd) {
    int MAX_VERSIONS = 4;
    MTableDescriptor tableDescriptor =
        new MTableDescriptor(ordererd ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

    tableDescriptor.setRedundantCopies(1).setMaxVersions(MAX_VERSIONS);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable("EmployeeTable", tableDescriptor);
    assertEquals(mtable.getName(), "EmployeeTable");

    assertTrue(mtable.isEmpty());

    for (int i = 0; i < numOfEntries; i++) {
      String key1 = "RowKey" + i;
      Put myput1 = new Put(Bytes.toBytes(key1));
      for (int j = 1; j <= MAX_VERSIONS; j++) {
        switch (j) {
          case 1:
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput1.setTimeStamp(j * 1000);
            break;
          case 2:
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.setTimeStamp(j * 1000);
            break;
          case 3:
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.setTimeStamp(j * 1000);
            break;
          case 4:
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
            myput1.setTimeStamp(j * 1000);
            break;
        }
        mtable.put(myput1);
      }
    }
    assertFalse(mtable.isEmpty());

    for (int i = 0; i < numOfEntries; i++) {
      String key1 = "RowKey" + i;
      Put myput1 = new Put(Bytes.toBytes(key1));
      Get myGet = new Get(key1);
      for (int j = 1; j <= MAX_VERSIONS; j++) {
        switch (j) {
          case 1:
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myGet.setTimeStamp(j * 1000);
            break;
          case 2:
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myGet.setTimeStamp(j * 1000);
            break;
          case 3:
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myGet.setTimeStamp(j * 1000);
            break;
          case 4:
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("Avinash" + i));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));
            myGet.setTimeStamp(j * 1000);
            break;
        }
        Row result = mtable.get(myGet);
        assertFalse(result.isEmpty());

        List<Cell> row = result.getCells();

        Iterator<MColumnDescriptor> iteratorColumnDescriptor =
            mtable.getTableDescriptor().getAllColumnDescriptors().iterator();
        // System.out.println("MTablePutGetDUnitTest.testPutGetMultiVersioned :: " + "Row Timestamp:
        // "+result.getRowTimeStamp());
        for (int k = 0; k < row.size() - 1; k++) {
          byte[] expectedColumnName = iteratorColumnDescriptor.next().getColumnName();
          byte[] expectedColumnValue =
              (byte[]) myput1.getColumnValueMap().get(new ByteArrayKey(expectedColumnName));
          if (!Bytes.equals(expectedColumnName, row.get(k).getColumnName())) {
            Assert.fail("Invalid Values for Column Name");
          }

          if (!Bytes.equals(expectedColumnValue, (byte[]) row.get(k).getColumnValue())) {
            System.out.println("expectedColumnValue =>  " + Arrays.toString(expectedColumnValue));
            System.out.println(
                "actuaColumnValue    =>  " + Arrays.toString((byte[]) row.get(k).getColumnValue()));
            Assert.fail("Invalid Values for Column Value");
          }
        }
      }
    }
    admin.deleteMTable("EmployeeTable");
  }

  private void doSimplePutGetUserTableFalse(boolean ordered) {

    MTableDescriptor tableDescriptor =
        new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
    tableDescriptor.addColumn(Bytes.toBytes("NAME")).addColumn(Bytes.toBytes("ID"))
        .addColumn(Bytes.toBytes("AGE")).addColumn(Bytes.toBytes("SALARY"));

    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.setUserTable(false);

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
          System.out.println("expectedColumnValue =>  " + Arrays.toString(expectedColumnValue));
          System.out.println(
              "actuaColumnValue    =>  " + Arrays.toString((byte[]) row.get(k).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
      }
    }
    admin.deleteMTable("EmployeeTable");
  }

  private void doEmptyPutGetWithColumnTypes(boolean ordered) {
    final int ROW_COUNT = 5;
    final String ROW_KEY_PREFIX = "rowKey_";
    final String TABLE_NAME = "empty_put_get_types";
    final String[] types = {"INT", "array<STRING>", "map<DOUBLE,BYTE>",
        "struct<c1:SHORT,c2:BOOLEAN>", "union<BIG_DECIMAL,STRING>"};
    final MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    /** create table **/
    final MTableDescriptor td =
        new MTableDescriptor(ordered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
    for (final String type : types) {
      td.addColumn("column_" + type.replaceAll("\\W", "_"), new MTableColumnType(type));
    }
    final MTable table = clientCache.getAdmin().createTable(TABLE_NAME, td);
    assertTrue(table.isEmpty());

    /** do some empty puts **/
    Exception expectedException = null;
    try {
      IntStream.range(0, ROW_COUNT).mapToObj(i -> new Put(ROW_KEY_PREFIX + i)).forEach(table::put);
    } catch (Exception e) {
      expectedException = e;
    }
    assertTrue(expectedException instanceof IllegalArgumentException);

    /** do the gets now.. **/
    IntStream.range(0, ROW_COUNT).mapToObj(i -> table.get(new Get(ROW_KEY_PREFIX + i)))
        .forEach(result -> {
          assertNotNull(result);
          assertTrue(result.isEmpty());
        });

    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testDoBasicPutGet() throws Exception {
    doBasicPutGet(true);
  }

  @Test
  public void testDoBasicPutGetUnordered() throws Exception {
    doBasicPutGet(false);
  }

  @Test
  public void testMultiVersionPutAndObjectTest() throws Exception {
    doMultiVersionedPutAndObjectTest(true);
  }

  @Test
  public void testMultiVersionPutAndObjectTestUnordered() throws Exception {
    doMultiVersionedPutAndObjectTest(false);
  }

  @Test
  public void testSingleVersionPutAndObjectTest() throws Exception {
    doSingleVersionedPutAndObjectTest(true);
  }

  @Test
  public void testSingleVersionPutAndObjectTestUnordered() throws Exception {
    doSingleVersionedPutAndObjectTest(false);
  }

  @Test
  public void testNullValidationTests() {
    doMTableOpsFromClientWithNullValues(vm3);
  }

  @Test
  public void testColumnNameValidationTests() {
    doMTableOpsFromClientWithInvalidColumn(vm3);
  }

  @Test
  public void testgetMaxVersionsAPIAfterMetaRegionDelete() {
    doMaxVersionTestAfterMetaregionDelete(true);
  }

  @Test
  public void testgetMaxVersionsAPIAfterMetaRegionDeleteUnordered() {
    doMaxVersionTestAfterMetaregionDelete(false);
  }

  @Test
  public void testgetMaxVersionsAPIAfterMetaRegionEntryDelete() {
    doMaxVersionTestAfterMetaRegionEntryDelete(true);
  }

  @Test
  public void testgetMaxVersionsAPIAfterMetaRegionEntryDeleteUnordered() {
    doMaxVersionTestAfterMetaRegionEntryDelete(false);
  }

  // @Test
  // public void testgetSelectedColumnValueAPI() {
  // assertNull(RowTupleBytesUtils.getSelectedColumnValue(null, null, null));
  // }

  @Test
  public void testSimplePutGet() {
    doSimplePutGet(true);
  }

  @Test
  public void testSimplePutGetUnordered() {
    doSimplePutGet(false);
  }

  @Test
  public void testPutGetMultiVersioned() {
    doPutGetMultiVersioned(true);
  }


  @Test
  public void testPutGetMultiVersionedUnordered() {
    doPutGetMultiVersioned(false);
  }


  @Test
  public void testSimplePutGetUserTableFalse() {
    doSimplePutGetUserTableFalse(true);
  }

  @Test
  public void testSimplePutGetUserTableFalseUnordered() {
    doSimplePutGetUserTableFalse(false);
  }

  /**
   * Assert that empty put (no data) and then get on columns works as expected.
   */
  @Test
  public void testEmptyPutAndGetWithColumnTypes() {
    doEmptyPutGetWithColumnTypes(true);
  }

  @Test
  public void testEmptyPutAndGetWithColumnTypesUnordered() {
    doEmptyPutGetWithColumnTypes(false);
  }
}
