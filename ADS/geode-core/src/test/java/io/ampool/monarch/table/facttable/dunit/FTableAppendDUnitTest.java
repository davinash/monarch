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
package io.ampool.monarch.table.facttable.dunit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.table.internal.AbstractTableDescriptor;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.StorageFormatter;
import io.ampool.monarch.table.internal.StorageFormatters;
import io.ampool.monarch.table.region.FTableByteUtils;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.BasicTypes;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

@Category(FTableTest.class)
public class FTableAppendDUnitTest extends MTableDUnitHelper {


  private static final int NUM_OF_COLUMNS = 10;
  private static final int NUM_OF_SPLITS = 113;
  private static final int NUM_ROWS = 10000;
  private static final String COLUMN_NAME_PREFIX = "COL";
  // private static final String TABLE_NAME = "MTableRaceCondition";

  public FTableAppendDUnitTest() {}

  private Host host = null;
  private VM vm0 = null;
  private VM vm1 = null;
  private VM vm2 = null;
  private VM vm3 = null;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());
    createClientCache(vm3);
    createClientCache();
  }

  /**
   * Verify if FTable.append api is working
   */
  @Test
  public void testFTableAppendBasic() {
    String tableName = getTestMethodName();
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();

    if (admin.existsFTable(tableName)) {
      admin.deleteFTable(tableName);
    }

    final FTable table = FTableAppendDUnitTest.createFTable(tableName,
        getFTableDescriptor(FTableAppendDUnitTest.NUM_OF_SPLITS, 0));
    assertNotNull(table);
    vm0.invoke(() -> {
      FTableAppendDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAppendDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm1.invoke(() -> {
      FTableAppendDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAppendDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm2.invoke(() -> {
      FTableAppendDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAppendDUnitTest.NUM_OF_SPLITS, 0));
    });

    Record record = new Record();
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }
    verifyValuesOnAllVMs(tableName);

    final int size = ((ProxyFTableRegion) table).getTableRegion().keySetOnServer().size();
    System.out.println("Keys on  server " + size);
    assertEquals(NUM_ROWS / FTableDescriptor.DEFAULT_BLOCK_SIZE, size);


    admin.deleteFTable(tableName);

  }

  public static Map<String, BasicTypes> columns = new LinkedHashMap<>();
  static {
    columns.put("BIN_COL", BasicTypes.BINARY);
    columns.put("SHORT_COL", BasicTypes.SHORT);
    columns.put("VARCHAR_COL", BasicTypes.VARCHAR);
    columns.put("DOUBLE_COL", BasicTypes.DOUBLE);
    columns.put("DATE_COL", BasicTypes.DATE);
    columns.put("BIGDEC_COL", BasicTypes.BIG_DECIMAL);
    columns.put("BOOL_COL", BasicTypes.BOOLEAN);
    columns.put("BYTE_COL", BasicTypes.BYTE);
    columns.put("CHAR_COL", BasicTypes.CHAR);
    columns.put("CHARS_COL", BasicTypes.CHARS);
    columns.put("FLOAT_COL", BasicTypes.FLOAT);
    columns.put("INT_COL", BasicTypes.INT);
    columns.put("LONG_COL", BasicTypes.LONG);
    columns.put("STRING_COL", BasicTypes.STRING);
    columns.put("TS_COL", BasicTypes.TIMESTAMP);
  }

  @Test
  public void testCDCLikeIngestion() {
    String tableName = getTestMethodName();

    Schema.Builder fSchemaBuilder = new Schema.Builder();
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    Arrays.asList("EVENTID", "OPERATION_TYPE", "RowKey", "VersionID").forEach(name -> {
      if (name.equalsIgnoreCase("RowKey")) {
        fSchemaBuilder.column(name, BasicTypes.BINARY);
        fTableDescriptor.setPartitioningColumn("RowKey");
      } else if (name.equalsIgnoreCase("VersionID")) {
        fSchemaBuilder.column(name, BasicTypes.LONG);
      } else {
        fSchemaBuilder.column(name, BasicTypes.STRING);
      }
    });

    columns.forEach((name, type) -> {
      fSchemaBuilder.column(name, type);
    });
    Schema ftableSchema = fSchemaBuilder.build();
    fTableDescriptor.setSchema(ftableSchema);

    ((AbstractTableDescriptor) fTableDescriptor).finalizeDescriptor();

    Record record = new Record();
    record.add("EVENTID", "ID1");
    record.add("OPERATION_TYPE", "DELETE");
    record.add("RowKey", new byte[] {0, 1, 1, 1});
    record.add("VersionID", 0l);

    // columns.forEach((name, type) -> {
    // fSchemaBuilder.column(name, type);
    // });

    byte[] bytes = FTableByteUtils.fromRecord(fTableDescriptor, record);

    StorageFormatter formatter = StorageFormatters.getInstance((byte) 3, (byte) 2, (byte) 0);
    Map<Integer, Pair<Integer, Integer>> offsetsForEachColumn =
        formatter.getOffsetsForEachColumn(fTableDescriptor, bytes, new ArrayList<>());

    System.out.println("FTableAppendDUnitTest.testCDCLikeIngestion :: 207 ");

  }

  /**
   * Verify if FTable.append api is working and value appended is BlockValue not
   * VMCachedDeserializable
   */
  @Test
  public void testFTableAppendAndValueType() {
    AtomicInteger actualRows = new AtomicInteger(0);
    String tableName = getTestMethodName();
    final FTable table = FTableAppendDUnitTest.createFTable(tableName,
        getFTableDescriptor(FTableAppendDUnitTest.NUM_OF_SPLITS, 0));
    assertNotNull(table);
    vm0.invoke(() -> {
      FTableAppendDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAppendDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm1.invoke(() -> {
      FTableAppendDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAppendDUnitTest.NUM_OF_SPLITS, 0));
    });
    vm2.invoke(() -> {
      FTableAppendDUnitTest.verifyTableOnServer(tableName,
          getFTableDescriptor(FTableAppendDUnitTest.NUM_OF_SPLITS, 0));
    });

    Record record = new Record();
    for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
      record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      table.append(record);
    }
    verifyValuesOnAllVMs(tableName);

    vm0.invoke(() -> {
      return checkForValueType(tableName);
    });

    vm1.invoke(() -> {
      return checkForValueType(tableName);
    });

    vm2.invoke(() -> {
      return checkForValueType(tableName);
    });

    // restart servers
    restartServers();

    System.out.println(
        "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");

    vm0.invoke(() -> {
      return checkForValueType(tableName);
    });

    vm1.invoke(() -> {
      return checkForValueType(tableName);
    });

    vm2.invoke(() -> {
      return checkForValueType(tableName);
    });


    final int size = ((ProxyFTableRegion) table).getTableRegion().keySetOnServer().size();
    System.out.println("Keys on  server " + size);
    assertEquals(NUM_ROWS / FTableDescriptor.DEFAULT_BLOCK_SIZE, size);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    admin.deleteFTable(tableName);

  }

  private void restartServers() {
    stopServerOn(vm0);
    stopServerOn(vm1);
    stopServerOn(vm2);
    asyncStartServerOn(vm0, DUnitLauncher.getLocatorString());
    asyncStartServerOn(vm1, DUnitLauncher.getLocatorString());
    asyncStartServerOn(vm2, DUnitLauncher.getLocatorString());

    try {
      Thread.sleep(5000l);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private Boolean checkForValueType(String tableName) {
    MCache anyInstance = MCacheFactory.getAnyInstance();
    Region<Object, Object> region = anyInstance.getRegion(tableName);

    Collection<Object> values = region.values();
    PartitionedRegion tableRegionPR = (PartitionedRegion) region;
    Set<Integer> allLocalBucketIds = tableRegionPR.getDataStore().getAllLocalBucketIds();
    allLocalBucketIds.forEach(bucketid -> {
      BucketRegion bucket = tableRegionPR.getDataStore().getLocalBucketById(bucketid);
      System.out.println("Operating for bucket " + bucketid + " isPrimary: "
          + bucket.getBucketAdvisor().isPrimary());
      SortedMap iMap =
          ((RowTupleConcurrentSkipListMap) bucket.getRegionMap().getInternalMap()).getInternalMap();
      Set<Map.Entry<IMKey, RegionEntry>> entries = iMap.entrySet();
      for (Map.Entry<IMKey, RegionEntry> entry : entries) {
        RegionEntry value1 = entry.getValue();
        Object value = value1._getValue();
        if (value != null) {
          if (value instanceof VMCachedDeserializable) {
            value = ((VMCachedDeserializable) value).getDeserializedForReading();
          }
          BlockValue blockValue = (BlockValue) value;
          System.out.println("FTableAppendDUnitTest.call :: " + "val class: " + value.getClass());
          assertEquals(BlockValue.class, blockValue.getClass());
        } else {
          System.out.println("FTableAppendDUnitTest.call :: " + "val class: NULL");
          fail("Value should not be null");
        }
      }
    });
    return true;
  }

  protected void verifyValuesOnAllVMs(String tableName) {
    AtomicInteger actualRows = new AtomicInteger(0);
    final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
    for (int i = 0; i < vmList.size(); i++) {
      final int res = (int) vmList.get(i).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          return verifyValues(tableName);
        }
      });
      actualRows.getAndAdd(res);
    }
    assertEquals(NUM_ROWS, actualRows.get());
  }

  protected int verifyValues(String tableName) {
    int entriesCount = 0;
    final Region<Object, Object> region = MCacheFactory.getAnyInstance().getRegion(tableName);
    assertNotNull(region);
    final Iterator<BucketRegion> allLocalPrimaryBucketRegions =
        ((PartitionedRegion) region).getDataStore().getAllLocalPrimaryBucketRegions().iterator();
    while (allLocalPrimaryBucketRegions.hasNext()) {
      final BucketRegion bucketRegion = allLocalPrimaryBucketRegions.next();
      final RowTupleConcurrentSkipListMap internalMap =
          (RowTupleConcurrentSkipListMap) bucketRegion.entries.getInternalMap();
      final Map concurrentSkipListMap = internalMap.getInternalMap();
      final Iterator<Entry> iterator = concurrentSkipListMap.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<IMKey, RegionEntry> entry = iterator.next();
        Object value = entry.getValue()._getValue();
        if (value instanceof VMCachedDeserializable) {
          value = ((VMCachedDeserializable) value).getDeserializedForReading();
        }

        final BlockValue blockValue = (BlockValue) value;
        final Iterator objectIterator = blockValue.iterator();
        while (objectIterator.hasNext()) {
          objectIterator.next();
          entriesCount++;
        }
      }
      // entriesCount += bucketRegion.size();
      System.out.println(
          "Bucket Region Name : " + bucketRegion.getName() + "   Size: " + bucketRegion.size());
    }
    System.out.println("FTableAppendDUnitTest.verifyValues :: " + "ECount: " + entriesCount);
    return entriesCount;
  }

  protected void stopServer(VM vm) {
    System.out.println("CreateMTableDUnitTest.stopServer :: " + "Stopping server....");
    try {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCacheFactory.getAnyInstance().close();
          return null;
        }
      });
    } catch (Exception ex) {
      fail(ex.getMessage());
    }
  }

  private static FTable createFTable(final String tableName,
      final FTableDescriptor tableDescriptor) {
    // System.out.println("CreateMTableDUnitTest.createFTable :: " + "Creating mtable:---- " +
    // tableDescriptor);
    int numberOfKeysPerBucket = 20;
    int splits = 10;
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    // System.out.println("CreateMTableDUnitTest.createFTable :: " + "isCacheClosed: " +
    // clientCache.isClosed());
    // MTableDescriptor tableDescriptor = getFTableDescriptor(splits);

    FTable mtable = null;
    try {
      mtable = clientCache.getAdmin().createFTable(tableName, tableDescriptor);
    } catch (Exception e) {
      System.out.println("CreateMTableDUnitTest.createFTable :: " + "Throwing from test");
      throw e;
    }
    System.out.println("CreateMTableDUnitTest.createFTable :: " + "mtable is " + mtable);
    return mtable;

  }

  private static FTableDescriptor getFTableDescriptor(final int splits, int descriptorIndex) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    if (descriptorIndex == 0) {
      tableDescriptor.setTotalNumOfSplits(splits);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        // sets a partitioningColumn
        if (colmnIndex == 0) {
          tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
        }
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setRedundantCopies(1);
    } else {
      tableDescriptor.setTotalNumOfSplits(splits);
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        // sets a partitioningColumn
        if (colmnIndex == 0) {
          tableDescriptor.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
        }
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setEvictionPolicy(MEvictionPolicy.OVERFLOW_TO_TIER);
      tableDescriptor.setRedundantCopies(2);
    }
    return tableDescriptor;
  }


  @Override
  public void tearDown2() throws Exception {
    // closeMClientCache(CreateMTableDUnitTest.vm0);
    // closeMClientCache(CreateMTableDUnitTest.vm1);
    // closeMClientCache(CreateMTableDUnitTest.vm2);
    closeMClientCache(vm3);
    closeMClientCache();
    // closeAllMCaches();
    super.tearDown2();
  }

  private static void verifyTableOnServer(final String tableName,
      final FTableDescriptor tableDescriptor) {
    final MCache serverCache = MCacheFactory.getAnyInstance();
    assertNotNull(serverCache);
    FTable mtable = null;
    int retries = 0;
    do {
      mtable = serverCache.getFTable(tableName);
      if (retries > 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      retries++;
      // if (retries > 1) {
      // System.out.println("CreateMTableDUnitTest.verifyTableOnServer :: " + "Attempting to fetch
      // table... Attempt " + retries);
      // }
    } while (mtable == null && retries < 500);
    assertNotNull(mtable);
    final Region<Object, Object> mregion = ((ProxyFTableRegion) mtable).getTableRegion();
    String path = mregion.getFullPath();
    assertTrue(path.contains(tableName));
    // To verify disk persistence
    assertEquals(tableDescriptor.isDiskPersistenceEnabled(),
        mregion.getAttributes().getDataPolicy().withPersistence());
  }
}


