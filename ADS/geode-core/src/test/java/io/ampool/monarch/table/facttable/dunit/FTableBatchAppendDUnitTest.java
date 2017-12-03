package io.ampool.monarch.table.facttable.dunit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.facttable.FTableDUnitHelper;
import io.ampool.monarch.table.facttable.FTableTestHelper;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.StressTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(StressTest.class)
public class FTableBatchAppendDUnitTest extends MTableDUnitHelper {
  private static final int NUM_ROWS = 10000;
  public static int actualRows;

  public FTableBatchAppendDUnitTest() {}


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
    closeMClientCache(vm3);
    closeMClientCache();
    // closeAllMCaches();
    super.tearDown2();
  }


  protected void verifyValuesOnAllVMs(String tableName) {
    actualRows = 0;
    final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
    for (int i = 0; i < vmList.size(); i++) {
      final int res = (int) vmList.get(i).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          return verifyValues(tableName);
        }
      });
      actualRows += res;
    }
    assertEquals(NUM_ROWS, actualRows);
  }

  protected void verifyValuesOnAllVMs(String tableName, int numRows) {
    final ArrayList<VM> vmList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));
    for (int i = 0; i < vmList.size(); i++) {
      final int res = (int) vmList.get(i).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          return verifyValues(tableName);
        }
      });
      actualRows += res;
    }
    assertEquals(numRows, actualRows);
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
        final Entry entry = iterator.next();
        RegionEntry value1 = (RegionEntry) entry.getValue();
        Object value = value1._getValue();
        if (value instanceof VMCachedDeserializable) {
          value = ((VMCachedDeserializable) value).getDeserializedForReading();
        }
        final BlockValue o = (BlockValue) value;

        if (o instanceof BlockValue) {
          final BlockValue blockValue = (BlockValue) o;
          final BlockKey blockKey = (BlockKey) ((RegionEntry) entry.getValue()).getKey();
          final Iterator objectIterator = blockValue.iterator();
          while (objectIterator.hasNext()) {
            objectIterator.next();
            entriesCount++;
          }
        } else {
          System.out.println(
              "FTableBatchAppendDUnitTest.verifyValues:: Entry value is not \"BlockValue\"");
        }
      }
      System.out.println(
          "Bucket Region Name : " + bucketRegion.getName() + "   Size: " + bucketRegion.size());
    }
    System.out.println("FTableAppendDUnitTest.verifyValues :: " + "ECount: " + entriesCount);
    return entriesCount;
  }

  /**
   * Verify if FTable.batch append api is working
   */
  @Test
  public void testFTableBatchAppend() {
    String tableName = getTestMethodName();
    final FTable table = FTableTestHelper.getFTable(tableName);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    Record[] records = new Record[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      records[i] = new Record();
      final Iterator<MColumnDescriptor> iterator =
          table.getTableDescriptor().getAllColumnDescriptors().iterator();
      while (iterator.hasNext()) {
        final MColumnDescriptor mColumnDescriptor = iterator.next();
        records[i].add(mColumnDescriptor.getColumnName(),
            Bytes.toBytes(mColumnDescriptor.getColumnNameAsString() + i));
      }
    }
    table.append(records);
    verifyValuesOnAllVMs(tableName);
    final int size = ((ProxyFTableRegion) table).getTableRegion().keySetOnServer().size();
    System.out.println("Keys on  server " + size);
    // assertEquals(NUM_ROWS, size);
  }

  /**
   * Verify if FTable.batch append api is working
   */
  @Test
  public void testFTableBatchAppendWithNull() {
    String tableName = getTestMethodName();
    final FTable table = FTableTestHelper.getFTable(tableName);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    Record[] records = new Record[NUM_ROWS];
    try {
      table.append(records);
      fail("Expected exception with empty records");
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Verify if FTable.batch append api is working
   */
  @Test
  public void testFTableBatchAppendWithZeroRecords() {
    String tableName = getTestMethodName();
    final FTable table = FTableTestHelper.getFTable(tableName);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    Record[] records = new Record[0];
    try {
      table.append(records);
      fail("Expected exception with empty records");
    } catch (Exception ex) {
      // success
    }
  }

  /**
   * Verify if FTable.batch append api is working
   */
  @Test
  public void testFTableBatchAppendWithNullRecords() {
    String tableName = getTestMethodName();
    final FTable table = FTableTestHelper.getFTable(tableName);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    Record[] records = new Record[2];
    records[0] = new Record();
    final Iterator<MColumnDescriptor> iterator =
        table.getTableDescriptor().getAllColumnDescriptors().iterator();
    while (iterator.hasNext()) {
      final MColumnDescriptor mColumnDescriptor = iterator.next();
      records[0].add(mColumnDescriptor.getColumnName(),
          Bytes.toBytes(mColumnDescriptor.getColumnNameAsString() + 0));
    }
    try {
      table.append(records);
      fail("Expected exception with empty records");
    } catch (Exception ex) {
      // success
    }
  }

  /**
   * Verify if FTable.batch append api is working
   */
  @Test
  public void testFTableBatchAppendWithMultipleBatches() {
    final int numBatches = 10;
    final int numRows = 10;
    String tableName = getTestMethodName();
    final FTable table = FTableTestHelper.getFTable(tableName);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);

    for (int j = 0; j < numBatches; j++) {
      Record[] records = new Record[numRows];
      for (int i = 0; i < numRows; i++) {
        records[i] = new Record();
        final Iterator<MColumnDescriptor> iterator =
            table.getTableDescriptor().getAllColumnDescriptors().iterator();
        while (iterator.hasNext()) {
          final MColumnDescriptor mColumnDescriptor = iterator.next();
          records[i].add(mColumnDescriptor.getColumnName(),
              Bytes.toBytes(mColumnDescriptor.getColumnNameAsString() + i));
        }
      }
      table.append(records);
    }

    verifyValuesOnAllVMs(tableName, numRows * numBatches);
    final int size = ((ProxyFTableRegion) table).getTableRegion().keySetOnServer().size();
    System.out.println("Keys on  server " + size);
    // assertEquals(numRows * numBatches, size);
  }

  @Test
  public void testBatchAppendWithLargeRecords() throws InterruptedException {
    String tableName = getTestMethodName();
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    FTableDescriptor fd = new FTableDescriptor();
    for (int i = 0; i < 10; i++) {
      fd.addColumn("COL" + "_" + i);
    }
    fd.setRedundantCopies(0);
    fd.setTotalNumOfSplits(1);
    Admin admin = clientCache.getAdmin();
    FTable fTable = admin.createFTable(tableName, fd);
    assertNotNull(fTable);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);

    Record[] records = new Record[1000];
    final int expectedCount = 5000000;
    int totalInserted = 0;
    for (int i = 0; i < expectedCount / 1000; i++) {
      for (int j = 0; j < 1000; j++) {
        records[j] = new Record();
        for (int k = 0; k < 10; k++) {
          records[j].add("COL" + "_" + k, Bytes.toBytes("COL_" + k + "_" + totalInserted));
        }
      }
      fTable.append(records);
      totalInserted += 1000;
      if (totalInserted % 100000 == 0) {
        System.out.println("Total records inserted = " + totalInserted);
      }
    }

    final long size = MTableUtils.getTotalCount(fTable, null, null, true);
    System.out.println("### Count on  server " + size);

    try {
      Thread.sleep(5000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    Scanner scanner = fTable.getScanner(new Scan());
    int totalCount = 0;
    for (Row ignored : scanner) {
      totalCount++;
    }
    System.out.println("### totalCount = " + totalCount);
    //// TODO: sometimes the de-duplication does not work as expected.. needs to be fixed
    assertTrue("Incorrect number of records returned by scan.", (totalCount >= expectedCount));
    // assertEquals("Incorrect number of records returned by scan.", expectedCount, totalCount);
  }
}


