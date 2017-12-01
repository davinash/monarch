package io.ampool.monarch.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;

import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MTableEvictionDUnitTest extends MTableDUnitHelper {
  public MTableEvictionDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "MTableOverflowToDiskDUnitTest";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int LATEST_TIMESTAMP = 300;
  protected final int MAX_VERSIONS = 5;
  protected final int TABLE_MAX_VERSIONS = 7;

  private final String[] COL_NAME = {"Name", "ID", "Age", "Salary", "City"};

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);

    new ArrayList<>(Arrays.asList(vm0)).forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    }));

    super.tearDown2();
  }

  private MTable createTable(final String name, int maxEntries, boolean ordered) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor =
        ordered ? new MTableDescriptor() : new MTableDescriptor(MTableType.UNORDERED);
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.addColumn(Bytes.toBytes(COL_NAME[0]));
    tableDescriptor.addColumn(Bytes.toBytes(COL_NAME[1]));
    tableDescriptor.addColumn(Bytes.toBytes(COL_NAME[2]));
    tableDescriptor.addColumn(Bytes.toBytes(COL_NAME[3]));
    tableDescriptor.addColumn(Bytes.toBytes(COL_NAME[4]));

    return clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);

  }

  public void verifyKeysOnServers(int evictedEntries) {

    new ArrayList<>(Arrays.asList(vm0)).forEach((VM) -> VM.invoke(new SerializableCallable() {
      int evictedEntriesCount = 0;

      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        PartitionedRegion pr = (PartitionedRegion) cache.getRegion(TABLE_NAME);
        Set<BucketRegion> allLocalPrimaryBucketRegions =
            pr.getDataStore().getAllLocalPrimaryBucketRegions();
        for (BucketRegion bucket : allLocalPrimaryBucketRegions) {
          RowTupleConcurrentSkipListMap internalMap =
              (RowTupleConcurrentSkipListMap) bucket.getRegionMap().getInternalMap();
          Map realMap = internalMap.getInternalMap();
          realMap.forEach((KEY, VALUE) -> {
            if (Token.NOT_A_TOKEN != ((RegionEntry) VALUE).getValueAsToken()) {
              evictedEntriesCount++;
            }
          });
        }
        assertEquals(evictedEntries, evictedEntriesCount);
        return null;
      }
    }));
  }


  public void verifyKeysOnServersUnordered(int evictedEntries) {

    new ArrayList<>(Arrays.asList(vm0)).forEach((VM) -> VM.invoke(new SerializableCallable() {
      int evictedEntriesCount = 0;

      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        PartitionedRegion pr = (PartitionedRegion) cache.getRegion(TABLE_NAME);
        Set<BucketRegion> allLocalPrimaryBucketRegions =
            pr.getDataStore().getAllLocalPrimaryBucketRegions();
        for (BucketRegion bucket : allLocalPrimaryBucketRegions) {
          CustomEntryConcurrentHashMap internalMap =
              (CustomEntryConcurrentHashMap) bucket.getRegionMap().getInternalMap();
          internalMap.forEach((KEY, VALUE) -> {
            if (Token.NOT_A_TOKEN != ((RegionEntry) VALUE).getValueAsToken()) {
              evictedEntriesCount++;
            }
          });
        }
        assertEquals(evictedEntries, evictedEntriesCount);
        return null;
      }
    }));
  }

  public void mTableEntriesEvictionTest(boolean ordered) {
    int totalEntries = 100;
    int maxEntries = 5;
    MTable table = createTable(TABLE_NAME, maxEntries, ordered);
    for (int i = 0; i < totalEntries; i++) {
      Put put = new Put(KEY_PREFIX + i);
      for (int j = 0; j < COL_NAME.length; j++) {
        put.addColumn(COL_NAME[j], Bytes.toBytes(VALUE_PREFIX + "-" + COL_NAME[j]));
      }
      table.put(put);
    }

    int entriesEvicted = 0;
    if (totalEntries > maxEntries)
      entriesEvicted = totalEntries - maxEntries;
    if (ordered) {
      verifyKeysOnServers(entriesEvicted);
    } else
      verifyKeysOnServersUnordered(entriesEvicted);

    for (int i = 0; i < totalEntries; i++) {
      Get get = new Get(KEY_PREFIX + i);
      Row row = table.get(get);
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);

  }


  public void mTableEntriesEvictionTestPartialGet(boolean ordered) {
    int totalEntries = 10;
    int maxEntries = 100;
    MTable table = createTable(TABLE_NAME, maxEntries, ordered);
    for (int i = 0; i < totalEntries; i++) {
      Put put = new Put(KEY_PREFIX + i);
      for (int j = 0; j < COL_NAME.length; j++) {
        put.addColumn(COL_NAME[j], Bytes.toBytes(VALUE_PREFIX + "-" + COL_NAME[j]));
      }
      table.put(put);
    }

    int entriesEvicted = 0;
    if (totalEntries > maxEntries)
      entriesEvicted = totalEntries - maxEntries;
    if (ordered) {
      verifyKeysOnServers(entriesEvicted);
    } else
      verifyKeysOnServersUnordered(entriesEvicted);

    for (int i = 0; i < totalEntries; i++) {
      Get get = new Get(KEY_PREFIX + i);
      get.addColumn(Bytes.toBytes(COL_NAME[0]));
      Row row = table.get(get);
      assertEquals(row.getCells().size(), 1);
      assertEquals(Bytes.toString(row.getCells().get(0).getColumnName()), COL_NAME[0]);

      assertEquals(Bytes.toString((byte[]) row.getCells().get(0).getColumnValue()),
          VALUE_PREFIX + "-" + COL_NAME[0]);
    }
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);

  }


  public void __DISABLED_testOrderedTableEntriesEviction() {
    mTableEntriesEvictionTest(true);
  }

  public void __DISABLED_testUnOrderedTableEntriesEviction() {
    mTableEntriesEvictionTest(false);
  }

  @Test
  public void testOrderedTableEntriesEvictionPartialGet() {
    mTableEntriesEvictionTestPartialGet(true);
  }

  @Test
  public void testUnOrderedTableEntriesEvictionPartialGet() {
    mTableEntriesEvictionTestPartialGet(false);
  }


}
