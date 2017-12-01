package io.ampool.monarch.table.region.map;

import static org.junit.Assert.assertTrue;
import static org.testng.Assert.*;

import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.table.internal.ProxyMTableRegion;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

@Category(MonarchTest.class)
public class AmpoolRegionMapFactoryTest {
  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {
    try {
      Cache cache = CacheFactory.getAnyInstance();
      cache.close();
    } catch (Exception ex) {
      // cache is already closed
    }

  }

  public void validateRegionMap(boolean deleteTable) {
    Properties properties = new Properties();
    properties.setProperty("mcast-port", "0");
    MCacheFactory mCacheFactory = new MCacheFactory(properties);
    MCache mCache = mCacheFactory.create();
    // create mtable ordered
    MTableDescriptor orderedMTableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    orderedMTableDescriptor.addColumn("id", BasicTypes.INT);
    MTable mTable_ordered =
        mCache.getAdmin().createMTable("MTable_Ordered", orderedMTableDescriptor);
    Put put = new Put("1");
    put.addColumn("id", 1);
    mTable_ordered.put(put);

    // check the region map and internal map
    RegionMap regionMap =
        ((LocalRegion) ((ProxyMTableRegion) mTable_ordered).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    assertTrue(regionMap instanceof RowTupleLRURegionMap);
    Set<BucketRegion> allLocalBucketRegions =
        ((PartitionedRegion) ((ProxyMTableRegion) mTable_ordered).getTableRegion()).getDataStore()
            .getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      assertTrue(BR.entries instanceof RowTupleLRURegionMap);
      assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentSkipListMap);
      assertTrue(((RowTupleConcurrentSkipListMap) BR.entries.getInternalMap())
          .getInternalMap() instanceof ConcurrentSkipListMap);
    });
    if (deleteTable) {
      mCache.getAdmin().deleteMTable("MTable_Ordered");
    }

    // create mtable unordered
    MTableDescriptor unorderedMTableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    unorderedMTableDescriptor.addColumn("id", BasicTypes.INT);
    MTable mTable_unordered =
        mCache.getAdmin().createMTable("MTable_Unordered", unorderedMTableDescriptor);
    mTable_unordered.put(put);

    // check the region map and internal map
    regionMap =
        ((LocalRegion) ((ProxyMTableRegion) mTable_unordered).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    assertTrue(regionMap instanceof RowTupleLRURegionMap);
    allLocalBucketRegions =
        ((PartitionedRegion) ((ProxyMTableRegion) mTable_unordered).getTableRegion()).getDataStore()
            .getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      assertTrue(BR.entries instanceof RowTupleLRURegionMap);
      assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentHashMap);
    });
    if (deleteTable) {
      mCache.getAdmin().deleteMTable("MTable_Unordered");
    }

    // create FTable
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    fTableDescriptor.addColumn("id", BasicTypes.INT);
    FTable fTable = mCache.getAdmin().createFTable("FTable", fTableDescriptor);
    Record record = new Record();
    record.add("id", 1);
    fTable.append(record);

    // check the region map and internal map
    regionMap = ((LocalRegion) ((ProxyFTableRegion) fTable).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    assertTrue(regionMap instanceof RowTupleLRURegionMap);
    allLocalBucketRegions = ((PartitionedRegion) ((ProxyFTableRegion) fTable).getTableRegion())
        .getDataStore().getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      assertTrue(BR.entries instanceof RowTupleLRURegionMap);
      assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentSkipListMap);
      assertTrue(((RowTupleConcurrentSkipListMap) BR.entries.getInternalMap())
          .getInternalMap() instanceof ConcurrentSkipListMap);
    });
    if (deleteTable) {
      mCache.getAdmin().deleteFTable("FTable");
    }
    mCache.close();
  }

  public void validateRegionMapAfterRecovery(boolean deleteTable) {
    Properties properties = new Properties();
    properties.setProperty("mcast-port", "0");
    MCacheFactory mCacheFactory = new MCacheFactory(properties);
    MCache mCache = mCacheFactory.create();
    // get mtable ordered
    MTable mTable_ordered = mCache.getMTable("MTable_Ordered");

    // check the region map and internal map
    RegionMap regionMap =
        ((LocalRegion) ((ProxyMTableRegion) mTable_ordered).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    assertTrue(regionMap instanceof RowTupleLRURegionMap);
    Set<BucketRegion> allLocalBucketRegions =
        ((PartitionedRegion) ((ProxyMTableRegion) mTable_ordered).getTableRegion()).getDataStore()
            .getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      assertTrue(BR.entries instanceof RowTupleLRURegionMap);
      assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentSkipListMap);
      assertTrue(((RowTupleConcurrentSkipListMap) BR.entries.getInternalMap())
          .getInternalMap() instanceof ConcurrentSkipListMap);
    });
    if (deleteTable) {
      mCache.getAdmin().deleteMTable("MTable_Ordered");
    }

    // get mtable unordered
    MTable mTable_unordered = mCache.getMTable("MTable_Unordered");

    // check the region map and internal map
    regionMap =
        ((LocalRegion) ((ProxyMTableRegion) mTable_unordered).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    assertTrue(regionMap instanceof RowTupleLRURegionMap);
    allLocalBucketRegions =
        ((PartitionedRegion) ((ProxyMTableRegion) mTable_unordered).getTableRegion()).getDataStore()
            .getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      assertTrue(BR.entries instanceof RowTupleLRURegionMap);
      assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentHashMap);
    });
    if (deleteTable) {
      mCache.getAdmin().deleteMTable("MTable_Unordered");
    }

    // get FTable
    FTable fTable = mCache.getFTable("FTable");

    // check the region map and internal map
    regionMap = ((LocalRegion) ((ProxyFTableRegion) fTable).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    assertTrue(regionMap instanceof RowTupleLRURegionMap);
    allLocalBucketRegions = ((PartitionedRegion) ((ProxyFTableRegion) fTable).getTableRegion())
        .getDataStore().getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      assertTrue(BR.entries instanceof RowTupleLRURegionMap);
      assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentSkipListMap);
      assertTrue(((RowTupleConcurrentSkipListMap) BR.entries.getInternalMap())
          .getInternalMap() instanceof ConcurrentSkipListMap);
    });
    if (deleteTable) {
      mCache.getAdmin().deleteFTable("FTable");
    }
    mCache.close();
  }

  public void validateRegionMapNOLRU(boolean deleteTable) {
    Properties properties = new Properties();
    properties.setProperty("mcast-port", "0");
    MCacheFactory mCacheFactory = new MCacheFactory(properties);
    MCache mCache = mCacheFactory.create();
    // create mtable ordered
    MTableDescriptor orderedMTableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    orderedMTableDescriptor.addColumn("id", BasicTypes.INT);
    orderedMTableDescriptor.setEvictionPolicy(MEvictionPolicy.NO_ACTION);
    MTable mTable_ordered =
        mCache.getAdmin().createMTable("MTable_Ordered_NOEviction", orderedMTableDescriptor);
    Put put = new Put("1");
    put.addColumn("id", 1);
    mTable_ordered.put(put);

    // check the region map and internal map
    RegionMap regionMap =
        ((LocalRegion) ((ProxyMTableRegion) mTable_ordered).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    assertTrue(regionMap instanceof RowTupleRegionMap);
    Set<BucketRegion> allLocalBucketRegions =
        ((PartitionedRegion) ((ProxyMTableRegion) mTable_ordered).getTableRegion()).getDataStore()
            .getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      assertTrue(BR.entries instanceof RowTupleRegionMap);
      assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentSkipListMap);
      assertTrue(((RowTupleConcurrentSkipListMap) BR.entries.getInternalMap())
          .getInternalMap() instanceof ConcurrentSkipListMap);
    });
    if (deleteTable) {
      mCache.getAdmin().deleteMTable("MTable_Ordered_NOEviction");
    }

    // create mtable unordered
    MTableDescriptor unorderedMTableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    unorderedMTableDescriptor.addColumn("id", BasicTypes.INT);
    unorderedMTableDescriptor.setEvictionPolicy(MEvictionPolicy.NO_ACTION);

    MTable mTable_unordered =
        mCache.getAdmin().createMTable("MTable_Unordered_NOEviction", unorderedMTableDescriptor);
    mTable_unordered.put(put);

    // check the region map and internal map
    regionMap =
        ((LocalRegion) ((ProxyMTableRegion) mTable_unordered).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    assertTrue(regionMap instanceof RowTupleRegionMap);
    allLocalBucketRegions =
        ((PartitionedRegion) ((ProxyMTableRegion) mTable_unordered).getTableRegion()).getDataStore()
            .getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      assertTrue(BR.entries instanceof RowTupleRegionMap);
      assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentHashMap);
    });
    if (deleteTable) {
      mCache.getAdmin().deleteMTable("MTable_Unordered_NOEviction");
    }
    mCache.close();
  }

  public void validateRegionMapAfterRecoveryNOLRU(boolean deleteTable) {
    Properties properties = new Properties();
    properties.setProperty("mcast-port", "0");
    MCacheFactory mCacheFactory = new MCacheFactory(properties);
    MCache mCache = mCacheFactory.create();
    // get mtable ordered
    MTable mTable_ordered = mCache.getMTable("MTable_Ordered_NOEviction");

    // check the region map and internal map
    RegionMap regionMap =
        ((LocalRegion) ((ProxyMTableRegion) mTable_ordered).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    assertTrue(regionMap instanceof RowTupleRegionMap);
    Set<BucketRegion> allLocalBucketRegions =
        ((PartitionedRegion) ((ProxyMTableRegion) mTable_ordered).getTableRegion()).getDataStore()
            .getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      assertTrue(BR.entries instanceof RowTupleRegionMap);
      assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentSkipListMap);
      assertTrue(((RowTupleConcurrentSkipListMap) BR.entries.getInternalMap())
          .getInternalMap() instanceof ConcurrentSkipListMap);
    });
    if (deleteTable) {
      mCache.getAdmin().deleteMTable("MTable_Ordered_NOEviction");
    }

    // get mtable unordered
    MTable mTable_unordered = mCache.getMTable("MTable_Unordered_NOEviction");

    // check the region map and internal map
    regionMap =
        ((LocalRegion) ((ProxyMTableRegion) mTable_unordered).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    assertTrue(regionMap instanceof RowTupleRegionMap);
    allLocalBucketRegions =
        ((PartitionedRegion) ((ProxyMTableRegion) mTable_unordered).getTableRegion()).getDataStore()
            .getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      assertTrue(BR.entries instanceof RowTupleRegionMap);
      assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentHashMap);
    });
    if (deleteTable) {
      mCache.getAdmin().deleteMTable("MTable_Unordered_NOEviction");
    }
    mCache.close();
  }


  @Test
  public void testRegionMap() {
    validateRegionMap(true);
  }

  @Test
  public void testRegionMapRecovery() {
    validateRegionMap(false);
    validateRegionMapAfterRecovery(true);
  }

  @Test
  public void testRegionMapNOLRU() {
    validateRegionMapNOLRU(true);
  }

  @Test
  public void testRegionMapRecoveryNOLRU() {
    validateRegionMapNOLRU(false);
    validateRegionMapAfterRecoveryNOLRU(true);
  }

}
