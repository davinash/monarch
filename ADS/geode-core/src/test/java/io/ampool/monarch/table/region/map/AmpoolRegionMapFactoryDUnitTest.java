package io.ampool.monarch.table.region.map;

import static org.junit.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.FTableImpl;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.table.internal.MTableImpl;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

@Category(MonarchTest.class)
public class AmpoolRegionMapFactoryDUnitTest extends MTableDUnitHelper {

  private final String mtableOrdered = "MTable_Ordered";
  private final String mtableUnOrdered = "MTable_UnOrdered";
  private final String mtableOrderedLRU = "MTable_Ordered_LRU";
  private final String mtableUnOrderedLRu = "MTable_UnOrdered_LRU";
  private final String ftableName = "FTable";

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    stopServerOn(this.vm0);
    stopServerOn(this.vm1);
    stopServerOn(this.vm2);
    super.tearDown2();
  }

  private void createMTable(String tableName, boolean isLRU, boolean isOrdered) {
    MClientCache mCache = MClientCacheFactory.getAnyInstance();
    // create mtable ordered
    MTableDescriptor orderedMTableDescriptor =
        new MTableDescriptor(isOrdered ? MTableType.ORDERED_VERSIONED : MTableType.UNORDERED);
    orderedMTableDescriptor.addColumn("id", BasicTypes.INT);
    if (!isLRU) {
      orderedMTableDescriptor.setEvictionPolicy(MEvictionPolicy.NO_ACTION);
    }
    MTable mTable_ordered = mCache.getAdmin().createMTable(tableName, orderedMTableDescriptor);
    Put put = new Put("1");
    put.addColumn("id", 1);
    mTable_ordered.put(put);
    put = new Put("2");
    put.addColumn("id", 2);
    mTable_ordered.put(put);
    put = new Put("3");
    put.addColumn("id", 3);
    mTable_ordered.put(put);
    put = new Put("4");
    put.addColumn("id", 4);
    mTable_ordered.put(put);
  }

  private void createFTable(String tableName) {
    MClientCache mCache = MClientCacheFactory.getAnyInstance();
    // create FTable
    FTableDescriptor fTableDescriptor = new FTableDescriptor();
    fTableDescriptor.addColumn("id", BasicTypes.INT);
    FTable fTable = mCache.getAdmin().createFTable(tableName, fTableDescriptor);
    Record record = new Record();
    record.add("id", 1);
    fTable.append(record);
    record = new Record();
    record.add("id", 2);
    fTable.append(record);
    record = new Record();
    record.add("id", 3);
    fTable.append(record);
    record = new Record();
    record.add("id", 4);
    fTable.append(record);

  }

  private void validateMTableRegionMapOnAllVMs(final String tableName, final boolean isOrdered,
      final boolean isLRU) {
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            validateMTableRegionMap(tableName, isOrdered, isLRU);
            return null;
          }
        }));
  }

  private void validateFTableRegionMapOnAllVMs(final String tableName) {
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            validateFTableRegionMap(tableName);
            return null;
          }
        }));
  }

  private void validateMTableRegionMap(final String tableName, final boolean isOrdered,
      final boolean isLRU) {
    MCache mCache = MCacheFactory.getAnyInstance();
    // get ftable
    MTable mTable_ordered = mCache.getMTable(tableName);

    // check the region map and internal map
    RegionMap regionMap =
        ((LocalRegion) ((MTableImpl) mTable_ordered).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    if (isLRU) {
      assertTrue(regionMap instanceof RowTupleLRURegionMap);
    } else {
      assertTrue(regionMap instanceof RowTupleRegionMap);
    }
    Set<BucketRegion> allLocalBucketRegions =
        ((PartitionedRegion) ((MTableImpl) mTable_ordered).getTableRegion()).getDataStore()
            .getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      if (isLRU) {
        assertTrue(BR.entries instanceof RowTupleLRURegionMap);
      } else {
        assertTrue(BR.entries instanceof RowTupleRegionMap);
      }
      if (isOrdered) {
        assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentSkipListMap);
        assertTrue(((RowTupleConcurrentSkipListMap) BR.entries.getInternalMap())
            .getInternalMap() instanceof ConcurrentSkipListMap);
      } else {
        assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentHashMap);
      }
    });
  }

  public void validateFTableRegionMap(final String tableName) {
    MCache mCache = MCacheFactory.getAnyInstance();
    FTable ftable = mCache.getFTable(tableName);
    // check the region map and internal map
    RegionMap regionMap = ((LocalRegion) ((FTableImpl) ftable).getTableRegion()).getRegionMap();
    assertNotNull(regionMap);
    assertTrue(regionMap instanceof RowTupleLRURegionMap);
    Set<BucketRegion> allLocalBucketRegions =
        ((PartitionedRegion) ((FTableImpl) ftable).getTableRegion()).getDataStore()
            .getAllLocalBucketRegions();
    allLocalBucketRegions.forEach((BR) -> {
      assertTrue(BR.entries instanceof RowTupleLRURegionMap);
      assertTrue(BR.entries.getInternalMap() instanceof RowTupleConcurrentSkipListMap);
      assertTrue(((RowTupleConcurrentSkipListMap) BR.entries.getInternalMap())
          .getInternalMap() instanceof ConcurrentSkipListMap);
    });
  }

  private void deleteTable(final String tableName) {
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(tableName);
  }

  private void restartServers() throws InterruptedException {
    closeMClientCache();
    stopServerOn(this.vm0);
    stopServerOn(this.vm1);
    stopServerOn(this.vm2);
    Thread.sleep(10000);
    AsyncInvocation asyncInvocationVM0 =
        asyncStartServerOn(this.vm0, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocationVM1 =
        asyncStartServerOn(this.vm1, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocationVM2 =
        asyncStartServerOn(this.vm2, DUnitLauncher.getLocatorString());
    asyncInvocationVM0.join();
    asyncInvocationVM1.join();
    asyncInvocationVM2.join();
    createClientCache();
  }


  @Test
  public void testRegionMap() {
    createMTable(mtableOrdered, false, true);
    validateMTableRegionMapOnAllVMs(mtableOrdered, true, false);
    deleteTable(mtableOrdered);
    createMTable(mtableUnOrdered, false, false);
    validateMTableRegionMapOnAllVMs(mtableUnOrdered, false, false);
    deleteTable(mtableUnOrdered);
    createMTable(mtableOrderedLRU, true, true);
    validateMTableRegionMapOnAllVMs(mtableOrderedLRU, true, true);
    deleteTable(mtableOrderedLRU);
    createMTable(mtableUnOrderedLRu, true, false);
    validateMTableRegionMapOnAllVMs(mtableUnOrderedLRu, false, true);
    deleteTable(mtableUnOrderedLRu);
    createFTable(ftableName);
    validateFTableRegionMapOnAllVMs(ftableName);
    deleteTable(ftableName);
  }

  @Test
  public void testRegionMapRecovery() throws InterruptedException {
    createMTable(mtableOrdered, false, true);
    validateMTableRegionMapOnAllVMs(mtableOrdered, true, false);
    createMTable(mtableUnOrdered, false, false);
    validateMTableRegionMapOnAllVMs(mtableUnOrdered, false, false);
    createMTable(mtableOrderedLRU, true, true);
    validateMTableRegionMapOnAllVMs(mtableOrderedLRU, true, true);
    createMTable(mtableUnOrderedLRu, true, false);
    validateMTableRegionMapOnAllVMs(mtableUnOrderedLRu, false, true);
    createFTable(ftableName);
    validateFTableRegionMapOnAllVMs(ftableName);
    restartServers();
    validateMTableRegionMapOnAllVMs(mtableOrdered, true, false);
    validateMTableRegionMapOnAllVMs(mtableUnOrdered, false, false);
    validateMTableRegionMapOnAllVMs(mtableOrderedLRU, true, true);
    validateMTableRegionMapOnAllVMs(mtableOrderedLRU, false, true);
    validateFTableRegionMapOnAllVMs(ftableName);
    deleteTable(mtableOrdered);
    deleteTable(mtableUnOrdered);
    deleteTable(mtableOrderedLRU);
    deleteTable(mtableUnOrderedLRu);
    deleteTable(ftableName);
  }
}
