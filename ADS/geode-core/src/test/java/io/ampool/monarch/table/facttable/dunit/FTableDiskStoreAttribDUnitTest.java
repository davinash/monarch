
package io.ampool.monarch.table.facttable.dunit;


import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.facttable.FTableDUnitHelper;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.DiskStoreFactoryImpl;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;

import static org.junit.Assert.*;

@Category(FTableTest.class)
public class FTableDiskStoreAttribDUnitTest extends MTableDUnitHelper {
  private static final int NUM_OF_COLUMNS = 1;
  private static final String COLUMN_NAME_PREFIX = "COL";
  private static String tableName;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    if (tableName != null) {
      deleteFTable(tableName);
    }
    closeMClientCache();
    super.tearDown2();
    tableName = null;
  }



  private void restart() throws InterruptedException {
    stopServerOn(vm0);
    stopServerOn(vm1);
    stopServerOn(vm2);
    closeMClientCache();
    Thread.sleep(10000);
    AsyncInvocation asyncInvocation = asyncStartServerOn(vm0, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocation1 = asyncStartServerOn(vm1, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocation2 = asyncStartServerOn(vm2, DUnitLauncher.getLocatorString());
    asyncInvocation.join();
    asyncInvocation1.join();
    asyncInvocation2.join();
    Thread.sleep(10000);
    createClientCache();
  }


  public static FTableDescriptor getFTableDescriptor(final int numSplits, int redundancy,
                                                     String diskStoreName, boolean synchronous) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < NUM_OF_COLUMNS; i++) {
      tableDescriptor.addColumn(COLUMN_NAME_PREFIX + i);
    }
    tableDescriptor.setTotalNumOfSplits(numSplits);
    tableDescriptor.setRedundantCopies(redundancy);
    if (synchronous) {
      tableDescriptor.enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS);
    }
    if (diskStoreName != null) {
      tableDescriptor.setDiskStore(diskStoreName);
    }
    return tableDescriptor;
  }

  public static FTable createFTable(String ftableName, final int numSplits, int redundancy,
                                    String diskStoreName, boolean synchronous) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    final FTable fTable = clientCache.getAdmin().createFTable(ftableName,
            getFTableDescriptor(numSplits, redundancy, diskStoreName, synchronous));
    return fTable;
  }

  public static FTable createFTable(String ftableName, final int numSplits, int redundancy,
                                    boolean synchronous) {
    return createFTable(ftableName, numSplits, redundancy, null, synchronous);
  }


  public static void deleteFTable(String ftableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    if (admin.existsFTable(tableName)) {
      clientCache.getAdmin().deleteFTable(ftableName);
    }
  }


  private void createDiskStoreOnserver(String disk_store_name, boolean allowDeltaPersistence,
                                       int maxOplogSize, VM[] vms, boolean autoCompact) {
    for (VM vm : vms) {
      vm.invoke(new SerializableCallable<Object>() {
        @Override
        public Object call() throws Exception {
          createDiskStore(disk_store_name, allowDeltaPersistence, maxOplogSize, autoCompact);
          return null;
        }
      });
    }
  }

  private void verifyDiskStore(boolean persistence, String disk_store_name, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(new SerializableCallable<Object>() {
        @Override
        public Object call() throws Exception {
          MCache cache = MCacheFactory.getAnyInstance();
          GemFireCacheImpl gfc = (GemFireCacheImpl) cache;
          Collection<DiskStoreImpl> diskStores = gfc.listDiskStores();
          assertNotNull(diskStores);
          assertFalse(diskStores.isEmpty());
          assertTrue(diskStores.size() == 4);

          for (DiskStoreImpl diskStore : diskStores) {
            if (diskStore.getName().equals(disk_store_name)) {
              assertEquals(persistence, diskStore.getEnableDeltaPersistence());
            }
          }
          return null;
        }
      });
    }
  }

  private void createDiskStore(String disk_store_name, boolean allowDeltaPersistence,
                               int maxOplogSize, boolean autocompact) {
    DiskStoreAttributes dsa = new DiskStoreAttributes();
    DiskStoreFactory dsf = new DiskStoreFactoryImpl(CacheFactory.getAnyInstance(), dsa);
    dsf.setAutoCompact(autocompact);
    dsf.setMaxOplogSize(maxOplogSize);
    dsf.setEnableDeltaPersistence(allowDeltaPersistence);
    dsf.create(disk_store_name);
  }

  @Test
  public void testDiskStoreAttributesWithDeltaPersistence() throws InterruptedException {
    int numSplits = 1;
    int redundancy = 2;
    String DISK_STORE_NAME = "ABC";
    String tableName = getTestMethodName();
    createDiskStoreOnserver(DISK_STORE_NAME, true, 1, new VM[] {vm0, vm1, vm2}, true);
    final FTable table = createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME, false);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    verifyDiskStore(true, DISK_STORE_NAME, vm0, vm1, vm2);
    restart();
    verifyDiskStore(true, DISK_STORE_NAME, vm0, vm1, vm2);
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    if (admin.existsFTable(tableName)) {
      clientCache.getAdmin().deleteFTable(tableName);
    }
  }

  @Test
  public void testDiskStoreAttributesWithoutDeltaPersistence() throws InterruptedException {
    int numSplits = 1;
    int redundancy = 2;
    String DISK_STORE_NAME = "ABC";
    String tableName = getTestMethodName();
    createDiskStoreOnserver(DISK_STORE_NAME, false, 1, new VM[] {vm0, vm1, vm2}, true);
    final FTable table = createFTable(tableName, numSplits, redundancy, DISK_STORE_NAME, false);
    assertNotNull(table);
    FTableDUnitHelper.verifyFTableONServer(tableName, vm0, vm1, vm2);
    verifyDiskStore(false, DISK_STORE_NAME, vm0, vm1, vm2);

    restart();
    verifyDiskStore(false, DISK_STORE_NAME, vm0, vm1, vm2);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    if (admin.existsFTable(tableName)) {
      clientCache.getAdmin().deleteFTable(tableName);
    }
  }



}
