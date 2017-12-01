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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.MonarchTest;
import static org.junit.Assert.*;

import org.apache.geode.cache.CacheClosedException;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;

@Category(MonarchTest.class)
public class MTableDiskPersistenceAllTests_withDiskStore extends MTableDiskPersistenceTestBase {
  public MTableDiskPersistenceAllTests_withDiskStore() {
    super();
  }

  private final String diskStoreName = "MTableDiskStore";

  @Override
  public void preSetUp() throws Exception {
    super.preSetUp();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startAllServers();
  }

  @Override
  public void tearDown2() throws Exception {
    stopAllServers();
    super.tearDown2();
  }

  @Override
  protected void startAllServers() {
    super.startAllServers();
    createDiskStore(diskStoreName);
  }

  @Override
  public AsyncInvocation asyncStartServerOn(VM vm, final String locators) {
    return vm.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        MCache c = null;
        try {
          c = MCacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        c = MCacheFactory.create(getSystem(props));
        CacheServer s = c.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        s.setPort(port);
        s.start();
        DiskStoreFactory diskStoreFactory = MCacheFactory.getAnyInstance().createDiskStoreFactory();
        diskStoreFactory.create(diskStoreName);
        MCacheFactory.getAnyInstance();
        // registerFunction();
        return port;
      }
    });
  }

  @Override
  protected void startAllServersAsync() {
    AsyncInvocation asyncInvocation1 =
        (AsyncInvocation) asyncStartServerOn(vm0, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocation2 =
        (AsyncInvocation) asyncStartServerOn(vm1, DUnitLauncher.getLocatorString());
    AsyncInvocation asyncInvocation3 =
        (AsyncInvocation) asyncStartServerOn(vm2, DUnitLauncher.getLocatorString());
    try {
      asyncInvocation1.join();
      asyncInvocation2.join();
      asyncInvocation3.join();
      createClientCache(this.client1);
      createClientCache();
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }
  }


  @Override
  protected void createTable(final String tableName) {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setRedundantCopies(1).enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
        .setDiskStore(diskStoreName).setMaxVersions(5);
    MTable table = clientCache.getAdmin().createTable(tableName, tableDescriptor);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);
  }

  @Override
  protected void createTable(final String tableName, final boolean ordered, int numSplits) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setMaxVersions(MAX_VERSIONS);
    tableDescriptor.setRedundantCopies(1);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
    tableDescriptor.setDiskStore(diskStoreName);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    MTable table = clientCache.getAdmin().createTable(tableName, tableDescriptor);
    assertEquals(table.getName(), tableName);
    assertNotNull(table);
  }

  @Test
  public void testPutsGetsWithFrequentColumnUpdatesOrdered() {
    putsGetsWithFrequentColumnUpdates(getTestMethodName(), true);
    stopAllServers();
    startAllServersAsync();
    putsGetsWithFrequentColumnUpdatesAfterRestart(getTestMethodName(), true);
  }

  @Test
  public void testPutsGetsWithFrequentColumnUpdatesUnordered() {
    putsGetsWithFrequentColumnUpdates(getTestMethodName(), false);
    stopAllServers();
    startAllServersAsync();
    putsGetsWithFrequentColumnUpdatesAfterRestart(getTestMethodName(), false);
  }

  @Test
  public void testPutsWithMaxVersionSpecifiedOrdered() {
    putsWithMaxVersionSpecified(getTestMethodName(), true);
    stopAllServers();
    startAllServersAsync();
    putsWithMaxVersionSpecifiedAfterRestart(getTestMethodName(), true);
  }

  @Test
  public void testPutsWithMaxVersionSpecifiedUnOrdered() {
    putsWithMaxVersionSpecified(getTestMethodName(), false);
    stopAllServers();
    startAllServersAsync();
    putsWithMaxVersionSpecifiedAfterRestart(getTestMethodName(), false);
  }

  @Test
  public void testPutsWithTimeStampAndGetWithTimeStampFromOtherClientOrdered() {
    putsWithTimeStampAndGetWithTimeStampFromOtherClient(getTestMethodName(), true);
    stopAllServers();
    startAllServersAsync();
    putsWithTimeStampAndGetWithTimeStampFromOtherClientAfterRestart(getTestMethodName(), true);
  }

  @Test
  public void testPutsWithTimeStampAndGetWithTimeStampFromOtherClientUnOrdered() {
    putsWithTimeStampAndGetWithTimeStampFromOtherClient(getTestMethodName(), false);
    stopAllServers();
    startAllServersAsync();
    putsWithTimeStampAndGetWithTimeStampFromOtherClientAfterRestart(getTestMethodName(), false);
  }

  @Test
  public void testPutsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClientOrdered() {
    putsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClient(getTestMethodName(), true);
    stopAllServers();
    startAllServersAsync();
    putsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClientAfterRestart(
        getTestMethodName(), true);
  }

  @Test
  public void testPutsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClientUnOrdered() {
    putsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClient(getTestMethodName(), false);
    stopAllServers();
    startAllServersAsync();
    putsWithMTableMultiBasicOpDunitTestVersionsAndVerifyFromOtherClientAfterRestart(
        getTestMethodName(), false);
  }

  @Test
  public void testUpdatePartialColumnGetAllColumnsOrdered() {
    updatePartialColumnGetAllColumns(getTestMethodName(), true);
    stopAllServers();
    startAllServersAsync();
    updatePartialColumnGetAllColumnsAfterRestart(getTestMethodName(), true);
  }

  @Test
  public void testUpdatePartialColumnGetAllColumnsUnOrdered() {
    updatePartialColumnGetAllColumns(getTestMethodName(), false);
    stopAllServers();
    startAllServersAsync();
    updatePartialColumnGetAllColumnsAfterRestart(getTestMethodName(), false);
  }

  @Test
  public void testPutAllColumnsGetPartialColumnsOrdered() {
    putAllColumnsGetPartialColumns(getTestMethodName(), true);
    stopAllServers();
    startAllServersAsync();
    putAllColumnsGetPartialColumnsAfterRestart(getTestMethodName(), true);
  }

  @Test
  public void testPutAllColumnsGetPartialColumnsUnOrdered() {
    putAllColumnsGetPartialColumns(getTestMethodName(), false);
    stopAllServers();
    startAllServersAsync();
    putAllColumnsGetPartialColumnsAfterRestart(getTestMethodName(), false);
  }

  @Test
  public void testPutFromOneClientAndGetFromAnotherOrdered() {
    putFromOneClientAndGetFromAnother(getTestMethodName(), true);
    stopAllServers();
    startAllServersAsync();
    putFromOneClientAndGetFromAnotherAfterRestart(getTestMethodName(), true);
  }

  @Test
  public void testPutFromOneClientAndGetFromAnotherUnOrdered() {
    putFromOneClientAndGetFromAnother(getTestMethodName(), false);
    stopAllServers();
    startAllServersAsync();
    putFromOneClientAndGetFromAnotherAfterRestart(getTestMethodName(), false);
  }


  // Tests for batch put gets
  @Test
  public void testBatchPutGetRowsOrdered() {
    batchPutGetRows(getTestMethodName(), true);
    stopAllServers();
    startAllServersAsync();
    batchPutGetRowsAfterRestart(getTestMethodName(), true);
  }

  @Test
  public void testBatchPutGetRowsUnOrdered() {
    batchPutGetRows(getTestMethodName(), false);
    stopAllServers();
    startAllServersAsync();
    batchPutGetRowsAfterRestart(getTestMethodName(), false);
  }

  @Test
  public void testBatchPutWithMaxVersionEqualsToOne() {
    batchPutWithMaxVersionEqualsToOne(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    batchPutWithMaxVersionEqualsToOneAfterRestart(getTestMethodName());
  }

  // Scan Tests

  /* Scan with null object should not be allowd */
  @Test
  public void testScanCreateWithNullObject() {
    scanCreateWithNullObject(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    scanCreateWithNullObjectAfterrestart(getTestMethodName());
  }

  @Test
  public void testScannerOnEmptyTable() {
    scannerOnEmptyTable(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    scanCreateWithNullObjectAfterrestart(getTestMethodName());
  }

  public void _testScannerUsingMCoprocessor() {
    scannerUsingMCoprocessor(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    scannerUsingMCoprocessorAfterRestart(getTestMethodName());
  }

  @Test
  public void testSimpleScannerUsingGetAll() throws IOException {
    List<byte[]> listOfKeys = simpleScannerUsingGetAll(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    simpleScannerUsingGetAllAfterRestart(getTestMethodName(), listOfKeys);
  }

  @Test
  public void testSimpleScannerUsingGetAllKeys() throws IOException {
    List<byte[]> listOfKeys = simpleScannerUsingGetAllKeys(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    simpleScannerUsingGetAllKeysAfterRestart(getTestMethodName(), listOfKeys);
  }

  @Test
  public void testSimpleScannerUsingGetAllNoKeys() throws IOException {
    List<byte[]> listOfKeys = simpleScannerUsingGetAllNoKeys(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    simpleScannerUsingGetAllNoKeysAfterRestart(getTestMethodName(), listOfKeys);
  }

  @Test
  public void testScannerUsingGetAll_UnOrdered() {
    // default
    int expectedSize = doScannerUsingGetAll_UnOrdered(getTestMethodName(), null);
    stopAllServers();
    startAllServersAsync();
    doScannerUsingGetAll_UnOrderedAfterRestart(getTestMethodName(), null, expectedSize);

  }

  @Test
  public void testScannerUsingGetAll_UnOrdered_keys() {
    // explicit request for keys
    int expectedsize = doScannerUsingGetAll_UnOrdered(getTestMethodName(), true);
    stopAllServers();
    startAllServersAsync();
    doScannerUsingGetAll_UnOrderedAfterRestart(getTestMethodName(), true, expectedsize);
  }

  @Test
  public void testScannerUsingGetAll_UnOrdered_nokeys() {
    // explicit request for no keys
    int expectedsize = doScannerUsingGetAll_UnOrdered(getTestMethodName(), false);
    stopAllServers();
    startAllServersAsync();
    doScannerUsingGetAll_UnOrderedAfterRestart(getTestMethodName(), false, expectedsize);
  }

  @Test
  public void testScanner_UnOrderedWithPredicates() {
    int dataLength = scanner_UnOrderedWithPredicates(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    scanner_UnOrderedWithPredicatesAfterRestart(getTestMethodName(), dataLength);
  }

  @Test
  public void testScanner_UnOrderedWithRange() {
    scanner_UnOrderedWithRange(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    scanner_UnOrderedWithRangeAfterRestart(getTestMethodName());
  }

  @Test
  public void testScanner_UnOrderedWithSelectedColumns() {
    scanner_UnOrderedWithSelectedColumns(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    scanner_UnOrderedWithSelectedColumnsAfterRestart(getTestMethodName());
  }

  @Test
  public void testClientScanner() {
    Map<Integer, List<byte[]>> integerListMap = clientScannerTest(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    clientScannerTestAfterRestart(getTestMethodName(), integerListMap);
  }

  @Test
  public void testScanSortOrder() {
    Set<String> strings = scanSortOrderTest(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    scanSortOrderTestAfterRestart(getTestMethodName(), strings);
  }

  @Test
  public void testScanResultBatching() {
    Set<String> strings = resultBatchingTest(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    resultBatchingTestAfterRestart(getTestMethodName(), strings);
  }

  @Test
  public void testScanByStopRow() {
    Set<String> strings = scanByStopRowTest(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    scanByStopRowTestAfterRestart(getTestMethodName(), strings);
  }

  @Test
  public void testScanByStartRow() {
    Set<String> strings = scanByStartRowTest(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    scanByStartRowTestAfterRestart(getTestMethodName(), strings);

  }

  @Test
  public void testFullTableScan() {
    Set<String> strings = fullTableScanTest(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    fullTableScanTestAfterRestart(getTestMethodName(), strings);
  }

  @Test
  public void testFullTableScanWithSelColumn() {
    Set<String> strings = fullTableScanWithSelColumnTest(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    fullTableScanWithSelColumnTestAfterRestart(getTestMethodName(), strings);
  }

  @Test
  public void testFullTableScanWithSelColumns() {
    Set<String> strings = fullTableScanWithSelColumnsTest(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    fullTableScanWithSelColumnsTestAfterRestart(getTestMethodName(), strings);
  }

  @Test
  public void testScanForMaxSize() {
    Set<String> strings = scanForMaxSizeTest(getTestMethodName());
    stopAllServers();
    startAllServersAsync();
    scanForMaxSizeTestAfterRestart(getTestMethodName(), strings);
  }



  // TODO: DeleteOp Tests

  // TODO: Coprocessor Tests


}
