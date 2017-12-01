/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.ampool.monarch.table;

import com.jayway.awaitility.Awaitility;
import io.ampool.monarch.cli.MashCliStrings;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.TableType;
import io.ampool.monarch.types.TypeUtils;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.commands.CliCommandTestBase;
import org.apache.geode.management.internal.cli.commands.MTableCommands;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The MTableCommandsDUnitTest class is a test suite of functional tests cases testing the proper
 * functioning of the 'list mtable' 'describe mtable' and 'delete mtable' commands.
 * </p>
 *
 * @see CliCommandTestBase
 * @see MTableCommands
 * @since 7.0
 */
@Category(MonarchTest.class)
public class MTableCommandsDUnitTest extends MTableDUnitHelperForCLICommands {

  Host host = null;
  private VM vm0 = null;
  private VM vm1 = null;
  private VM vm2 = null;
  private VM client1 = null;

  private int DEFAULT_VERSIONS = 1;
  private int DEFAULT_REDUNDENCY = 1;
  private int DEFAULT_SPLITS = 113;

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "Employee";// _Table_1_2";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 2;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";

  Set<ExpectedException> exceptions = new HashSet<ExpectedException>();

  public MTableCommandsDUnitTest() {
    super();
  }

  @Override
  public void preSetUp() {

  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    client1 = host.getVM(3);

    startServerOn(this.vm0, DUnitLauncher.getLocatorString(), true);
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
    createDefaultSetup(null);
    createClientCache();
    setupGemFireContainers();

  }

  @Override
  public void tearDown2() throws Exception {
    deleteTable(client1, "TestMTablePeer2");
    deleteTable(client1, "TestMTablePeer1");

    closeMClientCache(client1);
    new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
        .forEach((VM) -> VM.invoke(new SerializableCallable() {
          @Override
          public Object call() throws Exception {
            MCacheFactory.getAnyInstance().close();
            return null;
          }
        }));

    super.tearDown2();
  }

  protected void setupGemFireContainers() throws Exception {
    createPersistentRegion(vm1, "consumers", "consumerData");
    createPersistentRegion(vm1, "observers", "observerData");
    createPersistentRegion(vm2, "producer", "producerData");
    createPersistentRegion(vm2, "producer-factory", "producerData");
    createMTable(client1, "TestMTablePeer1");
    createMTable(client1, "TestMTablePeer2");

  }


  private static void forceEvictiononServer(final VM vm, final String regionName)
      throws RMIException {

    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() throws Exception {
        try {
          MCacheFactory.getAnyInstance().getResourceManager().setEvictionHeapPercentage(1);
          final PartitionedRegion pr =
              (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(regionName);
          assertNotNull(pr);

          raiseFakeNotification();

          /** wait for 60 seconds till all entries are evicted.. **/
          Awaitility.await().with().pollInterval(1, TimeUnit.SECONDS).atMost(60, TimeUnit.SECONDS)
              .until(() -> getTotalEntryCount(pr) == 0);

          assertEquals("Expected no entries.", 0, getTotalEntryCount(pr));
        } finally {
          ((MonarchCacheImpl) MCacheFactory.getAnyInstance())
              .getHeapEvictor().testAbortAfterLoopCount = Integer.MAX_VALUE;
          HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
          System.clearProperty("gemfire.memoryEventTolerance");
        }

      }
    });
  }


  /**
   * Get the total count of entries in the region; aggregate entries across all buckets.
   *
   * @param pr the partitioned region
   * @return the total number of entries
   */
  private static int getTotalEntryCount(final PartitionedRegion pr) {
    return pr.getDataStore().getAllLocalBucketRegions().stream().mapToInt(BucketRegion::size).sum();
  }


  /**
   * Raise a fake notification so that eviction gets started..
   */
  public static void raiseFakeNotification() {
    ((MonarchCacheImpl) MCacheFactory.getAnyInstance()).getHeapEvictor().testAbortAfterLoopCount =
        1;
    HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
    System.setProperty("gemfire.memoryEventTolerance", "0");
    MCacheFactory.getAnyInstance().getResourceManager().setEvictionHeapPercentage(85);
    HeapMemoryMonitor hmm =
        ((MonarchCacheImpl) MCacheFactory.getAnyInstance()).getResourceManager().getHeapMonitor();
    hmm.setTestMaxMemoryBytes(100);
    hmm.updateStateAndSendEvent(90);
  }

  protected static String toString(final Result result) {
    assert result != null : "The Result object from the command execution cannot be null!";

    final StringBuilder buffer = new StringBuilder(System.getProperty("line.separator"));

    while (result.hasNextLine()) {
      buffer.append(result.nextLine());
      buffer.append(System.getProperty("line.separator"));
    }

    return buffer.toString().trim();
  }

  protected void deleteTable(final VM vm, final String tableName) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        MClientCache cache = MClientCacheFactory.getAnyInstance();
        cache.getAdmin().deleteTable(tableName);
      }
    });
  }

  protected void createMTable(final VM vm, final String tableName) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {

        MClientCache cache = MClientCacheFactory.getAnyInstance();
        MTableDescriptor tableDescriptor = new MTableDescriptor();

        for (int admin = 0; admin < 10; ++admin) {
          tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes("COLUMN" + admin));
        }

        tableDescriptor.setRedundantCopies(1);
        cache.getAdmin().createTable(tableName, tableDescriptor);
      }
    });
  }

  protected void createFTable(final VM vm, final String tableName) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {

        MClientCache cache = MClientCacheFactory.getAnyInstance();
        FTableDescriptor tableDescriptor = new FTableDescriptor();

        for (int admin = 0; admin < 10; ++admin) {
          tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes("COLUMN" + admin));
        }

        tableDescriptor.setRedundantCopies(1);
        cache.getAdmin().createFTable(tableName, tableDescriptor);
      }
    });
  }


  protected void createPersistentRegion(final VM vm, final String regionName,
      final String diskStoreName) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        final MCache cache = MCacheFactory.getAnyInstance();

        DiskStore diskStore = cache.findDiskStore(diskStoreName);
        final RegionFactory regionFactory = cache.createRegionFactory();
        regionFactory.create(regionName);
      }
    });
  }

  @Test
  public void testListMTable() throws Exception {
    final CommandResult commandResult = executeCommand(MashCliStrings.LIST_MTABLE);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertTrue(commandResult.toString().contains("\"TestMTablePeer2\",\"TestMTablePeer1\""));

  }

  @Test
  public void testHintMTable() throws Exception {
    CommandResult commandResult = executeCommand("hint");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    GfJsonArray message;
    message = commandResult.getResultData().getGfJsonObject().getJSONObject("content")
        .getJSONArray("message");
    boolean found = false;
    for (int index = 0; index < message.size(); index++) {
      if (((String) message.get(index)).toLowerCase().contains("MTable".toLowerCase())) {
        found = true;
        break;
      }
    }
    assertTrue("MTable hint was not found", found);

    commandResult = executeCommand("hint Ampool MTable");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    GfJsonObject result = commandResult.getResultData().getGfJsonObject().getJSONObject("content")
        .getJSONObject("__sections__-Commands And Help");
    assertEquals(9, result.size()); // Because we have 8 commands under MTable hint as of now
  }

  @Test
  public void testDescribeMTablewithValidName() throws Exception {
    final Result commandResult =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=TestMTablePeer1");

    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
  }

  /**
   * Testing only primary buckets
   *
   * @throws Exception
   */
  @Test
  public void testShowDistribution() throws Exception {
    String tableName = getTestMethodName();
    int NUM_BUCKETS = 5;
    int keysPerPart = 10;

    Result commandResult = executeCommand(
        MashCliStrings.CREATE_MTABLE + " --name=" + tableName + " --type=ORDERED_VERSIONED "
            + "--columns=ID,NAME,AGE --ncopies=1 --nbuckets=" + NUM_BUCKETS);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    MTable mTable = mClientCache.getMTable(tableName);
    MTableDescriptor tableDescriptor = mTable.getTableDescriptor();

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(tableDescriptor.getTotalNumOfSplits(), keysPerPart);

    Put put = new Put("DUMMY");

    final AtomicInteger index = new AtomicInteger(0);
    keysForAllBuckets.forEach((k, v) -> {
      v.forEach(key -> {
        put.setRowKey(key);
        put.addColumn("ID", Bytes.toBytes(index.get()));
        put.addColumn("NAME", Bytes.toBytes("NAME_" + index.get()));
        put.addColumn("AGE", Bytes.toBytes(index.getAndIncrement()));
        mTable.put(put);
      });
    });

    assertEquals((tableDescriptor.getTotalNumOfSplits() * keysPerPart), mTable.getKeyCount());

    commandResult = executeCommand(MashCliStrings.SHOW__DISTRIBUTION + " --name=" + tableName);
    //
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    // actual value comparision
    GfJsonObject contentObj =
        ((CommandResult) commandResult).getResultData().getGfJsonObject().getJSONObject("content");

    Iterator<String> sectionNames = contentObj.keys();

    while (sectionNames.hasNext()) {
      String sectionName = sectionNames.next();
      GfJsonObject section = contentObj.getJSONObject(sectionName);
      if (sectionName.contains("misc")) {
        // misc data section will have table name
        assertEquals(tableName, (String) section.get("TableName"));
      }
      if (sectionName.contains("primary")) {
        GfJsonArray jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID);

        assertEquals(NUM_BUCKETS, jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertTrue((Integer) jsonArray.get(i) >= 0);
          assertTrue((Integer) jsonArray.get(i) < NUM_BUCKETS);
        }

        jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT);

        assertEquals(NUM_BUCKETS, jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertEquals(keysPerPart, jsonArray.get(i));
        }
      }
    }
  }

  /**
   * Testing both primary and secondary buckets
   *
   * @throws Exception
   */
  @Test
  public void testShowDistributionSecondary() throws Exception {
    String tableName = getTestMethodName();
    int NUM_BUCKETS = 5;
    int keysPerPart = 10;
    int ncopies = 1;
    Result commandResult = executeCommand(
        MashCliStrings.CREATE_MTABLE + " --name=" + tableName + " --type=ORDERED_VERSIONED "
            + "--columns=ID,NAME,AGE --ncopies=" + ncopies + " --nbuckets=" + NUM_BUCKETS);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    MTable mTable = mClientCache.getMTable(tableName);
    MTableDescriptor tableDescriptor = mTable.getTableDescriptor();

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(tableDescriptor.getTotalNumOfSplits(), keysPerPart);

    Put put = new Put("DUMMY");

    final AtomicInteger index = new AtomicInteger(0);
    keysForAllBuckets.forEach((k, v) -> {
      v.forEach(key -> {
        put.setRowKey(key);
        put.addColumn("ID", Bytes.toBytes(index.get()));
        put.addColumn("NAME", Bytes.toBytes("NAME_" + index.get()));
        put.addColumn("AGE", Bytes.toBytes(index.getAndIncrement()));
        mTable.put(put);
      });
    });

    assertEquals((tableDescriptor.getTotalNumOfSplits() * keysPerPart), mTable.getKeyCount());

    commandResult = executeCommand(MashCliStrings.SHOW__DISTRIBUTION + " --name=" + tableName
        + " --include-secondary-buckets=true");
    //
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    // actual value comparision
    GfJsonObject contentObj =
        ((CommandResult) commandResult).getResultData().getGfJsonObject().getJSONObject("content");

    Iterator<String> sectionNames = contentObj.keys();

    while (sectionNames.hasNext()) {
      String sectionName = sectionNames.next();
      GfJsonObject section = contentObj.getJSONObject(sectionName);
      if (sectionName.contains("misc")) {
        // misc data section will have table name
        assertEquals(tableName, (String) section.get("TableName"));
      }
      if (sectionName.contains("primary")) {
        GfJsonArray jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID);

        assertEquals(NUM_BUCKETS, jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertTrue((Integer) jsonArray.get(i) >= 0);
          assertTrue((Integer) jsonArray.get(i) < NUM_BUCKETS);
        }

        jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT);

        assertEquals(NUM_BUCKETS, jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertEquals(keysPerPart, jsonArray.get(i));
        }
      }

      if (sectionName.contains("secondary")) {
        GfJsonArray jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID);

        assertEquals(NUM_BUCKETS * (ncopies), jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertTrue((Integer) jsonArray.get(i) >= 0);
          assertTrue((Integer) jsonArray.get(i) < NUM_BUCKETS);
        }

        jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT);

        assertEquals(NUM_BUCKETS * (ncopies), jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertEquals(keysPerPart, jsonArray.get(i));
        }
      }
    }
  }

  /**
   * Testing only primary buckets
   *
   * @throws Exception
   */
  @Test
  public void testShowDistributionUnorderedTable() throws Exception {
    String tableName = getTestMethodName();
    int NUM_BUCKETS = 5;
    int keysPerPart = 10;

    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE + " --name=" + tableName
        + " --type=UNORDERED " + "--columns=ID,NAME,AGE --ncopies=1 --nbuckets=" + NUM_BUCKETS);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    MTable mTable = mClientCache.getMTable(tableName);
    MTableDescriptor tableDescriptor = mTable.getTableDescriptor();

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(tableDescriptor.getTotalNumOfSplits(), keysPerPart);

    Put put = new Put("DUMMY");

    final AtomicInteger index = new AtomicInteger(0);
    keysForAllBuckets.forEach((k, v) -> {
      v.forEach(key -> {
        put.setRowKey(key);
        put.addColumn("ID", Bytes.toBytes(index.get()));
        put.addColumn("NAME", Bytes.toBytes("NAME_" + index.get()));
        put.addColumn("AGE", Bytes.toBytes(index.getAndIncrement()));
        mTable.put(put);
      });
    });

    assertEquals((tableDescriptor.getTotalNumOfSplits() * keysPerPart), mTable.getKeyCount());

    commandResult = executeCommand(MashCliStrings.SHOW__DISTRIBUTION + " --name=" + tableName);
    //
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    // actual value comparision
    GfJsonObject contentObj =
        ((CommandResult) commandResult).getResultData().getGfJsonObject().getJSONObject("content");

    Iterator<String> sectionNames = contentObj.keys();

    while (sectionNames.hasNext()) {
      String sectionName = sectionNames.next();
      GfJsonObject section = contentObj.getJSONObject(sectionName);
      if (sectionName.contains("misc")) {
        // misc data section will have table name
        assertEquals(tableName, (String) section.get("TableName"));
      }
      if (sectionName.contains("primary")) {
        GfJsonArray jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID);

        assertEquals(NUM_BUCKETS, jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertTrue((Integer) jsonArray.get(i) >= 0);
          assertTrue((Integer) jsonArray.get(i) < NUM_BUCKETS);
        }

        jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT);

        assertEquals(NUM_BUCKETS, jsonArray.size());
      }
    }
  }

  /**
   * Testing both primary and secondary buckets
   *
   * @throws Exception
   */
  @Test
  public void testShowDistributionSecondaryUnorderedTable() throws Exception {
    String tableName = getTestMethodName();
    int NUM_BUCKETS = 5;
    int keysPerPart = 10;
    int ncopies = 1;
    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE + " --name=" + tableName
        + " --type=UNORDERED " + "--columns=ID,NAME,AGE --ncopies=1 --nbuckets=" + NUM_BUCKETS);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    MTable mTable = mClientCache.getMTable(tableName);
    MTableDescriptor tableDescriptor = mTable.getTableDescriptor();

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(tableDescriptor.getTotalNumOfSplits(), keysPerPart);

    Put put = new Put("DUMMY");

    final AtomicInteger index = new AtomicInteger(0);
    keysForAllBuckets.forEach((k, v) -> {
      v.forEach(key -> {
        put.setRowKey(key);
        put.addColumn("ID", Bytes.toBytes(index.get()));
        put.addColumn("NAME", Bytes.toBytes("NAME_" + index.get()));
        put.addColumn("AGE", Bytes.toBytes(index.getAndIncrement()));
        mTable.put(put);
      });
    });

    assertEquals((tableDescriptor.getTotalNumOfSplits() * keysPerPart), mTable.getKeyCount());

    commandResult = executeCommand(MashCliStrings.SHOW__DISTRIBUTION + " --name=" + tableName
        + " --include-secondary-buckets=true");
    //
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    // actual value comparision
    GfJsonObject contentObj =
        ((CommandResult) commandResult).getResultData().getGfJsonObject().getJSONObject("content");

    Iterator<String> sectionNames = contentObj.keys();

    while (sectionNames.hasNext()) {
      String sectionName = sectionNames.next();
      GfJsonObject section = contentObj.getJSONObject(sectionName);
      if (sectionName.contains("misc")) {
        // misc data section will have table name
        assertEquals(tableName, (String) section.get("TableName"));
      }
      if (sectionName.contains("primary")) {
        GfJsonArray jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID);

        assertEquals(NUM_BUCKETS, jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertTrue((Integer) jsonArray.get(i) >= 0);
          assertTrue((Integer) jsonArray.get(i) < NUM_BUCKETS);
        }

        jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT);

        assertEquals(NUM_BUCKETS, jsonArray.size());
      }

      if (sectionName.contains("secondary")) {
        GfJsonArray jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID);

        assertEquals(NUM_BUCKETS * (ncopies), jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertTrue((Integer) jsonArray.get(i) >= 0);
          assertTrue((Integer) jsonArray.get(i) < NUM_BUCKETS);
        }

        jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT);

        assertEquals(NUM_BUCKETS * (ncopies), jsonArray.size());
      }
    }
  }

  /**
   * Testing only primary buckets
   *
   * @throws Exception
   */
  @Test
  public void testShowDistributionImmutableTable() throws Exception {
    String tableName = getTestMethodName();
    int NUM_BUCKETS = 5;
    int keysPerPart = 10;

    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE + " --name=" + tableName
        + " --type=IMMUTABLE " + "--columns=ID,NAME,AGE --ncopies=1 --nbuckets=" + NUM_BUCKETS);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    FTable mTable = mClientCache.getFTable(tableName);
    FTableDescriptor tableDescriptor = mTable.getTableDescriptor();

    for (int i = 0; i < NUM_BUCKETS * keysPerPart * tableDescriptor.getBlockSize(); i++) {
      Record record = new Record();
      record.add("ID", Bytes.toBytes(i));
      record.add("NAME", Bytes.toBytes("NAME_" + i));
      record.add("AGE", Bytes.toBytes(i));
      mTable.append(record);
    }

    commandResult = executeCommand(MashCliStrings.SHOW__DISTRIBUTION + " --name=" + tableName);
    //
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    // actual value comparision
    GfJsonObject contentObj =
        ((CommandResult) commandResult).getResultData().getGfJsonObject().getJSONObject("content");

    Iterator<String> sectionNames = contentObj.keys();

    while (sectionNames.hasNext()) {
      String sectionName = sectionNames.next();
      GfJsonObject section = contentObj.getJSONObject(sectionName);
      if (sectionName.contains("misc")) {
        // misc data section will have table name
        assertEquals(tableName, (String) section.get("TableName"));
      }
      if (sectionName.contains("primary")) {
        GfJsonArray jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID);

        assertEquals(NUM_BUCKETS, jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertTrue((Integer) jsonArray.get(i) >= 0);
          assertTrue((Integer) jsonArray.get(i) < NUM_BUCKETS);
        }

        jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT);

        assertEquals(NUM_BUCKETS, jsonArray.size());
      }
    }
  }

  /**
   * Testing both primary and secondary buckets
   *
   * @throws Exception
   */
  @Test
  public void testShowDistributionSecondaryImmutableTable() throws Exception {
    String tableName = getTestMethodName();
    int NUM_BUCKETS = 5;
    int keysPerPart = 10;
    int ncopies = 1;
    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE + " --name=" + tableName
        + " --type=IMMUTABLE " + "--columns=ID,NAME,AGE --ncopies=1 --nbuckets=" + NUM_BUCKETS);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    FTable mTable = mClientCache.getFTable(tableName);
    FTableDescriptor tableDescriptor = mTable.getTableDescriptor();

    for (int i = 0; i < NUM_BUCKETS * keysPerPart * tableDescriptor.getBlockSize(); i++) {
      Record record = new Record();
      record.add("ID", Bytes.toBytes(i));
      record.add("NAME", Bytes.toBytes("NAME_" + i));
      record.add("AGE", Bytes.toBytes(i));
      mTable.append(record);
    }

    commandResult = executeCommand(MashCliStrings.SHOW__DISTRIBUTION + " --name=" + tableName
        + " --include-secondary-buckets=true");
    //
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    // actual value comparision
    GfJsonObject contentObj =
        ((CommandResult) commandResult).getResultData().getGfJsonObject().getJSONObject("content");

    Iterator<String> sectionNames = contentObj.keys();

    while (sectionNames.hasNext()) {
      String sectionName = sectionNames.next();
      GfJsonObject section = contentObj.getJSONObject(sectionName);
      if (sectionName.contains("misc")) {
        // misc data section will have table name
        assertEquals(tableName, (String) section.get("TableName"));
      }
      if (sectionName.contains("primary")) {
        GfJsonArray jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID);

        assertEquals(NUM_BUCKETS, jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertTrue((Integer) jsonArray.get(i) >= 0);
          assertTrue((Integer) jsonArray.get(i) < NUM_BUCKETS);
        }

        jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT);

        assertEquals(NUM_BUCKETS, jsonArray.size());
      }

      if (sectionName.contains("secondary")) {
        GfJsonArray jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_BUCKET_ID);

        assertEquals(NUM_BUCKETS * (ncopies), jsonArray.size());

        for (int i = 0; i < jsonArray.size(); i++) {
          assertTrue((Integer) jsonArray.get(i) >= 0);
          assertTrue((Integer) jsonArray.get(i) < NUM_BUCKETS);
        }

        jsonArray = section.getJSONObject("__tables__-0").getJSONObject("content")
            .getJSONArray(MashCliStrings.SHOW__DISTRIBUTION__COL_NAME_KEY_COUNT);

        assertEquals(NUM_BUCKETS * (ncopies), jsonArray.size());
      }
    }
  }

  @Test
  public void testDescribeMTablewithInvalidName() throws Exception {
    final Result commandResult =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=NonExistingTable");

    assertNotNull(commandResult);
    assertEquals(Result.Status.ERROR, commandResult.getStatus());
    assertEquals(MashCliStrings.format(MashCliStrings.MTABLE_NOT_FOUND, "NonExistingTable"),
        toString(commandResult));
  }

  @Test
  public void testDeleteExistingMTable() {

    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));
    System.out.println("MTableCommandsDUnitTest.testCreateMTable :: " + commandResult);

    commandResult = executeCommand(MashCliStrings.DELETE_MTABLE + " --name=MTable1");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    System.out.println("MTableCommandsDUnitTest.testCreateMTable :: " + commandResult);


  }

  // @Test
  public void testDeleteExistingFTable() {

    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=FTable2 --type=IMMUTABLE --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "FTable1"),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.DELETE_MTABLE + " --name=FTable2");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

  }

  @Test
  public void testDeleteNonExistingMTable() {
    final Result commandResult =
        executeCommand(MashCliStrings.DELETE_MTABLE + " --name=/NonExistingTable");

    assertNotNull(commandResult);
    assertEquals(Result.Status.ERROR, commandResult.getStatus());
    assertEquals("Table \"/NonExistingTable\" not found", toString(commandResult));
  }

  // @Test
  public void testDeleteNonExistingFTable() {
    final Result commandResult =
        executeCommand(MashCliStrings.DELETE_MTABLE + " --name=/NonExistingTable");

    assertNotNull(commandResult);
    assertEquals(Result.Status.ERROR, commandResult.getStatus());
    assertEquals("Memory Table \"/NonExistingTable\" not found", toString(commandResult));
  }

  @Test
  public void testCreateMTable() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));
    System.out.println("MTableCommandsDUnitTest.testCreateMTable :: " + commandResult);
  }


  protected void verifyMTableAttributes(final VM vm, final String tableName) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        MClientCache clientCache = MClientCacheFactory.getAnyInstance();
        MTable mtable1 = clientCache.getTable(tableName);
        assertEquals(MEvictionPolicy.OVERFLOW_TO_DISK,
            mtable1.getTableDescriptor().getEvictionPolicy());
        assertEquals(100, mtable1.getTableDescriptor().getExpirationAttributes().getTimeout());
        assertEquals(MExpirationAction.DESTROY,
            mtable1.getTableDescriptor().getExpirationAttributes().getAction());
      }
    });
  }

  protected void verifyMTableDescribe(CommandResult result, final String tableName, String columns,
      int ncopies, int versions, boolean diskPersistence, int nsplits, TableType tableType,
      final MDiskWritePolicy writePolicy, final MEvictionPolicy evictionPolicy,
      final MExpirationAttributes expirationAttributes, Long recoveryDelay,
      Long startupRecoveryDelay) throws GfJsonException {

    final GfJsonArray resultKeyName = result.getResultData().getGfJsonObject()
        .getJSONObject("content").getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getJSONArray("Name");
    final GfJsonArray resultValue = result.getResultData().getGfJsonObject()
        .getJSONObject("content").getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getJSONArray("Value");

    final String[] colNames = columns.split(",");
    List columnsList = new ArrayList<String>();
    for (int j = 0; j < colNames.length; j++) {
      columnsList.add(colNames[j].trim());
    }

    for (int i = 0; i < resultKeyName.size(); i++) {
      final String key = (String) resultKeyName.get(i);
      final String value = (String) resultValue.get(i);

      if (key.equalsIgnoreCase("entry-time-to-live.timeout")) {
        System.out.println("DESCRIBE TTL = " + value.toLowerCase());
        assertEquals(String.valueOf(expirationAttributes.getTimeout()).toLowerCase(),
            value.toLowerCase());
      }

      if (key.equalsIgnoreCase("entry-time-to-live.action")) {
        System.out.println("DESCRIBE TTL.action = " + value.toLowerCase());
        assertEquals(expirationAttributes.getAction().getActionName().toLowerCase(),
            value.toLowerCase());
      }

      if (key.equalsIgnoreCase("eviction-action")) {
        System.out.println("DESCRIBE TTL.action = " + value.toLowerCase());
        assertEquals(evictionPolicy.name().toLowerCase().replace("_", "-"), value.toLowerCase());
      }

      if (key.equalsIgnoreCase("recovery-delay")) {
        System.out.println("DESCRIBE recovery delay = " + value.toLowerCase());
        assertEquals(recoveryDelay, Long.valueOf(value));
      }

      if (key.equalsIgnoreCase("startup-recovery-delay")) {
        System.out.println("DESCRIBE startup recovery delay = " + value.toLowerCase());
        assertEquals(startupRecoveryDelay, Long.valueOf(value));
      }

      if (key.equalsIgnoreCase("total-num-buckets")) {
        if (nsplits != -1) {
          assertEquals(String.valueOf(nsplits).toLowerCase(), value.toLowerCase());
        } else {
          assertEquals(String.valueOf(DEFAULT_SPLITS).toLowerCase(), value.toLowerCase());
        }
      }
    }
  }

  /**
   * Tests MTable creation with --eviction-policy=OVERFLOW_TO_DISK --expiration-timeout=20
   * --expiration-action=DESTROY from mash, and verify that the table is created and attributes are
   * set.
   */
  @Test
  public void testCreateMTable_2() throws Exception {
    final Result commandResult_ord = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable_Ordered --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --nsplits=67 --versions=4  --eviction-policy=OVERFLOW_TO_DISK --expiration-timeout=100 --expiration-action=DESTROY");
    assertNotNull(commandResult_ord);
    assertEquals(Result.Status.OK, commandResult_ord.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable_Ordered"),
        toString(commandResult_ord));

    final Result commandResult_uord = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable_UnorderedVersioned --type=UNORDERED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --nsplits=67 --versions=4 "
        + " --eviction-policy=OVERFLOW_TO_DISK --expiration-timeout=100 --expiration-action=DESTROY");
    assertNotNull(commandResult_uord);
    assertEquals(Result.Status.OK, commandResult_uord.getStatus());
    assertEquals(
        CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable_UnorderedVersioned"),
        toString(commandResult_uord));

    final Result commandResult_unord = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable_Unordered --type=UNORDERED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --nsplits=67 --eviction-policy=OVERFLOW_TO_DISK --expiration-timeout=100 --expiration-action=DESTROY");
    assertNotNull(commandResult_unord);
    assertEquals(Result.Status.OK, commandResult_unord.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable_Unordered"),
        toString(commandResult_unord));

    final CommandResult describeTableCmd_ord =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=" + "MTable_Ordered");
    assertNotNull(describeTableCmd_ord);
    verifyMTableDescribe(describeTableCmd_ord, "MTable_Ordered", "ID,NAME,AGE,SEX,DESIGNATION", 2,
        4, true, 67, TableType.ORDERED_VERSIONED, null, MEvictionPolicy.OVERFLOW_TO_DISK,
        new MExpirationAttributes(100, MExpirationAction.DESTROY), null, null);

    final CommandResult describeTableCmd_unord =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=" + "MTable_Unordered");
    assertNotNull(describeTableCmd_ord);
    verifyMTableDescribe(describeTableCmd_ord, "MTable_Ordered", "ID,NAME,AGE,SEX,DESIGNATION", 2,
        4, true, 67, TableType.UNORDERED, null, MEvictionPolicy.OVERFLOW_TO_DISK,
        new MExpirationAttributes(100, MExpirationAction.DESTROY), null, null);

    // Verify table attributes, Get a table using Java APIs and verify the tableDescriptor
    // attributes
    verifyMTableAttributes(client1, "MTable_Ordered");
    verifyMTableAttributes(client1, "MTable_Unordered");

  }

  @Test
  public void testCreateMTableWithRecoveryDelay() throws GfJsonException {
    final Result commandResult_ord = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable_Ordered --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,GEN,DESIGNATION --ncopies=1 --recovery-delay=21");
    assertNotNull(commandResult_ord);
    assertEquals(Result.Status.OK, commandResult_ord.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable_Ordered"),
        toString(commandResult_ord));
    final CommandResult commandResult_desc =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=MTable_Ordered ");
    verifyMTableDescribe(commandResult_desc, "MTable_Ordered", "ID,NAME,AGE,SEX,DESIGNATION", 2, 4,
        true, 67, TableType.ORDERED_VERSIONED, null, MEvictionPolicy.OVERFLOW_TO_DISK,
        new MExpirationAttributes(100, MExpirationAction.DESTROY), Long.valueOf(21), null);
  }

  @Test
  public void testCreateMTableWithStartupRecoveryDelay() throws GfJsonException {
    final Result commandResult_ord = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable_Ordered --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,GEN,DESIGNATION --ncopies=1 --startup-recovery-delay=32");
    assertNotNull(commandResult_ord);
    assertEquals(Result.Status.OK, commandResult_ord.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable_Ordered"),
        toString(commandResult_ord));
    final CommandResult commandResult_desc =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=MTable_Ordered ");
    verifyMTableDescribe(commandResult_desc, "MTable_Ordered", "ID,NAME,AGE,SEX,DESIGNATION", 2, 4,
        true, 67, TableType.ORDERED_VERSIONED, null, MEvictionPolicy.OVERFLOW_TO_DISK,
        new MExpirationAttributes(100, MExpirationAction.DESTROY), null, Long.valueOf(32));
  }

  @Test
  public void testCreateFTableWithRecoveryDelay() throws GfJsonException {
    final Result commandResult_ord = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=testFTable --type=IMMUTABLE --columns=ID,NAME,AGE,GEN,DESIGNATION --ncopies=1 --recovery-delay=21");
    assertNotNull(commandResult_ord);
    assertEquals(Result.Status.OK, commandResult_ord.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "testFTable"),
        toString(commandResult_ord));
    final CommandResult commandResult_desc =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=testFTable ");
    verifyMTableDescribe(commandResult_desc, "testFTable", "ID,NAME,AGE,SEX,DESIGNATION", 2, 4,
        true, 67, TableType.IMMUTABLE, null, MEvictionPolicy.OVERFLOW_TO_TIER,
        new MExpirationAttributes(100, MExpirationAction.DESTROY), Long.valueOf(21), null);
  }

  @Test
  public void testCreateFTableWithStartupRecoveryDelay() throws GfJsonException {
    final Result commandResult_ord = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=testFTable2 --type=IMMUTABLE --columns=ID,NAME,AGE,GEN,DESIGNATION --ncopies=1 --startup-recovery-delay=32");
    assertNotNull(commandResult_ord);
    assertEquals(Result.Status.OK, commandResult_ord.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "testFTable2"),
        toString(commandResult_ord));
    final CommandResult commandResult_desc =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=testFTable2");
    verifyMTableDescribe(commandResult_desc, "testFTable2", "ID,NAME,AGE,SEX,DESIGNATION", 2, 4,
        true, 67, TableType.IMMUTABLE, null, MEvictionPolicy.OVERFLOW_TO_TIER,
        new MExpirationAttributes(100, MExpirationAction.DESTROY), null, Long.valueOf(32));
  }

  @Test
  public void testCreateFTableWithBlockFormat_1() throws GfJsonException {
    final String name = "testFTable2";
    final Result result = executeCommand(MashCliStrings.CREATE_MTABLE + " --name=" + name
        + " --type=IMMUTABLE --columns=ID,NAME,AGE,GEN,DESIGNATION");
    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + name), toString(result));
    assertDescribeTable(name, new String[] {"block-format"}, new String[] {"AMP_BYTES"});
  }

  @Test
  public void testCreateFTableWithBlockFormat_2() throws GfJsonException {
    final String name = "testFTable2";
    final Result result = executeCommand(MashCliStrings.CREATE_MTABLE + " --name=" + name
        + " --type=IMMUTABLE --columns=ID,NAME,AGE,GEN,DESIGNATION --block-format=ORC_BYTES");
    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + name), toString(result));
    assertDescribeTable(name, new String[] {"block-format"}, new String[] {"ORC_BYTES"});
  }

  @Test
  public void testCreateFTableWithBlockFormat_3() throws GfJsonException {
    final String name = "testFTable2";
    final Result result = executeCommand(MashCliStrings.CREATE_MTABLE + " --name=" + name
        + " --type=IMMUTABLE --columns=ID,NAME,AGE,GEN,DESIGNATION --block-format=AMP_SNAPPY");
    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + name), toString(result));
    assertDescribeTable(name, new String[] {"block-format"}, new String[] {"AMP_SNAPPY"});
  }

  private void assertDescribeTable(final String name, final String[] pNames,
      final String[] expectedValues) throws GfJsonException {
    final CommandResult res = executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=" + name);
    assertNotNull(res);
    assertEquals(Result.Status.OK, res.getStatus());

    final GfJsonObject content = res.getContent().getJSONObject("__sections__-0")
        .getJSONObject("__sections__-0").getJSONObject("__tables__-0").getJSONObject("content");
    final GfJsonArray names = content.getJSONArray("Name");
    final GfJsonArray values = content.getJSONArray("Value");
    final Map<Object, Object> map = new HashMap<>(names.size());
    for (int i = 0; i < names.size(); i++) {
      map.put(names.get(i), values.get(i));
    }
    for (int i = 0; i < pNames.length; i++) {
      assertEquals("Incorrect value for property: " + pNames[i], expectedValues[i],
          map.get(pNames[i]));
    }
  }

  /**
   * A simple test for creation of table with schema.
   */
  @Test
  public void testCreateWithSchema() throws Exception {
    final String schema = " --" + MashCliStrings.CREATE_MTABLE_SCHEMA
        + "=\"{'c1':'INT', 'c2':'map<STRING,array<DOUBLE>>', 'c3':'union<BYTE,BIG_DECIMAL,LONG>'}\"";
    final String tableName = "table_with_schema";
    final String cmd = MashCliStrings.CREATE_MTABLE + " --type=ORDERED_VERSIONED --versions=4 ";
    final Result commandResult = executeCommand(cmd + " --name=" + tableName + schema);

    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + tableName),
        toString(commandResult));
  }

  /**
   * Helper method to verify data using GET.
   */
  private void assertGetData(final String table, final int id, final String n0, final String v0,
      final String n1, final String v1) throws GfJsonException {
    Result result = executeCommand(MashCliStrings.MGET + " --table=/" + table + " --key=key_" + id);
    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    /** assert on the values **/
    GfJsonObject map;
    map = ((CommandResult) result).getContent().getJSONObject("__sections__-0");
    assertEquals("Incorrect key.", "key_" + id, map.getString("KEY"));
    assertEquals("Incorrect value for: " + n0, v0, map.getString(n0));
    assertEquals("Incorrect value for: " + n1, v1, map.getString(n1));
  }

  /**
   * Simple test for PUT using JSON and then verify the data using GET.
   */
  @Test
  public void testPutWithSchema() throws GfJsonException {
    final String[][] COLUMNS = {{"ID", "LONG"}, {"MY_MAP", "map<STRING,array<DOUBLE>>"}};
    final Object[] VALUES = {"{'ID': '11', 'MY_MAP': {'ABC' : ['11.111', '11.222']}}",
        "{'ID': '22', 'MY_MAP': {'PQR' : ['22.111', '22.222']}}",};
    final String schema = " --" + MashCliStrings.CREATE_MTABLE_SCHEMA + "=\""
        + TypeUtils.getJsonSchema(COLUMNS) + "\"";
    final String tableName = "table_with_schema";
    final String cmd = MashCliStrings.CREATE_MTABLE + " --type=ORDERED_VERSIONED --versions=1 ";

    Result result;
    result = executeCommand(cmd + " --name=" + tableName + schema);

    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    String putCmd = MashCliStrings.MPUT + " --table=/" + tableName;
    result = executeCommand(putCmd + " --key=key_0" + " --value-json=\"" + VALUES[0] + "\"");
    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    result = executeCommand(putCmd + " --key=key_1" + " --value-json=\"" + VALUES[1] + "\"");
    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    assertGetData(tableName, 0, "ID", "11", "MY_MAP", "{ABC=[11.111, 11.222]}");
    assertGetData(tableName, 1, "ID", "22", "MY_MAP", "{PQR=[22.111, 22.222]}");
  }

  @Test
  public void testNonExistingMTable() throws Exception {
    final Result commandResultMPut =
        executeCommand(MashCliStrings.MPUT + " --key=1 --value=ID=1 --table=/MTable1");
    assertNotNull(commandResultMPut);
    assertEquals(Status.ERROR, commandResultMPut.getStatus());

    final Result commandResultMGet =
        executeCommand(MashCliStrings.MGET + " --key=1 --table=/MTable1");
    assertNotNull(commandResultMGet);
    assertEquals(Status.ERROR, commandResultMGet.getStatus());

    final Result commandResultMScan = executeCommand(MashCliStrings.MSCAN + " --table=/MTable1");
    assertNotNull(commandResultMScan);
    assertEquals(Status.ERROR, commandResultMScan.getStatus());

  }

  @Test
  public void testCreateMTableAllCombinations() throws Exception {

    /**
     * Currently MTable supports
     *
     * Following options 1) name 2) type 3) columns -- only above three are shown when we do tab
     * with out entering anything 4) ncopies 5) versions 6) disk-persistence 7) disk-write-policy 8)
     * disk-store
     *
     *
     */
    int i = 0;

    // Default create table
    String cmd1 = MashCliStrings.CREATE_MTABLE + " --name=mtable1";
    CommandResult res = executeCommand(cmd1);
    // expected to get null as not other mandatory options are provided
    assertNull(res);

    cmd1 = MashCliStrings.CREATE_MTABLE + " --name=mtable1 --type=ORDERED_VERSIONED";
    res = executeCommand(cmd1);
    /** as --columns and --schema-json combination is mandatory -- expect error **/
    assertNotNull(res);
    assertEquals(Status.ERROR, res.getStatus());

    GfJsonArray message = null;

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable11 --type=ORDERED_VERSIONED --columns=ID,NAME";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable11", "ID,NAME", -1, false, -1,
        TableType.ORDERED_VERSIONED, null);
    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable2 --type=ORDERED_VERSIONED --columns=ID,NAME --ncopies=2";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable2", "ID,NAME", -1, false, 2,
        TableType.ORDERED_VERSIONED, null);

    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable3 --type=ORDERED_VERSIONED --columns=ID,NAME --versions=3";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable3", "ID,NAME", 3, false, -1,
        TableType.ORDERED_VERSIONED, null);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable4 --type=ORDERED_VERSIONED --columns=ID,NAME --versions=3 --disk-persistence=true";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable4", "ID,NAME", 3, true, -1,
        TableType.ORDERED_VERSIONED, null);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable5 --type=ORDERED_VERSIONED --columns=ID,NAME --versions=3 --disk-persistence=true --disk-write-policy=ASYNCHRONOUS";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable5", "ID,NAME", 3, true, -1,
        TableType.ORDERED_VERSIONED, MDiskWritePolicy.ASYNCHRONOUS);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable6 --type=ORDERED_VERSIONED --columns=ID,NAME --versions=3 --disk-persistence=true --disk-write-policy=SYNCHRONOUS";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable6", "ID,NAME", 3, true, -1,
        TableType.ORDERED_VERSIONED, MDiskWritePolicy.SYNCHRONOUS);

    // -- Unordered --
    cmd1 =
        MashCliStrings.CREATE_MTABLE + " --name=mtable8 --type=ORDERED_VERSIONED --columns=ID,NAME";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable8", "ID,NAME", -1, false, -1,
        TableType.ORDERED_VERSIONED, null);
    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable9 --type=ORDERED_VERSIONED --columns=ID,NAME --ncopies=2";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable9", "ID,NAME", -1, false, 2,
        TableType.ORDERED_VERSIONED, null);

    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable10 --type=ORDERED_VERSIONED --columns=ID,NAME --versions=3";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable10", "ID,NAME", 3, false, -1,
        TableType.ORDERED_VERSIONED, null);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable11 --type=ORDERED_VERSIONED --columns=ID,NAME --versions=3 --disk-persistence=true";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable11", "ID,NAME", 3, true, -1,
        TableType.ORDERED_VERSIONED, null);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable12 --type=ORDERED_VERSIONED --columns=ID,NAME --versions=3 --disk-persistence=true --disk-write-policy=ASYNCHRONOUS";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable12", "ID,NAME", 3, true, -1,
        TableType.ORDERED_VERSIONED, MDiskWritePolicy.ASYNCHRONOUS);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable13 --type=ORDERED_VERSIONED --columns=ID,NAME --versions=3 --disk-persistence=true --disk-write-policy=SYNCHRONOUS";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable13", "ID,NAME", 3, true, -1,
        TableType.ORDERED_VERSIONED, MDiskWritePolicy.SYNCHRONOUS);

    // UNORDERED

    cmd1 = MashCliStrings.CREATE_MTABLE + " --name=mtable14 --type=UNORDERED";
    res = executeCommand(cmd1);
    /** as --columns and --schema-json combination is mandatory -- expect error **/
    assertNotNull(res);
    assertEquals(Status.ERROR, res.getStatus());

    cmd1 = MashCliStrings.CREATE_MTABLE + " --name=mtable14 --type=UNORDERED --columns=ID,NAME";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable14", "ID,NAME", -1, false, -1,
        TableType.UNORDERED, null);
    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable15 --type=UNORDERED --columns=ID,NAME --ncopies=2";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable15", "ID,NAME", -1, false, 2,
        TableType.UNORDERED, null);

    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE + " --name=mtable16 --type=UNORDERED --columns=ID,NAME";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable16", "ID,NAME", 3, false, -1,
        TableType.UNORDERED, null);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable17 --type=UNORDERED --columns=ID,NAME --disk-persistence=true";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable17", "ID,NAME", 3, true, -1,
        TableType.UNORDERED, null);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable18 --type=UNORDERED --columns=ID,NAME --disk-persistence=true --disk-write-policy=ASYNCHRONOUS";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable18", "ID,NAME", 3, true, -1,
        TableType.UNORDERED, MDiskWritePolicy.ASYNCHRONOUS);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable19 --type=UNORDERED --columns=ID,NAME --disk-persistence=true --disk-write-policy=SYNCHRONOUS";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable19", "ID,NAME", 3, true, -1,
        TableType.UNORDERED, MDiskWritePolicy.SYNCHRONOUS);

    // -- Unordered --
    cmd1 = MashCliStrings.CREATE_MTABLE + " --name=mtable20 --type=UNORDERED --columns=ID,NAME";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable20", "ID,NAME", -1, false, -1,
        TableType.UNORDERED, null);
    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable21 --type=UNORDERED --columns=ID,NAME --ncopies=2";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable21", "ID,NAME", -1, false, 2,
        TableType.UNORDERED, null);

    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE + " --name=mtable22 --type=UNORDERED --columns=ID,NAME ";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable22", "ID,NAME", 3, false, -1,
        TableType.UNORDERED, null);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable23 --type=UNORDERED --columns=ID,NAME --disk-persistence=true";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable23", "ID,NAME", 3, true, -1,
        TableType.UNORDERED, null);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable24 --type=UNORDERED --columns=ID,NAME --disk-persistence=true --disk-write-policy=ASYNCHRONOUS";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable24", "ID,NAME", 3, true, -1,
        TableType.UNORDERED, MDiskWritePolicy.ASYNCHRONOUS);

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=mtable25 --type=UNORDERED --columns=ID,NAME --disk-persistence=true --disk-write-policy=SYNCHRONOUS";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    verifyCreatedTableandDoPutGetDescribeListMtables(res, "mtable25", "ID,NAME", 3, true, -1,
        TableType.UNORDERED, MDiskWritePolicy.SYNCHRONOUS);

    // cmd1 = MashCliStrings.CREATE_MTABLE + " --name=mtable1 --type=ORDERED_VERSIONED
    // --columns=ID,NAME";
    // res = executeCommand(cmd1);
    // // expected to get null as not other mandatory options are provided
    // assertNull(res);

    // final Result commandResult = executeCommand(
    // MashCliStrings.CREATE_MTABLE + " --name=MTable1 --type=ORDERED_VERSIONED
    // --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    // assertNotNull(commandResult);
    // assertEquals(Result.Status.OK, commandResult.getStatus());
    // assertEquals(CliStrings.format( MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
    // toString(commandResult));

  }

  private void verifyCreatedTableandDoPutGetDescribeListMtables(CommandResult result,
      final String tableName, String columns, int versions, boolean diskPersistence, int ncopies,
      TableType tableType, final MDiskWritePolicy writePolicy) throws GfJsonException {
    String matchingMessage = "Successfully created the table:";
    boolean found = false;
    GfJsonArray message;
    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());
    message =
        result.getResultData().getGfJsonObject().getJSONObject("content").getJSONArray("message");
    found = false;
    for (int index = 0; index < message.size(); index++) {
      if (((String) message.get(index)).toLowerCase().contains(matchingMessage.toLowerCase())) {
        found = true;
        break;
      }
    }
    assertTrue("Table create message not found.", found);

    // execute list table command and verify
    String cmd1 = MashCliStrings.LIST_MTABLE;
    result = executeCommand(cmd1);

    message =
        result.getResultData().getGfJsonObject().getJSONObject("content").getJSONArray("Name");

    found = false;
    for (int index = 0; index < message.size(); index++) {
      System.out
          .println("MTableCommandsDUnitTest.verifyCreatedTableandDoPutGetDescribeListMtables :: "
              + "----> " + ((String) message.get(index)));
      if (((String) message.get(index)).toLowerCase().contains(tableName.toLowerCase())) {
        found = true;
        break;
      }
    }
    assertTrue("Table create message not found.", found);

    // perform put get describe table
    cmd1 = MashCliStrings.DESCRIBE_MTABLE + " --name=" + tableName;
    result = executeCommand(cmd1);
    final GfJsonArray resultKeyName = result.getResultData().getGfJsonObject()
        .getJSONObject("content").getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getJSONArray("Name");
    final GfJsonArray resultValue = result.getResultData().getGfJsonObject()
        .getJSONObject("content").getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getJSONArray("Value");

    final String[] colNames = columns.split(",");
    List columnsList = new ArrayList<String>();
    for (int j = 0; j < colNames.length; j++) {
      columnsList.add(colNames[j].trim());
    }

    for (int i = 0; i < resultKeyName.size(); i++) {
      final String key = (String) resultKeyName.get(i);
      final String value = (String) resultValue.get(i);

      if (key.equalsIgnoreCase("Max Versions") && (tableType == TableType.ORDERED_VERSIONED)) {
        // as we dont support versioning unordered versions
        if (versions != -1) {
          assertEquals(String.valueOf(versions).toLowerCase(), value.toLowerCase());
        } else {
          assertEquals(String.valueOf(DEFAULT_VERSIONS).toLowerCase(), value.toLowerCase());
        }
      }

      if (key.equalsIgnoreCase("Redundant Configuration")) {
        if (ncopies != -1) {
          assertEquals(value.toLowerCase(), String.valueOf(ncopies).toLowerCase());
        } else {
          assertEquals(value.toLowerCase(), String.valueOf(DEFAULT_REDUNDENCY).toLowerCase());
        }
      }

      if (key.equalsIgnoreCase("Persistence Enabled")) {
        assertEquals(Boolean.valueOf(value.toLowerCase()), Boolean.valueOf(diskPersistence));
      }

      if (key.equalsIgnoreCase("Table Type")) {
        assertEquals(0, TableType.valueOf(value).compareTo(tableType));
      }

      if (key.equalsIgnoreCase("Disk Write Policy")) {
        if (writePolicy != null) {
          assertEquals(0, MDiskWritePolicy.valueOf(value).compareTo(writePolicy));
        } else {
          assertEquals(0, MDiskWritePolicy.valueOf(value).compareTo(MDiskWritePolicy.ASYNCHRONOUS));
        }
      }

      if (key.trim().startsWith("Column")) {
        if (!columnsList.contains(value.split(":")[0].trim())) {
          fail("Failed to find given column");
        }
      }
    }

    // do put
    String key = "key1";
    cmd1 = MashCliStrings.MPUT + " --table=/" + tableName + " --key=\"" + key + "\"";
    // add values
    String valueStr = "";
    for (int k = 0; k < columnsList.size(); k++) {
      final String colName = (String) columnsList.get(k);
      valueStr = valueStr + colName + "=VALUE_" + colName;
      if (k < columnsList.size() - 1) {
        valueStr = valueStr + ",";
      }
    }
    cmd1 = cmd1 + " --value=\"" + valueStr + "\"";
    result = executeCommand(cmd1);
    assertNotNull(result);
    assertEquals(Status.OK, result.getStatus());
    //
    cmd1 = MashCliStrings.MGET + " --table=/" + tableName + " --key=\"" + key + "\"";
    result = executeCommand(cmd1);
    assertNotNull(result);
    assertEquals(Status.OK, result.getStatus());

    cmd1 = MashCliStrings.DELETE_MTABLE + " --name=/" + tableName;
    result = executeCommand(cmd1);
    // assertNotNull(result);
    // assertEquals(Status.OK, result.getStatus());
  }

  private void verifyCreatedTableAndDelete(CommandResult result, final String tableName,
      String columns, boolean diskPersistence, int ncopies, final MDiskWritePolicy writePolicy)
      throws GfJsonException {
    String matchingMessage = "Successfully created the memory table:";
    boolean found = false;
    GfJsonArray message;
    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());
    message =
        result.getResultData().getGfJsonObject().getJSONObject("content").getJSONArray("message");
    found = false;
    for (int index = 0; index < message.size(); index++) {
      if (((String) message.get(index)).toLowerCase().contains(matchingMessage.toLowerCase())) {
        found = true;
        break;
      }
    }
    assertTrue("Table create message not found.", found);

    // execute list table command and verify
    String cmd1 = MashCliStrings.LIST_MTABLE;
    result = executeCommand(cmd1);

    message =
        result.getResultData().getGfJsonObject().getJSONObject("content").getJSONArray("Name");

    found = false;
    for (int index = 0; index < message.size(); index++) {
      System.out
          .println("MTableCommandsDUnitTest.verifyCreatedTableandDoPutGetDescribeListMtables :: "
              + "----> " + ((String) message.get(index)));
      if (((String) message.get(index)).toLowerCase().contains(tableName.toLowerCase())) {
        found = true;
        break;
      }
    }
    assertTrue("Table create message not found.", found);

    // perform put get describe table
    cmd1 = MashCliStrings.DESCRIBE_MTABLE + " --name=" + tableName;
    result = executeCommand(cmd1);
    final GfJsonArray resultKeyName = result.getResultData().getGfJsonObject()
        .getJSONObject("content").getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getJSONArray("Name");
    final GfJsonArray resultValue = result.getResultData().getGfJsonObject()
        .getJSONObject("content").getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getJSONArray("Value");

    final String[] colNames = columns.split(",");
    List columnsList = new ArrayList<String>();
    for (int j = 0; j < colNames.length; j++) {
      columnsList.add(colNames[j].trim());
    }

    for (int i = 0; i < resultKeyName.size(); i++) {
      final String key = (String) resultKeyName.get(i);
      final String value = (String) resultValue.get(i);

      if (key.equalsIgnoreCase("Redundant Configuration")) {
        if (ncopies != -1) {
          assertEquals(value.toLowerCase(), String.valueOf(ncopies).toLowerCase());
        } else {
          assertEquals(value.toLowerCase(), String.valueOf(DEFAULT_REDUNDENCY).toLowerCase());
        }
      }

      if (key.equalsIgnoreCase("Persistence Enabled")) {
        assertEquals(Boolean.valueOf(value.toLowerCase()), Boolean.valueOf(diskPersistence));
      }

      if (key.equalsIgnoreCase("Disk Write Policy")) {
        if (writePolicy != null) {
          assertEquals(0, MDiskWritePolicy.valueOf(value).compareTo(writePolicy));
        } else {
          assertEquals(0, MDiskWritePolicy.valueOf(value).compareTo(MDiskWritePolicy.ASYNCHRONOUS));
        }
      }

      if (key.trim().startsWith("Column")) {
        if (!columnsList.contains(value.split(":")[0].trim())) {
          fail("Failed to find given column");
        }
      }
    }

    cmd1 = MashCliStrings.DELETE_MTABLE + " --name=/" + tableName;
    result = executeCommand(cmd1);
    // assertNotNull(result);
    // assertEquals(Status.OK, result.getStatus());
  }

  @Test
  public void testCreateMTableWithPersistence() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME --disk-persistence=true");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable2 --type=ORDERED_VERSIONED --columns=ID,NAME --disk-persistence=false");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable2"),
        toString(commandResult));
    commandResult = executeCommand(
        MashCliStrings.CREATE_MTABLE + " --name=MTable3 --type=ORDERED_VERSIONED --columns=ID,NAME "
            + "--disk-persistence=true --disk-write-policy=ASYNCHRONOUS");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable3"),
        toString(commandResult));
    commandResult = executeCommand(
        MashCliStrings.CREATE_MTABLE + " --name=MTable4 --type=ORDERED_VERSIONED --columns=ID,NAME "
            + "--disk-persistence=true --disk-write-policy=SYNCHRONOUS");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable4"),
        toString(commandResult));

    // Create a disk store on VM0, VM1 VMs
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);

    for (final VM vm : (new VM[] {vm0, vm1, vm2})) {
      vm.invoke(new SerializableRunnable() {
        public void run() {
          MCache cache = MCacheFactory.getAnyInstance();
          File diskStoreDirFile = new File("diskstore" + vm.getPid());
          diskStoreDirFile.mkdirs();
          DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
          diskStoreFactory.setDiskDirs(new File[] {diskStoreDirFile});
          diskStoreFactory.create("ds1");
        }
      });
    }

    // commandResult = executeCommand("create disk-store --name=ds1 --dir=/tmp");
    // assertNotNull(commandResult);
    // assertEquals(Result.Status.OK, commandResult.getStatus());
    commandResult = executeCommand(
        MashCliStrings.CREATE_MTABLE + " --name=MTable5 --type=ORDERED_VERSIONED --columns=ID,NAME "
            + "--disk-persistence=true --disk-store=ds1");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable5"),
        toString(commandResult));
    commandResult = executeCommand(
        MashCliStrings.CREATE_MTABLE + " --name=MTable6 --type=ORDERED_VERSIONED --columns=ID,NAME "
            + "--disk-persistence=true --disk-write-policy=SYNCHRONOUS --disk-store=ds1");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable6"),
        toString(commandResult));
    commandResult = executeCommand(
        MashCliStrings.CREATE_MTABLE + " --name=MTable7 --type=ORDERED_VERSIONED --columns=ID,NAME "
            + "--disk-persistence=true --disk-write-policy=ASYNCHRONOUS --disk-store=ds1");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable7"),
        toString(commandResult));
  }


  @Test
  public void testMputMGetValidIntKeyMTable() throws Exception {
    final Result commandResult =
        executeCommand(MashCliStrings.CREATE_MTABLE + " --name=MTable1 --type=ORDERED_VERSIONED "
            + "--columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    final Result commandResultMPut = executeCommand(MashCliStrings.MPUT
        + " --key=1 --value=ID=1,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");

    assertNotNull(commandResultMPut);
    assertEquals(Result.Status.OK, commandResultMPut.getStatus());

    final Result commandResultMGet =
        executeCommand(MashCliStrings.MGET + " --key=1 --table=/MTable1");
    assertNotNull(commandResultMGet);
    assertEquals(Result.Status.OK, commandResultMGet.getStatus());

  }

  @Test
  public void testCreateMTableWithLocalMaxMemory() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED "
        + "--columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4 --local-max-memory=300");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));
  }

  @Test
  public void testCreateFTableWithLocalMaxMemory() throws Exception {
    Result commandResult =
        executeCommand(MashCliStrings.CREATE_MTABLE + " --name=FTable1 --type=IMMUTABLE "
            + "--columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --local-max-memory=300");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "FTable1"),
        toString(commandResult));
  }

  @Test
  public void testCreateMTableWithLocalMaxMemoryPct() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED "
        + "--columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4 --local-max-memory-pct=30");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));
  }

  @Test
  public void testCreateFTableWithLocalMaxMemoryPct() throws Exception {
    Result commandResult =
        executeCommand(MashCliStrings.CREATE_MTABLE + " --name=FTable1 --type=IMMUTABLE "
            + "--columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --local-max-memory-pct=30");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "FTable1"),
        toString(commandResult));
  }

  @Test
  public void testCreateMTableWithLocalMaxMemoryPctInvalidValues() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED "
        + "--columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4 --local-max-memory-pct=0");
    assertNotNull(commandResult);
    assertEquals(Status.ERROR, commandResult.getStatus());
    assertEquals(CliStrings.format("Invalid value for local-max-memory-pct: \"0\""),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED "
        + "--columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4 --local-max-memory-pct=91");
    assertNotNull(commandResult);
    assertEquals(Status.ERROR, commandResult.getStatus());
    assertEquals(CliStrings.format("Invalid value for local-max-memory-pct: \"91\""),
        toString(commandResult));

  }

  @Test
  public void testCreateFTableWithLocalMaxMemoryPctInvalidValues() throws Exception {
    Result commandResult =
        executeCommand(MashCliStrings.CREATE_MTABLE + " --name=FTable1 --type=IMMUTABLE "
            + "--columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --local-max-memory-pct=0");
    assertNotNull(commandResult);
    assertEquals(Status.ERROR, commandResult.getStatus());
    assertEquals(CliStrings.format("Invalid value for local-max-memory-pct: \"0\""),
        toString(commandResult));

    commandResult =
        executeCommand(MashCliStrings.CREATE_MTABLE + " --name=FTable1 --type=IMMUTABLE "
            + "--columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --local-max-memory-pct=91");
    assertNotNull(commandResult);
    assertEquals(Status.ERROR, commandResult.getStatus());
    assertEquals(CliStrings.format("Invalid value for local-max-memory-pct: \"91\""),
        toString(commandResult));
  }

  @Test
  public void testMputMGetInValidIntKeyMTable() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));
    final Result commandResultMGet =
        executeCommand(MashCliStrings.MGET + " --key=1 --table=/MTable1");
    assertNotNull(commandResultMGet);

    assertEquals(Result.Status.ERROR, commandResultMGet.getStatus());

  }

  @Test
  public void testMScanMTable() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    final Result commandResultMPut1 = executeCommand(MashCliStrings.MPUT
        + " --key=1 --value=ID=1,NAME=SAtish,AGE=26,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut1);
    assertEquals(Result.Status.OK, commandResultMPut1.getStatus());

    final Result commandResultMPut2 = executeCommand(MashCliStrings.MPUT
        + " --key=2 --value=ID=2,NAME=Prasad,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut2);

    assertEquals(Result.Status.OK, commandResultMPut2.getStatus());
    final Result commandResultMPut3 = executeCommand(MashCliStrings.MPUT
        + " --key=3 --value=ID=3,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut3);
    assertEquals(Result.Status.OK, commandResultMPut3.getStatus());
    final Result commandResultMGet4 =
        executeCommand(MashCliStrings.MSCAN + " --table=/MTable1 --startkey=1 --stopkey=3");
    System.out.println("commandResultMGet4" + commandResultMGet4);
    assertNotNull(commandResultMGet4);
    /** with key-type **/
    final Result commandResultMGet5 = executeCommand(
        MashCliStrings.MSCAN + " --table=/MTable1 --startkey=49 --stopkey=51 --key-type=BINARY");
    System.out.println("commandResultMGet5" + commandResultMGet4);
    assertNotNull(commandResultMGet5);
  }

  @Test
  public void testMScan() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    for (int i = 0; i < 19; i++) {
      Result commandResultMPut1 = executeCommand(MashCliStrings.MPUT + " --key=" + i
          + " --value=ID=" + i + ",NAME=Satish,AGE=26,SEX=M,DESIGNATION=D --table=/MTable1");
      assertNotNull(commandResultMPut1);
      assertEquals(Result.Status.OK, commandResultMPut1.getStatus());
    }
    Result commandResultMScan = executeCommand(MashCliStrings.MSCAN + " --table=/MTable1");
    System.out.println("commandResultMScan" + commandResultMScan);
    assertNotNull(commandResultMScan);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    commandResultMScan = executeCommand(MashCliStrings.MSCAN + " --table=/MTable1 --max-limit=10");
    System.out.println("commandResultMScan" + commandResultMScan);
    assertNotNull(commandResultMScan);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    Thread.sleep(5000);

    String csvFile = "/tmp/testMScan.csv";
    File file = new File(csvFile);

    if (file.delete()) {
      System.out.println(file.getName() + " is deleted!");
    } else {
      System.out.println("Delete operation is failed.");
    }

    commandResultMScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/MTable1" + " --file=" + csvFile);
    System.out.println("commandResultMScan" + commandResultMScan);
    assertNotNull(commandResultMScan);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    BufferedReader reader = new BufferedReader(new FileReader(csvFile));

    String[] arr = new String[] {"ID", "NAME", "AGE", "SEX", "DESIGNATION"};
    int count = 0;
    String line = "";
    while ((line = reader.readLine()) != null) {
      System.out.println("MTableCommandsDUnitTest.testMScan " + line);
      String[] array = line.trim().split(",");
      count++;

      if (count == 1) {
        for (int i = 0; i < 5; i++) {
          assertEquals(arr[i] + ":BINARY", array[i + 1]);
        }
      }
    }
    assertEquals(20, count);

    csvFile = "/tmp/testMScan.csv";
    file = new File(csvFile);

    if (file.delete()) {
      System.out.println(file.getName() + " is deleted!");
    } else {
      System.out.println("Delete operation is failed.");
    }

    commandResultMScan = executeCommand(
        MashCliStrings.MSCAN + " --table=/MTable1" + " --file=" + csvFile + "--max-limit=10");
    System.out.println("commandResultMScan" + commandResultMScan);
    assertNotNull(commandResultMScan);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    Thread.sleep(5000);

    reader = new BufferedReader(new FileReader(csvFile));

    count = 0;
    line = "";
    while ((line = reader.readLine()) != null) {
      System.out.println("MTableCommandsDUnitTest.testMScan " + line);
      String[] array = line.trim().split(",");
      count++;

      if (count == 1) {
        for (int i = 0; i < 5; i++) {
          assertEquals(arr[i] + ":BINARY", array[i + 1]);
        }
      }
    }
    assertEquals(11, count);
  }

  @Test
  public void testServerSideScanner() throws IOException {
    final Result commandResult = executeCommand(
        MashCliStrings.CREATE_MTABLE + " --name=MTable2 --type=ORDERED_VERSIONED --columns=ID");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable2"),
        toString(commandResult));

    for (int i = 0; i < 1000; i++) {
      Result commandResultMPut1 = executeCommand(
          MashCliStrings.MPUT + " --key=" + i + " --value=ID=" + i + " --table=/MTable2");
      assertNotNull(commandResultMPut1);
      assertEquals(Result.Status.OK, commandResultMPut1.getStatus());
    }

    String csvFile = "/tmp/testMScanUnordered.csv";
    File file = new File(csvFile);

    if (file.delete()) {
      System.out.println(file.getName() + " is deleted!");
    } else {
      System.out.println("Delete operation is failed.");
    }

    Result commandResultMScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/MTable2" + " --file=" + csvFile);
    System.out.println("commandResultMScan" + commandResultMScan);
    assertNotNull(commandResultMScan);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    BufferedReader reader = new BufferedReader(new FileReader(csvFile));

    String[] arr = new String[] {"ID"};
    int count = 0;
    String line = "";
    while ((line = reader.readLine()) != null) {
      count++;
    }
    assertEquals(1001, count);
  }


  @Test
  public void testMScanUnordered() throws Exception {
    final Result commandResult = executeCommand(
        MashCliStrings.CREATE_MTABLE + " --name=MTable2 --type=UNORDERED --columns=ID");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable2"),
        toString(commandResult));

    for (int i = 0; i < 19; i++) {
      Result commandResultMPut1 = executeCommand(
          MashCliStrings.MPUT + " --key=" + i + " --value=ID=" + i + " --table=/MTable2");
      assertNotNull(commandResultMPut1);
      assertEquals(Result.Status.OK, commandResultMPut1.getStatus());
    }
    Result commandResultMScan = executeCommand(MashCliStrings.MSCAN + " --table=/MTable2");
    System.out.println("commandResultMScan" + commandResultMScan);
    assertNotNull(commandResultMScan);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    System.out.println(commandResultMScan);

    commandResultMScan = executeCommand(MashCliStrings.MSCAN + " --table=/MTable2 --max-limit=10");
    System.out.println("commandResultMScan" + commandResultMScan);
    assertNotNull(commandResultMScan);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    System.out.println(commandResultMScan);

    for (int i = 19; i < 109; i++) {
      Result commandResultMPut1 = executeCommand(
          MashCliStrings.MPUT + " --key=" + i + " --value=ID=" + i + " --table=/MTable2");
      assertNotNull(commandResultMPut1);
      assertEquals(Result.Status.OK, commandResultMPut1.getStatus());
    }

    String csvFile = "/tmp/testMScanUnordered.csv";
    File file = new File(csvFile);

    if (file.delete()) {
      System.out.println(file.getName() + " is deleted!");
    } else {
      System.out.println("Delete operation is failed.");
    }

    commandResultMScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/MTable2" + " --file=" + csvFile);
    System.out.println("commandResultMScan" + commandResultMScan);
    assertNotNull(commandResultMScan);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    BufferedReader reader = new BufferedReader(new FileReader(csvFile));

    String[] arr = new String[] {"ID"};
    int count = 0;
    String line = "";
    while ((line = reader.readLine()) != null) {
      System.out.println("MTableCommandsDUnitTest.testMScanUnordered " + line);
      String[] array = line.trim().split(",");
      count++;

      if (count == 1) {
        for (int i = 0; i < 1; i++) {
          assertEquals(arr[i] + ":BINARY", array[i + 1]);
        }
      }
    }

    String locatorString = DUnitLauncher.getLocatorString();
    String substring = locatorString.substring(10, locatorString.length() - 1);
    System.out.println("MTableCommandsDUnitTest.testMScanUnordered locatorString = " + locatorString
        + " substring = " + substring);

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();

    List<String> columnNames = Arrays.asList("ID");
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    tableDescriptor.addColumn(Bytes.toBytes(columnNames.get(0)));

    String tableName = "MTable2";
    MTable table = clientCache.getMTable(tableName);

    if (table != null) {
      System.out.println("Table [MTable2] is created successfully!");
    }

    Scan scanner = new Scan();
    Scanner results = table.getScanner(scanner);
    Iterator itr = results.iterator();
    int count2 = 0;
    while (itr.hasNext()) {
      Object next = itr.next();
      count2++;
    }
    System.out.println(
        "MTableCommandsDUnitTest.testMScanUnordered count = " + count + " count2 = " + count2);
    assertEquals(109, count2);

    // [ ToDo: ] Open this assert when server side scanner implementation gets fixed
    // assertEquals(110, count);

    clientCache.close();
  }


  @Test
  public void testMScanMTableWithNullStartKeyandNullEndKey() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    final Result commandResultMPut1 = executeCommand(MashCliStrings.MPUT
        + " --key=1 --value=ID=1,NAME=SAtish,AGE=26,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut1);
    assertEquals(Result.Status.OK, commandResultMPut1.getStatus());

    final Result commandResultMPut2 = executeCommand(MashCliStrings.MPUT
        + " --key=2 --value=ID=2,NAME=Prasad,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut2);

    assertEquals(Result.Status.OK, commandResultMPut2.getStatus());
    final Result commandResultMPut3 = executeCommand(MashCliStrings.MPUT
        + " --key=3 --value=ID=3,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut3);
    assertEquals(Result.Status.OK, commandResultMPut3.getStatus());

    final Result commandResultMScan = executeCommand(MashCliStrings.MSCAN + " --table=/MTable1 ");
    System.out.println("commandResultMGet4" + commandResultMScan);
    assertNotNull(commandResultMScan);
    assertEquals(Result.Status.OK, commandResultMScan.getStatus());

    Thread.sleep(5000);

    // -----------------------------------------------------------------------------------------------------

    String csvFile = "/tmp/testMScanMTableWithNullStartKeyandNullEndKey.csv";
    File file = new File(csvFile);

    if (file.delete()) {
      System.out.println(file.getName() + " is deleted!");
    } else {
      System.out.println("Delete operation is failed.");
    }

    Result result =
        executeCommand(MashCliStrings.MSCAN + " --table=/MTable1" + " --file=" + csvFile);
    System.out.println("commandResultMScan" + result);
    assertNotNull(result);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    BufferedReader reader = new BufferedReader(new FileReader(csvFile));

    String[] arr = new String[] {"ID", "NAME", "AGE", "SEX", "DESIGNATION"};

    int count = 0;
    String line = "";
    while ((line = reader.readLine()) != null) {
      System.out.println("MTableCommandsDUnitTest.testMScan " + line);
      String[] array = line.trim().split(",");
      count++;

      if (count == 1) {
        for (int i = 0; i < 5; i++) {
          assertEquals(arr[i] + ":BINARY", array[i + 1]);
        }
      }
    }
    assertEquals(4, count);

    // -------------------------------------------------------------------------------------------------------
  }


  @Test
  public void testMScanMTableWithNullStartKeyandValidEndKey() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    final Result commandResultMPut1 = executeCommand(MashCliStrings.MPUT
        + " --key=1 --value=ID=1,NAME=SAtish,AGE=26,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut1);
    assertEquals(Result.Status.OK, commandResultMPut1.getStatus());

    final Result commandResultMPut2 = executeCommand(MashCliStrings.MPUT
        + " --key=2 --value=ID=2,NAME=Prasad,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut2);

    assertEquals(Result.Status.OK, commandResultMPut2.getStatus());
    final Result commandResultMPut3 = executeCommand(MashCliStrings.MPUT
        + " --key=3 --value=ID=3,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut3);
    assertEquals(Result.Status.OK, commandResultMPut3.getStatus());

    final Result commandResultMGet4 =
        executeCommand(MashCliStrings.MSCAN + " --table=/MTable1 --stopkey=2 --key-type=STRING");
    System.out.println("commandResultMGet4" + commandResultMGet4);
    assertNotNull(commandResultMGet4);
    assertEquals(Result.Status.OK, commandResultMGet4.getStatus());

    /** with key-type **/
    final Result commandResultMGet5 =
        executeCommand(MashCliStrings.MSCAN + " --table=/MTable1 --stopkey=50 --key-type=BINARY ");
    System.out.println("commandResultMGet4" + commandResultMGet5);
    assertNotNull(commandResultMGet5);
    assertEquals(Result.Status.OK, commandResultMGet5.getStatus());

  }


  @Test
  public void testMScanMTableWithValidStartKeyandNullValidEndKey() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    final Result commandResultMPut1 = executeCommand(MashCliStrings.MPUT
        + " --key=1 --value=ID=1,NAME=SAtish,AGE=26,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut1);
    assertEquals(Result.Status.OK, commandResultMPut1.getStatus());

    final Result commandResultMPut2 = executeCommand(MashCliStrings.MPUT
        + " --key=2 --value=ID=2,NAME=Prasad,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut2);

    assertEquals(Result.Status.OK, commandResultMPut2.getStatus());
    final Result commandResultMPut3 = executeCommand(MashCliStrings.MPUT
        + " --key=3 --value=ID=3,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut3);
    assertEquals(Result.Status.OK, commandResultMPut3.getStatus());

    final Result commandResultMGet4 =
        executeCommand(MashCliStrings.MSCAN + " --table=/MTable1 --startkey=3 ");
    System.out.println("commandResultMGet4" + commandResultMGet4);
    assertNotNull(commandResultMGet4);
    assertEquals(Result.Status.OK, commandResultMGet4.getStatus());


  }

  @Test
  public void testMScanMTableWithInValidStartKeyandInValidEndKey() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    final Result commandResultMPut1 = executeCommand(MashCliStrings.MPUT
        + " --key=1 --value=ID=1,NAME=SAtish,AGE=26,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut1);
    assertEquals(Result.Status.OK, commandResultMPut1.getStatus());

    final Result commandResultMPut2 = executeCommand(MashCliStrings.MPUT
        + " --key=2 --value=ID=2,NAME=Prasad,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut2);

    assertEquals(Result.Status.OK, commandResultMPut2.getStatus());
    final Result commandResultMPut3 = executeCommand(MashCliStrings.MPUT
        + " --key=3 --value=ID=3,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut3);
    assertEquals(Result.Status.OK, commandResultMPut3.getStatus());

    final Result commandResultMGet4 =
        executeCommand(MashCliStrings.MSCAN + " --table=/MTable1 --startkey=5 --stopkey=10");
    System.out.println("commandResultMGet4" + commandResultMGet4);
    assertNotNull(commandResultMGet4);
    assertEquals(Result.Status.OK, commandResultMGet4.getStatus());

  }


  @Test
  public void testMputMGetValidStringKeyMTable() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    final Result commandResultMPut = executeCommand(MashCliStrings.MPUT
        + " --key=Ampool --value=ID=1,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut);
    assertEquals(Result.Status.OK, commandResultMPut.getStatus());

    final Result commandResultMGet =
        executeCommand(MashCliStrings.MGET + " --key=Ampool --table=/MTable1");
    assertNotNull(commandResultMGet);
    assertEquals(Result.Status.OK, commandResultMGet.getStatus());

  }

  @Test
  public void testMputMGetInValidStringKeyMTable() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    final Result commandResultMGet =
        executeCommand(MashCliStrings.MGET + " --key=Ampool --table=/MTable1");
    assertNotNull(commandResultMGet);
    assertEquals(Result.Status.ERROR, commandResultMGet.getStatus());

  }

  @Test
  public void testMputMDeleteValidStringKeyMTable() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    final Result commandResultMPut = executeCommand(MashCliStrings.MPUT
        + " --key=Ampool --value=ID=1,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut);
    assertEquals(Result.Status.OK, commandResultMPut.getStatus());

    final Result commandResultMGet =
        executeCommand(MashCliStrings.MDELETE + " --key=Ampool --table=/MTable1");
    assertNotNull(commandResultMGet);
    assertEquals(Result.Status.OK, commandResultMGet.getStatus());

  }

  @Test
  public void testMputMDeleteInValidStringKeyMTable() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));

    final Result commandResultMPut = executeCommand(MashCliStrings.MPUT
        + " --key=Ampool --value=ID=1,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1");
    assertNotNull(commandResultMPut);
    assertEquals(Result.Status.OK, commandResultMPut.getStatus());

    final Result commandResultMGet =
        executeCommand(MashCliStrings.MDELETE + " --key=Monarch --table=/MTable1");
    assertNotNull(commandResultMGet);
    assertEquals(Status.ERROR, commandResultMGet.getStatus());

  }

  protected void createTable2(int noOfPartitions, TableType tableType) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (tableType == TableType.ORDERED_VERSIONED) {
      tableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    } else if (tableType == TableType.UNORDERED) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    }
    // tableDescriptor.addCoprocessor("io.ampool.monarch.table.coprocessor.ComputeAverageFunction");
    tableDescriptor.setTotalNumOfSplits(noOfPartitions);
    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  protected void createTableFrom(VM vm, int noOfPartitions, TableType tableType) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable2(noOfPartitions, tableType);
        return null;
      }
    });
  }

  protected Object doPuts(VM vm, List<Integer> versionlist, List<String> columnsToBeDeleted) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return doPuts(versionlist, columnsToBeDeleted);
      }
    });
  }

  @Test
  public void testCreateExistingMTable() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));
    System.out.println("MTableCommandsDUnitTest.testCreateMTable :: " + commandResult);

    commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Status.ERROR, commandResult.getStatus());
    assertEquals("Table \"MTable1\" Already Exists", toString(commandResult));
    System.out.println("MTableCommandsDUnitTest.testCreateMTable :: " + commandResult);

  }

  // Removing this test as no cache store is into the code
  // public void testCreateMTablewithInvalidStoreName() throws Exception {
  // final Result commandResult = executeCommand(
  // MashCliStrings.CREATE_MTABLE + " --name=MTable1 --type=ORDERED_VERSIONED
  // --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4
  // --cache-store=NonExistingStore");
  // assertNotNull(commandResult);
  // assertEquals(Result.Status.ERROR, commandResult.getStatus());
  // assertEquals(MashCliStrings.CREATE_MTABLE__STORE__ERROR,
  // toString(commandResult));
  // }


  @Test
  public void testCreateMTablewithncopiesAndversions() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals("Successfully created the table: MTable1", toString(commandResult));
  }

  @Test
  public void testCreateMTableUnorderedWithncopiesAndversions() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=UNORDERED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals("Successfully created the table: MTable1", toString(commandResult));
  }

  private Map<Integer, List<byte[]>> doPuts(List<Integer> versionlist, List<String> columns) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

    // List<byte[]> allKeys =
    // getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(),
    // NUM_OF_ROWS);
    Map<Integer, List<byte[]>> allKeys =
        getBucketToKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);
    int totalKeyCount = 0;
    for (Map.Entry<Integer, List<byte[]>> entry : allKeys.entrySet()) {
      totalKeyCount += entry.getValue().size();
    }
    System.out.println(
        "Generate keys buckets " + allKeys.keySet().size() + " key count  " + totalKeyCount);
    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), totalKeyCount);
    int totalPutKeys = 0;

    for (Map.Entry<Integer, List<byte[]>> entry : allKeys.entrySet()) {
      for (byte[] key : entry.getValue()) {
        Get get = new Get(key);
        Row result = table.get(get);
        assertTrue(result.isEmpty());
        Put record = new Put(key);
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }

        // Add multiple versions
        for (Integer version : versionlist) {
          record.setTimeStamp(version.intValue());
          table.put(record);
        }

        totalPutKeys++;
      }
    }

    assertEquals(table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS, totalKeyCount);
    return allKeys;
  }

  protected void doDeleteInCoprocessorExecution(VM vm, Map<Integer, List<byte[]>> bucketWiseKeys,
      List<Integer> versionlist, List<String> columnsToBeDeleted) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        _doDeleteInCoprocessorExecution(bucketWiseKeys, versionlist, columnsToBeDeleted);
        return null;
      }
    });
  }

  private void _doDeleteInCoprocessorExecution(Map<Integer, List<byte[]>> bucketWiseKeys,
      List<Integer> versionlist, List<String> columnsToBeDeleted) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mtable = clientCache.getTable(TABLE_NAME);

    {
      List<Object> arguments = new ArrayList<>();
      arguments.add(bucketWiseKeys);
      if (versionlist != null && versionlist.size() > 0) {
        arguments.add(versionlist);
      }

      if (columnsToBeDeleted != null && columnsToBeDeleted.size() > 0) {
        arguments.add(columnsToBeDeleted);
      }

      // Execution request : <Keys> <Versions> <Columns>

      MExecutionRequest request = new MExecutionRequest();
      request.setArguments(arguments);
      Map<Integer, List<Object>> result = mtable.coprocessorService(
          "io.ampool.monarch.table.coprocessor.ScanAfterDeleteCoProcessor", "scanAfterDelete", null,
          null, request);
      System.out.println("Coprocessor executed successfully..!");
    }
  }

  protected void deleteTable(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        _deleteTable();
        return null;
      }
    });
  }

  private void _deleteTable() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mtable = clientCache.getTable(TABLE_NAME);
    clientCache.getAdmin().deleteTable(TABLE_NAME);
  }

  private void scanFromMash(String tableName) {
    // Scan from mash
    CommandResult commandResultMScan =
        executeCommand(MashCliStrings.MSCAN + " --table=" + tableName);

    // validate the scan result.
    assertNotNull(commandResultMScan);
    System.out.println(commandResultMScan.toString());
    assertEquals(Result.Status.OK, commandResultMScan.getStatus());
  }

  /*
   * Bug:994: Mash scan throws exception in following scenario.
   * 
   * Step:1-> Delete a row from a co-processor Step:2-> Scan a table from Mash Exception: Table
   * <org.apache.geode.management.internal.cli.functions.CliFunctionResult cannot be cast to
   * java.util.List> not found in any of the members
   */
  @Test
  public void testBug964() throws Exception {
    int noOfBuckets = 5;
    List<Integer> versionlist = new ArrayList<Integer>(Arrays.asList(100, 500, 1000));
    List<String> columnsToBeDeleted =
        new ArrayList<>(Arrays.asList("COLUMN1", "COLUMN4", "COLUMN7"));
    {
      createTableFrom(client1, noOfBuckets, TableType.ORDERED_VERSIONED);
      Map<Integer, List<byte[]>> bucketWiseKeys =
          (Map<Integer, List<byte[]>>) doPuts(client1, versionlist, columnsToBeDeleted);

      // Do coprocessor-execution. This will run on all 5 buckets and delete 1 key from each bucket.
      // Test-1 : delete a entire row and do scan
      {
        System.out.println("Bug964: Executing testcase-1");
        doDeleteInCoprocessorExecution(client1, bucketWiseKeys, null, null);
        scanFromMash(TABLE_NAME);
      }

      // Test-2 : Delete a row of a specified version and do a scan
      {
        System.out.println("Bug964: Executing testcase-2");
        doDeleteInCoprocessorExecution(client1, bucketWiseKeys, versionlist, null);
        scanFromMash(TABLE_NAME);
      }

      // Test-3 : Delete a specified columns of a row with given timestamp. Specified cols values
      // are set null
      {
        System.out.println("Bug964: Executing testcase-3");
        doDeleteInCoprocessorExecution(client1, bucketWiseKeys, versionlist, columnsToBeDeleted);
        scanFromMash(TABLE_NAME);
      }

      deleteTable(client1);
    }
  }

  @Test
  public void testMputMDeleteNonExistingColumnMTable() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));
    final Result commandResultMPut100 = executeCommand(MashCliStrings.MPUT
        + " --key=Ampool --value=ID=1,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1 --timestamp=100");
    assertNotNull(commandResultMPut100);
    assertEquals(Result.Status.OK, commandResultMPut100.getStatus());
    final Result commandResultMPut101 = executeCommand(MashCliStrings.MPUT
        + " --key=Ampool --value=ID=1,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1 --timestamp=101");
    assertNotNull(commandResultMPut101);
    assertEquals(Result.Status.OK, commandResultMPut101.getStatus());
    final Result commandResultMDelete =
        executeCommand(MashCliStrings.MDELETE + " --key=Ampool --table=/MTable1 --column=SALARY");
    assertNotNull(commandResultMDelete);
    System.out.println("MTableCommandsDUnitTest.testMputPartialMDeleteMTable --> "
        + commandResultMDelete.toString());
    assertEquals(Result.Status.ERROR, commandResultMDelete.getStatus());
  }

  @Test
  public void testMputPartialMDeleteMTable() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=MTable1 --type=ORDERED_VERSIONED --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2 --versions=4");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "MTable1"),
        toString(commandResult));
    final Result commandResultMPut100 = executeCommand(MashCliStrings.MPUT
        + " --key=Ampool --value=ID=1,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1 --timestamp=100");
    assertNotNull(commandResultMPut100);
    scanFromMash("MTable1");
    assertEquals(Result.Status.OK, commandResultMPut100.getStatus());
    final Result commandResultMPut101 = executeCommand(MashCliStrings.MPUT
        + " --key=Ampool --value=ID=1,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1 --timestamp=101");
    assertNotNull(commandResultMPut101);
    assertEquals(Result.Status.OK, commandResultMPut101.getStatus());
    final Result commandResultMPut102 = executeCommand(MashCliStrings.MPUT
        + " --key=Ampool --value=ID=1,NAME=A,AGE=34,SEX=M,DESIGNATION=D --table=/MTable1 --timestamp=102");
    assertNotNull(commandResultMPut102);
    assertEquals(Result.Status.OK, commandResultMPut102.getStatus());
    final Result commandResultMDelete =
        executeCommand(MashCliStrings.MDELETE + " --key=Ampool --table=/MTable1 --timestamp=100");
    assertNotNull(commandResultMDelete);
    System.out.println("MTableCommandsDUnitTest.testMputPartialMDeleteMTable --> "
        + commandResultMDelete.toString());
    assertEquals(Result.Status.OK, commandResultMDelete.getStatus());
    final Result commandResultMGet =
        executeCommand(MashCliStrings.MGET + " --key=Ampool --table=/MTable1 ");
    assertNotNull(commandResultMGet);
    assertEquals(Status.OK, commandResultMGet.getStatus());
  }

}
