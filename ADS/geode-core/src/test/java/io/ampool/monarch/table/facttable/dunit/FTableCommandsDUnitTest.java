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

import com.jayway.awaitility.Awaitility;
import io.ampool.monarch.cli.MashCliStrings;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDUnitHelperForCLICommands;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import io.ampool.monarch.table.internal.TableType;
import io.ampool.monarch.types.TypeUtils;
import io.ampool.store.StoreHandler;
import io.ampool.tierstore.SampleTierStoreImpl;
import io.ampool.tierstore.SampleTierStoreReaderImpl;
import io.ampool.tierstore.SampleTierStoreWriterImpl;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreFactory;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.tierstore.internal.DefaultStore;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.CliCommandTestBase;
import org.apache.geode.management.internal.cli.commands.MTableCommands;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.json.JSONArray;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.ampool.monarch.cli.MashCliStrings.CREATE_MTABLE__DISK_PERSISTENCE;
import static io.ampool.monarch.cli.MashCliStrings.CREATE_MTABLE__DISK_WRITE_POLICY;
import static io.ampool.monarch.cli.MashCliStrings.CREATE_MTABLE__IMMUTABLE_TYPE_NOT;
import static io.ampool.monarch.cli.MashCliStrings.CREATE_MTABLE__MAX_VERSIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The FTableCommandsDUnitTest class is a test suite of functional tests cases testing the proper
 * functioning of the 'list mtable' 'describe mtable' and 'delete mtable' commands.
 * </p>
 *
 * @see CliCommandTestBase
 * @see MTableCommands
 * @since 7.0
 */
@Category(FTableTest.class)
public class FTableCommandsDUnitTest extends MTableDUnitHelperForCLICommands {

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
  private List<VM> allServers = null;

  Set<ExpectedException> exceptions = new HashSet<ExpectedException>();

  public FTableCommandsDUnitTest() {
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
    createFTableforScan(this.client1, "FTable1", MEvictionPolicy.NO_ACTION);
    createDefaultSetup(null);
    allServers = Arrays.asList(vm0, vm1, vm2);

    setupGemFireContainers();

  }

  @Override
  public void tearDown2() throws Exception {
    deleteTable(this.client1, "FTable1");
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

  protected void deleteTable(final VM vm, final String tableName) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        MClientCache cache = MClientCacheFactory.getAnyInstance();
        cache.getAdmin().deleteTable(tableName);
      }
    });
  }

  protected void setupGemFireContainers() throws Exception {
    createPersistentRegion(vm1, "consumers", "consumerData");
    createPersistentRegion(vm1, "observers", "observerData");
    createPersistentRegion(vm2, "producer", "producerData");
    createPersistentRegion(vm2, "producer-factory", "producerData");
  }



  public void forceEvictionTask(String ftable) {
    /* Force eviction task */
    forceEvictiononServer(vm1, ftable);
    forceEvictiononServer(vm2, ftable);

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
          Awaitility.await().with().pollInterval(1, TimeUnit.SECONDS).atMost(220, TimeUnit.SECONDS)
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


  private void createFTableforScan(VM vm, String fTable1, MEvictionPolicy overflowToTier) {

    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {


        FTableDescriptor ftd = new FTableDescriptor();
        ftd.setEvictionPolicy(overflowToTier);
        ftd.setRedundantCopies(0);
        ftd.setTotalNumOfSplits(2);;
        for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
          ftd = ftd.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
        }
        ftd.setPartitioningColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + 0));
        MClientCache cache = MClientCacheFactory.getAnyInstance();
        FTable table = cache.getAdmin().createFTable(fTable1, ftd);


        for (int i = 0; i < 4; i++) {
          Record record = new Record();
          for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
            record.add(COLUMN_NAME_PREFIX + colIndex, Bytes.toBytes(COLUMN_NAME_PREFIX + colIndex));
          }
          table.append(record);
        }

      }
    });

  }



  protected static String toString(final Result result) {
    assert result != null : "The Result object from the command execution cannot be null!";

    final StringBuilder buffer = new StringBuilder(System.getProperty("line.separator"));

    while (result.hasNextLine()) {
      buffer.append(result.nextLine());
      buffer.append(System.getProperty("line.separator"));
    }
    result.resetToFirstLine();
    return buffer.toString().trim();
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
    assertTrue(commandResult.toString().contains("\"FTable1\""));
  }

  @Test
  public void testScanFTable() throws Exception {
    CommandResult commandResultScan = executeCommand(MashCliStrings.MSCAN + " --table=/FTable1");
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.OK, commandResultScan.getStatus());

  }

  @Test
  public void testScanFTableCSV() throws Exception {

    String csvFile = "/tmp/testScanFTableCSV.csv";
    File file = new File(csvFile);

    if (file.delete()) {
      System.out.println(file.getName() + " is deleted!");
    } else {
      System.out.println("Delete operation is failed.");
    }

    createFTableforScan(this.client1, "testScanFTableCSV", MEvictionPolicy.NO_ACTION);
    Thread.sleep(5000);
    CommandResult commandResultScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/testScanFTableCSV" + " --file=" + csvFile);
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.OK, commandResultScan.getStatus());

    BufferedReader reader = new BufferedReader(new FileReader(csvFile));

    int count = 0;
    String line = "";
    while ((line = reader.readLine()) != null) {
      String substring = line.substring(1);
      String[] array = substring.trim().split(",");
      count++;

      if (count == 1) {
        for (int i = 0; i < NUM_OF_COLUMNS; i++) {
          assertEquals(COLUMN_NAME_PREFIX + i + ":BINARY", array[i]);
        }
      }
    }
    assertEquals(5, count);

    if (file.delete()) {
      System.out.println(file.getName() + " is deleted!");
    } else {
      System.out.println("Delete operation is failed.");
    }

    // Test with no .csv file extension
    csvFile = "/tm";
    commandResultScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/testScanFTableCSV" + " --file=" + csvFile);
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.ERROR, commandResultScan.getStatus());
    String s = commandResultScan.nextLine();
    assertEquals("Invalid file type, the file extension must be \".csv\"", s.trim());


    // Test with no non existent file path
    csvFile = "/uknown/abc.csv";
    commandResultScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/testScanFTableCSV" + " --file=" + csvFile);
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.ERROR, commandResultScan.getStatus());
    s = commandResultScan.nextLine();
    assertEquals("/uknown/abc.csv (No such file or directory)", s.trim());

    deleteTable(this.client1, "testScanFTableCSV");
  }

  @Test
  public void testScanFTableCSVWithMaxLimit() throws Exception {

    String csvFile = "/tmp/testScanFTableCSVWithMaxLimit.csv";
    File file = new File(csvFile);

    if (file.delete()) {
      System.out.println(file.getName() + " is deleted!");
    } else {
      System.out.println("Delete operation is failed.");
    }

    createFTableforScan(this.client1, "testScanFTableCSVWithMaxLimit", MEvictionPolicy.NO_ACTION);
    Thread.sleep(5000);
    CommandResult commandResultScan = executeCommand(MashCliStrings.MSCAN
        + " --table=/testScanFTableCSVWithMaxLimit --max-limit=2" + " --file=" + csvFile);
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.OK, commandResultScan.getStatus());

    BufferedReader reader = new BufferedReader(new FileReader(csvFile));

    int count = 0;
    String line = "";
    while ((line = reader.readLine()) != null) {
      String substring = line.substring(1);
      String[] array = substring.trim().split(",");
      count++;

      if (count == 1) {
        for (int i = 0; i < NUM_OF_COLUMNS; i++) {
          assertEquals(COLUMN_NAME_PREFIX + i + ":BINARY", array[i]);
        }
      }
    }
    assertEquals(3, count);

    commandResultScan = executeCommand(MashCliStrings.MSCAN
        + " --table=/testScanFTableCSVWithMaxLimit --max-limit=1" + " --file=" + csvFile);
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.ERROR, commandResultScan.getStatus());

    deleteTable(this.client1, "testScanFTableCSVWithMaxLimit");
  }

  @Test
  public void testScanFTableWithMaxLimit() throws Exception {
    CommandResult commandResultScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/FTable1 --max-limit=1");
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.OK, commandResultScan.getStatus());
  }

  @Test
  public void testDescribeFTableCountsinMemory() throws Exception {

    CommandResult commandResultDescribe =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=/FTable1");
    assertNotNull(commandResultDescribe);
    assertEquals(Result.Status.OK, commandResultDescribe.getStatus());
    Object o = commandResultDescribe.getResultData().getGfJsonObject().getJSONObject("content")
        .getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getInternalJsonObject()
        .getJSONArray("Value").get(2);
    assertEquals(o, "4");
  }

  @Test
  public void testDescribeFTableCountsPostEviction() throws Exception {

    createFTableforScan(this.client1, "FTable9", MEvictionPolicy.OVERFLOW_TO_TIER);
    CommandResult commandResultDescribe =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=/FTable9");
    assertNotNull(commandResultDescribe);
    assertEquals(Result.Status.OK, commandResultDescribe.getStatus());
    Object o = commandResultDescribe.getResultData().getGfJsonObject().getJSONObject("content")
        .getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getInternalJsonObject()
        .getJSONArray("Value").get(2);
    assertEquals(o, "4");

    forceEvictionTask("FTable9");

    CommandResult commandResultDescribePostEviction =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=/FTable9");
    assertNotNull(commandResultDescribePostEviction);
    assertEquals(Result.Status.OK, commandResultDescribePostEviction.getStatus());

    Object e = commandResultDescribePostEviction.getResultData().getGfJsonObject()
        .getJSONObject("content").getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getInternalJsonObject()
        .getJSONArray("Value").get(2);
    assertEquals(e, "4");
    assertEquals(o, e);
    deleteTable(this.client1, "FTable9");
  }


  @Test
  public void testScanFTableFromStorage() throws Exception {
    createFTableforScan(this.client1, "testScanFTableFromStorage",
        MEvictionPolicy.OVERFLOW_TO_TIER);
    CommandResult commandResultScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/testScanFTableFromStorage");
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.OK, commandResultScan.getStatus());
    forceEvictionTask("testScanFTableFromStorage");
    commandResultScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/testScanFTableFromStorage");
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.OK, commandResultScan.getStatus());
    deleteTable(this.client1, "testScanFTableFromStorage");
  }


  @Test
  public void testScanFTableWithMaxLimitFromStorage() throws Exception {

    createFTableforScan(this.client1, "FmaxTable", MEvictionPolicy.OVERFLOW_TO_TIER);
    CommandResult commandResultScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/FmaxTable --max-limit=1");
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.OK, commandResultScan.getStatus());
    forceEvictionTask("FmaxTable");
    commandResultScan = executeCommand(MashCliStrings.MSCAN + " --table=/FmaxTable --max-limit=1");
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.OK, commandResultScan.getStatus());
    deleteTable(this.client1, "FmaxTable");
  }

  @Test
  public void testDescribeFTablewithValidName() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=TestFTablePeer1 --type=IMMUTABLE --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2");
    commandResult = executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=TestFTablePeer1");

    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());


  }

  @Test
  public void testDescribeFTableTier() throws Exception {
    Result commandResult = executeCommand(
        MashCliStrings.CREATE_MTABLE + " --name=TierFTable --type=IMMUTABLE --columns=ID");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "TierFTable"),
        toString(commandResult));

    CommandResult descCommandResult =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=TierFTable");

    assertNotNull(descCommandResult);
    assertEquals(Result.Status.OK, descCommandResult.getStatus());

    Object o = descCommandResult.getResultData().getGfJsonObject().getJSONObject("content")
        .getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getInternalJsonObject()
        .getJSONArray("Value").get(15);

    assertEquals(o, "DefaultLocalORCStore");
  }


  @Test
  public void testDeleteExistingFTable() {

    Result commandResult = executeCommand(

        MashCliStrings.CREATE_MTABLE
            + " --name=FTable2 --type=IMMUTABLE --columns=ID,NAME,AGE,SEX,DESIGNATION --ncopies=2");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "FTable2"),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.DELETE_MTABLE + " --name=FTable2");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

  }

  @Test
  public void testDeleteNonExistingFTable() {
    final Result commandResult =
        executeCommand(MashCliStrings.DELETE_MTABLE + " --name=/NonExistingTable");

    assertNotNull(commandResult);
    assertEquals(Result.Status.ERROR, commandResult.getStatus());
    assertEquals("Table \"/NonExistingTable\" not found", toString(commandResult));
  }

  @Test
  public void testCreateFTable() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=testCreateFTable --type=IMMUTABLE --columns=ID,NAME,AGE,SEX,DESIGNATION");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "testCreateFTable"),
        toString(commandResult));
  }

  @Test
  public void testCreateFTableWithBlockSize() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=testCreateFTableWithBlockSize --type=IMMUTABLE --columns=ID,NAME,AGE,SEX,DESIGNATION --block-size=2000");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(
        CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "testCreateFTableWithBlockSize"),
        toString(commandResult));
  }

  /**
   * Helper method to verify data using GET.
   * 
   * @throws GfJsonException
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

  @Test
  public void testCreateFTableAllCombinations() throws Exception {

    /**
     * Currently MTable supports
     *
     * Following options 1) name 2) type 3) columns -- only above three are shown when we do tab
     * with out entering anything 4) ncopies 5) versions - expected to get exception 6)
     * disk-persistence - expected to be overriden to true 7) disk-write-policy - expected to get
     * exception 8) disk-store - expected to get exception
     *
     *
     */
    int i = 0;

    // Default create table
    String cmd1 = MashCliStrings.CREATE_MTABLE + " --name=ftable1";
    CommandResult res = executeCommand(cmd1);
    // expected to get null as not other mandatory options are provided
    assertNull(res);

    cmd1 = MashCliStrings.CREATE_MTABLE + " --name=ftable1 --type=IMMUTABLE";
    res = executeCommand(cmd1);
    /** as --columns and --schema-json combination is mandatory -- expect error **/
    assertNotNull(res);
    assertEquals(Result.Status.ERROR, res.getStatus());

    GfJsonArray message = null;


    cmd1 = MashCliStrings.CREATE_MTABLE + " --name=ftable11 --type=IMMUTABLE --columns=ID,NAME";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    // disk persistence is always going to be true
    // result name cols diskpersistence ncopies diskwritepolicy
    verifyCreatedTableAndDelete(res, "ftable11", "ID,NAME", true, -1, null, 1000);
    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=ftable_block_size --type=IMMUTABLE --columns=ID,NAME --block-size=5000";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    // disk persistence is always going to be true
    // result name cols diskpersistence ncopies diskwritepolicy
    verifyCreatedTableAndDelete(res, "ftable_block_size", "ID,NAME", true, -1, null, 5000);
    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=ftable11 --type=IMMUTABLE --columns=ID,NAME --ncopies=2";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    // disk persistence is always going to be true
    // result name cols diskpersistence ncopies diskwritepolicy
    verifyCreatedTableAndDelete(res, "ftable11", "ID,NAME", true, 2, null, 1000);
    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=ftable11 --type=IMMUTABLE --columns=ID,NAME --ncopies=2 --versions=3";
    // expecting error as versions are not supported
    res = executeCommand(cmd1);
    String recMessage = (String) ((JSONArray) res.getContent().get("message")).get(0);
    assertEquals(CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__MAX_VERSIONS),
        recMessage);
    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=ftable11 --type=IMMUTABLE --columns=ID,NAME --versions=3";
    // expecting error as versions are not supported
    res = executeCommand(cmd1);
    recMessage = (String) ((JSONArray) res.getContent().get("message")).get(0);
    assertEquals(CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__MAX_VERSIONS),
        recMessage);

    // --------------------------------------------------------------------------------
    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=ftable11 --type=IMMUTABLE --columns=ID,NAME --ncopies=2 --disk-persistence=true";
    res = executeCommand(cmd1);
    recMessage = (String) ((JSONArray) res.getContent().get("message")).get(0);
    assertEquals(
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__DISK_PERSISTENCE),
        recMessage);

    // ----------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=ftable11 --type=IMMUTABLE --columns=ID,NAME --ncopies=2 --disk-persistence=false";
    res = executeCommand(cmd1);
    recMessage = (String) ((JSONArray) res.getContent().get("message")).get(0);
    assertEquals(
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__DISK_PERSISTENCE),
        recMessage);

    // -------------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=ftable11 --type=IMMUTABLE --columns=ID,NAME --ncopies=2 --disk-write-policy=ASYNCHRONOUS";
    res = executeCommand(cmd1);
    recMessage = (String) ((JSONArray) res.getContent().get("message")).get(0);
    assertEquals(
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__DISK_WRITE_POLICY),
        recMessage);

    // --------------------------------------------------------------------------------

    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=ftable11 --type=IMMUTABLE --columns=ID,NAME --ncopies=2 --disk-write-policy=SYNCHRONOUS";
    res = executeCommand(cmd1);
    recMessage = (String) ((JSONArray) res.getContent().get("message")).get(0);
    assertEquals(
        CliStrings.format(CREATE_MTABLE__IMMUTABLE_TYPE_NOT, CREATE_MTABLE__DISK_WRITE_POLICY),
        recMessage);

    // -------------------------------------------------------------------------------------------
    cmd1 = MashCliStrings.CREATE_MTABLE
        + " --name=ftable11 --type=IMMUTABLE --columns=ID,NAME --ncopies=2 ";
    res = executeCommand(cmd1);
    // expected to get non null as all mandatory options are provided
    // disk persistence is always going to be true
    // result name cols diskpersistence ncopies diskwritepolicy
    verifyCreatedTableAndDelete(res, "ftable11", "ID,NAME", true, 2, null, 1000);
    // --------------------------------------------------------------------------------

  }

  private void verifyCreatedTableAndDelete(CommandResult result, final String tableName,
      String columns, boolean diskPersistence, int ncopies, final MDiskWritePolicy writePolicy,
      int blockSize) throws GfJsonException {
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
    // Adding this column to verify.
    columnsList.add("__INSERTION_TIMESTAMP__");

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
        if (writePolicy != null)
          assertEquals(0, MDiskWritePolicy.valueOf(value).compareTo(writePolicy));
        else
          assertEquals(0, MDiskWritePolicy.valueOf(value).compareTo(MDiskWritePolicy.ASYNCHRONOUS));
      }

      if (key.trim().startsWith("Column")) {
        if (!columnsList.contains(value.split(":")[0].trim())) {
          fail("Failed to find given column");
        }
      }

      if (key.equalsIgnoreCase("Block Size")) {
        assertEquals(value.toLowerCase(), String.valueOf(blockSize).toLowerCase());
      }
    }

    cmd1 = MashCliStrings.DELETE_MTABLE + " --name=/" + tableName;
    result = executeCommand(cmd1);
    // assertNotNull(result);
    // assertEquals(Status.OK, result.getStatus());
  }

  protected void createTable2(int noOfPartitions, TableType tableType) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (tableType == TableType.ORDERED_VERSIONED)
      tableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    else if (tableType == TableType.UNORDERED)
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
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


  @Test
  public void testAppendWithSchemaFactTable()
      throws GfJsonException, IOException, InterruptedException {
    final String[][] COLUMNS = {{"ID", "LONG"}, {"MY_MAP", "map<STRING,array<DOUBLE>>"}};
    final Object[] VALUES = {"{'ID': '11', 'MY_MAP': {'ABC' : ['11.111', '11.222']}}",
        "{'ID': '22', 'MY_MAP': {'PQR' : ['22.111', '22.222']}}",};
    final String schema = " --" + MashCliStrings.CREATE_MTABLE_SCHEMA + "=\""
        + TypeUtils.getJsonSchema(COLUMNS) + "\"";
    final String tableName = "table_with_schema";
    final String cmd = MashCliStrings.CREATE_MTABLE + " --type=IMMUTABLE ";

    Result result;
    result = executeCommand(cmd + " --name=" + tableName + schema);

    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    String putCmd = MashCliStrings.APPEND + " --table=/" + tableName;
    result = executeCommand(putCmd + " --value-json=\"" + VALUES[0] + "\"");
    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());
    result = executeCommand(putCmd + " --value-json=\"" + VALUES[1] + "\"");
    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    CommandResult commandResultScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/table_with_schema");
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.OK, commandResultScan.getStatus());

    // ------------------------------------------------------------------------------
    String csvFile = "/tmp/testAppendWithSchemaFactTable.csv";
    File file = new File(csvFile);

    if (file.delete()) {
      System.out.println(file.getName() + " is deleted!");
    } else {
      System.out.println("Delete operation is failed.");
    }
    Thread.sleep(5000);
    commandResultScan =
        executeCommand(MashCliStrings.MSCAN + " --table=/table_with_schema" + " --file=" + csvFile);
    assertNotNull(commandResultScan);
    assertEquals(Result.Status.OK, commandResultScan.getStatus());
    Thread.sleep(5000);

    BufferedReader reader = new BufferedReader(new FileReader(csvFile));

    String[] arr = new String[] {"ID:LONG", "MY_MAP:map<STRING", "array<DOUBLE>>"};
    int count = 0;
    String line = "";
    while ((line = reader.readLine()) != null) {
      System.out.println("MTableCommandsDUnitTest.testAppendWithSchemaFactTable " + line);
      String substring = line.substring(1); // remove #
      String[] array = substring.trim().split(",");
      count++;

      if (count == 1) {
        for (int i = 0; i < 3; i++) {
          assertEquals(arr[i], array[i]);
        }
      }
    }
    assertEquals(3, count);
    // ---------------------------------------------------------------------------------
  }

  @Test
  public void testDestroyTierStore() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testDestroyTierStore --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(
        CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testDestroyTierStore"),
        toString(commandResult));

    commandResult =
        executeCommand(MashCliStrings.DESTROY_TIER_STORE + " --name=testDestroyTierStore");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.DESTROY_TIER_STORE__MSG__SUCCESS),
        toString(commandResult));
    verifyStoreDestroyed("testDestroyTierStore");
  }


  @Test
  public void testDestroyDefaultTierStore() throws Exception {
    Result commandResult =
        executeCommand(MashCliStrings.DESTROY_TIER_STORE + " --name=" + DefaultStore.STORE_NAME);
    assertNotNull(commandResult);
    assertEquals(Result.Status.ERROR, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.DESTROY_TIER_STORE__DEFAULT_STORE_MSG__ERROR),
        toString(commandResult));

  }


  @Test
  public void testsmallCase() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=testsmallCase --type=UNORDERED --schema-json={ID:int,name:string,A:INT,B:STRING}");
    assertNotNull(commandResult);

    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "testsmallCase"),
        toString(commandResult));

  }

  private void verifyStoreDestroyed(final String storeName) {
    verifyStoreDestroyedOnAllServers(storeName);

  }

  private void verifyStoreDestroyedOnAllServers(final String storeName) {
    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          assertNull(StoreHandler.getInstance().getTierStores().get(storeName));
          assertNull(MCacheFactory.getAnyInstance()
              .getRegion(MTableUtils.AMPL_STORE_META_REGION_NAME).get(storeName));
          return null;
        }
      });
    }
  }


  @Test
  public void testTwoTiersInDescribeTable() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=TierStore1 --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "TierStore1"),
        toString(commandResult));


    commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=TierStore2 --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "TierStore2"),
        toString(commandResult));


    commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=TwoTiers --type=IMMUTABLE --columns=ID,NAME,AGE,SEX,DESIGNATION --tier-stores=TierStore1,TierStore2");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "TwoTiers"),
        toString(commandResult));

    CommandResult descCommandResult =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=TwoTiers");

    assertNotNull(descCommandResult);
    assertEquals(Result.Status.OK, descCommandResult.getStatus());

    Object o1 = descCommandResult.getResultData().getGfJsonObject().getJSONObject("content")
        .getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getInternalJsonObject()
        .getJSONArray("Value").get(19);

    Object o2 = descCommandResult.getResultData().getGfJsonObject().getJSONObject("content")
        .getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getInternalJsonObject()
        .getJSONArray("Value").get(22);

    assertEquals(o1, "TierStore1");
    assertEquals(o2, "TierStore2");

  }

  @Test
  public void testDestroyTierStoreFailure() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testDestroyTierStoreFailure --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(
        CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testDestroyTierStoreFailure"),
        toString(commandResult));

    commandResult =
        executeCommand(MashCliStrings.DESTROY_TIER_STORE + " --name=testDestroyTierStoreFailure1");
    assertNotNull(commandResult);
    assertEquals(Result.Status.ERROR, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.DESTROY_TIER_STORE__MSG__ERROR + ": "
        + "Tier store testDestroyTierStoreFailure1 Does not exist"), toString(commandResult));
  }

  @Test
  public void testDestroyTierStoreFailureInUse() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testDestroyTierStoreFailureInUse --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(
        CliStrings
            .format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testDestroyTierStoreFailureInUse"),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=testCreateFTable1 --type=IMMUTABLE --columns=ID,NAME,AGE,SEX,DESIGNATION --tier-stores=testDestroyTierStoreFailureInUse");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "testCreateFTable1"),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=testCreateFTable2 --type=IMMUTABLE --columns=ID,NAME,AGE,SEX,DESIGNATION --tier-stores=testDestroyTierStoreFailureInUse");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "testCreateFTable2"),
        toString(commandResult));


    CommandResult commandResultDescribe =
        executeCommand(MashCliStrings.DESCRIBE_MTABLE + " --name=testCreateFTable2 ");
    assertNotNull(commandResultDescribe);
    assertEquals(Result.Status.OK, commandResultDescribe.getStatus());

    Object o = commandResultDescribe.getResultData().getGfJsonObject().getJSONObject("content")
        .getJSONObject("__sections__-0").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getInternalJsonObject()
        .getJSONArray("Value").get(19);

    assertEquals(o, "testDestroyTierStoreFailureInUse");


    commandResult = executeCommand(
        MashCliStrings.DESTROY_TIER_STORE + " --name=testDestroyTierStoreFailureInUse");
    assertNotNull(commandResult);
    assertEquals(Result.Status.ERROR, commandResult.getStatus());
    assertEquals(
        CliStrings.format(MashCliStrings.DESTROY_TIER_STORE__MSG__ERROR + ": "
            + "Tier store is being used by tables: testCreateFTable1, testCreateFTable2"),
        toString(commandResult));
  }

  @Test
  public void testCreateTierStore() throws Exception {
    final Result commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testTierStore --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testTierStore"),
        toString(commandResult));
  }

  @Test
  public void testCreateTierStoreWithPropertiesWithNullPropsFile() throws Exception {
    String[] files = createPropertiesFiles();
    String storeName = "store1";

    Result commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE + " --name=" + storeName
        + " --handler=io.ampool.tierstore.SampleTierStoreImpl"
        + " --reader=io.ampool.tierstore.SampleTierStoreReaderImpl"
        + " --writer=io.ampool.tierstore.SampleTierStoreWriterImpl" + " --reader-opts=" + files[1]
        + " --writer-opts=" + files[2]);
    assertNotNull(commandResult);

    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + storeName),
        toString(commandResult));
    verifyTierStoreOnAllServers(storeName, "io.ampool.tierstore.SampleTierStoreImpl", null,
        "io.ampool.tierstore.SampleTierStoreReaderImpl",
        "io.ampool.tierstore.SampleTierStoreWriterImpl", files[1], files[2]);

    storeName = storeName + "2";
    commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE + " --name=" + storeName
        + " --handler=io.ampool.tierstore.SampleTierStoreImpl"
        + " --reader=io.ampool.tierstore.SampleTierStoreReaderImpl"
        + " --writer=io.ampool.tierstore.SampleTierStoreWriterImpl" + " --props-file=" + files[0]
        + " --writer-opts=" + files[2]);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + storeName),
        toString(commandResult));
    verifyTierStoreOnAllServers(storeName, "io.ampool.tierstore.SampleTierStoreImpl", files[0],
        "io.ampool.tierstore.SampleTierStoreReaderImpl",
        "io.ampool.tierstore.SampleTierStoreWriterImpl", null, files[2]);

    storeName = storeName + "3";
    commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE + " --name=" + storeName
        + " --handler=io.ampool.tierstore.SampleTierStoreImpl"
        + " --reader=io.ampool.tierstore.SampleTierStoreReaderImpl"
        + " --writer=io.ampool.tierstore.SampleTierStoreWriterImpl" + " --reader-opts=" + files[1]
        + " --props-file=" + files[0]);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + storeName),
        toString(commandResult));
    verifyTierStoreOnAllServers(storeName, "io.ampool.tierstore.SampleTierStoreImpl", null,
        "io.ampool.tierstore.SampleTierStoreReaderImpl",
        "io.ampool.tierstore.SampleTierStoreWriterImpl", files[1], null);

    deletePropertiesFiles(files);
  }

  @Test
  public void testCreateTierStoreWithProperties() throws Exception {
    String[] files = createPropertiesFiles();
    final Result commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testTierStore " + "--handler=io.ampool.tierstore.SampleTierStoreImpl "
        + "--reader=io.ampool.tierstore.SampleTierStoreReaderImpl "
        + "--writer=io.ampool.tierstore.SampleTierStoreWriterImpl " + "--props-file=" + files[0]
        + " --reader-opts=" + files[1] + " --writer-opts=" + files[2]);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testTierStore"),
        toString(commandResult));
    verifyTierStoreOnAllServers("testTierStore", "io.ampool.tierstore.SampleTierStoreImpl",
        files[0], "io.ampool.tierstore.SampleTierStoreReaderImpl",
        "io.ampool.tierstore.SampleTierStoreWriterImpl", files[1], files[2]);
    deletePropertiesFiles(files);
  }

  private void deletePropertiesFiles(String[] files) {
    for (String file : files) {
      File tbd = new File(file);
      tbd.delete();
    }
  }

  private void verifyTierStoreOnAllServers(String storeName, String storeClass,
      String storePropsFile, String storeReader, String storeWriter, String readerPropsFile,
      String writerPropsFile) throws IOException {
    FileInputStream storePropsFIS = null;
    FileInputStream readerPropsFIS = null;
    FileInputStream writerPropsFIS = null;

    Properties storeProps = new Properties();
    Properties readerProps = new Properties();
    Properties writerProps = new Properties();

    if (storePropsFile != null) {
      storePropsFIS = new FileInputStream(storePropsFile);
      storeProps.load(storePropsFIS);
    }
    if (readerPropsFile != null) {
      readerPropsFIS = new FileInputStream(readerPropsFile);
      readerProps.load(readerPropsFIS);
    }
    if (writerPropsFile != null) {
      writerPropsFIS = new FileInputStream(writerPropsFile);
      writerProps.load(writerPropsFIS);
    }

    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() {
          verifyTierStore(storeName, storeClass, storeProps, storeReader, storeWriter, readerProps,
              writerProps);
          return null;
        }
      });
    }
  }

  private void verifyTierStore(String storeName, String storeClass, Properties storeProps,
      String storeReader, String storeWriter, Properties readerProps, Properties writerProps) {
    Exception e = null;
    TierStore store = StoreHandler.getInstance().getTierStore(storeName);
    assertNotNull(store);
    Class clazz = null;
    try {
      clazz = Class.forName(storeClass);
    } catch (ClassNotFoundException e1) {
      e = e1;
      e.printStackTrace();
    }
    assertNull(e);
    e = null;
    assertTrue(clazz.isInstance(store));


    TierStoreReader reader = store.getReader("dummy", 0, null);
    try {
      clazz = Class.forName(storeReader);
    } catch (ClassNotFoundException e1) {
      e = e1;
      e.printStackTrace();
    }
    assertNull(e);
    e = null;
    assertTrue(clazz.isInstance(reader));

    TierStoreWriter writer = store.getWriter("dummy", 0, null);
    try {
      clazz = Class.forName(storeWriter);
    } catch (ClassNotFoundException e1) {
      e = e1;
      e.printStackTrace();
    }
    assertNull(e);
    e = null;
    assertTrue(clazz.isInstance(writer));

    // Check Properties of store, reader and writer in instances.
    SampleTierStoreImpl sampleStore = (SampleTierStoreImpl) store;
    SampleTierStoreReaderImpl sampleReader = (SampleTierStoreReaderImpl) reader;
    SampleTierStoreWriterImpl sampleWriter = (SampleTierStoreWriterImpl) writer;

    compareProperties(storeProps, sampleStore.getProperties());
    compareProperties(readerProps, sampleReader.getProperties());
    compareProperties(writerProps, sampleWriter.getProperties());

    // Check the meta region
    Region<String, Map<String, Object>> region =
        MCacheFactory.getAnyInstance().getRegion(MTableUtils.AMPL_STORE_META_REGION_NAME);
    assertNotNull(region);
    Map<String, Object> rootProps = region.get(storeName);
    assertNotNull(rootProps);

    assertTrue(storeName.compareTo((String) rootProps.get(TierStoreFactory.STORE_NAME_PROP)) == 0);
    assertTrue(
        storeClass.compareTo((String) rootProps.get(TierStoreFactory.STORE_HANDLER_PROP)) == 0);
    compareProperties(storeProps, (Properties) rootProps.get(TierStoreFactory.STORE_OPTS_PROP));

    assertTrue(
        storeReader.compareTo((String) rootProps.get(TierStoreFactory.STORE_READER_PROP)) == 0);
    compareProperties(readerProps,
        (Properties) rootProps.get(TierStoreFactory.STORE_READER_PROPS_PROP));

    assertTrue(
        storeWriter.compareTo((String) rootProps.get(TierStoreFactory.STORE_WRITER_PROP)) == 0);
    compareProperties(writerProps,
        (Properties) rootProps.get(TierStoreFactory.STORE_WRITER_PROPS_PROP));
  }

  private void compareProperties(Properties exp, Properties act) {
    assertTrue((exp == null && act == null) || (exp != null && act != null)
        || (exp.size() == 0 && act == null) || exp.size() == 0 && act.size() == 0);

    if (act == null || exp.size() == 0) {
      return;
    }

    assertEquals(exp.size(), act.size());

    for (Map.Entry entry : exp.entrySet()) {
      String expKey = (String) entry.getKey();
      Object expValue = entry.getValue();
      if (expKey.equalsIgnoreCase("time-to-expire")
          || expKey.equalsIgnoreCase("partition-interval")) {
        // convert to nano
        expValue = convertToMS(Double.parseDouble(String.valueOf(expValue)));
      }
      // assertTrue(((String)act.get(expKey)).compareTo(expValue) == 0);
      assertEquals("Incorrect value for: " + expKey, String.valueOf(expValue),
          String.valueOf(act.get(expKey)));
    }
  }

  private long convertToMS(final Double time) {
    long HOUR_TO_MS = 3600000l;
    return (long) (HOUR_TO_MS * time);
  }

  private String[] createPropertiesFiles() throws IOException {
    String[] files = new String[] {"storeProps.props", "readerProps.props", "writerProps.props"};
    FileOutputStream fosStoreProps = new FileOutputStream(files[0]);
    FileOutputStream fosReaderProps = new FileOutputStream(files[1]);
    FileOutputStream fosWriterProps = new FileOutputStream(files[2]);

    fosStoreProps.write(Bytes.toBytes("store.prop.prop1=value1\n"));
    fosStoreProps.write(Bytes.toBytes("store.prop.prop2=value11\n"));
    fosReaderProps.write(Bytes.toBytes("store.reader.prop.prop1=value2\n"));
    fosReaderProps.write(Bytes.toBytes("store.reader.prop.prop2=value22\n"));
    fosWriterProps.write(Bytes.toBytes("store.writer.prop.prop1=value3\n"));
    fosWriterProps.write(Bytes.toBytes("store.writer.prop.prop2=value33\n"));

    fosStoreProps.close();
    fosReaderProps.close();
    fosWriterProps.close();

    return files;
  }

  @Test
  public void testCreateTierStoreFailure() throws Exception {
    IgnoredException.addIgnoredException(
        "java.lang.ClassNotFoundException: io.ampool.tierstore.SampleTierStoreImpl1");
    final Result commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testCreateTierStoreFailure --handler=io.ampool.tierstore.SampleTierStoreImpl1 --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.ERROR, commandResult.getStatus());
    assertEquals(
        CliStrings.format(MashCliStrings.CREATE_TIERSTORE__FAILURE + "testCreateTierStoreFailure"
            + "\njava.lang.ClassNotFoundException: io.ampool.tierstore.SampleTierStoreImpl1"),
        toString(commandResult));
  }

  @Test
  public void testDescribeTierStoreBasic() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testDescribeTierStoreBasic --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(
        CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testDescribeTierStoreBasic"),
        toString(commandResult));

    commandResult =
        executeCommand(MashCliStrings.DESCRIBE_TIER_STORE + " --name=testDescribeTierStoreBasic");
    assertEquals(Result.Status.OK, commandResult.getStatus());
  }

  @Test
  public void testDescribeTierStoreBasicWithProperties() throws Exception {
    String[] files = createPropertiesFiles();
    CommandResult commandResult = executeCommand(
        MashCliStrings.CREATE_TIERSTORE + " --name=testDescribeTierStoreBasicWithProperties "
            + "--handler=io.ampool.tierstore.SampleTierStoreImpl "
            + "--reader=io.ampool.tierstore.SampleTierStoreReaderImpl "
            + "--writer=io.ampool.tierstore.SampleTierStoreWriterImpl " + "--props-file=" + files[0]
            + " --reader-opts=" + files[1] + " --writer-opts=" + files[2]);

    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(
        CliStrings.format(
            MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testDescribeTierStoreBasicWithProperties"),
        toString(commandResult));

    commandResult = executeCommand(
        MashCliStrings.DESCRIBE_TIER_STORE + " --name=testDescribeTierStoreBasicWithProperties");
    assertEquals(Result.Status.OK, commandResult.getStatus());
    // TODO: add verification
    deletePropertiesFiles(files);
  }

  @Test
  public void testCreateFTableWithTierStore() throws Exception {
    Result commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testTierStore --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testTierStore"),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=testCreateFTable1 --type=IMMUTABLE --columns=ID,NAME,AGE,SEX,DESIGNATION --tier-stores=testTierStore");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "testCreateFTable1"),
        toString(commandResult));
  }

  private void verifyTableOnAllServers(String tableName, Properties storeProps, String[] storeName,
      int numberOfStores) {
    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() {
          verifyTable(tableName, storeProps, storeName, numberOfStores);
          return null;
        }
      });
    }
  }

  private void verifyTable(String tableName, Properties storeProps, String[] storeNames,
      int numberOfTierStores) {
    MCache anyInstance = MCacheFactory.getAnyInstance();
    Admin admin = anyInstance.getAdmin();
    Map<String, FTableDescriptor> stringFTableDescriptorMap = admin.listFTables();

    for (Map.Entry<String, FTableDescriptor> entry : stringFTableDescriptorMap.entrySet()) {
      if (entry.getKey().equalsIgnoreCase("FTable1")) {
        continue;
      }
      System.out.println(entry.getKey() + "/" + entry.getValue());
      assertEquals(tableName, entry.getKey());
      FTableDescriptor value = entry.getValue();

      LinkedHashMap<String, TierStoreConfiguration> tierStores = value.getTierStores();
      assertEquals(numberOfTierStores, tierStores.size());

      Object[] objects = tierStores.keySet().toArray();
      assertEquals(objects, storeNames);

      for (Map.Entry<String, TierStoreConfiguration> entry2 : tierStores.entrySet()) {
        TierStoreConfiguration tierStoreConfiguration = entry2.getValue();
        assertNotNull(tierStoreConfiguration);

        Properties tierProperties = tierStoreConfiguration.getTierProperties();

        compareProperties(storeProps, tierProperties);
      }
    }
  }

  @Test
  public void testCreateFTableWithTierStore2() throws Exception {

    Result commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testTierStoreA --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testTierStoreA"),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=testCreateFTableA --type=IMMUTABLE --columns=ID,NAME,AGE,SEX,DESIGNATION --tier-stores=testTierStoreA --tier1-time-partition-interval=10 --tier1-time-to-expire=10");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "testCreateFTableA"),
        toString(commandResult));

    Properties properties = new Properties();
    properties.put(TierStoreConfiguration.TIER_TIME_TO_EXPIRE, 10L);
    properties.put(TierStoreConfiguration.TIER_PARTITION_INTERVAL, 10L);
    verifyTableOnAllServers("testCreateFTableA", properties, new String[] {"testTierStoreA"}, 1);

    commandResult = executeCommand(MashCliStrings.DELETE_MTABLE + " --name=testCreateFTableA");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

    commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testTierStoreB --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testTierStoreB"),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.CREATE_MTABLE
        + " --name=testCreateFTableB --type=IMMUTABLE --columns=ID,NAME,AGE,SEX,DESIGNATION --tier-stores=testTierStoreA,testTierStoreB --tier1-time-partition-interval=5 --tier1-time-to-expire=10 --tier2-time-partition-interval=5 --tier2-time-to-expire=10");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_MTABLE__SUCCESS + "testCreateFTableB"),
        toString(commandResult));

    Properties properties2 = new Properties();
    properties2.put(TierStoreConfiguration.TIER_TIME_TO_EXPIRE, 10L);
    properties2.put(TierStoreConfiguration.TIER_PARTITION_INTERVAL, 5L);
    verifyTableOnAllServers("testCreateFTableB", properties2,
        new String[] {"testTierStoreA", "testTierStoreB"}, 2);

    commandResult = executeCommand(MashCliStrings.DELETE_MTABLE + " --name=testCreateFTableB");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());

  }

  @Test
  public void testListTierStores() throws Exception {

    Result commandResult = executeCommand(MashCliStrings.LIST_TIER_STORES);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals("Name\n" + "--------------------\n" + "DefaultLocalORCStore",
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testTierStore2 --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testTierStore2"),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.LIST_TIER_STORES);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertTrue(commandResult.toString().contains("\"testTierStore2\""));

    commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testTierStore3 --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testTierStore3"),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.LIST_TIER_STORES);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertTrue(commandResult.toString().contains("\"testTierStore2\",\"testTierStore3\""));

    commandResult = executeCommand(MashCliStrings.CREATE_TIERSTORE
        + " --name=testTierStore4 --handler=io.ampool.tierstore.SampleTierStoreImpl --reader=io.ampool.tierstore.SampleTierStoreReaderImpl --writer=io.ampool.tierstore.SampleTierStoreWriterImpl");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.CREATE_TIERSTORE__SUCCESS + "testTierStore4"),
        toString(commandResult));

    commandResult = executeCommand(MashCliStrings.LIST_TIER_STORES);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertTrue(commandResult.toString()
        .contains("\"testTierStore2\",\"testTierStore3\",\"testTierStore4\""));

    commandResult = executeCommand(MashCliStrings.DESTROY_TIER_STORE + " --name=testTierStore3");
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertEquals(CliStrings.format(MashCliStrings.DESTROY_TIER_STORE__MSG__SUCCESS),
        toString(commandResult));
    verifyStoreDestroyed("testTierStore3");

    commandResult = executeCommand(MashCliStrings.LIST_TIER_STORES);
    assertNotNull(commandResult);
    assertEquals(Result.Status.OK, commandResult.getStatus());
    assertTrue(commandResult.toString().contains("\"testTierStore2\",\"testTierStore4\""));
  }
}
