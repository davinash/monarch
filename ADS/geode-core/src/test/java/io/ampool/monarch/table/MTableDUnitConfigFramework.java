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

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.CDCConfigImpl;
import io.ampool.monarch.table.internal.CDCInformation;
import io.ampool.monarch.table.internal.MTableImpl;
import io.ampool.monarch.types.DataTypeFactory;
import org.apache.geode.cache.*;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class MTableDUnitConfigFramework extends MTableDUnitHelper {

  private static final Logger logger = LogService.getLogger();

  private Host host = Host.getHost(0);
  protected VM vm0 = host.getVM(0);
  protected VM vm1 = host.getVM(1);
  protected VM vm2 = host.getVM(2);
  protected VM client1 = host.getVM(3);
  private List<VM> allVMList = new ArrayList<>(Arrays.asList(vm0, vm1, vm2));

  public static String COLUMNNAME_PREFIX = "COL", VALUE_PREFIX = "VAL";
  protected static int NUM_COLUMNS = 5;
  protected static String[] types = {"BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "DOUBLE", "FLOAT",
      "BIG_DECIMAL", "STRING", "BINARY", "CHARS", "VARCHAR", "DATE", "TIMESTAMP"};
  public String TABLENAME = "testTable";
  public static int currentConfigId = 0;
  public static MTableDescriptor currentTableDescriptor = null;

  /**
   * MTable reference variable which will point to table with specific configuration currently under
   * consideration
   */
  private static MTable table = null;

  public MTableDUnitConfigFramework() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());

    createClientCache(client1);
    createClientCache();
  }

  private void closeMCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        cache.close();
        return null;
      }
    });
  }

  private void closeMCacheAll() {
    closeMCache(this.vm0);
    closeMCache(this.vm1);
    closeMCache(this.vm2);
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(this.client1);
    closeMCacheAll();
    super.tearDown2();
  }

  public void restartTestFramework() {
    stopAllServers();

    startAllServersAsync();
  }

  protected void stopAllServers() {
    try {
      closeMClientCache();
      closeMClientCache(client1);
      new ArrayList<>(Arrays.asList(vm0, vm1, vm2))
          .forEach((VM) -> VM.invoke(new SerializableCallable() {
            @Override
            public Object call() throws Exception {
              MCache anyInstance = MCacheFactory.getAnyInstance();
              ArrayList<CDCInformation> cdcInformations =
                  ((MTableImpl) anyInstance.getTable(TABLENAME)).getTableDescriptor()
                      .getCdcInformations();
              Region tableRegion = ((MTableImpl) anyInstance.getTable(TABLENAME)).getTableRegion();
              AttributesMutator attributesMutator = tableRegion.getAttributesMutator();
              for (int i = 0; i < cdcInformations.size(); i++) {
                attributesMutator.removeAsyncEventQueueId(cdcInformations.get(i).getQueueId());
              }
              anyInstance.close();
              return null;
            }
          }));
    } catch (Exception ex) {
      fail(ex.getMessage());
    }
  }

  public Object startServerOn(VM vm, final String locators) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "config");
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
        // MCacheFactory.getAnyInstance().createDiskStoreFactory().create(diskStoreName);
        // MCacheFactory.getAnyInstance();
        // registerFunction();
        return port;
      }
    });
  }

  public AsyncInvocation asyncStartServerOn(VM vm, final String locators) {
    return vm.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "config");
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
        // MCacheFactory.getAnyInstance().createDiskStoreFactory().create(diskStoreName);
        // MCacheFactory.getAnyInstance();
        // registerFunction();
        return port;
      }
    });
  }

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

  /**
   * Static method returning current instance of MTable
   *
   * @return MTable instance
   */
  public static MTable getTable() {
    return table;
  }

  public static int getCurrentConfigId() {
    return currentConfigId;
  }

  public static MTableDescriptor createMTableDescriptor(boolean withTypes) {
    return createMTableDescriptor(withTypes, MTableType.ORDERED_VERSIONED);
  }

  public static MTableDescriptor createMTableDescriptor(boolean withTypes, MTableType tableType) {
    MTableDescriptor mTableDescriptor = new MTableDescriptor(tableType);
    if (!withTypes) {
      for (int i = 0; i < NUM_COLUMNS; i++) {
        mTableDescriptor.addColumn(Bytes.toBytes(COLUMNNAME_PREFIX + i));
      }
    } else {
      for (int i = 0; i < types.length; i++) {
        mTableDescriptor.addColumn(COLUMNNAME_PREFIX + i,
            new MTableColumnType(DataTypeFactory.getTypeFromString(types[i])));
      }
    }
    return mTableDescriptor;
  }

  /**
   * Generating keyspace for MTable
   *
   * @return keyspace
   */
  private static Map<Integer, Pair<byte[], byte[]>> getKeySpace() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("000"), Bytes.toBytes("010")));
    map.put(1, new Pair<>(Bytes.toBytes("011"), Bytes.toBytes("020")));
    map.put(2, new Pair<>(Bytes.toBytes("021"), Bytes.toBytes("030")));
    map.put(3, new Pair<>(Bytes.toBytes("031"), Bytes.toBytes("040")));
    map.put(4, new Pair<>(Bytes.toBytes("041"), Bytes.toBytes("050")));
    map.put(5, new Pair<>(Bytes.toBytes("051"), Bytes.toBytes("060")));
    map.put(6, new Pair<>(Bytes.toBytes("061"), Bytes.toBytes("070")));
    map.put(7, new Pair<>(Bytes.toBytes("071"), Bytes.toBytes("080")));
    map.put(8, new Pair<>(Bytes.toBytes("081"), Bytes.toBytes("090")));
    map.put(9, new Pair<>(Bytes.toBytes("091"), Bytes.toBytes("100")));
    return map;
  }

  /**
   * Map holding all MTable configurations with integer id
   */
  private static Object[] mTableDescriptors = new Object[2];

  private static final String diskStoreName = "MTableDiskStore";

  static {
    mTableDescriptors[0] = new HashMap<Integer, MTableDescriptor>();
    mTableDescriptors[1] = new HashMap<Integer, MTableDescriptor>();

    setDescriptors(0, false);
    setDescriptors(1, true);
  }

  // Set #1 MTable with default splits i.e. 113
  private static void setDescriptors(int index, boolean withTypes) {
    Map<Integer, MTableDescriptor> mTableDescriptorMap =
        (Map<Integer, MTableDescriptor>) mTableDescriptors[index];

    mTableDescriptorMap.put(1,
        createMTableDescriptor(withTypes)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null)); // ORDERED
    // -
    // NO
    // PERSISTENCE
    // -
    // 1
    // MAXVERSION
    mTableDescriptorMap.put(2,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null)); // UNORDERED
    // -
    // NOPERSISTENCE
    // -
    // 1
    // MAXVERSION
    mTableDescriptorMap.put(3,
        createMTableDescriptor(withTypes).setMaxVersions(5)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null)); // ORDERED
    // -
    // NOPERSISTENCE
    // -
    // 5
    // MAXVERSION
    mTableDescriptorMap.put(4,
        createMTableDescriptor(withTypes).enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P4", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true))); // ORDERED -
    // DISKPERSISTENCE
    // - 1
    // MAXVERSION
    mTableDescriptorMap.put(5,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P5", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setDiskSynchronous(true).setPersistent(true))); // UNORDERED -
    // NOPERSISTENCE
    // - 1
    // MAXVERSION
    mTableDescriptorMap.put(6,
        createMTableDescriptor(withTypes)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));// ORDERED
    // -
    // NOPERSISTENCE
    // - 1
    // MAXVERSION
    // - 3
    // COPIES
    mTableDescriptorMap.put(7,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));

    // Set #2 MTable with splits set to 1

    mTableDescriptorMap.put(8,
        createMTableDescriptor(withTypes).setTotalNumOfSplits(1)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null)); // ORDERED
    // -
    // NO
    // PERSISTENCE
    // -
    // 1
    // MAXVERSION
    mTableDescriptorMap.put(9,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setTotalNumOfSplits(1)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null)); // UNORDERED
    // -
    // NOPERSISTENCE
    // -
    // 1
    // MAXVERSION
    mTableDescriptorMap.put(10,
        createMTableDescriptor(withTypes).setTotalNumOfSplits(1).setMaxVersions(5)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null)); // ORDERED
    // -
    // NOPERSISTENCE
    // -
    // 5
    // MAXVERSION
    mTableDescriptorMap.put(11,
        createMTableDescriptor(withTypes).setTotalNumOfSplits(1)
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P11", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setDiskSynchronous(false).setPersistent(true))); // ORDERED -
    // DISKPERSISTENCE
    // - 1
    // MAXVERSION
    mTableDescriptorMap.put(12,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setTotalNumOfSplits(1)
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P12", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false))); // UNORDERED -
    // NOPERSISTENCE
    // - 1
    // MAXVERSION
    mTableDescriptorMap.put(13,
        createMTableDescriptor(withTypes).setTotalNumOfSplits(1).setRedundantCopies(3)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")); // ORDERED
    // -
    // NOPERSISTENCE
    // -
    // 1
    // MAXVERSION
    // -
    // 3
    // COPIES
    mTableDescriptorMap.put(14,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setTotalNumOfSplits(1)
            .setRedundantCopies(3)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")); // UNORDERED
    // -
    // NOPERSISTENCE
    // -
    // 1
    // MAXVERSION
    // -
    // 3
    // COPIES

    // Set #3 MTable with setting keyspace

    mTableDescriptorMap.put(15,
        createMTableDescriptor(withTypes).setKeySpace(getKeySpace())
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));
    // ORDERED - NO PERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(16,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setKeySpace(getKeySpace())
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));
    // UNORDERED - NOPERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(17,
        createMTableDescriptor(withTypes).setKeySpace(getKeySpace()).setMaxVersions(5)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));
    // ORDERED - NOPERSISTENCE - 5 MAXVERSION
    mTableDescriptorMap.put(18,
        createMTableDescriptor(withTypes).setKeySpace(getKeySpace())
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P18", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    // ORDERED - DISKPERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(19,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setKeySpace(getKeySpace())
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P19", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    // UNORDERED - NOPERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(20,
        createMTableDescriptor(withTypes).setKeySpace(getKeySpace()).setRedundantCopies(3)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor"));
    // ORDERED - NOPERSISTENCE - 1 MAXVERSION - 3 COPIES
    mTableDescriptorMap.put(21,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setKeySpace(getKeySpace())
            .setRedundantCopies(3)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor"));
    // UNORDERED - NOPERSISTENCE - 1 MAXVERSION - 3 COPIES

    // Set #4 MTable configuration with setting setStartStopRangeKey with default splits i.e. 113

    mTableDescriptorMap.put(22,
        createMTableDescriptor(withTypes)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));
    // ORDERED - NO PERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(23,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));
    // UNORDERED - NOPERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(24,
        createMTableDescriptor(withTypes)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100")).setMaxVersions(5)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));
    // ORDERED - NOPERSISTENCE - 5 MAXVERSION
    mTableDescriptorMap.put(25,
        createMTableDescriptor(withTypes)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P25", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    // ORDERED - DISKPERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(26,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P26", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    // UNORDERED - NOPERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(27,
        createMTableDescriptor(withTypes)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100")).setRedundantCopies(3)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor"));
    // ORDERED - NOPERSISTENCE - 1 MAXVERSION - 3 COPIES
    mTableDescriptorMap.put(28,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100")).setRedundantCopies(3)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor"));
    // UNORDERED - NOPERSISTENCE - 1 MAXVERSION - 3 COPIES

    // Set #5 MTable configuration with setting setStartStopRangeKey with splits set to 10

    mTableDescriptorMap.put(29, createMTableDescriptor(withTypes)
        .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100")).setTotalNumOfSplits(10)
        .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
        .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));
    // ORDERED - NO PERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(30, createMTableDescriptor(withTypes, MTableType.UNORDERED)
        .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100")).setTotalNumOfSplits(10)
        .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
        .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));
    // UNORDERED - NOPERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(31,
        createMTableDescriptor(withTypes)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .setTotalNumOfSplits(10).setMaxVersions(5)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));
    // ORDERED - NOPERSISTENCE - 5 MAXVERSION
    mTableDescriptorMap.put(32,
        createMTableDescriptor(withTypes)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .setTotalNumOfSplits(10).enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P32", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    // ORDERED - DISKPERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(33,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .setTotalNumOfSplits(10).enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P33", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    // UNORDERED - NOPERSISTENCE - 1 MAXVERSION
    mTableDescriptorMap.put(34,
        createMTableDescriptor(withTypes)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .setTotalNumOfSplits(10).setRedundantCopies(3)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor"));
    // ORDERED - NOPERSISTENCE - 1 MAXVERSION - 3 COPIES
    mTableDescriptorMap.put(35,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .setTotalNumOfSplits(10).setRedundantCopies(3)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor"));
    // UNORDERED - NOPERSISTENCE - 1 MAXVERSION - 3 COPIES

    mTableDescriptorMap.put(36,
        createMTableDescriptor(withTypes).enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P36", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(37,
        createMTableDescriptor(withTypes).enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS)
            .setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P37", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    mTableDescriptorMap.put(38,
        createMTableDescriptor(withTypes).enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P38", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(39,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P39", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(40,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P40", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    mTableDescriptorMap.put(41,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P41", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(42,
        createMTableDescriptor(withTypes).setTotalNumOfSplits(1)
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P42", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(43,
        createMTableDescriptor(withTypes).setTotalNumOfSplits(1)
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P43", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    mTableDescriptorMap.put(44,
        createMTableDescriptor(withTypes).setTotalNumOfSplits(1)
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P44", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(45,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setTotalNumOfSplits(1)
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P45", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(46,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setTotalNumOfSplits(1)
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P46", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    mTableDescriptorMap.put(47,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setTotalNumOfSplits(1)
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P47", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(48,
        createMTableDescriptor(withTypes).setKeySpace(getKeySpace())
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P48", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(49,
        createMTableDescriptor(withTypes).setKeySpace(getKeySpace())
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P49", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    mTableDescriptorMap.put(50,
        createMTableDescriptor(withTypes).setKeySpace(getKeySpace())
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P50", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(51,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setKeySpace(getKeySpace())
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P51", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(52,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setKeySpace(getKeySpace())
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P52", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    mTableDescriptorMap.put(53,
        createMTableDescriptor(withTypes, MTableType.UNORDERED).setKeySpace(getKeySpace())
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P53", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(54,
        createMTableDescriptor(withTypes)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P54", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(55,
        createMTableDescriptor(withTypes)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P55", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    mTableDescriptorMap.put(56,
        createMTableDescriptor(withTypes)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P56", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(57,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P57", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(58,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS).setDiskStore(diskStoreName)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P58", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    mTableDescriptorMap.put(59,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P59", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(60,
        createMTableDescriptor(withTypes)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .setTotalNumOfSplits(10).enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P60", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(61, createMTableDescriptor(withTypes)
        .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100")).setTotalNumOfSplits(10)
        .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS).setDiskStore(diskStoreName)
        .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
        .addCDCStream("CDCStream_P61", "io.ampool.monarch.table.cdc.CDCEventListener",
            new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    mTableDescriptorMap.put(62, createMTableDescriptor(withTypes)
        .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100")).setTotalNumOfSplits(10)
        .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS).setDiskStore(diskStoreName)
        .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
        .addCDCStream("CDCStream_P62", "io.ampool.monarch.table.cdc.CDCEventListener",
            new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(63,
        createMTableDescriptor(withTypes, MTableType.UNORDERED)
            .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100"))
            .setTotalNumOfSplits(10).enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .addCDCStream("CDCStream_P63", "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    mTableDescriptorMap.put(64, createMTableDescriptor(withTypes, MTableType.UNORDERED)
        .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100")).setTotalNumOfSplits(10)
        .enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS).setDiskStore(diskStoreName)
        .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
        .addCDCStream("CDCStream_P64", "io.ampool.monarch.table.cdc.CDCEventListener",
            new CDCConfigImpl().setPersistent(true).setDiskSynchronous(false)));
    mTableDescriptorMap.put(65, createMTableDescriptor(withTypes, MTableType.UNORDERED)
        .setStartStopRangeKey(Bytes.toBytes("000"), Bytes.toBytes("100")).setTotalNumOfSplits(10)
        .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS).setDiskStore(diskStoreName)
        .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
        .addCDCStream("CDCStream_P65", "io.ampool.monarch.table.cdc.CDCEventListener",
            new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));
    // Add CoProcessor
    mTableDescriptorMap.put(66,
        createMTableDescriptor(withTypes)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRowCountCoprocessor")
            .enableDiskPersistence(MDiskWritePolicy.SYNCHRONOUS).addCDCStream("CDCStream_P66",
                "io.ampool.monarch.table.cdc.CDCEventListener",
                new CDCConfigImpl().setPersistent(true).setDiskSynchronous(true)));

    mTableDescriptorMap.put(67,
        createMTableDescriptor(withTypes).setEvictionPolicy(MEvictionPolicy.NO_ACTION)
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null)); // GEN-887
    mTableDescriptorMap.put(68,
        createMTableDescriptor(withTypes)
            .addCoprocessor("io.ampool.monarch.table.coprocessor.SampleRegionObserver2")
            .addCDCStream("CDCStream", "io.ampool.monarch.table.cdc.CDCEventListener", null));
  }


  public MTable createTable(String tableName, MTableDescriptor tableDescriptor) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = admin.createTable(tableName, tableDescriptor);
    assertEquals(mtable.getName(), tableName);
    return mtable;
  }

  public void deleteTable(String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    admin.deleteTable(tableName);
    deleteDiskStore(tableName);
  }

  private void deleteDiskStore(String tableName) {
    allVMList.forEach((V) -> V.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        DiskStore diskStore = MCacheFactory.getAnyInstance().findDiskStore(tableName + "-ds");
        if (diskStore != null) {
          diskStore.destroy();
        }
        return null;
      }
    }));
  }

  private void truncateStoreFiles(String storeName) {
    try {
      Stream<Path> list = Files.list(new File(".").toPath());
      Object[] paths = list.toArray();
      for (Object path : paths) {
        if (path.toString().contains(storeName)) {
          Files.write((Path) path, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public MTable truncateTable(String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    try {
      admin.truncateTable(tableName);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    return getTableFromClientCache();
  }

  protected interface FrameworkRunnable {
    void run();

    default void runAfterRestart() {}
  }

  public static MTableDescriptor getCurrentTableDescriptor() {
    return currentTableDescriptor;
  }

  /**
   * Framework runner which accepts runnable instance and runs the runnable for various
   * configurations as specificed in {@link #mTableDescriptors}. This method runs configurations
   * even after restart
   *
   * @param runnable
   */
  public void runAllConfigs(FrameworkRunnable runnable) {
    String tableName = getTestMethodName();
    runAllConfigs(tableName, runnable, true, false);
  }

  public void runConfig(int configId, FrameworkRunnable runnable, boolean doRestart,
      boolean withTypes) {
    int index = 0;
    if (withTypes) {
      index = 1;
      NUM_COLUMNS = types.length;
    } else {
      index = 0;
      NUM_COLUMNS = 5;
    }

    Map<Integer, MTableDescriptor> descriptorMap =
        (Map<Integer, MTableDescriptor>) mTableDescriptors[index];
    MTableDescriptor mTableDescriptor = descriptorMap.get(configId);

    System.out.println("MTableDUnitConfigFramework.frameworkRunner :: "
        + "===========================================================================");
    System.out
        .println("MTableDUnitConfigFramework.frameworkRunner ::     Running Configuration id: "
            + configId + "(" + (withTypes ? "With types" : "Without types") + ")");
    System.out.println("MTableDUnitConfigFramework.runAllConfigs :: " + "MTable Configuration::\n"
        + mTableDescriptor);

    executeConfig(runnable, doRestart, withTypes, mTableDescriptor);
  }

  public void runAllConfigs(FrameworkRunnable runnable, boolean doRestart, boolean withTypes) {
    String tableName = getTestMethodName();
    runAllConfigs(tableName, runnable, doRestart, withTypes);
  }

  /**
   * Same as {@link #runAllConfigs(FrameworkRunnable)}}, but gives option to turn off restart
   * functionality.
   *
   * @param runnable
   * @param doRestart
   */
  public void runAllConfigs(String tableName, FrameworkRunnable runnable, boolean doRestart,
      boolean withTypes) {
    TABLENAME = tableName;
    logger.info("TableName ->> " + TABLENAME);

    int index = 0;
    if (withTypes) {
      index = 1;
      NUM_COLUMNS = types.length;
    } else {
      index = 0;
      NUM_COLUMNS = 5;
    }

    Map<Integer, MTableDescriptor> descriptorMap =
        (Map<Integer, MTableDescriptor>) mTableDescriptors[index];

    for (Map.Entry<Integer, MTableDescriptor> integerMTableDescriptorEntry : descriptorMap
        .entrySet()) {
      // TABLENAME = baseTableName + integerMTableDescriptorEntry.getKey();
      currentConfigId = integerMTableDescriptorEntry.getKey();
      MTableDescriptor mTableDescriptor = integerMTableDescriptorEntry.getValue();
      currentTableDescriptor = mTableDescriptor;
      System.out.println("MTableDUnitConfigFramework.frameworkRunner :: "
          + "===========================================================================");
      System.out
          .println("MTableDUnitConfigFramework.frameworkRunner ::     Running Configuration id: "
              + integerMTableDescriptorEntry.getKey() + "("
              + (withTypes ? "With types" : "Without types") + ")" + " of total "
              + descriptorMap.size());
      System.out.println("MTableDUnitConfigFramework.runAllConfigs :: " + "MTable Configuration::\n"
          + mTableDescriptor);
      executeConfig(runnable, doRestart, withTypes, mTableDescriptor);
    }
  }

  private void executeConfig(final FrameworkRunnable runnable, final boolean doRestart,
      final boolean withTypes, MTableDescriptor tableDescriptor) {
    closeMClientCache();
    closeMClientCache(client1);
    createClientCache(this.client1);
    createClientCache();
    System.out.println("MTableDUnitConfigFramework.frameworkRunner :: "
        + "===========================================================================");
    table = createTable(TABLENAME, tableDescriptor);
    assertNotNull(table);
    System.out
        .println("MTableDUnitConfigFramework.frameworkRunner :: " + "Running user test method");
    runnable.run();
    if (doRestart) {
      System.out.println("MTableDUnitConfigFramework.frameworkRunner :: "
          + "------------------------------------------------------------------------");
      System.out.println("MTableDUnitConfigFramework.frameworkRunner :: " + "       Restarting...");
      // Restart
      restartTestFramework();
      System.out.println("MTableDUnitConfigFramework.frameworkRunner :: "
          + "------------------------------------------------------------------------");
      table = getTableFromClientCache();
      assertNotNull(table);
      System.out.println("MTableDUnitConfigFramework.frameworkRunner :: "
          + "Running user test method after restart");
      runnable.runAfterRestart();
    }
    deleteTable(TABLENAME);
    System.out.println("MTableDUnitConfigFramework.frameworkRunner :: "
        + "===========================================================================");
  }

  private void recreateClientCache() {
    closeMClientCache();
    closeMClientCache(client1);
    createClientCache(this.client1);
    createClientCache();
  }

  protected MTable getTableFromClientCache() {
    return MClientCacheFactory.getAnyInstance().getTable(TABLENAME);
  }
}
