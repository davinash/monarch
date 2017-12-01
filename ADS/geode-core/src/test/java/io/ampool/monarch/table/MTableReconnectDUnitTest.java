package io.ampool.monarch.table;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Category(MonarchTest.class)
public class MTableReconnectDUnitTest extends MTableDUnitHelper {

  protected static final Logger logger = LogService.getLogger();

  // private VM client1 = host.getVM(3);

  static int locatorPort;
  static Locator locator;
  static DistributedSystem savedSystem;
  static int locatorVMNumber = 3;
  static Thread gfshThread;

  protected final int NUM_OF_COLUMNS = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int NUM_OF_ROWS = 100;
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final String TABLE_NAME1 = "SampleTable_1";
  protected final String TABLE_NAME2 = "SampleTable_2";

  Properties dsProperties;

  public MTableReconnectDUnitTest() {
    super();
  }

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    IgnoredException
        .addIgnoredException("org.apache.geode.ForcedDisconnectException||Possible loss of quorum");
    System.out
        .println("MTableReconnectDUnitTest.preSetUp locator = " + DUnitLauncher.getLocatorString());
    startServerWithReconnectConfigOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerWithReconnectConfigOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerWithReconnectConfigOn(this.vm2, DUnitLauncher.getLocatorString());

    // createClientCache(this.client1);
    // createClientCache();

  }

  public Object startServerWithReconnectConfigOn(VM vm, final String locators) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
        props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
        // props.setProperty(DistributionConfig.MAX_WAIT_TIME_FOR_RECONNECT_NAME, "20000");
        // props.setProperty(DistributionConfig.ENABLE_NETWORK_PARTITION_DETECTION_NAME, "true");
        props.setProperty(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, "false");
        // props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        // props.setProperty(DistributionConfig.MEMBER_TIMEOUT_NAME, "1000");
        // props.setProperty("max-num-reconnect-tries", "2");
        // props.setProperty("enable-cluster-configuration", "true");
        // props.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME,"true");
        MCache c = null;
        try {
          c = MCacheFactory.getAnyInstance();
          c.close();
        } catch (CacheClosedException cce) {
        }
        c = MCacheFactory.create(getSystem(props));
        IgnoredException.addIgnoredException(
            "org.apache.geode.ForcedDisconnectException||Possible loss of quorum");
        CacheServer s = c.addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        s.setPort(port);
        s.start();

        // create disk-store
        // DiskStoreFactory diskStoreFactory =
        // MCacheFactory.getAnyInstance().createDiskStoreFactory();
        // diskStoreFactory.create(diskStoreName);

        MCacheFactory.getAnyInstance();
        // registerFunction();
        return port;
      }
    });
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
    // closeMClientCache();
    closeMCacheAll();
    super.tearDown2();
  }

  /*
   * @Override public final void postSetUp() throws Exception { this.locatorPort =
   * AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET); final int locPort =
   * this.locatorPort; Host.getHost(0).getVM(locatorVMNumber) .invoke(new
   * SerializableRunnable("start locator") { public void run() { try { InternalDistributedSystem ds
   * = InternalDistributedSystem.getConnectedInstance(); if (ds != null) { ds.disconnect(); }
   * locatorPort = locPort; Properties props = getDistributedSystemProperties(); locator =
   * Locator.startLocatorAndDS(locatorPort, new File(""), props); IgnoredException.
   * addIgnoredException("org.apache.geode.ForcedDisconnectException||Possible loss of quorum"); //
   * MembershipManagerHelper.getMembershipManager(InternalDistributedSystem.getConnectedInstance()).
   * setDebugJGroups(true); } catch (IOException e) { Assert.fail("unable to start locator", e); } }
   * });
   * 
   * //beginCacheXml(); //createRegion("myRegion", createAtts()); //finishCacheXml("MyDisconnect");
   * MCache cache = getAmplCache(); closeCache(); getSystem().disconnect();
   * LogWriterUtils.getLogWriter().fine("MCache Closed "); }
   */

  /*
   * @Override public Properties getDistributedSystemProperties() { if (dsProperties == null) {
   * dsProperties = super.getDistributedSystemProperties();
   * dsProperties.put(DistributionConfig.MAX_WAIT_TIME_FOR_RECONNECT_NAME, "20000");
   * dsProperties.put(DistributionConfig.ENABLE_NETWORK_PARTITION_DETECTION_NAME, "true");
   * dsProperties.put(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, "false");
   * dsProperties.put(DistributionConfig.LOCATORS_NAME, "localHost["+this.locatorPort+"]");
   * dsProperties.put(DistributionConfig.MCAST_PORT_NAME, "0");
   * dsProperties.put(DistributionConfig.MEMBER_TIMEOUT_NAME, "1000");
   * dsProperties.put(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel()); }
   * return dsProperties; }
   * 
   * @Override public final void postTearDownCacheTestCase() throws Exception { try {
   * Host.getHost(0).getVM(locatorVMNumber).invoke(new SerializableRunnable("stop locator") { public
   * void run() { if (locator != null) { LogWriterUtils.getLogWriter().info("stopping locator " +
   * locator); locator.stop(); } } }); } finally { Invoke.invokeInEveryVM(new SerializableRunnable()
   * { public void run() { MTableReconnectDUnitTest.savedSystem = null; } }); disconnectAllFromDS();
   * } }
   * 
   *//**
     * Creates some region attributes for the regions being created.
     *//*
       * private RegionAttributes createAtts() { AttributesFactory factory = new
       * AttributesFactory();
       * 
       * { // TestCacheListener listener = new TestCacheListener(){}; // this needs to be
       * serializable //callbacks.add(listener); //factory.setDataPolicy(DataPolicy.REPLICATE);
       * factory.setDataPolicy(DataPolicy.REPLICATE); factory.setScope(Scope.DISTRIBUTED_ACK); //
       * factory.setCacheListener(listener); }
       * 
       * return factory.create(); }
       */

  private void doPuts(String tableName) {
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
  }

  private void doGets(String tableName) {
    System.out.println("MTableReconnectDUnitTest.doGets for table = " + tableName);
    MTable table = MCacheFactory.getAnyInstance().getTable(tableName);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();

      /* Verify the row Id features */
      if (!Bytes.equals(result.getRowId(), Bytes.toBytes(KEY_PREFIX + rowIndex))) {
        System.out.println(
            "expectedColumnName => " + Arrays.toString(Bytes.toBytes(KEY_PREFIX + rowIndex)));
        System.out.println("aSectualColumnName   => " + Arrays.toString(result.getRowId()));
        org.junit.Assert.fail("Invalid Row Id");
      }

      for (int i = 0; i < row.size() - 1; i++) {
        org.junit.Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out
              .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
          org.junit.Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
          System.out.println(
              "actualValue    => " + Arrays.toString((byte[]) row.get(i).getColumnValue()));
          org.junit.Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
    }
  }

  protected void createTable(String tableName) {
    MCache cache = MCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    // tableDescriptor.setRedundantCopies(0);
    tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);

    // tableDescriptor.setDiskStore(diskStoreName);
    MTable table = null;
    try {
      table = cache.getAdmin().createTable(tableName, tableDescriptor);
    } catch (MTableExistsException mte) {
      table = cache.getTable(tableName);
    }

    assertEquals(table.getName(), tableName);
    assertNotNull(table);
  }

  @Test
  public void testReconnectOnForcedDisconnect() throws Exception {
    doTestReconnectOnForcedDisconnect();
  }

  public void doTestReconnectOnForcedDisconnect() throws Exception {

    IgnoredException.addIgnoredException("killing member's ds");

    SerializableCallable gets = new SerializableCallable("Create MCache and mtables") {
      public Object call() throws Exception {
        Thread.sleep(10000);

        MTableReconnectDUnitTest.savedSystem =
            MCacheFactory.getAnyInstance().getDistributedSystem();
        // verify gets on mTables after reconnect
        doGets("SampleTable_1");

        doGets("SampleTable_2");

        Region metaRegion =
            MCacheFactory.getAnyInstance().getRegion(MTableUtils.AMPL_META_REGION_NAME);
        System.out.println("MTableReconnectDUnitTest.call MetaR.size() " + metaRegion.size());
        // verify regions
        Iterator<Map.Entry> iterator = metaRegion.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry entry = iterator.next();
          System.out.println("MTableReconnectDUnitTest.call key = " + entry.getKey());
          System.out.println("MTableReconnectDUnitTest.call value = " + entry.getValue());
        }

        Thread.sleep(10000);

        // list diskstore for this members
        Collection<DiskStoreImpl> diskStores = ((MonarchCacheImpl) MCacheFactory.getAnyInstance())
            .listDiskStoresIncludingRegionOwned();
        for (DiskStoreImpl ds : diskStores) {
          System.out.println("MTableReconnectDUnitTest.call DS Name = " + ds.getName());
        }

        return MCacheFactory.getAnyInstance().getDistributedSystem().getDistributedMember();
      }
    };

    /*
     * SerializableCallable createDiskStore = new SerializableCallable( "Create MCache and mtables")
     * { public Object call() throws Exception { MTableReconnectDUnitTest.savedSystem =
     * MCacheFactory.getAnyInstance().getDistributedSystem(); DiskStoreFactory diskStoreFactory =
     * MCacheFactory.getAnyInstance().createDiskStoreFactory();
     * diskStoreFactory.create(diskStoreName); return savedSystem.getDistributedMember(); } };
     */


    SerializableCallable create1 = new SerializableCallable("Create MCache and mtables") {
      public Object call() throws Exception {
        MTableReconnectDUnitTest.savedSystem =
            MCacheFactory.getAnyInstance().getDistributedSystem();
        // CreateTable
        createTable("SampleTable_1");

        // doPuts
        doPuts("SampleTable_1");
        return savedSystem.getDistributedMember();
      }
    };

    SerializableCallable create2 = new SerializableCallable("Create MCache and mtables") {
      public Object call() throws Exception {
        // CreateTable
        createTable("SampleTable_2");

        // doPuts
        doPuts("SampleTable_2");
        MTableReconnectDUnitTest.savedSystem =
            MCacheFactory.getAnyInstance().getDistributedSystem();
        return savedSystem.getDistributedMember();
      }
    };
    this.vm0.invoke(create1);
    DistributedMember dm = (DistributedMember) this.vm1.invoke(create2);
    forceDisconnect(this.vm1);

    DistributedMember newdm =
        (DistributedMember) this.vm1.invoke(new SerializableCallable("wait for reconnect(1)") {
          public Object call() {
            final DistributedSystem ds = MTableReconnectDUnitTest.savedSystem;
            MTableReconnectDUnitTest.savedSystem = null;
            Wait.waitForCriterion(new WaitCriterion() {
              public boolean done() {
                return ds.isReconnecting();
              }

              public String description() {
                return "waiting for ds to begin reconnecting";
              }
            }, 30000, 1000, true);
            LogWriterUtils.getLogWriter().info("entering reconnect wait for " + ds);
            LogWriterUtils.getLogWriter().info("ds.isReconnecting() = " + ds.isReconnecting());
            boolean failure = true;
            try {
              ds.waitUntilReconnected(60, TimeUnit.SECONDS);
              MTableReconnectDUnitTest.savedSystem = ds.getReconnectedSystem();
              InternalLocator locator = (InternalLocator) Locator.getLocator();
              assertTrue("Expected system to be restarted", ds.getReconnectedSystem() != null);
              assertTrue("Expected system to be running", ds.getReconnectedSystem().isConnected());
              // assertTrue("Expected there to be a locator", locator != null);
              // assertTrue("Expected locator to be restarted", !locator.isStopped());
              failure = false;
              return ds.getReconnectedSystem().getDistributedMember();
            } catch (InterruptedException e) {
              LogWriterUtils.getLogWriter().warning("interrupted while waiting for reconnect");
              return null;
            } finally {
              if (failure) {
                ds.disconnect();
              }
            }
          }
        });
    assertNotSame(dm, newdm);

    // this.vm1.invoke(createDiskStore);
    this.vm1.invoke(gets);
    // force another reconnect and show that stopReconnecting works
    /*
     * forceDisconnect(vm1); boolean stopped = (Boolean)vm1.invoke(new
     * SerializableCallable("wait for reconnect and stop") { public Object call() { final
     * DistributedSystem ds = MTableReconnectDUnitTest.savedSystem;
     * MTableReconnectDUnitTest.savedSystem = null; Wait.waitForCriterion(new WaitCriterion() {
     * public boolean done() { return ds.isReconnecting() || ds.getReconnectedSystem() != null; }
     * public String description() { return "waiting for reconnect to commence in " + ds; }
     * 
     * }, 10000, 1000, true); ds.stopReconnecting(); assertFalse(ds.isReconnecting());
     * DistributedSystem newDs = InternalDistributedSystem.getAnyInstance(); if (newDs != null) {
     * LogWriterUtils.getLogWriter().warning("expected distributed system to be disconnected: " +
     * newDs); return false; } return true; } });
     * assertTrue("expected DistributedSystem to disconnect", stopped);
     */

    // recreate the system in vm1 without a locator and crash it
    dm = (DistributedMember) this.vm1.invoke(create1);
    forceDisconnect(this.vm1);
    newdm = waitForReconnect(this.vm1);
    assertNotSame("expected a reconnect to occur in member", dm, newdm);
    // DistributedTestUtils.deleteLocatorStateFile(locPort);
    // DistributedTestUtils.deleteLocatorStateFile(secondLocPort);
  }

  private DistributedMember waitForReconnect(VM vm) {
    return (DistributedMember) vm
        .invoke(new SerializableCallable("wait for Reconnect and return ID") {
          public Object call() {
            System.out.println("waitForReconnect invoked");
            final DistributedSystem ds = MTableReconnectDUnitTest.savedSystem;
            MTableReconnectDUnitTest.savedSystem = null;
            Wait.waitForCriterion(new WaitCriterion() {
              public boolean done() {
                return ds.isReconnecting();
              }

              public String description() {
                return "waiting for ds to begin reconnecting";
              }
            }, 30000, 1000, true);
            long waitTime = 120;
            LogWriterUtils.getLogWriter().info("VM" + VM.getCurrentVMNum() + " waiting up to "
                + waitTime + " seconds for reconnect to complete");
            try {
              ds.waitUntilReconnected(waitTime, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              fail("interrupted while waiting for reconnect");
            }
            assertTrue("expected system to be reconnected", ds.getReconnectedSystem() != null);
            int oldViewId =
                MembershipManagerHelper.getMembershipManager(ds).getLocalMember().getVmViewId();
            int newViewId =
                ((InternalDistributedMember) ds.getReconnectedSystem().getDistributedMember())
                    .getVmViewId();
            if (!(newViewId > oldViewId)) {
              fail("expected a new ID to be assigned.  oldViewId=" + oldViewId + "; newViewId="
                  + newViewId);
            }
            return ds.getReconnectedSystem().getDistributedMember();
          }
        });
  }

  public static volatile int reconnectTries;

  public static volatile boolean initialized = false;

  public static volatile boolean initialRolePlayerStarted = false;


  void addReconnectListener() {
    reconnectTries = 0; // reset the count for this listener
    LogWriterUtils.getLogWriter().info("adding reconnect listener");
    InternalDistributedSystem.ReconnectListener reconlis =
        new InternalDistributedSystem.ReconnectListener() {
          public void reconnecting(InternalDistributedSystem oldSys) {
            LogWriterUtils.getLogWriter().info("reconnect listener invoked");
            reconnectTries++;
          }

          public void onReconnect(InternalDistributedSystem system1,
              InternalDistributedSystem system2) {}
        };
    InternalDistributedSystem.addReconnectListener(reconlis);
  }

  private void waitTimeout() throws InterruptedException {
    Thread.sleep(500);

  }

  public boolean forceDisconnect(VM vm) {
    return (Boolean) vm.invoke(new SerializableCallable("crash distributed system") {
      public Object call() throws Exception {
        IgnoredException.addIgnoredException(
            "org.apache.geode.ForcedDisconnectException||Possible loss of quorum");
        // since the system will disconnect and attempt to reconnect
        // a new system the old reference to DTC.system can cause
        // trouble, so we first null it out.
        nullSystem();
        final DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
        // final Locator oldLocator = Locator.getLocator();
        MembershipManagerHelper.crashDistributedSystem(msys);
        /*
         * if (oldLocator != null) { WaitCriterion wc = new WaitCriterion() { public boolean done()
         * { return msys.isReconnecting(); } public String description() { return
         * "waiting for locator to start reconnecting: " + oldLocator; } };
         * Wait.waitForCriterion(wc, 10000, 50, true); }
         */
        return true;
      }
    });
  }

}
