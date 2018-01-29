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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MCacheClosedException;
import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.internal.AMPLJUnit4CacheTestCase;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.MTableCreationFunction;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

public class MTableDUnitHelper extends AMPLJUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();
  protected static MClientCache mClientCache;
  protected Host host = null;
  protected VM vm0 = null;
  protected VM vm1 = null;
  protected VM vm2 = null;
  protected VM vm3 = null;
  protected VM client1 = null;

  public MTableDUnitHelper() {
    super();
  }

  @Override
  public void preSetUp() throws Exception {
    disconnectAllFromDS();
    super.preSetUp();
  }

  @Override
  public void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    client1 = host.getVM(3);
  }

  public Object startServer(final String locators) throws IOException {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
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
    // registerFunction();
    return port;
  }

  public Object startServerOn(VM vm, final String locators) {
    System.out.println("Starting server on VM: " + vm.toString());
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return startServer(locators);
      }
    });
  }

  public Object startServerOn(VM vm, final String locators, final Properties props) {
    System.out.println("Starting server on VM: " + vm.toString());
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
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
        MCacheFactory.getAnyInstance();
        // registerFunction();
        return port;
      }
    });
  }

  public void registerFunction() {
    FunctionService.registerFunction(new MTableCreationFunction());
  }

  public void createClientCache() {
    String testName = getTestMethodName();
    String logFileName = testName + "-client.log";
    mClientCache = new MClientCacheFactory().addPoolLocator("127.0.0.1", getLocatorPort())
        .set("log-file", logFileName).create();
    assertNotNull(mClientCache);
  }

  public void createClientCacheUsingDeprecatedAPI() {
    String testName = getTestMethodName();
    String logFileName = testName + "-client.log";
    MConfiguration configuration = MConfiguration.create();
    configuration.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, "127.0.0.1");
    configuration.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, getLocatorPort());
    configuration.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, logFileName);

    mClientCache = new MClientCacheFactory().create(configuration);
    assertNotNull(mClientCache);
  }

  public void createClientCacheUsingDeprecatedAPI(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        createClientCacheUsingDeprecatedAPI();
        return null;
      }
    });
  }

  protected MClientCache getmClientCache() {
    return mClientCache;
  }


  public void createClientCache(VM vm) {
    vm.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        createClientCache();
        return null;
      }
    });
  }


  public void closeMClientCache() {
    try {
      MClientCache clientCache = MClientCacheFactory.getAnyInstance();
      if (clientCache != null) {
        clientCache.close();
        clientCache = null;
      }
    } catch (MCacheClosedException m) {
    }

  }

  public void closeMClientCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        closeMClientCache();
        return null;
      }
    });
  }


  public int getLocatorPort() {
    if (DUnitLauncher.isLaunched()) {
      String locatorString = DUnitLauncher.getLocatorString();
      int index = locatorString.indexOf("[");
      return Integer.parseInt(locatorString.substring(index + 1, locatorString.length() - 1));
    }
    // Running in hydra
    else {
      return getDUnitLocatorPort();
    }
  }

  /**
   *
   * @return
   */
  private int getDUnitLocatorPort() {
    return DistributedTestUtils.getDUnitLocatorPort();
  }

  public void cleanUpAllVMs() throws Exception {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache c = null;
        try {
          c = MCacheFactory.getAnyInstance();
          if (c != null) {
            c.close();
          }
        } catch (CacheClosedException e) {
        }
        return null;
      }
    });
  }

  public void verifyInternalRegionAllVMs(final String regionName, boolean regionExists) {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache c = MCacheFactory.getAnyInstance();
        assertNotNull(c);
        Region r = c.getRegion(regionName);
        if (regionExists) {
          assertNotNull(r);
          PartitionedRegion pr = (PartitionedRegion) r;
          assertNotNull(pr);
        } else {
          assertNull(r);
        }
        return null;
      }
    });

  }

  public void verifyRegionSizeAllVMs(final String regionName, final int expectedSize) {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache c = MCacheFactory.getAnyInstance();
        assertNotNull(c);
        Region r = c.getRegion(regionName);
        PartitionedRegion pr = (PartitionedRegion) r;
        assertNotNull(pr);
        assertEquals(expectedSize, pr.size());
        return null;
      }
    });

  }

  public void closeMCacheAllVMs() {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        cache.close();
        return null;
      }
    });
  }

  public void closeMCacheOnVM(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          MCache cache = MCacheFactory.getAnyInstance();
          cache.close();
        } catch (CacheClosedException cacheClosed) {
        }
        return null;
      }
    });
  }

  private Random rand = new Random();

  public long nextLong(final long lower, final long upper) throws IllegalArgumentException {
    if (lower >= upper) {
      throw new IllegalArgumentException("Wrong Input");
    }
    final long max = (upper - lower) + 1;
    if (max <= 0) {
      while (true) {
        final long r = rand.nextLong();
        if (r >= lower && r <= upper) {
          return r;
        }
      }
    } else if (max < Integer.MAX_VALUE) {
      return lower + rand.nextInt((int) max);
    } else {
      return lower + nextLong(max);
    }
  }


  public Object stopServerOn(VM vm) {
    System.out.println("Stopping server on VM: " + vm.toString());
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          MCache mCache = MCacheFactory.getAnyInstance();
          if (mCache != null && !mCache.isClosed()) {
            Iterator iter = mCache.getCacheServers().iterator();
            if (iter.hasNext()) {
              CacheServer server = (CacheServer) iter.next();
              server.stop();
              mCache.close();
            }
          }
        } catch (CacheClosedException e) {
          e.printStackTrace();
          System.out.println("MCache is closed.");
        } catch (Exception e) {
          fail("failed while stopServer()" + e);
        }

        return null;
      }
    });
  }

  public AsyncInvocation ayncStopServerOn(VM vm) {
    return vm.invokeAsync(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          Iterator iter = MCacheFactory.getAnyInstance().getCacheServers().iterator();
          if (iter.hasNext()) {
            CacheServer server = (CacheServer) iter.next();
            server.stop();
          }
        } catch (Exception e) {
          fail("failed while stopServer()" + e);
        }
        return null;
      }
    });
  }

  private long nextLong(final long n) throws IllegalArgumentException {
    if (n > 0) {
      final byte[] byteArray = new byte[8];
      long bits;
      long val;
      do {
        rand.nextBytes(byteArray);
        bits = 0;
        for (final byte b : byteArray) {
          bits = (bits << 8) | (((long) b) & 0xffL);
        }
        bits &= 0x7fffffffffffffffL;
        val = bits % n;
      } while (bits - val + (n - 1) < 0);
      return val;
    }
    throw new IllegalArgumentException("something wrong ");
  }

  public List<byte[]> getKeysInRange(byte[] start, byte[] stop, int numOfKeys) {
    byte[] aPadded;
    byte[] bPadded;
    Set<byte[]> result = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    if (start.length < stop.length) {
      aPadded = Bytes.padTail(start, stop.length - start.length);
      bPadded = stop;
    } else if (stop.length < start.length) {
      aPadded = start;
      bPadded = Bytes.padTail(stop, start.length - stop.length);
    } else {
      aPadded = start;
      bPadded = stop;
    }
    if (Bytes.compareTo(aPadded, bPadded) >= 0) {
      throw new IllegalArgumentException("b <= a");
    }
    if (numOfKeys <= 0) {
      throw new IllegalArgumentException("numOfKeys cannot be <= 0");
    }

    byte[] prependHeader = {1, 0};
    final BigInteger startBI = new BigInteger(Bytes.add(prependHeader, aPadded));
    final BigInteger stopBI = new BigInteger(Bytes.add(prependHeader, bPadded));

    BigInteger diffBI = stopBI.subtract(startBI);
    long difference = diffBI.longValue();
    if (diffBI.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0) {
      difference = Long.MAX_VALUE;
    }
    byte[] padded = null;
    for (int i = 0; i < numOfKeys; i++) {
      do {
        BigInteger keyBI = startBI.add(BigInteger.valueOf(nextLong(0, difference)));
        padded = keyBI.toByteArray();
        if (padded[1] == 0)
          padded = Bytes.tail(padded, padded.length - 2);
        else
          padded = Bytes.tail(padded, padded.length - 1);
      } while (!result.add(padded));
    }
    return new ArrayList<>(result);
  }

  /*
   * Helper function which will generate all the keys required to have uniform distribution across
   * the partitions ( i.e. buckets in our case ) One need to pass numOfPartitions and how many keys
   * one would required for each partition.
   */
  public Map<Integer, List<byte[]>> getKeysForAllBuckets(int numOfPartitions,
      int numOfKeysEachPartition) {
    Map<Integer, Pair<byte[], byte[]>> splitsMap =
        MTableUtils.getUniformKeySpaceSplit(numOfPartitions);
    assertEquals(numOfPartitions, splitsMap.size());
    Map<Integer, List<byte[]>> bucket2KeysMap = new HashMap<>();

    splitsMap.forEach((K, V) -> {
      List<byte[]> keysInRange =
          getKeysInRange(V.getFirst(), V.getSecond(), numOfKeysEachPartition);
      assertEquals(numOfKeysEachPartition, keysInRange.size());
      // keysInRange.forEach((B) -> allKeys.add(B));
      bucket2KeysMap.put(K, keysInRange);
    });

    Long[] resust = new Long[3];
    List<Integer> result = new ArrayList<>();
    result.add(123);

    return bucket2KeysMap;

  }


  /**
   * Function to generate keys which which spawns on given number of partitions.
   *
   *
   * @param numOfPartitions
   * @param numOfKeysEachPartition
   * @return Returns the map of partition id to generate keys
   */
  public Map<Integer, List<byte[]>> getBucketToKeysForAllBuckets(int numOfPartitions,
      int numOfKeysEachPartition) {
    Map<Integer, List<byte[]>> bucketIdToKeys = new HashMap<>();
    Map<Integer, Pair<byte[], byte[]>> splitsMap =
        MTableUtils.getUniformKeySpaceSplit(numOfPartitions);
    assertEquals(numOfPartitions, splitsMap.size());
    splitsMap.forEach((K, V) -> {
      List<byte[]> keysInRange =
          getKeysInRange(V.getFirst(), V.getSecond(), numOfKeysEachPartition);
      assertEquals(numOfKeysEachPartition, keysInRange.size());
      bucketIdToKeys.put(K, keysInRange);
      // keysInRange.forEach((B) -> bucketIdToKeys.put(K, B);
    });
    return bucketIdToKeys;
  }


  public void tearDown2() throws Exception {}

  @Override
  public void preTearDownCacheTestCase() throws Exception {
    tearDown2();
    super.preTearDownCacheTestCase();
  }

  public void closeAllMCaches() {
    Host host = Host.getHost(0);
    for (int i = 0; i < host.getVMCount(); i++) {
      host.getVM(i).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCache cache = MCacheFactory.getAnyInstance();
          if (cache != null)
            cache.close();
          return null;
        }
      });
    }
  }


  /**
   * Create table with default schema | COL1 | COL2 | COL3 | COL4 | COL5 |
   *
   * @param tableName
   * @return
   */
  public MTable createMTable(String tableName) {
    String COL_PREFIX = "COL";
    MTableDescriptor tableDescriptor = new MTableDescriptor();

    for (int i = 1; i <= 5; i++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COL_PREFIX + i));
    }
    tableDescriptor.setMaxVersions(5);
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    MTable mtable = null;
    try {
      mtable = admin.createTable(tableName, tableDescriptor);
    } catch (MTableExistsException e) {
      e.printStackTrace();
    }
    assertNotNull(mtable);
    assertEquals(mtable.getName(), tableName);
    return mtable;
  }

  /**
   * Puts rowsPerBucket rows into each bucket
   *
   * @param mtable
   * @param rowsPerBucket
   */
  public List<byte[]> putDataInEachBucket(MTable mtable, int rowsPerBucket) {
    final MTableDescriptor mtableDesc = mtable.getTableDescriptor();
    Map<Integer, Pair<byte[], byte[]>> splitsMap =
        MTableUtils.getUniformKeySpaceSplit(mtableDesc.getTotalNumOfSplits());
    List<byte[]> keysInRange = new ArrayList<>();
    for (int bucketId = 0; bucketId < mtableDesc.getTotalNumOfSplits(); bucketId++) {
      Pair<byte[], byte[]> pair = splitsMap.get(bucketId);
      keysInRange.addAll(getKeysInRange(pair.getFirst(), pair.getSecond(), rowsPerBucket));
    }
    putKeysInRange(mtable, keysInRange);
    return keysInRange;
  }

  private void putKeysInRange(MTable table, List<byte[]> keysInRange) {
    int i = 0;
    List<Put> puts = new ArrayList<>();
    for (byte[] key : keysInRange) {
      Put myput1 = new Put(key);
      myput1.addColumn(Bytes.toBytes("COL1"), Bytes.toBytes("User" + i));
      myput1.addColumn(Bytes.toBytes("COL2"), Bytes.toBytes(i * 2));
      myput1.addColumn(Bytes.toBytes("COL3"), Bytes.toBytes(i * 3));
      myput1.addColumn(Bytes.toBytes("COL4"), Bytes.toBytes(i * 4));
      myput1.addColumn(Bytes.toBytes("COL5"), Bytes.toBytes(i * 5));
      puts.add(myput1);
      i++;
    }
    table.put(puts);
  }

  /**
   * Deletes given table
   *
   * @param tableName
   */
  public void deleteMTable(String tableName) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    Admin admin = clientCache.getAdmin();
    admin.deleteTable(tableName);
  }

  /**
   * Restart the servers on the specified VMs. It stops all VMs and then restarts the server on
   * respective VMs asynchronously.
   *
   * @param vms server VMs to be restarted
   */
  public void restartServers(final List<VM> vms) {
    for (VM vm : vms) {
      stopServerOn(vm);
    }
    vms.parallelStream().forEach(vm -> {
      try {
        asyncStartServerOn(vm, DUnitLauncher.getLocatorString()).join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
  }
}
