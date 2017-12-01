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
package io.ampool.monarch;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.MTableCreationFunction;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import io.ampool.conf.Constants;
import io.ampool.monarch.standalone.DUnitLauncher;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.*;

/**
 * Helper utility for DUnit
 */
public class MTableDUnitHelper extends CacheTestCase {
  private static final Logger logger = LogService.getLogger();
  protected static MClientCache mClientCache;

  public MTableDUnitHelper(String name) {
    super(name);
  }

  public Object startServerOn(VM vm, final String locators) {
    try {
      return vm.invoke(new SerializableCallable() {

        @Override
        public Object call() throws Exception {
          Properties props = new Properties();
          props.setProperty(DistributionConfig.LOG_LEVEL_NAME, String.valueOf("config"));
          props.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");
          props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
          props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
          props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "Stats");
          Cache c = null;
          try {
            c = CacheFactory.getAnyInstance();
            c.close();
          } catch (CacheClosedException cce) {
          }
          c = CacheFactory.create(getSystem(props));
          CacheServer s = c.addCacheServer();
          int port = AvailablePortHelper.getRandomAvailableTCPPort();
          s.setPort(port);
          s.start();

          //registerFunction();
          return port;
        }
      });
    } catch (RMIException e) {
      e.printStackTrace();
    }
    return null;
  }

  public void registerFunction() {
    FunctionService.registerFunction(new MTableCreationFunction());
  }

  public void createClientCache() {
    MConfiguration mconf = MConfiguration.create();
    mconf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, "127.0.0.1");
    mconf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, getLocatorPort());

    String testMethod = getTestName();
    String testName = getTestClass().getName() + '-' + testMethod;
    String logFileName = testName + "-client.log";

    mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, logFileName);

    //MConnection mcc = MConnectionFactory.createConnection(mconf);
    mClientCache = new MClientCacheFactory().create(mconf);
  }

  protected MClientCache getmClientCache(){
    return mClientCache;
  }


  public void createClientCache(VM vm) {
    try {
      vm.invoke(new SerializableCallable() {

        @Override
        public Object call() throws Exception {
          createClientCache();
          return null;
        }
      });
    } catch (RMIException e) {
      e.printStackTrace();
    }
  }


  public void startClientWithSpecifiedServer(final int port) {

    MConfiguration mconf = MConfiguration.create();
    //mconf.set(Constants.MonarchServer.MONARCH_SERVER_ADDRESS, "127.0.0.1");
    //mconf.setInt(Constants.MonarchServer.MONARCH_SERVER_PORT, port);

    String testMethod = getTestName();
    String testName = getTestClass().getName() + '-' + testMethod;
    String logFileName = testName + "-client.log";

    mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, logFileName);

    MClientCache clientCache = new MClientCacheFactory().create(mconf);
  }

  public void startClientWithSpecifiedServerOn(VM vm, final int port) {
    try {
      vm.invoke(new SerializableCallable() {

        @Override
        public Object call() throws Exception {
          startClientWithSpecifiedServer(port);
          return null;
        }
      });
    } catch (RMIException e) {
      e.printStackTrace();
    }
  }

  public void closeMClientCache() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    if (clientCache != null) {
      clientCache.close();
      clientCache = null;
    }
  }

  public void closeMClientCache(VM vm) {
    try {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          closeMClientCache();
          return null;
        }
      });
    } catch (RMIException e) {
      e.printStackTrace();
    }
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

  public void cleanUpAllVMs() throws Exception {
    invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Cache c = null;
        try {
          c = CacheFactory.getAnyInstance();
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
    try {
      invokeInEveryVM(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          Cache c = CacheFactory.getAnyInstance();
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
    } catch (RMIException e) {
      e.printStackTrace();
    }

  }

  public void verifyRegionSizeAllVMs(final String regionName, final int expectedSize) {
    try {
      invokeInEveryVM(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          Cache c = CacheFactory.getAnyInstance();
          assertNotNull(c);
          Region r = c.getRegion(regionName);
          PartitionedRegion pr = (PartitionedRegion) r;
          assertNotNull(pr);
          assertEquals(expectedSize, pr.size());
          return null;
        }
      });
    } catch (RMIException e) {
      e.printStackTrace();
    }

  }

  public void closeMCacheAllVMs() {
    try {
      invokeInEveryVM(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCache cache = MCacheFactory.getAnyInstance();
          cache.close();
          return null;
        }
      });
    } catch (RMIException e) {
      e.printStackTrace();
    }
  }

  public void closeMCacheOnVM(VM vm) {
    try {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          MCache cache = MCacheFactory.getAnyInstance();
          cache.close();
          return null;
        }
      });
    } catch (RMIException e) {
      e.printStackTrace();
    }
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
    List<byte[]> result = new ArrayList<>();
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

    for (int i = 0; i < numOfKeys; i++) {
      BigInteger keyBI = startBI.add(BigInteger.valueOf(nextLong(0, difference)));
      byte[] padded = keyBI.toByteArray();
      if (padded[1] == 0)
        padded = Bytes.tail(padded, padded.length - 2);
      else
        padded = Bytes.tail(padded, padded.length - 1);
      result.add(padded);
    }
    return result;
  }



  
  /* Helper function which will generate all the keys required to have
   * uniform distribution across the partitions ( i.e. buckets in our case )
   * One need to pass numOfPartitions and how many keys one would required
   * for each partition.
   */
  public Map<Integer, List<byte[]>> getKeysForAllBuckets(int numOfPartitions, int numOfKeysEachPartition) {
    Map<Integer, Pair<byte[], byte[]>> splitsMap = MTableUtils.getUniformKeySpaceSplit(numOfPartitions);
    assertEquals(numOfPartitions, splitsMap.size());
    Map<Integer, List<byte[]>> bucket2KeysMap = new HashMap<>();

    splitsMap.forEach((K,V) -> {
      List<byte[]> keysInRange = getKeysInRange(V.getFirst(), V.getSecond(), numOfKeysEachPartition);
      assertEquals(numOfKeysEachPartition, keysInRange.size());
      //keysInRange.forEach((B) -> allKeys.add(B));
      bucket2KeysMap.put(K, keysInRange);
    });

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
  public Map<Integer, List<byte[]>> getBucketToKeysForAllBuckets(int numOfPartitions, int numOfKeysEachPartition) {
    Map<Integer, List<byte[]>> bucketIdToKeys = new HashMap<>();
    Map<Integer, Pair<byte[], byte[]>> splitsMap = MTableUtils.getUniformKeySpaceSplit(numOfPartitions);
    assertEquals(numOfPartitions, splitsMap.size());
    splitsMap.forEach((K,V) -> {
      List<byte[]> keysInRange = getKeysInRange(V.getFirst(), V.getSecond(), numOfKeysEachPartition);
      assertEquals(numOfKeysEachPartition, keysInRange.size());
      bucketIdToKeys.put(K, keysInRange);
      //keysInRange.forEach((B) -> bucketIdToKeys.put(K, B);
    });
    return bucketIdToKeys;
  }
}
