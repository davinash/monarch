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

package util;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.*;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.junit.categories.MonarchTest;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


@Category(MonarchTest.class)
public class ConcurrentRowTuplePutGetJUnitTest extends TestCase {
  private int numberOfPutsThreads = 5; // 5;

  private int numberOfGetsThreads = 4; // 4;

  // private int numberOfDestroysThreads = 3;

  // private int numberOfClearThreads = 2;

  // protected int numberOfForceRollThreads = 3;

  /**
   * ms to run concurrent ops for before signalling them to stop
   */
  protected int TIME_TO_RUN = 1000;

  private boolean exceptionOccuredInPuts = false;

  private boolean exceptionOccuredInGets = false;

  // private boolean exceptionOccuredInDestroys = false;

  // private boolean exceptionOccuredInClears = false;

  // protected boolean exceptionOccuredInForceRolls = false;

  // if this test is to run for a longer time, make this true
  private static final boolean longTest = false;

  protected boolean failure = false;

  private boolean validate;

  protected Region region1;

  private Region region2;

  private Map<Integer, Lock> map = new ConcurrentHashMap<Integer, Lock>();

  private static int counter = 0;

  private Cache cache;

  protected LogWriter logWriter;

  protected Properties getDSProps() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    props.put("log-level", "config");
    // props.put("log-file", "/work/code/ampool/hbaseJunit.log");
    return props;
  }

  public void setUp() throws Exception {
    // super.setUp();
    Properties props = getDSProps();
    cache = new CacheFactory(props).create();
    logWriter = cache.getLogger();
    counter++;
    if (longTest) {
      TIME_TO_RUN = 10000;
      numberOfPutsThreads = 5;
      numberOfGetsThreads = 4;
      // numberOfDestroysThreads = 3;
      // numberOfClearThreads = 2;
      // numberOfForceRollThreads = 3;
    }
  }

  // @After
  public void tearDown() throws Exception {
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
  }

  /*
   * @Test public void testPersistSyncConcurrency() { this.validate = true; DiskRegionProperties p =
   * new DiskRegionProperties(); p.setRegionName(this.getName() + counter); p.setDiskDirs(dirs);
   * p.setCompactionThreshold(99); region1 = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
   * p, Scope.LOCAL); region2 = concurrencyTest(region1); region1 =
   * DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, p, Scope.LOCAL); validate(region1,
   * region2); }
   */

  private Region createRegion(String regioNname) {
    RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION);
    /*
     * RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION).
     * setRegionDataOrder(true);
     */
    Region r = rf.create(regioNname);
    return r;
  }

  @Test
  public void testRowTupleOps() {/*
                                  * this.validate = true; Region region1 =
                                  * createRegion("testRegion1"); region2 = concurrencyTest(region1);
                                  * validate(region1, region2);
                                  * 
                                  * logWriter.info("Nilkanth patel");
                                  */

  }

  private final AtomicBoolean timeToStop = new AtomicBoolean();

  private boolean isItTimeToStop() {
    return this.timeToStop.get();
  }

  private CyclicBarrier startLine;

  private void waitForAllStartersToBeReady() {
    try {
      startLine.await();
    } catch (InterruptedException ie) {
      fail("unexpected " + ie);
    } catch (BrokenBarrierException ex) {
      fail("unexpected " + ex);
    }
  }

  @SuppressWarnings("synthetic-access")
  class DoesPuts implements Runnable {

    public void run() {
      waitForAllStartersToBeReady();
      while (!isItTimeToStop()) {
        put();
      }
    }
  }

  @SuppressWarnings("synthetic-access")
  class DoesGets implements Runnable {

    public void run() {
      waitForAllStartersToBeReady();
      while (!isItTimeToStop()) {
        get();
      }
    }
  }

  private Random random = new Random();

  void put() {
    int randomInt1 = random.nextInt() % 10;
    if (randomInt1 < 0) {
      randomInt1 = randomInt1 * (-1);
    }
    int randomInt2 = random.nextInt() % 100;
    Integer integer1 = Integer.valueOf(randomInt1);
    Integer integer2 = Integer.valueOf(randomInt2);
    Object v = null;
    Object expected = null;
    Lock lock = null;
    if (this.validate) {
      lock = map.get(integer1);
      lock.lock();
    }
    try {
      try {
        logWriter.info("Nilkanth putting key  - " + integer1.intValue() + "  and value = "
            + integer2.intValue());
        v = region1.put(integer1, integer2);
        if (this.validate) {
          expected = region2.put(integer1, integer2);
        }
      } catch (Exception e) {
        e.printStackTrace();
        exceptionOccuredInPuts = true;
        logWriter.severe("Exception occured in puts ", e);
        fail(" failed during put due to " + e);
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
    if (this.validate) {
      if (v != null) {
        assertEquals(expected, v);
      }
    }
  }

  void get() {
    int randomInt1 = random.nextInt() % 10;
    if (randomInt1 < 0) {
      randomInt1 = randomInt1 * (-1);
    }

    Integer integer1 = Integer.valueOf(randomInt1);
    Object v = null;
    Object expected = null;
    Lock lock = null;
    if (this.validate) {
      lock = map.get(integer1);
      lock.lock();
    }
    try {
      try {
        logWriter.info("Key = " + integer1.longValue());
        v = region1.get(integer1);
        if (this.validate) {
          expected = region2.get(integer1);
        }
      } catch (Exception e) {
        e.printStackTrace();
        exceptionOccuredInGets = true;
        logWriter.severe("Exception occured in get ", e);
        fail(" failed during get due to " + e);
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
    if (this.validate) {
      assertEquals(expected, v);
    }
  }

  private Region concurrencyTest(Region r1) {
    if (this.validate) {
      for (int i = 0; i < 10; i++) {
        map.put(Integer.valueOf(i), new ReentrantLock());
      }
      /*
       * region2 = cache.createVMRegion("testRegion2", new AttributesFactory()
       * .createRegionAttributes());
       */
      region2 = createRegion("testRegion2");
    }
    this.startLine = new CyclicBarrier(numberOfPutsThreads + numberOfGetsThreads
    // + numberOfDestroysThreads
    // + numberOfClearThreads
    // + numberOfForceRollThreads
    );
    DoesPuts doesPuts = new DoesPuts();
    DoesGets doesGets = new DoesGets();

    // TODO:enable it once destroy op is supported
    // DoesDestroy doesDestroy = new DoesDestroy();
    // DoesClear doesClear = new DoesClear();
    // DoesForceRoll doesForceRoll = new DoesForceRoll();

    Thread[] putThreads = new Thread[numberOfPutsThreads];
    Thread[] getThreads = new Thread[numberOfGetsThreads];
    // Thread[] destroyThreads = new Thread[numberOfDestroysThreads];
    // Thread[] clearThreads = new Thread[numberOfClearThreads];
    // Thread[] forceRollThreads = new Thread[numberOfForceRollThreads];
    for (int i = 0; i < numberOfPutsThreads; i++) {
      putThreads[i] = new Thread(doesPuts);
      putThreads[i].setName("PutThread" + i);
    }

    for (int i = 0; i < numberOfGetsThreads; i++) {
      getThreads[i] = new Thread(doesGets);
      getThreads[i].setName("GetThread" + i);
    }
    // TODO:enable it once destroy, clear and rolling op is supported
    /*
     * for (int i = 0; i < numberOfDestroysThreads; i++) { destroyThreads[i] = new
     * Thread(doesDestroy); destroyThreads[i].setName("DelThread" + i); }
     * 
     * for (int i = 0; i < numberOfClearThreads; i++) { clearThreads[i] = new Thread(doesClear);
     * clearThreads[i].setName("ClearThread" + i); }
     * 
     * for (int i = 0; i < numberOfForceRollThreads; i++) { forceRollThreads[i] = new
     * Thread(doesForceRoll); forceRollThreads[i].setName("ForceRoll" + i); }
     */

    this.timeToStop.set(false);
    try {
      for (int i = 0; i < numberOfPutsThreads; i++) {
        putThreads[i].start();
      }
      for (int i = 0; i < numberOfGetsThreads; i++) {
        getThreads[i].start();
      }

      /*
       * for (int i = 0; i < numberOfDestroysThreads; i++) { destroyThreads[i].start(); } for (int i
       * = 0; i < numberOfClearThreads; i++) { clearThreads[i].start(); } for (int i = 0; i <
       * numberOfForceRollThreads; i++) { forceRollThreads[i].start(); }
       */

      try {
        Thread.sleep(TIME_TO_RUN);
      } catch (InterruptedException e) {
        fail("interrupted");
      }
    } finally {
      this.timeToStop.set(true);
    }
    for (int i = 0; i < numberOfPutsThreads; i++) {
      ThreadUtils.join(putThreads[i], 10 * 1000);
    }
    for (int i = 0; i < numberOfGetsThreads; i++) {
      ThreadUtils.join(getThreads[i], 10 * 1000);
    }

    /*
     * for (int i = 0; i < numberOfDestroysThreads; i++) {
     * DistributedTestCase.join(destroyThreads[i], 10*1000, null); } for (int i = 0; i <
     * numberOfClearThreads; i++) { DistributedTestCase.join(clearThreads[i], 10*1000, null); } for
     * (int i = 0; i < numberOfForceRollThreads; i++) {
     * DistributedTestCase.join(forceRollThreads[i], 10*1000, null); }
     */

    if (this.validate) {
      Collection entrySet = region2.entrySet();
      Iterator iterator = entrySet.iterator();
      Map.Entry mapEntry = null;
      Object key, value = null;
      // ((LocalRegion)r1).getDiskRegion().forceFlush();
      while (iterator.hasNext()) {
        mapEntry = (Map.Entry) iterator.next();
        key = mapEntry.getKey();
        value = mapEntry.getValue();
        if (!(r1.containsKey(key))) {
          fail(" region1 does not contain Key " + key + " but was expected to be there");
        }
        if (!(((LocalRegion) r1).getValueOnDisk(key).equals(value))) {
          fail(" value for key " + key + " is " + ((LocalRegion) r1).getValueOnDisk(key)
              + " which is not consistent, it is supposed to be " + value);
        }
      }
    }
    r1.close();

    /*
     * if (exceptionOccuredInDestroys) { fail("Exception occured while destroying"); } if
     * (exceptionOccuredInClears) { fail("Exception occured while clearing"); }
     * 
     * if (exceptionOccuredInForceRolls) { fail("Exception occured while force Rolling"); }
     */

    if (exceptionOccuredInGets) {
      fail("Exception occured while doing gets");
    }

    if (exceptionOccuredInPuts) {
      fail("Exception occured while doing puts");
    }

    return region2;
  }

  void validate(Region r1, Region r2) {
    if (!this.validate)
      return;

    Collection entrySet = r2.entrySet();
    Iterator iterator = entrySet.iterator();
    Map.Entry mapEntry = null;
    Object key, value = null;
    while (iterator.hasNext()) {
      mapEntry = (Map.Entry) iterator.next();
      key = mapEntry.getKey();
      value = mapEntry.getValue();
      if (!(r1.containsKey(key))) {
        fail(" region1 does not contain Key " + key + " but was expected to be there");
      }
      if (!(r1.get(key).equals(value))) {
        fail(" value for key " + key + " is " + r1.get(key)
            + " which is not consistent, it is supposed to be " + value);
      }
    }
    assertEquals(r2.size(), r1.size());

    r1.destroyRegion();
    r2.destroyRegion();
  }

}
