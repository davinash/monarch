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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Category(MonarchTest.class)
public class MTableOverflowDunitTest extends MTableDUnitHelper {
  public MTableOverflowDunitTest() {
    super();
  }

  private static final Logger logger = LogService.getLogger();

  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MTableOverflowDunitTest";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 1;
  private final String COLUMN_NAME_PREFIX = "COLUMN";

  final float heapPercentage = 50.9f;
  final int evictorInterval = 100;
  private int entryCountOnThisVM = 0;

  /*
   * Following method creates 3 cache servers and 2 clients
   */
  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);

    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
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

  public MTableDescriptor createTable(final boolean ordered) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }

    tableDescriptor.setRedundantCopies(1);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    MTable table = clientCache.getAdmin().createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
    return tableDescriptor;
  }


  private List<byte[]> doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

    Map<Integer, List<byte[]>> keysForAllBuckets =
        getKeysForAllBuckets(table.getTableDescriptor().getTotalNumOfSplits(), NUM_OF_ROWS);

    List<byte[]> allKeys = new ArrayList<>(keysForAllBuckets.size());
    keysForAllBuckets.forEach((BID, KEY_LIST) -> KEY_LIST.forEach((KEY) -> allKeys.add(KEY)));

    assertEquals((table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS), allKeys.size());

    allKeys.forEach((K) -> {
      Put record = new Put(K);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    });

    return allKeys;
  }

  private void doGets(List<byte[]> keys) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    keys.forEach((K) -> {
      Get get = new Get(K);
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS + 1, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (int i = 0; i < row.size() - 1; i++) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, row.get(i).getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out
              .println("actualColumnName   => " + Arrays.toString(row.get(i).getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) row.get(i).getColumnValue())) {
          System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
          System.out.println(
              "actualValue    => " + Arrays.toString((byte[]) row.get(i).getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
    });
  }

  protected void raiseFakeNotification() {
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

  final SerializableCallable assertBucketAttributesAndEviction =
      new SerializableCallable("Assert bucket attributes and eviction") {
        public Object call() throws Exception {

          try {
            MCacheFactory.getAnyInstance().getResourceManager()
                .setEvictionHeapPercentage(heapPercentage);
            final int maximumWaitSeconds = 60; // seconds
            final int pollWaitMillis = evictorInterval * 2;
            assertTrue(pollWaitMillis < (TimeUnit.SECONDS.toMillis(maximumWaitSeconds) * 4));
            final PartitionedRegion pr =
                (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(TABLE_NAME);
            assertNotNull(pr);
            entryCountOnThisVM = 0;
            for (final Iterator i = pr.getDataStore().getAllLocalBuckets().iterator(); i
                .hasNext();) {
              final Map.Entry entry = (Map.Entry) i.next();
              final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
              if (bucketRegion == null) {
                continue;
              }
              assertTrue(
                  bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm().isLRUHeap());
              assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                  .isOverflowToDisk());
              entryCountOnThisVM++;
            }
            raiseFakeNotification();
            WaitCriterion wc = new WaitCriterion() {
              String excuse;

              public boolean done() {
                // we have a primary
                if (pr.getDiskRegionStats().getNumOverflowOnDisk() == entryCountOnThisVM) {
                  return true;
                }
                return false;
              }

              public String description() {
                return excuse;
              }
            };
            Wait.waitForCriterion(wc, 60000, 1000, true);

            int entriesEvicted = 0;

            entriesEvicted += pr.getDiskRegionStats().getNumOverflowOnDisk();
            return new Integer(entriesEvicted);
          } finally {
            ((MonarchCacheImpl) MCacheFactory.getAnyInstance())
                .getHeapEvictor().testAbortAfterLoopCount = Integer.MAX_VALUE;
            HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
            System.clearProperty("gemfire.memoryEventTolerance");
          }
        }
      };


  @Test
  public void testSimplePutsAndGets() {
    MTableDescriptor tableDescriptor = createTable(true);
    List<byte[]> keys = doPuts();

    final Integer v0i = (Integer) vm0.invoke(assertBucketAttributesAndEviction);
    final Integer v1i = (Integer) vm1.invoke(assertBucketAttributesAndEviction);
    final Integer v2i = (Integer) vm2.invoke(assertBucketAttributesAndEviction);

    final int totalEvicted = v0i.intValue() + v1i.intValue() + v2i.intValue();
    assertEquals(tableDescriptor.getTotalNumOfSplits() * NUM_OF_ROWS
        * (tableDescriptor.getRedundantCopies() + 1), totalEvicted);

    doGets(keys);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);

  }
}
