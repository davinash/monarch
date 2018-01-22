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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableDUnitHelper;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.BlockKey;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.utils.TimestampUtil;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.FTableTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(FTableTest.class)
public class FTableBlockKeyDUnitTest extends MTableDUnitHelper {
  private static final int NUM_COLS = 10;
  List<VM> allServers = null;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    startServerOn(vm0, DUnitLauncher.getLocatorString());
    startServerOn(vm1, DUnitLauncher.getLocatorString());
    startServerOn(vm2, DUnitLauncher.getLocatorString());
    createClientCache(client1);
    createClientCache();
    allServers = Arrays.asList(vm0, vm1, vm2);
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    allServers.forEach((VM) -> VM.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCacheFactory.getAnyInstance().close();
        return null;
      }
    }));

    super.tearDown2();
  }

  private FTable createFtable(String tableName) {
    FTableDescriptor fd = new FTableDescriptor();
    fd.setBlockSize(1000);

    for (int i = 0; i < NUM_COLS; i++) {
      fd.addColumn("COL_" + i);
    }

    fd.setRedundantCopies(2);

    FTable ftable = MClientCacheFactory.getAnyInstance().getAdmin().createFTable(tableName, fd);

    /*
     * Make sure tbale is created
     */
    checkTableOnServers(tableName);

    return ftable;
  }

  private void checkTableOnServers(String tableName) {
    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          assertNotNull(MCacheFactory.getAnyInstance().getTableDescriptor(tableName));
          return null;
        }
      });
    }
  }

  private void verifyRecordCountOnClient(String tableName, int expCount) {
    FTable table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    Scanner scanner = table.getScanner(new Scan());
    Iterator itr = scanner.iterator();
    int actualCount = 0;
    while (itr.hasNext()) {
      itr.next();
      actualCount++;
    }
    assertEquals(expCount, actualCount);
  }



  private void verifyRecordCountOnServers(String tableName, int expectedCount) {
    for (VM vm : allServers) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          int actualCount = 0;
          MCache amplCache = MCacheFactory.getAnyInstance();
          TableDescriptor td = amplCache.getTableDescriptor(tableName);
          int numSplits = td.getTotalNumOfSplits();
          PartitionedRegion pr = (PartitionedRegion) amplCache.getRegion(tableName);
          for (int i = 0; i < numSplits; i++) {
            BucketRegion br = pr.getDataStore().getLocalBucketById(i);
            if (br != null) {
              RowTupleConcurrentSkipListMap internalMap =
                      (RowTupleConcurrentSkipListMap) br.getRegionMap().getInternalMap();
              Map realMap = internalMap.getInternalMap();
              Iterator<Map.Entry<IMKey, RegionEntry>> itr = realMap.entrySet().iterator();
              while (itr.hasNext()) {
                Map.Entry<IMKey, RegionEntry> entry = itr.next();
                BlockKey blockKey = (BlockKey) entry.getKey();
                Object value = entry.getValue()._getValue();
                if (value == null || Token.isInvalidOrRemoved(value)) {
                  continue;
                }
                if (value instanceof VMCachedDeserializable) {
                  value = ((VMCachedDeserializable) value).getDeserializedForReading();
                }
                BlockValue blockValue = (BlockValue) value;
                int recordCountInBlock = blockValue.getCurrentIndex();
                actualCount += recordCountInBlock;
              }
            }
          }
          assertEquals(expectedCount, actualCount);
          return null;
        }
      });
    }
  }

  @Test
  public void testBlocksMultiNodeMultiBucket() {
    String tableName = "testBlocksMultiNodeMultiBucket";
    Exception e = null;

    FTable table = createFtable(tableName);
    table = MClientCacheFactory.getAnyInstance().getFTable(tableName);
    Random r = new Random();

    for (int j = 0; j < 399; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + r.nextInt(i + 1)));
      }
      table.append(record);
    }

    long start = TimestampUtil.getCurrentTime();


    for (int j = 0; j < 601; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE" + r.nextInt(i + 1)));
      }
      table.append(record);
    }

    long end = TimestampUtil.getCurrentTime();

    for (int j = 0; j < 1000; j++) {
      Record record = new Record();
      for (int i = 0; i < NUM_COLS; i++) {
        record.add("COL_" + i, Bytes.toBytes("COL_BEFORE_CHANGE" + r.nextInt(i + 1)));
      }
      table.append(record);
    }

    verifyRecordCountOnClient(tableName, 2000);
    verifyRecordCountOnServers(tableName, 2000);
  }
}
