/*
 * /* Copyright (c) 2017 Ampool, Inc. All rights reserved.
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
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MKeyBase;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.EntryOperationImpl;
import org.apache.geode.internal.cache.MTableRangePartitionResolver;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

/**
 */
@Category(MonarchTest.class)
public class MTableBucketFunctionExecutionDUnitTest extends MTableDUnitHelper {
  public MTableBucketFunctionExecutionDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "MTableAllOpsClientServerDUnitTest";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 2;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int LATEST_TIMESTAMP = 300;
  protected final int MAX_VERSIONS = 5;
  protected final int TABLE_MAX_VERSIONS = 7;
  final int numOfEntries = 10;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    startServerOn(this.vm0, DUnitLauncher.getLocatorString());
    startServerOn(this.vm1, DUnitLauncher.getLocatorString());
    startServerOn(this.vm2, DUnitLauncher.getLocatorString());
    createClientCache(this.client1);
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
    closeMClientCache(client1);
    closeMCacheAll();
    super.tearDown2();
  }

  private class CreateTableUsingFE extends FunctionAdapter implements Declarable {

    @Override
    public void init(Properties props) {

    }

    @Override
    public void execute(FunctionContext context) {
      MCache mCache = MCacheFactory.getAnyInstance();
      MTableDescriptor tableDescriptor = new MTableDescriptor();
      for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
        tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
      }
      tableDescriptor.setRedundantCopies(1);
      Admin admin = mCache.getAdmin();
      MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
      assertEquals(table.getName(), TABLE_NAME);
      assertNotNull(table);
      context.getResultSender().lastResult(null);
    }

    @Override
    public String getId() {
      return this.getClass().getName();

    }
  }

  private void registerFunctionOn(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        FunctionService.registerFunction(new MTableGetKeysFE());
        FunctionService.registerFunction(new MTableGetKeysFE2());
        FunctionService.registerFunction(new RegionGetBucketKeysFunction());
        FunctionService.registerFunction(new LogRegionBucketsFunction());
        return null;
      }
    });
  }

  private void verifyBucketsOnServer(VM vm) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        Region region = cache.getRegion(TABLE_NAME);
        PartitionedRegion reg = (PartitionedRegion) region;
        List<Integer> bucketList = reg.getLocalBucketsListTestOnly();
        bucketList.forEach((K) -> {
          System.out.println("verifyBucketsOnServer - local bucket " + K);
        });

        reg.getLocalPrimaryBucketsListTestOnly().forEach((K) -> {
          System.out.println("verifyBucketsOnServer - Primary bucket - " + K);
        });
        return null;
      }
    });
  }

  private void runFunctionFromClient(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CreateTableUsingFE fte = new CreateTableUsingFE();
        FunctionService.registerFunction(fte);
        MClientCache cache = MClientCacheFactory.getAnyInstance();
        Execution members = FunctionService.onServers(((MonarchCacheImpl) cache).getDefaultPool());

        members.execute(fte.getId());

        /* Try Accesing the same Table on Client */
        MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNotNull(table);

        return null;
      }
    });
  }

  private void verifyTableAccessFromVM(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        assertNotNull(table);
        return null;
      }
    });
  }

  private void doPutsUsingFE() {
    MTablePutOPsFE tablePutOPsFE = new MTablePutOPsFE();
    FunctionService.registerFunction(tablePutOPsFE);
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    Execution members = FunctionService.onServers(((MonarchCacheImpl) cache).getDefaultPool());

    members.execute(tablePutOPsFE.getId());

  }

  private class MTablePutOPsFE extends FunctionAdapter implements Declarable {

    @Override
    public void init(Properties props) {

    }

    @Override
    public void execute(FunctionContext context) {
      MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
      for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
        Put record = new Put(Bytes.toBytes(KEY_PREFIX + rowIndex));
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        table.put(record);
      }
      context.getResultSender().lastResult(null);
    }

    @Override
    public String getId() {
      return this.getClass().getName();

    }
  }

  private class MTableGetKeysFE extends FunctionAdapter implements Declarable {

    @Override
    public void init(Properties props) {

    }

    @Override
    public void execute(FunctionContext context) {
      synchronized (this) {
        RegionFunctionContext contx = (RegionFunctionContext) context;
        try {
          Region region = PartitionRegionHelper.getLocalDataForContext(contx);
          System.out.println("Function.onRegionBucket In bucket function execution ");
          System.out.println("Function.onRegionBucket filters - " + contx.getFilter());
          // Remove later
          // PartitionedRegion pr = (PartitionedRegion) contx.getDataSet();

          Set<IMKey> mtableKsys = new HashSet<>();
          extracted(region, mtableKsys);

          // if (pr != null) {
          // Set<BucketRegion> allLocalBuckets =
          // pr.getDataStore().getAllLocalBucketRegions();
          // System.out.println("Default total buckets in server - " +
          // allLocalBuckets.size());
          // for (BucketRegion eachLocalBucket : allLocalBuckets) {
          // if (eachLocalBucket.getBucketAdvisor().isPrimary() &&
          // eachLocalBucket.size() != 0) {
          // System.out.println("Default primary local bucket - " +
          // eachLocalBucket.getId());
          // for (Object key : eachLocalBucket.keys()) {
          // mtableKsys.add((IMKey) key);
          // }
          // }
          // }
          // }
          // remove

          // Set<Object> keys = pr.keys();
          // // System.out.println("Keys in geode region for table - "+
          // // keys.size());
          // int setSize = keys.size();
          // Iterator keysIterator = keys.iterator();
          // for (int i = 1; i <= setSize; i++) {
          // IMKey key = (IMKey) keysIterator.next();
          // mtableKsys.add(key);
          // }
          System.out.println("Function.onRegionBucket sending keys - " + mtableKsys.size());

          context.getResultSender().lastResult(mtableKsys);

        } catch (GemFireException re) {
        }
      }
    }

    private void extracted(Region pr, Set<IMKey> mtableKsys) {
      mtableKsys.addAll(pr.keySet());
      // pr.keys().forEach((K) -> {
      // mtableKsys.add((IMKey) K);
      // });
    }

    @Override
    public String getId() {
      return this.getClass().getName();

    }
  }

  private class MTableGetKeysFE2 extends FunctionAdapter implements Declarable {

    @Override
    public void init(Properties props) {

    }

    @Override
    public void execute(FunctionContext context) {
      synchronized (this) {

        RegionFunctionContext contx = (RegionFunctionContext) context;
        Map<Integer, List<IMKey>> bucketToKeys = new HashMap<>();
        try {
          MCache cache = MCacheFactory.getAnyInstance();

          // PartitionedRegion pr = (PartitionedRegion)
          // cache.getRegion(regionName);

          PartitionedRegion pr = (PartitionedRegion) contx.getDataSet();
          Set<Integer> bucketFilter = (Set<Integer>) contx.getFilter();
          System.out.println("Function.onRegionBucket2 bucket filter " + bucketFilter);

          if (pr != null) {
            Set<BucketRegion> allLocalBuckets = pr.getDataStore().getAllLocalBucketRegions();
            for (BucketRegion eachLocalBucket : allLocalBuckets) {
              if (eachLocalBucket.getBucketAdvisor().isPrimary() && eachLocalBucket.size() != 0) {
                // if (bucketFilter != null &&
                // !bucketFilter.contains(eachLocalBucket.getId())) {
                // System.out.println("Function.onRegionBucket2 skipping bucket
                // id -" + eachLocalBucket.getId());
                // continue;
                // }
                for (Object key : eachLocalBucket.keys()) {
                  if (bucketToKeys.containsKey(eachLocalBucket.getId())) {
                    List<IMKey> keys = bucketToKeys.get(eachLocalBucket.getId());
                    if (keys.contains(key)) {
                      System.out.println("ERROR!!");
                    }
                    keys.add((IMKey) key);
                  } else {
                    List<IMKey> keys = new ArrayList<>();
                    keys.add((IMKey) key);
                    bucketToKeys.put(eachLocalBucket.getId(), keys);
                  }
                }

              }

            }
          }
        } catch (GemFireException re) {
        }
        context.getResultSender().lastResult(bucketToKeys);
      }

    }

    private void extracted(Region pr, Set<IMKey> mtableKsys) {
      pr.keys().forEach((K) -> {
        mtableKsys.add((IMKey) K);
      });
    }

    @Override
    public String getId() {
      return this.getClass().getName();

    }
  }

  private class MTableGetBucketWiseKeysFE extends FunctionAdapter implements Declarable {

    @Override
    public void init(Properties props) {

    }

    @Override
    public void execute(FunctionContext context) {

      RegionFunctionContext contx = (RegionFunctionContext) context;
      try {
        MCache cache = MCacheFactory.getAnyInstance();
        Region region = PartitionRegionHelper.getLocalDataForContext(contx);

        Set<Object> keys = region.keys();
        int setSize = keys.size();
        Iterator keysIterator = keys.iterator();
        Set<IMKey> mtableKsys = new HashSet<>();
        for (int i = 1; i < (setSize - 1); i++) {
          IMKey key = (IMKey) keysIterator.next();
          mtableKsys.add(key);
        }
        context.getResultSender().lastResult(mtableKsys);

      } catch (GemFireException re) {
      }
    }

    @Override
    public String getId() {
      return this.getClass().getName();

    }
  }

  class RegionGetBucketKeysFunction extends FunctionAdapter implements Declarable {

    @Override
    public void execute(FunctionContext context) {
      synchronized (this) {

        RegionFunctionContext contx = (RegionFunctionContext) context;
        Map<Integer, Set<IMKey>> bucketToKeys = new HashMap<>();
        try {
          MCache cache = MCacheFactory.getAnyInstance();

          // PartitionedRegion pr = (PartitionedRegion)
          // Region region = cache.getRegion(TABLE_NAME);
          Region region = PartitionRegionHelper.getLocalDataForContext(contx);
          System.out.println("Function.onReion filter - " + contx.getFilter());
          Set<IMKey> keys = new HashSet<>();
          region.keySet().forEach((K) -> {
            keys.add((IMKey) K);
          });

          bucketToKeys.put(1, keys);

          // Region pr = PartitionRegionHelper.getLocalData(region);
          // PartitionedRegion pr = (PartitionedRegion) contx.getDataSet();
          // System.out
          // .println("Number of buckets - " + pr.getTotalNumberOfBuckets());
          // if (pr != null) {
          // Set<BucketRegion> allLocalBuckets = pr.getDataStore()
          // .getAllLocalBucketRegions();
          // System.out.println("Function.onReion total buckets in server - "
          // + allLocalBuckets.size());
          // for (BucketRegion eachLocalBucket : allLocalBuckets) {
          // if (eachLocalBucket.getBucketAdvisor().isPrimary()
          // && eachLocalBucket.size() != 0) {
          // for (Object key : eachLocalBucket.keys()) {
          // if (bucketToKeys.containsKey(eachLocalBucket.getId())) {
          // Set<IMKey> keys = bucketToKeys
          // .get(eachLocalBucket.getId());
          // if (keys.contains((IMKey) key)) {
          // System.out.println("ERROR!!");
          // } else {
          // keys.add((IMKey) key);
          // }
          // } else {
          // System.out.println("Function.onReion local bucket - "
          // + eachLocalBucket.getId());
          // Set<IMKey> keys = new HashSet<>();
          // keys.add((IMKey) key);
          // bucketToKeys.put(eachLocalBucket.getId(), keys);
          // }
          // }
          //
          // }
          //
          // }
          // }
        } catch (GemFireException re) {
        }
        int totalMKeys = 0;
        for (Set<IMKey> values : bucketToKeys.values()) {
          totalMKeys += values.size();
        }
        System.out.println("Function.onRegion server send keys - " + totalMKeys);
        context.getResultSender().lastResult(bucketToKeys);
      }

    }

    @Override
    public String getId() {
      return this.getClass().getName();
    }

    @Override
    public void init(Properties props) {
      // TODO Auto-generated method stub

    }
  }

  class LogRegionBucketsFunction extends FunctionAdapter implements Declarable {

    @Override
    public void execute(FunctionContext context) {
      synchronized (this) {

        RegionFunctionContext contx = (RegionFunctionContext) context;
        Map<Integer, Set<IMKey>> bucketToKeys = new HashMap<>();
        try {
          MCache cache = MCacheFactory.getAnyInstance();

          PartitionedRegion pr = (PartitionedRegion) contx.getDataSet();

          if (pr != null) {
            Set<BucketRegion> allLocalBuckets = pr.getDataStore().getAllLocalBucketRegions();
            System.out.println(
                "LogRegionBucketsFunction - total buckets in server - " + allLocalBuckets.size());
            for (BucketRegion eachLocalBucket : allLocalBuckets) {
              if (eachLocalBucket.getBucketAdvisor().isPrimary()) {
                System.out.println("LogRegionBucketsFunction - primary bucket "
                    + eachLocalBucket.getId() + " size" + eachLocalBucket.keySet().size());
              }

            }
          }
        } catch (GemFireException re) {
        }
        context.getResultSender().lastResult(null);
      }

    }

    @Override
    public String getId() {
      return this.getClass().getName();
    }

    @Override
    public void init(Properties props) {
      // TODO Auto-generated method stub

    }
  }

  private void doGets() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
      Row result = table.get(get);
      assertEquals(NUM_OF_COLUMNS, result.size());
      assertFalse(result.isEmpty());
      int columnIndex = 0;
      List<Cell> row = result.getCells();
      for (Cell cell : row) {
        Assert.assertNotEquals(10, columnIndex);
        byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
        byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

        if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
          System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
          System.out.println("actualColumnName   => " + Arrays.toString(cell.getColumnName()));
          Assert.fail("Invalid Values for Column Name");
        }
        if (!Bytes.equals(exptectedValue, (byte[]) cell.getColumnValue())) {
          System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
          System.out
              .println("actualValue    => " + Arrays.toString((byte[]) cell.getColumnValue()));
          Assert.fail("Invalid Values for Column Value");
        }
        columnIndex++;
      }
    }
  }

  public void doVerifyGetsOnAllVMs(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
        for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
          Get get = new Get(Bytes.toBytes(KEY_PREFIX + rowIndex));
          Row result = table.get(get);
          assertEquals(NUM_OF_COLUMNS, result.size());
          assertFalse(result.isEmpty());
          int columnIndex = 0;
          List<Cell> row = result.getCells();
          for (Cell cell : row) {
            Assert.assertNotEquals(10, columnIndex);
            byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
            byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

            if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
              System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
              System.out.println("actualColumnName   => " + Arrays.toString(cell.getColumnName()));
              Assert.fail("Invalid Values for Column Name");
            }
            if (!Bytes.equals(exptectedValue, (byte[]) cell.getColumnValue())) {
              System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
              System.out
                  .println("actualValue    => " + Arrays.toString((byte[]) cell.getColumnValue()));
              Assert.fail("Invalid Values for Column Value");
            }
            columnIndex++;
          }
        }

        return null;
      }
    });
  }

  protected void createTable(int noOfPartitions) {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
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

  protected void createTableFrom(VM vm, int noOfPartitions) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(noOfPartitions);
        return null;
      }
    });
  }

  private void doPutFromClient(VM vm, String tableName, List<byte[]> keys) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {

          MTable mtable = MClientCacheFactory.getAnyInstance().getTable(tableName);
          Random randomGenerator = new Random();
          for (int i = 0; i < keys.size(); i++) {
            Put myput1 = new Put(keys.get(i));
            myput1.addColumn(Bytes.toBytes("NAME"), Bytes.toBytes("User" + i));
            myput1.addColumn(Bytes.toBytes("ID"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("AGE"), Bytes.toBytes(i + 10));
            myput1.addColumn(Bytes.toBytes("SALARY"), Bytes.toBytes(i + 10));

            mtable.put(myput1);
          }

        } catch (CacheClosedException cce) {
        }

        return null;
      }
    });
  }

  void updatePutKeys(List<byte[]> totalPutKeys, byte[] key) {
    totalPutKeys.add(key);
  }

  private Map<Integer, List<byte[]>> doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);

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
        table.put(record);
        totalPutKeys++;
      }
    }

    assertEquals(table.getTableDescriptor().getTotalNumOfSplits() * NUM_OF_ROWS, totalKeyCount);
    return allKeys;
  }

  private Object doPutFrom(VM vm) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return doPuts();
      }
    });
  }

  private void doGetFromClient(String tableName, List<byte[]> keys, final boolean order) {
    try {

      MTable mtable = MClientCacheFactory.getAnyInstance().getTable(tableName);
      for (int rowIndex = 0; rowIndex < keys.size(); rowIndex++) {
        Get get = new Get(keys.get(rowIndex));
        Row result = mtable.get(get);
        assertEquals(NUM_OF_COLUMNS, result.size());
        assertFalse(result.isEmpty());
        int columnIndex = 0;
        List<Cell> row = result.getCells();
        for (Cell cell : row) {
          Assert.assertNotEquals(10, columnIndex);
          byte[] expectedColumnName = Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex);
          byte[] exptectedValue = Bytes.toBytes(VALUE_PREFIX + columnIndex);

          if (!Bytes.equals(expectedColumnName, cell.getColumnName())) {
            System.out.println("expectedColumnName => " + Arrays.toString(expectedColumnName));
            System.out.println("actualColumnName   => " + Arrays.toString(cell.getColumnName()));
            Assert.fail("Invalid Values for Column Name");
          }
          if (!Bytes.equals(exptectedValue, (byte[]) cell.getColumnValue())) {
            System.out.println("exptectedValue => " + Arrays.toString(exptectedValue));
            System.out
                .println("actualValue    => " + Arrays.toString((byte[]) cell.getColumnValue()));
            Assert.fail("Invalid Values for Column Value");
          }
          columnIndex++;
        }
      }

    } catch (CacheClosedException cce) {
    }
  }

  private void doGets(Map<Integer, List<byte[]>> keys) {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    assertNotNull(table);

    keys.forEach((K, V) -> {
      V.forEach((X) -> {
        Get get = new Get(X);
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

    });
  }

  @Test
  public void testBucketWiseKeysGeneration() {
    Map<Integer, List<byte[]>> allKeys = getBucketToKeysForAllBuckets(10, 10);
    int totalKeysCount = 0;
    Set<ByteBuffer> keySet = new HashSet<>();
    MTableRangePartitionResolver pr = new MTableRangePartitionResolver(10);
    for (Map.Entry<Integer, List<byte[]>> entry : allKeys.entrySet()) {
      System.out.println(
          "generate bucket id - " + entry.getKey() + " bucket size " + entry.getValue().size());
      totalKeysCount += entry.getValue().size();
      entry.getValue().forEach((X) -> {
        IMKey mKey = new MKeyBase(X);
        EntryOperationImpl op = new EntryOperationImpl(null, null, mKey, null, null);

        int bucketId = (int) pr.getRoutingObject(op);
        assertEquals(entry.getKey().intValue(), bucketId);

        if (keySet.contains(ByteBuffer.wrap(X))) {
          System.out.println("ERROR!! duplicate key found");
        }
        keySet.add(ByteBuffer.wrap(X));
      });
    }

    assertEquals(100, totalKeysCount);
  }

  @Test
  public void testMTableScanKeysThroughFE() {
    String MTABLE_NAME = "mtable";
    registerFunctionOn(this.vm0);
    registerFunctionOn(this.vm1);
    registerFunctionOn(this.vm2);

    int noOfBuckets = 10;
    createTableFrom(this.client1, noOfBuckets);

    Map<Integer, List<byte[]>> bucketWiseKeys = doPuts();
    doGets(bucketWiseKeys);

    // For function execution with default onRegion execution would result
    // with result count of cache servers
    verifyScanFunExecutionOnClient(this.client1, noOfBuckets * NUM_OF_ROWS);
  }

  public void startClient() {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTable mTable = clientCache.getTable(TABLE_NAME);
    assertNotNull(mTable);
  }

  @Test
  public void testMTableBucketScanKeysThroughFE() {
    registerFunctionOn(this.vm0);
    registerFunctionOn(this.vm1);
    registerFunctionOn(this.vm2);

    int noOfBuckets = 113;
    createTableFrom(this.client1, noOfBuckets);
    Map<Integer, List<byte[]>> bucketWiseKeys = doPuts();
    doGets(bucketWiseKeys);

    // verify region primary buckets through function execution
    // logRegionBucketKeys(this.client1);

    // Get bucket wise keys
    getBucketWiseKeys(this.client1, noOfBuckets * NUM_OF_ROWS);

    List<Integer> noResultBuckets = new ArrayList<>();
    startClient();
    // Call bucket wise function of specific buckets
    MTable table = MCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    bucketWiseKeys.forEach((K, V) -> {
      if (!verifyBucketWiseFunExe(K, V, table)) {
        noResultBuckets.add(K);
      }
    });

    System.out.println("failed for buckets - " + noResultBuckets);
    assertTrue(noResultBuckets.isEmpty());
  }

  private void logRegionBucketKeys(VM client12) {
    client12.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MonarchCacheImpl c = ((MonarchCacheImpl) MCacheFactory.getAnyInstance());
        Region region = c.getRegion(TABLE_NAME);
        Function fun = new LogRegionBucketsFunction();
        FunctionService.registerFunction(fun);
        FunctionService.onRegion(region).execute(fun.getId()).getResult();
        return null;
      }
    });
  }

  private void getBucketWiseKeys(VM client12, int expectedTotalRowCount) {
    client12.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MonarchCacheImpl c = ((MonarchCacheImpl) MCacheFactory.getAnyInstance());
        Region region = c.getRegion(TABLE_NAME);
        Function fun = new RegionGetBucketKeysFunction();
        FunctionService.registerFunction(fun);
        List<Map<Integer, Set<IMKey>>> orignalResult =
            (List<Map<Integer, Set<IMKey>>>) FunctionService.onRegion(region).execute(fun.getId())
                .getResult();

        System.out.println("Function.onRegion - Results received - " + orignalResult.size());

        Map<Integer, Set<IMKey>> allBucketToKeys = combineResults(orignalResult);

        // assertTrue(allBucketToKeys.keySet().size() <= 113);
        int totalKeysCount = 0;
        for (Map.Entry<Integer, Set<IMKey>> val : allBucketToKeys.entrySet()) {
          System.out.println(
              "Function.onRegion bucket key " + val.getKey() + " value " + val.getValue().size());
          totalKeysCount = totalKeysCount + val.getValue().size();
        }
        assertEquals(expectedTotalRowCount, totalKeysCount);
        return null;
      }
    });
  }

  private Map<Integer, Set<IMKey>> combineResults(List<Map<Integer, Set<IMKey>>> result) {
    Map<Integer, Set<IMKey>> allBucketToKeys = new HashMap<>();
    for (Map<Integer, Set<IMKey>> res : result) {
      for (Map.Entry<Integer, Set<IMKey>> entry : res.entrySet()) {
        if (allBucketToKeys.containsKey(entry.getKey())) {
          Set<IMKey> keys = allBucketToKeys.get(entry.getKey());
          keys.addAll(entry.getValue());
        } else {
          allBucketToKeys.put(entry.getKey(), entry.getValue());
        }
      }
    }
    return allBucketToKeys;
  }

  private boolean verifyBucketWiseFunExe(Integer bucketId, List<byte[]> v, MTable table) {

    MTableGetKeysFE mTableGetKeysFE = new MTableGetKeysFE();
    FunctionService.registerFunction(mTableGetKeysFE);

    Execution execution = FunctionService.onMTable(table).withTableSplitId(bucketId);

    ResultCollector rc = execution.execute(mTableGetKeysFE);


    List<HashSet<IMKey>> results = (List<HashSet<IMKey>>) rc.getResult();
    // List<Map<Integer, List<IMKey>>> results = (List<Map<Integer,
    // List<IMKey>>>) rc.getResult();
    // Expecting single result count as all all results are wrapped in
    // collection & only 1 primary bucket should send results
    if (!results.isEmpty()) {
      assertEquals(1, results.size());

      // verifying just keys results
      assertEquals(v.size(), results.get(0).size());
      // Verification for bucket wise keys results

      // assertEquals(1, results.get(0).size());
      // assertEquals(v.size(), results.get(0).get(k).size());
      System.out.println("Results matched for bucketId -" + bucketId);
      return true;
    } else {
      System.out.println("No results retrived for bucketId - " + bucketId);
      return false;
    }
  }

  private boolean keysMatch(List<byte[]> expected, HashSet<IMKey> resultKeys) {
    Set<IMKey> keys = new HashSet<>();
    expected.forEach((K) -> {
      keys.add(new MKeyBase(K));
    });

    for (IMKey key : resultKeys) {
      if (!keys.contains(key)) {
        return false;
      }
    }

    return true;
  }

  public void verifyScanFunExecutionOnClient(VM vm, int totalRows) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyDefaultFunExecution(totalRows);
        return null;
      }
    });
  }

  public void verifyDefaultFunExecution(int totalRows) {
    MonarchCacheImpl c = ((MonarchCacheImpl) MCacheFactory.getAnyInstance());
    Region region = c.getRegion(TABLE_NAME);
    // ServerRegionProxy regionProxy = new ServerRegionProxy(region);

    Function fun = new MTableGetKeysFE();
    FunctionService.registerFunction(fun);
    List<HashSet<IMKey>> results =
        (List<HashSet<IMKey>>) FunctionService.onRegion(region).execute(fun.getId()).getResult();
    int totalKeysCount = 0;
    for (HashSet<IMKey> keys : results) {
      totalKeysCount += keys.size();
    }
    System.out.println("Result received from servers - " + results.size());
    assertEquals(totalRows, totalKeysCount);
  }

  public void verifyFunExecutionOnBucket(int noOfBuckets) {
    MonarchCacheImpl c = ((MonarchCacheImpl) MCacheFactory.getAnyInstance());
    Region region = c.getRegion(TABLE_NAME);
    ServerRegionProxy regionProxy = new ServerRegionProxy(region);

    Function fun = new MTableGetKeysFE();
    FunctionService.registerFunction(fun);
    List<HashSet<IMKey>> results =
        (List<HashSet<IMKey>>) FunctionService.onRegion(region).execute(fun.getId()).getResult();

    assertEquals(noOfBuckets, results.size());
  }

  public void verifyParallelBucketScanFunExecution() {

  }

}
