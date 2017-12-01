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



import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MTableLocationInfo;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import java.util.*;

@Category(MonarchTest.class)
public class MTableLocationInfoDUnitTest extends MTableDUnitHelper {
  public MTableLocationInfoDUnitTest() {
    super();
  }

  protected static final Logger logger = LogService.getLogger();

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "MTableBucketIDServerLocationDUnitTest";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 10;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";
  protected final int LATEST_TIMESTAMP = 300;
  protected final int MAX_VERSIONS = 5;
  protected final int TABLE_MAX_VERSIONS = 7;
  protected final int NUM_BUCKETS = 113;
  private static final Map<String, Integer> vmToPortMap = new HashMap<>();
  private static final Map<Integer, byte[]> bucketIdToOneRowKeyMap = new HashMap<>();

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    vmToPortMap.put("vm0", (Integer) startServerOn(this.vm0, DUnitLauncher.getLocatorString()));
    vmToPortMap.put("vm1", (Integer) startServerOn(this.vm1, DUnitLauncher.getLocatorString()));
    vmToPortMap.put("vm2", (Integer) startServerOn(this.vm2, DUnitLauncher.getLocatorString()));
    createClientCache(this.client1);
    createClientCache();
  }

  @Override
  public void tearDown2() throws Exception {
    closeMClientCache();
    closeMClientCache(client1);
    super.tearDown2();
  }

  private void createTableClient() {

    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }
    tableDescriptor.setTotalNumOfSplits(NUM_BUCKETS);
    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
    assertNotNull(table);
  }

  private void createTableFromClient(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTableClient();
        return null;
      }
    });
  }

  private void doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    Map<Integer, Pair<byte[], byte[]>> splitsMap = MTableUtils.getUniformKeySpaceSplit(NUM_BUCKETS);

    Set<ByteArrayKey> inputStartKeys = new HashSet<>();
    Set<ByteArrayKey> inputEndKeys = new HashSet<>();

    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId++) {
      Pair<byte[], byte[]> pair = splitsMap.get(bucketId);
      List<byte[]> keysInRange = getKeysInRange(pair.getFirst(), pair.getSecond(), NUM_OF_ROWS);
      bucketIdToOneRowKeyMap.put(bucketId, keysInRange.get(bucketId % NUM_OF_ROWS));
      keysInRange.forEach((K) -> {
        Put record = new Put(K);
        for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
          record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
              Bytes.toBytes(VALUE_PREFIX + columnIndex));
        }
        table.put(record);
      });

    }
    Assert.assertTrue(bucketIdToOneRowKeyMap.size() == NUM_BUCKETS);
  }

  private void doPutFrom(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doPuts();
        return null;
      }
    });
  }

  private void verifyBucketActuallyMoved(
      final Map<String, ServerLocation> oldServerLocationOfMovedBucket) {
    this.vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        PartitionedRegion pr = (PartitionedRegion) cache.getRegion(TABLE_NAME);
        assertNotNull(pr);

        oldServerLocationOfMovedBucket.forEach((ID, SL) -> {

          String[] keyPart = ID.split(":");
          assertTrue(keyPart.length == 2);

          Bucket bucket = pr.getRegionAdvisor().getBucket(Integer.valueOf(keyPart[1]));

          assertFalse("VM0 member is still hosting moved bucket", bucket.isHosting());

          BucketRegion bucketRegion =
              bucket.getBucketAdvisor().getProxyBucketRegion().getHostedBucketRegion();

          assertNull("BucketRegion is not null on VM0 member", bucketRegion);
        });

        return null;
      }
    });
  }

  @Test
  public void testLocationInfo() {
    // create table from client-1
    createTableFromClient(this.client1);
    /* put from client 2 */
    MClientCache cache = MClientCacheFactory.getAnyInstance();
    assertNotNull(cache);
    MTable table = cache.getTable(TABLE_NAME);
    assertNotNull(table);
    doPuts();

    MTableLocationInfo mTableLocationInfo = table.getMTableLocationInfo();
    assertNotNull(mTableLocationInfo);
    List<MServerLocation> allMTableLocations = mTableLocationInfo.getAllMTableLocations();
    assertNotNull(allMTableLocations);
    assertTrue(allMTableLocations.size() == NUM_BUCKETS);
    for (MServerLocation mServerLocation : allMTableLocations) {
      assertNotNull(mServerLocation);
      assertNotNull(mServerLocation.getHostName());
      assertNotNull(mServerLocation.getPort());
      assertNotNull(mServerLocation.getBucketId());
      assertNotNull(mServerLocation.getStartKey());
      assertNotNull(mServerLocation.getEndKey());
    }

    /* verify the meta region on client-2 */
    // Region metaRegion = MTableUtils.getMetaTableBktIdToSLocRegion(true);

    String[] bucketsToMov = new String[5];
    /* Get some random bucket Ids from VM0 to make Move to another node */
    String[] bucketMovedKeys = (String[]) this.vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        PartitionedRegion pr =
            (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(TABLE_NAME);
        Set<BucketRegion> allLocalBucketRegions =
            pr.getDataStore().getAllLocalPrimaryBucketRegions();

        int index = 0;
        for (BucketRegion BR : allLocalBucketRegions) {
          if (index >= 5) {
            break;
          }
          if (pr.getDataStore().isManagingBucket(BR.getId())) {
            bucketsToMov[index++] = MTableUtils.getTableToBucketIdKey(TABLE_NAME, BR.getId());
          }
        }
        return bucketsToMov;
      }
    });

    /*
     * Make sure that none of the bucket id is null If that is the case then we need to change test
     * logic of having 5 bucket movement.
     */
    Map<String, ServerLocation> oldSlOfBucketsTobeMoved = new HashMap<>();
    for (String bucketId : bucketMovedKeys) {
      assertNotNull(bucketId);
      /* store the old bucket id and its server location */
      // oldSlOfBucketsTobeMoved.put(bucketId,(ServerLocation) metaRegion.get(bucketId));
    }

    /* Perform Bucket Move from vm1 */

    final InternalDistributedMember vm1MemberId =
        (InternalDistributedMember) vm0.invoke(new SerializableCallable() {
          public Object call() throws Exception {
            return InternalDistributedSystem.getAnyInstance().getDistributedMember();
          }
        });

    /* Move buckets from vm0 to vm1 */
    this.vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        PartitionedRegion pr =
            (PartitionedRegion) MCacheFactory.getAnyInstance().getRegion(TABLE_NAME);
        for (int i = 0; i < bucketsToMov.length; i++) {
          String[] keys = bucketMovedKeys[i].split(":");
          System.out.println("MTableLocationInfoDUnitTest.call:: Moving Bucket:" + keys[1]);
          assertTrue(pr.getDataStore().moveBucket(Integer.valueOf(keys[1]), vm1MemberId, true));
        }
        return null;
      }
    });

    // now query the meta region for the new bucket-ids.
    for (int i = 0; i < table.getTableDescriptor().getTotalNumOfSplits(); i++) {
      // assertNotNull(metaRegion.get(MTableUtils.getTableToBucketIdKey(TABLE_NAME, i)));
    }
    // search the old bucketId-ServerLocation in new
    // They should not match, since the bucket is moved
    // and our logic to update should have executed.

    verifyBucketActuallyMoved(oldSlOfBucketsTobeMoved);

    /* Verify the actual value is changed or not */
    oldSlOfBucketsTobeMoved.forEach((KEY, SL) -> {
      ServerLocation oldSL = SL;
      // ServerLocation newSL = (ServerLocation) metaRegion.get(KEY);
      System.out.println("Old server location - " + oldSL);
      // System.out.println("New server location - " + newSL);
      // assertFalse(oldSL.equals(newSL));

    });

    /* Verifying if rowkey from moved buckets shows server location as vm1 from client */
    verifyLocationInfo(mTableLocationInfo, bucketMovedKeys, vmToPortMap, bucketIdToOneRowKeyMap);

    /* Verifying if rowkey from moved buckets shows server location as vm1 from client 1 */
    verifyLocationInfoFromVM(this.client1, bucketMovedKeys, vmToPortMap, bucketIdToOneRowKeyMap);

    /*
     * Verifying server location from peer We are using CMS so this is invalid now GEN-740
     */
    // verifyLocationInfoFromVM(this.vm2, bucketMovedKeys, vmToPortMap, bucketIdToOneRowKeyMap);

  }

  private void verifyLocationInfoFromVM(VM vm, final String[] bucketMovedKeys,
      Map<String, Integer> vmToPortMap, Map<Integer, byte[]> bucketIdToOneRowKeyMap) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        MTable table = cache.getTable(TABLE_NAME);
        assertNotNull(table);
        MTableLocationInfo mTableLocationInfo = table.getMTableLocationInfo();
        assertNotNull(mTableLocationInfo);
        verifyLocationInfo(mTableLocationInfo, bucketMovedKeys, vmToPortMap,
            bucketIdToOneRowKeyMap);
        return null;
      }
    });
  }

  private void verifyLocationInfo(MTableLocationInfo mTableLocationInfo, String[] bucketMovedKeys,
      Map<String, Integer> vmToPortMap, Map<Integer, byte[]> bucketIdToOneRowKeyMap) {
    for (String bucketKeys : bucketMovedKeys) {
      int bucketid = Integer.valueOf(bucketKeys.split(":")[1]);
      byte[] rowkey = bucketIdToOneRowKeyMap.get(bucketid);
      assertNotNull(rowkey);
      MServerLocation mTableLocation = mTableLocationInfo.getMTableLocation(rowkey);
      int port = vmToPortMap.get("vm1");
      assertTrue(mTableLocation.getPort() == port);
      assertTrue(mTableLocation.getBucketId() == bucketid);
    }
  }
}
