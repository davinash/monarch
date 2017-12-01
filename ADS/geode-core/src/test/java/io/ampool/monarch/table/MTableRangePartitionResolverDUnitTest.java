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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.TableInvalidConfiguration;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.InternalTable;
import io.ampool.monarch.table.internal.MKeyBase;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.table.region.map.RowTupleLRURegionMap;
import io.ampool.monarch.types.BasicTypes;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.BucketServerLocation66;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.MTableRangePartitionResolver;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MTableRangePartitionResolverDUnitTest extends MTableDUnitHelper {
  private final int NUM_OF_COLUMNS = 10;
  private final String TABLE_NAME = "MTableRangePartitionResolverDUnitTest";
  private final String KEY_PREFIX = "KEY";
  private final String VALUE_PREFIX = "VALUE";
  private final int NUM_OF_ROWS = 10000;
  private final String COLUMN_NAME_PREFIX = "COLUMN";

  private Random rand = new Random();

  public MTableRangePartitionResolverDUnitTest() {
    super();
  }

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
    super.tearDown2();
  }

  public void createTable(final boolean ordered, int numSplits, byte[] startRange,
      byte[] stopRange) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = null;
    if (ordered == false) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    } else {
      tableDescriptor = new MTableDescriptor();
    }
    tableDescriptor.setRedundantCopies(1).setTotalNumOfSplits(numSplits)
        .setStartStopRangeKey(startRange, stopRange);
    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }

    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
  }

  protected void createTableOn(VM vm, final boolean ordered, int numSplits, byte[] startRange,
      byte[] stopRange) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(ordered, numSplits, startRange, stopRange);
        return null;
      }
    });
  }

  private void verifyPartitionResolverPropertyOn(VM vm, final boolean isPartitionResolverAttached) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        Region r = cache.getRegion(TABLE_NAME);
        assertNotNull(r);
        RegionAttributes<?, ?> rattr = r.getAttributes();
        PartitionResolver<?, ?> pr = rattr.getPartitionAttributes().getPartitionResolver();
        if (isPartitionResolverAttached) {
          assertTrue(pr instanceof MTableRangePartitionResolver);
        } else {
          assertFalse(pr instanceof MTableRangePartitionResolver);
        }
        return null;
      }
    });
  }

  @Test
  public void testTableRegionProperties() {
    byte xFF = (byte) 0xFF;
    createTableOn(this.client1, true, 113, new byte[0], new byte[] {xFF});
    verifyPartitionResolverPropertyOn(this.vm0, true);
    verifyPartitionResolverPropertyOn(this.vm1, true);
    verifyPartitionResolverPropertyOn(this.vm2, true);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
    createTableOn(this.client1, false, 113, new byte[0], new byte[] {xFF});
    verifyPartitionResolverPropertyOn(this.vm0, false);
    verifyPartitionResolverPropertyOn(this.vm1, false);
    verifyPartitionResolverPropertyOn(this.vm2, false);
    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  private byte[] getRandomByteArray() {
    byte[] b = new byte[rand.nextInt((2048 - 10) + 1) + 10];
    rand.nextBytes(b);
    return b;
  }

  private void doPuts() {
    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
      byte[] key = getRandomByteArray();
      Put record = new Put(key);
      for (int columnIndex = 0; columnIndex < NUM_OF_COLUMNS; columnIndex++) {
        record.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + columnIndex),
            Bytes.toBytes(VALUE_PREFIX + columnIndex));
      }
      table.put(record);
    }
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

  private void verifyBucketDataOn(VM vm, byte[] startRangeKey, byte[] stopRangeKey) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {


        MCache cache = MCacheFactory.getAnyInstance();
        assertNotNull(cache);
        PartitionedRegion r = (PartitionedRegion) cache.getRegion(TABLE_NAME);
        assertNotNull(r);

        if (Bytes.compareTo(startRangeKey, stopRangeKey) >= 0) {
          throw new IllegalArgumentException("start of range must be <= end of range");
        }

        byte[][] splits =
            Bytes.split(startRangeKey, stopRangeKey, true, r.getTotalNumberOfBuckets() - 1);

        Map<Integer, Pair<byte[], byte[]>> splitsMap = new HashMap<>();

        for (int index = 1; index < splits.length; index++) {
          splitsMap.put(index - 1, new Pair<byte[], byte[]>(splits[index - 1], splits[index]));
        }


        Set<BucketRegion> bucketIds = r.getDataStore().getAllLocalBucketRegions();
        for (BucketRegion br : bucketIds) {
          RegionMap regionMap = br.entries;
          assertTrue(regionMap instanceof RowTupleLRURegionMap);
          assertTrue(regionMap.getInternalMap() instanceof RowTupleConcurrentSkipListMap);
          RowTupleConcurrentSkipListMap rowTupleConcurrentSkipListMap =
              (RowTupleConcurrentSkipListMap) regionMap.getInternalMap();
          Map cskl = rowTupleConcurrentSkipListMap.getInternalMap();
          assertNotNull(cskl);

          Set navigableKeySet = cskl.keySet();
          for (Iterator iterator = navigableKeySet.iterator(); iterator.hasNext();) {
            Object keyObject = iterator.next();
            assertTrue(keyObject instanceof IMKey);
            byte[] keyByteArray = IMKey.getBytes(keyObject);
            int expectedBucketId = MTableUtils.getBucketId(keyByteArray, splitsMap);

            assertEquals(expectedBucketId, br.getId());
          }
        }
        return null;
      }
    });
  }

  @Test
  public void testDataInVariousBuckets() {
    byte[] startRange = new byte[] {(byte) 0x00};
    byte[] stopRange = new byte[] {(byte) 0xFF};
    createTableOn(this.client1, true, 113, startRange, stopRange);
    doPutFrom(this.client1);

    verifyBucketDataOn(this.vm0, startRange, stopRange);
    verifyBucketDataOn(this.vm1, startRange, stopRange);
    verifyBucketDataOn(this.vm2, startRange, stopRange);

    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    byte[][] actualStartKeys = table.getMTableLocationInfo().getStartKeys();
    byte[][] actualEndKeys = table.getMTableLocationInfo().getEndKeys();

    // assertEquals(actualEndKeys.length, actualStartKeys.length);

  }

  @Test
  public void testDataInVariousBucketsWithRange() {
    byte[] startRange = new byte[] {(byte) 0x00};
    byte[] stopRange = new byte[] {(byte) 0xFF};

    createTableOn(this.client1, true, 4, startRange, stopRange);
    doPutFrom(this.client1);

    verifyBucketDataOn(this.vm0, startRange, stopRange);
    verifyBucketDataOn(this.vm1, startRange, stopRange);
    verifyBucketDataOn(this.vm2, startRange, stopRange);

    MTable table = MClientCacheFactory.getAnyInstance().getTable(TABLE_NAME);
    byte[][] actualStartKeys = table.getMTableLocationInfo().getStartKeys();
    byte[][] actualEndKeys = table.getMTableLocationInfo().getEndKeys();

    // assertEquals(actualEndKeys.length, actualStartKeys.length);

  }

  @Test
  public void testBasicRanges() {
    final int buckets = 10;
    final String prefix = "k";
    final Map<Integer, Pair<byte[], byte[]>> ks = new HashMap<>();
    byte[] key0 = null, key9 = null;
    byte[][] keys = new byte[buckets][];
    for (int i = 0; i < buckets; i++) {
      final byte[] f = (prefix + i).getBytes();
      final byte[] e = (prefix + i + "9").getBytes();
      if (i == 0)
        key0 = f;
      key9 = e;
      ks.put(i, new Pair<>(f, e));
      keys[i] = (prefix + i + "4").getBytes();
    }

    MTableRangePartitionResolver resolver =
        new MTableRangePartitionResolver(buckets, key0, key9, ks);
    for (int i = 0; i < buckets; i++) {
      assertEquals("Incorrect bucket id for key: " + BasicTypes.STRING.deserialize(keys[i]), i,
          resolver.getInternalRO(keys[i]));
    }
  }

  @Test
  public void testPartitionResolver() throws Exception {
    final String tableName = "test_part_resolver";
    final int buckets = 10;
    final String prefix = "k";
    final Map<Integer, Pair<byte[], byte[]>> ks = new HashMap<>();
    final IMKey[] keys = new IMKey[buckets];
    for (int i = 0; i < buckets; i++) {
      final byte[] f = (prefix + i).getBytes();
      final byte[] e = (prefix + i + "9").getBytes();
      keys[i] = new MKeyBase((prefix + i + "4").getBytes());
      ks.put(i, new Pair<>(f, e));
    }
    final MTableDescriptor td = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    td.setTotalNumOfSplits(buckets).setRedundantCopies(0).setKeySpace(ks)
        .setSchema(new Schema(new String[] {"c1", "c2"}));
    final SerializableCallableIF<List<ServerLocation>> getLocationsServer = () -> {
      MCache cache = MCacheFactory.getAnyInstance();
      final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(tableName);
      final Map<Integer, List<BucketServerLocation66>> map =
          pr.getRegionAdvisor().getAllClientBucketProfiles();
      final MTableRangePartitionResolver res =
          (MTableRangePartitionResolver) pr.getPartitionResolver();
      return Arrays.stream(keys).map(key -> {
        final BucketServerLocation66 bsl = map.get((int) res.getInternalRO(key.getBytes())).get(0);
        return new ServerLocation(bsl.getHostName(), bsl.getPort());
      }).collect(Collectors.toList());
    };
    final SerializableCallableIF<List<ServerLocation>> getLocationsClient = () -> {
      final LocalRegion region =
          (LocalRegion) ((InternalTable) MClientCacheFactory.getAnyInstance().getMTable(tableName))
              .getInternalRegion();
      final ClientMetadataService cms =
          ((MonarchCacheImpl) MCacheFactory.getAnyInstance()).getClientMetadataService();
      cms.getClientPRMetadata(region);
      return Arrays.stream(keys).map(key -> {
        return cms.getBucketServerLocation(region, Operation.GET, key, null, null);
      }).collect(Collectors.toList());
    };

    this.client1.invoke(() -> {
      final MClientCache cache = MClientCacheFactory.getAnyInstance();
      final MTable table = cache.getAdmin().createMTable(tableName, td);
      for (final IMKey key : keys) {
        Put put = new Put(key.getBytes());
        put.addColumn("c1", new byte[10]);
        put.addColumn("c2", new byte[10]);
        table.put(put);
      }
      assertEquals("Incorrect number of rows retrieved.", keys.length, table.getKeyCount());
    });
    final List<ServerLocation> c0List = getLocationsClient.call();
    final List<ServerLocation> c1List = this.client1.invoke(getLocationsClient);
    final List<ServerLocation> s1List = this.vm0.invoke(getLocationsServer);

    System.out.println("c0List = " + c0List);
    System.out.println("c1List = " + c1List);
    System.out.println("s1List = " + s1List);
    final ClientMetadataService cms =
        ((MonarchCacheImpl) MCacheFactory.getAnyInstance()).getClientMetadataService();
    final PartitionResolver resolver =
        cms.getClientPartitionAdvisor("/" + tableName).getPartitionResolver();

    for (int i = 0; i < buckets; i++) {
      assertEquals(
          "Incorrect bucket id for key: " + BasicTypes.STRING.deserialize(keys[i].getBytes()), i,
          resolver.getRoutingObject(new EntryEventImpl(keys[i])));
      assertEquals("[Client] Incorrect server location for bucket: " + i, c0List.get(i),
          c1List.get(i));
      assertEquals("[Server] Incorrect server location for bucket: " + i, s1List.get(i),
          c1List.get(i));
    }
  }

  @Test
  public void testInvalidFirstLastRangeKeysCreateTable() {

    byte[] stopRange = new byte[] {(byte) 0x00};
    byte[] startRange = new byte[] {(byte) 0xFF};

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    Exception e = null;
    try {
      tableDescriptor.setRedundantCopies(1).setStartStopRangeKey(startRange, stopRange);
    } catch (Exception ex) {
      e = ex;
    }
    assertTrue(e instanceof TableInvalidConfiguration);

    e = null;
    try {
      tableDescriptor.setRedundantCopies(1).setStartStopRangeKey(100, 0);
    } catch (Exception ex) {
      e = ex;
    }
    assertTrue(e instanceof TableInvalidConfiguration);

    e = null;
    try {
      tableDescriptor.setRedundantCopies(1).setStartStopRangeKey(100L, 0L);
    } catch (Exception ex) {
      e = ex;
    }
    assertTrue(e instanceof TableInvalidConfiguration);


    e = null;
    try {
      tableDescriptor.setRedundantCopies(1).setStartStopRangeKey(stopRange, startRange);
    } catch (Exception ex) {
      e = ex;
    }
    assertNull(e);
  }

  @Test
  public void testNegativeNumberOfSplits() {

    MTableDescriptor tableDescriptor = new MTableDescriptor();
    Exception e = null;
    try {
      tableDescriptor.setRedundantCopies(1).setTotalNumOfSplits(-1);
    } catch (Exception ex) {
      e = ex;
    }
    assertTrue(e instanceof IllegalArgumentException);

    e = null;
    try {
      tableDescriptor.setRedundantCopies(1).setTotalNumOfSplits(0);
    } catch (Exception ex) {
      e = ex;
    }
    assertTrue(e instanceof IllegalArgumentException);

    e = null;
    try {
      tableDescriptor.setRedundantCopies(1).setTotalNumOfSplits(113);
    } catch (Exception ex) {
      e = ex;
    }
    assertNull(e);
  }
}
