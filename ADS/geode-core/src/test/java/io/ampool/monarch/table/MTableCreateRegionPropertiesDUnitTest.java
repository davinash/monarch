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

import org.apache.geode.cache.Region;
import io.ampool.internal.RegionDataOrder;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

@Category(MonarchTest.class)
public class MTableCreateRegionPropertiesDUnitTest extends MTableDUnitHelper {
  public MTableCreateRegionPropertiesDUnitTest() {
    super();
  }

  protected final int NUM_OF_COLUMNS = 10;
  protected final String TABLE_NAME = "ALLOPERATION";
  protected final String KEY_PREFIX = "KEY";
  protected final String VALUE_PREFIX = "VALUE";
  protected final int NUM_OF_ROWS = 10000;
  protected final String COLUMN_NAME_PREFIX = "COLUMN";

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

  private void createTable(final RegionDataOrder rdo, final int numOfBucket,
      final int redundantCopies) {
    MClientCache clientCache = MClientCacheFactory.getAnyInstance();
    MTableDescriptor tableDescriptor = new MTableDescriptor();

    if (rdo.equals(RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED)) {
      tableDescriptor = new MTableDescriptor(MTableType.ORDERED_VERSIONED);
    } else if (rdo.equals(RegionDataOrder.ROW_TUPLE_UNORDERED)) {
      tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
    }

    if (numOfBucket != 113) {
      tableDescriptor.setTotalNumOfSplits(numOfBucket);
    }

    tableDescriptor.setRedundantCopies(redundantCopies);

    for (int colmnIndex = 0; colmnIndex < NUM_OF_COLUMNS; colmnIndex++) {
      tableDescriptor = tableDescriptor.addColumn(Bytes.toBytes(COLUMN_NAME_PREFIX + colmnIndex));
    }


    Admin admin = clientCache.getAdmin();
    MTable table = admin.createTable(TABLE_NAME, tableDescriptor);
    assertEquals(table.getName(), TABLE_NAME);
  }

  private void createTableOn(VM vm, final RegionDataOrder rdo, final int numOfBucket,
      final int redundantCopies) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createTable(rdo, numOfBucket, redundantCopies);
        return null;
      }
    });
  }

  private void verifyRegionProperties(final RegionDataOrder rdo, final int totalNumOfBucket,
      final int redundantCopies) {
    MCache cache = MCacheFactory.getAnyInstance();
    Region r = cache.getRegion(TABLE_NAME);
    assertNotNull(r);
    PartitionedRegion pr = (PartitionedRegion) r;
    assertNotNull(pr);
    assertTrue(
        ((AmpoolTableRegionAttributes) pr.getCustomAttributes()).getRegionDataOrder().equals(rdo));
    // assertTrue(pr.getRegionDataOrder().equals(rdo));
    assertEquals(totalNumOfBucket, pr.getTotalNumberOfBuckets());
    assertEquals(redundantCopies, pr.getRedundantCopies());

  }

  private void verifyRegionPropertiesOn(VM vm, final RegionDataOrder rdo,
      final int totalNumOfBucket, final int redundantCopies) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        verifyRegionProperties(rdo, totalNumOfBucket, redundantCopies);
        return null;
      }
    });
  }

  @Test
  public void testCreateTableWithDefaultParameters() {
    RegionDataOrder rdo = RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED;
    int totalNumOfBuckets = 113;
    int numOfRedundantCopies = 1;
    createTableOn(this.client1, rdo, totalNumOfBuckets, numOfRedundantCopies);

    verifyRegionPropertiesOn(this.vm0, rdo, totalNumOfBuckets, numOfRedundantCopies);
    verifyRegionPropertiesOn(this.vm1, rdo, totalNumOfBuckets, numOfRedundantCopies);
    verifyRegionPropertiesOn(this.vm2, rdo, totalNumOfBuckets, numOfRedundantCopies);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testCreateTableWithCustomParameters() {
    RegionDataOrder rdo = RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED;
    int totalNumOfBuckets = 113;
    int numOfRedundantCopies = 3;
    createTableOn(this.client1, rdo, totalNumOfBuckets, numOfRedundantCopies);

    verifyRegionPropertiesOn(this.vm0, rdo, totalNumOfBuckets, numOfRedundantCopies);
    verifyRegionPropertiesOn(this.vm1, rdo, totalNumOfBuckets, numOfRedundantCopies);
    verifyRegionPropertiesOn(this.vm2, rdo, totalNumOfBuckets, numOfRedundantCopies);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testCreateTableWithCustomParameters1() {
    RegionDataOrder rdo = RegionDataOrder.ROW_TUPLE_ORDERED_VERSIONED;
    int totalNumOfBuckets = 100;
    int numOfRedundantCopies = 3;
    createTableOn(this.client1, rdo, totalNumOfBuckets, numOfRedundantCopies);

    verifyRegionPropertiesOn(this.vm0, rdo, totalNumOfBuckets, numOfRedundantCopies);
    verifyRegionPropertiesOn(this.vm1, rdo, totalNumOfBuckets, numOfRedundantCopies);
    verifyRegionPropertiesOn(this.vm2, rdo, totalNumOfBuckets, numOfRedundantCopies);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }

  @Test
  public void testCreateTableWithCustomParameters2() {
    RegionDataOrder rdo = RegionDataOrder.ROW_TUPLE_UNORDERED;
    int totalNumOfBuckets = 100;
    int numOfRedundantCopies = 3;
    createTableOn(this.client1, rdo, totalNumOfBuckets, numOfRedundantCopies);

    verifyRegionPropertiesOn(this.vm0, rdo, totalNumOfBuckets, numOfRedundantCopies);
    verifyRegionPropertiesOn(this.vm1, rdo, totalNumOfBuckets, numOfRedundantCopies);
    verifyRegionPropertiesOn(this.vm2, rdo, totalNumOfBuckets, numOfRedundantCopies);

    MClientCacheFactory.getAnyInstance().getAdmin().deleteTable(TABLE_NAME);
  }
}
