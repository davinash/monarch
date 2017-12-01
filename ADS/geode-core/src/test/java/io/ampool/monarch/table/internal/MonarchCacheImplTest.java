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

package io.ampool.monarch.table.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.ampool.conf.Constants;
import io.ampool.monarch.AbstractTestDUnitBase;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MCacheInternalErrorException;
import io.ampool.monarch.table.exceptions.MScanFailedException;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.types.TypeUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;

@Category(MonarchTest.class)
public class MonarchCacheImplTest extends AbstractTestDUnitBase implements Serializable {
  private static final String SCHEMA = "{'c1':'INT','c2':'array<LONG>','c3':'STRING'}";
  private static final String TABLE_NAME_PFX = "dummy_table_";

  /**
   * The operations to be executed on the table(s). - CREATE_TABLES : create all (specified number
   * of) tables via client cache; MTable and FTable each - ACCESS_TABLES : access all (specified
   * number of) tables via client cache - DELETE_TABLES : delete all (specified number of) tables
   * via client cache
   *
   * Following entry operations on all the specified number of tables with the prefix. While
   * performing the below operations also assert that the exception is thrown with root-cause
   * RegionDestroyedException. - PUT : Put dummy entries in each table (both single and batch) - GET
   * : Get dummy entries from each table (both single and batch) - SCAN : Scan all the tables -
   * DELETE : Delete dummy entries from each table (only single delete at the moment)
   *
   */
  private enum TableOp {
    /**
     * Create the specified number of tables with a fixed prefix. It keeps on toggling between
     * FTable and MTable starting with FTable.
     */
    CREATE_TABLES((count) -> {
      FTableDescriptor ftd = new FTableDescriptor();
      MTableDescriptor mtd = new MTableDescriptor();
      ftd.setRedundantCopies(1);
      mtd.setRedundantCopies(2);
      TypeUtils.addColumnsFromJson(SCHEMA, ftd);
      TypeUtils.addColumnsFromJson(SCHEMA, mtd);

      Admin admin = MCacheFactory.getAnyInstance().getAdmin();
      Table ignored;
      for (int i = 0; i < count; i++) {
        String tableName = TABLE_NAME_PFX + i;
        ignored =
            i % 2 == 0 ? admin.createFTable(tableName, ftd) : admin.createMTable(tableName, mtd);
      }
    }),

    /**
     * Access the specified number of tables (both MTable and FTable) with the provided prefix.
     */
    ACCESS_TABLES((count) -> {
      MClientCache cache = MClientCacheFactory.getAnyInstance();
      for (int i = 0; i < count; i++) {
        Table table =
            i % 2 == 0 ? cache.getFTable(TABLE_NAME_PFX + i) : cache.getMTable(TABLE_NAME_PFX + i);
      }
    }),

    /**
     * Scan the specified number of tables (both MTable and FTable) with the provided prefix.
     */
    SCAN((count) -> {
      MClientCache cache = MClientCacheFactory.getAnyInstance();
      final Scan scan = new Scan();
      for (int i = 0; i < count; i++) {
        InternalTable table = (InternalTable) (i % 2 == 0 ? cache.getFTable(TABLE_NAME_PFX + i)
            : cache.getMTable(TABLE_NAME_PFX + i));
        try {
          int counter = 0;
          Scanner scanner = table.getScanner(scan);
          for (Row ignored : scanner) {
            counter++;
          }
          fail("Expected RegionDestroyedException.");
        } catch (MScanFailedException e) {
          assertRootCauseIsRegionDestroyed(e.getRootCause());
        }
      }
    }),

    /**
     * Put entries in the specified number of tables (both MTable and FTable) with the provided
     * prefix.
     */
    PUT((count) -> {
      MClientCache cache = MClientCacheFactory.getAnyInstance();
      for (int i = 0; i < count; i++) {
        try {
          if ((i % 2) == 0) {
            FTable table = cache.getFTable(TABLE_NAME_PFX + i);
            if ((i % 4) == 0) {
              table.append(new Record());
            } else {
              table.append(new Record(), new Record());
            }
          } else {
            MTable table = cache.getMTable(TABLE_NAME_PFX + i);
            if ((i % 3) == 0) {
              Put put = new Put(new byte[] {0, 1});
              put.addColumn("c1", 100);
              put.addColumn("c2", new Long[] {10l});
              put.addColumn("c3", "dummy-val");
              table.put(put);
            } else {
              Put put1 = new Put(new byte[] {0, 1});
              Put put2 = new Put(new byte[] {0, 2});

              put1.addColumn("c1", 100);
              put1.addColumn("c2", new Long[] {10l});
              put1.addColumn("c3", "dummy-val");
              table.put(put1);

              put2.addColumn("c1", 100);
              put2.addColumn("c2", new Long[] {10l});
              put2.addColumn("c3", "dummy-val");
              table.put(put2);
            }
          }
          fail("Expected RegionDestroyedException.");
        } catch (MCacheInternalErrorException e) {
          assertRootCauseIsRegionDestroyed(e.getRootCause());
        }
      }
    }),

    /**
     * Get entries from the specified number of tables (only MTable) with the provided prefix.
     */
    GET((count) -> {
      MClientCache cache = MClientCacheFactory.getAnyInstance();
      for (int i = 0; i < count; i++) {
        try {
          if ((i % 2) == 0) {
            /** nothing for FTable **/
          } else {
            MTable table = cache.getMTable(TABLE_NAME_PFX + i);
            if ((i % 3) == 0) {
              table.get(new Get(new byte[] {0, 1}));
            } else {
              table.get(Arrays.asList(new Get(new byte[] {0, 1}), new Get(new byte[] {0, 2})));
            }
            fail("Expected RegionDestroyedException.");
          }
        } catch (MCacheInternalErrorException e) {
          assertRootCauseIsRegionDestroyed(e.getRootCause());
        }
      }
    }),

    /**
     * Delete entries from the specified number of tables (only MTable) with the provided prefix.
     */
    DELETE((count) -> {
      MClientCache cache = MClientCacheFactory.getAnyInstance();
      for (int i = 0; i < count; i++) {
        try {
          if ((i % 2) == 0) {
            /** nothing for FTable **/
          } else {
            MTable table = cache.getMTable(TABLE_NAME_PFX + i);
            if ((i % 3) == 0) {
              table.delete(new Delete(new byte[] {0, 1}));
              fail("Expected RegionDestroyedException.");
            } else {
              // table.delete(Arrays.asList(new MDelete(new byte[]{0, 1}), new MDelete(new byte[]{0,
              // 2})));
              // fail("Expected RegionDestroyedException.");
            }
          }
        } catch (MCacheInternalErrorException e) {
          assertRootCauseIsRegionDestroyed(e.getRootCause());
        }
      }
    }),

    /**
     * Delete all the tables (both MTable and FTable) starting with fixed prefix.
     */
    DELETE_TABLES((count) -> {
      Admin admin = MCacheFactory.getAnyInstance().getAdmin();
      String tableName;
      for (int i = 0; i < count; i++) {
        tableName = TABLE_NAME_PFX + i;
        switch ((i % 2)) {
          case 0:
            if (admin.existsFTable(tableName))
              admin.deleteFTable(tableName);
            break;
          case 1:
            if (admin.existsMTable(tableName))
              admin.deleteMTable(tableName);
            break;
        }
      }
    });
    private final Consumer<Integer> consumer;

    TableOp(final Consumer<Integer> consumer) {
      this.consumer = consumer;
    }

    /**
     * Execute the operation with specified count.
     *
     * @param count the number (of tables)
     */
    public void execute(final int count) {
      this.consumer.accept(count);
    }

    /**
     * Assert that the thrown exception is RegionDestroyedException.
     *
     * @param throwable the exception thrown
     */
    private static void assertRootCauseIsRegionDestroyed(final Throwable throwable) {
      assertTrue("Incorrect exception received: " + throwable.getMessage(),
          throwable instanceof RegionDestroyedException);
    }
  }

  /**
   * A simple class to verify the table count from the meta-region (locally) from a client cache. It
   * is executed from the client caches with PROXY and CACHING_PROXY region. The assertion is done,
   * with the respective counts, before and after accessing the tables from client cache.
   */
  private static final class MetaRegionTableCountAssertRunnable extends SerializableRunnable {
    final int totalCount;
    final int beforeAccessCount;
    final int afterAccessCount;
    final TableOp tableOperation;

    /**
     * Constructor..
     *
     * @param totalCount total number of tables
     * @param beforeAccessCount the number of tables before accessing the tables
     * @param afterAccessCount the number of tables after accessing the tables
     * @param op the operation to executed; ACCESS for accessing and DELETE for deleting tables
     */
    public MetaRegionTableCountAssertRunnable(final int totalCount, final int beforeAccessCount,
        final int afterAccessCount, final TableOp op) {
      super("MetaRegionTableCountAssertRunnable");
      this.totalCount = totalCount;
      this.beforeAccessCount = beforeAccessCount;
      this.afterAccessCount = afterAccessCount;
      this.tableOperation = op;
    }

    @Override
    public void run() throws Exception {
      assertTableCountInMetaRegion(beforeAccessCount);
      tableOperation.execute(totalCount);
      assertTableCountInMetaRegion(afterAccessCount);
    }
  }

  /**
   * Assert that the meta-region (locally) holds the expected number of table entries in the client
   * cache (with PROXY/CACHING_PROXY).
   *
   * @param expectedCount the expected number of table entries in meta region
   */
  private static void assertTableCountInMetaRegion(final int expectedCount) {
    MonarchCacheImpl cache = MonarchCacheImpl.getInstance();
    assertNotNull("Expected valid cache handle.", cache);
    Region metaRegion = cache.getRegion(MTableUtils.AMPL_META_REGION_NAME);
    assertNotNull("Meta-region does not exist.", metaRegion);
    assertEquals("Incorrect number of tables in meta-region.", expectedCount, metaRegion.size());
  }

  /**
   * A simple helper to create client cache with or without meta-region caching enabled.
   *
   * @param enableCaching true for CACHING_PROXY; false for PROXY
   */
  private static void createCache(final boolean enableCaching) {
    // MClientCacheFactory.getAnyInstance().close();
    // new MClientCacheFactory()
    // .addPoolLocator("127.0.0.1", Integer.parseInt(TEST_BASE.getLocatorPort()))
    // .enableMetaRegionCaching(enableCaching).create();
    Properties confWithoutCaching = new Properties();
    confWithoutCaching.setProperty(Constants.MonarchLocator.MONARCH_LOCATORS,
        TEST_BASE.getLocatorString());
    confWithoutCaching.setProperty(Constants.MClientCacheconfig.ENABLE_META_REGION_CACHING,
        String.valueOf(enableCaching));
    MCacheFactory.getAnyInstance().close();
    MClientCacheFactory.getOrCreate(confWithoutCaching);
  }

  /**
   * The test creates a fixed number of tables from a client cache and then asserts that the
   * respective PROXY and CACHING_PROXY clients have the appropriate number of tables locally.
   *
   * @throws Exception
   */
  @Test
  public void testClientMetaRegionWith_PROXY_CACHING_PROXY() throws Exception {
    final VM clientVm = TEST_BASE.getVm(1);

    /** close the cache and recreate with CACHING_PROXY meta-region in _this_ VM **/
    createCache(false);

    /** close the cache and recreate with CACHING_PROXY meta-region in client VM **/
    clientVm.invoke(() -> createCache(true));

    /** create the tables from client with PROXY meta-region **/
    final int totalTableCount = 10;

    TableOp.CREATE_TABLES.execute(totalTableCount);

    /**
     * client with PROXY meta-region does not hold any entry even though tables were created from
     * the same client-cache.
     */
    new MetaRegionTableCountAssertRunnable(totalTableCount, 0, 0, TableOp.ACCESS_TABLES).run();

    /**
     * client with CACHING_PROXY initially does not hold any entry but after accessing the tables,
     * meta-region has all the accessed table descriptors.
     */
    clientVm.invoke(new MetaRegionTableCountAssertRunnable(totalTableCount, 0, totalTableCount,
        TableOp.ACCESS_TABLES));

    /** delete the regions and then assert on the table count **/
    TableOp.DELETE_TABLES.execute(totalTableCount);

    /** the state remains same after deleting the tables via PROXY client **/
    new MetaRegionTableCountAssertRunnable(totalTableCount, 0, 0, TableOp.ACCESS_TABLES).run();

    /**
     * Execute the scan from the client with CACHING_PROXY to see if there are any failures.
     */
    clientVm.invoke(new MetaRegionTableCountAssertRunnable(totalTableCount, totalTableCount,
        totalTableCount, TableOp.SCAN));
    clientVm.invoke(new MetaRegionTableCountAssertRunnable(totalTableCount, totalTableCount,
        totalTableCount, TableOp.PUT));
    clientVm.invoke(new MetaRegionTableCountAssertRunnable(totalTableCount, totalTableCount,
        totalTableCount, TableOp.GET));
    clientVm.invoke(new MetaRegionTableCountAssertRunnable(totalTableCount, totalTableCount,
        totalTableCount, TableOp.DELETE));

    /**
     * since the CACHING_PROXY cache is not updated, before deleting the tables the meta-region has
     * all the tables accessed earlier but none after deleting them.
     */
    clientVm.invoke(new MetaRegionTableCountAssertRunnable(totalTableCount, totalTableCount, 0,
        TableOp.DELETE_TABLES));
  }
}
