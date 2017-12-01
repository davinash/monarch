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
package io.ampool.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.exceptions.MCacheInternalErrorException;
import io.ampool.monarch.table.exceptions.MScanFailedException;
import org.apache.geode.cache.RegionDestroyedException;

/**
 * A simple demonstrating how to enable the client side caching of tables. It also shows how to deal
 * with the stale instances of tables, on client side, in case the tables are deleted meantime from
 * server(s) via some other client.
 * <p>
 * The client side caching of meta region, that holds table descriptors, can be enabled by setting
 * property "enable-meta-region-caching" to "true" when creating client cache. Once client side
 * caching is enabled, the stale table instances, if any, need to be cleared explicitly by the user.
 * <p>
 * It is assumed that the table is already created, from some client, and this client caches the
 * used instances of table/table-descriptors. It makes the subsequent access to the same table
 * faster, without needing to make a round-trip to a server.
 * <p>
 * But, caching the instances on client side also causes the stale instances. In case the cached
 * table instance(s) are deleted via some other client, the cached instances are not updated
 * accordingly. Thus, the subsequent operation on such tables causes an exception and such instances
 * need to be removed from the local cache.
 * <p>
 * Though the example demonstrates usage for MTable, it is equally applicable to FTables when using
 * the respective operations.
 * <p>
 * NOTE: It is recommended that the stale instances should be cleared from the client local memory
 * for correct behaviour, if client side caching is enabled.
 * <p>
 * The
 * <p>
 * Since version: 1.2.3
 */
public class TableWithClientCachingEnabled {
  private static final int MAX_RECORDS = 10;

  /**
   * The main method.. Arguments: - <table-name> : mandatory - <locator-host>: optional (default:
   * localhost) - <locator-port>: optional (default: 10334)
   *
   * @param args the arguments
   */
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("Need at least table-name to make query.");
      System.out.println("# Arguments: <table-name> [<locator-host> <locator-port>]");
      return;
    }
    final String tableName = args[0];

    System.out.printf("[INFO] Make sure that the table (MTable) `%s` exists; then hit <Enter>...",
        tableName);
    int read = System.in.read();

    final AmpoolClient ac = getClientCacheWithMetaRegionCachingEnabled(args);
    MTable table = ac.getMTable(tableName);

    if (table == null) {
      System.out.printf("[ERROR] The table with name `%s` (MTable) does not exist.\n\n", tableName);
      return;
    }

    executeScan(table);

    System.out.printf("[INFO] Delete the table `%s`; then hit <Enter>...", tableName);
    read = System.in.read();

    /**
     * Once the table is deleted from some other client/MASH but since it was cached in current
     * client cache, the local table handle is still valid. Executed
     */
    table = ac.getMTable(tableName);
    try {
      executeScan(table);
    } catch (MScanFailedException exception) {
      if (exception.getRootCause() instanceof RegionDestroyedException) {
        System.out.println(
            "[INFO] SCAN: Got the expected exception: " + exception.getRootCause().getMessage());
        /** to clean-up the stale table from local cache.. delete **/
        // cache.getAdmin().deleteMTable(tableName);
      } else {
        System.out.println("[ERROR] Expected RegionDestroyedException.");
      }
    }

    /** similarly for get/put operations catch the exception and check for the cause **/
    table = ac.getMTable(tableName);
    try {
      // /** execute get operation **/
      // final MGet dummyGet = new MGet(new byte[]{0, 1, 2});
      // table.get(dummyGet);

      /** execute put operation **/
      final Put dummyPut = new Put(new byte[] {0, 1, 2});
      table.put(dummyPut);
    } catch (MCacheInternalErrorException exception) {
      if (exception.getRootCause() instanceof RegionDestroyedException) {
        System.out.println(
            "[INFO] PUT: Got the expected exception: " + exception.getRootCause().getMessage());
        /** to clean-up the stale table from local cache.. delete **/
        ac.getAdmin().deleteMTable(tableName);
      } else {
        System.out.println("[ERROR] Expected RegionDestroyedException.");
      }
    }

    table = ac.getMTable(tableName);
    if (table != null) {
      System.out.println("[ERROR] Table should not exist.");
    }
  }

  /**
   * Simple method that executes the scan and prints first `n` records/rows.
   *
   * @param table the table
   */
  private static void executeScan(MTable table) {
    System.out.println("[INFO] Executing the scan to display `" + MAX_RECORDS + "` rows.");
    final Scanner scanner = table.getScanner(new Scan());
    int counter = 0;
    for (Row result : scanner) {
      if (counter++ == MAX_RECORDS) {
        break;
      }
      System.out.printf("[INFO] RowId= %s: %s\n", Arrays.toString(result.getRowId()),
          "result.getCells()");
    }
    if (counter == 0) {
      System.out.println("[INFO] No rows were found in table: " + table.getName());
    }
  }

  /**
   * Create and return the client cache with property "enable-meta-region-caching" set to "true".
   * This enables caching the tables (i.e. table descriptors) on client side.
   *
   * @return the ampool client
   */
  private static AmpoolClient getClientCacheWithMetaRegionCachingEnabled(String[] args) {
    String locator_host = "localhost";
    int locator_port = 10334;
    if (args.length >= 3) {
      locator_host = args[1];
      locator_port = Integer.parseInt(args[2]);
    }

    Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.ENABLE_META_REGION_CACHING, "true");
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG,
        "TableWithClientCachingEnabled.log");
    return new AmpoolClient(locator_host, locator_port, props);
  }
}
