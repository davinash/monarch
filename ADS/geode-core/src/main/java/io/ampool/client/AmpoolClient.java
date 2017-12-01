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
package io.ampool.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.exceptions.MCacheInternalErrorException;
import io.ampool.monarch.table.ftable.FTable;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.MonarchCacheImpl;

import java.util.Properties;

public class AmpoolClient {
  private static volatile GemFireCacheImpl geodeCache = null;
  private static volatile Admin admin = null;
  private static volatile MClientCache clientCache;

  /**
   * Create the AmpoolClient with default locator host (localhost) and port (10334).
   */
  public AmpoolClient() {
    this("localhost", 10334);
  }

  /**
   * Create the client for communicating with the Ampool cluster using provided details.
   *
   * @param locatorHost the locator host
   * @param locatorPort the locator port
   */
  public AmpoolClient(final String locatorHost, final int locatorPort) {
    this(locatorHost, locatorPort, new Properties());
  }

  /**
   * Create the client for communicating with Ampool cluster using provided details. User can
   * provide the custom properties along with locator host/port.
   *
   * @param locatorHost the locator host
   * @param locatorPort the locator port
   * @param props the properties
   */
  public AmpoolClient(final String locatorHost, final int locatorPort, final Properties props) {
    if (clientCache == null || clientCache.isClosed()) {
      synchronized (AmpoolClient.class) {
        if (clientCache == null || clientCache.isClosed()) {
          final Properties p = new Properties();
          for (final String pName : props.stringPropertyNames()) {
            p.setProperty(pName, props.getProperty(pName));
          }
          p.setProperty(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, locatorHost);
          p.setProperty(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, String.valueOf(locatorPort));
          clientCache = new MClientCacheFactory().getOrCreate(p);
          admin = clientCache.getAdmin();
          geodeCache = MonarchCacheImpl.getGeodeCacheInstance();
        }
      }
    }
  }

  /**
   * Whether or not the client is connected to the cluster.
   *
   * @return true if it has a valid connection; false otherwise
   */
  public boolean isConnected() {
    return !clientCache.isClosed();
  }

  /**
   * Close the client connection.
   */
  public void close() {
    synchronized (AmpoolClient.class) {
      clientCache.close();
      clientCache = null;
      admin = null;
      geodeCache = null;
    }
  }

  /**
   * Retrieve an Admin {@link Admin} instance to perform Administrative operations.
   *
   * @return the admin instance
   */
  public Admin getAdmin() {
    return admin;
  }


  /**
   * Checks if the MTable with the given tableName exists or not
   *
   * @param tableName tableName to check
   * @return true if table exists else false
   * @throws MCacheInternalErrorException Internal Error
   */
  public boolean existsMTable(String tableName) {
    return clientCache.getAdmin().existsMTable(tableName);
  }

  /**
   * Checks if the fact table with the given tableName exists or not
   *
   * @param tableName tableName to check
   * @return true if table exists else false
   * @throws MCacheInternalErrorException Internal Error
   */
  public boolean existsFTable(String tableName) {
    return clientCache.getAdmin().existsFTable(tableName);
  }


  /**
   * Return the existing MTable with the specified name.
   *
   * @param tableName the name of the Table
   * @return the MTable or null if not found
   * @throws IllegalArgumentException if name is null, the empty string, or with Invalid characters
   */
  public MTable getMTable(String tableName) throws IllegalArgumentException {
    return clientCache.getMTable(tableName);
  }

  /**
   * Return the existing FTable with the specified name
   *
   * @param tableName the name of the Table
   * @return the FTable or null if not found
   * @throws IllegalArgumentException if name is null, the empty string, or with Invalid characters
   */
  public FTable getFTable(String tableName) throws IllegalArgumentException {
    return clientCache.getFTable(tableName);
  }

  /**
   * Returns the handle of the MClient Cache.
   * 
   * @return the client cache
   */
  public MClientCache getClientCache() {
    return clientCache;
  }

  public GemFireCache getGeodeCache() {
    return geodeCache;
  }

}
