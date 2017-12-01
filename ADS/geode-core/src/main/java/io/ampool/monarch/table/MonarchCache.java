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

import java.util.Set;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.MTableService;
import io.ampool.store.StoreHandler;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;

/**
 * MonarchCache represents the singleton proxy to access Ampool Datastore.
 *
 * Instance of this interface are created using one of the following methods:
 * <ul>
 * <li>Client should use {@link MClientCacheFactory#getAnyInstance()} to create (if does not exist)
 * or get an instance of {@link io.ampool.monarch.table.client.MClientCache} which implements this
 * {@link MonarchCache} interface</li>
 * </ul>
 *
 * @since 0.3.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MonarchCache extends MTableService {

  /**
   * Retreive an Admin {@link Admin} instance to perform Administrative operations on the Table.
   */
  Admin getAdmin();

  /**
   * Return the MTable with the specified name if present.
   *
   * @param tableName the name of the Table
   * @return the handle of MTable {@link MTable} or null if not found
   * @throws IllegalArgumentException if name is null, the empty string, or with Invalid characters
   * @deprecated use {@link #getMTable(String)} or {@link #getFTable(String)}
   */
  @Deprecated
  MTable getTable(String tableName) throws IllegalArgumentException;

  /**
   * Return the existing MTable with the specified name.
   *
   * @param tableName the name of the Table
   * @return the MTable or null if not found
   * @throws IllegalArgumentException if name is null, the empty string, or with Invalid characters
   */
  MTable getMTable(String tableName) throws IllegalArgumentException;

  /**
   * Return the existing FTable with the specified name
   *
   * @param tableName the name of the Table
   * @return the FTable or null if not found
   * @throws IllegalArgumentException if name is null, the empty string, or with Invalid characters
   */
  FTable getFTable(String tableName) throws IllegalArgumentException;

  /**
   * Return the existing MTable with the specified name.
   *
   * @param tableName the name of the Table
   * @return the MTable Key Count or null if not found
   * @throws IllegalArgumentException if name is null, the empty string, or with Invalid characters
   */
  public Set<Object> getMTableKeySet(String tableName) throws IllegalArgumentException;


  /**
   * Get the socket buffer size being used for this client cache
   *
   * @return the size of socket buffers
   *
   */
  int getSocketBufferSize();

  void close();

  /**
   * Checks if cache is closed.
   *
   * @return true if cache is closed
   */
  boolean isClosed();

  /**
   * Return the StoreHandler with this cache.
   *
   * @return the handle of StoreHandler {@link StoreHandler}
   */
  StoreHandler getStoreHandler();

  /**
   * Returns the Region handle for Store Meta
   */
  Region getStoreMetaRegion();

  /**
   * Returns the FTableDescriptor
   *
   * @return FTableDescriptor instance for the table
   *
   */
  FTableDescriptor getFTableDescriptor(final String tableName);

  MTableDescriptor getMTableDescriptor(final String tableName);

  TableDescriptor getTableDescriptor(final String tableName);

  MCache getReconnectedCache();

}
