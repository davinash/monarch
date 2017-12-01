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

import org.apache.geode.test.junit.categories.MonarchTest;
import org.junit.experimental.categories.Category;

@Category(MonarchTest.class)
public class MTableAllOpsDUnitTestWithCacheStore /* extends MTableAllOpsDUnitTest */ {/*
                                                                                       * public
                                                                                       * MTableAllOpsDUnitTestWithCacheStore
                                                                                       * () {
                                                                                       * super(); }
                                                                                       * 
                                                                                       * protected
                                                                                       * void
                                                                                       * createTable
                                                                                       * () {
                                                                                       * logger.
                                                                                       * info("Creating table with cachstore attached"
                                                                                       * );
                                                                                       * MClientCache
                                                                                       * clientCache
                                                                                       * =
                                                                                       * MClientCacheFactory
                                                                                       * .
                                                                                       * getAnyInstance
                                                                                       * ();
                                                                                       * MTableDescriptor
                                                                                       * tableDescriptor
                                                                                       * = new
                                                                                       * MTableDescriptor
                                                                                       * (); for
                                                                                       * (int
                                                                                       * colmnIndex
                                                                                       * = 0;
                                                                                       * colmnIndex
                                                                                       * <
                                                                                       * NUM_OF_COLUMNS;
                                                                                       * colmnIndex+
                                                                                       * +) {
                                                                                       * tableDescriptor
                                                                                       * =
                                                                                       * tableDescriptor
                                                                                       * .addColumn(
                                                                                       * Bytes.
                                                                                       * toBytes(
                                                                                       * COLUMN_NAME_PREFIX
                                                                                       * +
                                                                                       * colmnIndex)
                                                                                       * ); }
                                                                                       * 
                                                                                       * tableDescriptor
                                                                                       * .
                                                                                       * setCacheStoreFactory
                                                                                       * (new
                                                                                       * MyCacheStoreFactory
                                                                                       * ());
                                                                                       * tableDescriptor
                                                                                       * .
                                                                                       * setCacheStoreConfig
                                                                                       * (new
                                                                                       * MyCacheStoreConfig
                                                                                       * ());
                                                                                       * 
                                                                                       * MAdmin
                                                                                       * admin =
                                                                                       * clientCache
                                                                                       * .getAdmin()
                                                                                       * ; MTable
                                                                                       * table =
                                                                                       * admin.
                                                                                       * createTable
                                                                                       * (
                                                                                       * TABLE_NAME,
                                                                                       * tableDescriptor
                                                                                       * );
                                                                                       * 
                                                                                       * assertEquals
                                                                                       * (table.
                                                                                       * getName(),
                                                                                       * TABLE_NAME)
                                                                                       * ; } }
                                                                                       * 
                                                                                       * class
                                                                                       * MyCacheStore
                                                                                       * implements
                                                                                       * CacheStore,
                                                                                       * Serializable
                                                                                       * {
                                                                                       * 
                                                                                       * @Override
                                                                                       * public void
                                                                                       * destroy(
                                                                                       * Region
                                                                                       * region)
                                                                                       * throws
                                                                                       * CacheStoreException
                                                                                       * {
                                                                                       * 
                                                                                       * }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public
                                                                                       * String
                                                                                       * getEventQueueName
                                                                                       * () { return
                                                                                       * "MyCacheStore";
                                                                                       * }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public void
                                                                                       * initialize(
                                                                                       * Region
                                                                                       * region)
                                                                                       * throws
                                                                                       * CacheStoreException
                                                                                       * {
                                                                                       * 
                                                                                       * }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public void
                                                                                       * initializeMtable
                                                                                       * (Region
                                                                                       * region)
                                                                                       * throws
                                                                                       * CacheStoreException
                                                                                       * {
                                                                                       * 
                                                                                       * }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public
                                                                                       * CacheStore
                                                                                       * alter(
                                                                                       * CacheStoreMutator
                                                                                       * cacheStoreMutator)
                                                                                       * { return
                                                                                       * null; }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public
                                                                                       * CacheStoreConfig
                                                                                       * getConfig()
                                                                                       * { return
                                                                                       * null; }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public
                                                                                       * CacheStoreInfo
                                                                                       * getCacheStoreInfo
                                                                                       * () { return
                                                                                       * null; }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public
                                                                                       * CacheStoreStats
                                                                                       * getCacheStoreStats
                                                                                       * () { return
                                                                                       * null; }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public void
                                                                                       * persist(
                                                                                       * Region
                                                                                       * region,
                                                                                       * Object o,
                                                                                       * Object o1,
                                                                                       * boolean
                                                                                       * isUpdate)
                                                                                       * throws
                                                                                       * CacheStoreException
                                                                                       * { //logger.
                                                                                       * info("persist called for region {} key {} value {} "
                                                                                       * , region.
                                                                                       * getName(),
                                                                                       * o, o1); }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public
                                                                                       * Object
                                                                                       * remove(
                                                                                       * Region
                                                                                       * region,
                                                                                       * Object o)
                                                                                       * throws
                                                                                       * CacheStoreException
                                                                                       * { //logger.
                                                                                       * info("persist called for region {} key {}  "
                                                                                       * , region.
                                                                                       * getName(),
                                                                                       * o);
                                                                                       * 
                                                                                       * return
                                                                                       * null; }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public
                                                                                       * Object
                                                                                       * retrieve(
                                                                                       * Region
                                                                                       * region,
                                                                                       * Object o)
                                                                                       * throws
                                                                                       * CacheStoreException
                                                                                       * { //logger.
                                                                                       * info("persist called for region {} key {}  "
                                                                                       * , region.
                                                                                       * getName(),
                                                                                       * o); return
                                                                                       * null; }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public void
                                                                                       * clear(
                                                                                       * Region
                                                                                       * region)
                                                                                       * throws
                                                                                       * CacheStoreException
                                                                                       * {
                                                                                       * 
                                                                                       * }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public void
                                                                                       * close() {
                                                                                       * 
                                                                                       * }
                                                                                       * 
                                                                                       * @Override
                                                                                       * public void
                                                                                       * destroyCacheStore
                                                                                       * () {} }
                                                                                       * 
                                                                                       * class
                                                                                       * MyCacheStoreFactory
                                                                                       * extends
                                                                                       * CacheStoreFactory
                                                                                       * implements
                                                                                       * Serializable
                                                                                       * {
                                                                                       * 
                                                                                       * @Override
                                                                                       * public
                                                                                       * CacheStore
                                                                                       * create(
                                                                                       * MCache
                                                                                       * cache,
                                                                                       * String
                                                                                       * storeName,
                                                                                       * CacheStoreConfig
                                                                                       * cacheStoreConfig)
                                                                                       * { return
                                                                                       * new
                                                                                       * MyCacheStore
                                                                                       * (); } }
                                                                                       * 
                                                                                       * class
                                                                                       * MyCacheStoreConfig
                                                                                       * extends
                                                                                       * CacheStoreConfig
                                                                                       * implements
                                                                                       * Serializable
                                                                                       * {
                                                                                       */
}
