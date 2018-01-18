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

package io.ampool.store;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.StoreInternalException;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.ftable.internal.ReaderOptions;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import io.ampool.store.internal.StoreDataMover;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.SharedTierStore;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.tierstore.wal.WALMonitoringThread;
import io.ampool.tierstore.wal.WALResultScanner;
import io.ampool.tierstore.wal.WriteAheadLog;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;


/**
 * StoreHandler handles all the operations over the stores. All table implementations must use this
 * for performing operations on the Store.
 * <p>
 * StoreHandler is a singleton instance which is created per node/cache. To retrieve instance of
 * StoreHandler use {@link StoreHandler#getInstance()}
 */
public class StoreHandler {

  private static final Logger logger = LogService.getLogger();

  // Following are the properties that we need to pass to writer
  public static final String TABLE_NAME = "table.name";
  public static final String PARTITION_ID = "partition.id";
  public static final String BASE_URI = "base.uri";


  private String monitoringDirectory;
  private static volatile StoreHandler storeHandler = null;
  private WriteAheadLog writeAheadLog = null;
  WALMonitoringThread monitorThread = null;
  /**
   * Tier name to store instance Map New impl.
   */
  private Map<String, TierStore> tierStoreMap;
  private Map<String, ConverterDescriptor> converterDescriptorMap = new ConcurrentHashMap<>();

  private Map<String, TierStoreWriter> writersCache = null;



  /**
   * Storehandler should do following tasks on startup 1. Load all store information from metatable
   * and create store instances for all stores and maintaing it in internal map 2. Init WAL writer's
   * 3. Spawn a WAL monitoring Thread for monitoring WAL logs. 4. Wait for WAL to complete the
   * startup/recovery process
   */
  private StoreHandler() {
    monitoringDirectory = "WAL";
    try {

      tierStoreMap = new ConcurrentHashMap<>();
      writersCache = new ConcurrentHashMap<>();

      // Init Write Ahead Log
      writeAheadLog = WriteAheadLog.getInstance();
      writeAheadLog.init(new File(monitoringDirectory).toPath());

      // Init WAL Thread
      // TODO : Pass properties
      monitorThread = new WALMonitoringThread(this, writeAheadLog, new Properties());
      // Starting Monitoring Thread
      monitorThread.start();

    } catch (IOException e) {
      // TODO: HANDLEME
      throw new StoreInternalException("Error while configuring WAL");
    }

  }

  public static StoreHandler getInstance() {
    if (storeHandler == null) {
      synchronized (StoreHandler.class) {
        if (storeHandler == null) {
          storeHandler = new StoreHandler();
        }
      }
    }
    return storeHandler;
  }


  public Map<String, TierStore> getTierStores() {
    return tierStoreMap;
  }

  /**
   * Register a store in cache
   *
   * @param name Name of the new store.
   * @param store Instance of the store class
   */
  public void registerStore(String name, TierStore store) {
    logger.info("Registering TierStore: name= {}, store= {}", name, store);
    tierStoreMap.put(name, store);
  }

  /**
   * Deregister a store from cache
   *
   * @param name name of the store to be removed.
   */
  public void deRegisterStore(String name) {
    final TierStore removedTS = tierStoreMap.remove(name);
    if (removedTS != null) {
      logger.info("Destroying TierStore: name= {}, store= {}", name, removedTS);
      removedTS.destroy();
    }
  }

  public void stopTierMonitoring() {
    for (Entry<String, TierStore> entry : tierStoreMap.entrySet()) {
      logger.info("Stopping tier evictor thread for  store: " + entry.getKey());
      entry.getValue().stopMonitoring();
    }
  }


  public int append(final String tableName, final int partitionId, final IMKey blockKey,
                    final BlockValue blockValue) throws IOException {
    /* TODO: for ORC format blocks WAL can be skipped */
    return writeAheadLog.append(tableName, partitionId, blockKey, blockValue);
  }

  public IStoreResultScanner getStoreScanner(String tableName, int partitionId,
                                             ReaderOptions readerOpts) {
    return new TierStoreResultScanner(this, getStoreHierarchy(tableName), tableName, partitionId,
            readerOpts);
  }

  public WALResultScanner getWALScanner(String tableName, int partitionId) {
    return writeAheadLog.getScanner(tableName, partitionId);
  }

  /**
   * TODO : Modify this to write to N tiers currently it writes to single tier
   */
  public void writeToStore(String tableName, int partitionId, StoreRecord[] storeRecords)
          throws IOException {
    // 2.2 Check tier1 store for capacity and ensure space
    // TODO Before writing check and make space if current store has nothing
    Map<String, TierStoreConfiguration> storeHierarchy = getStoreHierarchy(tableName);
    // hoping storeHierachy has atleast one tier
    Iterator<Entry<String, TierStoreConfiguration>> entryIterator =
            storeHierarchy.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Entry<String, TierStoreConfiguration> entry = entryIterator.next();
      TierStore store = tierStoreMap.get(entry.getKey());

      // try {
      Properties newProperties = new Properties();
      newProperties.putAll(entry.getValue().getTierProperties());
      newProperties.put(TABLE_NAME, tableName);
      newProperties.put(PARTITION_ID, String.valueOf(partitionId));
      newProperties.put(BASE_URI, store.getBaseURI().toString());

      TierStoreWriter storeWriter = null;
      if (writersCache.containsKey(tableName + "_" + partitionId)) {
        // writer is cached
        storeWriter = writersCache.get(tableName + "_" + partitionId);
      } else {
        storeWriter = store.getWriter(tableName, partitionId);
        storeWriter.setConverterDescriptor(store.getConverterDescriptor(tableName));
        writersCache.put(tableName + "_" + partitionId, storeWriter);
      }
      int numberOfRecordsWritten = storeWriter.write(newProperties, storeRecords);
      /* throw an error if all rows were not written */
      if (numberOfRecordsWritten == 0 && storeRecords != null && storeRecords.length > 0) {
        throw new IOException("Failed to write records: table= " + tableName + ", bucketId= "
                + partitionId + ", store= " + store.getName());
      }
      // }catch (IOException e) {
      // // should we throw or handle
      // // for now handling
      // logger.error("Error in writing to store: ",e);
      // e.printStackTrace();
      // }
      // TODO break is for now until we make generic N tier write
      break;
    }
  }

  // private ConverterDescriptor getConverterDescriptor(final String tableName) {
  // ConverterDescriptor converterDescriptor = null;
  // if (!converterDescriptorMap.containsKey(tableName)) {
  // converterDescriptor = ConverterUtils.getConverterDescriptor(tableName);
  // converterDescriptorMap.put(tableName, converterDescriptor);
  // } else {
  // converterDescriptor = converterDescriptorMap.get(tableName);
  // }
  // return converterDescriptor;
  // }

  public LinkedHashMap<String, TierStoreConfiguration> getStoreHierarchy(final String tableName) {
    TableDescriptor tableDescriptor = MTableUtils
            .getTableDescriptor((MonarchCacheImpl) MCacheFactory.getAnyInstance(), tableName);
    if (tableDescriptor instanceof FTableDescriptor) {
      return ((FTableDescriptor) tableDescriptor).getTierStores();
    } else {
      // Confirm if we should return empty
      return new LinkedHashMap<>();
    }
  }

  private void checkAndMakeSpace(StoreHierarchy storeHierarchy, int numRecords,
                                 final TableDescriptor tableDescriptor) {
    // TODO implementation to be done at the time of multi tier eviction policy
  }

  /**
   * Delete a table data from all archive store tiers
   */
  public void deleteTable(String tableName) {
    WriteAheadLog.getInstance().deleteTable(tableName);

    Map<String, TierStoreConfiguration> storeHierarchy = getStoreHierarchy(tableName);

    // hoping storeHierachy has atleast one tier
    Iterator<Entry<String, TierStoreConfiguration>> entryIterator =
            storeHierarchy.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Entry<String, TierStoreConfiguration> tierEntry = entryIterator.next();
      TierStore tierStore = getTierStore(tierEntry.getKey());
      tierStore.deleteTable(tableName);
    }
    /* Files opened for read / scan will be closed lazily when the reader/scanner is done */
  }

  /**
   * Delete a bucket data from all archive store tiers
   */
  public void deleteBucket(String tableName, int partitionId) {
    WriteAheadLog.getInstance().deleteBucket(tableName, partitionId);
    Map<String, TierStoreConfiguration> storeHierarchy = getStoreHierarchy(tableName);
    // hoping storeHierachy has atleast one tier
    Iterator<Entry<String, TierStoreConfiguration>> entryIterator =
            storeHierarchy.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Entry<String, TierStoreConfiguration> tierEntry = entryIterator.next();
      TierStore tierStore = getTierStore(tierEntry.getKey());
      tierStore.deletePartition(tableName, partitionId);
    }
    /* Files opened for read / scan will be closed lazily when the reader/scanner is done */
  }

  public void pauseWALMonitoring(String tableName, int partitionID) {
    monitorThread.pauseWALMonitor(tableName, partitionID);
  }

  public void resumeWALMonitoring(String tableName, int partitionID) {
    monitorThread.resumeWALMonitor(tableName, partitionID);
  }

  /**
   * Stop WAL monitor thread.
   */
  public void stopWALMonitoring() {
    monitorThread.interrupt();
  }

  public void moveStore(BucketRegion region, final InternalDistributedMember recipient) {

    Map<String, TierStoreConfiguration> storeHierarchy = getStoreHierarchy(region.getDisplayName());
    // hoping storeHierachy has atleast one tier
    Iterator<Entry<String, TierStoreConfiguration>> entryIterator =
            storeHierarchy.entrySet().iterator();
    int tierId = 1;
    while (entryIterator.hasNext()) {
      Entry<String, TierStoreConfiguration> tierEntry = entryIterator.next();
      TierStore tierStore = getTierStore(tierEntry.getKey());
      if (!(tierStore instanceof SharedTierStore)) {
        new StoreDataMover(tierStore, tierId++, region, region.getId(), recipient).start();
      }
    }
  }

  public void flushWriteAheadLog(String tableName, int partitionId) {
    this.writeAheadLog.flush(tableName, partitionId);
  }


  /**
   * Get a store instance for the given store name from store handler
   *
   * @return TierStore instance
   */
  public TierStore getTierStore(String storeName) {
    return tierStoreMap.get(storeName);
  }

  /**
   * Get a store instance for the given table name and hierarchy index from store handler
   *
   * @return TierStore instance
   */
  public TierStore getTierStore(String tableName, int tierId) {
    LinkedHashMap<String, TierStoreConfiguration> storeHierarchy = getStoreHierarchy(tableName);
    int index = 1;
    Iterator<Entry<String, TierStoreConfiguration>> iterator = storeHierarchy.entrySet().iterator();
    while (iterator.hasNext()) {
      if (index == tierId) {
        Entry<String, TierStoreConfiguration> next = iterator.next();
        TierStore tierStore = getTierStore(next.getKey());
        return tierStore;
      }
      index++;
    }
    // TODO should we send null or throw exception?
    return null;
  }
}
