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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.internal.ReaderOptions;
import io.ampool.tierstore.TierStoreReader;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

/**
 * Wrapper scanner for handling scan operations on all the tiers of the store hierarchy.
 */
public class TierStoreResultScanner implements IStoreResultScanner, Iterator<StoreRecord> {
  private static final Logger logger = LogService.getLogger();

  private Iterator<StoreRecord> internalScanner = Collections.emptyIterator();
  private String tableName = null;
  private int partitionId = -1;
  private final StoreHandler storeHandler;
  private final Iterator<String> tierNameIterator;
  private final ReaderOptions readerOptions;

  private Iterator<Entry<String, TierStoreConfiguration>> storeHierarchyIterator = null;


  public TierStoreResultScanner(StoreHandler storeHandler,
      LinkedHashMap<String, TierStoreConfiguration> storeHierarchy, String tableName,
      int partitionId, final ReaderOptions readerOptions) {
    this.storeHandler = storeHandler;
    this.tableName = tableName;
    this.partitionId = partitionId;
    this.readerOptions = readerOptions;
    this.tierNameIterator = storeHierarchy.keySet().iterator();
  }

  /**
   * Iterate until either internal-scanner has more results or more tiers yet to be iterated over.
   *
   * @return true if more results are to be returned; false otherwise
   */
  @Override
  public boolean hasNext() {
    if (this.internalScanner.hasNext()) {
      return true;
    }
    while (tierNameIterator.hasNext()) {
      final String tier = tierNameIterator.next();
      logger.debug("Scanning tier= {}; table= {}, partitionId= {}", tier, tableName, partitionId);
      final TierStoreReader reader =
          storeHandler.getTierStore(tier).getReader(tableName, partitionId);
      reader.setConverterDescriptor(
          storeHandler.getTierStore(tier).getConverterDescriptor(tableName));
      if (this.readerOptions != null) {
        reader.setReaderOptions(this.readerOptions);
      }
      this.internalScanner = reader.iterator();
      if (this.internalScanner.hasNext()) {
        return true;
      }
    }
    logger.debug("Scan completed for all tiers; table= {}, partitionId= {}", tableName,
        partitionId);
    return false;
  }

  @Override
  public StoreRecord next() {
    return this.internalScanner.next();
  }

  @Override
  public void close() {

  }

  @Override
  public Iterator iterator() {
    return this;
  }
}
