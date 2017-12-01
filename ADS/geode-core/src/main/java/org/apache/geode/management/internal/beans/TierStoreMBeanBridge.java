/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.beans;

import io.ampool.tierstore.TierStore;
import org.apache.geode.internal.cache.TierStoreStats;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.ManagementStrings;
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor;
import org.apache.logging.log4j.Logger;

import java.net.URI;

/**
 * Bridge class to act as an interface between JMX layer and TierStores
 * 
 * 
 */
public class TierStoreMBeanBridge {

  private TierStore tierStore;
  private MBeanStatsMonitor monitor;
  private TierStoreStats tierStoreStats;
  private static final Logger logger = LogService.getLogger();

  public TierStoreMBeanBridge(TierStore ts) {

    this.tierStore = (TierStore) ts;

    this.monitor = new MBeanStatsMonitor(ManagementStrings.TIERSTORE_MONITOR.toLocalizedString());
    this.tierStoreStats = ts.getStats();
    addTierStoreStats(tierStoreStats);
  }


  public void stopMonitor() {
    monitor.stopListener();
  }

  /** Statistics **/

  public TierStoreMBeanBridge() {
    this.monitor = new MBeanStatsMonitor(ManagementStrings.TIERSTORE_MONITOR.toLocalizedString());
  }



  public void addTierStoreStats(TierStoreStats stats) {
    /*
     * The tier has been created but the stats have not been invoked hence could be a possibility
     * that the getStats is not populated.
     */
    try {
      monitor.addStatisticsToMonitor(stats.getStats());
    } catch (Exception e) {
      logger.warn("The Statistics have not been populated yet" + e.toString());
    }
  }

  public int getTierStoreWriters() {
    return monitor.getStatistic("writes").intValue();
  }


  public long getBytesWritten() {
    return this.tierStore.getBytesWritten();
  }

  public long getBytesRead() {
    return this.tierStore.getBytesRead();
  }


  // TODO: To take up the implementation later as these are not exposed for now

  public void destroy() {
    tierStore.destroy();
  }

  public URI getURI() {
    return tierStore.getBaseURI();
  }


}
