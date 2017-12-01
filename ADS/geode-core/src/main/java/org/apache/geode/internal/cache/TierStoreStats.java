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
package org.apache.geode.internal.cache;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * GemFire statistics about a {@link io.ampool.tierstore.TierStore}.
 *
 *
 */
public class TierStoreStats {

  private static final StatisticsType type;

  //////////////////// Statistic "Id" Fields ////////////////////

  private static final int writesId;

  static {
    String statName = "TierStoreStatistics";
    String statDescription = "Statistics about a Table's use of the tier store";

    final String writesDesc = "The total number of writers created for a TierStore.";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    type = f.createType(statName, statDescription,
        new StatisticDescriptor[] {f.createIntCounter("writes", writesDesc, "ops"),});

    // Initialize id fields
    writesId = type.nameToId("writes");
  }

  ////////////////////// Instance Fields //////////////////////

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>TierStoreStatistics</code> for the given table.
   */
  public TierStoreStats(StatisticsFactory f, String name) {
    this.stats = f.createAtomicStatistics(type, name);
  }

  ///////////////////// Instance Methods /////////////////////

  public void close() {
    this.stats.close();
  }

  /**
   * Returns the total number of writers created for the tierstore.
   */
  public int getWriters() {
    return this.stats.getInt(writesId);
  }

  public void puttWriters() {
    this.stats.incInt(writesId, 1);
  }

  public void settWriters() {
    this.stats.setInt(writesId, 0);
  }

  public Statistics getStats() {
    return stats;
  }
}
