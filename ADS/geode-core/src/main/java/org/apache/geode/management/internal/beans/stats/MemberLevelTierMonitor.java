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
package org.apache.geode.management.internal.beans.stats;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.internal.statistics.StatisticId;
import org.apache.geode.internal.statistics.StatisticNotFoundException;
import org.apache.geode.internal.statistics.StatisticsListener;
import org.apache.geode.internal.statistics.StatisticsNotification;
import org.apache.geode.internal.statistics.ValueMonitor;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO:Add the actual tier level stats . This will require handling
 *
 */
public class MemberLevelTierMonitor extends MBeanStatsMonitor {


  private volatile int queueSize = 0;

  private Map<Statistics, ValueMonitor> monitors;

  private Map<Statistics, MemberLevelTierStatisticsListener> listeners;
  private Number writer;

  public MemberLevelTierMonitor(String name) {
    super(name);
    monitors = new HashMap<Statistics, ValueMonitor>();
    listeners = new HashMap<Statistics, MemberLevelTierStatisticsListener>();
  }

  @Override
  public void addStatisticsToMonitor(Statistics stats) {
    ValueMonitor diskMonitor = new ValueMonitor();
    MemberLevelTierStatisticsListener listener = new MemberLevelTierStatisticsListener();
    diskMonitor.addListener(listener);
    diskMonitor.addStatistics(stats);
    monitors.put(stats, diskMonitor);
    listeners.put(stats, listener);
  }

  @Override
  public void removeStatisticsFromMonitor(Statistics stats) {
    ValueMonitor monitor = monitors.remove(stats);
    if (monitor != null) {
      monitor.removeStatistics(stats);
    }
    MemberLevelTierStatisticsListener listener = listeners.remove(stats);
    if (listener != null) {
      monitor.removeListener(listener);
    }
    listener.decreaseDiskStoreStats(stats);
  }

  @Override
  public void stopListener() {
    for (Statistics stat : listeners.keySet()) {
      ValueMonitor monitor = monitors.get(stat);
      monitor.removeListener(listeners.get(stat));
      monitor.removeStatistics(stat);
    }
    listeners.clear();
    monitors.clear();
  }

  @Override
  public Number getStatistic(String name) {
    return 0;
  }

  public Number getWriter() {
    return writer;
  }

  private class MemberLevelTierStatisticsListener implements StatisticsListener {

    DefaultHashMap statsMap = new DefaultHashMap();

    private boolean removed = false;

    @Override
    public void handleNotification(StatisticsNotification notification) {
      synchronized (statsMap) {
        if (removed) {
          return;
        }
        for (StatisticId statId : notification) {
          StatisticDescriptor descriptor = statId.getStatisticDescriptor();
          String name = descriptor.getName();
          Number value;
          try {
            value = notification.getValue(statId);
          } catch (StatisticNotFoundException e) {
            value = 0;
          }
          log(name, value);

          Number deltaValue = computeDelta(statsMap, name, value);
          statsMap.put(name, value);
          increaseStats(name, deltaValue);

        }

      }
    }

    /**
     * Only decrease those values which can both increase and decrease and not values which can only
     * increase like read/writes
     *
     * Remove last sample value from the aggregate. Last Sampled value can be obtained from the
     * DefaultHashMap for the disk
     *
     * @param stats
     */

    public void decreaseDiskStoreStats(Statistics stats) {
      synchronized (statsMap) {
        queueSize -= statsMap.get(StatsKey.DISK_QUEUE_SIZE).intValue();
        removed = true;

      }

    }

  };

  private Number computeDelta(DefaultHashMap statsMap, String name, Number currentValue) {
    return 0;
  }

  private void increaseStats(String name, Number value) {}

  public int getQueueSize() {
    return queueSize;
  }
}
