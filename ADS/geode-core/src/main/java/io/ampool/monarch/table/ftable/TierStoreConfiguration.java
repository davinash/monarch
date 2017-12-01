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

package io.ampool.monarch.table.ftable;

import java.io.Serializable;
import java.util.Properties;

/**
 * TierStoreConfiguration per table per tier
 * <p>
 * Following properties are supported time-to-expire - Time in hours after which record(s) will be
 * eligible for expiration to next tier partition-interval - Interval in hours to find out eligible
 * partitions for expiration to next tier
 * <p>
 */
public class TierStoreConfiguration implements Serializable {

  /**
   * time to expire in hours.
   */
  public static final String TIER_TIME_TO_EXPIRE = "time-to-expire";

  /**
   * default time to expire in hours.
   */
  public static final long DEFAULT_TIER_TIME_TO_EXPIRE_MS = Long.MAX_VALUE;

  /**
   * tier partitioning interval in hours.
   */
  public static final String TIER_PARTITION_INTERVAL = "partition-interval";

  /**
   * default tier partitioning interval in hours. Value is 10 min / 0.16 hour
   */
  public static final long DEFAULT_TIER_PARTITION_INTERVAL_MS = 600_000L;

  private static final String[] PROPERTIES =
      new String[] {TIER_TIME_TO_EXPIRE, TIER_PARTITION_INTERVAL};

  private Properties tierProperties;
  private Object obj;

  /**
   * Tier configuration for this tier. All time values are in nano-seconds
   * 
   * @return the configuration properties
   */
  public Properties getTierProperties() {
    return tierProperties;
  }

  public Object getObj() {
    return obj;
  }

  /**
   * Creates configuration with default configurations
   */
  public TierStoreConfiguration() {
    this.tierProperties = new Properties();
    this.tierProperties.put(TIER_TIME_TO_EXPIRE, DEFAULT_TIER_TIME_TO_EXPIRE_MS);
    this.tierProperties.put(TIER_PARTITION_INTERVAL, DEFAULT_TIER_PARTITION_INTERVAL_MS);
  }

  @Override
  public String toString() {
    return "TierStoreConfiguration{" + "tierProperties=" + tierProperties + '}';
  }

  public void setTierProperties(Properties properties) {
    for (String property : PROPERTIES) {
      if (properties.containsKey(property)) {
        String s = properties.getProperty(property);
        Double time = Double.parseDouble(s);
        this.tierProperties.put(property, convertToMS(time));
      }
    }
  }

  public static long convertToMS(final Double time) {
    long HOUR_TO_MS = 3600_000L;
    return (long) (HOUR_TO_MS * time);
  }

  /**
   * Will be used later to set convertible objects
   * 
   * @param obj
   */
  /*
   * public void setConverterObj(Object obj) { this.obj = obj; }
   */

}
