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

import org.apache.geode.management.TierStoreMXBean;

import javax.management.NotificationBroadcasterSupport;

/**
 * DiskStore MBean represent a TierStore which provides tier storage for one or more tables.
 * 
 * 
 */
public class TierStoreMBean extends NotificationBroadcasterSupport implements TierStoreMXBean {

  private TierStoreMBeanBridge bridge;

  public TierStoreMBean(TierStoreMBeanBridge bridge) {
    this.bridge = bridge;
  }


  public TierStoreMBeanBridge getBridge() {
    return bridge;
  }

  public void stopMonitor() {
    bridge.stopMonitor();
  }

  /**
   * Returns the number of backups of this TierStore that have been completed.
   */
  @Override
  public int getTotalWritersCreated() {
    return bridge.getTierStoreWriters();
  }

  @Override
  public long getBytesWritten() {
    return bridge.getBytesWritten();
  }

  /**
   * Returns the total number of bytes read from tiers.
   */
  @Override
  public long getBytesRead() {
    return bridge.getBytesRead();
  }

  // TODO: To take up the implementation later

  // /**
  // * Destroy the tier-store and cleanup any resources it may have been managing.
  // */
  // @Override
  // public void destroy() {
  // bridge.destroy();
  // }
  //
  // /**
  // * Destroy the tier-store and cleanup any resources it may have been managing.
  // */
  // @Override
  // public URI listURI() {
  // return bridge.getURI();
  // }
}
