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

import java.util.HashMap;
import java.util.Map;

public class StoreHierarchy {
  private static StoreHierarchy instance = null;
  private int numTiers = 0;
  private final Map<Integer, String> storeNameTierMap = new HashMap<>();

  private StoreHierarchy() {}

  public static StoreHierarchy getInstance() {
    if (instance == null) {
      synchronized (StoreHierarchy.class) {
        if (instance == null) {
          instance = new StoreHierarchy();
        }
      }
    }
    return instance;
  }

  public void addStore(final String storeName) {
    // TODO : Implemented
    /** add the tier only if it not present **/
    if (!this.storeNameTierMap.containsValue(storeName)) {
      synchronized (this.storeNameTierMap) {
        this.numTiers++;
        this.storeNameTierMap.put(numTiers, storeName);
      }
    }
  }

  /**
   * Remove a store from hierarchy
   */
  public void removeStore(final String storeName) {
    int tier = 0;
    for (Map.Entry<Integer, String> entry : storeNameTierMap.entrySet()) {
      if (entry.getValue().compareTo(storeName) == 0) {
        tier = entry.getKey();
        break;
      }
    }
    if (tier != 0) {
      synchronized (this.storeNameTierMap) {
        if (this.storeNameTierMap.remove(tier) != null) {
          numTiers--;
        }
      }
    }
  }

  public String getStoreName(int tier) {
    return this.storeNameTierMap.get(tier);
  }

  public int getNumTiers() {
    return numTiers;
  }
}
