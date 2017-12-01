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

package io.ampool.monarch.table.coprocessor.internal;

import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MTableObserver;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MCoprocessorManager
 *
 * @since 0.2.0.0
 */

public class MCoprocessorManager {

  private static Map<String, CoprocessorContainer> regionToCoprocessor = new HashMap<>();

  /**
   * Returns observers for region
   *
   * @param region Region name
   * @return returns the list of observers for the given region
   */
  public static List<MTableObserver> getObserversList(String region) {
    CoprocessorContainer coprocessorContainer = regionToCoprocessor.get(region);
    // GEN-897
    // Added check for null ptr.
    if (coprocessorContainer != null && regionToCoprocessor.get(region) != null) {
      return regionToCoprocessor.get(region).getObservers();
    }
    return new ArrayList<MTableObserver>();
  }

  /**
   * Returns end point coprocessor for region
   *
   * @param region
   * @return returns the list of endpoint coprocessors for the given region
   */
  public static List<MCoprocessor> getEndpointsList(String region) {
    return regionToCoprocessor.get(region).getCoprocessors();
  }

  public static void registerCoprocessors(String regionName, List<String> coproList) {
    List<MTableObserver> observersList = new ArrayList();
    List<MCoprocessor> endpointsList = new ArrayList();
    for (String className : coproList) {
      Object instance = MCoprocessorUtils.createInstance(className);
      if (instance instanceof MTableObserver) {
        MTableObserver observer = (MTableObserver) instance;
        observersList.add(observer);
        MCoprocessorUtils.getLogger().info("Adding observer " + observer.getClass().getName());
      } else {
        MCoprocessorUtils.getLogger()
            .info("CLASS " + className + " is not valid observer or endpoint!");
        if (!(instance instanceof MCoprocessor)) {
          throw new MCoprocessorException(
              "MCoprocessorException::CLASS " + className + " is not valid observer or endpoint!");
        }
      }
    }
    CoprocessorContainer container = new CoprocessorContainer();
    if (regionToCoprocessor.containsKey(regionName)) {
      container = regionToCoprocessor.get(regionName);
    }
    container.addObserver(observersList);
    regionToCoprocessor.put(regionName, container);
  }

  public static void registerCoprocessor(String regionName, MCoprocessor endpoint) {
    CoprocessorContainer container = new CoprocessorContainer();
    if (regionToCoprocessor.containsKey(regionName)) {
      container = regionToCoprocessor.get(regionName);
    }
    container.addCoprocessor(endpoint);
    // container.addObserver(observersList);
    regionToCoprocessor.put(regionName, container);
  }

  public static void unRegisterCoprocessors(String regionName) {
    MCoprocessorUtils.getLogger().info("Unregistering co processors for Table: " + regionName);
    if (regionToCoprocessor.containsKey(regionName)) {
      regionToCoprocessor.remove(regionName);
    }
  }

}
