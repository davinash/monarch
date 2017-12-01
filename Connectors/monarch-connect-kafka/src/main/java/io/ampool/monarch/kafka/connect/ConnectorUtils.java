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

package io.ampool.monarch.kafka.connect;

import org.slf4j.Logger;

import java.util.Map;

/**
 * Logger utility class
 *
 * Since version: 1.2.3
 */
public class ConnectorUtils {
  public static void dumpConfiguration(Map<String, String> map, Logger log) {
    log.trace("Starting connector with configuration:");
    for (Map.Entry entry : map.entrySet()) {
      log.trace("{}: {}", entry.getKey(), entry.getValue());
    }
  }

  public static boolean validate(String input) {
    for(String pair : input.split(",")){
      if(pair.split(":").length != 2){
        return false;
      }
    }
    return true;
  }

  public static boolean isNotValidInt(String text) {
    try {
      Integer.parseInt(text);
      return false;
    } catch (NumberFormatException e) {
      return true;
    }
  }
}
