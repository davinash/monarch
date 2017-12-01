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

package io.ampool.conf;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class Constants {
  /**
   * Security configuration.
   */
  public static final class Security {
    public static final String AUTH_SERVER_ADDRESS = "";
    public static final String AUTH_SERVER_BIND_ADDRESS = "";
  }

  /**
   * Properties to specify Ampool locator details like locator host-name and port.
   */
  public static final class MonarchLocator {
    public static final String MONARCH_LOCATOR_PORT = "ampool.monarch.locator.port";
    public static final String MONARCH_LOCATOR_ADDRESS = "ampool.monarch.locator.address";
    /**
     * locators can have the following format address1[port1],address2[port]
     */
    public static final String MONARCH_LOCATORS = "ampool.monarch.locators";
  }


  /**
   * Ampool Client Cache configurations
   */
  public static final class MClientCacheconfig {
    public static final String MONARCH_CLIENT_LOG = "ampool.monarch.client.log";
    public static final String MONARCH_CLIENT_SOCK_BUFFER_SIZE =
        "ampool.monarch.client.socket.buffersize";
    public static final String MONARCH_CLIENT_READ_TIMEOUT = "ampool.monarch.client.read.timeout";
    public static final String ENABLE_META_REGION_CACHING = "enable-meta-region-caching";
    public static final String ENABLE_POOL_SUBSCRIPTION = "enable-pool-subscription";


  }


  /**
   * Generic Configurations
   */

  public static final class General {
    public static final String SERVER_NAME_SEPARATOR = ":";
  }

}
