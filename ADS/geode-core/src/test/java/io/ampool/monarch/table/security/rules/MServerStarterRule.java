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
package io.ampool.monarch.table.security.rules;

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;

import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.junit.rules.ExternalResource;

import java.io.Serializable;
import java.util.Properties;

public class MServerStarterRule extends ExternalResource implements Serializable {

  public MCache cache;
  public CacheServer server;

  private Properties properties;

  public MServerStarterRule(Properties properties) {
    this.properties = properties;
  }

  public void startServer() throws Exception {
    startServer(0, false);
  }

  public void startServer(int locatorPort) throws Exception {
    startServer(locatorPort, false);
  }

  public void startServer(int locatorPort, boolean pdxPersistent) throws Exception {
    if (!properties.containsKey(MCAST_PORT)) {
      properties.setProperty(MCAST_PORT, "0");
    }
    if (!properties.containsKey(NAME)) {
      properties.setProperty(NAME, this.getClass().getName());
    }
    if (!properties.containsKey(LOCATORS)) {
      if (locatorPort > 0) {
        properties.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
      } else {
        properties.setProperty(LOCATORS, "");
      }
    }
    if (properties.containsKey(JMX_MANAGER_PORT)) {
      int jmxPort = Integer.parseInt(properties.getProperty(JMX_MANAGER_PORT));
      if (jmxPort > 0) {
        if (!properties.containsKey(JMX_MANAGER))
          properties.put(JMX_MANAGER, "true");
        if (!properties.containsKey(JMX_MANAGER_START))
          properties.put(JMX_MANAGER_START, "true");
      }
    }

    properties.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    properties.setProperty(DistributionConfig.LOG_FILE_NAME, "system.log");

    MCacheFactory cf = new MCacheFactory(properties);
    cache = cf.create();
    server = cache.addCacheServer();
    server.setPort(0);
    server.start();
    MCacheFactory.getAnyInstance();
  }

  /**
   * if you use this class as a rule, the default startServer will be called in the before. You need
   * to make sure your properties to start the server with has the locator information it needs to
   * connect to, otherwise, this server won't connect to any locator
   */
  protected void before() throws Throwable {
    startServer();
  }

  @Override
  public void after() {
    // make sure this cache is the once currently open. A server cache can be recreated due to
    // importing a new set of cluster configuration.
    cache = MonarchCacheImpl.getInstance();
    if (cache != null) {
      cache.close();
      cache = null;
    }
    if (server != null) {
      server.stop();
      server = null;
    }
  }
}
