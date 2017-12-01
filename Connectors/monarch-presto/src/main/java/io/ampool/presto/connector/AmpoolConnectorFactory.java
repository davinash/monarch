/*
* Copyright (c) 2017 Ampool, Inc. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you
* may not use this file except in compliance with the License. You
* may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
* implied. See the License for the specific language governing
* permissions and limitations under the License. See accompanying
* LICENSE file.
*/

package io.ampool.presto.connector;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.ampool.client.AmpoolClient;
import io.ampool.conf.Constants;

import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class AmpoolConnectorFactory implements ConnectorFactory {
  //TODO: Finish logging
  private static final Logger log = Logger.get(AmpoolConnectorFactory.class);

  public String getName() {
    return "ampool";
  }

  public ConnectorHandleResolver getHandleResolver() {
    log.info("INFORMATION: AmpoolConnectorFactory getHandleResolver() called.");
    return new AmpoolHandleResolver();
  }

  public Connector create(final String connectorId, Map<String, String> requiredConfig,
                          ConnectorContext context) {
    requireNonNull(requiredConfig, "requiredConfig is null");

    final String
        locator_host =
        requiredConfig
            .getOrDefault(MonarchProperties.LOCATOR_HOST, MonarchProperties.LOCATOR_HOST_DEFAULT);
    final int
        locator_port =
        Integer.parseInt(requiredConfig
            .getOrDefault(MonarchProperties.LOCATOR_PORT, MonarchProperties.LOCATOR_PORT_DEFAULT));

    // Create a client that connects to the Ampool cluster via a locator (that is already running!).
    final Properties props = new Properties();
    props.setProperty(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, requiredConfig
        .getOrDefault(MonarchProperties.MONARCH_CLIENT_LOG, MonarchProperties.MONARCH_CLIENT_LOG_DEFAULT_LOCATION));
    final AmpoolClient aClient = new AmpoolClient(locator_host, locator_port, props);
    log.info("INFORMATION: AmpoolClient created successfully.");

    try {
      Bootstrap
          app =
          new Bootstrap(new AmpoolModule(connectorId, aClient, context.getTypeManager()));

      Injector injector = app
          .doNotInitializeLogging()
          .setRequiredConfigurationProperties(requiredConfig)
          .initialize();

      log.info("INFORMATION: Injector initialized successfully.");
      return injector.getInstance(AmpoolConnector.class);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
