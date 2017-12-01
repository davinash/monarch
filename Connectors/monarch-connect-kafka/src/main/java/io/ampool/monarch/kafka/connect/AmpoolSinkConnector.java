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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.ampool.monarch.kafka.connect.AmpoolSinkConnectorConfig.*;

/**
 * Kafka connect sink connector implementation for ampool
 *
 * Since version: 1.2.3
 */
public class AmpoolSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(AmpoolSinkConnector.class);
  private AmpoolSinkConnectorConfig config;
  private Map<String, String> configProperties;

  private String locatorHost;
  private String locatorPort;
  private String batchSize;
  private String tables;
  private String topics;
  private String rowKeyColumns;

  private String errorPolicy;
  private String retryInterval;
  private String maxRetries;

  @Override
  public String version()
  {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    /**
     * This will be executed once per connector. This can be used to handle connector level setup.
     */

    log.info("Parsing configuration!");

    locatorHost = map.get(LOCATOR_HOST);
    if (locatorHost == null || locatorHost.isEmpty()) {
      throw new ConnectException("Missing " + LOCATOR_HOST + " config");
    }

    locatorPort = map.get(LOCATOR_PORT);
    if (locatorPort == null || locatorPort.isEmpty()) {
      throw new ConnectException("Missing " + LOCATOR_PORT + " config");
    }

    batchSize = map.get(BATCH_SIZE);
    if (batchSize == null || batchSize.isEmpty()){
      batchSize =  String.valueOf(AmpoolSinkConnectorConfig.DEFALUT_BATCH_SIZE);
      map.put(BATCH_SIZE, batchSize);
    }

    tables = map.get(TABLES);

    topics = map.get(TOPICS);

    rowKeyColumns = map.get(TABLES_ROWKEY_COLUMNS);
    if(rowKeyColumns!= null) {
      if (!ConnectorUtils.validate(rowKeyColumns)) {
        throw new ConfigException("RowKey columns are not specified in expected format, expected format is - TOPIC1:KEY1,TOPIC2:KEY2");
      }
    }

    if (tables.split(",").length != topics.split(",").length) {
      throw new ConfigException("The number of topics should be the same as the number of tables");
    }

    errorPolicy = map.get(ERROR_POLICY);
    retryInterval = map.get(RETRY_INTERVAL_MS);
    maxRetries = map.get(MAX_RETRIES);

    if (retryInterval == null || retryInterval.isEmpty()){
      retryInterval =  String.valueOf(AmpoolSinkConnectorConfig.RETRY_INTERVAL_MS_DEFAULT);
      map.put(RETRY_INTERVAL_MS, retryInterval);
    }

    if (maxRetries == null || maxRetries.isEmpty()){
      maxRetries =  String.valueOf(AmpoolSinkConnectorConfig.MAX_RETRIES_DEFAULT);
      map.put(MAX_RETRIES, maxRetries);
    }

    if(ConnectorUtils.isNotValidInt(retryInterval) || ConnectorUtils.isNotValidInt(maxRetries)){
      throw new ConfigException("Invalid configuration specified for retry-interval or max-retries!");
    }

    ConnectorUtils.dumpConfiguration(map, log);

    try {
      configProperties = map;
      config = new AmpoolSinkConnectorConfig(map);
    } catch (ConfigException e) {
      throw new ConfigException("Couldn't start AmpoolSinkConnector due to configuration error", e);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    //Return your task implementation.
    return AmpoolSinkTask.class;
  }

  /**
   * Returns a set of configurations for Tasks based on the current configuration,
   * producing at most maxTasks configurations.
   *
   * @param maxTasks maximum number of task to start
   * @return configurations for tasks
   */
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    //Define the individual task configurations that will be executed.
    //This is used to schedule the number of tasks that will be running. This should not exceed maxTasks.
    List<Map<String, String>> taskConfigs = new ArrayList<>();

    //Check partitoning equally among group of node.
    // refer ConnectorUtils.groupPartition

    Map<String, String> taskProps = new HashMap<>();
    taskProps.putAll(configProperties);
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
    //TODO: Do things that are necessary to stop your connector.
  }

  @Override
  public ConfigDef config() {
    return AmpoolSinkConnectorConfig.getConfig();
  }
}
