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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

/**
 * Kafka connect - sink connector config implementation for ampool
 *
 * Since version: 1.2.3
 */
public class AmpoolSinkConnectorConfig extends AbstractConfig {

  public static final String LOCATOR_HOST = "locator.host";
  public static final String LOCATOR_HOST_DOC = "hostname of ampool locator";
  public static final String LOCATOR_PORT = "locator.port";
  public static final String LOCATOR_PORT_DOC = "port of ampool locator";
  public static final String BATCH_SIZE = "batch.size";
  public static final String BATCH_SIZE_DOC = "Count of records in each polling";
  public static final String TABLES = "ampool.tables";
  public static final String TABLES_DOC = "Ampool tables";
  public static final String TOPICS = "topics";
  public static final String TOPIC_PREFIX_DOC = "Topics";
  public static final String TABLES_ROWKEY_COLUMNS = "ampool.tables.rowkey.column";
  public static final String TABLES_ROWKEY_COLUMNS_DOC = "rowkey to be used for a MTable, needs to specify in a TABLENAME:ROWKEY format";


  public static final int DEFALUT_BATCH_SIZE = 1000;
  public static final String DEFALUT_TABLES_ROWKEY_COLUMNS = "none";

  //Retry configuration in case of task failure.

  public static final String ERROR_POLICY = "error.policy";
  public static final String ERROR_POLICY_DEFAULT = "RETRY";
  public static final String ERROR_POLICY_DOC =
      "Error policy to take into action when task fails";

  public static final String MAX_RETRIES = "max.retries";
  public static final int MAX_RETRIES_DEFAULT = 10;
  public static final String MAX_RETRIES_DOC =
      "The maximum number of times to retry on errors before failing the sink task.";
  public static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

  public static final String RETRY_INTERVAL_MS = "retry.interval.ms";
  //default is 1 min.
  public static final int RETRY_INTERVAL_MS_DEFAULT = 60000;
  public static final String RETRY_INTERVAL_MS_DOC =
      "The time in milliseconds to wait following an error before a retry attempt is made.";
  public static final String RETRY_INTERVAL_MS_DISPLAY = "Retry Interval (millis)";

  public AmpoolSinkConnectorConfig(Map<String, String> props) {
    super(config, props);
  }

  public static final ConfigDef config = new ConfigDef()
      .define(LOCATOR_HOST, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, LOCATOR_HOST_DOC)
      .define(LOCATOR_PORT, Type.INT, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, LOCATOR_PORT_DOC)
      .define(BATCH_SIZE, Type.INT, DEFALUT_BATCH_SIZE, Importance.LOW,  BATCH_SIZE_DOC)
      .define(TABLES, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.LOW, TABLES_DOC)
      .define(TOPICS, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.LOW, TOPIC_PREFIX_DOC)
      .define(TABLES_ROWKEY_COLUMNS, Type.STRING, DEFALUT_TABLES_ROWKEY_COLUMNS, Importance.LOW, TABLES_ROWKEY_COLUMNS_DOC)
      .define(ERROR_POLICY, Type.STRING, ERROR_POLICY_DEFAULT, Importance.MEDIUM, ERROR_POLICY_DOC)
      .define(MAX_RETRIES, Type.INT, MAX_RETRIES_DEFAULT, Importance.MEDIUM, MAX_RETRIES_DOC)
      .define(RETRY_INTERVAL_MS, Type.INT, RETRY_INTERVAL_MS_DEFAULT, Importance.MEDIUM, RETRY_INTERVAL_MS_DOC
      );
  public static ConfigDef getConfig() {
    return config;
  }

  public static void main(String[] args) {
    System.out.println(config.toRst());
  }

  /**
   * Validates the properties to ensure the rowkey property is configured for each table.
   */
  /*public void validate() {
    final String topicsAsStr = properties.get(ConnectorConfig.TOPICS_CONFIG);
    final String[] topics = topicsAsStr.split(",");
    for(String topic : topics) {
      String key = String.format(TABLE_ROWKEY_COLUMNS_TEMPLATE, topic);
      if(!properties.containsKey(key)) {
        throw new ConfigException(String.format(" No rowkey has been configured for table [%s]", key));
      }
    }
  }*/

}
