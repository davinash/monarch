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

import io.ampool.conf.Constants;
import io.ampool.monarch.kafka.connect.logical.DateFieldConverter;
import io.ampool.monarch.kafka.connect.logical.DecimalFieldConverter;
import io.ampool.monarch.kafka.connect.logical.TimeFieldConverter;
import io.ampool.monarch.kafka.connect.logical.TimestampFieldConverter;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MConfiguration;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.Table;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Kafka connect - sink connector task implementation for ampool
 * <p>
 * Since version: 1.2.3
 */
public class AmpoolSinkTask extends SinkTask {
  private static Logger log = LoggerFactory.getLogger(AmpoolSinkTask.class);
  private String locatorHost;
  private Integer locatorPort;
  private Integer batchSize;
  private String tables;
  private String topics;
  private String rowKeyColumns;

  private String errorPolicy;
  private String retryInterval;
  private String maxRetries;

  private int remainingRetries;

  private Map<String, Table> topicToTableMapping;
  private Map<String, String> topicToKeyColumn;

  private List<String> tablesList;
  private List<String> topicsList;

  MClientCache clientCache;

  public int getRemainingRetries() {
    return remainingRetries;
  }

  @Override
  public String version() {
    return new AmpoolSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> map) {
    //Create resources like database or api connections here.
    AmpoolSinkConnectorConfig config = new AmpoolSinkConnectorConfig(map);
    //do we need to validate config here?
    //config.validate();
    SchemaUtils.registerFieldConvertor(new DateFieldConverter());
    SchemaUtils.registerFieldConvertor(new DecimalFieldConverter());
    SchemaUtils.registerFieldConvertor(new TimeFieldConverter());
    SchemaUtils.registerFieldConvertor(new TimestampFieldConverter());

    try {
      locatorPort = Integer.parseInt(map.get(AmpoolSinkConnectorConfig.LOCATOR_PORT));
    } catch (Exception e) {
      throw new ConnectException("Setting " + AmpoolSinkConnectorConfig.LOCATOR_PORT + " should be an integer");
    }

    try {
      batchSize = Integer.parseInt(map.get(AmpoolSinkConnectorConfig.BATCH_SIZE));
    } catch (Exception e) {
      throw new ConnectException("Setting " + AmpoolSinkConnectorConfig.BATCH_SIZE + " should be an integer");
    }

    rowKeyColumns = map.get(AmpoolSinkConnectorConfig.TABLES_ROWKEY_COLUMNS);
    topicToKeyColumn = new HashMap<>();

    if (rowKeyColumns != null) {
      if (ConnectorUtils.validate(rowKeyColumns)) {
        for (String pair : rowKeyColumns.split("\\s*,\\s*")) {
          String[] pairs = pair.split(":");
          topicToKeyColumn.put(pairs[0], pairs[1]);
        }
      } else {
        throw new ConfigException("RowKey columns are not specified in expected format, expected format is - TOPIC1:KEY1,TOPIC2:KEY2");
      }
    }

    locatorHost = map.get(AmpoolSinkConnectorConfig.LOCATOR_HOST);
    tables = map.get(AmpoolSinkConnectorConfig.TABLES);
    topics = map.get(AmpoolSinkConnectorConfig.TOPICS);

    tablesList = Arrays.asList(tables.split("\\s*,\\s*"));
    topicsList = Arrays.asList(topics.split("\\s*,\\s*"));

    if (tablesList.size() != topicsList.size()) {
      throw new ConfigException("Number of topics and ampool tables must be same!");
    }

    //retry config
    errorPolicy = map.get(AmpoolSinkConnectorConfig.ERROR_POLICY);
    retryInterval = map.get(AmpoolSinkConnectorConfig.RETRY_INTERVAL_MS);
    maxRetries = map.get(AmpoolSinkConnectorConfig.MAX_RETRIES);

    if (ConnectorUtils.isNotValidInt(retryInterval) || ConnectorUtils.isNotValidInt(maxRetries)) {
      throw new ConfigException("Invalid configuration specified for retry-interval or max-retries!");
    }
    remainingRetries = Integer.parseInt(maxRetries);
    //

    MConfiguration mconf = MConfiguration.create();
    mconf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, locatorHost);
    mconf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, locatorPort);
    mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "/tmp/MTableClientAsKafkaTask.log");
    try {
      clientCache = new MClientCacheFactory().create(mconf);
      topicToTableMapping = new HashMap<>();
      for (int i = 0; i < topicsList.size(); i++) {
        String ampoolTableName = tablesList.get(i);
        MTable mTable = clientCache.getMTable(ampoolTableName);
        if (mTable == null) {
          FTable fTable = clientCache.getFTable(ampoolTableName);
          if (fTable == null) {
            // ignore for now
          } else {
            topicToTableMapping.put(topicsList.get(i), fTable);
          }
        } else {
          topicToTableMapping.put(topicsList.get(i), mTable);
        }
      }
    } catch (Exception ex) {
      if (ex instanceof ServerConnectivityException) {
        log.warn("Ampool cluster is not up and running..!");
      }
    }

  }

  private boolean isAllTablesAvailable() {
    for (int i = 0; i < topicsList.size(); i++) {
      String ampoolTableName = tablesList.get(i);
      MTable mTable = clientCache.getMTable(ampoolTableName);
      if (mTable == null) {
        FTable fTable = clientCache.getFTable(ampoolTableName);
        if (fTable == null) {
          // ignore for now
        } else {
          topicToTableMapping.put(topicsList.get(i), fTable);
        }
      } else {
        topicToTableMapping.put(topicsList.get(i), mTable);
      }
    }
    return true;
  }

  @Override
  public void put(Collection<SinkRecord> collection) {
    if (collection.isEmpty()) {
      return;
    }
    try {
      //At every sinkTask.put() check for the table availability.
      isAllTablesAvailable();

      List<SinkRecord> records = new ArrayList<>(collection);
      for (int i = 0; i < records.size(); i++) {
        Map<String, List<Put>> bulks = new HashMap<>();
        Map<String, List<Record>> fTableRecords = new HashMap<>();

        for (int j = 0; j < batchSize && i < records.size(); j++, i++) {
          SinkRecord record = records.get(i);
          String topic = record.topic();
          Table table = this.topicToTableMapping.get(topic);

          //check for the record.value() type..? This should not throw ClassCastException.
          Struct recordAsStruct = (Struct) record.value();
          Map<String, Object> columnValueMap = SchemaUtils.toAmpoolRecord(recordAsStruct);
          if (table != null) {
            if (table instanceof MTable) {
              byte[] key = null;

              if (topicToKeyColumn.containsKey(topic)) {
                //fetch value of ID from each record
                key = SchemaUtils.toAmpoolKey(recordAsStruct, topicToKeyColumn.get(topic));

              } else {
                key = Bytes.toBytes(record.topic() + "|" + record.kafkaPartition() + "|" + record.kafkaOffset());
              }
              Put put = new Put(key);
              columnValueMap.forEach((k, v) -> put.addColumn(k, v));
              if (bulks.get(topic) == null) {
                bulks.put(topic, new ArrayList<Put>());
              }
              log.trace("Adding to bulk: {}", put.toString());
              bulks.get(topic).add(put);
            } else if (table instanceof FTable) {
              Record record1 = new Record();
              columnValueMap.forEach((k, v) -> record1.add(k, v));
              if (fTableRecords.get(topic) == null) {
                fTableRecords.put(topic, new ArrayList<>());
              }
              fTableRecords.get(topic).add(record1);
            }
          } else {
            log.error("Ampool table corresponding to topic [" + topic + "] is not created..!");
            throw new ServerConnectivityException("Ampool table corresponding to topic [" + topic + "] is not created..!");
          }
        }
        i--;
        log.trace("Executing bulk");
        for (String key : bulks.keySet()) {
          try {
            Table table = topicToTableMapping.get(key);
            if (table != null && table instanceof MTable) {
              ((MTable) table).put(bulks.get(key));
            } else {
              log.error("Ampool table corresponding to topic [" + key + "] is not created..!");
              throw new ServerConnectivityException("Ampool table corresponding to topic [ " + key + " ] is not created..!");
            }
          } catch (Exception e) {
            //System.out.println("Debug Error in batchPut!");
            log.error(e.getMessage());
            if(e instanceof ServerConnectivityException) {
              throw e;
            }
          }
        }
        for (String key : fTableRecords.keySet()) {
          try {
            Table table = topicToTableMapping.get(key);
            if (table != null && table instanceof FTable) {
              List<Record> list = fTableRecords.get(key);
              Record[] entries = list.toArray(new Record[list.size()]);
              ((FTable) table).append(entries);
            } else {
              log.error("Ampool table corresponding to topic [" + key + "] is not created..!");
              throw new ServerConnectivityException("Ampool table corresponding to topic [" + key + "] is not created..!");
            }
          } catch (Exception e) {
            //System.out.println("Debug Error in batchPut!");
            log.error(e.getMessage());
            if(e instanceof ServerConnectivityException) {
              throw e;
            }
          }
        }
      }
    } catch (Exception ex) {
      log.info("Cought exception while trying to ingest data, " + ex.getMessage());
      if(ex instanceof ServerConnectivityException) {
        log.info("Connection to ampool locator failed, remainingRetries={}", remainingRetries);
        if (remainingRetries == 0) {
          throw new ConnectException(ex);
        } else {
          remainingRetries--;
          if(context != null){
            context.timeout(Integer.parseInt(retryInterval));
          }
          throw new RetriableException(ex);
        }
      }
      log.info("All retries are completed, now existing with exception "+ ex.getMessage());
    }
    //reset the remainingRetries
    remainingRetries = Integer.parseInt(maxRetries);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Do nothing as the connector manages the offset
  }

  @Override
  public void stop() {
    //Close resources here.
    //System.out.println("Debug closing the ampool client cache!");
    if (clientCache != null) {
      clientCache.close();
    }
  }

}
