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

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Kafka connect - source connector task implementation template.
 * Source connector implementation is not added.
 *
 * Since version: 1.2.3
 */
public class AmpoolSourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(AmpoolSourceTask.class);

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    //TODO: Do things here that are required to start your task. This could be open a connection to a database, etc.
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    //TODO: Create SourceRecord objects that will be sent the kafka cluster.
    throw new UnsupportedOperationException("This has not been implemented.");
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }
}