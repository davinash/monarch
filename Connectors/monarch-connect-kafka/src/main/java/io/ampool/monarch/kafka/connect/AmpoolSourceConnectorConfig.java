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
 * Kafka connect - source connector config implementation template.
 * Source connector implementation is not added.
 *
 * Since version: 1.2.3
 */
public class AmpoolSourceConnectorConfig extends AbstractConfig {

  public static final String MY_SETTING_CONFIG = "my.setting";
  private static final String MY_SETTING_DOC = "This is a setting important to my connector.";

  public AmpoolSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public AmpoolSourceConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(MY_SETTING_CONFIG, Type.STRING, Importance.HIGH, MY_SETTING_DOC);
  }

  public String getMy(){
    return this.getString(MY_SETTING_CONFIG);
  }
}
