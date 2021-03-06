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
package io.ampool.monarch.kafka.connect.logical;

import io.ampool.monarch.kafka.connect.SinkFieldConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;

import java.sql.Timestamp;

/**
 * TimeFieldConverter, Defines contract for the Time conversion
 * Converts from kafka connect Time to ampool compatible java.sql.Timestamp
 *
 * @since 1.5.1
 */
public class TimeFieldConverter extends SinkFieldConverter {

  public TimeFieldConverter() {
    super(Time.SCHEMA);
  }

  public TimeFieldConverter(Schema schema) {
    super(schema);
  }

  @Override
  public Timestamp toAmpool(Object data) {
    //Get a ampool supported java.sql.Timestamp
    return new Timestamp(((java.util.Date)data).getTime());
  }
}
