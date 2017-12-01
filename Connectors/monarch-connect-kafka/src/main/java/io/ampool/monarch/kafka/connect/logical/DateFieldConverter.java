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
import org.apache.kafka.connect.data.Date;

/**
 * DateFieldConverter, Defines contract for the date conversion
 * Converts from kafka connect date (java.util.Date) to ampool compatible date (java.sql.Date)
 *
 * @since 1.5.1
 */
public class DateFieldConverter extends SinkFieldConverter {

  public DateFieldConverter() {
    super(Date.SCHEMA);
  }

  @Override
  public java.sql.Date toAmpool(Object data) {
    //convert date from java.util.Date to ampool supported date format, i.e java.sql.Date
    return  new java.sql.Date(((java.util.Date)data).getTime());
  }
}
