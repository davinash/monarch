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
package io.ampool.tierstore.parquet.utils;

import io.ampool.tierstore.internal.ParquetColumnConverterDescriptor;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public class ParquetUtils {

  /**
   * Creates the avro record schema
   * 
   * @param recordName - Name of record.
   * @param columnNames - Column/field names
   * @param schemas - Avro schema for each field/column
   * @return - Avro Record schema
   */
  public static Schema createAvroRecordSchema(String recordName, String[] columnNames,
      Schema[] schemas) {
    Schema record = Schema.createRecord(recordName, null, null, false);
    // create fields
    List<Schema.Field> fields = new ArrayList<>();
    // String field
    for (int i = 0; i < columnNames.length; i++) {
      String fieldName = columnNames[i];
      Schema fieldType = schemas[i];
      Schema.Field field = new Schema.Field(fieldName, fieldType, null, null,
          ParquetColumnConverterDescriptor.DEFAULT_ORDER);
      fields.add(field);
    }
    record.setFields(fields);
    return record;
  }
}
