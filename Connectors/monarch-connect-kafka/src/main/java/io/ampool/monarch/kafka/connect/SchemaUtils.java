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

import io.ampool.monarch.table.Bytes;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Schema utility class
 * <p>
 * Since version: 1.2.3
 */
public class SchemaUtils {
  private static Logger logger = LoggerFactory.getLogger(AmpoolSinkTask.class);

  public static final Set<String> LOGICAL_TYPE_NAMES = new HashSet<>(
      Arrays.asList(org.apache.kafka.connect.data.Date.LOGICAL_NAME, Decimal.LOGICAL_NAME,
          Time.LOGICAL_NAME, Timestamp.LOGICAL_NAME)
  );

  public static Map<String, SinkFieldConverter> logicalConverters = new HashMap<>();

  public static void registerFieldConvertor(SinkFieldConverter converter){
    SchemaUtils.logicalConverters.put(converter.getSchema().name(), converter);
  }

  private static boolean isSupportedLogicalType(Schema schema) {

    if(schema.name() == null) {
      return false;
    }
    return LOGICAL_TYPE_NAMES.contains(schema.name());
  }

  private static SinkFieldConverter getConverter(Schema schema) {

    SinkFieldConverter converter = null;

    if(isSupportedLogicalType(schema)) {
      converter = logicalConverters.get(schema.name());
    }

    if (converter == null) {
      throw new ConnectException("error no registered converter found for " + schema.type().getName());
    }

    return converter;
  }

  private static void addLogicalTypeField(Map<String, Object> columnNameToValueMap, Field field, Struct struct){
    //Debug statements
    /*
    logger.debug("SchemaUtils.addLogicalTypeField columnName = " + field.name());
    logger.debug("SchemaUtils.addLogicalTypeField columnType = " + field.schema().type());
    logger.debug("SchemaUtils.addLogicalTypeField columnValue = " + struct.get(field));
    */
    columnNameToValueMap.put(field.name(), getConverter(field.schema()).toAmpool(struct.get(field), field.schema()));
    return;
  }

  /**
   * @param struct
    * @return Map containing columnName-ColumnValue
   */
  public static Map<String, Object> toAmpoolRecord(Struct struct) {
    Map<String, Object> columnNameToValueMap = new HashMap<String, Object>(0);
    List<Field> fields = struct.schema().fields();

    for (Field field : fields) {
      String columnName = field.name();
      Schema.Type columnType = field.schema().type();

      if (isSupportedLogicalType(field.schema())) {
        //add logicalType field
        addLogicalTypeField(columnNameToValueMap, field, struct);
        //columnNameToValueMap.put(columnName, getValueForLogicalType(struct.get(field), field.schema()));
      } else {

        switch (columnType) {
        case INT8:
          columnNameToValueMap.put(columnName, struct.getInt8(columnName));
          break;
        case BOOLEAN:
          columnNameToValueMap.put(columnName, struct.getBoolean(columnName));
          break;
        case STRING:
          columnNameToValueMap.put(columnName, struct.getString(columnName));
          break;
        case INT16:
          columnNameToValueMap.put(columnName, struct.getInt16(columnName));
          break;
        case INT32:
          columnNameToValueMap.put(columnName, struct.getInt32(columnName));
          break;
        case INT64:
          columnNameToValueMap.put(columnName, struct.getInt64(columnName));
          break;
        case FLOAT32:
          columnNameToValueMap.put(columnName, struct.getFloat32(columnName));
          break;
        case FLOAT64:
          columnNameToValueMap.put(columnName, struct.getFloat64(columnName));
          break;
        case BYTES:
          columnNameToValueMap.put(columnName, struct.getBytes(columnName));
          break;
        case STRUCT:
          columnNameToValueMap.put(columnName, struct.getStruct(columnName));
          break;
        }
      }
    }
    return columnNameToValueMap;
  }

  public static byte[] toAmpoolKey(Struct struct, String keyColumn) {
    Field rowKeyField = struct.schema().field(keyColumn);
    byte[] rowKey = null;

    if (rowKeyField == null || !rowKeyField.name().equals(keyColumn)) {
      throw new IllegalStateException("keyColumn [" + keyColumn + "] does not exist in the schema.");
    }
    String columnName = rowKeyField.name();

    Schema.Type columnType = rowKeyField.schema().type();
    switch (columnType) {
    case INT8:
      return Bytes.toBytes(struct.getInt8(columnName));
    case BOOLEAN:
      return Bytes.toBytes(struct.getBoolean(columnName));

    case STRING:
      return Bytes.toBytes(struct.getString(columnName));

    case INT16:
      return Bytes.toBytes(struct.getInt16(columnName));

    case INT32:
      return Bytes.toBytes(struct.getInt32(columnName));

    case INT64:
      return Bytes.toBytes(struct.getInt64(columnName));

    case FLOAT32:
      return Bytes.toBytes(struct.getFloat32(columnName));

    case FLOAT64:
      return Bytes.toBytes(struct.getFloat64(columnName));

    case BYTES:
      return struct.getBytes(columnName);
    }
    throw new IllegalStateException("Key column type [" + columnType + " ] not supported.");
  }
}
