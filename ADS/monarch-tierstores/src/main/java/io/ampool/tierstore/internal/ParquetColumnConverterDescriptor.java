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

package io.ampool.tierstore.internal;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.ListType;
import io.ampool.monarch.types.MapType;
import io.ampool.monarch.types.StructType;
import io.ampool.monarch.types.UnionType;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.DataType.Category;
import io.ampool.tierstore.ColumnConverterDescriptor;
import io.ampool.tierstore.parquet.utils.ParquetUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A converter descriptor column level that will provide serialization and deserilization from
 * Ampool types to AVRO types.
 */
public class ParquetColumnConverterDescriptor implements ColumnConverterDescriptor {
  private DataType columnType;
  private String columnName;
  private Function<Object, Object> readerFunction;
  private Function<Object, Object> writerFunction;

  public static Schema.Field.Order DEFAULT_ORDER = Schema.Field.Order.ASCENDING;

  public static final Map<String, Function<Object, Object>> readerFunctions =
      new HashMap<String, Function<Object, Object>>(20) {
        {
          put(BasicTypes.STRING.name(), e -> e.toString());
          put(BasicTypes.VARCHAR.toString(), e -> e.toString());
          put(BasicTypes.CHARS.toString(), e -> e.toString());
          put(BasicTypes.CHAR.toString(), e -> (e).toString().charAt(0));
          put(BasicTypes.O_INT.name(), e -> ((Integer) e));
          put(BasicTypes.O_LONG.name(), e -> ((Long) e));
          put(BasicTypes.INT.name(), e -> ((Integer) e));
          put(BasicTypes.LONG.name(), e -> ((Long) e));
          put(BasicTypes.DOUBLE.name(), e -> ((Double) e));
          put(BasicTypes.BINARY.name(), e -> e instanceof java.nio.ByteBuffer
              ? ((java.nio.ByteBuffer) e).array() : (byte[]) e);
          put(BasicTypes.BOOLEAN.name(), e -> ((Boolean) e));
          put(BasicTypes.BYTE.name(), e -> e instanceof java.nio.ByteBuffer
              ? ((java.nio.ByteBuffer) e).array()[0] : ((byte[]) e)[0]);
          put(BasicTypes.DATE.name(), e -> new Date(((Long) e)));
          put(BasicTypes.FLOAT.name(), e -> ((Float) e));
          put(BasicTypes.SHORT.name(),
              e -> (e instanceof Short ? (Short) e : ((Integer) e).shortValue()));
          put(BasicTypes.TIMESTAMP.name(), e -> new Timestamp(((Long) e)));
          put(BasicTypes.BIG_DECIMAL.name(),
              // e will be instance of BigDecimal only in case of test
              e -> Bytes.toBigDecimal(e instanceof java.nio.ByteBuffer
                  ? ((java.nio.ByteBuffer) e).array() : (byte[]) e));
        }
      };

  public static final Map<String, Function<Object, Object>> writerFunctions =
      new HashMap<String, Function<Object, Object>>(20) {
        {
          put(BasicTypes.STRING.name(), e -> e.toString());
          put(BasicTypes.VARCHAR.toString(), e -> e.toString());
          put(BasicTypes.CHARS.toString(), e -> e.toString());
          put(BasicTypes.CHAR.toString(), e -> e.toString());
          put(BasicTypes.O_INT.name(), e -> new Integer((int) e));
          put(BasicTypes.O_LONG.name(), e -> new Long((long) e));
          put(BasicTypes.INT.name(), e -> new Integer((int) e));
          put(BasicTypes.LONG.name(), e -> new Long((long) e));
          put(BasicTypes.DOUBLE.name(), e -> new Double((double) e));
          put(BasicTypes.BINARY.name(), e -> (byte[]) e);
          put(BasicTypes.BOOLEAN.name(), e -> new Boolean((boolean) e));
          put(BasicTypes.BYTE.name(), e -> new byte[] {(byte) e});
          put(BasicTypes.DATE.name(), e -> ((Date) e).getTime());
          put(BasicTypes.FLOAT.name(), e -> new Float((float) e));
          put(BasicTypes.SHORT.name(), e -> new Short((short) e));
          put(BasicTypes.TIMESTAMP.name(), e -> ((Timestamp) e).getTime());
          put(BasicTypes.BIG_DECIMAL.name(), e -> Bytes.toBytes((BigDecimal) e));
        }
      };

  public static final Map<String, Schema> parquetAvroSchemaMap = new HashMap<String, Schema>(20) {
    {
      put(BasicTypes.STRING.toString(), Schema.create(Schema.Type.STRING));
      put(BasicTypes.VARCHAR.toString(), Schema.create(Schema.Type.STRING));
      put(BasicTypes.CHARS.toString(), Schema.create(Schema.Type.STRING));
      put(BasicTypes.CHAR.toString(), Schema.create(Schema.Type.STRING));
      put(BasicTypes.O_INT.toString(), Schema.create(Schema.Type.INT));
      put(BasicTypes.O_LONG.toString(), Schema.create(Schema.Type.LONG));
      put(BasicTypes.INT.toString(), Schema.create(Schema.Type.INT));
      put(BasicTypes.LONG.toString(), Schema.create(Schema.Type.LONG));
      put(BasicTypes.DOUBLE.toString(), Schema.create(Schema.Type.DOUBLE));
      put(BasicTypes.BINARY.toString(), Schema.create(Schema.Type.BYTES));
      put(BasicTypes.BOOLEAN.toString(), Schema.create(Schema.Type.BOOLEAN));
      put(BasicTypes.BYTE.toString(), Schema.create(Schema.Type.BYTES));
      put(BasicTypes.DATE.toString(), Schema.create(Schema.Type.LONG));
      put(BasicTypes.FLOAT.toString(), Schema.create(Schema.Type.FLOAT));
      put(BasicTypes.SHORT.toString(), Schema.create(Schema.Type.INT));
      put(BasicTypes.TIMESTAMP.toString(), Schema.create(Schema.Type.LONG));
      put(BasicTypes.BIG_DECIMAL.name(), Schema.create(Schema.Type.BYTES));
    }
  };

  public ParquetColumnConverterDescriptor(final DataType dataType, final String colName) {
    this.columnType = dataType;
    this.columnName = colName;
    this.readerFunction = readerFunctions.get(columnType.toString());
    this.writerFunction = writerFunctions.get(columnType.toString());
  }

  @Override
  public Object getWritable(Object o) {
    return getWritableComplexType(o, columnType);
  }

  @Override
  public Object getReadable(Object o) {
    return getReadableComplexType(o, columnType);
  }

  @Override
  public Schema getTypeDescriptor() {
    if (columnType.getCategory() == Category.Basic) {
      return parquetAvroSchemaMap.get(columnType.toString());
    } else {
      return getComplexTypeDesc(columnType);
    }
  }

  @Override
  public String getColumnName() {
    return this.columnName;
  }

  @Override
  public DataType getColumnType() {
    return this.columnType;
  }

  @SuppressWarnings("unchecked")
  private Object getWritableComplexType(Object jObject, DataType type) {
    if (jObject == null) {
      return null;
    }

    Object resultObject = null;
    switch (type.getCategory()) {
      case Basic:
        resultObject = writerFunction != null ? writerFunction.apply(jObject)
            : writerFunctions.get(type.toString()).apply(jObject);
        break;
      case Struct:
        Object[] valueArray = (Object[]) jObject;
        Schema complexTypeDesc = getComplexTypeDesc(type);
        StructType structObjectType = (StructType) type;
        resultObject = new GenericData.Record(complexTypeDesc);
        for (int i = 0; i < valueArray.length; i++) {
          ((GenericRecord) resultObject).put(i,
              getWritableComplexType(valueArray[i], structObjectType.getColumnTypes()[i]));
        }
        break;
      case List:
        List inList = (List) jObject;
        ListType listObjectType = (ListType) type;
        List outList = new ArrayList();
        for (int i = 0; i < inList.size(); i++) {
          outList.add(getWritableComplexType(inList.get(i), listObjectType.getTypeOfElement()));
        }
        resultObject = outList;
        break;
      case Union:
        final UnionType unionObjectType = (UnionType) type;
        Object[] unionIn = (Object[]) jObject;
        int tag = (byte) unionIn[0];
        resultObject = getWritableComplexType(unionIn[1], unionObjectType.getColumnTypes()[tag]);
        break;
      case Map:
        Map iMap = (Map) jObject;
        MapType mapObjectType = (MapType) type;
        // Parquet/Avro only support string as a key type
        Map<String, Object> outMap = new HashMap();
        iMap.forEach((K, V) -> {
          outMap.put(String.valueOf(K), getWritableComplexType(V, mapObjectType.getTypeOfValue()));
        });
        resultObject = outMap;
        break;
      default:
        break;

    }
    return resultObject;
  }

  @SuppressWarnings("unchecked")
  private Object getReadableComplexType(Object wObject, DataType type) {
    if (wObject == null) {
      return null;
    }
    Object resultObject = null;
    switch (type.getCategory()) {
      case Basic:
        resultObject = readerFunction != null ? readerFunction.apply(wObject)
            : readerFunctions.get(type.toString()).apply(wObject);
        break;
      case Struct:
        final GenericRecord record = (GenericRecord) wObject;
        StructType structObjectType = (StructType) type;
        int fields = structObjectType.getColumnNames().length;
        Object[] valueArray = new Object[fields];
        for (int i = 0; i < valueArray.length; i++) {
          valueArray[i] =
              getReadableComplexType(record.get(i), structObjectType.getColumnTypes()[i]);
        }
        resultObject = valueArray;
        break;
      case List:
        List list = (List) wObject;
        ListType listObjectType = (ListType) type;
        List outList = new ArrayList();
        for (int i = 0; i < list.size(); i++) {
          outList.add(i, getReadableComplexType(list.get(i), listObjectType.getTypeOfElement()));
        }
        resultObject = outList;
        break;
      case Union:
        final UnionType unionObjectType = (UnionType) type;
        Schema unionSchema = getComplexTypeDesc(unionObjectType);
        int unionObjectIndex = GenericData.get().resolveUnion(unionSchema, wObject);
        Object[] unionOut = new Object[2];
        unionOut[0] = unionObjectIndex;
        unionOut[1] =
            getReadableComplexType(wObject, unionObjectType.getColumnTypes()[unionObjectIndex]);
        resultObject = unionOut;
        break;
      case Map:
        Map iMap = (Map) wObject;
        MapType mapObjectType = (MapType) type;
        Map outMap = new HashMap();
        iMap.forEach((K, V) -> {
          outMap.put(K, getReadableComplexType(V, mapObjectType.getTypeOfValue()));
        });
        resultObject = outMap;
        break;
      default:
        break;
    }
    return resultObject;
  }


  private Schema getComplexTypeDesc(DataType type) {
    Schema resultTypeInfo = null;
    switch (type.getCategory()) {
      case Basic:
        resultTypeInfo = parquetAvroSchemaMap.get(type.toString());
        break;
      case Struct:
        // create record here
        StructType structObjectType = (StructType) type;

        List<DataType> dataTypes = Arrays.asList(structObjectType.getColumnTypes());
        Schema[] schemas = dataTypes.stream().map(dataType -> getComplexTypeDesc(dataType))
            .toArray(size -> new Schema[size]);

        resultTypeInfo = ParquetUtils.createAvroRecordSchema(columnName,
            structObjectType.getColumnNames(), schemas);
        break;
      case List:
        resultTypeInfo =
            Schema.createArray(getComplexTypeDesc(((ListType) type).getTypeOfElement()));
        break;
      case Union:
        final UnionType unionObjectType = (UnionType) type;
        final DataType[] columnTypes1 = unionObjectType.getColumnTypes();
        List<Schema> colTypes = new ArrayList<>();
        for (int i = 0; i < columnTypes1.length; i++) {
          colTypes.add(getComplexTypeDesc(columnTypes1[i]));
        }
        resultTypeInfo = Schema.createUnion(colTypes);
        break;
      case Map:
        MapType mapObjectType = (MapType) type;
        resultTypeInfo = Schema.createMap(getComplexTypeDesc(mapObjectType.getTypeOfValue()));
        break;
      default:
        break;

    }
    return resultTypeInfo;
  }


}
