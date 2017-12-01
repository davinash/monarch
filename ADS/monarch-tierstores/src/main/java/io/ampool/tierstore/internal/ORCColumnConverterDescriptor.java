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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.ampool.monarch.types.ListType;
import io.ampool.monarch.types.MapType;
import io.ampool.monarch.types.StructType;
import io.ampool.monarch.types.UnionType;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.interfaces.DataType.Category;
import io.ampool.tierstore.ColumnConverterDescriptor;
import org.apache.hadoop.hive.ql.io.orc.FTableOrcStruct;
import org.apache.hadoop.hive.ql.io.orc.FTableOrcUnion;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class ORCColumnConverterDescriptor implements ColumnConverterDescriptor {
  private DataType columnType;
  private String columnName;
  private Function<Object, Object> readerFunction;
  private Function<Object, Object> writerFunction;

  public ORCColumnConverterDescriptor(final DataType dataType, final String colName) {
    this.columnType = dataType;
    this.columnName = colName;
    this.readerFunction = ConverterUtils.ORCReaderFunctions.get(columnType.toString());
    this.writerFunction = ConverterUtils.ORCWriterFunctions.get(columnType.toString());;
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
  public TypeInfo getTypeDescriptor() {
    if (columnType.getCategory() == Category.Basic) {
      return ConverterUtils.ORCTYpeInfoMap.get(columnType.toString());
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
  private static Object getWritableComplexType(Object jObject, DataType type) {
    if (jObject == null) {
      return null;
    }

    Object resultObject = null;
    switch (type.getCategory()) {
      case Basic:
        resultObject = ConverterUtils.ORCWriterFunctions.get(type.toString()).apply(jObject);
        break;
      case Struct:
        Object[] valueArray = (Object[]) jObject;
        StructType structObjectType = (StructType) type;
        FTableOrcStruct structOut = new FTableOrcStruct(valueArray.length);
        for (int i = 0; i < valueArray.length; i++) {
          structOut.setFieldValue(i,
              getWritableComplexType(valueArray[i], structObjectType.getColumnTypes()[i]));
        }
        resultObject = structOut.getOrcStruct();
        break;
      case List:
        List inList = (List) jObject;
        ListType listObjectType = (ListType) type;
        List outList = new ArrayList();
        for (int i = 0; i < inList.size(); i++) {
          outList.add(i, getWritableComplexType(inList.get(i), listObjectType.getTypeOfElement()));
        }
        resultObject = outList;
        break;
      case Union:
        final UnionType unionObjectType = (UnionType) type;
        Object[] unionIn = (Object[]) jObject;
        FTableOrcUnion unionOut = new FTableOrcUnion();
        int tag = (byte) unionIn[0];
        Object value = getWritableComplexType(unionIn[1], unionObjectType.getColumnTypes()[tag]);
        unionOut.set((byte) tag, value);
        resultObject = unionOut.get();
        break;
      case Map:
        Map iMap = (Map) jObject;
        MapType mapObjectType = (MapType) type;
        Map outMap = new HashMap();
        iMap.forEach((K, V) -> {
          outMap.put(getWritableComplexType(K, mapObjectType.getTypeOfKey()),
              getWritableComplexType(V, mapObjectType.getTypeOfValue()));
        });
        resultObject = outMap;
        break;
      default:
        break;

    }
    return resultObject;
  }

  @SuppressWarnings("unchecked")
  private static Object getReadableComplexType(Object wObject, DataType type) {
    if (wObject == null)
      return null;
    Object resultObject = null;
    switch (type.getCategory()) {
      case Basic:
        resultObject = ConverterUtils.ORCReaderFunctions.get(type.toString()).apply(wObject);
        break;
      case Struct:
        final FTableOrcStruct fTableOrcStruct = new FTableOrcStruct((OrcStruct) wObject);
        StructType structObjectType = (StructType) type;
        Object[] valueArray = new Object[fTableOrcStruct.getNumFields()];
        for (int i = 0; i < valueArray.length; i++) {
          valueArray[i] =
              getReadableComplexType(valueArray[i], structObjectType.getColumnTypes()[i]);
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
        FTableOrcUnion unionIn = new FTableOrcUnion(wObject);
        byte tag = unionIn.getTag();
        Object[] unionOut = new Object[2];
        unionOut[0] = tag;
        unionOut[1] =
            getReadableComplexType(unionIn.getObject(), unionObjectType.getColumnTypes()[tag]);
        resultObject = unionOut;
        break;
      case Map:
        Map iMap = (Map) wObject;
        MapType mapObjectType = (MapType) type;
        Map outMap = new HashMap();
        iMap.forEach((K, V) -> {
          outMap.put(getReadableComplexType(K, mapObjectType.getTypeOfKey()),
              getReadableComplexType(V, mapObjectType.getTypeOfValue()));
        });
        resultObject = outMap;
        break;
      default:
        break;
    }
    return resultObject;
  }


  private static TypeInfo getComplexTypeDesc(DataType type) {
    TypeInfo resultTypeInfo = null;
    switch (type.getCategory()) {
      case Basic:
        resultTypeInfo = ConverterUtils.ORCTYpeInfoMap.get(type.toString());
        break;
      case Struct:
        StructType structObjectType = (StructType) type;
        final DataType[] columnTypes = structObjectType.getColumnTypes();
        List<TypeInfo> typeInfos = new ArrayList<>();
        for (int i = 0; i < columnTypes.length; i++) {
          typeInfos.add(getComplexTypeDesc(columnTypes[i]));
        }
        resultTypeInfo = TypeInfoFactory
            .getStructTypeInfo(Arrays.asList(structObjectType.getColumnNames()), typeInfos);;
        break;
      case List:
        ListType listObjectType = (ListType) type;
        resultTypeInfo =
            TypeInfoFactory.getListTypeInfo(getComplexTypeDesc(listObjectType.getTypeOfElement()));
        break;
      case Union:
        final UnionType unionObjectType = (UnionType) type;
        final DataType[] columnTypes1 = unionObjectType.getColumnTypes();
        List<TypeInfo> unionTypeInfos = new ArrayList<>();
        for (int i = 0; i < columnTypes1.length; i++) {
          unionTypeInfos.add(getComplexTypeDesc(columnTypes1[i]));
        }
        resultTypeInfo = TypeInfoFactory.getUnionTypeInfo(unionTypeInfos);
        break;
      case Map:
        MapType mapObjectType = (MapType) type;
        resultTypeInfo =
            TypeInfoFactory.getMapTypeInfo(getComplexTypeDesc(mapObjectType.getTypeOfKey()),
                getComplexTypeDesc(mapObjectType.getTypeOfValue()));
        break;
      default:
        break;

    }
    return resultTypeInfo;
  }


}
