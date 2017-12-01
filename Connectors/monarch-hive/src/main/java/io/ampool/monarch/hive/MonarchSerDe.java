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
package io.ampool.monarch.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import io.ampool.monarch.types.interfaces.DataType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStructBase;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Monarch SerDe for Hive.
 * It is a columnar based SerDe where serialization and deserialization happens per column.
 * The data for the column is converted to/from bytes rather then single byte-array per row.
 * <p>
 * The deserialization for For Simple/Basic types happens when the objects are retrieved.
 * But for complex types, like list/map/struct/union, the deserialization for all children,
 * ,
 * deserialization for complex types like list/map.
 * For Simple/Basic objects deserialization happens when the objects are retrieved,
 * but for complex types deserialization happens for all children, recursively, at
 * same time unlike Hive's objects extending LazyObjectBase.
 * <p>

 */
public class MonarchSerDe extends ColumnarSerDeBase {
  private List<String> columnList = Collections.emptyList();
  private List<TypeInfo> typeInfoList = Collections.emptyList();
  private List<DataType> objectTypeList;

  /** the bytes buffers required for ser-de **/
  BytesRefArrayWritable serializeCache = new BytesRefArrayWritable();
  BytesRefWritable field[];
  ByteStream.Output serializeStream = new ByteStream.Output();
  ColumnarStructBase cachedLazyStruct;
  ObjectInspector rowOI;

  /** Logger **/
  private static final Logger logger = LoggerFactory.getLogger(MonarchSerDe.class);

  public MonarchSerDe() throws SerDeException {
    super();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    String[] cols = tbl.getProperty("columns").split(",");
    String types = tbl.getProperty("columns.types");
    if (types == null) {
      types = Collections.nCopies(cols.length, "string").stream().collect(Collectors.joining(","));
    }

    this.columnList = Arrays.asList(cols);
    this.typeInfoList = TypeInfoUtils.getTypeInfosFromTypeString(types);

    /** initialize storage for fields **/
    int size = columnList.size();
    field = new BytesRefWritable[size];
    for (int i = 0; i < size; i++) {
      field[i] = new BytesRefWritable();
      serializeCache.set(i, field[i]);
    }
    serializedSize = 0;

    /** the columns to skip **/
    List notSkipIDs = new ArrayList();
    if(conf != null && !ColumnProjectionUtils.isReadAllColumns(conf)) {
      notSkipIDs = ColumnProjectionUtils.getReadColumnIDs(conf);
    } else {
      for(int i = 0; i < typeInfoList.size(); ++i) {
        notSkipIDs.add(i);
      }
    }

    /**
     * create the object inspector for row.. use native Java object inspectors for
     * the objects for which deserialization is done by us and not Hive.
     * Cache Monarch object types as well.. for all rows (serialize)..
     */
    List<ObjectInspector> oiList = new ArrayList<>(columnList.size());
    this.objectTypeList = new ArrayList<>(columnList.size());
    for (final TypeInfo ti : typeInfoList) {
      DataType type = null;
      try {
        type = MonarchPredicateHandler.getMonarchFieldType(ti.getTypeName());
      } catch (Exception e) {
        //
      }
      if (type != null) {
        oiList.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(ti));
      } else {
        oiList.add(LazyBinaryUtils.getLazyBinaryObjectInspectorFromTypeInfo(ti));
      }
      this.objectTypeList.add(type);
    }
    this.rowOI = ObjectInspectorFactory.getColumnarStructObjectInspector(columnList, oiList);

    /** Initialize the lazy structure for on-demand de-serialization **/
    this.cachedLazyStruct = new MonarchColumnarStruct(rowOI, notSkipIDs);
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new SerDeException(getClass().toString()
        + " can only serialize struct types, but we got: "
        + objInspector.getTypeName());
    }


    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);

    LazyBinarySerDe.BooleanRef warnedOnceNullMapKey = new LazyBinarySerDe.BooleanRef(false);
    serializeStream.reset();
    serializedSize = 0;
    int streamOffset = 0;
    // Serialize each field
    for (int i = 0; i < fields.size(); i++) {
      // Get the field objectInspector and the field object.
      ObjectInspector foi = fields.get(i).getFieldObjectInspector();
      Object f = (list == null ? null : list.get(i));
      //empty strings are marked by an invalid utf single byte sequence. A valid utf stream cannot
      //produce this sequence
      if ((f != null) && (foi.getCategory().equals(ObjectInspector.Category.PRIMITIVE))
        && ((PrimitiveObjectInspector) foi).getPrimitiveCategory().equals(
        PrimitiveObjectInspector.PrimitiveCategory.STRING)
        && ((StringObjectInspector) foi).getPrimitiveJavaObject(f).length() == 0) {
        serializeStream.write(INVALID_UTF__SINGLE_BYTE, 0, 1);
      } else {
        if (MonarchPredicateHandler.isMonarchTypeSupported(foi)) {
          /** wherever possible use our serialization **/
          try {
            serializeStream.write(objectTypeList.get(i).serialize(convertToJavaObject(foi, f)));
          } catch (IOException e) {
            logger.error("Failed to serialize Field= {}, Type= {}",
              fields.get(i).getFieldName(), foi.getTypeName(), e);
          }
        } else {
          /** for the rest continue to use LazyBinarySerDe as binary/bytes **/
          LazyBinarySerDe.serialize(serializeStream, f, foi, true, warnedOnceNullMapKey);
        }
      }
      field[i].set(serializeStream.getData(), streamOffset, serializeStream.getLength() - streamOffset);
      streamOffset = serializeStream.getLength();
    }
    serializedSize = serializeStream.getLength();
    lastOperationSerialize = true;
    lastOperationDeserialize = false;
    return serializeCache;
  }

  /**
   * Convert the provided lazy object to respective Java object using the specified
   * object-inspector. The serialization APIs ({@link io.ampool.monarch.types.BasicTypes})
   * need all java objects and cannot interpret the hive lazy objects hence need to convert
   * pure Java objects before serialization.
   * The primitive and complex (List/Map/Struct) objects are all lazy objects and are interpreted
   * only along with the respective object inspector.
   * <p>
   * @param oi the object inspector
   * @param lazyObject the lazy object
   * @return the respective Java object
   */
  private Object convertToJavaObject(final ObjectInspector oi, final Object lazyObject) {
    Object jObject = null;
    switch (oi.getCategory()) {
      case PRIMITIVE:
        jObject = ((PrimitiveObjectInspector) oi).getPrimitiveJavaObject(lazyObject);
        if (oi instanceof HiveDecimalObjectInspector) {
          jObject = ((HiveDecimal)jObject).bigDecimalValue();
        }
        break;
      case LIST:
        final ListObjectInspector loi = (ListObjectInspector) oi;
        final List<?> oList = loi.getList(lazyObject);
        if (oList != null) {
          final ObjectInspector eoi = loi.getListElementObjectInspector();
          final List<Object> jList = new ArrayList<>(oList.size());
          oList.forEach(el -> jList.add(convertToJavaObject(eoi, el)));
          jObject = jList;
        }
        break;
      case MAP:
        final MapObjectInspector moi = (MapObjectInspector) oi;
        final Map<?, ?> oMap = moi.getMap(lazyObject);
        if (oMap != null) {
          final ObjectInspector koi = moi.getMapKeyObjectInspector();
          final ObjectInspector voi = moi.getMapValueObjectInspector();
          final Map<Object, Object> jMap = new LinkedHashMap<>(oMap.size());
          oMap.forEach((k,v) -> jMap.put(convertToJavaObject(koi, k), convertToJavaObject(voi, v)));
          jObject = jMap;
        }
        break;
      case STRUCT:
        final StructObjectInspector soi = (StructObjectInspector) oi;
        final List<?> fList = soi.getStructFieldsDataAsList(lazyObject);
        if (fList != null) {
          int size = fList.size();
          final List<? extends StructField> ois = soi.getAllStructFieldRefs();
          final Object[] jObjects = new Object[size];
          int i = 0;
          for (int j = 0; j < size; j++) {
            jObjects[i++] = convertToJavaObject(ois.get(j).getFieldObjectInspector(), fList.get(j));
          }
          jObject = jObjects;
        }
        break;
      case UNION:
        final UnionObjectInspector uoi = (UnionObjectInspector) oi;
        final List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
        if (ois != null) {
          final Object[] jObjects = new Object[2];
          final byte tag = uoi.getTag(lazyObject);
          jObjects[0] = tag;
          jObjects[1] = convertToJavaObject(ois.get(tag), uoi.getField(lazyObject));
          jObject = jObjects;
        }
        break;
    }
    return jObject;
  }

  static final byte[] INVALID_UTF__SINGLE_BYTE = {(byte)Integer.parseInt("10111111", 2)};

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    if(!(blob instanceof BytesRefArrayWritable)) {
      throw new SerDeException(this.getClass().toString() + ": expects BytesRefArrayWritable!");
    } else {
      BytesRefArrayWritable cols = (BytesRefArrayWritable)blob;
      this.cachedLazyStruct.init(cols);
      this.lastOperationSerialize = false;
      this.lastOperationDeserialize = true;
      return this.cachedLazyStruct;
    }
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return rowOI;
  }

  public String toString() {
    return this.getClass().getName() + "[" + this.getColumnNames() +
      " --> " + this.getColumnTypes() + "]";
  }

  public List<String> getColumnNames() {
    return this.columnList;
  }

  public List<TypeInfo> getColumnTypes() {
    return this.typeInfoList;
  }
}
