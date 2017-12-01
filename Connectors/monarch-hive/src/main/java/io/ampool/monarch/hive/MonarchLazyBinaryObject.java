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

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import io.ampool.monarch.types.DataTypeFactory;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

/**
 * Defines the basic holder-class for lazy deserialization - from bytes to the
 * respective object type.
 * This class uses a {@link Writable} to hold the data that is provided to
 * the Hive query engine, as required. It uses various Writable classes,
 * like IntWritable, LongWritable etc., to hold the data.
 * <p>
 * These are created (newInstance) and set (invoke) using reflection. Each
 * Writable class has a method called "set" that takes respective primitive
 * class as an argument. This method is used to set the writable data.
 * <p>
 * This is kind of eager deserialization for complex types like list/map.
 * For Simple/Basic objects deserialization happens when the objects are retrieved,
 * but for complex types deserialization happens for all children, recursively, at
 * same time unlike Hive's objects extending LazyObjectBase.

 * Created on: 2016-01-06
 * Since version: 0.2.0
 */
public class MonarchLazyBinaryObject extends LazyBinaryObject<ObjectInspector> {
  DataType objectType;
  Object object;
  Function<Object, Object> fn;

  private static final Function<Object, Object> CONVERT_STRING_TO_HIVE_CHAR =
    (Function<Object, Object> & Serializable) (new PrimitiveObjectInspectorConverter
      .HiveCharConverter(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaHiveCharObjectInspector)::convert);

  private static final Function<Object, Object> CONVERT_STRING_TO_HIVE_VAR_CHAR =
    (Function<Object, Object> & Serializable) (new PrimitiveObjectInspectorConverter
      .HiveVarcharConverter(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaHiveVarcharObjectInspector)::convert);

  private static final Function<Object, Object> CONVERT_BIG_DECIMAL_TO_HIVE_DECIMAL =
    (Function<Object, Object> & Serializable) (e -> HiveDecimal.create((BigDecimal) e));

  private static final Map<String, Function<Object, Object>> POST_DESERIALIZE_FUNCTION_MAP =
    new HashMap<String, Function<Object, Object>>(5) {{
      put(serdeConstants.CHAR_TYPE_NAME, CONVERT_STRING_TO_HIVE_CHAR);
      put(serdeConstants.VARCHAR_TYPE_NAME, CONVERT_STRING_TO_HIVE_VAR_CHAR);
      put(serdeConstants.DECIMAL_TYPE_NAME, CONVERT_BIG_DECIMAL_TO_HIVE_DECIMAL);
    }};

  /**
   * Constructor.. create the object-type required for deserialization..
   *
   * @param oi the object inspector
   */
  public MonarchLazyBinaryObject(final ObjectInspector oi) {
    super(oi);
    this.objectType = DataTypeFactory.getTypeFromString(oi.getTypeName(),
      MonarchPredicateHandler.TYPE_HIVE_TO_MONARCH_MAP, POST_DESERIALIZE_FUNCTION_MAP);
  }

  @Override
  public int hashCode() {
    return object == null ? 0 : object.hashCode();
  }

  /**
   * Initialize lazy binary object.
   *
   * @param bar the byte array to be used for deserialization
   * @param offset the offset in byte-array
   * @param length length of the actual byte-array
   */
  @Override
  public void init(final ByteArrayRef bar, int offset, int length) {
    object = objectType.deserialize(bar.getData(), offset, length);
  }
  /**
   * Provide the object after deserialization.
   *
   * @return the object
   */
  @Override
  public Object getObject() {
    return object;
  }
}
