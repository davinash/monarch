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

import io.ampool.monarch.types.interfaces.DataType;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;

/**
 * Defines the holder-class for lazy deserialization of objects of union type. Since
 * union cannot be represented by any standard Java object, we have to use the Hive's
 * {@link org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion}
 * representation. So this is just wraps the actual Java object and the respective tag
 * and returns to Hive.
 * <p>
 *
 * Created on: 2016-01-25
 * Since version: 0.2.0
 */
public class MonarchLazyUnionObject extends LazyBinaryObject<ObjectInspector> {
  private StandardUnionObjectInspector.StandardUnion unionObj;
  DataType objectType;

  /**
   * Constructor.. create object-type required for deserialization. Also, create
   * and reuse the (Hive's) StandardUnion object used to represent post deserialization.
   *
   * @param oi the object inspector
   */
  public MonarchLazyUnionObject(final ObjectInspector oi) {
    super(oi);
    this.objectType = MonarchPredicateHandler.getMonarchFieldType(oi.getTypeName());
    unionObj = new StandardUnionObjectInspector.StandardUnion();
  }

  @Override
  public int hashCode() {
    return unionObj == null ? 0 : unionObj.hashCode();
  }

  /**
   * Deserialize and construct the StandardUnion object from byte-array.
   *
   * @param bar    the byte array to be used for deserialization
   * @param offset the offset in byte-array
   * @param length length of the actual byte-array
   */
  @Override
  public void init(ByteArrayRef bar, int offset, int length) {
    Object[] objects = (Object[]) objectType.deserialize(bar.getData(), offset, length);
    unionObj.setTag((byte) objects[0]);
    unionObj.setObject(objects[1]);
  }

  /**
   * Provide the object after deserialization.
   *
   * @return the object
   */
  @Override
  public Object getObject() {
    return unionObj;
  }
}