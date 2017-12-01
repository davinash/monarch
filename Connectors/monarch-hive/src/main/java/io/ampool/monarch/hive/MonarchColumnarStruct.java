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

import java.util.List;

import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarStruct;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryFactory;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Provides the respective {@link LazyBinaryObject} required for deserialization by
 * the Hive query engine. For the data-types with custom serialization/deserialization
 * it uses {@link MonarchLazyBinaryObject} as holder class for deserialization whereas
 * for the other types respective lazy-binary-object is used.
 * <p>
 * Created on: 2016-01-06
 * Since version: 0.2.0
 */
public class MonarchColumnarStruct extends LazyBinaryColumnarStruct {

  /**
   * Constructor..
   *
   * @param oi the object inspector
   * @param notSkippedColumnIDs not skipped column IDs
   */
  public MonarchColumnarStruct(ObjectInspector oi, List<Integer> notSkippedColumnIDs) {
    super(oi, notSkippedColumnIDs);
  }

  /**
   * Create the respective objects for lazy loading of the data. For the data-types
   * that have custom serialization/deserialization implemented use the respective
   * deserialization whereas others use Hive's lazy objects.
   *
   * @param oi the object inspector
   * @return the corresponding lazy object
   */
  @Override
  protected LazyObjectBase createLazyObjectBase(final ObjectInspector oi) {
    if (MonarchPredicateHandler.isMonarchTypeSupported(oi)) {
      /** since union cannot be represented by any standard Java object, we need to use
       * Hive's StandardUnion class.. hence it needs to be wrapped separately.
       */
      if (ObjectInspector.Category.UNION.equals(oi.getCategory())) {
        return new MonarchLazyUnionObject(oi);
      } else {
        return new MonarchLazyBinaryObject(oi);
      }
    } else {
      return LazyBinaryFactory.createLazyBinaryObject(oi);
    }
  }
}
