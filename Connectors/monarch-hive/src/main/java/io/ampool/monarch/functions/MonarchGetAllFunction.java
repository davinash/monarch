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
package io.ampool.monarch.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;

import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.types.MPredicateHolder;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;

/**
 * A sample GetAll function that returns the only values that match the specified
 *    predicate. For example, the values are expected to be stored as 2-D byte array
 *    i.e. byte[][] where first ary indicates the column and other the column data
 *    (in bytes).
 * This is supposed to demonstrate how the predicate push-down (filter/function)
 *    should work. It de-serializes the individual column-value and executes the
 *    predicate specified predicate. Only elements matching the specified predicate
 *    are returned to the caller.
 *
 * Created on: 2015-12-15
 * Since version: 0.2.0
 */
public class MonarchGetAllFunction implements Declarable, Function {
  /**
   * C'tor..
   */
  public MonarchGetAllFunction() {
  }

  @Override
  public void init(Properties properties) {
    // empty..
  }

  @Override
  public void execute(FunctionContext functionContext) {
    Object[] args = (Object[])functionContext.getArguments();
    List<Object> output = new ArrayList<>(5);
    MPredicateHolder ph = (MPredicateHolder) args[1];

    int colId = ph.columnIdx;
    Predicate p = ph.getPredicate();

    try {
      Cache cache = CacheFactory.getAnyInstance();
      Region<Object, Object> region = cache.getRegion(args[0].toString());
      for (Map.Entry<Object, Object> e : region.entrySet()) {
        /** TODO: refactor below to handle generically.. various conditions **/
        if (colId < 0) {
          String key = e.getKey() instanceof MTableKey ? new String(((MTableKey)e.getKey()).getBytes()) : e.getKey().toString();
          if (p.test(key)) {
            String val = e.getValue() instanceof byte[] ? new String(Arrays.copyOfRange((byte[])e.getValue(), 12, ((byte[])e.getValue()).length)) : e.getValue().toString();
            output.add(new String[]{key,val});
          }
        } else {
          byte[][] bytes = (byte[][]) e.getValue();
          Object o = ph.getArgObjectType().deserialize(bytes[colId]);
          if (p.test(o)) {
            output.add(bytes);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    functionContext.getResultSender().lastResult(output);
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }
}
