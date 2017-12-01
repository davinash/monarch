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

package org.apache.geode.internal.cache;

import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class GetTierStoresFunction implements Function, InternalEntity {
  private static final long serialVersionUID = -8352523648695192816L;

  public GetTierStoresFunction() {
    super();
  }

  @Override
  public String getId() {
    return GetTierStoresFunction.class.getName();
  }

  @Override
  public void execute(FunctionContext context) {
    try {
      Set<String> allTierStores = new HashSet<>();
      Region metaRegion =
          CacheFactory.getAnyInstance().getRegion(MTableUtils.AMPL_STORE_META_REGION_NAME);
      if (metaRegion == null || metaRegion.size() == 0) {
        context.getResultSender().lastResult(allTierStores);
      } else {
        Set set = metaRegion.keySet();
        Iterator iterator = set.iterator();
        while (iterator.hasNext()) {
          Object next = iterator.next();
          allTierStores.add((String) next);
        }
        context.getResultSender().lastResult(allTierStores);
      }
    } catch (CacheClosedException e) {
      context.getResultSender().sendException(e);
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  }
}
