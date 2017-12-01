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
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;

import java.util.Map;


public class DescribeTierStoreFunction extends FunctionAdapter implements InternalEntity {
  private static final long serialVersionUID = -330450516219085170L;

  @Override
  public void execute(FunctionContext context) {
    String storeName = ((String) context.getArguments());
    try {
      Map<String, Object> storeInfoMap = MTableUtils.getStoreInfo(storeName);
      context.getResultSender().lastResult(storeInfoMap);
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  }

  @Override
  public String getId() {
    return getClass().getName();
  }
}
