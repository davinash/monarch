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

import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.internal.AdminImpl;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;


public class DestroyTierStoreFunction extends FunctionAdapter implements InternalEntity {

  private static final long serialVersionUID = 3672468300307404629L;

  @Override
  public void execute(FunctionContext context) {
    String memberNameOrId = CliUtil.getMemberNameOrId(
        CacheFactory.getAnyInstance().getDistributedSystem().getDistributedMember());
    String storeName = ((String) context.getArguments());
    AdminImpl admin = (AdminImpl) MCacheFactory.getAnyInstance().getAdmin();
    try {
      admin.destroyTierStore(storeName);
    } catch (Exception e) {
      context.getResultSender().sendException(e);
      return;
    }
    context.getResultSender().lastResult(new CliFunctionResult(memberNameOrId, true, "Success"));
  }

  @Override
  public String getId() {
    return getClass().getName();
  }
}
