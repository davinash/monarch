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
import io.ampool.tierstore.*;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;


public class CreateTierStoreFunction extends FunctionAdapter implements InternalEntity {
  private static final Logger logger = LogService.getLogger();

  public CreateTierStoreFunction() {
    super();
  }

  @Override
  public void execute(FunctionContext context) {
    String memberNameOrId = CliUtil.getMemberNameOrId(
        CacheFactory.getAnyInstance().getDistributedSystem().getDistributedMember());
    Map<String, Object> properties = (Map) context.getArguments();

    try {
      String name = (String) properties.get(TierStoreFactory.STORE_NAME_PROP);
      if (name == null) {
        throw new IllegalArgumentException("Tier store name can not be null");
      }

      String handler = (String) properties.get(TierStoreFactory.STORE_HANDLER_PROP);
      if (handler == null) {
        throw new IllegalArgumentException("Tier store handler can not be null");
      }

      Properties storeOpts = (Properties) properties.get(TierStoreFactory.STORE_OPTS_PROP);

      String readerClass = (String) properties.get(TierStoreFactory.STORE_READER_PROP);
      if (readerClass == null) {
        throw new IllegalArgumentException("Tier store reader can not be null");
      }

      String writerClass = (String) properties.get(TierStoreFactory.STORE_WRITER_PROP);
      if (writerClass == null) {
        throw new IllegalArgumentException("Tier store writer can not be null");
      }

      Properties readerOpts = (Properties) properties.get(TierStoreFactory.STORE_READER_PROPS_PROP);
      Properties writerOpts = (Properties) properties.get(TierStoreFactory.STORE_WRITER_PROPS_PROP);

      AdminImpl admin = (AdminImpl) MCacheFactory.getAnyInstance().getAdmin();
      admin.createTierStore(name, handler, storeOpts, writerClass, writerOpts, readerClass,
          readerOpts);
    } catch (Exception e) {
      context.getResultSender()
          .lastResult(new CliFunctionResult(memberNameOrId, false, e.toString()));
      return;
    }
    context.getResultSender().lastResult(new CliFunctionResult(memberNameOrId, true, "Success"));
  }

  @Override
  public String getId() {
    return CreateTierStoreFunction.class.getName();
  }
}
