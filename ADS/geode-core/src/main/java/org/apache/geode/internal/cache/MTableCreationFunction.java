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

import java.util.List;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.MException;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MTableCreationFunction extends FunctionAdapter implements InternalEntity {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = -6927327639168595045L;

  public MTableCreationFunction() {
    super();
  }

  @Override
  public void execute(FunctionContext context) {
    List<Object> args = (List<Object>) context.getArguments();

    final String tableName = (String) args.get(0);
    final TableDescriptor tableDescriptor = (TableDescriptor) args.get(1);
    try {
      MCache cache = MCacheFactory.getAnyInstance();
      MTableUtils.createRegionInGeode((MonarchCacheImpl) cache, tableName, tableDescriptor);
    } catch (GemFireException | MException re) {
      logger.error("createRegionInGeode Failed " + re);
      context.getResultSender().sendResult(false);
      context.getResultSender().sendException(re);
    }
    context.getResultSender().lastResult(true);
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }

}

