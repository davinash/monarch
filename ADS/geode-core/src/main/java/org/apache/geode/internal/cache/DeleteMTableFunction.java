/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

/**
 * Function used by the 'create disk-store' gfsh command to create a disk store on each member.
 * 
 * @since 8.0
 */

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.logging.log4j.Logger;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class DeleteMTableFunction extends FunctionAdapter implements InternalEntity {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;


  public DeleteMTableFunction() {
    super();
  }

  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("In DeleteMTableFunction");
      }
      final Object[] args = (Object[]) context.getArguments();
      String MTableName = (String) args[0];

      MTableName = MTableName.replace("/", "");
      if (logger.isDebugEnabled()) {
        logger.debug("MTableName: DeleteMTableFunction" + MTableName);
        logger.debug("Gettting cache");
      }
      MCache cache = MCacheFactory.getAnyInstance();
      if (logger.isDebugEnabled()) {
        logger.debug("Got cache");
      }
      if (cache.getAdmin().tableExists(MTableName)) {
        cache.getAdmin().deleteTable(MTableName);
        if (logger.isDebugEnabled()) {
          logger.debug("Deleted Table");
        }
        context.getResultSender().lastResult(new CliFunctionResult(memberId, true, "Success"));
      } else {
        context.getResultSender().lastResult(null);
      }

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);

      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();

    }
  }

  @Override
  public String getId() {
    return DeleteMTableFunction.class.getName();
  }
}
