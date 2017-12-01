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
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.logging.log4j.Logger;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class CreateMTableFunction extends FunctionAdapter implements InternalEntity {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;


  public CreateMTableFunction() {
    super();
  }

  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    System.out.println("CreateMTableFunction.execute");
    String memberId = "";
    String memberNameOrId = "";
    try {
      memberNameOrId = CliUtil.getMemberNameOrId(
          MCacheFactory.getAnyInstance().getDistributedSystem().getDistributedMember());
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("memberNameOrId = " + memberNameOrId);
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("In CreateMTableFunction");
      }
      final Object[] args = (Object[]) context.getArguments();
      final String mTableName = (String) args[0];
      final TableDescriptor tableDescriptor = (TableDescriptor) args[1];
      if (logger.isDebugEnabled()) {
        logger.debug("TableDescriptor: CreateMTableFunction" + tableDescriptor.toString());
        logger.debug("MTableName: CreateMTableFunction" + mTableName);
      }
      boolean isImmutableTable = false;

      if (tableDescriptor instanceof FTableDescriptor)
        isImmutableTable = true;


      MCache cache = MCacheFactory.getAnyInstance();
      System.out.println("cache = " + cache);
      if (cache.getAdmin().tableExists(mTableName)) {
        logger.info("Table Already exists");
        context.getResultSender().lastResult(null);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Getting Cache: CreateMTableFunction" + cache);
        }
        if (isImmutableTable) {
          logger.info("table descriptor: " + tableDescriptor);
          cache.getAdmin().createFTable(mTableName, (FTableDescriptor) tableDescriptor);
        } else {
          System.out.println("CreateMTableFunction.execute.calling createMTable");
          cache.getAdmin().createMTable(mTableName, (MTableDescriptor) tableDescriptor);
        }

        context.getResultSender().lastResult(new CliFunctionResult(memberId, true, "Success"));
      }
    } catch (CacheClosedException cce) {
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, null));

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;

    } catch (Exception e) {
      SystemFailure.checkFailure();
      logger.error("Could not create Table: {}", e.getMessage(), e);
      String exceptionMsg = e.getMessage();
      if (exceptionMsg == null) {
        exceptionMsg = CliUtil.stackTraceAsString(e);
      }
      context.getResultSender().lastResult(handleException(memberNameOrId, exceptionMsg, e));
    }
  }

  private CliFunctionResult handleException(final String memberNameOrId, final String exceptionMsg,
      final Exception e) {
    if (e != null && logger.isDebugEnabled()) {
      logger.debug(e.getMessage(), e);
    }
    if (exceptionMsg != null) {
      return new CliFunctionResult(memberNameOrId, false, exceptionMsg);
    }

    return new CliFunctionResult(memberNameOrId);
  }

  @Override
  public String getId() {
    return CreateMTableFunction.class.getName();
  }
}
