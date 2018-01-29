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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.exceptions.FTableExistsException;
import org.apache.geode.CopyHelper;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.*;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.MException;
import io.ampool.monarch.table.exceptions.MTableExistsException;
import io.ampool.monarch.table.internal.MTableUtils;


/**
 * This is a controller function which will be executed on one of the server. This will perform
 * following activites 1) Take a entry level lock on ampool meta region entry whose key is name of
 * the table to be created 2) Create respective geode region on each server participating in the
 * distributed system. 3) If all passed, add entry to the ampool meta region with table descriptor
 * used to create table. 4) Release the lock and return as true 5) If step 2 is failed, then
 * rollback the complete operation, release the lock and return false
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public final class CreateMTableControllerFunction extends FunctionAdapter
    implements InternalEntity {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 5374008317019350750L;

  public CreateMTableControllerFunction() {
    super();
  }

  @Override
  public void execute(FunctionContext context) {
    List<Object> args = (List<Object>) context.getArguments();

    String tableName = (String) args.get(0);
    TableDescriptor tableDescriptor = (TableDescriptor) args.get(1);
    Lock distLock = null;
    Exception ex = null;
    try {
      // Actual create mtable code, this will always be on servers
      MCache cache = MCacheFactory.getAnyInstance();
      final Region<Object, Object> metaRegion = cache.getRegion(MTableUtils.AMPL_META_REGION_NAME);
      distLock = metaRegion.getDistributedLock(tableName);
      // locking on region name
      distLock.lock();
      final Object existingDesc = metaRegion.get(tableName);
      if (existingDesc == null) {
        logger.debug("Instantiating CreateMTableControllerFunction on distributed member "
            + cache.getDistributedSystem().getDistributedMember());
        // call function on each member to create region
        Function tableCreationFunction = new MTableCreationFunction();

        java.util.List<Object> inputList = new java.util.ArrayList<Object>();
        inputList.add(tableName);
        inputList.add(tableDescriptor);
        Execution members =
            FunctionService.onMembers(MTableUtils.getAllDataMembers(cache)).withArgs(inputList);
        List createTableResult =
            (ArrayList) members.execute(tableCreationFunction.getId()).getResult();

        Boolean finalRes = true;
        for (int i = 0; i < createTableResult.size(); i++) {
          Object receivedObj = createTableResult.get(i);
          if (receivedObj instanceof Boolean) {
            finalRes = finalRes && (Boolean) receivedObj;
          } else if (receivedObj instanceof Exception) {
            finalRes = finalRes && false;
            ex = (Exception) receivedObj;
          }
        }
        if (!finalRes) {
          // something has failed
          // Do rollback
          logger.debug("Rolling back create MTable process for MTable: " + tableName);
          if (cache.getRegion(tableName) != null) {
            cache.getRegion(tableName).destroyRegion();
          }

          logger.debug("Rollback complete for MTable: " + tableName);
        } else {
          // originally we were setting key space here
          // but after GEN-725 it is removed...
          metaRegion.put(tableName, CopyHelper.copy(tableDescriptor));
        }
      } else {
        if (existingDesc instanceof FTableDescriptor) {
          throw new FTableExistsException("Table " + tableName + " already exists");
        }
        throw new MTableExistsException("Table " + tableName + " already exists");
      }
    } catch (GemFireException | MException re) {
      ex = re;
    } finally {
      // unlock if lock is taken
      if (distLock != null) {
        distLock.unlock();
      }
      if (ex != null) {
        logger.error("CreateMTableControllerFunction:execute:: Exception -> " + ex);
        context.getResultSender().sendResult(false);
        context.getResultSender().sendException(ex);
      } else {
        context.getResultSender().lastResult(true);
      }
    }
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }
}

