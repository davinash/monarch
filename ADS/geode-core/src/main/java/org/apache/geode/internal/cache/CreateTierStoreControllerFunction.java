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

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.exceptions.MException;
import io.ampool.monarch.table.exceptions.TierStoreExistsException;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.tierstore.TierStoreFactory;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.*;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.locks.Lock;


@InterfaceAudience.Private
@InterfaceStability.Stable
public class CreateTierStoreControllerFunction extends FunctionAdapter implements InternalEntity {

  private static final Logger logger = LogService.getLogger();

  public CreateTierStoreControllerFunction() {
    super();
  }

  @Override
  public void execute(FunctionContext context) {
    List<Object> args = (List<Object>) context.getArguments();

    String storeName = (String) args.get(0);

    Lock distLock = null;
    Exception ex = null;
    try {
      // Actual create mtable code, this will always be on servers
      MCache cache = MCacheFactory.getAnyInstance();
      final Region<Object, Object> storeMetaRegion =
          cache.getRegion(MTableUtils.AMPL_STORE_META_REGION_NAME);
      distLock = storeMetaRegion.getDistributedLock(storeName);
      // locking on region name
      distLock.lock();
      final Object existingDesc = storeMetaRegion.get(storeName);
      if (existingDesc == null) {
        logger.debug("Instantiating CreateTierStoreControllerFunction on distributed member "
            + cache.getDistributedSystem().getDistributedMember());
        // call function on each member to create region
        Function tierStoreCreationFunction = new TierStoreCreationFunction();


        Execution members =
            FunctionService.onMembers(MTableUtils.getAllDataMembers(cache)).withArgs(args);
        List createTierStoreResult =
            (ArrayList) members.execute(tierStoreCreationFunction.getId()).getResult();

        Boolean finalRes = true;
        for (int i = 0; i < createTierStoreResult.size(); i++) {
          Object receivedObj = createTierStoreResult.get(i);
          if (receivedObj instanceof Boolean) {
            finalRes = finalRes && (Boolean) receivedObj;
          }
          if (receivedObj instanceof Exception) {

            ex = (Exception) receivedObj;
          }
        }
        if (finalRes && ex == null) {
          Map<String, Object> storePropsMap = new HashMap<>();
          storePropsMap.put(TierStoreFactory.STORE_NAME_PROP, storeName);
          storePropsMap.put(TierStoreFactory.STORE_HANDLER_PROP, args.get(1));
          storePropsMap.put(TierStoreFactory.STORE_OPTS_PROP, args.get(2));
          storePropsMap.put(TierStoreFactory.STORE_WRITER_PROP, args.get(3));
          storePropsMap.put(TierStoreFactory.STORE_WRITER_PROPS_PROP, args.get(4));
          storePropsMap.put(TierStoreFactory.STORE_READER_PROP, args.get(5));
          storePropsMap.put(TierStoreFactory.STORE_READER_PROPS_PROP, args.get(6));
          storeMetaRegion.put(storeName, storePropsMap);
        }
      } else {

        throw new TierStoreExistsException("Store " + storeName + " already exists");
      }
    } catch (GemFireException | MException re) {
      ex = re;
    } finally {
      // unlock if lock is taken
      if (distLock != null) {
        distLock.unlock();
      }
      if (ex != null) {
        // context.getResultSender().lastResult(false);
        context.getResultSender().sendException(ex);
      }
      context.getResultSender().lastResult(true);
    }
  }

  @Override
  public String getId() {
    return this.getClass().getName();
  }
}


