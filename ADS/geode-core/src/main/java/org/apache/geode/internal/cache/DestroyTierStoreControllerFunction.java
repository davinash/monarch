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
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.ftable.exceptions.TierStoreBusyException;
import io.ampool.monarch.table.ftable.exceptions.TierStoreNotAvailableException;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.*;
import org.apache.geode.internal.InternalEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class DestroyTierStoreControllerFunction extends FunctionAdapter implements InternalEntity {

  private static final long serialVersionUID = 7446320116589277374L;

  public DestroyTierStoreControllerFunction() {
    super();
  }

  @Override
  public void execute(FunctionContext context) {
    String storeName = ((String) context.getArguments());
    Lock distLock = null;
    Exception ex = null;
    try {
      MCache cache = MCacheFactory.getAnyInstance();
      final Region<Object, Object> storeMetaRegion =
          cache.getRegion(MTableUtils.AMPL_STORE_META_REGION_NAME);
      distLock = storeMetaRegion.getDistributedLock(storeName);
      // locking on region name
      distLock.lock();
      final Object existingDesc = storeMetaRegion.get(storeName);
      if (existingDesc != null) {

        /* Check for references to the store */
        StringBuilder sb = new StringBuilder();
        boolean inuse = false;
        final Region<String, TableDescriptor> tableMetaRegion =
            cache.getRegion(MTableUtils.AMPL_META_REGION_NAME);
        for (Map.Entry<String, TableDescriptor> tableEntry : tableMetaRegion.entrySet()) {
          TableDescriptor td = tableEntry.getValue();
          if (td instanceof MTableDescriptor) {
            continue;
          }
          Map<String, TierStoreConfiguration> tierStoreInfo =
              ((FTableDescriptor) td).getTierStores();

          if (tierStoreInfo == null || tierStoreInfo.size() == 0) {
            continue;
          }
          if (tierStoreInfo.keySet().contains(storeName)) {
            sb.append(inuse == false ? "" : ", ");
            sb.append(tableEntry.getKey());
            inuse = true;
          }
        }
        if (inuse) {
          throw new TierStoreBusyException("Tier store is being used by tables: " + sb.toString());
        }

        /* Remove/deregister store from all servers */
        Function tierStoreDeletionFunction = new TierStoreDeletionFunction();

        Execution members =
            FunctionService.onMembers(MTableUtils.getAllDataMembers(cache)).withArgs(storeName);
        List deleteTierStoreResult =
            (ArrayList) members.execute(tierStoreDeletionFunction.getId()).getResult();

        Boolean finalRes = true;
        for (int i = 0; i < deleteTierStoreResult.size(); i++) {
          Object receivedObj = deleteTierStoreResult.get(i);
          if (receivedObj instanceof Boolean) {
            finalRes = finalRes && (Boolean) receivedObj;
          }
          if (receivedObj instanceof Exception) {
            ex = (Exception) receivedObj;
          }
        }
        if (finalRes && ex == null) {
          storeMetaRegion.destroy(storeName);
        }
      } else {
        throw new TierStoreNotAvailableException("Tier store " + storeName + " Does not exist");
      }
    } catch (Exception re) {
      ex = re;
    } finally {
      if (distLock != null) {
        distLock.unlock();
      }
      if (ex != null) {
        context.getResultSender().sendException(ex);
      }
      context.getResultSender().lastResult(true);
    }
  }

  @Override
  public String getId() {
    return getClass().getName();
  }
}
