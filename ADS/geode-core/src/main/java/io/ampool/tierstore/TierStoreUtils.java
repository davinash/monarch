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

package io.ampool.tierstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.geode.internal.cache.CreateTierStoreControllerFunction;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;

public class TierStoreUtils {

  public static boolean createTierStore(final String name, final String clazz,
      final Properties storeProps, final String writerClass, final Properties writerOpts,
      final String readerClass, final Properties readerOpts, final DistributedMember member)
      throws Exception {
    Function createTierStoreControllerFunction = new CreateTierStoreControllerFunction();
    FunctionService.registerFunction(createTierStoreControllerFunction);
    List<Object> inputList = new ArrayList();
    inputList.add(name);
    inputList.add(clazz);
    inputList.add(storeProps);
    inputList.add(writerClass);
    inputList.add(writerOpts);
    inputList.add(readerClass);
    inputList.add(readerOpts);

    Execution members = FunctionService.onMember(member).withArgs(inputList);
    List createStoreResult = null;
    Boolean finalRes = true;
    Exception ex = null;
    try {
      createStoreResult =
          (ArrayList) members.execute(createTierStoreControllerFunction.getId()).getResult();
    } catch (Exception e) {
      // If function execution is failed due to member failure
      // then handle it by rollbacking...
      // Function HA should handle this.
      ex = e;
      finalRes = false;
    }

    if (createStoreResult != null) {
      for (int i = 0; i < createStoreResult.size(); i++) {
        Object receivedObj = createStoreResult.get(i);
        if (receivedObj instanceof Boolean) {
          finalRes = finalRes && (Boolean) receivedObj;
        }
        if (receivedObj instanceof Exception) {
          ex = (Exception) receivedObj;
        }
      }
    }
    if (!finalRes || ex != null) {
      throw ex;
    }
    return true;
  }
}
