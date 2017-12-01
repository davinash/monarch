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

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;

import java.util.concurrent.TimeUnit;


public class TableIsEmptyResultCollector implements ResultCollector<Boolean, Boolean> {

  /** Collected table status. */
  private boolean isEmpty = true;

  public synchronized void addResult(DistributedMember memberID, Boolean singleRes) {
    if (singleRes != null) {
      isEmpty = isEmpty && singleRes;
    }
  }

  public void clearResults() {
    isEmpty = true;
  }

  public void endResults() {
    // Nothing to do.
  }

  public Boolean getResult() throws FunctionException {
    return getResult(0, null);
  }

  public Boolean getResult(long timeout, TimeUnit unit) {
    return isEmpty;
  }
}
