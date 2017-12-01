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

package io.ampool.monarch.table.coprocessor.internal;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;

import java.util.concurrent.TimeUnit;

/**
 * MResultCollectorImpl
 *
 * @since 0.2.0.0
 */

public class MResultCollectorImpl implements MResultCollector {

  private ResultCollector resultCollector;

  public MResultCollectorImpl(ResultCollector resultCollector) {
    this.resultCollector = resultCollector;
  }

  public ResultCollector getResultCollector() {
    return resultCollector;
  }

  @Override
  public Object getResult() throws FunctionException {
    return resultCollector.getResult();
  }

  @Override
  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    return resultCollector.getResult();
  }

  @Override
  public void addResult(DistributedMember memberID, Object resultOfSingleExecution) {
    resultCollector.addResult(memberID, resultOfSingleExecution);
  }

  @Override
  public void endResults() {
    resultCollector.endResults();
  }

  @Override
  public void clearResults() {
    resultCollector.clearResults();
  }
}
