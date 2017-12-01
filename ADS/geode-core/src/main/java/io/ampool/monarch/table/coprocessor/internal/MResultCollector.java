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
import org.apache.geode.distributed.DistributedMember;

import java.util.concurrent.TimeUnit;

/**
 * MResultCollector provides mechanism to create custom resultCollector. For now default
 * resultCollector is used. This is intended for future release.
 *
 * @since 0.2.0
 */

public interface MResultCollector<T, S> {

  /**
   * Method used to pull results from the ResultCollector. It returns the result of coprocessor
   * execution, potentially blocking until {@link #endResults() all the results are available} has
   * been called.
   *
   * @return the result
   * @throws FunctionException if result retrieval fails
   */
  S getResult() throws FunctionException;

  /**
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return the result
   * @throws FunctionException if result retrieval fails
   * @throws InterruptedException if execution is interrupted
   */
  S getResult(long timeout, TimeUnit unit) throws FunctionException, InterruptedException;

  /**
   * Method used to feed result to the ResultCollector. It adds a single coprocessor execution
   * result to the ResultCollector It is invoked every time a result is sent using ResultSender.
   *
   * @param memberID DistributedMember ID to which result belongs
   * @param resultOfSingleExecution
   */
  void addResult(DistributedMember memberID, T resultOfSingleExecution);

  /**
   * Ampool will invoke this method when coprocessor execution has completed and all results for the
   * execution have been obtained and {@link #addResult(DistributedMember, Object) added to the
   * MResultCollector} Unless the {@link MResultCollector} has received
   * {@link io.ampool.monarch.table.coprocessor.MResultSender#lastResult(Object) last result} from
   * all the executing nodes, it keeps waiting for more results to come.
   *
   */
  void endResults();

  /**
   * This is to clear the previous execution results from resultCollector
   */
  void clearResults();
}
