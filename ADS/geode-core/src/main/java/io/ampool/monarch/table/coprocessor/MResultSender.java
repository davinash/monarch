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

package io.ampool.monarch.table.coprocessor;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * MResultSender. Provides way to send results back to the co-processor calling thread.
 * <p>
 * Co-processor can use this in following way
 * <p>
 * {@code public void run(MCoprocessorContext context) { // perform operation
 * <p>
 * for(int i=0;i<10;i++){ context.getResultSender().sendResult(i); }
 * context.getResultSender().lastResult(i);
 * <p>
 * }
 * <p>
 * }
 *
 * @since 0.2.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MResultSender<T> {

  /**
   * Sends result back to calling thread.
   *
   * @param oneResult - Result to send
   */
  void sendResult(T oneResult);

  /**
   * Sends result back to calling thread.
   * <p>
   * The ResultCollector will keep waiting for results until it receives last result. Therefore, it
   * is very important to use this method to indicate end of function execution.
   *
   * @param lastResult - Result to send
   */
  void lastResult(T lastResult);

  /**
   *
   * Sends an Exception back to the coprocessor calling thread. sendException adds exception to
   * ResultCollector as a result. If sendException is called then coprocessor will not throw
   * exception but will have exception as a part of results received. Calling sendException will act
   * as a lastResult.
   *
   * @param throwable - Exception to be added.
   */
  void sendException(Throwable throwable);
}
