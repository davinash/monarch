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
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableRegion;

/**
 * MCoprocessorContext context used in coprocessor execution.
 *
 * @since 0.3.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MCoprocessorContext {

  /**
   * Returns the request object associated with context of Co-processor
   *
   * @return See {@link MExecutionRequest }
   */
  MExecutionRequest getRequest();

  /**
   * Returns unique Coprocessor Identifier
   * 
   * @return Returns unique Coprocessor Identifier
   */
  String getCoprocessorId();

  /**
   * Result sender associated with the coprocessor
   * 
   * @param <T>
   * @return Result sender associated with the coprocessor
   */
  <T> MResultSender<T> getResultSender();

  /**
   * Returns a boolean to identify whether this is a re-execute. Returns true if it is a re-execute
   * else returns false
   * 
   * @return Returns a boolean to identify whether this is a re-execute. Returns true if it is a
   *         re-execute else returns false
   */
  boolean isPossibleDuplicate();

  /**
   * Return Table handle associated with MCoprocessor
   * 
   * @return See {@link MTable}
   */
  MTable getTable();

  /**
   * Returns the MTableRegion associated with the context of MCoprocessor.
   * 
   * @return See {@link MTableRegion}
   */
  MTableRegion getMTableRegion();
}
