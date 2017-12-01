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

package io.ampool.monarch.table.internal;


import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.MCache;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MonarchCache;
import org.apache.geode.CancelCriterion;

/**
 * A MTableService provides access to existing {@link MTable tables} that exist in a
 * {@link MonarchCache Ampool cache}.
 * <p>
 * Instances of the interface are created as follows: {@link MCacheFactory#getAnyInstance()} creates
 * an instance of {@link MCache}
 * <p>
 *
 * @since 0.3.0.0
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface MTableService extends AutoCloseable {
  /**
   * the cancellation criterion for this service
   *
   * @return the service's cancellation object
   */
  CancelCriterion getCancelCriterion();
}
