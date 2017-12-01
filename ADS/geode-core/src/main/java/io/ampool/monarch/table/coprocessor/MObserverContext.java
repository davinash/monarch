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
import org.apache.geode.cache.EntryEvent;

/**
 *
 * Context for MTableObservers
 *
 * @since 0.2.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MObserverContext {

  /**
   * Returns table associated with the Observer
   *
   * @return See {@link MTable}
   */
  MTable getTable();

  /**
   * Returns Region of table.
   * 
   * @return See {@link MTableRegion}
   */
  MTableRegion getTableRegion();

  /**
   *
   * @return True if co-processor returned value is to be used instead of normal MTable obtained
   *         value.
   */
  boolean getBypass();

  /**
   * Call to indicate that the current coprocessor's return value should be used in place of the
   * normal MTable obtained value.
   * 
   * @param bypass - Whether to bypass
   */
  void setBypass(boolean bypass);

  /**
   * Returns the EntryEvent
   * 
   * @return Returns the EntryEvent
   */
  EntryEvent getEntryEvent();
}
