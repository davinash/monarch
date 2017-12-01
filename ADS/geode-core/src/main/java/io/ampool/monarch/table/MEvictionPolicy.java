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
package io.ampool.monarch.table;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

import java.io.Serializable;

/**
 * Action policies available when heap space runs low; currently only overflow to disk for
 * {@link MTable}. and overflow to next Tier for {@link io.ampool.monarch.table.ftable.FTable}.
 * {@link MEvictionPolicy#OVERFLOW_TO_TIER} enables data to evict to be evicted tiered storage
 * {@link MEvictionPolicy#OVERFLOW_TO_DISK} enables data to evict to be evicted to Disk
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public enum MEvictionPolicy implements Serializable {
  OVERFLOW_TO_DISK, OVERFLOW_TO_TIER,
  /**
   * When selected this policy, data qualified for eviction will be destroyed from Memory
   */
  LOCAL_DESTROY, NO_ACTION
}
