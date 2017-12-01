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
package io.ampool.monarch.table.region.map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;

/**
 * RowTupleConcurrentHashMap is just over-riding get method to adjust the key
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class RowTupleConcurrentHashMap<K, V> extends CustomEntryConcurrentHashMap<K, V> {

  private static final long serialVersionUID = -5327588024179878770L;

  public RowTupleConcurrentHashMap(int initialCapacity, final float loadFactor,
      int concurrencyLevel, final boolean isIdentityMap, HashEntryCreator<K, V> entryCreator) {
    super(initialCapacity, loadFactor, concurrencyLevel, isIdentityMap, entryCreator);
  }

  public RowTupleConcurrentHashMap(final int initialCapacity, final float loadFactor,
      final int concurrencyLevel, final boolean isIdentityMap) {
    super(initialCapacity, loadFactor, concurrencyLevel, isIdentityMap);
  }

  @Override
  public V get(Object key) {
    Object value = super.get(key);
    return (V) value;
  }

  @Override
  public V getLastEntry() {
    return null;
  }
}
