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

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Pair;
import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMaps;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;
import org.apache.logging.log4j.Logger;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class RowTupleConcurrentSkipListMap<K, V> extends CustomEntryConcurrentHashMap<K, V> {
  private static final int MAP_TYPE = Integer.getInteger("use.map.type", 44);
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = 8978106910201401024L;
  private final SortedMap<Object, Object> map;

  private static SortedMap<Object, Object> getMap(final int mapType) {
    final SortedMap<Object, Object> sm;
    switch (mapType) {
      case 0:
        logger.info("Using: Object2ObjectAVLTreeMap");
        sm = Object2ObjectSortedMaps.synchronize(new Object2ObjectAVLTreeMap<>());
        break;
      default:
        logger.info("Using: ConcurrentSkipListMap");
        sm = new ConcurrentSkipListMap<>();
        break;
    }
    return sm;
  }

  public RowTupleConcurrentSkipListMap() {
    super(false);
    this.map = getMap(MAP_TYPE);
  }

  public SortedMap<Object, Object> getInternalMap() {
    return this.map;
  }

  @Override
  public final V put(final K key, final V value) {
    if (value == null) {
      throw new NullPointerException();
    }
    return (V) this.map.put(key, value);
  }

  /**
   * {@inheritDoc}
   *
   * @return the previous value associated with the specified key, or <tt>null</tt> if there was no
   *         mapping for the key
   * @throws NullPointerException if the specified key or value is null
   */
  @Override
  public final V putIfAbsent(final K key, final V value) {
    // return (V) this.map.put(new MTableKey(((String)key).getBytes()), value);
    return (V) this.map.putIfAbsent(key, value);
  }

  @Override
  public V get(final Object key) {
    Object value = this.map.get(key);
    return (V) value;
  }

  @Override
  public boolean remove(final Object key, final Object value) {
    if (value == null) {
      return false;
    }
    return map.remove(key) != null;
  }

  @Override
  public boolean create(final K key, final V value) {
    throw new RuntimeException("create create create FIX THIS ONE FIX FIX FIX");
  }

  @Override
  public int size() {
    return this.map.size();
  }

  @Override
  public void clear() {
    this.map.clear();
  }

  @Override
  public boolean isEmpty() {
    return this.map.isEmpty();
  }

  @Override
  public Collection<V> values() {
    return (Collection<V>) this.map.values();
  }

  @Override
  public V getLastEntry() {
    if (this.map instanceof NavigableMap) {
      Entry lastEntry = ((NavigableMap) this.map).lastEntry();
      if (lastEntry != null) {
        return (V) lastEntry.getValue();
      }
      return null;
    } else {
      return (V) this.map.get(this.map.lastKey());
    }
  }

  public Pair<K, V> lastEntryPair() {
    if (this.map instanceof NavigableMap) {
      Map.Entry<K, V> e = ((NavigableMap<K, V>) this.map).lastEntry();
      return e == null ? null : new Pair<>(e.getKey(), e.getValue());
    } else {
      final K k = (K) this.map.lastKey();
      return k == null ? null : new Pair<>(k, (V) this.map.get(k));
    }
  }

}
