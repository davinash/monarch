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
import io.ampool.internal.RegionDataOrder;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import org.apache.geode.cache.CustomRegionAttributes;
import org.apache.geode.internal.cache.AbstractRegionEntry;
import org.apache.geode.internal.cache.AbstractRegionMap;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PlaceHolderDiskRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;

import java.util.Collection;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class RowTupleRegionMap extends AbstractRegionMap {
  public RowTupleRegionMap(InternalRegionArguments internalRegionArgs) {
    super(internalRegionArgs);
  }

  public RowTupleRegionMap(Object owner, Attributes attr,
      InternalRegionArguments internalRegionArgs) {
    super(internalRegionArgs);
    initialize(owner, attr, internalRegionArgs, false);
  }

  @Override
  protected void initialize(Object owner, Attributes attr,
      InternalRegionArguments internalRegionArgs, boolean isLRU) {
    super.initialize(owner, attr, internalRegionArgs, isLRU);
    CustomRegionAttributes customRegionAttributes =
        owner instanceof LocalRegion ? ((LocalRegion) owner).getAttributes().getCustomAttributes()
            : ((PlaceHolderDiskRegion) owner).getCustomAttributes();
    if (AmpoolTableRegionAttributes.isAmpoolTableOrdered(customRegionAttributes)) {
      _setMap(new RowTupleConcurrentSkipListMap<>());
    } else if (AmpoolTableRegionAttributes.isAmpoolMTableUnOrdered(customRegionAttributes)) {
      _setMap(createRowTupleConcurrentMap(attr.initialCapacity, attr.loadFactor,
          attr.concurrencyLevel, false, new AbstractRegionEntry.HashRegionEntryCreator()));
    }
  }

  protected RowTupleConcurrentHashMap<Object, Object> createRowTupleConcurrentMap(
      int initialCapacity, float loadFactor, int concurrencyLevel, boolean isIdentityMap,
      CustomEntryConcurrentHashMap.HashEntryCreator<Object, Object> entryCreator) {
    if (entryCreator != null) {
      return new RowTupleConcurrentHashMap<>(initialCapacity, loadFactor, concurrencyLevel,
          isIdentityMap, entryCreator);
    } else {
      return new RowTupleConcurrentHashMap<Object, Object>(initialCapacity, loadFactor,
          concurrencyLevel, isIdentityMap);
    }
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  public Collection<RegionEntry> regionEntries() {
    return (Collection) _getMap().values();
  }

}
