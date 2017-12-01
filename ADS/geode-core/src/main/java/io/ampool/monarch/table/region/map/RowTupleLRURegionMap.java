package io.ampool.monarch.table.region.map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.internal.RegionDataOrder;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import org.apache.geode.cache.CustomRegionAttributes;
import org.apache.geode.internal.cache.AbstractLRURegionMap;
import org.apache.geode.internal.cache.AbstractRegionEntry;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PlaceHolderDiskRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.lru.EnableLRU;
import org.apache.geode.internal.cache.lru.NewLRUClockHand;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;

import java.util.Collection;

@InterfaceAudience.Private
@InterfaceStability.Stable
public final class RowTupleLRURegionMap extends AbstractLRURegionMap {

  public RowTupleLRURegionMap(Object owner, Attributes attr,
      InternalRegionArguments internalRegionArgs) {
    super(internalRegionArgs);
    initialize(owner, attr, internalRegionArgs);
    CustomRegionAttributes customRegionAttributes =
        owner instanceof LocalRegion ? ((LocalRegion) owner).getAttributes().getCustomAttributes()
            : ((PlaceHolderDiskRegion) owner).getCustomAttributes();
    if (AmpoolTableRegionAttributes.isAmpoolTableOrdered(customRegionAttributes)) {
      _setMap(new RowTupleConcurrentSkipListMap<Object, Object>());
    } else if (AmpoolTableRegionAttributes.isAmpoolMTableUnOrdered(customRegionAttributes)) {
      _setMap(createRowTupleConcurrentMap(attr.initialCapacity, attr.loadFactor,
          attr.concurrencyLevel, false, new AbstractRegionEntry.HashRegionEntryCreator()));
    }
  }

  @Override
  protected void initialize(Object owner, Attributes attr,
      InternalRegionArguments internalRegionArgs) {
    super.initialize(owner, attr, internalRegionArgs);
  }

  protected RowTupleConcurrentHashMap<Object, Object> createRowTupleConcurrentMap(

      int initialCapacity, float loadFactor, int concurrencyLevel, boolean isIdentityMap,
      CustomEntryConcurrentHashMap.HashEntryCreator<Object, Object> entryCreator) {
    if (entryCreator != null) {
      return new RowTupleConcurrentHashMap<Object, Object>(initialCapacity, loadFactor,
          concurrencyLevel, isIdentityMap, entryCreator);
    } else {
      return new RowTupleConcurrentHashMap<Object, Object>(initialCapacity, loadFactor,
          concurrencyLevel, isIdentityMap);
    }
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  public Collection<RegionEntry> regionEntries() {
    return (Collection) _getMap().values();
  }


  // LRU fields and accessors
  /**
   * A tool from the eviction controller for sizing entries and expressing limits.
   */
  private EnableLRU ccHelper;
  /** The list of nodes in LRU order */
  private NewLRUClockHand lruList;

  @Override
  protected final void _setCCHelper(EnableLRU ccHelper) {
    this.ccHelper = ccHelper;
  }

  @Override
  protected final EnableLRU _getCCHelper() {
    return this.ccHelper;
  }

  @Override
  protected final void _setLruList(NewLRUClockHand lruList) {
    this.lruList = lruList;
  }

  @Override
  public final NewLRUClockHand _getLruList() {
    return this.lruList;
  }
}
