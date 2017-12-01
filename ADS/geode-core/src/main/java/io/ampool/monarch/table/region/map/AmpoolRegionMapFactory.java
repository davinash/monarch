package io.ampool.monarch.table.region.map;

import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PlaceHolderDiskRegion;
import org.apache.geode.internal.cache.ProxyRegionMap;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.internal.cache.RegionMapFactory;


public class AmpoolRegionMapFactory implements RegionMapFactory {
  /**
   * Creates a RowTupleRegionMap that is stored in the VM.
   * 
   * @param localRegion the region that will be the owner of the map
   * @param internalRegionArgs internal region args
   * @return region map instance
   */
  @Override
  public RegionMap create(final LocalRegion localRegion,
      final InternalRegionArguments internalRegionArgs) {
    RegionMap.Attributes ma = new RegionMap.Attributes();
    ma.statisticsEnabled = localRegion.getStatisticsEnabled();
    ma.loadFactor = localRegion.getLoadFactor();
    ma.initialCapacity = localRegion.getInitialCapacity();
    ma.concurrencyLevel = localRegion.getConcurrencyLevel();
    if (localRegion.isProxy()) {
      return new ProxyRegionMap(localRegion, ma, internalRegionArgs);
    } else if (localRegion.getEvictionController() != null) {
      return new RowTupleLRURegionMap(localRegion, ma, internalRegionArgs);
    } else {
      return new RowTupleRegionMap(localRegion, ma, internalRegionArgs);
    }
  }

  /**
   * Creates a RowTupleRegionMap that is stored in the VM. Called during DiskStore recovery before
   * the region actually exists.
   * 
   * @param placeHolderDiskRegion the place holder disk region that will be the owner of the map
   * @param internalRegionArgs internal region args
   * @return region map instance
   */
  @Override
  public RegionMap create(final PlaceHolderDiskRegion placeHolderDiskRegion,
      final InternalRegionArguments internalRegionArgs) {
    RegionMap.Attributes ma = new RegionMap.Attributes();
    ma.statisticsEnabled = placeHolderDiskRegion.getStatisticsEnabled();
    ma.loadFactor = placeHolderDiskRegion.getLoadFactor();
    ma.initialCapacity = placeHolderDiskRegion.getInitialCapacity();
    ma.concurrencyLevel = placeHolderDiskRegion.getConcurrencyLevel();
    if (placeHolderDiskRegion.getLruAlgorithm() != 0) {
      RowTupleLRURegionMap rowTupleLRURegionMap =
          new RowTupleLRURegionMap(placeHolderDiskRegion, ma, internalRegionArgs);

      return rowTupleLRURegionMap;
    } else {
      RowTupleRegionMap rowTupleRegionMap =
          new RowTupleRegionMap(placeHolderDiskRegion, ma, internalRegionArgs);
      return rowTupleRegionMap;
    }
  }
}
