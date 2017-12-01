package io.ampool.monarch.table.ftable.utils;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.table.internal.Table;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import org.apache.geode.internal.cache.MGetMetadataFunction;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.ServerLocation;

/**
 * Utilities to get more internal details of FTable
 */
public class FTableUtils {


  public static Map<Integer, Set<ServerLocation>> getBucketToServerMap(final String tableName,
      final Set<Integer> buckets) {
    Map<Integer, ServerLocation> primaryBucketMap = new HashMap<>(113);
    Map<Integer, Set<ServerLocation>> secondaryBucketMap = new HashMap<>(113);
    ProxyFTableRegion fTable =
        (ProxyFTableRegion) MClientCacheFactory.getAnyInstance().getFTable(tableName);
    getLocationMap(fTable, null, primaryBucketMap, secondaryBucketMap);

    // Adding primary buckets
    Map<Integer, Set<ServerLocation>> allBucketMap = new HashMap<>();
    for (Map.Entry<Integer, ServerLocation> integerServerLocationEntry : primaryBucketMap
        .entrySet()) {
      Integer bucketId = integerServerLocationEntry.getKey();
      Set<ServerLocation> serverLocations = allBucketMap.get(bucketId);
      if (serverLocations == null) {
        serverLocations = new LinkedHashSet<>();
        allBucketMap.put(bucketId, serverLocations);
      }
      serverLocations.add(integerServerLocationEntry.getValue());
    }

    // Adding secondary buckets
    for (Map.Entry<Integer, Set<ServerLocation>> integerListEntry : secondaryBucketMap.entrySet()) {
      Integer bucketId = integerListEntry.getKey();
      Set<ServerLocation> serverLocations = allBucketMap.get(bucketId);
      if (serverLocations == null) {
        serverLocations = new LinkedHashSet<>();
        allBucketMap.put(bucketId, serverLocations);
      }
      for (ServerLocation serverLocation : integerListEntry.getValue()) {
        serverLocations.add(serverLocation);
      }
    }
    return allBucketMap;
  }


  @SuppressWarnings("unchecked")
  public static void getLocationMap(final Table table, Set<Integer> buckets,
      Map<Integer, ServerLocation> primaryMap, Map<Integer, Set<ServerLocation>> secondaryMap) {
    final ProxyFTableRegion fTable = (ProxyFTableRegion) table;
    MGetMetadataFunction.Args args = new MGetMetadataFunction.Args(fTable.getName(), buckets);

    Object output = FunctionService.onServers((fTable).getTableRegion().getRegionService())
        .withArgs(args).execute(MGetMetadataFunction.GET_METADATA_FUNCTION).getResult();

    /** extract only server-location from the original map **/
    for (Object[] objects : ((List<Object[]>) output)) {
      if (primaryMap != null) {
        ((Map<Integer, Pair<ServerLocation, Long>>) objects[0])
            .forEach((k, v) -> primaryMap.put(k, v.getFirst()));
      }
      if (secondaryMap != null) {
        for (Map.Entry<Integer, Set<Pair<ServerLocation, Long>>> integerSetEntry : ((Map<Integer, Set<Pair<ServerLocation, Long>>>) objects[1])
            .entrySet()) {
          Set<Pair<ServerLocation, Long>> value = integerSetEntry.getValue();
          Set<ServerLocation> locationSet = new LinkedHashSet<>();
          for (Pair<ServerLocation, Long> serverLocationLongPair : value) {
            locationSet.add(serverLocationLongPair.getFirst());
          }
          secondaryMap.put(integerSetEntry.getKey(), locationSet);
        }
      }
    }
  }


  /**
   * Sets the columnname or column ids whichever is not present in scan in case of selective scan.
   * 
   * @param scan
   * @param tableDescriptor
   * @return Updated scan object with both columnnames and columnids
   */
  public static Scan setColumnNameOrIds(Scan scan, FTableDescriptor tableDescriptor) {
    // Handling column setting
    if (scan.getColumnNameList().size() > 0 && scan.getColumns().isEmpty()) {
      List<Integer> columnIds = getColumnIds(scan.getColumnNameList(), tableDescriptor);
      scan.setColumns(columnIds);
    } else if (scan.getColumns().size() > 0 && scan.getColumnNameList().isEmpty()) {
      scan = scan.addColumns(getColumnNames(scan.getColumns(), tableDescriptor));
    }
    return scan;
  }

  /**
   * Get the column-index list to be retrieved during scan. In case user has provided the column
   * names convert these column names to the respective column-index using the table descriptor.
   * Once it is done the column-index list can be used for retrieving the respective columns.
   * 
   * @param columnNames
   * @param tableDescriptor
   * @return list of columns indexes corresponding to given column names
   */
  public static List<Integer> getColumnIds(List<byte[]> columnNames,
      FTableDescriptor tableDescriptor) {
    Map<MColumnDescriptor, Integer> cdMap = tableDescriptor.getColumnDescriptorsMap();
    List<Integer> columns = columnNames.stream().sequential()
        .map(column -> cdMap.get(new MColumnDescriptor(column))).collect(Collectors.toList());
    return columns;
  }


  /**
   * Utility API to convert given list of columns indexes to corresponding column names
   * 
   * @param columns
   * @param tableDescriptor
   * @return returns list of columns names corresponding to given column indexes
   */
  public static List<byte[]> getColumnNames(List<Integer> columns,
      FTableDescriptor tableDescriptor) {
    Map<MColumnDescriptor, Integer> columnDescriptorsMap =
        tableDescriptor.getColumnDescriptorsMap();

    List<MColumnDescriptor> columnDescriptors = columnDescriptorsMap.entrySet().stream()
        .sequential().filter(e -> columns.contains(e.getValue())).map(e -> e.getKey())
        .collect(Collectors.toList());

    List<byte[]> columnNames = tableDescriptor.getColumnsByName().entrySet().stream().sequential()
        .filter(e -> columnDescriptors.contains(e.getValue())).map(e -> e.getKey().getByteArray())
        .collect(Collectors.toList());

    return columnNames;
  }

}
