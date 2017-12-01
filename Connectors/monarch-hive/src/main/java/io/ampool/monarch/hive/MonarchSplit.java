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
package io.ampool.monarch.hive;

import io.ampool.client.AmpoolClient;
import io.ampool.monarch.functions.MonarchGetAllFunction;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.Table;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.CompareOp;
import io.ampool.monarch.types.MPredicateHolder;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Created on: 2015-11-16
 * Since version: 0.2.0
 *
 * The Monarch splits are created for every 100k entries. The keys are
 *    simple incremental numbers which allow, at the moment, to define
 *    splits easily.
 *
 */
public class MonarchSplit extends FileSplit {
  private static final Logger logger = LoggerFactory.getLogger(MonarchSplit.class);

  public static final long DEFAULT_SPLIT_SIZE = 1000000;      // 1000k - calculated by loading 4m records
  private String keyPrefix = "";
  private TreeSet<Integer> bucketIds = new TreeSet<>();
  private Map<Integer, Set<ServerLocation>> bucketToServerMap = null;
  private String[] hosts = new String[0];

  /** query to fetch the key-prefix and number of blocks for each prefix **/
  private static final String QUERY_FMT = "select key,value from /%s.entries where key LIKE '%%"+ MonarchUtils.KEY_BLOCKS_SFX +"'";

  private MonarchSplit() {
    super(null, 0, 0, (String[])null);
    this.keyPrefix = "";
  }
  public MonarchSplit(Path file, long start, long length, String[] hosts, final String keyPrefix) {
    super(file, start, length, hosts);
    this.keyPrefix = keyPrefix;
  }

  public MonarchSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
    super(file, start, length, hosts, inMemoryHosts);
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    super.write(out);
    DataSerializer.writeStringArray(hosts, out);
    DataSerializer.writeHashMap(bucketToServerMap, out);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    super.readFields(in);
    try {
      this.hosts = DataSerializer.readStringArray(in);
      this.bucketToServerMap = DataSerializer.readHashMap(in);
      this.bucketIds.addAll(bucketToServerMap.keySet());
    } catch (ClassNotFoundException e) {
    }
  }

  /**
   * Provide the required splits from the specified configuration. By default this
   *   method makes query (function-execution) on the region with `_meta' suffix
   *   so need to be make sure that the region-name is passed accordingly.
   *
   * @param conf the job configuration
   * @param numSplits the required number of splits
   * @return the required splits to read/write the data
   * @throws IOException if table does not exist.
   */
  public static InputSplit[] getSplits(final JobConf conf, final int numSplits) throws IOException {
    final Path[] tablePaths = FileInputFormat.getInputPaths(conf);
    /** initialize cache if not done yet.. **/
    final AmpoolClient aClient = MonarchUtils.getConnectionFromConf(conf);
    String tableName = conf.get(MonarchUtils.REGION);
    boolean isFTable = MonarchUtils.isFTable(conf);
    Table table = null;
    if (isFTable) {
      table = aClient.getFTable(tableName);
    } else {
      table = aClient.getMTable(tableName);
    }
    if (table == null) {
      throw new IOException("Table " + tableName + "does not exist.");
    }
    int totalnumberOfSplits = table.getTableDescriptor().getTotalNumOfSplits();
    Map<Integer, Set<ServerLocation>> bucketMap = new HashMap<>(numSplits);
    final AtomicLong start = new AtomicLong(0L);
    MonarchSplit[] splits = MTableUtils
      .getSplitsWithSize(tableName, numSplits, totalnumberOfSplits, bucketMap)
      .stream().map(e -> {
        MonarchSplit ms = convertToSplit(tablePaths, start.get(), e, bucketMap);
        start.addAndGet(e.getSize());
        return ms;
      }).toArray(MonarchSplit[]::new);
    logger.info("numSplits= {}; MonarchSplits= {}", numSplits, Arrays.toString(splits));
    return splits;
  }

  /**
   * Convert MTableUtils.MSplit to MonarchSplit and populate the required details of split.
   *
   * @param tablePaths        the table paths
   * @param start             the start-offset
   * @param split             the generic split information
   * @param bucketToServerMap the bucket-id to server-location map
   * @return the newly created MonarchSplit
   */
  public static MonarchSplit convertToSplit(final Path[] tablePaths, final long start,
                                            final MTableUtils.MSplit split,
                                            final Map<Integer, Set<ServerLocation>> bucketToServerMap) {
    MonarchSplit ms = new MonarchSplit(tablePaths[0], start, split.getSize(), split.getServersArray(), split.getServersArray());
    ms.hosts = split.getServersArray();
    Map<Integer, Set<ServerLocation>> locationMap = bucketToServerMap.entrySet().stream()
      .filter(e -> split.getBuckets().contains(e.getKey()))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    ms.getBucketIds().addAll(split.getBuckets());
    ms.setBucketToServerMap(locationMap);
    return ms;
  }
  public Set<Integer> getBucketIds() {
    return this.bucketIds;
  }

  /**
   * Get the bucket-id to server-location map.
   *
   * @return the map containing bucket-id to server-location
   */
  public Map<Integer, Set<ServerLocation>> getBucketToServerMap() {
    return this.bucketToServerMap;
  }

  /**
   * Set the bucket-id to server-location map.
   *
   * @param bucketToServerMap the map containing bucket-id to server-location
   */
  public void setBucketToServerMap(final Map<Integer, Set<ServerLocation>> bucketToServerMap) {
    this.bucketToServerMap = bucketToServerMap;
  }
  @SuppressWarnings("unchecked")
  public static InputSplit[] getSplits(final JobConf conf, final int numSplits, int dummy) {
    final Path[] tablePaths = FileInputFormat.getInputPaths(conf);
    long splitSize = NumberUtils.toLong(conf.get(MonarchUtils.SPLIT_SIZE_KEY), DEFAULT_SPLIT_SIZE);

    final String regionName = conf.get(MonarchUtils.REGION) + MonarchUtils.META_TABLE_SFX;

    MPredicateHolder ph = new MPredicateHolder(-1, BasicTypes.STRING,
      CompareOp.REGEX, ".*"+MonarchUtils.KEY_BLOCKS_SFX);

    MonarchGetAllFunction func = new MonarchGetAllFunction();
    final AmpoolClient aClient = MonarchUtils.getConnectionFromConf(conf);
    Execution exec = FunctionService.onServer(((GemFireCacheImpl)(aClient.getGeodeCache())).getDefaultPool())
    .withArgs(new Object[]{regionName, ph});
    ResultCollector rc = exec.execute(func);
    /** TODO: refactor below code.. change below required in case the function is changed to return in some way **/
    List<String[]> output = (List<String[]>)((List) rc.getResult()).get(0);
    if (output.isEmpty()) {
      logger.error("No entries found in region= {} with key_prefix= %-{}",
        regionName, MonarchUtils.KEY_BLOCKS_SFX);
      return new MonarchSplit[0];
    }

    List<MonarchSplit> list = new ArrayList<>(output.size());
    String prefix;
    long numberOfBlocks;
    for (final String[] arr : output) {
      prefix = arr[0].substring(0, arr[0].length() - 6);
      numberOfBlocks = Long.valueOf(arr[1]);
      if (numberOfBlocks > splitSize) {
        Collections.addAll(list, MonarchSplit.getInputSplits(tablePaths[0], prefix, splitSize, numberOfBlocks));
      } else {
        list.add(new MonarchSplit(tablePaths[0], 0, numberOfBlocks, null, prefix));
      }
    }
    return list.toArray(new MonarchSplit[list.size()]);
  }

  /**
   * Provide the required splits with correct configuration: start, end, length of split
   *
   * @param filePath the file path
   * @param keyPrefix the prefix for keys in the split
   * @param splitSize the split size
   * @param totalCount the total number of records for the query
   * @return an array of the required splits
   */
  protected static MonarchSplit[] getInputSplits(final Path filePath, final String keyPrefix,
                                                 final long splitSize, final long totalCount) {
    final int nSplits = (int)Math.ceil((double)totalCount/ splitSize);
    MonarchSplit[] splits = new MonarchSplit[nSplits];
    long end, start;
    if (logger.isDebugEnabled()) {
      logger.debug("splitSize= {}; totalCount={}; nSplits= {}", splitSize, totalCount, nSplits);
    }
    for (int i = 0; i < nSplits; ++i) {
      start = i * splitSize;
      end = start + splitSize > totalCount ? totalCount : (start + splitSize);
      splits[i] = new MonarchSplit(filePath, start, (end-start), null, keyPrefix);
      if (logger.isDebugEnabled()) {
        logger.debug("split[{}]= {}", i, splits[i]);
      }
    }
    return splits;
  }

  /**
   * Get the end of the split.
   *
   * @return the last key for the split
   */
  public long getEnd() {
    return super.getStart() + super.getLength();
  }

  /**
   * Get the key-prefix for this split.
   *
   * @return the prefix for all the keys that are part of this split
   */
  public String getKeyPrefix() {
    return this.keyPrefix;
  }

  public String toString() {
    return String.format("[path= %s; locations= %s; length= %d; bucketIds= %s]",
      getPath(), Arrays.toString(hosts), getLength(), getBucketIds());
  }
  @Override
  public String[] getLocations() {
    return this.hosts;
  }
}
