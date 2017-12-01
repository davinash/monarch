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

package org.apache.geode.internal.cache;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.internal.AmpoolOpType;
import io.ampool.internal.RegionDataOrder;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.lru.Sizeable;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiFunction;

/**
 * A function to fetch bucket-to-server mapping primary or secondary as required. Use following
 * wrapper methods to retrieve mapping and/or count per bucket. - MTableUtils.getLocationAndCount -
 * MTableUtils.getLocationMap
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class MGetMetadataFunction extends FunctionAdapter implements InternalEntity {
  /**
   * Singleton instance of CountFunction that can be reused..
   */
  public static final MGetMetadataFunction GET_METADATA_FUNCTION = new MGetMetadataFunction();

  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = -3150906692851375826L;
  private static final BiFunction<TableBucketRegion, Long, Long> F_GET_SIZE = (br, o) -> {
    final long perEntryOverhead =
        AmpoolTableRegionAttributes.isAmpoolFTable(br.getCustomAttributes()) ? br.size() * 16 : 0;
    return br.getTotalBytes() - (((TableBucketRegion) br).getActualCount() * o) - perEntryOverhead;
  };
  private static final BiFunction<TableBucketRegion, Long, Long> F_GET_COUNT =
      (br, o) -> (long) ((TableBucketRegion) br).getActualCount();

  public enum Opt {
    COUNT(F_GET_COUNT), SIZE(F_GET_SIZE);
    private BiFunction<TableBucketRegion, Long, Long> getFun;

    Opt(final BiFunction<TableBucketRegion, Long, Long> getFun) {
      this.getFun = getFun;
    }

    public long get(final TableBucketRegion br, final long overhead) {
      return getFun.apply(br, overhead);
    }
  }
  /**
   * Wrapper class to hold arguments to this function.
   */
  public static final class Args implements Serializable {
    private String regionName = null;
    private Set<Integer> bucketIds;
    private int parentOp;
    private Opt opt = Opt.COUNT;

    public Args(final String regionName) {
      this.regionName = regionName;
      this.parentOp = AmpoolOpType.ANY_OP;
    }

    public Args(final String regionName, final Set<Integer> bucketIds) {
      this.regionName = regionName;
      this.bucketIds = bucketIds;
      this.parentOp = AmpoolOpType.ANY_OP;
    }

    public Args(final String regionName, final Set<Integer> bucketIds, final Opt opt) {
      this.regionName = regionName;
      this.bucketIds = bucketIds;
      this.parentOp = AmpoolOpType.ANY_OP;
      this.opt = opt;
    }

    public Args(final String regionName, final Set<Integer> bucketIds, int parentOp) {
      this.regionName = regionName;
      this.bucketIds = bucketIds;
      this.parentOp = parentOp;
    }

    public String getRegionName() {
      return this.regionName;
    }

    public int getParentOp() {
      return this.parentOp;
    }

    public void setParentOp(int parentOp) {
      this.parentOp = parentOp;
    }
  }

  @Override
  public void execute(FunctionContext fc) {
    MCache cache = MCacheFactory.getAnyInstance();
    final Args args = (Args) fc.getArguments();
    if (args == null || args.regionName == null) {
      throw new IllegalArgumentException("Region name must be provided.");
    }

    int csPort = cache.getCacheServers().get(0).getPort();
    String csHost = cache.getDistributedSystem().getDistributedMember().getHost();
    Region region = cache.getRegion(args.regionName);
    if (region == null) {
      logger.info("MGetMetadataFunction: Region does not exist: {}", args.regionName);
      Map<Integer, Pair<ServerLocation, Long>> primaryMap = Collections.emptyMap();
      Map<Integer, Pair<ServerLocation, Long>> secondaryMap = Collections.emptyMap();
      fc.getResultSender().lastResult(new Object[] {primaryMap, secondaryMap});
      return;
    }
    if (!(region instanceof PartitionedRegion)) {
      throw new IllegalArgumentException("Supported only for PartitionedRegion.");
    }
    final TableDescriptor td = cache.getTableDescriptor(args.regionName);
    final long overhead = args.opt == Opt.COUNT ? 0 : getOverheadPerRow(td);

    Map<Integer, Pair<ServerLocation, Long>> primaryMap = new HashMap<>(20);
    Map<Integer, Set<Pair<ServerLocation, Long>>> secondaryMap = new HashMap<>(20);
    Map<String, ServerLocation> locations = new HashMap<>(1);
    Map<ServerLocation, List<Integer>> serverToBucketMap = new HashMap<>(1);
    PartitionedRegion pRegion = (PartitionedRegion) region;

    PartitionedRegion prRgion = (PartitionedRegion) region;
    Map<Integer, List<BucketServerLocation66>> bucketToServerLocations =
        prRgion.getRegionAdvisor().getAllClientBucketProfiles();

    Set<Integer> bucketIds = args.bucketIds == null || args.bucketIds.isEmpty()
        ? pRegion.getDataStore().getAllLocalBucketIds() : args.bucketIds;


    // boolean isFTable =
    // ((PartitionedRegion) region).getRegionDataOrder() == RegionDataOrder.IMMUTABLE;
    for (List<BucketServerLocation66> serverLocations : bucketToServerLocations.values()) {
      for (BucketServerLocation66 bl : serverLocations) {

        if (!bucketIds.contains(bl.getBucketId()) || !bl.getHostName().equals(csHost)
            || bl.getPort() != csPort) {
          continue;
        }
        ServerLocation location = locations.get(bl.getHostName() + bl.getPort());
        if (location == null) {
          location = new ServerLocation(bl.getHostName(), bl.getPort());
          locations.put(bl.getHostName() + bl.getPort(), location);
        }
        long size = args.opt.get(
            (TableBucketRegion) prRgion.getDataStore().getLocalBucketById(bl.getBucketId()),
            overhead);
        if (prRgion.getRegionAdvisor().getBucketAdvisor(bl.getBucketId()).isPrimary()) {
          primaryMap.put(bl.getBucketId(), new Pair<>(location, size));
          if (!serverToBucketMap.containsKey(location)) {
            serverToBucketMap.put(location, new ArrayList<>(10));
          }
          serverToBucketMap.get(location).add(bl.getBucketId());
        } else {
          Set<Pair<ServerLocation, Long>> secServerLocations = secondaryMap.get(bl.getBucketId());
          if (secServerLocations == null) {
            secServerLocations = new LinkedHashSet<>();
            secServerLocations.add(new Pair<>(location, size));
            secondaryMap.put(bl.getBucketId(), secServerLocations);
          } else {
            secServerLocations.add(new Pair<>(location, size));
          }
        }
      }
    }
    fc.getResultSender().lastResult(new Object[] {primaryMap, secondaryMap, serverToBucketMap});
  }

  /* byte-array object overhead + row-header + timestamp-length */
  private static final int OVERHEAD_UNORDERED = Sizeable.PER_OBJECT_OVERHEAD + 4 + 4 + 8;

  public static long getOverheadPerRow(final TableDescriptor td) {
    long overhead;
    if (td instanceof FTableDescriptor) {
      overhead = td.getColumnDescriptors().stream().filter(e -> !e.getColumnType().isFixedLength())
          .mapToInt(e -> Integer.BYTES).sum();
      /* one-bit per column and one-int per variable length column */
    } else {
      /* int (4-bytes) per variable length column */
      long vLen = td.getColumnDescriptors().stream()
          .filter(e -> !e.getColumnType().isFixedLength()
              && !Bytes.equals(MTableUtils.KEY_COLUMN_NAME_BYTES, e.getColumnName()))
          .mapToInt(e -> Integer.BYTES).sum();
      /* one-bit per column and one-int per variable length column */
      overhead = OVERHEAD_UNORDERED + (int) (Math.ceil(1.0 * td.getNumOfColumns() / Byte.SIZE)) // number
                                                                                                // of
                                                                                                // bits
                                                                                                // per
                                                                                                // column
          + vLen; // variable length column lengths
    }
    return overhead;
  }

  /**
   * Return a unique function identifier, used to register the function with
   * {@link org.apache.geode.cache.execute.FunctionService}
   *
   * @return string identifying this function
   */
  @Override
  public String getId() {
    return this.getClass().getName();
  }
}
