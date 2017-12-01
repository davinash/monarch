/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.cache;

import java.util.*;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.internal.InternalTable;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

/**
 * The DescMTableFunction class is an implementation of GemFire Function interface used to describe
 * an mtable.
 * </p>
 * 
 * @see org.apache.geode.cache.execute.Function
 * @see org.apache.geode.cache.execute.FunctionAdapter
 * @see org.apache.geode.cache.execute.FunctionContext
 * @see org.apache.geode.internal.InternalEntity
 * @since 7.0
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class DescMTableFunction extends FunctionAdapter implements InternalEntity {

  /* constants */
  public static final String ATTRIBUTES = "Table Attributes";
  public static final String SCHEMA = "Table Schema";
  public static final String TIER_ATTRIBUTES = "Tier Attributes";
  public static final String TYPE = "table-type";
  public static final String PERSISTENCE_ENABLED = "persistence-enabled";
  public static final String DISK_WRITE_POLICY = "disk-write-policy";
  public static final String M_MAX_VERSIONS = "max-versions";
  public static final String F_BLOCK_SIZE = "block-size";
  public static final String F_BLOCK_FORMAT = "block-format";
  public static final String F_TOTAL_BLOCK_COUNT = "number-of-blocks";
  public static final String TIER_PFX = "tier";
  private static final long HOUR_TO_MS = 3600_000L;
  private static final String OBSERVER_COPROCESSORS = "observer-coprocessors";
  private static final String CACHE_LOADERS_CLASS = "cache-loader";
  private static final String F_PARTIONING_COLUMN = "partitioning-column";

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogService.getLogger();

  @SuppressWarnings("unused")
  public void init(final Properties props) {}

  public String getId() {
    return getClass().getName();
  }

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  public void execute(final FunctionContext context) {

    try {
      Map<String, Map<String, String>> resultMap = new LinkedHashMap<>();

      final Object[] args = (Object[]) context.getArguments();
      final String tableName = ((String) args[0]).replace("/", "");
      final TableDescriptor td = MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
      if (td == null) {
        context.getResultSender().lastResult(resultMap);
        return;
      }

      Map<String, String> attributesMap = new LinkedHashMap<>();
      Map<String, String> schemaMap = new LinkedHashMap<>();
      resultMap.put(ATTRIBUTES, attributesMap);
      resultMap.put(SCHEMA, schemaMap);

      /* populate schema */
      for (final MColumnDescriptor cd : td.getAllColumnDescriptors()) {
        schemaMap.put(cd.getColumnNameAsString(), cd.getColumnType().toString());
      }

      if (td instanceof MTableDescriptor) {
        final MTableDescriptor mtd = (MTableDescriptor) td;
        attributesMap.put(TYPE, mtd.getTableType().name());
        attributesMap.put(M_MAX_VERSIONS, String.valueOf(mtd.getMaxVersions()));
        attributesMap.put(PERSISTENCE_ENABLED, String.valueOf(mtd.isDiskPersistenceEnabled()));
        attributesMap.put(DISK_WRITE_POLICY, String.valueOf(mtd.getDiskWritePolicy()));
        attributesMap.put(OBSERVER_COPROCESSORS,
            String.valueOf(String.join(",", mtd.getCoprocessorList())));
        attributesMap.put(CACHE_LOADERS_CLASS, String.valueOf(mtd.getCacheLoaderClassName()));
      } else if (td instanceof FTableDescriptor) {
        FTableDescriptor ftd = (FTableDescriptor) td;
        attributesMap.put(TYPE, ftd.getType().name());
        attributesMap.put(F_BLOCK_SIZE, String.valueOf(ftd.getBlockSize()));
        attributesMap.put(F_BLOCK_FORMAT, String.valueOf(ftd.getBlockFormat()));

        final InternalTable table =
            (InternalTable) MCacheFactory.getAnyInstance().getFTable(tableName);
        long totalCount = MTableUtils.getTotalCount(table, null, null, true);
        attributesMap.put(F_TOTAL_BLOCK_COUNT, String.valueOf(totalCount));

        attributesMap.put(PERSISTENCE_ENABLED, String.valueOf(ftd.isDiskPersistenceEnabled()));
        if (ftd.isDiskPersistenceEnabled()) {
          attributesMap.put(DISK_WRITE_POLICY, String.valueOf(ftd.getDiskWritePolicy()));
        }

        final Map<String, String> taMap = getTierAttributes(ftd.getTierStores());
        if (taMap.size() > 0) {
          resultMap.put(TIER_ATTRIBUTES, taMap);
        }
        if (ftd.getPartitioningColumn() != null) {
          attributesMap.put(F_PARTIONING_COLUMN,
              Bytes.toString(ftd.getPartitioningColumn().getByteArray()));
        }
      }
      context.getResultSender().lastResult(resultMap);
    } catch (Exception e) {
      logger.error("EXCEPTION while executing describe table " + e.getMessage());
      context.getResultSender().sendException(e);
    }
  }

  /**
   * Get the tier-attributes as a map.
   *
   * @param stores the configured tiers for this table
   * @return map of all tier attributes
   */
  private Map<String, String> getTierAttributes(
      final LinkedHashMap<String, TierStoreConfiguration> stores) {
    if (stores == null || stores.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> tierAttrsMap = new LinkedHashMap<>();
    String pfx;
    int i = 1;
    for (Map.Entry<String, TierStoreConfiguration> entry : stores.entrySet()) {
      pfx = TIER_PFX + i + "-";
      tierAttrsMap.put(pfx + "store", entry.getKey());
      Properties tierProperties = entry.getValue().getTierProperties();

      Object value;
      String valueStr;
      value = tierProperties.get(TierStoreConfiguration.TIER_PARTITION_INTERVAL);
      valueStr =
          value == null || value.equals(TierStoreConfiguration.DEFAULT_TIER_PARTITION_INTERVAL_MS)
              ? "10 minutes" : String.format("%.02f Hours", ((long) value) / (float) HOUR_TO_MS);
      tierAttrsMap.put(pfx + TierStoreConfiguration.TIER_PARTITION_INTERVAL, valueStr);

      value = tierProperties.get(TierStoreConfiguration.TIER_TIME_TO_EXPIRE);
      valueStr =
          value == null || value.equals(TierStoreConfiguration.DEFAULT_TIER_TIME_TO_EXPIRE_MS) ? "-"
              : String.format("%.02f Hours", ((long) value) / (float) HOUR_TO_MS);
      tierAttrsMap.put(pfx + TierStoreConfiguration.TIER_TIME_TO_EXPIRE, valueStr);
      i++;
    }
    return tierAttrsMap;
  }
}
