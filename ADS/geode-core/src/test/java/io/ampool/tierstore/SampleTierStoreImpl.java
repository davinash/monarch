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
package io.ampool.tierstore;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.filter.Filter;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.cache.TierStoreStats;

public class SampleTierStoreImpl implements TierStore {
  TierStoreReader reader;
  TierStoreWriter writer;
  Properties properties;
  private final String name;
  private final TierStoreStats stats;

  public SampleTierStoreImpl(final String tierName) {
    this.name = tierName;
    MonarchCacheImpl instance = MonarchCacheImpl.getInstance();
    StatisticsFactory factory = instance.getDistributedSystem();
    this.stats = new TierStoreStats(factory, getName());
  }

  @Override
  public void init(Properties storeProperties, TierStoreWriter writer, TierStoreReader reader)
      throws IOException {
    properties = storeProperties;
    this.writer = writer;
    this.reader = reader;
  }

  @Override
  public TierStoreWriter getWriter(String tableName, int partitionId, Properties writerProps) {
    return writer;
  }

  @Override
  public TierStoreWriter getWriter(final String tableName, final int partitionId) {
    return null;
  }

  @Override
  public TierStoreReader getReader(String tableName, int partitionId, Properties readerProps) {
    return reader;
  }

  @Override
  public TierStoreReader getReader(final String tableName, final int partitionId) {
    return null;
  }

  @Override
  public void deleteTable(String tableName) {
    return;
  }

  @Override
  public void deletePartition(String tableName, int partitionId) {
    return;
  }

  @Override
  public void stopMonitoring() {
    return;
  }

  @Override
  public void delete(String... unitPath) {
    //// nothing..
  }

  @Override
  public URI getBaseURI() {
    return null;
  }

  @Override
  public long getBytesRead() {
    return 0;
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public TierStoreReader getReader(Properties props, String... args) {
    return null;
  }

  @Override
  public TierStoreWriter getWriter(Properties props, String... args) {
    return null;
  }

  @Override
  public void destroy() {
    return;
  }

  @Override
  public void truncateBucket(String tableName, int partitionId, Filter filter, TableDescriptor td,
      Properties tierProperties) throws IOException {}

  @Override
  public void updateBucket(String tableName, int partitionId, Filter filter,
      Map<Integer, Object> colValues, TableDescriptor td, Properties tierProperties)
      throws IOException {}

  /**
   * TODO DOCUMENTATION
   *
   * @param tableName
   * @return
   */
  @Override
  public ConverterDescriptor getConverterDescriptor(String tableName) {
    return null;
  }

  @Override
  public TierStoreStats getStats() {
    return this.stats;
  }

  @Override
  public long getBytesWritten() {
    return 0;
  }
}
