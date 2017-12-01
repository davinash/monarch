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

package io.ampool.tierstore.stores;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.TierStoreConfiguration;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.ThinRow;
import io.ampool.monarch.table.region.ScanUtils;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.FileFormats;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreReader;
import io.ampool.tierstore.TierStoreWriter;
import io.ampool.tierstore.config.CommonConfig;
import io.ampool.tierstore.internal.ORCConverterDescriptor;
import io.ampool.tierstore.internal.ParquetConverterDescriptor;
import io.ampool.tierstore.internal.TierEvictorThread;
import io.ampool.tierstore.readers.orc.TierStoreORCReader;
import io.ampool.tierstore.utils.ConfigurationUtils;
import io.ampool.tierstore.writers.orc.TierStoreORCWriter;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.TierStoreStats;
import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Logger;

/**
 * Tier store implementation that will store files on local disk.
 */
public class LocalTierStore implements TierStore {

  private final static String BASE_DIR_PATH = "base.dir.path";

  private static final Logger logger = LogService.getLogger();

  private Properties storeProperties;
  private TierStoreWriter writer;
  private TierStoreReader reader;
  private final String name;
  private Thread evictorThread;
  private FileSystem fileSystem;
  private FileFormats fileFormat;
  private int DELETE_RETRIES = 3;
  private TierStoreStats stats;
  public static boolean test_Hook_disable_stats = false;

  public LocalTierStore(final String tierName) {
    this.name = tierName;
    // The cache reference should be available here
    // MonarchCacheImpl.getGeodeCacheInstance();
    if (test_Hook_disable_stats) {
      GemFireCacheImpl instance = GemFireCacheImpl.getInstance();
      StatisticsFactory factory = instance.getDistributedSystem();
      this.stats = new TierStoreStats(factory, getName());
    }
  }

  @Override
  public void init(final Properties storeProperties, final TierStoreWriter writer,
      final TierStoreReader reader) throws IOException {
    this.storeProperties = storeProperties;
    this.writer = writer;
    this.reader = reader;
    // based on reader writer deduce file format
    if (this.writer instanceof TierStoreORCWriter && this.reader instanceof TierStoreORCReader) {
      this.fileFormat = FileFormats.ORC;
    } else {
      this.fileFormat = FileFormats.PARQUET;
    }

    this.fileSystem = FileSystem.get(getConfigurationFromProps());
    this.DELETE_RETRIES = Integer.parseInt(storeProperties.getProperty("tier.delete.retries", "3"));
    this.evictorThread = new TierEvictorThread(this, fileSystem, getBaseURI().getPath());
    this.evictorThread.start();
  }

  /**
   * Cleanup the store.. interrupts the evictor-thread.
   */
  @Override
  public void destroy() {
    evictorThread.interrupt();
  }

  @Override
  public TierStoreWriter getWriter(final String tableName, final int partitionId,
      final Properties writerProps) {
    // As writer member is store level, if any use want to get writer different properties
    // we should make new instance and send as to avoid any problems.

    Properties props = new Properties();
    // add store properties
    if (this.storeProperties != null) {
      props.putAll(this.storeProperties);
    }
    // add reader properties
    if (reader.getProperties() != null) {
      props.putAll(reader.getProperties());
    }

    // override with user properties
    if (writerProps != null) {
      props.putAll(writerProps);
    }

    props.setProperty(CommonConfig.TABLE_NAME, tableName);
    props.setProperty(CommonConfig.PARTITION_ID, String.valueOf(partitionId));
    props.setProperty(CommonConfig.BASE_URI, getBaseURI().toString());

    try {
      TierStoreWriter tierStoreWriter = writer.getClass().newInstance();
      tierStoreWriter.init(props);
      if (test_Hook_disable_stats)
        this.stats.puttWriters();
      return tierStoreWriter;
    } catch (InstantiationException e) {
      logger.error("Error in instantiating writer " + writer.getClass(), e);
    } catch (IllegalAccessException e) {
      logger.error("Error in instantiating writer " + writer.getClass(), e);
    }
    return null;
  }

  @Override
  public TierStoreWriter getWriter(final String tableName, final int partitionId) {
    return getWriter(tableName, partitionId, writer.getProperties());
  }

  @Override
  public TierStoreReader getReader(final String tableName, final int partitionId,
      final Properties readerProps) {
    // As writer member is store level, if any use want to get writer different properties
    // we should make new instance and send as to avoid any problems.

    Properties props = new Properties();
    // add store properties
    if (this.storeProperties != null) {
      props.putAll(this.storeProperties);
    }
    // add reader properties
    if (reader.getProperties() != null) {
      props.putAll(reader.getProperties());
    }
    // override with user properties
    if (readerProps != null) {
      props.putAll(readerProps);
    }

    props.setProperty(CommonConfig.TABLE_NAME, tableName);
    props.setProperty(CommonConfig.PARTITION_ID, String.valueOf(partitionId));
    props.setProperty(CommonConfig.BASE_URI, getBaseURI().toString());

    try {
      TierStoreReader tierStoreReader = reader.getClass().newInstance();
      tierStoreReader.init(props);
      return tierStoreReader;
    } catch (InstantiationException e) {
      logger.error("Error in instantiating reader " + reader.getClass(), e);
    } catch (IllegalAccessException e) {
      logger.error("Error in instantiating reader " + reader.getClass(), e);
    }
    return null;
  }

  @Override
  public TierStoreReader getReader(final String tableName, final int partitionId) {
    return getReader(tableName, partitionId, reader.getProperties());

  }

  /**
   * Get the tier-store-reader for reading specific portion of the table. It can get the reader to
   * read full table or a bucket from a table or a specific time-partition for a bucket of a table.
   *
   * @param readerProps the reader properties
   * @param args the table part paths
   * @return the reader for the specific unit
   */
  @Override
  public TierStoreReader getReader(final Properties readerProps, final String... args) {
    Properties props = new Properties();
    // add store properties
    if (this.storeProperties != null) {
      props.putAll(this.storeProperties);
    }
    // add reader properties
    if (reader.getProperties() != null) {
      props.putAll(reader.getProperties());
    }
    // override with user properties
    if (readerProps != null) {
      props.putAll(readerProps);
    }

    props.setProperty(CommonConfig.TABLE_PATH_PARTS,
        String.join(CommonConfig.TABLE_PATH_PARTS_SEPARATOR, args));
    props.setProperty(CommonConfig.BASE_URI, getBaseURI().toString());

    try {
      TierStoreReader tierStoreReader = reader.getClass().newInstance();
      tierStoreReader.init(props);
      return tierStoreReader;
    } catch (InstantiationException | IllegalAccessException e) {
      logger.error("Error in instantiating reader " + reader.getClass(), e);
    }
    return null;
  }

  /**
   * Get the tier-store-writer to write a specific chunk/set of records to the table as a single
   * unit.
   *
   * @param writerProps the writer properties
   * @param args the table part paths
   * @return the writer
   */
  @Override
  public TierStoreWriter getWriter(final Properties writerProps, final String... args) {
    Properties props = new Properties();
    // add store properties
    if (this.storeProperties != null) {
      props.putAll(this.storeProperties);
    }
    // add reader properties
    if (reader.getProperties() != null) {
      props.putAll(reader.getProperties());
    }
    // override with user properties
    if (writerProps != null) {
      props.putAll(writerProps);
    }

    props.setProperty(CommonConfig.TABLE_PATH_PARTS,
        String.join(CommonConfig.TABLE_PATH_PARTS_SEPARATOR, args));
    props.setProperty(CommonConfig.BASE_URI, getBaseURI().toString());

    try {
      TierStoreWriter tierStoreWriter = writer.getClass().newInstance();
      tierStoreWriter.init(props);
      return tierStoreWriter;
    } catch (InstantiationException | IllegalAccessException e) {
      logger.error("Error in instantiating writer " + writer.getClass(), e);
    }
    return null;
  }

  @Override
  public void deleteTable(final String tableName) {
    delete(tableName);
  }

  @Override
  public void deletePartition(final String tableName, final int partitionId) {
    delete(tableName, String.valueOf(partitionId));
  }

  @Override
  public void stopMonitoring() {
    evictorThread.interrupt();
  }

  /**
   * Delete the content at the specified path from the tier-store. It deletes the file or the
   * complete directory, recursively, with max-retries (default 3).
   *
   * @param unitPath the unit-path to be deleted
   */
  @Override
  public void delete(final String... unitPath) {
    final Path path = new Path(getBaseURI().toString(),
        String.join(CommonConfig.TABLE_PATH_PARTS_SEPARATOR, unitPath));
    try {
      if (!this.fileSystem.exists(path)) {
        logger.debug("Path does not exist: baseUri= {}, path= {}", getBaseURI(), path);
        return;
      }
    } catch (IOException e) {
      logger.warn("Error accessing file: baseUri= {}, path= {}", getBaseURI(), path);
    }
    boolean isDeleted = false;
    int i = 0;
    while (!isDeleted && i < DELETE_RETRIES) {
      try {
        isDeleted = this.fileSystem.delete(path, true);
      } catch (IOException e) {
        logger.error("Error in deleting store files: baseUri= {}, path= {}", getBaseURI(), path, e);
      }
      i++;
    }
    if (!isDeleted) {
      logger.warn("Failed to delete store files after {} attempts: baseUri= {}, path= {}", i,
          getBaseURI(), path);
    }
  }

  @Override
  public URI getBaseURI() {
    return URI.create(this.storeProperties.getProperty(BASE_DIR_PATH, "."));
  }

  private Configuration getConfigurationFromProps() throws IOException {
    return ConfigurationUtils.getConfiguration(getMergedProperties());
  }

  private Properties getMergedProperties() {
    Properties props = new Properties();
    // add store properties
    if (this.storeProperties != null) {
      props.putAll(this.storeProperties);
    }
    // add reader properties
    if (reader.getProperties() != null) {
      props.putAll(reader.getProperties());
    }
    // override with user properties
    if (writer.getProperties() != null) {
      props.putAll(writer.getProperties());
    }

    return props;
  }

  @Override
  public void truncateBucket(String tableName, int partitionId, Filter filter, TableDescriptor td,
      Properties tierProperties) throws IOException {
    removeRecordsFromBucket(tableName, partitionId, filter, td, tierProperties);
  }

  @Override
  public void updateBucket(String tableName, int partitionId, Filter filter,
      Map<Integer, Object> colValues, TableDescriptor td, Properties tierProperties)
      throws IOException {
    updateRecordsInBucket(tableName, partitionId, filter, colValues, td, tierProperties);
  }

  /**
   * Update records in a tier bucket.
   *
   * @param tableName Name of the table
   * @param partitionId partition Id
   * @param filter Filter to select records for updation.
   * @param colValues A map of col name to new col values.
   * @param td Table descriptor
   * @param tierProps Tier properties.
   */
  private void updateRecordsInBucket(String tableName, int partitionId, Filter filter,
      Map<Integer, Object> colValues, TableDescriptor td, Properties tierProps) throws IOException {
    FileSystem fs = FileSystem.get(getConfigurationFromProps());
    Path basePath = new Path(getBaseURI().getPath());
    if (!fs.exists(basePath)) {
      logger.debug("No files to process at location: {}", basePath);
      return;
    }

    final Map<String, TierStoreConfiguration> tiers = td != null && td instanceof FTableDescriptor
        ? ((FTableDescriptor) td).getTierStores() : null;
    if (tiers == null || tiers.isEmpty()) {
      logger.debug("{}: Skipping table= {}; either it is not an FTable or does not have any tiers.",
          tableName);
      return;
    }

    final ConverterDescriptor cd = getConverterDescriptor(tableName);

    Path tableStore = new Path(basePath.toString() + File.separator + tableName);
    if (fs.isDirectory(tableStore)) {
      Path partitionStore = new Path(tableStore.toString() + File.separator + partitionId);
      final FileStatus[] timePartitions = fs.listStatus(partitionStore);
      for (FileStatus timePartition : timePartitions) {
        final String timePartId = timePartition.getPath().getName();
        final String[] unitPath = Paths.get(tableName, Integer.toString(partitionId), timePartId)
            .toString().split(File.separator);
        final int batchSize = (int) tierProps.getOrDefault("update-batch-size", 1_000);
        updateRecordsInUnit(cd, batchSize, timePartition, td, filter, colValues, unitPath);
      }
    } else {
      throw new IOException(tableStore.toString() + " is not a directory");
    }
  }

  /**
   * Update records in a partition directory
   *
   * @param cd the converter descriptor
   * @param batchSize the batch-size to be used for writing to the next tier
   * @param td Table descriptor
   * @param filter Filter criteria
   * @param unitPath the absolute path for the unit to be moved (table/bucket/time-part) @throws
   */
  private void updateRecordsInUnit(final ConverterDescriptor cd, final int batchSize,
      FileStatus timePartition, TableDescriptor td, Filter filter, Map<Integer, Object> colValues,
      final String... unitPath) throws IOException {

    FileSystem fs = FileSystem.get(getConfigurationFromProps());
    int updatedCount = 0;
    boolean processingError = false;
    String[] tmpUnitPath = new String[unitPath.length];
    for (int i = 0; i < unitPath.length; i++) {
      tmpUnitPath[i] = unitPath[i];
    }
    tmpUnitPath[tmpUnitPath.length - 1] = tmpUnitPath[tmpUnitPath.length - 1] + ".tmp";
    // Path tmpPartitionStore = new Path(partitionStore.toString() + "tmp");
    Properties props = new Properties();

    final ThinRow row = ThinRow.create(td, ThinRow.RowFormat.F_FULL_ROW);

    try {
      final TierStoreReader reader = getReader(props, unitPath);
      final TierStoreWriter writer = getWriter(props, tmpUnitPath);
      reader.setConverterDescriptor(cd);
      writer.setConverterDescriptor(cd);
      writer.openChunkWriter(props);
      final Iterator<StoreRecord> srItr = reader.iterator();
      List<StoreRecord> srs = new ArrayList<>(batchSize);
      final Encoding enc = td.getEncoding();
      while (srItr.hasNext()) {
        StoreRecord storeRecord = srItr.next();

        boolean matched = true;
        if (filter != null) {
          row.reset(null, (byte[]) enc.serializeValue(td, storeRecord));
          matched = ScanUtils.executeSimpleFilter(row, filter, td);
        }
        if (matched) {
          updateSingleRecord(storeRecord, colValues);
          updatedCount++;
        }
        srs.add(storeRecord);
        if (srs.size() == batchSize) {
          writer.writeChunk(srs);
          srs.clear();
        }
      }
      if (srs.size() > 0) {
        writer.writeChunk(srs);
        srs.clear();
      }
      writer.closeChunkWriter();
      processingError = false;
    } catch (Exception e) {
      logger.error("{}: Error while updating data in tier= {} for: {}", getName(), unitPath, e);
      processingError = true;
      fs.delete(new Path(timePartition.getPath().toString() + ".tmp"), true);
    }
    if (processingError) {
      logger.info("{}: Skipping updation as error occurred while processing.", getName());
      throw new IOException(
          "Error while updating data in tier= " + getName() + " for: " + unitPath);
    } else {
      /* TODO: this has to be done under a lock */
      fs.delete(timePartition.getPath(), true);
      fs.rename(new Path(timePartition.getPath().toString() + ".tmp"), timePartition.getPath());
      logger.debug("Updated {} records in `{}`", updatedCount, getName());
    }
  }

  private void updateSingleRecord(StoreRecord storeRrecord, Map<Integer, Object> colValues) {
    for (Map.Entry<Integer, Object> entry : colValues.entrySet()) {
      storeRrecord.updateValue(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Get the converter descriptor for the specified table. The converter descriptor can convert the
   * values that to/from ORC into respective Java objects when reading/writing the data from/to the
   * respective tier-store and Ampool.
   *
   * @param tableName the table name
   * @return the converter descriptor
   */
  @Override
  public ConverterDescriptor getConverterDescriptor(String tableName) {
    if (!converterDescriptors.containsKey(tableName)) {
      TableDescriptor tableDescriptor =
          MCacheFactory.getAnyInstance().getTableDescriptor(tableName);
      if (this.fileFormat == FileFormats.PARQUET) {
        converterDescriptors.put(tableName, new ParquetConverterDescriptor(tableDescriptor));
      } else {
        converterDescriptors.put(tableName, new ORCConverterDescriptor(tableDescriptor));
      }
    }
    return converterDescriptors.get(tableName);
  }

  @Override
  public TierStoreStats getStats() {
    return this.stats;
  }

  @Override
  public long getBytesWritten() {
    try {
      return FileSystem.getStatistics(fileSystem.getScheme(), fileSystem.getClass())
          .getBytesWritten();
    } catch (Exception e) {
      // logger.error(" The getBytesWritten was not returned correctly" + e.toString());
      return 0;
    }
  }


  @Override
  public long getBytesRead() {
    try {
      return FileSystem.getStatistics(fileSystem.getScheme(), fileSystem.getClass()).getBytesRead();
    } catch (Exception e) {
      // logger.error(" The getBytesRead was not returned correctly" + e.toString());
      return 0;
    }
  }

  /**
   * Remove records from this bucket which match the filter.
   *
   * @param tableName Name of the table
   * @param partitionId Parition Id
   * @param filter Filter criteria
   * @param td Table descriptor
   * @param tierProps Table specific tier properties
   * @throws IOException in case there was an exception while reading or writing
   */
  void removeRecordsFromBucket(String tableName, int partitionId, Filter filter, TableDescriptor td,
      Properties tierProps) throws IOException {
    FileSystem fs = FileSystem.get(getConfigurationFromProps());
    Path basePath = new Path(getBaseURI().getPath());
    if (!fs.exists(basePath)) {
      logger.debug("No files to process at location: {}", basePath);
      return;
    }

    final Map<String, TierStoreConfiguration> tiers = td != null && td instanceof FTableDescriptor
        ? ((FTableDescriptor) td).getTierStores() : null;
    if (tiers == null || tiers.isEmpty()) {
      logger.debug("{}: Skipping table= {}; either it is not an FTable or does not have any tiers.",
          tableName);
      return;
    }

    final ConverterDescriptor cd = getConverterDescriptor(tableName);

    Path tableStore = new Path(basePath.toString() + File.separator + tableName);
    if (fs.isDirectory(tableStore)) {
      Path partitionStore = new Path(tableStore.toString() + File.separator + partitionId);
      final FileStatus[] timePartitions = fs.listStatus(partitionStore);
      for (FileStatus timePartition : timePartitions) {
        final String timePartId = timePartition.getPath().getName();
        final String[] unitPath = Paths.get(tableName, Integer.toString(partitionId), timePartId)
            .toString().split(File.separator);
        final int batchSize = (int) tierProps.getOrDefault("move-batch-size", 1_000);
        removeRecordsFromUnit(cd, batchSize, timePartition, td, filter, unitPath);
      }
    } else {
      throw new IOException(tableStore.toString() + " is not a directory");
    }
  }

  /**
   * Truncate data from a partition directory
   *
   * @param cd the converter descriptor
   * @param batchSize the batch-size to be used for writing to the next tier
   * @param td Table descriptor
   * @param filter Filter criteria
   * @param unitPath the absolute path for the unit to be moved (table/bucket/time-part) @throws
   *        IOException in case there were errors reading or writing
   */
  private void removeRecordsFromUnit(final ConverterDescriptor cd, final int batchSize,
      FileStatus timePartition, TableDescriptor td, Filter filter, final String... unitPath)
      throws IOException {

    FileSystem fs = FileSystem.get(getConfigurationFromProps());
    int movedCount = 0;
    boolean processingError = false;
    String[] tmpUnitPath = new String[unitPath.length];
    for (int i = 0; i < unitPath.length; i++) {
      tmpUnitPath[i] = unitPath[i];
    }
    tmpUnitPath[tmpUnitPath.length - 1] = tmpUnitPath[tmpUnitPath.length - 1] + "tmp";
    // Path tmpPartitionStore = new Path(partitionStore.toString() + "tmp");
    Properties props = new Properties();

    final ThinRow row = ThinRow.create(td, ThinRow.RowFormat.F_FULL_ROW);

    try {
      final TierStoreReader reader = getReader(props, unitPath);
      final TierStoreWriter writer = getWriter(props, tmpUnitPath);
      reader.setConverterDescriptor(cd);
      writer.setConverterDescriptor(cd);
      writer.openChunkWriter(props);
      final Iterator<StoreRecord> srItr = reader.iterator();
      final Encoding enc = td.getEncoding();
      // TODO can be improved
      List<StoreRecord> srs = new ArrayList<>(batchSize);
      while (srItr.hasNext()) {
        StoreRecord storeRecord = srItr.next();

        boolean matched = false;
        if (filter != null) {
          row.reset(null, (byte[]) enc.serializeValue(td, storeRecord));
          matched = ScanUtils.executeSimpleFilter(row, filter, td);
        }
        if (!matched) {
          srs.add(storeRecord);
          movedCount++;
        }
        if (srs.size() == batchSize) {
          writer.writeChunk(srs);
          srs.clear();
        }
      }
      if (srs.size() > 0) {
        writer.writeChunk(srs);
        srs.clear();
      }
      writer.closeChunkWriter();
      processingError = false;
    } catch (Exception e) {
      logger.error("{}: Error while deleting data from tier= {} for: {}", getName(), unitPath, e);
      processingError = true;
      fs.delete(new Path(timePartition.getPath().toString() + "tmp"), true);
    }
    if (processingError) {
      logger.info("{}: Skipping deletion as error occurred while processing.", getName());
      throw new IOException(
          "Error while deleting data from tier= " + getName() + " for: " + unitPath);
    } else {
      /* TODO: this has to be done under a lock */
      fs.delete(timePartition.getPath(), true);
      fs.rename(new Path(timePartition.getPath().toString() + "tmp"), timePartition.getPath());
      logger.debug("Deleted {} records from `{}`", movedCount, getName());
    }
  }

  @Override
  public Properties getProperties() {
    return storeProperties;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String toString() {
    return "LocalTierStore[" + this.name + "]{" + "storeProperties=" + storeProperties + ", writer="
        + writer + ", reader=" + reader + '}';
  }

  private static ConcurrentHashMap<String, ConverterDescriptor> converterDescriptors =
      new ConcurrentHashMap<>();
}
