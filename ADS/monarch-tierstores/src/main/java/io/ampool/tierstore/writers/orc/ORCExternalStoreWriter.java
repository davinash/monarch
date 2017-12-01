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
package io.ampool.tierstore.writers.orc;

import io.ampool.monarch.table.ArchiveConfiguration;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.store.ExternalStoreWriter;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.config.CommonConfig;
import io.ampool.tierstore.config.orc.ORCWriterConfig;
import io.ampool.tierstore.internal.ORCConverterDescriptor;
import io.ampool.tierstore.wal.WriteAheadLog;
import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;


public class ORCExternalStoreWriter implements ExternalStoreWriter {
  private static final Logger logger = LogService.getLogger();

  /**
   * This map stores tablename -> sequence number.
   */
  private static Map<String, AtomicLong> bucketSeqNoMap = new HashMap<>();

  public ORCExternalStoreWriter() {}

  @Override
  public void write(TableDescriptor tableDescriptor, Row[] rows, ArchiveConfiguration configuration)
      throws Exception {
    Properties newProps = new Properties();
    newProps.putAll(configuration.getFsProperties());

    URI baseURI = URI.create(newProps.getProperty(ArchiveConfiguration.BASE_DIR_PATH));
    Configuration conf = new Configuration();
    String hdfsSiteXMLPath = newProps.getProperty(CommonConfig.HDFS_SITE_XML_PATH);
    String hadoopSiteXMLPath = newProps.getProperty(CommonConfig.HADOOP_SITE_XML_PATH);
    if (hdfsSiteXMLPath != null) {
      conf.addResource(Paths.get(hdfsSiteXMLPath).toUri().toURL());
    }
    if (hadoopSiteXMLPath != null) {
      conf.addResource(Paths.get(hadoopSiteXMLPath).toUri().toURL());
    }
    newProps.entrySet().forEach((PROP) -> {
      conf.set(String.valueOf(PROP.getKey()), String.valueOf(PROP.getValue()));
    });

    // process records and write

    CompressionKind compression =
        CompressionKind.valueOf(newProps.getProperty(ORCWriterConfig.COMPRESSION_KIND,
            String.valueOf(TierStoreORCWriter.COMPRESSION_KIND)));
    int bufferSize = Integer.parseInt(newProps.getProperty(ORCWriterConfig.BUFFER_SIZE,
        String.valueOf(TierStoreORCWriter.BUFFER_SIZE)));
    int stripeSize = Integer.parseInt(newProps.getProperty(ORCWriterConfig.STRIPE_SIZE,
        String.valueOf(TierStoreORCWriter.STRIPE_SIZE)));
    int newIndexStride = Integer.parseInt(newProps.getProperty(ORCWriterConfig.NEW_INDEX_STRIDE,
        String.valueOf(TierStoreORCWriter.NEW_INDEX_STRIDE)));
    int numberOfRowsWritten = 0;
    try {
      OrcWriterWrapper writer = createWriter(tableDescriptor,
          Paths.get(baseURI.getPath(), tableDescriptor.getTableName()), conf, compression,
          bufferSize, stripeSize, newIndexStride, WriteAheadLog.WAL_FILE_RECORD_LIMIT);;
      /** multiple records with insertion-time in the interval will be stored in same partition **/
      for (final Row row : rows) {
        writer.writeOneRow(row);
        numberOfRowsWritten++;
      }
      if (writer != null) {
        writer.close();
      }
    } catch (Throwable e) {
      logger.error("Failed to append records to store: tableName= {}",
          tableDescriptor.getTableName(), e);
      numberOfRowsWritten = 0;
      throw e;
    } finally {
      logger.info("Writtern records : ", +numberOfRowsWritten);
    }
  }

  /**
   * Create the ORC writer for the specified path and configuration. The ORC files are created by
   * using a sequential numbering scheme starting with 0. For each subsequent file the counter is
   * incremented and maintained per table per bucket per time-partition. In case the same file is
   * getting used (due to some issues in sequence counter generation) an exception is thrown.
   *
   *
   * @param tableDescriptor
   * @param path the path of the ORC file to be created
   * @param conf the ORC writer configuration
   * @param compression
   * @param bufferSize
   * @param stripeSize
   * @param newIndexStride @return the ORC writer
   * @param walFileRecordLimit
   * @throws IOException if the file is already present or there was an issue creating the writer
   */
  public OrcWriterWrapper createWriter(TableDescriptor tableDescriptor,
      final java.nio.file.Path path, final Configuration conf, final CompressionKind compression,
      final int bufferSize, final int stripeSize, final int newIndexStride,
      final int walFileRecordLimit) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);
    final String pathStr = path.toString();
    final Path newFilePath;
    while (true) {
      long seqNo = getNextSeqNo(pathStr);
      Path newPath = new Path(pathStr + "/" + String.valueOf(seqNo));
      if (!fileSystem.exists(newPath)) {
        newFilePath = newPath;
        break;
      }
    }
    return new OrcWriterWrapper(fileSystem, newFilePath, conf,
        new ORCConverterDescriptor(tableDescriptor), stripeSize, compression, bufferSize,
        newIndexStride, walFileRecordLimit);
  }

  /**
   * Get the next sequence number for the specified path that includes: table-name, partition/bucket
   * id, time-partition-id
   *
   * @param path the unique path against which the sequence id is maintained
   * @return the next available sequence id
   */
  public long getNextSeqNo(final String path) {
    synchronized (bucketSeqNoMap) {
      AtomicLong mapValue = bucketSeqNoMap.get(path);
      if (mapValue != null) {
        return mapValue.getAndIncrement();
      }
      bucketSeqNoMap.put(path, new AtomicLong(1));
      return 0;
    }
  }
}
