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

import io.ampool.store.StoreRecord;
import io.ampool.tierstore.config.orc.ORCWriterConfig;
import io.ampool.tierstore.internal.AbstractTierStoreWriter;
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
import java.util.List;
import java.util.Properties;

public class TierStoreORCWriter extends AbstractTierStoreWriter {

  private static final Logger logger = LogService.getLogger();

  protected static final CompressionKind COMPRESSION_KIND = CompressionKind.NONE;
  protected static final int STRIPE_SIZE = 1000;
  protected static final int BUFFER_SIZE = 1000;
  protected static final int NEW_INDEX_STRIDE = 1000;

  /**
   * Default constructor which will system will call
   */
  public TierStoreORCWriter() {
    super();
  }

  @Override
  protected int _write(final Properties props, final List<StoreRecord> rows, long timeInterval,
      URI baseURI, String tableName, int partitionId, Configuration conf)
      throws MalformedURLException {

    CompressionKind compression = getCompressionKind(props);
    int bufferSize = getBufferSize(props);
    int stripeSize = getStripeSize(props);
    int newIndexStride = getNewIndexStride(props);

    int numberOfRowsWritten = 0;
    try {
      long tpi, lastTpi = 0;
      OrcWriterWrapper writer = null;
      /** multiple records with insertion-time in the interval will be stored in same partition **/
      for (final StoreRecord record : rows) {
        if (record == null) {
          continue;
        }
        tpi = getTimePartitionKey(record.getTimeStamp(), timeInterval);
        if (lastTpi != tpi
            || writer == null /* || (numberOfRowsWritten % FLUSH_COUNT_FOR_TESTING == 0) */) {
          if (writer != null) {
            writer.close();
          }
          writer = createWriter(
              Paths.get(baseURI.getPath(), tableName, String.valueOf(partitionId),
                  String.valueOf(tpi)),
              conf, compression, bufferSize, stripeSize, newIndexStride,
              WriteAheadLog.WAL_FILE_RECORD_LIMIT);
        }
        lastTpi = tpi;
        writer.writeOneRecord(record);
        numberOfRowsWritten++;
      }
      if (writer != null) {
        writer.close();
      }
    } catch (IOException e) {
      logger.error("Failed to append records to store: tableName= {}, partitionId= {}", tableName,
          partitionId, e);
      numberOfRowsWritten = 0;
    } catch (Throwable e) {
      logger.error("Failed to append records to store: tableName= {}, partitionId= {}", tableName,
          partitionId, e);
      numberOfRowsWritten = 0;
    } finally {
      return numberOfRowsWritten;
    }
  }

  private int getNewIndexStride(Properties props) {
    return Integer.parseInt(props.getProperty(ORCWriterConfig.NEW_INDEX_STRIDE,
        String.valueOf(TierStoreORCWriter.NEW_INDEX_STRIDE)));
  }

  private int getStripeSize(Properties props) {
    return Integer.parseInt(props.getProperty(ORCWriterConfig.STRIPE_SIZE,
        String.valueOf(TierStoreORCWriter.STRIPE_SIZE)));
  }

  private int getBufferSize(Properties props) {
    return Integer.parseInt(props.getProperty(ORCWriterConfig.BUFFER_SIZE,
        String.valueOf(TierStoreORCWriter.BUFFER_SIZE)));
  }

  private CompressionKind getCompressionKind(Properties props) {
    return CompressionKind.valueOf(props.getProperty(ORCWriterConfig.COMPRESSION_KIND,
        String.valueOf(TierStoreORCWriter.COMPRESSION_KIND)));
  }


  private OrcWriterWrapper chunkWriter = null;

  @Override
  public void _openChunkWriter(final Properties props, URI baseURI, String[] tablePathParts,
      Configuration conf) throws IOException {
    CompressionKind compression = getCompressionKind(props);
    int bufferSize = getBufferSize(props);
    int stripeSize = getStripeSize(props);
    int newIndexStride = getNewIndexStride(props);
    this.chunkWriter = createWriter(Paths.get(baseURI.getPath(), tablePathParts), conf, compression,
        bufferSize, stripeSize, newIndexStride, WriteAheadLog.WAL_FILE_RECORD_LIMIT);
  }

  @Override
  public int writeChunk(final List<StoreRecord> records) throws IOException {
    for (final StoreRecord record : records) {
      this.chunkWriter.writeOneRecord(record);
    }
    return records.size();
  }

  @Override
  public void closeChunkWriter() throws IOException {
    this.chunkWriter.close();
  }

  /**
   * Create the ORC writer for the specified path and configuration. The ORC files are created by
   * using a sequential numbering scheme starting with 0. For each subsequent file the counter is
   * incremented and maintained per table per bucket per time-partition. In case the same file is
   * getting used (due to some issues in sequence counter generation) an exception is thrown.
   * 
   * @param path the path of the ORC file to be created
   * @param conf the ORC writer configuration
   * @param newIndexStride @return the ORC writer
   * @throws IOException if the file is already present or there was an issue creating the writer
   */
  public OrcWriterWrapper createWriter(final java.nio.file.Path path, final Configuration conf,
      final CompressionKind compression, final int bufferSize, final int stripeSize,
      final int newIndexStride, final int walFileRecordLimit) throws IOException {
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
    return new OrcWriterWrapper(fileSystem, newFilePath, conf, converterDescriptor, stripeSize,
        compression, bufferSize, newIndexStride, walFileRecordLimit);
  }



}
