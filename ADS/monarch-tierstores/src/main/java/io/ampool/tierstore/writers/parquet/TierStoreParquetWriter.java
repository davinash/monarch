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

package io.ampool.tierstore.writers.parquet;

import io.ampool.store.StoreRecord;
import io.ampool.tierstore.config.parquet.ParquetWriterConfig;
import io.ampool.tierstore.internal.AbstractTierStoreWriter;
import io.ampool.tierstore.wal.WriteAheadLog;
import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/**
 * Parquet Writer
 */
public class TierStoreParquetWriter extends AbstractTierStoreWriter {

  private static final Logger logger = LogService.getLogger();

  protected static final CompressionCodecName COMPRESSION_KIND = CompressionCodecName.UNCOMPRESSED;
  protected static final int BLOCK_SIZE = 256 * 1024 * 1024;
  protected static final int PAGE_SIZE = 64 * 1024;
  private ParquetWriterWrapper chunkWriter;

  public TierStoreParquetWriter() {}

  @Override
  public int writeChunk(List<StoreRecord> records) throws IOException {
    for (final StoreRecord record : records) {
      this.chunkWriter.writeOneRecord(record);
    }
    return records.size();
  }

  @Override
  public void closeChunkWriter() throws IOException {
    this.chunkWriter.close();
  }

  @Override
  protected int _write(Properties props, List<StoreRecord> rows, long timeInterval, URI baseURI,
      String tableName, int partitionId, Configuration conf) throws MalformedURLException {
    CompressionCodecName compression = getCompressionKind(props);
    int blockSize = getBlockSize(props);
    int pageSize = getPageSize(props);

    int numberOfRowsWritten = 0;
    try {
      long tpi, lastTpi = 0;
      ParquetWriterWrapper writer = null;
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
          writer = createWriter(props,
              Paths.get(baseURI.getPath(), tableName, String.valueOf(partitionId),
                  String.valueOf(tpi)),
              conf, compression, blockSize, pageSize, WriteAheadLog.WAL_FILE_RECORD_LIMIT);
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

  @Override
  protected void _openChunkWriter(Properties props, URI baseURI, String[] tablePathParts,
      Configuration conf) throws IOException {
    CompressionCodecName compression = getCompressionKind(props);
    int blockSize = getBlockSize(props);
    int pageSize = getPageSize(props);

    this.chunkWriter = createWriter(props,
        Paths.get(baseURI.getPath(), tablePathParts[0], String.valueOf(tablePathParts[1]),
            String.valueOf(tablePathParts[2])),
        conf, compression, blockSize, pageSize, WriteAheadLog.WAL_FILE_RECORD_LIMIT);
  }

  private int getBlockSize(Properties props) {
    return Integer.parseInt(props.getProperty(ParquetWriterConfig.BLOCK_SIZE,
        String.valueOf(TierStoreParquetWriter.BLOCK_SIZE)));
  }

  private int getPageSize(Properties props) {
    return Integer.parseInt(props.getProperty(ParquetWriterConfig.PAGE_SIZE,
        String.valueOf(TierStoreParquetWriter.PAGE_SIZE)));
  }

  private CompressionCodecName getCompressionKind(Properties props) {
    return CompressionCodecName
        .valueOf(props.getProperty(ParquetWriterConfig.COMPRESSION_CODEC_NAME,
            String.valueOf(TierStoreParquetWriter.COMPRESSION_KIND)));
  }

  /**
   * Create the Parquet writer for the specified path and configuration. The Parquet files are
   * created by using a sequential numbering scheme starting with 0. For each subsequent file the
   * counter is incremented and maintained per table per bucket per time-partition. In case the same
   * file is getting used (due to some issues in sequence counter generation) an exception is
   * thrown.
   * 
   * @param path the path of the Parquet file to be created
   * @param conf the Parquet writer configuration
   * @throws IOException if the file is already present or there was an issue creating the writer
   */
  public ParquetWriterWrapper createWriter(Properties props, final java.nio.file.Path path,
      final Configuration conf, final CompressionCodecName compression, final int blockSize,
      final int pageSize, final int walFileRecordLimit) throws IOException {
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
    return new ParquetWriterWrapper(props, fileSystem, newFilePath, conf, converterDescriptor,
        compression, blockSize, pageSize, walFileRecordLimit);
  }
}
