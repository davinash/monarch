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

package io.ampool.tierstore.readers.parquet;

import io.ampool.monarch.types.TypeHelper;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.ColumnConverterDescriptor;
import io.ampool.tierstore.internal.AbstractTierStoreReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Parquet Reader.
 */
public class TierStoreParquetReader extends AbstractTierStoreReader {
  private static final Logger logger = LogService.getLogger();
  private static final String Crc = ".crc";

  private boolean isInitialized = false;
  private ParquetReader<GenericRecord> reader;
  private GenericRecord recordToServe = null;

  public TierStoreParquetReader() {

  }

  /**
   * Returns an iterator over elements of type {@code T}.
   * 
   * @return an Iterator.
   */
  @Override
  public Iterator<StoreRecord> iterator() {
    return new Iterator<StoreRecord>() {
      @Override
      public boolean hasNext() {
        try {
          if (!isInitialized) {
            readProperties();
            isInitialized = true;
          }
          if (fileList == null) {
            // initialize file list
            fileList = getFilesToScan(tableName, partitionId, scan.getRanges(), Crc);
            logger.debug("ParquetFilePath= {}, TimePartRanges= {}, ScannedFiles= {}", partPath,
                scan.getRanges(), TypeHelper.deepToString(fileList));
            if (fileList == null) {
              return false;
            }
          }
          if (recordToServe != null) {
            return true;
          } else if (reader != null) {
            recordToServe = reader.read();
            if (recordToServe == null) {
              reader.close();
              reader = null;
            }
            return hasNext();
          } else if (fileList.length > 0 && fileListIndex < fileList.length) {
            Path path = new Path(partPath.toString(), fileList[fileListIndex++]);
            reader = AvroParquetReader.<GenericRecord>builder(path).build();
            recordToServe = reader.read();
            if (recordToServe == null) {
              // End of the file
              reader.close();
              reader = null;
            }
            return hasNext();
          } else {
            // end of file list return false
            return false;
          }
        } catch (IOException e) {
          // move to next file and get next reader
          e.printStackTrace();
        }
        return false;
      }

      @Override
      public StoreRecord next() {
        if (recordToServe == null) {
          try {
            reader.close();
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
          return null;
        }
        StoreRecord storerecord = getStorerecord(recordToServe);
        recordToServe = null;
        return storerecord;
      }
    };
  }

  private StoreRecord getStorerecord(GenericRecord record) {
    final List<ColumnConverterDescriptor> columnConverters =
        converterDescriptor.getColumnConverters();
    StoreRecord storeRecord = new StoreRecord(columnConverters.size());
    columnConverters.forEach(columnConverterDescriptor -> {
      storeRecord.addValue(columnConverterDescriptor
          .getReadable(record.get(columnConverterDescriptor.getColumnName())));
    });
    return storeRecord;
  }

  @Override
  public String toString() {
    return "TierStoreORCReader{" + "Reader=" + reader + ", isInitialized=" + isInitialized + '}';
  }
}
