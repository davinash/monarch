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
package io.ampool.tierstore.readers.orc;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import io.ampool.monarch.table.ftable.internal.ReaderOptions;
import io.ampool.monarch.types.TypeHelper;
import io.ampool.orc.OrcUtils;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.ColumnConverterDescriptor;
import io.ampool.tierstore.internal.AbstractTierStoreReader;
import org.apache.geode.internal.logging.LogService;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.FTableOrcStruct;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.logging.log4j.Logger;

public class TierStoreORCReader extends AbstractTierStoreReader {
  private static final Logger logger = LogService.getLogger();
  private RecordReader recordReader = null;
  private static final String Crc = ".crc";

  private boolean isInitialized = false;

  public TierStoreORCReader() {

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
            logger.debug("OrcFilePath= {}, TimePartRanges= {}, ScannedFiles= {}, ReaderOptions= {}",
                    partPath, scan.getRanges(), TypeHelper.deepToString(fileList), getReaderOptions());
            if (fileList == null) {
              return false;
            }
          }
          if (recordReader != null && recordReader.hasNext()) {
            return true;
          } else if (fileList.length > 0 && fileListIndex < fileList.length) {
            Path path = new Path(partPath.toString(), fileList[fileListIndex++]);
            final Reader fis = OrcFile.createReader(path, OrcFile.readerOptions(conf));
            final ReaderOptions opts = getReaderOptions();
            // can be called more parameterized rows to filter at ORC level
            recordReader = opts == null ? fis.rows()
                    : fis.rowsOptions(((OrcUtils.OrcOptions) opts).getOptions());
            return hasNext();
          } else {
            // end of file list return false
            return false;
          }
        } catch (IOException e) {
          // move to next file and get next reader
          e.printStackTrace();
          logger.error("Error in creating the ORC Reader.", e);
        }
        return false;
      }

      @Override
      public StoreRecord next() {
        if (recordReader == null) {
          return null;
        }
        try {
          FTableOrcStruct row = null;
          if (recordReader.hasNext()) {
            return getStorerecord((OrcStruct) recordReader.next(row));
          } else {
            recordReader.close();
            recordReader = null;
          }
        } catch (IOException e) {
          logger.error(e.getMessage());
        }
        return null;
      }
    };
  }

  private StoreRecord getStorerecord(OrcStruct orcStruct) {
    final FTableOrcStruct fTableOrcStruct = new FTableOrcStruct(orcStruct);
    StoreRecord storeRecord = new StoreRecord(fTableOrcStruct.getNumFields());
    final List<ColumnConverterDescriptor> columnConverters =
            converterDescriptor.getColumnConverters();
    for (int i = 0; i < fTableOrcStruct.getNumFields(); i++) {
      storeRecord.addValue(columnConverters.get(i).getReadable(fTableOrcStruct.getFieldValue(i)));
    }
    return storeRecord;
  }

  @Override
  public String toString() {
    return "TierStoreORCReader{" + "recordReader=" + recordReader + ", isInitialized="
            + isInitialized + '}';
  }
}
