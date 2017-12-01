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

import static io.ampool.monarch.hive.MonarchPredicateHandler.getPushDownFilters;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.filter.FilterList;
import io.ampool.monarch.table.filter.SingleColumnValueFilter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.InternalTable;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.SingleVersionRow;
import io.ampool.monarch.table.internal.VersionedRow;
import io.ampool.monarch.table.results.FormatAwareRow;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Monarch record reader for Hive.
 * <p>
 */
public class MonarchRecordReader implements RecordReader<Writable, Writable> {
  private static final Logger logger = LoggerFactory.getLogger(MonarchRecordReader.class);
  private InternalTable anyTable = null;
  private float progress = 0f;

  /** the reader statistics **/
  private long startTime = 0;
  private long lastNextTime = 0;
  private long rowCount = 0;
  private long totalBytes = 0;
  private long timeInNext = 0;
  private long timeInGetAll = 0;
  private long timeOutsideNext = 0;

  /** the block (of records/rows) details **/
  private int batchSize = MonarchUtils.MONARCH_BATCH_SIZE_DEFAULT;

  private Scanner mResultScanner = null;

  private Iterator<Row> valueIterator = Collections.emptyIterator();

  /**
   * empty byte.. used for dummy key
   **/
  private static final byte[] EMPTY_BYTE = new byte[0];
  private String[] columns = null;
  protected FilterList pushDownfilters = null;
  protected List<Integer> readColIds = null;



  public MonarchRecordReader(final Configuration conf) {
    String conf_columns = conf.get("columns");
    this.columns = conf_columns == null ? null : conf_columns.split(",");
    boolean isFTable = MonarchUtils.isFTable(conf);
    if (isFTable) {
      this.anyTable = (InternalTable) MonarchUtils.getFTableInstance(conf, this.columns);
    } else {
      this.anyTable = (InternalTable) MonarchUtils.getTableInstance(conf, this.columns);
    }
  }

  public void initialize(final InputSplit split, final Configuration conf) throws IOException {
    this.startTime = System.currentTimeMillis();

    /** batch size for reading multiple records together **/
    batchSize = NumberUtils.toInt(conf.get(MonarchUtils.MONARCH_BATCH_SIZE), MonarchUtils.MONARCH_BATCH_SIZE_DEFAULT);
    final MonarchSplit ms = (MonarchSplit) split;

    this.readColIds = ColumnProjectionUtils.getReadColumnIDs(conf);

    final String expression = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (expression != null && columns != null) {
      this.pushDownfilters = getPushDownFilters(expression, columns);
      if (this.pushDownfilters != null) {
        for (Filter mFilter : pushDownfilters.getFilters()) {
          logger.info("Pushing filter= {}", mFilter);
          int id=-1;
          for(int i=0; i< columns.length; i++){
            if(columns[i].equalsIgnoreCase(((SingleColumnValueFilter) mFilter).getColumnNameString())) {
              id =i;
              break;
            }
          }
          if (!readColIds.contains(id) && readColIds.size() > 0 && id != -1) {
            readColIds.add(id);
          }
        }
      }
    }

    /** create the scan with required parameters.. **/
    Scan scan = new Scan();
    scan.setBucketIds(ms.getBucketIds());
    scan.setBatchSize(batchSize);
    scan.setReturnKeysFlag(false);
    final String str = conf.get(MonarchUtils.READ_FILTER_ON_LATEST_VERSION);
    if (str != null) {
      scan.setFilterOnLatestVersionOnly(Boolean.getBoolean(str));
    }
    final boolean isOldestFirst = Boolean.getBoolean(conf.get(MonarchUtils.READ_OLDEST_FIRST));
    final int maxVersions = NumberUtils.toInt(conf.get(MonarchUtils.READ_MAX_VERSIONS), 1);
    scan.setMaxVersions(maxVersions, isOldestFirst);

    if (pushDownfilters != null) {
      scan.setFilter(pushDownfilters);
    }

    scan.setBucketToServerMap(ms.getBucketToServerMap());
    Collections.sort(this.readColIds);

    if (! readColIds.isEmpty()) {
      scan.setColumns(readColIds);
    }
    logger.info("Retrieving columns= {}", scan.getColumns());

    this.mResultScanner = this.anyTable.getScanner(scan);
    this.valueIterator = this.mResultScanner.iterator();

    if (logger.isDebugEnabled()) {
      logger.debug("{} - Initialize MonarchRecordReader: batchSize= {}, split= {}", new Date(), batchSize, ms);
    }
  }

  /**
   * Provide the current progress ratio. Indicates how much data has been read.
   * Once the reading is complete it return 1.0 (100%).
   *
   * @return the fraction between 0-1 indicating the amount of data already read
   * @throws IOException
   */
  public float getProgress() throws IOException {
    if (progress >= 1.0f) {
      progress = 1.0f;
    }
    return progress;
  }

  private Iterator<?> versionIterator = Collections.emptyIterator();
  /**
   * Read and provide the next key-value pair. The key is dummy, an empty-byte, as
   * it is not used in further processing. The value contains the next row
   * read from monarch.
   * <p>
   * When no more values/records are to be read this returns false; indicating EOF
   * to the reader (hive query engine).
   *
   * @param key   the key
   * @param value the next row read from monarch
   * @return true if more records are to be read; false otherwise
   * @throws IOException
   */
  public boolean next(Writable key, Writable value) throws IOException {
    long t1 = System.nanoTime();
    if (this.lastNextTime > 0) {
      this.timeOutsideNext += (t1 - this.lastNextTime);
    }

    if (!valueIterator.hasNext() && !versionIterator.hasNext()) {
      this.mResultScanner.close();
      return false;
    }

    Object result;
    List<Cell> cells;
    if (versionIterator.hasNext()) {
      result = versionIterator.next();
    } else {
      result = valueIterator.next();
      if (result instanceof FormatAwareRow || result instanceof VersionedRow) {
        versionIterator = ((Row) result).getAllVersions().values().iterator();
        result = versionIterator.next();
      }
    }
    cells = result instanceof SingleVersionRow
      ? ((SingleVersionRow) result).getCells() : ((Row) result).getCells();

    ((BytesWritable) key).set(EMPTY_BYTE, 0, EMPTY_BYTE.length);
    BytesRefArrayWritable braw = (BytesRefArrayWritable) value;

    /** assign the next key and value.. record the execution time **/
    braw.resetValid(columns.length);
    int i = 0;
    if (readColIds.size() == 0) {
      for (final Cell cell : cells) {
        String columnName = Bytes.toString(cell.getColumnName());
        if (!columnName.equals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME) &&
            !columnName.equals(MTableUtils.KEY_COLUMN_NAME)) {
          int length = cell.getValueLength();
          braw.get(i++).set(cell.getValueArray(), cell.getValueOffset(), length);
          totalBytes += length;
        }
      }
    } else {
      for (final Cell cell : cells) {
        int length = cell.getValueLength();
        braw.get(readColIds.get(i++)).set(cell.getValueArray(), cell.getValueOffset(), length);
        totalBytes += length;
      }
    }
    rowCount++;

    this.timeInNext += (System.nanoTime() - t1);
    this.lastNextTime = System.nanoTime();
    return true;
  }

  public Writable createKey() {
    return new BytesWritable();
  }

  public Writable createValue() {
    return new BytesRefArrayWritable();
  }

  public long getPos() throws IOException {
    return rowCount;
  }

  /**
   * Log the reader statistics..
   *
   * @throws IOException
   */
  public void close() throws IOException {
    // log the record-reader statistics..
    logger.info("{} - MonarchRecordReader completed.  Statistics:", new Date());
    logger.info("Read: batch_size= {}, row_count= {}. Time (ms) from_init= {}",
      batchSize, rowCount, (System.currentTimeMillis() - startTime));

    logger.info(MonarchUtils.getTimeString("TimeInNext", rowCount, timeInNext));
    logger.info(MonarchUtils.getTimeString("TimeOutsideNext", rowCount, timeOutsideNext));
    logger.info(MonarchUtils.getDataRateString("GetBytesPerSec", totalBytes, timeInGetAll));
  }
}
