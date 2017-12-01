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

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.Put;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.Record;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Monarch record writer for Hive.
 *   This writes data from Hive into Monarch.
 *
 * Created on: 2015-11-02
 * Since version: 0.2.0
 */
public class MonarchRecordWriter implements RecordWriter {
  public static final String MAPREDUCE_TASK_PARTITION = "mapreduce.task.partition";
  public static final String MAPREDUCE_TASK_ID = "mapreduce.task.id";
  public static final String MONARCH_UNIQUE_VAR = "monarch.unique.var";
  private int posInBlock = 0;
  private long recordCounter = 0;
  private long totalBytes = 0;
  private long blockCounter = 0;
  private String keyPrefix;
  private static final Logger logger = LoggerFactory.getLogger(MonarchRecordWriter.class);

  /** writer statistics **/
  private long timeInWrite = 0;
  private long timeInPut = 0;
  private long startTime = 0;
  private MTable metaTable;
  private MTable dataTable = null;
  private FTable fTable = null;
  private byte[][] columns;
  private int batchSize = MonarchUtils.MONARCH_BATCH_SIZE_DEFAULT;
  private List<Put> putList;
  private Record[] records = null;
  private int keyColumnIdx = -1;

  /* interface that decides which key should be used for put */
  private interface KeyProducer {
    byte[] get(String a, long b, BytesRefArrayWritable c, int d) throws IOException;
  }

  private static final KeyProducer PREFIX_KEY = (a, b, c, d) -> (a + b).getBytes("UTF-8");
  private static final KeyProducer COLUMN_KEY = (a, b, c, d) -> c.get(d).getBytesCopy();
  private KeyProducer keyProducer;

  public MonarchRecordWriter(Configuration conf) {
    logger.debug("{} - Initializing MonarchRecordWriter", new Date());
    this.startTime = System.currentTimeMillis();

    this.batchSize = NumberUtils.toInt(conf.get(MonarchUtils.MONARCH_BATCH_SIZE), MonarchUtils.MONARCH_BATCH_SIZE_DEFAULT);
    logger.debug("keyPrefix= {}, batchSize= {}", keyPrefix, batchSize);
    /** create and cache the Put (row) objects.. reuse these for puts..**/
    this.putList = IntStream.range(0, batchSize)
      .mapToObj(e -> new Put(new byte[]{0})).collect(Collectors.toList());

    /** convert column names from String to byte[] only once and reuse during put **/
    final String columnStr = conf.get("columns");
    this.columns = columnStr != null
      ? Arrays.stream(columnStr.split(",")).map(String::getBytes).toArray(byte[][]::new) : null;
    String[] cols = columnStr == null ? null : columnStr.split(",");
    boolean fTable = MonarchUtils.isFTable(conf);
    if (fTable) {
      this.fTable = MonarchUtils.getFTableInstance(conf, cols);
      this.records = IntStream.range(0, batchSize)
        .mapToObj(e -> new Record()).collect(Collectors.toList()).toArray(new Record[batchSize]);
    } else {
      this.dataTable = MonarchUtils.getTableInstance(conf, cols);
      this.metaTable = MonarchUtils.getMetaTableInstance(this.dataTable);

      /** get the unique-id from the mapper task, when executing from MR **/
      this.keyPrefix = getKeyPrefixFromId(conf.get(MAPREDUCE_TASK_ID));
      if ("".equals(keyPrefix)) {
        String s = conf.get(MAPREDUCE_TASK_PARTITION);
        logger.debug("Using monarch.unique.var= {}, value= {}", MAPREDUCE_TASK_PARTITION, s);
        if (s != null) {
          keyPrefix = s + "-";
        } else {
          final String uniqueKey = conf.get(MONARCH_UNIQUE_VAR);
          s = uniqueKey != null ? conf.get(uniqueKey) : null;
          logger.debug("Using monarch.unique.var= {}, value= {}", uniqueKey, s);
          if (s != null) {
            keyPrefix = s + "-";
          } else {
            /** in case nothing is found get object-id as unique-prefix **/
            keyPrefix = System.identityHashCode(this) + "-";
          }
        }
      }

      /** initialize the block-counter if we are inserting data in a table via separate queries **/
      Row result = this.metaTable.get(new Get(keyPrefix + MonarchUtils.KEY_BLOCKS_SFX));
      if (result.isEmpty() || result.getCells().get(0) == null) {
        this.blockCounter = 0;
        this.recordCounter = 0;
      } else {
        try {
          this.blockCounter = Long.valueOf(result.getCells().get(0).getColumnValue().toString());
          this.recordCounter = Long.valueOf(result.getCells().get(0).getColumnValue().toString());
        } catch (NumberFormatException e) {
          logger.debug("Failed to retrieve existing blockCounter.. initializing it to 0.");
          this.blockCounter = 0;
          this.recordCounter = 0;
        }
      }
    }
    this.keyColumnIdx = conf.getInt(MonarchUtils.WRITE_KEY_COLUMN, -1);
    this.keyProducer = this.keyColumnIdx < 0 ? PREFIX_KEY : COLUMN_KEY;
  }

  /**
   * Get the key-prefix for the monarch writer.
   *   ** Only for tests.. **
   *
   * @return the key prefix to be used for the monarch writer
   */
  protected String getKeyPrefix() {
    return this.keyPrefix;
  }
  /**
   * Return the substring after last underscore (_).
   *   Mostly for getting unique identifier from MR task id like ...some_id_0001
   *
   * @param str the string
   * @return the substring after last _; empty-string ("") otherwise
   */
  protected static String getKeyPrefixFromId(final String str) {
    final String keyPrefix;
    if (str == null || str.isEmpty() || str.indexOf('_') < 0) {
      keyPrefix = "";
    } else {
      keyPrefix = str.substring(str.lastIndexOf('_') + 1) + '-';
    }
    return keyPrefix;
  }

  public void write(Writable writable) throws IOException {
    /** TODO: better blockCounter than incremental counter?? **/
    long t1 = System.nanoTime();
    if (dataTable != null) {
      BytesRefArrayWritable braw = (BytesRefArrayWritable)writable;
      Put row = this.putList.get(this.posInBlock++);
      row.setRowKey(this.keyProducer.get(keyPrefix, recordCounter, braw, keyColumnIdx));
      row.clear();
      for (int i = 0; i < braw.size(); ++i) {
        row.addColumn(this.columns[i], braw.get(i).getBytesCopy());
        totalBytes += braw.get(i).getLength();
      }
      /** put multiple (of batchSize) rows in a single put operation **/
      if (this.posInBlock == this.batchSize) {
        long t2 = System.nanoTime();
        this.dataTable.put(this.putList);
        this.timeInPut += (System.nanoTime() - t2);
        this.posInBlock = 0;
        blockCounter++;
        logger.debug("Flushing cache. recordCounter= {}, blockCounter= {}, totalBytes= {}",
          recordCounter, blockCounter, totalBytes);
      }
    } else if (fTable != null) {
      BytesRefArrayWritable braw = (BytesRefArrayWritable)writable;
      Record record = this.records[this.posInBlock++];
      for (int i = 0; i < braw.size(); ++i) {
        record.add(this.columns[i], braw.get(i).getBytesCopy());
        totalBytes += braw.get(i).getLength();
      }
      if (this.posInBlock == this.batchSize) {
        long t2 = System.nanoTime();
        this.fTable.append(records);

        this.timeInPut += (System.nanoTime() - t2);
        this.posInBlock = 0;
        blockCounter++;
        logger.debug("Flushing cache. recordCounter= {}, blockCounter= {}, totalBytes= {}",
          recordCounter, blockCounter, totalBytes);
      }
    }
    else {
      throw new RuntimeException("Expected instance of BytesWritable.");
    }
    recordCounter++;
    this.timeInWrite += (System.nanoTime() - t1);
  }

  public void close(boolean b) throws IOException {

    /** put remaining data to MTable **/
    if (this.dataTable != null) {
      if (this.posInBlock > 0) {
        this.dataTable.put(this.putList.subList(0, posInBlock));
      }

      Put row = new Put(keyPrefix + MonarchUtils.KEY_BLOCKS_SFX);
      row.addColumn(MonarchUtils.META_TABLE_COL_LIST.get(0), String.valueOf(recordCounter));
      MonarchUtils.putInTable(metaTable, row);

      /** log the record-writer statistics **/
      if (logger.isDebugEnabled()) {
        logger.debug("{} - MonarchRecordWriter completed.  Statistics:", new Date());
        logger.debug("Wrote: keyPrefix={}, blockCounter= {}, recordCounter= {}. Time (ms) from: init= {}",
          keyPrefix, blockCounter, recordCounter, (System.currentTimeMillis() - startTime));
        logger.debug(MonarchUtils.getTimeString("TimeInWrite", recordCounter, timeInWrite));
        logger.debug(MonarchUtils.getTimeString("TimeInPut", blockCounter, timeInPut));
        logger.debug(MonarchUtils.getDataRateString("PutBytesPerSec", totalBytes, timeInPut));
      }

    } else if (this.fTable != null) {
      if (this.posInBlock > 0) {
        this.fTable.append(Arrays.copyOfRange(this.records, 0, posInBlock));
      }
      /** log the record-writer statistics **/
      if (logger.isDebugEnabled()) {
        logger.debug("{} - MonarchRecordWriter completed.  Statistics:", new Date());
        logger.debug("Wrote: keyPrefix={}, blockCounter= {}, recordCounter= {}. Time (ms) from: init= {}",
          keyPrefix, blockCounter, recordCounter, (System.currentTimeMillis() - startTime));
        logger.debug(MonarchUtils.getTimeString("TimeInWrite", recordCounter, timeInWrite));
        logger.debug(MonarchUtils.getTimeString("TimeInPut", blockCounter, timeInPut));
        logger.debug(MonarchUtils.getDataRateString("PutBytesPerSec", totalBytes, timeInPut));
      }
    }
  }
}
