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
package io.ampool.monarch.table.internal;

import static io.ampool.internal.MPartList.DES_ROW;
import static io.ampool.internal.MPartList.ROW;
import static io.ampool.monarch.table.region.ScanCommand.readValueFromMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.DeSerializedRow;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTable;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.ProxyFTableRegion;
import io.ampool.monarch.table.ftable.internal.FTableScanner;
import io.ampool.monarch.table.region.AmpoolTableRegionAttributes;
import io.ampool.monarch.table.region.ScanContext;
import io.ampool.monarch.table.region.ScanEntryHandler;
import io.ampool.monarch.table.region.ScanUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

/**
 * Class that implement Scanner on the server side This scanner will run only on the local machine
 * on which this is invokes If the data does not fit in the buckets found on the local node then
 * this will not return any information. Scanner will work only on the buckets which are primary and
 * not on the secondary. It gets data from the buckets specified via {@link Scan#setBucketId(int)}
 * or {@link Scan#setBucketIds(Set)}, if none is specified, it will get data from all local primary
 * buckets of the server on which it is run.
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class ScannerServerImpl extends Scanner {

  private Scan scan;
  private final InternalTable table;

  private static final Logger logger = LogService.getLogger();

  /* this is the current iterator of the any of the map */
  private Iterator currentIterator = new ArrayList().iterator();

  /* this is the current iterator of the any of the map */
  private Iterator<Integer> bucketIdIterator;

  private PartitionedRegionDataStore prDataStore = null;

  private long recordsConsumed = 0;

  private boolean isFtable = false;

  private ScanContext scanContext = null;
  private TableDescriptor td;

  public ScannerServerImpl(Scan scan, InternalTable table) {
    if (scan == null) {
      throw new IllegalArgumentException("Scan Object cannot be null");
    }
    this.scan = new Scan(scan);
    this.scan.setMessageChunkSize(2);
    this.table = table;

    if (table instanceof FTable) {
      isFtable = true;
    }
    // this.storageFormatter = MTableUtils.getStorageFormatter(this.table.getTableDescriptor());
    this.td = table.getTableDescriptor();
    this.scan = MTableUtils.setColumnNameOrIds(this.scan, td);
    verifyStartAndStopKey();
    initScan();
  }

  private void verifyStartAndStopKey() {
    if (this.scan.getStartRow() != null && this.scan.getStopRow() != null) {
      if (Bytes.compareTo(this.scan.getStartRow(), this.scan.getStopRow()) > 0) {
        throw new IllegalArgumentException("Start key is smaller than Stop Key");
      }
    }
  }

  private BiFunction<byte[], Object, Row> rowProducer = null;
  private final HeapDataOutputStream hdos = new HeapDataOutputStream(1024, null, true);

  private void initScan() {
    /* Get the associated Region with this table */
    Region tableRegion = null;
    if (isFtable) {
      tableRegion = ((ProxyFTableRegion) table).getTableRegion();
    } else {
      tableRegion = ((ProxyMTableRegion) table).getTableRegion();
    }
    this.rowProducer = ScanUtils.getRowProducer(scan, td);
    if (tableRegion == null) {
      /* this should never happen */
      throw new IllegalStateException("Scan could not get the handle of internal Handle");
    }
    /*
     * Cast this region to PartitionedRegion, As all our regions associated with the table is
     * Partitioned Region.
     */
    prDataStore = ((PartitionedRegion) tableRegion).getDataStore();

    prDataStore.getCachePerfStats().incScans();
    ((PartitionedRegion) tableRegion).getPrStats().incScanCount(1);

    TreeSet<Integer> bucketIdSet;
    int bucketId = scan.getBucketId();
    if (bucketId != -1) {
      bucketIdSet = new TreeSet<>();
      bucketIdSet.add(bucketId);
    } else {
      bucketIdSet = new TreeSet<>((scan.getBucketIds().size() > 0) ? scan.getBucketIds()
          : getApplicableLocalBucketIds(scan, td, prDataStore));
    }
    if (this.scan.isReversed()) {
      this.bucketIdIterator = bucketIdSet.descendingIterator();
    } else {
      this.bucketIdIterator = bucketIdSet.iterator();
    }
    try {
      this.scanContext = new ScanContext(null, tableRegion, this.scan, td, null, null);
    } catch (InterruptedException e) {
      logger.error("Failed to create scanContext " + e.getMessage());
    }
  }

  private Set<Integer> getApplicableLocalBucketIds(Scan scan, TableDescriptor tableDescriptor,
      PartitionedRegionDataStore prDataStore) {
    Set<Integer> bucketIdSet =
        MTableUtils.getBucketIdSet(scan.getStartRow(), scan.getStopRow(), tableDescriptor);
    Set<Integer> allLocalPrimaryBucketIds = prDataStore.getAllLocalPrimaryBucketIds();

    bucketIdSet.retainAll(allLocalPrimaryBucketIds);
    return bucketIdSet;
  }

  private void updateIterator(int bucketId) {
    /*
     * Filter over the buckets to get only primary buckets and store them into sorted Map Since we
     * would like to read using sorted information. If the bucket is empty then also we are ignoring
     * that bucket.
     */
    BucketRegion bucket = prDataStore.getLocalBucketById(bucketId);
    // TODO make it generic
    this.scan.setBucketId(bucketId);
    BucketRegion localBucketById = prDataStore.getLocalBucketById(bucketId);
    if (localBucketById != null && localBucketById.getBucketAdvisor().isPrimary()
        && this.scanContext != null) {
      this.currentIterator =
          this.scanContext.getBucketIterator(bucket.getRegionMap().getInternalMap());
    }
  }

  private Row scanNew(ScanContext scanContext, final Iterator<Map.Entry> currentIterator) {
    try {
      ScanEntryHandler.Status status;
      Map.Entry entry;
      Function<Map.Entry, Object> VALUE_PROCESSOR = scanContext.td instanceof FTableDescriptor
          ? Map.Entry::getValue : (e) -> readValueFromMap(e, scanContext);
      while (currentIterator.hasNext()) {
        entry = currentIterator.next();
        if (entry == null || !(entry.getKey() instanceof IMKey)) {
          continue;
        }

        status = scanContext.handler.handle((Row) VALUE_PROCESSOR.apply(entry), scanContext);
        if (status == ScanEntryHandler.Status.STOP) {
          return null;
        } else if (status == ScanEntryHandler.Status.SEND) {
          Object[] values = scanContext.drainRow();
          hdos.reset();
          Object value;
          switch ((byte) values[0]) {
            case ROW:
              ((InternalRow) values[2]).writeSelectedColumns(hdos, this.scan.getColumns());
              value = Arrays.copyOfRange(hdos.toByteArray(), 2, hdos.size());
              break;
            case DES_ROW:
              td.getEncoding().writeDesRow(hdos, td, (DeSerializedRow) values[2],
                  this.scan.getColumns());
              value = Arrays.copyOfRange(hdos.toByteArray(), 2, hdos.size());
              break;
            default:
              value = values[2];
          }
          return this.rowProducer.apply((byte[]) values[1], value);
        }
      }
    } catch (Exception e) {
      logger.warn("Exception during scan; table= {}.", scanContext.region.getName(), e);
    }
    return null;
  }

  @Override
  public Row next() {
    if (recordsConsumed == scan.getMaxResultLimit()) {
      close();
      return null;
    }
    if (!this.currentIterator.hasNext() && bucketIdIterator.hasNext()) {
      updateIterator(bucketIdIterator.next());
    }

    if (this.currentIterator == null || !this.currentIterator.hasNext()) {
      return null;
    }

    final Row row = scanNew(this.scanContext, this.currentIterator);
    recordsConsumed++;
    return row;
  }

  @Override
  public Row[] next(int nbRows) {
    Row[] results = new Row[nbRows];
    for (int i = 0; i < nbRows; i++) {
      results[i] = next();
    }
    return results;
  }

  @Override
  public void close() {
    this.currentIterator = null;
  }

  @Override
  public Iterator<Row> iterator() {
    return new Iterator<Row>() {
      // The next RowResult, possibly pre-read
      Row next = null;

      @Override
      public boolean hasNext() {
        if (next == null) {
          next = ScannerServerImpl.this.next();
          return next != null;
        }
        return true;
      }

      @Override
      public Row next() {
        if (!hasNext()) {
          return null;
        }
        Row temp = next;
        next = null;
        return temp;
      }
    };
  }

  @Override
  public Row[] nextBatch() {
    throw new UnsupportedOperationException("Batching is not applicable to this scanner");
  }

}
