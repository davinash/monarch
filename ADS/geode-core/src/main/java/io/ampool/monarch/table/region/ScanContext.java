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
package io.ampool.monarch.table.region;

import io.ampool.internal.MPartList;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.DeSerializedRow;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.internal.FTableScanner;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.InternalRow;
import io.ampool.monarch.table.internal.MKeyBase;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.ServerScanStatus;
import io.ampool.monarch.table.internal.StorageFormatter;
import io.ampool.monarch.table.internal.ThinRow;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.monarch.types.interfaces.Function3;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.tier.sockets.ChunkedStreamMessage;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;

/**
 * The scan context.. contains the required stuff to process the scan.
 * <p>
 */
public class ScanContext {
  /* inputs */
  public final ServerConnection conn;
  public final Region region;
  public final Scan scan;
  final MTableKey scanKey;
  final boolean returnFullValue;
  public final TableDescriptor td;
  private final ScanCommandSenderThread senderThread;
  private final Object2LongOpenHashMap<String> statsMap;
  int versionOrderIdentifier = -1;
  /* stats */
  long nextLoopTime = 0;
  public long readValueTime = 0;
  long addObjPartTime = 0;
  long sendChunkTime = 0;
  int dataSentCounter = 0;
  long execFilterTime = 0;
  long nextEntryTime = 0;
  public long nextValueTime = 0;

  /* (re)used objects */
  ThinRow row;
  public MPartList values;
  public ScanEntryHandler handler;
  public StorageFormatter storageFormatter;
  private InternalRow[] rows = null;
  boolean isKeyOnlyFilter = false;

  public ScanContext(final ServerConnection conn, final Region region, final Scan scan,
      final TableDescriptor td, final Object2LongOpenHashMap<String> statsMap,
      ScanCommandSenderThread senderThread) throws InterruptedException {
    this.conn = conn;
    this.region = region;
    this.scan = scan;
    this.td = td;
    this.scanKey = new MTableKey();
    this.scanKey.setColumnPositions(scan.getColumns());
    this.senderThread = senderThread;
    this.statsMap = statsMap;
    this.isKeyOnlyFilter = ScanUtils.isKeyOnlyFilter(scan.getFilter());
    this.returnFullValue = (scan.getColumns() == null || scan.getColumns().isEmpty())
        && (td instanceof MTableDescriptor && ((MTableDescriptor) td).getMaxVersions() == 1);
    if (scan.isOldestFirst()) {
      versionOrderIdentifier = 1;
    }
    int responseSize = scan.getMessageChunkSize() + 1;
    /* TODO: _this_ should not escape from c'tor.. need to refactor */
    if (senderThread != null) {
      for (int i = 0; i < ScanCommand.BUFFER_COUNT; i++) {
        final MPartList pl = new MPartList(responseSize, scan.getReturnKeysFlag(), this);
        senderThread.getReturnQueue().put(pl);
      }
      this.values = senderThread.getReturnQueue().take();
    } else {
      this.values = new MPartList(responseSize, scan.getReturnKeysFlag(), this);
    }
    createStorageFormatter();
    initHandler();
    Function3<byte[], Object, Encoding, InternalRow> rp = ScanUtils.serverRowProducer(scan, td);
    this.rows = new InternalRow[scan.getMessageChunkSize()];
    for (int i = 0; i < scan.getMessageChunkSize(); i++) {
      rows[i] = rp.apply(null, null, td.getEncoding());
    }
  }

  public InternalRow getInternalRow() {
    return this.rows[dataSentCounter % scan.getMessageChunkSize()];
  }

  private void createStorageFormatter() {
    if (this.td instanceof MTableDescriptor) {
      this.storageFormatter = MTableUtils.getStorageFormatter(this.td);
    } else {
      this.storageFormatter = null;
    }
  }

  public StorageFormatter getStorageFormatter() {
    return storageFormatter;
  }

  public TableDescriptor getTableDescriptor() {
    return this.td;
  }

  public Scan getScan() {
    return this.scan;
  }

  public ScanEntryHandler getHandler() {
    return this.handler;
  }

  private void initHandler() {
    Filter f = scan.getFilter();
    /*
     * do not process rows if the complete row-value is to be returned; no filters are specified OR
     * more versions are requested than max number of versions
     *
     * use ThinRow class to reuse the row object for filter execution: only single version is
     * requested OR only non-intrusive filters are used like:
     * RowFilter/TimestampFilter/SingleColumnValueFilter
     *
     * In all the remaining cases the VersionedRow is used for processing.
     */
    if (f == null && ScanUtils.isSingleVersion(td, scan)) {
      this.handler = ScanEntryHandler.NoFilterHandler.SELF;
    } else if (ScanUtils.isSingleVersion(td, scan) && ScanUtils.isSimpleFilter(f)) {
      this.handler = ScanEntryHandler.SimpleFilterHandler.SELF;
    } /*
       * else if (td instanceof MTableDescriptor && ((MTableDescriptor) td).getMaxVersions() <=
       * scan.getMaxVersions() && versionOrderIdentifier == 1) {
       * System.out.println("ScanContext.initHandler :: 1246 " + "NoFilterHandler"); this.handler =
       * NoFilterHandler.SELF; }
       */ else {
      this.handler = ScanEntryHandler.MultiVersionHandler.SELF;
    }
  }

  ScanEntryHandler getEntryHandler() {
    return this.handler;
  }

  @SuppressWarnings("unchecked")
  public Iterator<Map.Entry> getBucketIterator(final Object iMap) {
    Iterator<Map.Entry> bucketIterator = Collections.emptyIterator();
    if (this.td instanceof FTableDescriptor) {
      bucketIterator = new FTableScanner(this, (RowTupleConcurrentSkipListMap) iMap);
    } else if (iMap instanceof RowTupleConcurrentSkipListMap) {
      byte[] startRow = scan.getStartRow();
      byte[] stopRow = scan.getStopRow();
      boolean includeStartRow = scan.getIncludeStartRow();
      ((RowTupleConcurrentSkipListMap) iMap).getInternalMap();
      SortedMap rangeMap =
          ScanUtils.getRangeMap(((RowTupleConcurrentSkipListMap) iMap).getInternalMap(),
              startRow == null ? null : new MKeyBase(startRow),
              stopRow == null ? null : new MKeyBase(stopRow), includeStartRow);
      if (scan.isReversed()) {
        if (rangeMap instanceof NavigableMap) {
          // for concurrentSkipListMap
          bucketIterator = ((NavigableMap) rangeMap).descendingMap().entrySet().iterator();
        } else {
          // Either not supported or supported in crude way
          // Set avlMapEntrySet = rangeMap.entrySet();
          // Object[] objects = avlMapEntrySet.toArray();
          // LinkedHashSet reverseEntrySet = new LinkedHashSet();
          // for (int i = objects.length - 1; i >= 0; i--) {
          // reverseEntrySet.add(objects[i]);
          // }
          // bucketIterator = reverseEntrySet.iterator();
          throw new IllegalStateException("Reverse ordering is not applicable with AVL tree map");
        }
      } else {
        bucketIterator = rangeMap.entrySet().iterator();
      }

    } else if (iMap instanceof Map) {
      bucketIterator = iMap instanceof CustomEntryConcurrentHashMap
          ? ((CustomEntryConcurrentHashMap) iMap).entrySet().iterator()
          : ((Map) iMap).entrySet().iterator();
      final byte[] startRowKey = scan.getStartRow();
      /* fast-forward, if required, to the last-key provided in previous batch **/
      if (startRowKey != null && startRowKey.length > 0) {
        byte[] keyBytes;
        while (bucketIterator.hasNext()) {
          keyBytes = IMKey.getBytes(bucketIterator.next().getKey());
          if (Bytes.equals(keyBytes, startRowKey)) {
            break;
          }
        }
      }
    }
    return bucketIterator;
  }

  public void addEntry(final Object key, final Object value)
      throws IOException, InterruptedException {
    if (value instanceof byte[]) {
      this.values.addPart(key, isKeyOnlyFilter ? ScanCommand.EMPTY_BYTES : value, MPartList.BYTES,
          null);
    } else {
      this.values.addPart(key, value, MPartList.OBJECT, null);
    }
    this.dataSentCounter++;
    if (this.values.size() == scan.getMessageChunkSize()) {
      this.send();
    }
  }

  public void addRow(final Row row) throws IOException, InterruptedException {
    this.values.addPart(row.getRowId(), row,
        row instanceof DeSerializedRow ? MPartList.DES_ROW : MPartList.ROW, null);
    this.dataSentCounter++;
    if (this.values.size() == scan.getMessageChunkSize()) {
      this.send();
    }
  }

  /**
   * Drain the recently added row to the underlying list of values/rows.
   *
   * @return an array with: type, key, and value
   */
  public Object[] drainRow() {
    this.dataSentCounter--;
    return this.values.drainValue();
  }

  boolean shouldStop() {
    return (scan.batchModeEnabled() && dataSentCounter == scan.getBatchSize())
        || (scan.getMaxResultLimit() > 0 && dataSentCounter == scan.getMaxResultLimit());
  }

  private void send() throws InterruptedException, IOException {
    /*
     * send the internal buffer asynchronously whereas get the next available one so that processing
     * is not blocked for sender.
     */
    if (this.senderThread != null) {
      this.senderThread.getValuesQueue().put(this.values);
      this.values = this.senderThread.getReturnQueue().take();
    } else {
      sendResponseChunk();
      this.values.clear();
    }
  }

  private void sendResponseChunk() throws IOException {
    long l = System.nanoTime();
    ChunkedStreamMessage chunkedResponseMsg = conn.getChunkedStreamResponseMessage();
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(false);
    chunkedResponseMsg.addObjPartNoCopying(this.values);
    addObjPartTime += (System.nanoTime() - l);
    l = System.nanoTime();
    chunkedResponseMsg.sendChunk(conn);
    sendChunkTime += (System.nanoTime() - l);
  }

  private void finalSend() throws InterruptedException, IOException {
    if (this.senderThread != null) {
      /* make sure that sender thread exits.. and all chunks have been sent. */
      this.senderThread.getValuesQueue().put(this.values);
      Thread.yield();
      this.senderThread.getValuesQueue().put(ScanCommand.END_MARKER);
      this.senderThread.await();
    } else {
      this.sendResponseChunk();
      this.values.clear();
    }
    if (statsMap != null) {
      statsMap.addTo("serverNextLoop", this.nextLoopTime);
      statsMap.addTo("serverReadValue", this.readValueTime);
      statsMap.addTo("serverObjPart", this.addObjPartTime);
      statsMap.addTo("serverSendChunk", this.sendChunkTime);
      statsMap.addTo("serverExecFilter", this.execFilterTime);
      statsMap.addTo("serverNextEntryTime", this.nextEntryTime);
      statsMap.addTo("serverNextValueTime", this.nextValueTime);
    }
  }

  void sendEndMarker(final byte[] lastKeyBytes, final ScanEntryHandler.Status status,
      final boolean hasNext) throws IOException, InterruptedException {
    ServerScanStatus sss;
    byte[] lastKey = lastKeyBytes;
    if (this.dataSentCounter == this.scan.getMaxResultLimit()) {
      sss = ServerScanStatus.SCAN_COMPLETED;
      lastKey = ScanCommand.EMPTY_BYTES;
    } else if (status == ScanEntryHandler.Status.STOP) {
      lastKey = ScanCommand.EMPTY_BYTES;
      sss = ServerScanStatus.SCAN_COMPLETED;
    } else if (this.scan.getStopRow() != null
        && Bytes.compareTo(lastKey, this.scan.getStopRow()) == 0) {
      lastKey = ScanCommand.EMPTY_BYTES;
      sss = ServerScanStatus.SCAN_COMPLETED;
    } else if (hasNext) {
      sss = ServerScanStatus.BATCH_COMPLETED;
    } else {
      lastKey = ScanCommand.EMPTY_BYTES;
      sss = ServerScanStatus.BUCKET_ENDED;
    }
    byte[][] marker = {sss.getStatusBytes(), lastKey};
    this.values.addObjectPart(ScanCommand.EMPTY_BYTES, marker, true, null);

    this.finalSend();
  }

  /**
   * Get the region that is being processed.
   * 
   * @return the region
   */
  public Region getRegion() {
    return this.region;
  }

  public ServerConnection getConnection() {
    return conn;
  }

  public MTableKey getScanKey() {
    return scanKey;
  }
}
