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
package io.ampool.monarch.table.ftable.internal;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import io.ampool.conf.Constants;
import io.ampool.internal.MPartList;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.InternalRow;
import io.ampool.monarch.table.internal.MKeyBase;
import io.ampool.monarch.table.internal.ServerScanStatus;
import io.ampool.monarch.table.region.ScanContext;
import io.ampool.monarch.table.region.ScanUtils;
import io.ampool.monarch.table.region.map.RowTupleConcurrentSkipListMap;
import io.ampool.orc.OrcUtils;
import io.ampool.store.StoreHandler;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.wal.WALRecord;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.tier.sockets.ChunkedStreamMessage;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.tier.sockets.command.Get70;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

public class FTableScanner implements Iterator {
  protected static final Logger logger = LogService.getLogger();

  private Region region;
  private FTableDescriptor fTableDescriptor;
  private Scan scan;
  private ServerConnection serverConnection;
  private MPartList values;
  private BlockKey lastkey;
  private BlockKey memFirstKey;
  private boolean clientToServer = false;
  private int[] scannedCounts = new int[3];

  // Block key which will be held until next block is started
  private BlockKey currentBlockKey;
  private byte[] currentBlockKeyBytes;

  private static final byte[] EMPTY_BYTES = new byte[0];
  private BlockKey startKey = null;
  private BlockKey stopKey = null;


  /**
   * C'tor used from tests only..
   *
   * @param region the region
   * @param scan the scan object
   * @param servConn the server connection
   * @param fTableDescriptor table descriptor
   */
  public FTableScanner(Region region, Scan scan, ServerConnection servConn,
      FTableDescriptor fTableDescriptor) {
    this.region = region;
    this.scan = scan;
    this.serverConnection = servConn;
    this.clientToServer = true;
    this.fTableDescriptor = fTableDescriptor;
    this.blockEncoding = fTableDescriptor.getEncoding();
    memFirstKey = null;
    init();
  }

  /**
   * Strictly for test purpose
   */
  public FTableScanner() {

  }

  /**
   * Use for server side scanner
   *
   * @param region
   * @param scan
   * @param fTableDescriptor
   */
  public FTableScanner(Region region, Scan scan, FTableDescriptor fTableDescriptor) {
    this(region, scan, null, fTableDescriptor);
    this.clientToServer = false;
  }

  private ScanContext sc;

  /**
   * Simple c'tor.. used from ScanCommand for client scan.
   *
   * @param sc the scan context
   * @param iMap the underlying map
   */
  public FTableScanner(final ScanContext sc, final RowTupleConcurrentSkipListMap iMap) {
    this.sc = sc;
    this.region = sc.getRegion();
    this.serverConnection = sc.getConnection();
    this.fTableDescriptor = (FTableDescriptor) sc.getTableDescriptor();
    this.blockEncoding = fTableDescriptor.getEncoding();
    this.scan = sc.getScan();
    this.memFirstKey = null;
    /* at the moment only for ORC but it could be generic.. */
    this.readerOptions = new OrcUtils.OrcOptions(scan.getFilter(), fTableDescriptor);
    initBlockIterator(iMap);
  }

  @SuppressWarnings("unchecked")
  private void initBlockIterator(RowTupleConcurrentSkipListMap iMap) {
    final byte[] startRow = scan.getStartRow();
    final byte[] stopRow = scan.getStopRow();
    final boolean includeStartRow = scan.getIncludeStartRow();
    final SortedMap rangeMap = ScanUtils.getRangeMap(iMap.getInternalMap(),
        startRow == null ? null : new MKeyBase(startRow),
        stopRow == null ? null : new MKeyBase(stopRow), includeStartRow);
    this.blockIterator = rangeMap.entrySet().iterator();
  }

  private final Iterator<Integer> tierIterator = Arrays.asList(0, 1).iterator();
  private Iterator blockIterator = Collections.emptyIterator();
  private Iterator valueIterator = Collections.emptyIterator();
  private final StoreHandler sh = StoreHandler.getInstance();
  private Encoding blockEncoding;
  private boolean isMemoryIterator = true;
  private ReaderOptions readerOptions = null;

  private void init() {
    try {
      PartitionedRegion tableRegionPR = (PartitionedRegion) region;
      BucketRegion bucket =
          tableRegionPR.getDataStore().getLocalBucketById(this.scan.getBucketId());
      if (bucket != null && bucket.getBucketAdvisor().isHosting()) {
        RowTupleConcurrentSkipListMap internalMap =
            (RowTupleConcurrentSkipListMap) bucket.getRegionMap().getInternalMap();
        if (internalMap == null) {
          return;
        }
        SortedMap realMap = internalMap.getInternalMap();

        byte[] startRow = scan.getStartRow();
        byte[] stopRow = scan.getStopRow();
        boolean includeStartRow = scan.getIncludeStartRow();

        SortedMap<IMKey, RegionEntry> rangeMap =
            ScanUtils.getRangeMap(realMap, startRow == null ? null : new MKeyBase(startRow),
                stopRow == null ? null : new MKeyBase(stopRow), includeStartRow);
        this.blockIterator = rangeMap.entrySet().iterator();
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Bucket does not exist on this server");
        }
        // TODO Handle bucket moved exception Anyway server side scanner does not have any existing
        // handling
        try {
          handleBucketMoved(region, this.scan, serverConnection);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } catch (BucketMovedException exe) {
      // TODO Handle bucket moved exception Anyway server side scanner does not have any existing
      // handling
      logger.error("Bucket moved exception occured", exe);
      try {
        handleBucketMoved(region, scan, serverConnection);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private boolean getBlockValueIterator() {
    while (blockIterator.hasNext()) {
      Object block = blockIterator.next();
      if (!updateValueIterator(block)) {
        continue;
      }
      if (valueIterator.hasNext()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the value from the region-entry. If the value is not present in-memory, fetch it from the
   * disk-region.
   *
   * @param entry the entry to be retrieved
   * @return the actual entry value
   */
  public static Object getValue(final ServerConnection conn, final Region region,
      final Entry<IMKey, RegionEntry> entry) {
    Object data;
    synchronized (entry.getValue()) {
      data = entry.getValue()._getValue();
    }
    if (data == null || Token.isInvalidOrRemoved(data)) {
      Get70 request = (Get70) Get70.getCommand();
      Get70.Entry e = request.getEntry(region, entry.getKey(), null, conn);
      data = e.value instanceof byte[] ? BlockValue.fromBytes((byte[]) e.value) : e.value;
    }
    if (data instanceof VMCachedDeserializable) {
      data = ((VMCachedDeserializable) data).getDeserializedValue(region, entry.getValue());
    }
    return data;
  }

  @SuppressWarnings("unchecked")
  private boolean updateValueIterator(Object block) {
    BlockKey bk;
    Object bv;
    if (block instanceof Map.Entry) {
      Entry e = (Entry) block;
      if (!(e.getKey() instanceof BlockKey) || !(e.getValue() instanceof RegionEntry)) {
        return false;
      }
      bk = (BlockKey) e.getKey();
      currentBlockKey = bk;
      currentBlockKeyBytes = bk.getBytes();
      bv = getValue(serverConnection, region, e);
      if (bv == null || Token.isInvalidOrRemoved(bv)) {
        return false;
      }
      if (memFirstKey == null) {
        memFirstKey = currentBlockKey;
      }
      BlockValue bVal = (BlockValue) bv;
      if (!OrcUtils.isBlockNeeded((OrcUtils.OrcOptions) readerOptions, bVal)) {
        return false;
      }
      this.blockEncoding = Encoding.getEncoding(bVal.getRowHeader().getEncoding());
      scannedCounts[0] += bVal.getCurrentIndex();
    } else if (block instanceof WALRecord) {
      this.isMemoryIterator = false;
      WALRecord wr = (WALRecord) block;
      currentBlockKey = lastkey = wr.getBlockKey();
      currentBlockKeyBytes = lastkey.getBytes();
      if (memFirstKey != null) {
        if (lastkey.compareTo(memFirstKey) > 0) {
          // stop the scan
          logger.info("Possible duplicate started from store scan. Stopping store scan.");
          return false;
        }
      }

      bv = wr.getBlockValue();
      BlockValue bVal = (BlockValue) bv;
      if (!OrcUtils.isBlockNeeded((OrcUtils.OrcOptions) readerOptions, bVal)) {
        return false;
      }
      this.blockEncoding = Encoding.getEncoding(bVal.getRowHeader().getEncoding());
      scannedCounts[1] += bVal.getCurrentIndex();
    } else {
      return false;
    }
    valueIterator = ((BlockValue) bv).iterator(this.readerOptions);
    return true;
  }

  private boolean getTierIterator() {
    int tierIndex = tierIterator.next();
    if (tierIndex == 0) {
      blockIterator = sh.getWALScanner(this.region.getName(), this.scan.getBucketId()).iterator();
      boolean isValueAvailable = getBlockValueIterator();
      if (isValueAvailable) {
        return true;
      }
      tierIndex = tierIterator.next();
    }
    /* else it is tierIndex == 1 */
    this.blockEncoding = fTableDescriptor.getEncoding();
    valueIterator =
        sh.getStoreScanner(region.getName(), scan.getBucketId(), readerOptions).iterator();
    return valueIterator.hasNext();
  }

  private boolean hasNextNew() {
    if (valueIterator.hasNext()) {
      return true;
    } else if (blockIterator.hasNext()) {
      boolean isValueAvailable = getBlockValueIterator();
      if (isValueAvailable) {
        return true;
      }
    }
    final boolean b = tierIterator.hasNext() && getTierIterator();
    /* log the scanned-counts, per tier, at the end of scanner */
    if (!b) {
      logger.debug("Scanned rows for table= {}, bucketId= {}: memory= {}, wal= {}, tier= {}",
          region.getName(), scan.getBucketId(), scannedCounts[0], scannedCounts[1],
          scannedCounts[2]);
    }
    return b;
  }

  private static final class DummyEntry implements Map.Entry<Object, Object> {
    private Object key;
    private Object value;

    public void reset(final Object key, final Object value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public Object getKey() {
      return this.key;
    }

    @Override
    public Object getValue() {
      return this.value;
    }

    @Override
    public Object setValue(final Object value) {
      Object old = this.value;
      this.value = value;
      return old;
    }
  }
  private static final class DummyKey implements IMKey {
    protected static final DummyKey D_KEY = new DummyKey();
    private static final byte[] EMPTY = new byte[0];

    @Override
    public byte[] getBytes() {
      return EMPTY;
    }

    @Override
    public int compareTo(IMKey o) {
      return Bytes.compareTo(getBytes(), o.getBytes());
    }
  }

  /* the dummy entry used to send the required (key-value) */
  private DummyEntry entry = new DummyEntry();

  private Object nextNew() {
    if (valueIterator.hasNext()) {
      Object next = valueIterator.next();
      byte[] bytes = null;
      if (next instanceof StoreRecord) {
        scannedCounts[2]++;
        StoreRecord sr = (StoreRecord) next;
        lastkey = new BlockKey(sr.getTimeStamp());

        /*
         * If during mem scan some records are moved from mem to the next tier then to avoid
         * duplicates stop the scan when we get a key which is greater than or equal to the first
         * key returned by mem scanner for that bucket.
         */
        if (!this.isMemoryIterator && memFirstKey != null) {
          if (lastkey.compareTo(memFirstKey) > 0) {
            // stop the scan
            if (logger.isDebugEnabled()) {
              logger.debug("Possible duplicate started from store scan. Stopping store scan.");
            }
            return null;
          }
        }
        entry.reset(DummyKey.D_KEY, sr);
      } else if (next instanceof OrcUtils.DummyRow) {
        OrcUtils.DummyRow dr = (OrcUtils.DummyRow) next;
        final InternalRow row = sc.getInternalRow();
        row.reset(currentBlockKeyBytes, dr.getBytes(), this.blockEncoding, dr.getOffset(),
            dr.getLength());
        entry.reset(DummyKey.D_KEY, row);
      } else if (next instanceof byte[]) {
        bytes = (byte[]) next;
        final InternalRow row = sc.getInternalRow();
        row.reset(currentBlockKeyBytes, bytes, this.blockEncoding, null);
        entry.reset(DummyKey.D_KEY, row);
      }
      return entry;
    } else {
      return null;
    }
  }

  @Override
  public boolean hasNext() {
    return hasNextNew();
  }

  @Override
  public Object next() {
    return nextNew();
  }

  public void handleBucketMoved(Region region, Scan scan, ServerConnection servConn)
      throws IOException {
    boolean bucketMoveHandled = false;

    MPartList values = new MPartList(1, true);
    ServerScanStatus serverScanStatus = ServerScanStatus.BUCKET_MOVED;

    // Search for the server hosting scan.getBucketId() and send it in exception
    PartitionedRegion tableRegionPR = (PartitionedRegion) region;
    Set<BucketAdvisor.ServerBucketProfile> clientBucketProfiles =
        tableRegionPR.getRegionAdvisor().getClientBucketProfiles(scan.getBucketId());

    if (clientBucketProfiles != null && clientBucketProfiles.size() > 0) {

      // Getting first serverbucketprofile which has been initialized
      BucketAdvisor.ServerBucketProfile targetServerBucketProfile = null;
      for (BucketAdvisor.ServerBucketProfile clientBucketProfile : clientBucketProfiles) {
        if (!clientBucketProfile.isInitializing) {
          targetServerBucketProfile = clientBucketProfile;
        }
      }

      if (targetServerBucketProfile != null
          && targetServerBucketProfile.getBucketServerLocations().size() > 0) {
        Set<? extends ServerLocation> bucketServerLocations =
            targetServerBucketProfile.getBucketServerLocations();

        // Getting only first server location from the serverlocation list
        if (bucketServerLocations != null && bucketServerLocations.size() > 0) {
          ServerLocation location = bucketServerLocations.iterator().next();
          if (location != null) {
            byte[][] serverStatusMarker =
                {serverScanStatus.getStatusBytes(), Bytes.toBytes(location.getHostName()
                    + Constants.General.SERVER_NAME_SEPARATOR + location.getPort())};
            values.addObjectPart(EMPTY_BYTES, serverStatusMarker, true, null);
            bucketMoveHandled = true;
          }
        }
      }
    }
    if (!bucketMoveHandled) {
      // In case bucket is not live anywhere skip that bucket
      byte[][] serverStatusMarker = {serverScanStatus.getStatusBytes(), Bytes.EMPTY_BYTE_ARRAY};
      values.addObjectPart(EMPTY_BYTES, serverStatusMarker, true, null);
    }
    sendSendResponseChunk(region, values, false, servConn);
  }

  /**
   * Send chunk of results to client.
   *
   * @param region
   * @param list
   * @param lastChunk
   * @param servConn
   * @throws IOException
   */
  private void sendSendResponseChunk(Region region, MPartList list, boolean lastChunk,
      ServerConnection servConn) throws IOException {
    long l = System.nanoTime();
    ChunkedStreamMessage chunkedResponseMsg = servConn.getChunkedStreamResponseMessage();
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPartNoCopying(list);
    long t1 = (System.nanoTime() - l);

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending {} mscan response chunk for region={}{}", servConn.getName(),
          (lastChunk ? " last " : " "), region.getFullPath(), (logger.isTraceEnabled()
              ? " values=" + list + " chunk=<" + chunkedResponseMsg + ">" : ""));
    }

    l = System.nanoTime();
    chunkedResponseMsg.sendChunk(servConn);
    // if (statsMap != null) {
    // statsMap.addTo("serverObjPart", t1);
    // statsMap.addTo("serverSendChunk", (System.nanoTime() - l));
    // }
  }

}
