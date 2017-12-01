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

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.conf.Constants;
import io.ampool.internal.MPartList;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.ScanProfiler;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.EncodingA;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.InternalRow;
import io.ampool.monarch.table.internal.MTableKey;
import io.ampool.monarch.table.internal.RowHeader;
import io.ampool.monarch.table.internal.ServerScanStatus;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.cache.operations.internal.GetOperationContextImpl;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.ChunkedStreamMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.tier.sockets.command.Get70;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.security.NotAuthorizedException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

/**
 * Server side command handling for {@link ScanOp}. It supports scans on both ordered and unordered
 * MTable. Scan gets all the keys within the range as specified by {@link Scan} and apply filters to
 * the records (if applied) and returns records to client. Results are send in batches in batching
 * mode{@link Scan#batchModeEnabled()} else it streams data to client.
 * <p>
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class ScanCommand extends BaseCommand {
  private final static ScanCommand singleton = new ScanCommand();
  private Object2LongOpenHashMap<String> statsMap = null;
  static final int BUFFER_COUNT = 2;
  static final byte[] END_MARKER = new byte[0];
  private static final boolean USE_SENDER_THREAD = Boolean.getBoolean("ampool.scan.sender.thread");

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException, InterruptedException {


    Part scanPart = null;
    String regionName = null;
    Scan scan = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    int partIdx = 0;
    long l = System.nanoTime();

    // Retrieve the region name from the message parts
    regionName = msg.getPart(partIdx++).getString();

    this.securityService.authorizeRegionRead(regionName);

    // Retrieve the MScan object from the message parts
    scanPart = msg.getPart(partIdx++);

    try {
      scan = (Scan) scanPart.getObject();
      if (scan instanceof ScanProfiler) {
        statsMap = new Object2LongOpenHashMap<>(5);
      }
    } catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    if (logger.isDebugEnabled()) {
      StringBuffer buffer = new StringBuffer();
      buffer.append(servConn.getName()).append(": Received mscan request (")
          .append(msg.getPayloadLength()).append(" bytes) from ").append(servConn.getSocketString())
          .append(" for region ").append(regionName).append(" keys ");
      logger.debug(buffer.toString());
    }

    // Process the mscan request
    if (regionName == null) {
      String message = null;
      message = LocalizedStrings.MScan_THE_INPUT_REGION_NAME_FOR_THE_MSCAN_REQUEST_IS_NULL
          .toLocalizedString();
      logger.warn("{}: {}", servConn.getName(), message);
      writeChunkedErrorResponse(msg, MessageType.SCAN_ERROR, message, servConn);
      servConn.setAsTrue(RESPONDED);
    } else {
      LocalRegion region = (LocalRegion) crHelper.getRegion(regionName);
      if (region == null) {
        String reason = " was not found during mscan request";
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
      } else {
        // Send header
        ChunkedStreamMessage chunkedResponseMsg = servConn.getChunkedStreamResponseMessage();
        chunkedResponseMsg.setMessageType(MessageType.RESPONSE);
        chunkedResponseMsg.setTransactionId(msg.getTransactionId());
        chunkedResponseMsg.sendHeader();
        // TODO: Need to enable this while adding tier Stats
        region.getCachePerfStats().incScans();

        // Send chunk response
        try {
          TableDescriptor tableDescriptor =
              MCacheFactory.getAnyInstance().getTableDescriptor(region.getName());
          fillAndSendScanResponseChunks(region, scan, servConn, tableDescriptor);
          MPartList pl = new MPartList(1, false);
          if (statsMap != null) {
            statsMap.addTo("serverCmdExecute", (System.nanoTime() - l));
            pl.addObjectPart(null, statsMap, true, null);
          }
          sendSendResponseChunk(region, pl, true, servConn);
          servConn.setAsTrue(RESPONDED);
        } catch (Exception e) {
          e.printStackTrace();
          // If an interrupted exception is thrown , rethrow it
          checkForInterrupt(servConn, e);

          // Otherwise, write an exception message and continue
          writeChunkedException(msg, e, false, servConn);
          servConn.setAsTrue(RESPONDED);
        } finally {
        }
      }
    }
  }


  private void fillAndSendScanResponseChunks(Region region, Scan scan, ServerConnection servConn,
      TableDescriptor tableDescriptor) throws IOException {

    if (scan.getMessageChunkSize() <= 0) {
      scan.setMessageChunkSize(maximumChunkSize);
    } else {
      // else caller is over-riding default
      if (logger.isDebugEnabled()) {
        logger.info("using chunk size " + scan.getMessageChunkSize() + " in scan");
      }
    }

    long l = System.nanoTime();
    /*
     * Initially set return keys to true; individual scan functions will set it to false if caller
     * requested or if it the default for that function.
     */
    ScanCommandSenderThread senderThread = null;
    try {
      PartitionedRegion tableRegionPR = (PartitionedRegion) region;
      BucketRegion bucket = tableRegionPR.getDataStore().getLocalBucketById(scan.getBucketId());
      if (bucket != null && bucket.getBucketAdvisor().isHosting()) {
        /* TODO: need to replace with generic thread-pool to avoid a thread per request */
        if (USE_SENDER_THREAD) {
          senderThread = new ScanCommandSenderThread();
          BlockingQueue<Object> valuesQueue = new ArrayBlockingQueue<>(1);
          BlockingQueue<MPartList> returnQueue = new ArrayBlockingQueue<>(BUFFER_COUNT);
          senderThread.setDetails(servConn, valuesQueue, returnQueue, statsMap);
          senderThread.setName("ScanResultSender_" + scan.getBucketId());
          senderThread.start();
        }
        final ScanContext sc =
            new ScanContext(servConn, region, scan, tableDescriptor, statsMap, senderThread);
        Iterator<Map.Entry> iterator = sc.getBucketIterator(bucket.getRegionMap().getInternalMap());
        if (iterator.hasNext()) {
          scanNew(sc, iterator);
        }
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Bucket does not exist on this server");
        }
        handleBucketMoved(region, scan, servConn);
      }
    } catch (BucketMovedException exe) {
      handleBucketMoved(region, scan, servConn);
    } catch (InterruptedException e) {
      logger.error("Unable to populate MPartList.");
    } finally {
      // values.release();
      if (statsMap != null) {
        statsMap.addTo("serverCmdScan", (System.nanoTime() - l));
      }
      if (senderThread != null && senderThread.isAlive()) {
        senderThread.interrupt();
      }
    }
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

  static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * Iterate on the entries from the bucket map and process and send, if required, to the client
   * after executing the provided filters.
   * 
   * @param sc the scan context
   * @param iterator the iterator for the underlying bucket map
   */
  @SuppressWarnings("unchecked")
  private void scanNew(final ScanContext sc, final Iterator<Map.Entry> iterator) {
    try {
      byte[] lastKeyBytes = null;
      ScanEntryHandler.Status status = ScanEntryHandler.Status.UNKNOWN;
      Map.Entry entry;
      long l = System.nanoTime();
      // long t1 = 0;
      Function<Map.Entry, Object> VALUE_PROCESSOR =
          sc.td instanceof FTableDescriptor ? Entry::getValue : (e) -> readValueFromMap(e, sc);
      while (iterator.hasNext()) {
        if (sc.shouldStop()) {
          break;
        }

        // l1 = System.nanoTime();
        entry = iterator.next();
        if (entry == null || !(entry.getKey() instanceof IMKey)) {
          continue;
        }
        // t1 += (System.nanoTime() - l1);
        lastKeyBytes = ((IMKey) entry.getKey()).getBytes();
        status = sc.handler.handle((Row) VALUE_PROCESSOR.apply(entry), sc);
        if (status == ScanEntryHandler.Status.STOP) {
          break;
        }
      }
      sc.nextLoopTime = (System.nanoTime() - l);
      // sc.nextEntryTime = t1;
      sc.sendEndMarker(lastKeyBytes, status, iterator.hasNext());
    } catch (Exception e) {
      logger.warn("Exception during scan; table= {}.", sc.region.getName(), e);
    } finally {
      sc.values.release();
    }
  }

  /**
   * Read the value from the underlying map or from overflow files, if required.
   * 
   * @param e the entry
   * @param sc the scan context
   * @return the row-value
   */
  public static Object readValueFromMap(final Entry<IMKey, RegionEntry> e, final ScanContext sc) {
    long l1 = System.nanoTime();
    RegionEntry regionEntry = e.getValue();
    Object data;
    synchronized (regionEntry) {
      data = regionEntry._getValue();
    }
    if (data == null || Token.isInvalidOrRemoved(data)) {
      sc.scanKey.setKey(e.getKey().getBytes());
      try {
        data = getValueUsingGeodeGetEntry(sc.conn, sc.scanKey, sc.region);
      } catch (IOException e1) {
        logger.warn("Exception during <getValueUsingGeodeGetEntry>.", e1);
        data = null;
      }
    }
    Encoding enc = Encoding.getEncoding(
        data instanceof byte[] ? ((byte[]) data)[RowHeader.OFFSET_ENCODING] : EncodingA.ENC_IDX);
    InternalRow row = sc.getInternalRow();
    row.reset(e.getKey().getBytes(), data, enc, sc.scan.getColumnNameList());
    if (data == null) {
      logger.warn("Got <null> value for key= {}", e.getKey());
    }
    sc.readValueTime += (System.nanoTime() - l1);
    return row;
  }

  /**
   * Gets the value from the Region Entry. First this method will try to get the value directly from
   * the RegionEntry If above method fails then will use Geode GetEntry API to get the value.
   * 
   * @return the entry retrieved via GET operation
   */
  public static Object getValueUsingGeodeGetEntry(ServerConnection servConn, MTableKey scanKey,
      Region region) throws IOException {
    long l = System.nanoTime();
    Object data;
    Get70 request = (Get70) Get70.getCommand();
    Get70.Entry entry = performGetEntry(region, servConn, request, scanKey);
    data = entry.value;
    return data;
  }

  /**
   * Perform PreAuthorization
   */
  private GetOperationContext performPreAuthorization(AuthorizeRequest authzRequest,
      String regionName, Object key, MPartList values, ServerConnection servConn) {
    GetOperationContext getContext = null;
    try {
      getContext = authzRequest.getAuthorize(regionName, key, null);
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Passed GET pre-authorization for key={}", servConn.getName(), key);
      }
    } catch (NotAuthorizedException ex) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.MScan_0_CAUGHT_THE_FOLLOWING_EXCEPTION_ATTEMPTING_TO_GET_VALUE_FOR_KEY_1,
          new Object[] {servConn.getName(), key}), ex);
      values.addExceptionPart(key, ex);
    }
    return getContext;
  }

  private static Get70.Entry performGetEntry(Region region, ServerConnection servConn,
      Get70 request, MTableKey scanKey) {
    Get70.Entry entry = request.getEntry(region, scanKey, null, servConn);
    return entry;
  }

  /**
   * Performs post authorization
   * 
   * @return data
   */
  private Object performPostAuthorization(AuthorizeRequestPP postAuthzRequest, String regionName,
      Object key, Object data, boolean isObject, GetOperationContext getContext, MPartList values,
      ServerConnection servConn) {
    try {
      getContext = postAuthzRequest.getAuthorize(regionName, key, data, isObject, getContext);
      GetOperationContextImpl gci = (GetOperationContextImpl) getContext;
      Object newData = gci.getRawValue();
      if (newData != data) {
        // user changed the value
        data = newData;
      }
    } catch (NotAuthorizedException ex) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.MScan_0_CAUGHT_THE_FOLLOWING_EXCEPTION_ATTEMPTING_TO_GET_VALUE_FOR_KEY_1,
          new Object[] {servConn.getName(), key}), ex);
      values.addExceptionPart(key, ex);
    } finally {
      if (getContext != null) {
        ((GetOperationContextImpl) getContext).release();
      }
    }
    return data;
  }

  /**
   * Send chunk of results to client.
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
    if (statsMap != null) {
      statsMap.addTo("serverObjPart", t1);
      statsMap.addTo("serverSendChunk", (System.nanoTime() - l));
    }
  }
}
