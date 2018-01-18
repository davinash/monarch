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

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.conf.Constants;
import io.ampool.internal.AmpoolOpType;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MTableType;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.ScanProfiler;
import io.ampool.monarch.table.exceptions.MCacheInternalErrorException;
import io.ampool.monarch.table.exceptions.MScanFailedException;
import io.ampool.monarch.table.exceptions.MScanSplitUnavailableException;
import io.ampool.monarch.table.ftable.FTable;
import org.apache.geode.GemFireException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MClientScannerUsingGetAll extends Scanner implements Runnable {

  private final static int BATCH_WAIT_SLEEP = 50; // milliseconds

  private final Scan scan;
  private final InternalTable table;
  private final MTableType tableType = null;
  private final int clientQueueSize;

  private byte[] lastStopKey;

  /* this is the current iterator of the any of the map */
  private Iterator<Integer> bucketIdIterator = Collections.emptyIterator();

  private BlockingQueue<Object> resultQueue;
  private Thread dataFetchThread = null;
  private Throwable dataFetchException = null;
  private ReentrantLock batchLock = null;
  private boolean inNextBatch = false;
  private volatile boolean scanCompleted = false;
  private volatile boolean started = false;
  private volatile boolean running = false;
  private volatile boolean scanInterrupted = false;

  // FOR SYSTEM TEST TEST ONLY
  private boolean injectNextFault = false;
  private boolean injectNextInternalFault = false;

  private Integer currentBucketId;
  // private boolean isScanSingleRowFetch;
  private volatile int numRecordsConsumed = 0;
  private int maxResultLimit;
  private ServerScanStatus serverScanStatus = ServerScanStatus.BUCKET_ENDED;
  final boolean isDebugEnabled = logger.isDebugEnabled();
  private Map<Integer, Set<ServerLocation>> bucketToServerLocationMap;

  private TreeSet<Integer> bucketIdSet = new TreeSet<>();

  public MClientScannerUsingGetAll(Scan scan, InternalTable table) {
    // take ownership to prevent caller changing scan object during scan
    // ... also construct the appropriate scan profiler, if required, with
    // reference to original profiler-map.
    this.scan = scan instanceof ScanProfiler
            ? new ScanProfiler(scan, ((ScanProfiler) scan).getProfileData()) : new Scan(scan);
    this.table = table;
    // this.tableType = table.getTableDescriptor().getTableType();
    this.clientQueueSize = scan.getClientQueueSize();

    initialize(this.scan, this.table);
  }

  private void initialize(Scan scan, Table table) {
    if (this.clientQueueSize < 2) {
      throw new IllegalArgumentException("batch size must be > 1");
    }

    scan.setReturnKeysFlag(true);

    this.bucketIdSet.addAll(getApplicableBucketIds(scan, table.getTableDescriptor()));

    // Handling columnnames and columnids in case of selective scan
    List<Integer> columnIds = scan.getColumns();
    MTableUtils.setColumnNameOrIds(scan, table.getTableDescriptor());

    this.bucketIdIterator =
            scan.isReversed() ? bucketIdSet.descendingIterator() : bucketIdSet.iterator();
    this.lastStopKey = null;
    /** in case of streaming mode chunks send over the queue.. so don't need larger queue **/
    resultQueue = new ArrayBlockingQueue<>(scan.batchModeEnabled() ? scan.getClientQueueSize() : 3);
    currentBucketId = -1;
    maxResultLimit = scan.getMaxResultLimit();

    if (scan.batchModeEnabled()) {
      batchLock = new ReentrantLock();
    }

    try {
      this.bucketToServerLocationMap =
              getBucketToServerLocation(table.getName(), scan, this.bucketIdSet);
    } catch (Exception e) {
      MTableUtils.checkSecurityException(e);
      throw e;
    }
    // if (!this.isStarted()) {
    // start data fetch
    dataFetchThread = new Thread(this);
    dataFetchThread.setName("DataFetchThread");
    dataFetchThread.setDaemon(true);
    dataFetchThread.start();
    setStarted(true);

    // }
  }

  /*
   * THIS FUNCTION FOR SYSTEM TEST ONLY!
   */
  public void setInjectNextFault(boolean inject) {
    logger.warn("System test function inject fault to scan next() called");
    this.injectNextFault = inject;
  }

  /*
   * THIS FUNCTION FOR SYSTEM TEST ONLY!
   */
  public void setInjectNextInternalFault(boolean inject) {
    logger.warn("System test function inject fault to scan nextInternal() called");
    this.injectNextInternalFault = inject;
  }

  private Map<Integer, Set<ServerLocation>> getBucketToServerLocation(String tableName, Scan scan,
                                                                      Set<Integer> bucketIdSet) {
    Map<Integer, Set<ServerLocation>> map = scan.getBucketToServerMap();
    if (map == null || map.isEmpty()) {
      map = MTableUtils.getBucketToServerMap(tableName, bucketIdSet, AmpoolOpType.SCAN_OP);
    }
    return map;
  }

  private boolean isRunning() {
    return this.running;
  }

  private boolean isStarted() {
    return this.started;
  }

  private void stop() {
    this.running = false;
    if (this.dataFetchThread != null) {
      this.dataFetchThread.interrupt();
      try {
        this.dataFetchThread.join(100);
        if (this.dataFetchThread.isAlive()) {
          logger.warn("Thread= {} still alive after join.", this.dataFetchThread.toString());
        }
      } catch (InterruptedException exc) {
      }
      this.dataFetchThread = null;
    }
  }

  private void setStarted(boolean started) {
    this.running = true;
    this.started = started;
  }

  @Override
  public void run() {
    while (nextInternal() && this.running) {
      if (numRecordsConsumed == maxResultLimit) {
        break;
      }
    }
    this.running = false;
  }

  private void setException(Throwable t) {
    this.dataFetchException = t;
  }

  public Throwable getException() {
    return this.dataFetchException;
  }

  public boolean scanCompleted() {
    return scanCompleted;
  }

  // private boolean checkIfScanIsSingleRowFetch() {
  // BigInteger startKey = new BigInteger(originalStartKey);
  // BigInteger stopKey = new BigInteger(originalStopKey);
  // return (stopKey.subtract(startKey) == BigInteger.ONE) ? true : false;
  // }

  private Iterator<Row> internalIterator = Collections.emptyIterator();

  /**
   * Return the next row in a scan. Note: always call {@link #close()} on a scanner object,
   * especially when the scan was ended before finishing.
   *
   * @return {@link Row} for the next row, or null if no more matching rows.
   */
  @SuppressWarnings("unchecked")
  @Override
  public Row next() {
    if (scanCompleted) {
      return null;
    }

    Row result = null;
    if (numRecordsConsumed == maxResultLimit) {
      // Case when we have hit max result limit
      if (isDebugEnabled) {
        logger.debug("Reached max limit: {}, stopping scan", maxResultLimit);
      }
      close();
      return null;
    } else {
      // Queue is empty since we encountered exception
      if (this.scanInterrupted || (resultQueue.size() == 0 && getException() != null)) {
        throwDataFetchException();
      }
      long t1, t2;
      try {
        t1 = System.nanoTime();
        if (!internalIterator.hasNext()) {
          /* a safeguard check in case adding end-marker had failed due to timeout */
          if (this.resultQueue.isEmpty() && !this.running) {
            return null;
          }
          Object o = resultQueue.take();
          if (o instanceof List) {
            internalIterator = ((List<Row>) o).iterator();
            result = internalIterator.next();
          } else if (o instanceof RowEndMarker) {
            result = (RowEndMarker) o;
          } else if (o instanceof Row) {
            result = (Row) o;
          }
        } else {
          result = internalIterator.next();
        }
        t2 = System.nanoTime();
        if (this.injectNextFault && numRecordsConsumed > 10) {
          throw new IllegalStateException("SYSTEM TEST SYSTEM TEST");
        }
        if (scan instanceof ScanProfiler) {
          ((ScanProfiler) scan).addMetric("clientTakeTime", (t2 - t1));
        }
      } catch (Exception e) {
        // make sure close is called on exception in case user does not have logic to do so
        close();
        logger.error("Unexpected error while getting result from internal result queue", e);
        throw new RuntimeException(
                "Unexpected error while getting result from internal result queue", e);
      }
      if (result instanceof RowEndMarker || this.scanInterrupted) {
        result = null;
        this.scanCompleted = true;
        if (dataFetchException != null) {
          throwDataFetchException();
        }
      }
      if (result != null) {
        numRecordsConsumed++;
      }
    }
    return result;
  }

  private void throwDataFetchException() {
    this.running = false;
    /*
     * There was an exception in processing the scan, so the scanner recorded the exception and
     * ended the scan; report it now.
     */
    Throwable dataFetchException = getException();
    if (dataFetchException.getCause() != null) {
      throw new MScanFailedException(dataFetchException.getCause());
    } else {
      if (dataFetchException instanceof MScanSplitUnavailableException) {
        throw (MScanSplitUnavailableException) dataFetchException;
      }
      // Hook for diagnostics/debugging. Uncomment when needed.
      // dataFetchException.printStackTrace();
      throw new MScanFailedException(dataFetchException);
    }
  }

  private void nextBatchInternal(ArrayList<Row> results) {
    if (batchLock == null) {
      throw new IllegalStateException("Batching is not enabled");
    }

    // insure we are not in a fetch
    batchLock.lock();
    inNextBatch = true;
    try {
      Row item = null;

      if (resultQueue.isEmpty()) {
        batchLock.unlock();
        // first time through, initiate first fetch
        item = next();
      } else {
        batchLock.unlock();
        // remove hold element and initiate next fetch
        next(); // element left in queue
        item = next();
      }

      // re-acquire lock
      batchLock.lock();

      if (item != null) {
        if (resultQueue.size() > 1) {
          results.add(item);
          while (resultQueue.size() > 1) {
            item = next();
            if (item == null) {
              break;
            } // scan ended
            results.add(item);
          }
        } else {
          results.add(item);
        }
        item = (Row) resultQueue.peek();
        if (item != null) {
          if (item instanceof RowEndMarker) {
            next(); // end the scan
          } else {
            // add, but do not remove, last item
            results.add(item);
          }
        }
      }
    } finally {
      if (batchLock.isHeldByCurrentThread()) {
        inNextBatch = false;
        batchLock.unlock();
      }
    }
  }

  /**
   * Get the next batch for a batch mode scan. A batch is defined as the data for one batch in the
   * scan, as defined by {@link Scan#setBatchSize(int)}. The batch may contain less than batch size
   * data if only a partial batch is remaining. Note: always call {@link #close()} on a scanner
   * object, especially in batch mode when the scan was ended before finishing.
   *
   * @return An array containing the next batch of data for the scan.
   */
  @Override
  public Row[] nextBatch() {
    if (!scan.batchModeEnabled()) {
      throw new IllegalStateException("cannot call nextBatch: scan is not set to batch mode");
    }

    ArrayList<Row> results = new ArrayList<>(scan.getBatchSize());

    nextBatchInternal(results);

    return results.toArray(new Row[0]);
  }

  private boolean batchWait() {
    // batch mode is for interactive sessions or clients
    // that take a long time processing each batch and do
    // not want to run into possible network timeouts.
    boolean interrupted = false;
    while (!resultQueue.isEmpty() && isRunning()) {
      try {
        Thread.sleep(BATCH_WAIT_SLEEP);
      } catch (InterruptedException iexc) {
        interrupted = true;
        break;
      }
    }
    if (!isRunning()) {
      interrupted = true;
    }
    return interrupted;
  }

  /**
   * Fetch Next batch should always succeed as we are calling it knowing last key in bucket
   */
  private boolean fetchNextBatch() {
    RowEndMarker resultEndMarker = null;

    scan.setStartRow(lastStopKey);
    scan.setIncludeStartRow(false);
    scan.setBucketId(currentBucketId);

    /*
     * This function is not expected to be used during typical operation where each bucket will be
     * fetched in its entirety. It exists to support the case where for some reason the server is
     * not able to return all data and requests we make another call to obtain the remaining bucket
     * data, or for batch mode where a client can leave at least one item in the queue and we will
     * wait until it is empty to request the next batch.
     */

    if (isDebugEnabled) {
      logger.debug(
              "Fetching next batch from bucket: {} from after key: {} with batch size: {} at time: {}",
              currentBucketId, Arrays.toString(lastStopKey), scan.getBatchSize(), System.nanoTime());
    }
    ServerLocation serverLocation = getServerLocation(scan.getBucketId());
    if (serverLocation != null) {
      resultEndMarker = runScan(this.scan, serverLocation, this.resultQueue);
    } else {
      // Safeguard handling:
      // This is happen in case metadata refresh has happen and bucket has moved to another server
      // Marking this as bucket ended and using last stop key
      resultEndMarker =
              new RowEndMarker(ServerScanStatus.BUCKET_ENDED.getStatusBytes(), this.lastStopKey);
    }
    updateEndMarkerKeys(resultEndMarker);
    if ((resultEndMarker == null) || (ServerScanStatus.SCAN_COMPLETED == serverScanStatus)
            || ((ServerScanStatus.BUCKET_ENDED == serverScanStatus) && !bucketIdIterator.hasNext())) {
      return false;
    } else {
      return true;
    }
  }

  private ServerLocation getServerLocation(int bucketId) {
    ServerLocation serverLocation = null;
    Set<ServerLocation> serverLocations = this.bucketToServerLocationMap.get(bucketId);
    if (serverLocations != null && serverLocations.size() > 0) {
      // int serverPos = new Random().nextInt(serverLocations.size());
      // int pos = 0;
      // Iterator<ServerLocation> iterator = serverLocations.iterator();
      // while (pos++ < serverPos) {
      // iterator.next();
      // }
      // GEN-1201
      // Returning first(primary) bucket at all times
      // as primary and secondary bucket counts are not in sync.
      serverLocation = serverLocations.iterator().next();
    }
    return serverLocation;
  }

  private RowEndMarker runScan(Scan scan, ServerLocation serverLocation,
                               BlockingQueue<Object> resultQueue) {
    RowEndMarker resultEndMarker = null;
    boolean retry;
    int numRetries = 5;
    do {
      /* if the scan is already completed.. do not fetch data from next buckets */
      if (this.scanCompleted || (resultEndMarker != null
              && resultEndMarker.getServerScanStatus() == ServerScanStatus.SCAN_COMPLETED)) {
        logger.debug("Skipping the scan... scanCompleted= {}, resultEndMarker= {}",
                this.scanCompleted, resultEndMarker);
        return new RowEndMarker(ServerScanStatus.SCAN_COMPLETED.getStatusBytes(), null);
      }
      retry = false;
      numRetries--;
      try {
        logger.debug(
                "Running scan on server: " + serverLocation + " for bucket id: " + scan.getBucketId());
        resultEndMarker = table.scan(scan, serverLocation, resultQueue);
        if (resultEndMarker != null
                && resultEndMarker.getServerScanStatus() == ServerScanStatus.BUCKET_MOVED) {
          if (Bytes.compareTo(resultEndMarker.getMarkerData(), Bytes.EMPTY_BYTE_ARRAY) != 0) {
            String[] newServerLocation = Bytes.toString(resultEndMarker.getMarkerData())
                    .split(Constants.General.SERVER_NAME_SEPARATOR);
            serverLocation =
                    new ServerLocation(newServerLocation[0], Integer.parseInt(newServerLocation[1]));
            logger.debug("Bucket with bucket id: " + scan.getBucketId() + " moved to server: "
                    + serverLocation + " .Retrying...");
            retry = true;
          } else {
            // Since server does not have information about that bucket,
            // we can assume that bucket has been destroyed, moving scan to next bucket
            resultEndMarker =
                    new RowEndMarker(ServerScanStatus.BUCKET_ENDED.getStatusBytes(), this.lastStopKey);
            logger.debug("Bucket with bucket id: " + scan.getBucketId() + " has been destroyed");
          }
        }
      } catch (Exception ex) {
        if (ex instanceof ServerConnectivityException || ex instanceof EOFException) {
          String exceptionMsg =
                  "Server [" + serverLocation.getHostName() + ":" + serverLocation.getPort()
                          + "] hosting split(bucket) [ID = " + scan.getBucketId() + "] is down";
          // If bucket scan is in progress and server holding that bucket goes down, throw
          // MScanSplitUnavailableException to end-user without failover
          if (ex.getCause() != null && ex.getCause().getMessage() != null
                  && (ex.getCause().getMessage().equals(
                  LocalizedStrings.Message_THE_CONNECTION_HAS_BEEN_RESET_WHILE_READING_THE_PAYLOAD
                          .toLocalizedString())
                  || ex.getCause().getMessage()
                  .equals(LocalizedStrings.ChunkedMessage_CHUNK_READ_ERROR_CONNECTION_RESET
                          .toLocalizedString()))) {
            // wrap the ex into the MScanSplitUnavailableException and throw it to enduser
            throw new MScanSplitUnavailableException(exceptionMsg);
          }
          this.bucketToServerLocationMap =
                  getBucketToServerLocation(table.getName(), scan, this.bucketIdSet);
          serverLocation = getServerLocation(scan.getBucketId());
          if (serverLocation == null) {
            // This means server was avilable when scan was initiated but it is not available now
            // and there are no alternate buckets available to fetch data from
            throw new MScanSplitUnavailableException(exceptionMsg);
          }
          logger.debug("Server connection problem. Refreshing metadata and retrying...");
          retry = true;
        } else {
          throw ex;
        }
      }
    } while (retry && numRetries > 0);
    logger.debug(
            "Server status for scan of bucket id: " + scan.getBucketId() + " is " + resultEndMarker);
    return resultEndMarker;
  }

  private boolean isOneBucketFound = false;

  private boolean fetchNextBucket() {
    if (!bucketIdIterator.hasNext()) {
      return false;
    }

    RowEndMarker resultEndMarker = null;

    // Setting start row based on last stop row key
    // if scan is reversed scan send start key every time to get range
    if (lastStopKey == null || scan.isReversed()) {
      // Going to fetch for first time
      scan.setIncludeStartRow(true);
    } else {
      scan.setStartRow(table instanceof FTable ? null : lastStopKey);
      scan.setIncludeStartRow(false);
    }

    if (isDebugEnabled) {
      logger.debug(
              "Fetching next bucket id starting from bucket id: {} with startkey: {} and stopkey: {} at time: {}",
              currentBucketId, scan.getStartRow(), scan.getStopRow(), System.nanoTime());
    }

    // Iterating over bucket set and ignoring empty buckets
    do {
      currentBucketId = bucketIdIterator.next();
      scan.setBucketId(currentBucketId);
      ServerLocation serverLocation = getServerLocation(currentBucketId);
      if (serverLocation != null) {
        isOneBucketFound = true;
        resultEndMarker = runScan(this.scan, serverLocation, this.resultQueue);
      }
      // resultEndMarker will be null only if serverLocation is null
    } while (resultEndMarker == null && bucketIdIterator.hasNext());

    if (!isOneBucketFound) {
      /**
       * just a safe-guard check.. in case the table is empty there are no buckets created but
       * region still exists. So no need to throw an exception in such cases.
       */
      try {
        if (MonarchCacheImpl.isMetaRegionCached()) {
          table.getInternalRegion().get(new MKeyBase(new byte[] {-31}));
        }
      } catch (GemFireException e) {
        if (e.getRootCause() instanceof RegionDestroyedException) {
          final String regionName = table.getName();
          final String reason =
                  "Region named /" + regionName + " was not found during scan request.";
          throw new MCacheInternalErrorException("Scan operation failed",
                  new RegionDestroyedException(reason, regionName));
        }
      }
    }
    updateEndMarkerKeys(resultEndMarker);

    if ((resultEndMarker == null) || (ServerScanStatus.SCAN_COMPLETED == serverScanStatus)
            || ((ServerScanStatus.BUCKET_ENDED == serverScanStatus) && !bucketIdIterator.hasNext())) {
      return false;
    } else {
      return true;
    }
  }

  private void updateEndMarkerKeys(RowEndMarker resultEndMarker) {
    // Server sends a marker record with the batch to indicate status at server side
    // Marker record includes status and laststopkey upto which data has been send
    // We decide our next action based on server scan status returned in marker record
    if (resultEndMarker != null) {
      serverScanStatus = resultEndMarker.getServerScanStatus();
      lastStopKey = resultEndMarker.getMarkerData();
    }
  }

  private boolean nextInternal() {
    boolean hasNext = false;

    if (batchLock != null) {
      if (batchWait()) {
        return false;
      }
      batchLock.lock();
    }

    try {
      do {
        switch (serverScanStatus) {
          case BATCH_COMPLETED:
            hasNext = fetchNextBatch();
            break;
          case BUCKET_ENDED:
            hasNext = fetchNextBucket();
            break;
          case SCAN_COMPLETED:
            hasNext = false;
            break;
        }
        if (this.injectNextInternalFault && numRecordsConsumed > 10) {
          throw new IllegalStateException("SYSTEM TEST SYSTEM TEST");
        }
        if (batchLock != null) {
          // for batched mode we want one full batch if possible
          if (inNextBatch) {
            if (hasNext && (resultQueue.size() < (clientQueueSize - 1))) {
              scan.setBatchSize((clientQueueSize - 1) - resultQueue.size());
            } else {
              break;
            }
          }
        }
      } while (hasNext && isRunning());
    } catch (Exception exc) {
      if (isDebugEnabled) {
        logger.debug("received exception from table scan");
      }
      hasNext = false;
      setException(exc);
      if (exc instanceof InterruptedException
              || (exc.getCause() != null && exc.getCause() instanceof InterruptedException)) {
        this.scanInterrupted = true;
      }
    } finally {
      if (batchLock != null) {
        scan.setBatchSize(clientQueueSize);
        batchLock.unlock();
      }
    }

    if (hasNext == false) {
      try {
        this.running = false;
        if (getException() != null) {
          logger.warn("adding end marker due to exception ", getException().getCause());
          // if there is an exception we end the scan but try to return rest of existing data
          resultQueue.offer(
                  new RowEndMarker(ServerScanStatus.SCAN_COMPLETED.getStatusBytes(), null), 100,
                  TimeUnit.MILLISECONDS);
          // if this add fails we have to interrupt the scan and dump any data in the queue
        } else if (!this.scanCompleted) {
          // in the no error case we put the end marker in so we can let client know we are done
          resultQueue.offer(
                  new RowEndMarker(ServerScanStatus.SCAN_COMPLETED.getStatusBytes(), null), 100,
                  TimeUnit.MILLISECONDS);
          // we expect to always be able to put end marker in no error case but it will timeout if
          // client is not draining the queue and we will terminate the scan.
        }
      } catch (Exception e) {
        logger.info("failed to add or offer end marker, interrupting scan");
        // if we cannot put and end marker interrupt scan and clear queue so that client notices
        this.scanInterrupted = true;
        if (this.resultQueue != null) {
          this.resultQueue.clear();
        }
        // what to do here? Probably OK to just ignore as scan is ended
      }
    }

    return hasNext;
  }

  @Override
  public void close() {
    if (this.started) {
      this.started = false;
      this.scanCompleted = true;
      if (this.resultQueue != null) {
        this.resultQueue.clear();
      }
      stop();
      this.bucketIdIterator = Collections.emptyIterator();
      setException(null);
    }
  }

  /*
   * TODO: Putting finalize here to call close for the case when a user does not finish a scan and
   * fails to close the scanner does not work since the thread holds references (so finalize will
   * not be called). Unclear how to handle this.
   */
}
