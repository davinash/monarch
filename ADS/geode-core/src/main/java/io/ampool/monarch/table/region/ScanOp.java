package io.ampool.monarch.table.region;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.internal.MPartList;
import io.ampool.monarch.table.MultiVersionValueWrapper;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.ScanProfiler;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.internal.MultiVersionValue;
import io.ampool.monarch.table.internal.RowEndMarker;
import io.ampool.monarch.table.internal.ServerScanStatus;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.AbstractOp;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.ConnectionStats;
import org.apache.geode.cache.client.internal.ExecutablePool;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.UserAttributes;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

/**
 * Client side operator for Scan operations. This operator runs scan operation on region
 * {@link org.apache.geode.cache.Region} as specified by {@link Scan} over server specified by
 * {@link org.apache.geode.distributed.internal.ServerLocation} and adds results in the
 * {@link ArrayBlockingQueue}.
 *
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class ScanOp {

  private static final Logger logger = LogService.getLogger();

  private ScanOp() {
    // no instances allowed
  }

  public static RowEndMarker execute(ExecutablePool pool, Region region, Scan scan,
      BlockingQueue<Object> resultQueue, TableDescriptor tableDescriptor, ServerLocation server) {
    AbstractOp op = new ScanOpImpl(region.getFullPath(), scan, resultQueue, tableDescriptor);
    Object result = null;
    // Serverlocation is null in case when we are scanning empty bucket (e.g. in case of empty
    // table)
    if (server != null) {
      boolean onlyUseExistingCnx = (((PoolImpl) pool).getMaxConnections() != -1
          && ((PoolImpl) pool).getConnectionCount() >= ((PoolImpl) pool).getMaxConnections()) ? true
              : false;
      try {
        UserAttributes.userAttributes.set(UserAttributes.userAttributes.get());
        result = pool.executeOn(server, op, true, onlyUseExistingCnx);
      } catch (AllConnectionsInUseException ex) {
        result = pool.execute(op);
      } finally {
        UserAttributes.userAttributes.set(null);
      }
    }
    return (RowEndMarker) result;
  }

  static class ScanOpImpl extends AbstractOp {
    private final Scan scan;
    private final BlockingQueue<Object> scanResultQueue;
    private final TableDescriptor tableDescriptor;
    private final BiFunction<byte[], Object, Row> rowProducer;

    public ScanOpImpl(String region, Scan scan, BlockingQueue<Object> resultQueue,
        TableDescriptor tableDescriptor) {
      super(MessageType.SCAN, 2);
      this.scan = scan;
      this.scanResultQueue = resultQueue;
      this.tableDescriptor = tableDescriptor;
      getMessage().addStringPart(region);
      // Initializing msg part here
      initMessagePart(scan);
      this.rowProducer = ScanUtils.getRowProducer(scan, tableDescriptor);
    }

    @Override
    protected void initMessagePart(Scan scan) {
      getMessage().addObjPart(scan);
    }

    @Override
    protected Message createResponseMessage() {
      // Copied from GetAllOp for now
      return new ChunkedMessage(1, Version.CURRENT);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Object processResponse(Message m, Connection con) throws Exception {
      Object o;
      long l = System.nanoTime();
      o = processScanResponse(m, con);
      if (scan instanceof ScanProfiler) {
        ((ScanProfiler) scan).addMetric("processTime", (System.nanoTime() - l));
        ((ScanProfiler) scan).addMetric("processCount", (1L));
      }
      return o;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.SCAN_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startScan();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endScanSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endScan(start, hasTimedOut(), hasFailed());
    }

    private RowEndMarker resultEndMarker = null;

    protected Object processScanResponse(Message msg, final Connection con) throws Exception {
      final Exception[] exceptionRef = new Exception[1];

      processChunkedResponse((ChunkedMessage) msg, "scan", !scan.batchModeEnabled() ?
      /** for MTable+streaming **/
          new ChunkHandler() {
            @SuppressWarnings("unchecked")
            public void handle(ChunkedMessage cm) throws Exception {
              Part part = cm.getPart(0);
              boolean offerFailed;
              Row row;
              try {
                Object o = part.getObject();
                if (o instanceof Throwable) {
                  String s = "While performing a remote scan";
                  exceptionRef[0] = new ServerOperationException(s, (Throwable) o);
                } else {
                  List<Row> list = new ArrayList<>(
                      scan.getMessageChunkSize() == 0 ? 101 : scan.getMessageChunkSize() + 1);
                  MPartList chunk = (MPartList) o;
                  Object key;
                  Object value;
                  long l, t1 = 0, t2 = 0;
                  for (int i = 0; i < chunk.size(); i++) {
                    key = chunk.getKey(i);
                    value = chunk.getValue(i);
                    if (value instanceof byte[] || value instanceof MultiVersionValueWrapper
                        || value instanceof MultiVersionValue) {
                      l = System.nanoTime();
                      row = rowProducer.apply((byte[]) key, value);
                      t1 += (System.nanoTime() - l);
                      // l = System.nanoTime();
                      list.add(row);
                      // t2 += (System.nanoTime() - l);
                    } else if (value instanceof byte[][]) {
                      // bucket always has last entry which is batch end marker
                      // which contains server status and last stop key in its value
                      byte[][] batchMarker = (byte[][]) value;
                      resultEndMarker = new RowEndMarker(batchMarker[0], batchMarker[1]);
                      if (scan instanceof ScanProfiler) {
                        ((ScanProfiler) scan).addMetric("clientReadChunk", cm.totalTime[1]);
                        ((ScanProfiler) scan).addMetric("clientReadRestTime", cm.totalTime[3]);
                        ((ScanProfiler) scan).addMetric("clientReadPayload", cm.totalTime[0]);
                        ((ScanProfiler) scan).addMetric("clientReadChunkCount", cm.totalTime[2]);
                      }
                      // System.out.println("ScanOpImpl.handle --> " + resultEndMarker[0]);
                    } else if (value instanceof Object2LongOpenHashMap
                        && scan instanceof ScanProfiler) {
                      for (Object2LongOpenHashMap.Entry<String> e : ((Object2LongOpenHashMap<String>) value)
                          .object2LongEntrySet()) {
                        ((ScanProfiler) scan).addMetric(e.getKey(), e.getLongValue());
                      }
                    }
                  }
                  if (list.size() > 0) {
                    offerResults(list, t1);
                  }
                }
              } catch (Exception e) {
                exceptionRef[0] = new ServerOperationException("Unable to deserialize value", e);
              }
            }
          } :
          /** for batch-mode (MTable/FTable) **/
          new ChunkHandler() {
            @SuppressWarnings("unchecked")
            public void handle(ChunkedMessage cm) throws Exception {
              Part part = cm.getPart(0);
              boolean offerFailed;
              long t1, t2, t3;
              Row row;
              try {
                Object o = part.getObject();
                if (o instanceof Throwable) {
                  String s = "While performing a remote scan";
                  exceptionRef[0] = new ServerOperationException(s, (Throwable) o);
                } else {
                  MPartList chunk = (MPartList) o;
                  Object key;
                  Object value;
                  for (int i = 0; i < chunk.size(); i++) {
                    key = chunk.getKey(i);
                    value = chunk.getValue(i);
                    if (value instanceof byte[] || value instanceof MultiVersionValueWrapper) {
                      t1 = System.nanoTime();
                      row = rowProducer.apply((byte[]) key, value);
                      t2 = System.nanoTime();
                      offerFailed =
                          scanResultQueue.offer(row, scan.getScanTimeout(), TimeUnit.SECONDS);
                      t3 = System.nanoTime();
                      if (!offerFailed) {
                        throw new TimeoutException(
                            "scan timed out waiting for client to consume results");
                      }
                      if (scan instanceof ScanProfiler) {
                        ((ScanProfiler) scan).addMetric("clientNewRowTime", (t2 - t1));
                        ((ScanProfiler) scan).addMetric("clientOfferTime", (t3 - t2));
                      }
                    } else if (value instanceof byte[][]) {
                      // bucket always has last entry which is batch end marker
                      // which contains server status and last stop key in its value
                      byte[][] batchMarker = (byte[][]) value;
                      resultEndMarker = new RowEndMarker(batchMarker[0], batchMarker[1]);
                      if (scan instanceof ScanProfiler) {
                        ((ScanProfiler) scan).addMetric("clientReadChunk", cm.totalTime[1]);
                        ((ScanProfiler) scan).addMetric("clientReadRestTime", cm.totalTime[3]);
                        ((ScanProfiler) scan).addMetric("clientReadPayload", cm.totalTime[0]);
                        ((ScanProfiler) scan).addMetric("clientReadChunkCount", cm.totalTime[2]);
                      }
                    } else if (value instanceof Object2LongOpenHashMap
                        && scan instanceof ScanProfiler) {
                      for (Object2LongOpenHashMap.Entry<String> e : ((Object2LongOpenHashMap<String>) value)
                          .object2LongEntrySet()) {
                        ((ScanProfiler) scan).addMetric(e.getKey(), e.getLongValue());
                      }
                    }
                  }
                }
              } catch (Exception e) {
                exceptionRef[0] = new ServerOperationException("Unable to deserialize value", e);
              }
            }
          });
      if (exceptionRef[0] != null) {
        throw exceptionRef[0];
      } else {
        return resultEndMarker;
      }
    }

    /**
     * Offer the results received from server to the client queue. In case InterruptedException was
     * received, the results are not sent over the client-queue as the consumer would have gone.
     *
     * @param list the list of rows received from server
     * @param clientNewRowTime the time taken to create the rows on client side
     * @throws TimeoutException if the offer was successful in the specified time
     */
    private void offerResults(final List<Row> list, final long clientNewRowTime)
        throws TimeoutException {
      long clientOfferTime = 0;
      boolean offerFailed;
      boolean isInterrupted = false;
      try {
        /* TODO: need reliable mechanism than interruption to know that the consumer has gone */
        if (!Thread.currentThread().isInterrupted()) {
          long l = System.nanoTime();
          offerFailed = scanResultQueue.offer(list, scan.getScanTimeout(), TimeUnit.SECONDS);
          clientOfferTime += (System.nanoTime() - l);
          if (!offerFailed) {
            if (scan.isInterruptOnTimeout()) {
              logger.warn("Interrupting DataFetchThread.");
              Thread.currentThread().interrupt();
            }
            logger.error("Waiting for client to consume results...");
            throw new TimeoutException("scan timed out waiting for client to consume results");
          }
        } else {
          isInterrupted = true;
        }
      } catch (InterruptedException ie) {
        isInterrupted = true;
      }
      if (isInterrupted) {
        resultEndMarker = new RowEndMarker(ServerScanStatus.SCAN_COMPLETED.getStatusBytes(), null);
      }
      if (scan instanceof ScanProfiler) {
        ((ScanProfiler) scan).addMetric("clientNewRowTime", clientNewRowTime);
        ((ScanProfiler) scan).addMetric("clientOfferTime", clientOfferTime);
      }
    }
  }

}
