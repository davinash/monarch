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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import io.ampool.internal.MPartList;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.geode.internal.cache.tier.sockets.ChunkedStreamMessage;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

/**
 * The sender thread that processes and sends the response asynchronously so that server can
 * continue processing while the data is being sent. The queue size of values is 1 so that only one
 * response is sent at once.
 * <p>
 */
public class ScanCommandSenderThread extends Thread {
  private static final Logger logger = LogService.getLogger();

  private ServerConnection serverConn;
  private BlockingQueue<Object> valuesQueue;
  private BlockingQueue<MPartList> returnQueue;
  private Object2LongOpenHashMap<String> statsMap;
  private ReentrantLock lock = new ReentrantLock(true);
  private Condition condition = lock.newCondition();
  private volatile boolean isSignalled = false;

  ScanCommandSenderThread() {
    setDaemon(true);
  }

  void setDetails(final ServerConnection serverConn, final BlockingQueue<Object> queue,
      final BlockingQueue<MPartList> returnQueue, Object2LongOpenHashMap<String> statsMap) {
    this.serverConn = serverConn;
    this.valuesQueue = queue;
    this.returnQueue = returnQueue;
    this.statsMap = statsMap;
  }

  @Override
  public void run() {
    Object o;
    /* process the data till end-marker is sent */
    ChunkedStreamMessage chunkedResponseMsg = serverConn.getChunkedStreamResponseMessage();
    long t, t1 = 0, t2 = 0, t3;
    int count = 0;
    t3 = System.nanoTime();
    for (;;) {
      try {
        o = valuesQueue.take();
        if (o == ScanCommand.END_MARKER) {
          /* take lock and notify the waiting threads that this thread is done */
          lock.lockInterruptibly();
          try {
            this.isSignalled = true;
            this.condition.signal();
          } finally {
            lock.unlock();
          }
          break;
        } else if (o instanceof MPartList) {
          t = System.nanoTime();
          chunkedResponseMsg.setNumberOfParts(1);
          chunkedResponseMsg.setLastChunk(false);
          chunkedResponseMsg.addObjPartNoCopying(o);
          t1 += (System.nanoTime() - t);

          t = System.nanoTime();
          chunkedResponseMsg.sendChunk(serverConn);
          t2 += (System.nanoTime() - t);
          count++;
          ((MPartList) o).clear();
          returnQueue.put((MPartList) o);
        }
      } catch (InterruptedException | IOException e) {
        logger.warn("Received error while sending response.");
        break;
      }
    }
    if (statsMap != null) {
      statsMap.addTo("serverObjPart", t1);
      statsMap.addTo("serverSendChunk", t2);
      statsMap.addTo("serverSenderTotal", (System.nanoTime() - t3));
    }
    returnQueue.clear();
  }

  /**
   * Wait until this thread processes the last entry i.e. END_MARKER.
   */
  public void await() {
    try {
      lock.lockInterruptibly();
      if (!this.isSignalled) {
        this.condition.await();
      }
    } catch (InterruptedException e) {
      logger.warn("{} await interrupted.", this.getName(), e);
    } finally {
      lock.unlock();
    }
  }

  BlockingQueue<Object> getValuesQueue() {
    return valuesQueue;
  }

  BlockingQueue<MPartList> getReturnQueue() {
    return returnQueue;
  }
}
