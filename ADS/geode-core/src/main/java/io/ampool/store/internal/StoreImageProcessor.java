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

package io.ampool.store.internal;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.ampool.store.StoreHandler;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreWriter;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;


/**
 * This is the processor that handles
 * {@link org.apache.geode.internal.cache.InitialImageOperation.ImageReplyMessage}s that arrive
 */
class StoreImageProcessor extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();

  /**
   * to know whether chunk recieved or not, since last checkpoint
   */
  private volatile boolean recievedChunk = false;

  /**
   * number of outstanding executors currently in-flight on this request
   */
  private final AtomicInteger msgsBeingProcessed = new AtomicInteger();

  private final String tableName;
  private final int partitionId;
  private final int tier;
  private TierStoreWriter writer;


  @Override
  public boolean isSevereAlertProcessingEnabled() {
    return isSevereAlertProcessingForced();
  }


  public StoreImageProcessor(final InternalDistributedSystem system,
      InternalDistributedMember member, final int tier, final String tableName,
      final int partitionId) {
    super(system, member);
    this.tier = tier;
    this.tableName = tableName;
    this.partitionId = partitionId;
    init();
  }

  public StoreImageProcessor(InternalDistributedSystem system, final int tier, Set members,
      final String tableName, final int partitionId) {
    super(system, members);
    this.tier = tier;
    this.tableName = tableName;
    this.partitionId = partitionId;
    init();
  }

  /**
   * Initialization.. currently deletes all the files from store since only full bucket movement is
   * supported at the moment. Once we move to incremental data movements across tiers, this must be
   * modified accordingly.
   */
  private void init() {
    /**
     * currently the store-data movement supports only full data movement.. hence current data needs
     * to be deleted before moveStore starts sending any data.
     */
    logger.info("Deleting store-data for tableName= {}, partitionId= {}", tableName, partitionId);
    final TierStore store = StoreHandler.getInstance().getTierStore(tableName, tier);
    store.deletePartition(tableName, partitionId);
    writer = store.getWriter(tableName, partitionId);
    final ConverterDescriptor cd = store.getConverterDescriptor(tableName);
    writer.setConverterDescriptor(cd);
  }

  private AtomicInteger chunksProcessed = new AtomicInteger(0);

  @Override
  protected boolean processTimeout() {
    // if chunk recieved then no need to process timeout
    boolean ret = this.recievedChunk;
    this.recievedChunk = false;
    return !ret;
  }


  @Override
  public void process(DistributionMessage msg) {
    // ignore messages from members not in the wait list
    if (!waitingOnMember(msg.getSender())) {
      return;
    }
    StoreImageReplyMessage replyMessage = (StoreImageReplyMessage) msg;
    try {
      this.msgsBeingProcessed.incrementAndGet();
      processChunk(replyMessage);
      checkForChunkProcessingFinish(replyMessage);
    } finally {
      this.msgsBeingProcessed.decrementAndGet();
    }
  }

  private AtomicInteger chunksToProcess = new AtomicInteger(-1);

  private void processChunk(StoreImageReplyMessage storeImageReplyMessage) {
    final List<StoreRecord> storeRecords = storeImageReplyMessage.getEntries();
    if (storeRecords == null && storeImageReplyMessage.getNumOfChunks() >= 0) {
      chunksToProcess.compareAndSet(-1, storeImageReplyMessage.getNumOfChunks());
    } else if (storeRecords != null && storeRecords.size() > 0) {
      try {
        writer.write(writer.getProperties(), storeRecords);
      } catch (IOException e) {
        e.printStackTrace();
      }
      // TODO To be done when handling bucket movement with new tierstore impl.
      // store.append(this.tableName, this.partitionId, storeRecords);
      chunksProcessed.incrementAndGet();
    }
  }

  private void checkForChunkProcessingFinish(StoreImageReplyMessage storeImageReplyMessage) {
    if (chunksToProcess.get() >= 0 && chunksToProcess.get() == chunksProcessed.get()) {
      finishedWaiting = true;
      super.process(storeImageReplyMessage, false);
    }
  }



  /**
   * True if we have signalled to stop waiting
   * <p>
   * Contract of {@link ReplyProcessor21#stillWaiting()} is that it must never return true after
   * having returned false.
   */
  private volatile boolean finishedWaiting = false;

  /**
   * Overridden to wait for messages being currently processed: This situation can come about if a
   * member departs while we are still processing data from that member
   */
  @Override
  protected boolean stillWaiting() {
    if (finishedWaiting) { // volatile fetch
      return false;
    }
    if (this.msgsBeingProcessed.get() > 0) {
      return true;
    }
    return true;
  }
}
