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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import io.ampool.store.StoreHandler;
import io.ampool.store.StoreRecord;
import io.ampool.tierstore.wal.WriteAheadLog;
import io.ampool.tierstore.ConverterDescriptor;
import io.ampool.tierstore.TierStore;
import io.ampool.tierstore.TierStoreReader;
import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;


public class RequestStoreImageMessage extends DistributionMessage implements MessageWithReply {
  private static int CHUNK_SIZE = WriteAheadLog.WAL_FILE_RECORD_LIMIT;
  private String tableName;
  private int partitionID;
  private int tier;
  private int processorID;
  private int batchSize = 1_000;

  public int getProcessorID() {
    return processorID;
  }

  public void setProcessorID(final int processorID) {
    this.processorID = processorID;
  }

  public int getPartitionID() {
    return partitionID;
  }

  public void setPartitionID(final int partitionID) {
    this.partitionID = partitionID;
  }

  public int getTier() {
    return tier;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(final String tableName) {
    this.tableName = tableName;
  }



  @Override
  public int getProcessorType() {
    return DistributionManager.HIGH_PRIORITY_EXECUTOR;
  }

  @Override
  protected void process(final DistributionManager dm) {
    StoreHandler.getInstance().pauseWALMonitoring(tableName, partitionID);
    StoreHandler.getInstance().flushWriteAheadLog(tableName, partitionID);
    processTierFiles(dm);
    StoreHandler.getInstance().resumeWALMonitoring(tableName, partitionID);
  }

  private void processTierFiles(final DistributionManager distributionManager) {
    TierStore tierStore = StoreHandler.getInstance().getTierStore(tableName, tier);
    final TierStoreReader reader = tierStore.getReader(getTableName(), getPartitionID());
    ArrayList<StoreRecord> chunk = new ArrayList<>(batchSize);
    int chunkCount = 0;
    final ConverterDescriptor cd = tierStore.getConverterDescriptor(tableName);
    reader.setConverterDescriptor(cd);
    try {
      final Iterator<StoreRecord> iterator = reader.iterator();
      while (iterator.hasNext()) {
        StoreRecord sr = iterator.next();
        if (sr == null) {
          break;
        }
        chunk.add(sr);
        if (chunk.size() == batchSize) {
          sendChunk(distributionManager, null, chunk, -1);
          chunk.clear();
          chunkCount++;
        }
      }
      if (chunk.size() > 0) {
        sendChunk(distributionManager, null, chunk, -1);
      }
      sendChunk(distributionManager, null, null, chunkCount);
    } catch (Throwable e) {
      e.printStackTrace();
    }
    // TODO To be done when handling bucket movement with new tierstore impl.
    // final LocalNativeORCArchiveStore orcArchiveStore = (LocalNativeORCArchiveStore)
    // StoreHandler.getInstance().getStore(tier);
    // final String[] filesToScan = orcArchiveStore.getFilesToScan(tableName, partitionID);
    // if(filesToScan != null && filesToScan.length > 0) {
    // for (final String file : filesToScan) {
    // final ArrayList<StoreRecord> chunk = getChunk(orcArchiveStore, file);
    // sendChunk(distributionManager, file, chunk, filesToScan.length);
    // }
    // }else {
    // sendChunk(distributionManager, null, null, 0);
    // }
  }

  // private ArrayList<StoreRecord> getChunk(final LocalNativeORCArchiveStore orcArchiveStore, final
  // String orcFileName){
  // final ArrayList<StoreRecord> recordList = new ArrayList();
  // final StoreScan storeScan = new StoreScan(new MScan());
  // final IStoreResultScanner scanner = orcArchiveStore.getScanner(tableName, partitionID,
  // orcFileName, storeScan);
  // StoreRecord record = null;
  // do {
  // record = scanner.next();
  // if (record == null) {
  // break;
  // }
  // recordList.add(record);
  // } while (record != null);
  // return recordList;
  // }

  private void sendChunk(final DistributionManager distributionManager, final String orcFileName,
      final ArrayList<StoreRecord> entries, final int totalChunks) {
    StoreImageReplyMessage reply = new StoreImageReplyMessage();
    reply.setProcessorId(processorID);
    reply.setTier(this.tier);
    reply.setRecipient(getSender());
    reply.setEntries(entries);
    reply.setTableName(tableName);
    reply.setPartitionId(partitionID);
    reply.setOrcFileName(orcFileName);
    reply.setNumOfChunks(totalChunks);
    distributionManager.putOutgoing(reply);
  }



  @Override
  public int getDSFID() {
    return AMPL_STORE_IMAGE_REQ;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.tableName = DataSerializer.readString(in);
    this.partitionID = DataSerializer.readInteger(in);
    this.tier = DataSerializer.readInteger(in);
    this.processorID = DataSerializer.readInteger(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.tableName, out);
    DataSerializer.writeInteger(this.partitionID, out);
    DataSerializer.writeInteger(this.tier, out);
    DataSerializer.writeInteger(this.processorID, out);
  }

  public void setTier(final int tier) {
    this.tier = tier;
  }


}
