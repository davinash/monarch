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

import io.ampool.store.StoreRecord;
import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;

public class StoreImageReplyMessage extends ReplyMessage {
  private ArrayList<StoreRecord> entries;
  private int tier;
  private String tableName;
  private int partitionId;
  private String orcFileName;
  private int numOfChunks = 0;


  public String getOrcFileName() {
    return orcFileName;
  }

  public void setOrcFileName(final String orcFileName) {
    this.orcFileName = orcFileName;
  }

  public ArrayList<StoreRecord> getEntries() {
    return entries;
  }

  public void setEntries(final ArrayList<StoreRecord> entries) {
    this.entries = entries;
  }

  public int getTier() {
    return tier;
  }

  public void setTier(final int tier) {
    this.tier = tier;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(final String tableName) {
    this.tableName = tableName;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(final int partitionId) {
    this.partitionId = partitionId;
  }

  public int getNumOfChunks() {
    return numOfChunks;
  }

  public void setNumOfChunks(final int numOfChunks) {
    this.numOfChunks = numOfChunks;
  }


  @Override
  public void process(DM dm, ReplyProcessor21 processor) {
    try {
      super.process(dm, processor);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  @Override
  public int getDSFID() {
    return AMPL_STORE_IMAGE_RES;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.entries = DataSerializer.readArrayList(in);
    this.tier = DataSerializer.readInteger(in);;
    this.tableName = DataSerializer.readString(in);
    this.partitionId = DataSerializer.readInteger(in);
    this.orcFileName = DataSerializer.readString(in);
    this.numOfChunks = DataSerializer.readInteger(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeArrayList(entries, out);
    DataSerializer.writeInteger(tier, out);
    DataSerializer.writeString(tableName, out);
    DataSerializer.writeInteger(partitionId, out);
    DataSerializer.writeString(orcFileName, out);
    DataSerializer.writeInteger(numOfChunks, out);
  }

  @Override
  public String toString() {
    return "StoreImageReplyMessage{" + "entries=" + entries + ", tier=" + tier + ", tableName='"
        + tableName + '\'' + ", partitionId=" + partitionId + ", orcFileName='" + orcFileName + '\''
        + ", numOfChunks=" + numOfChunks + '}';
  }
}
