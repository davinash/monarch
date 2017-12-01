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

import io.ampool.monarch.table.CDCEvent;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.MEventOperation;
import io.ampool.monarch.table.MEventSequenceID;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.results.FormatAwareRow;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.wan.EventSequenceID;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


public class CDCEventImpl implements CDCEvent {
  private MTable table;
  private MTableKey tableKey;
  private byte[] mValue;
  private static final Logger logger = LogService.getLogger();
  private MEventOperation op;
  private Row row;

  /**
   * The identifier of this event
   */
  protected EventID id;
  /**
   * Whether this event is a possible duplicate
   */

  private boolean possibleDuplicate;

  private MEventSequenceID sequenceID;

  /**
   * Constructor. No-arg constructor for data serialization.
   */
  public CDCEventImpl() {}

  public CDCEventImpl(AsyncEvent ce) {

    final GatewaySenderEventImpl event = (GatewaySenderEventImpl) ce;

    // logger.info("CDCEventImpl region name = " + event.getRegion().getName());
    if (event.getRegion() != null) {
      this.table = MCacheFactory.getAnyInstance().getTable(event.getRegion().getName());
    }

    IMKey imKey = (IMKey) event.getKey();
    if (imKey != null) {
      this.tableKey = new MTableKey(imKey.getBytes());
    }

    // logger.info("MTableKey isPutOp = " + tableKey.isPutOp());

    // Initialize possible duplicate
    this.possibleDuplicate = event.getPossibleDuplicate();
    EventSequenceID eventSequenceID = ce.getEventSequenceID();
    this.sequenceID = new MEventSequenceID(eventSequenceID.getMembershipID(),
        eventSequenceID.getThreadID(), eventSequenceID.getSequenceID());

    Object value = event.getAmpoolValue();

    if (event.getOperation().isCreate()) {
      this.op = MEventOperation.CREATE;
      this.row = constructMEventRow(value);
    } else if (event.getOperation().isUpdate()) {
      if (event.getOpInfo() != null && event.getOpInfo().getOp() == MOperation.DELETE) {
        this.op = MEventOperation.UPDATE;
        this.row = constructMEventRow(value);
      } else {
        this.op = MEventOperation.UPDATE;
        this.row = constructMEventRow(value);
      }
    } else if (event.getOperation().isDestroy()) {
      this.row = constructMEventRow(value);
      this.op = MEventOperation.DELETE;
    }
  }

  private Row constructMEventRow(Object value) {
    if (this.table != null && this.tableKey != null) {
      if (value instanceof MultiVersionValue) {
        // get latest version
        byte[][] latestVersion = ((MultiVersionValue) value).getVersions();
        List<byte[]> colNameList = new ArrayList<>();
        // getColNameList(latestVersion, this.table.getTableDescriptor());
        return new FormatAwareRow(tableKey.getBytes(), latestVersion,
            this.table.getTableDescriptor(), colNameList, true);
      } else if (value instanceof byte[]) {
        // single version value
        List<byte[]> colNameList = getColNameList((byte[]) value, this.table.getTableDescriptor());
        return new FormatAwareRow(tableKey.getBytes(), (byte[]) value,
            this.table.getTableDescriptor(), colNameList, false);
      } else if (value == null) {
        return new FormatAwareRow(tableKey.getBytes(), value, this.table.getTableDescriptor(),
            Collections.emptyList(), false);
      }
    }
    return null;
  }

  private List<byte[]> getColNameList(byte[] value, MTableDescriptor tableDescriptor) {
    IBitMap bitSet = MTableStorageFormatter.readBitMap(tableDescriptor, value);
    List<byte[]> colNameList = new LinkedList<>();
    tableDescriptor.getColumnDescriptors().forEach(columnDescriptor -> {
      if (bitSet.get(columnDescriptor.getIndex())) {
        colNameList.add(columnDescriptor.getColumnName());
      }
    });
    return colNameList;
  }

  @Override
  public MTable getMTable() {
    return this.table;
  }

  @Override
  public MTableDescriptor getMTableDescriptor() {
    if (this.table != null) {
      return table.getTableDescriptor();
    } else {
      return null;
    }
  }

  @Override
  public MEventOperation getOperation() {
    return this.op;
  }

  @Override
  public byte[] getKey() {
    return this.tableKey != null ? this.tableKey.getBytes() : null;
  }

  @Override
  public Row getRow() {
    return this.row;
  }

  @Override
  public MEventSequenceID getEventSequenceID() {
    return this.sequenceID;
  }

  @Override
  public boolean getPossibleDuplicate() {
    return this.possibleDuplicate;
  }


}
