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
package io.ampool.monarch.table.cdc;

import io.ampool.monarch.table.MEventOperation;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.Row;

import java.io.Serializable;
import java.util.Arrays;

public class EventData implements Serializable {
  public byte[] rowKey = null;
  /* public MTable table = null; */
  public MTableDescriptor tableDescriptor = null;
  public MEventOperation operation = null;
  public Row row = null;
  public boolean possibleDuplicate = false;


  public EventData(byte[] key, /* MTable mTable, */ MTableDescriptor mTableDescriptor,
      MEventOperation operation, Row row, boolean possibleDuplicate) {
    this.rowKey = key;
    /* this.table = mTable; */
    this.tableDescriptor = mTableDescriptor;
    this.operation = operation;
    this.row = row;
    this.possibleDuplicate = possibleDuplicate;
  }

  @Override
  public String toString() {
    return "EventData{" + "rowKey=" + Arrays.toString(rowKey) + ", tableDescriptor="
        + tableDescriptor + ", operation=" + operation + ", row=" + row + ", possibleDuplicate="
        + possibleDuplicate + '}';
  }
}
