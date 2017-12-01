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
package io.ampool.monarch.table.coprocessor.internal;

import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableRegion;
import io.ampool.monarch.table.coprocessor.MObserverContext;
import org.apache.geode.cache.EntryEvent;

public class MObserverContextImpl implements MObserverContext {
  private MTable table;
  private MTableRegion tableRegion;
  private boolean bypass;
  private EntryEvent event = null;

  MObserverContextImpl(MTable table) {
    this.table = table;
    this.bypass = false;
  }

  public MObserverContextImpl(MTable table, EntryEvent event) {
    this(table);
    this.event = event;
  }

  @Override
  public MTable getTable() {
    return this.table;
  }

  @Override
  public MTableRegion getTableRegion() {
    return this.tableRegion;
  }

  @Override
  public boolean getBypass() {
    return this.bypass;
  }

  @Override
  public void setBypass(boolean bypass) {
    this.bypass = true;
  }

  @Override
  public EntryEvent getEntryEvent() {
    return this.event;
  }


  public void setTable(MTable table) {
    this.table = table;
  }


  public void setTableRegion(MTableRegion tableRegion) {
    this.tableRegion = tableRegion;
  }
}
