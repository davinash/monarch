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

import io.ampool.monarch.table.CDCConfig;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CDCConfigImpl implements CDCConfig, DataSerializable {

  private static final long serialVersionUID = 2266411345563167934L;
  private static final boolean DEFAULT_PERSISTENT = true;
  private static final int DEFAULT_MEMORY = 100;
  private static final boolean DEFAULT_DISK_SYNCHRONOUS = true;
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final int DEFAULT_BATCH_INTERVAL = 5;
  private static final boolean DEFAULT_CONFLATION = false;
  private static final int DEFAULT_DISPATCHER_THREADS = 5;

  String diskStoreName;
  int memory;
  boolean diskSynchronous;
  int batchSize;
  int batchInterval;
  boolean isPersistent;
  boolean isConflation;
  int dispatcherThreads;

  public CDCConfigImpl() {
    diskStoreName = null;
    memory = DEFAULT_MEMORY;
    diskSynchronous = DEFAULT_DISK_SYNCHRONOUS;
    batchSize = DEFAULT_BATCH_SIZE;
    batchInterval = DEFAULT_BATCH_INTERVAL;
    isPersistent = DEFAULT_PERSISTENT;
    isConflation = DEFAULT_CONFLATION;
    dispatcherThreads = DEFAULT_DISPATCHER_THREADS;
  }

  @Override
  public CDCConfig setDiskStoreName(String name) {
    this.diskStoreName = name;
    return this;
  }

  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  @Override
  public CDCConfig setMaximumQueueMemory(int memory) {
    this.memory = memory;
    return this;
  }

  public int getMaximumQueueMemory() {
    return this.memory;
  }

  @Override
  public CDCConfig setDiskSynchronous(boolean isSynchronous) {
    this.diskSynchronous = isSynchronous;
    return this;
  }

  public boolean getDiskSynchronous() {
    return this.diskSynchronous;
  }

  @Override
  public CDCConfig setBatchSize(int size) {
    this.batchSize = size;
    return this;
  }

  public int getBatchSize() {
    return this.batchSize;
  }

  @Override
  public CDCConfig setBatchTimeInterval(int interval) {
    this.batchInterval = interval;
    return this;
  }

  public int getBatchTimeInterval() {
    return this.batchInterval;
  }

  @Override
  public CDCConfig setPersistent(boolean isPersistent) {
    this.isPersistent = isPersistent;
    return this;
  }

  public boolean getPersistent() {
    return this.isPersistent;
  }

  @Override
  public CDCConfig setBatchConflationEnabled(boolean isConflation) {
    this.isConflation = isConflation;
    return this;
  }

  public boolean getBatchConflationEnabled() {
    return this.isConflation;
  }

  @Override
  public CDCConfig setDispatcherThreads(int numThreads) {
    this.dispatcherThreads = numThreads;
    return this;
  }

  public int getDispatcherThreads() {
    return this.dispatcherThreads;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.diskStoreName, out);
    DataSerializer.writeInteger(this.memory, out);
    DataSerializer.writeBoolean(this.diskSynchronous, out);
    DataSerializer.writeInteger(this.batchSize, out);
    DataSerializer.writeInteger(this.batchInterval, out);
    DataSerializer.writeBoolean(this.isPersistent, out);
    DataSerializer.writeBoolean(this.isConflation, out);
    DataSerializer.writeInteger(this.dispatcherThreads, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.diskStoreName = DataSerializer.readString(in);
    this.memory = DataSerializer.readInteger(in);
    this.diskSynchronous = DataSerializer.readBoolean(in);
    this.batchSize = DataSerializer.readInteger(in);
    this.batchInterval = DataSerializer.readInteger(in);
    this.isPersistent = DataSerializer.readBoolean(in);
    this.isConflation = DataSerializer.readBoolean(in);
    this.dispatcherThreads = DataSerializer.readInteger(in);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof CDCConfigImpl)) {
      return false;
    }

    CDCConfigImpl that = (CDCConfigImpl) other;

    return (this.diskStoreName == null ? that.diskStoreName == null
        : this.diskStoreName.equals(that.diskStoreName)) && this.memory == that.memory
        && this.diskSynchronous == that.diskSynchronous && this.batchSize == that.batchSize
        && this.batchInterval == that.batchInterval && this.isPersistent == that.isPersistent
        && this.isConflation == that.isConflation
        && this.dispatcherThreads == that.dispatcherThreads;
  }

  @Override
  public String toString() {
    return "CDCConfigImpl{" + "diskStoreName='" + diskStoreName + '\'' + ", memory=" + memory
        + ", diskSynchronous=" + diskSynchronous + ", batchSize=" + batchSize + ", batchInterval="
        + batchInterval + ", isPersistent=" + isPersistent + ", isConflation=" + isConflation
        + ", dispatcherThreads=" + dispatcherThreads + '}';
  }
}
