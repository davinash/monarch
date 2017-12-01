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

/**
 * This class holds the following information about CDC stream. 1) CDC Stream ID - An unique id 2)
 * Listner implementation - Fully qualified class path for implementation of
 * {@link io.ampool.monarch.table.MAsyncEventListener} 3) CDC configuration - User configurations
 * {@link CDCConfig}
 */
public class CDCInformation implements DataSerializable {
  private static final long serialVersionUID = -2894734152403824870L;
  private String queueId = null;
  private String listenerClassPath = null;
  private CDCConfigImpl cdcConfig;

  /**
   * Constructor. No-arg constructor for data serialization.
   *
   */
  public CDCInformation() {

  }

  public CDCInformation(String id, String listenerclassPath, CDCConfig config) {
    this.queueId = id;
    this.listenerClassPath = listenerclassPath;
    this.cdcConfig = (CDCConfigImpl) config;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.queueId, out);
    DataSerializer.writeString(this.listenerClassPath, out);
    DataSerializer.writeObject(this.cdcConfig, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.queueId = DataSerializer.readString(in);
    this.listenerClassPath = DataSerializer.readString(in);
    this.cdcConfig = DataSerializer.readObject(in);
  }

  public String getQueueId() {
    return this.queueId;
  }

  public String getListenerClassPath() {
    return this.listenerClassPath;
  }

  public CDCConfig getCDCConfig() {
    return this.cdcConfig;
  }

  @Override
  public String toString() {
    return "CDCInformation{" + "queueId='" + queueId + '\'' + ", listenerClassPath='"
        + listenerClassPath + '\'' + ", cdcConfig=" + cdcConfig + '}';
  }
}
