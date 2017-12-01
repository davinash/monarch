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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class RowEndMarker implements Row {

  private static final long serialVersionUID = 8897742271346506026L;
  private final ServerScanStatus serverScanStatus;
  private final byte[] markerData;

  public RowEndMarker(byte[] serverScanStatus, byte[] markerData) {
    this.serverScanStatus = ServerScanStatus.getServerStatus(serverScanStatus);
    this.markerData = markerData;
  }

  public ServerScanStatus getServerScanStatus() {
    return serverScanStatus;
  }

  public byte[] getMarkerData() {
    return markerData;
  }

  @Override
  public List<Cell> getCells() {
    return null;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public Long getRowTimeStamp() {
    return null;
  }

  @Override
  public byte[] getRowId() {
    return markerData;
  }

  @Override
  public int compareTo(Row item) {
    return 0;
  }

  @Override
  public String toString() {
    return "MResultEndMarker{" + "serverScanStatus=" + serverScanStatus + ", markerData="
        + Arrays.toString(markerData) + '}';
  }

  /**
   * Should not be used
   */
  @Override
  public Map<Long, SingleVersionRow> getAllVersions() {
    return null;
  }

  /**
   * Should not be used
   */
  @Override
  public SingleVersionRow getVersion(final Long timestamp) {
    return null;
  }

  /**
   * Should not be used
   */
  @Override
  public SingleVersionRow getLatestRow() {
    return null;
  }

  /**
   * Get the raw value respective to this row.
   *
   * @return the raw value
   */
  @Override
  public Object getRawValue() {
    return null;
  }

}
