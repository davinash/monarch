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

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;


@InterfaceAudience.Private
@InterfaceStability.Stable
public enum ServerScanStatus {
  BATCH_COMPLETED("BATCH_COMPLETED"), BUCKET_ENDED("BUCKET_ENDED"), SCAN_COMPLETED(
      "SCAN_COMPLETED"), BUCKET_MOVED("BUCKET_MOVED"),;

  private String status;

  ServerScanStatus(String status) {
    this.status = status;
  }

  public byte[] getStatusBytes() {
    return Bytes.toBytes(status);
  }

  public static ServerScanStatus getServerStatus(byte[] serverStatus) {
    String serverStatusString = Bytes.toString(serverStatus);
    ServerScanStatus serverScanStatus = ServerScanStatus.valueOf(serverStatusString);
    return serverScanStatus;
  }

  @Override
  public String toString() {
    return "ServerScanStatus{" + "status='" + status + '\'' + '}';
  }
}
