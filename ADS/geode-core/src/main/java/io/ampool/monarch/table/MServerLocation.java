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

package io.ampool.monarch.table;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.Arrays;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.internal.MTableUtils;

/**
 * Data Structure to hold details of Monarch Server location i.e. hostname and port. Also, holds
 * other information like bucketid, startkey and stopkey Comparable, compares hostname and port.
 * <p>
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MServerLocation implements Serializable, Comparable {

  private static final long serialVersionUID = 4137326857054062182L;
  private final String hostName;
  private final int port;
  // Adding bucketId, startkey and stopkey here for now.
  // or should we create bucketInfo class and add these 3 params there?
  /**
   * Bucket id of bucked on this server
   */
  private final int bucketId;
  /**
   * Start key for this bucket
   */
  private final byte[] startKey;
  /**
   * End key for this bucket
   */
  private final byte[] endKey;

  public MServerLocation(String hostName, int port, int bucketId, byte[] startKey, byte[] endKey) {
    this.hostName = hostName;
    this.port = port;
    this.bucketId = bucketId;
    this.startKey = startKey;
    this.endKey = endKey;
  }

  public String getHostName() {
    return hostName;
  }

  public int getPort() {
    return port;
  }

  public int getBucketId() {
    return bucketId;
  }

  public byte[] getStartKey() {
    return startKey;
  }

  public byte[] getEndKey() {
    return endKey;
  }

  @Override
  public int compareTo(Object o) {
    MServerLocation other = (MServerLocation) o;
    int difference = hostName.compareTo(other.hostName);
    if (difference != 0) {
      return difference;
    }
    return port - other.getPort();
  }

  @Override
  public int hashCode() {
    return this.hostName.hashCode() + this.port + this.bucketId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof MServerLocation))
      return false;
    final MServerLocation other = (MServerLocation) obj;
    if (hostName == null) {
      if (other.hostName != null)
        return false;
    } else if (other.hostName == null) {
      return false;
    } else if (!hostName.equals(other.hostName)) {
      String canonicalHostName;
      try {
        canonicalHostName = MTableUtils.getLocalHost().getCanonicalHostName();
      } catch (UnknownHostException e) {
        throw new IllegalStateException("getLocalHost failed with " + e);
      }
      if ("localhost".equals(hostName)) {
        if (!canonicalHostName.equals(other.hostName)) {
          return false;
        }
      } else if ("localhost".equals(other.hostName)) {
        if (!canonicalHostName.equals(hostName)) {
          return false;
        }
      } else {
        return false;
      }
    }
    if (port != other.port)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "MServerLocation{" + "hostName='" + hostName + '\'' + ", port=" + port + ", bucketId="
        + bucketId + ", startKey=" + Arrays.toString(startKey) + ", endKey="
        + Arrays.toString(endKey) + '}';
  }
}
