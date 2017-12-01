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
package io.ampool.monarch.table.region;

import java.util.List;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.ftable.internal.FTableKey;
import io.ampool.monarch.table.internal.MValue;
import org.apache.geode.internal.cache.EventIDHolder;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

public class FTableByteUtils {

  public static Object getKey(final FTableKey key, final Object value,
      final ClientProxyMembershipID proxyID, final EventIDHolder clientEvent) {
    return key;
  }


  public static Object getValue(final Object key, final Object value) {
    if (value instanceof byte[]) {
      return getValue(key, value);
    } else if (value instanceof VMCachedDeserializable) {
      MValue v = (MValue) ((VMCachedDeserializable) value).getDeserializedForReading();
      return getValue(key, v.getValue());
    }
    return value;
  }

  /**
   * Create the value byte[] from {@link io.ampool.monarch.table.ftable.Record}
   *
   * @param td the table descriptor
   * @param record an existing put object
   * @return byte[] value with header
   */
  public static byte[] fromRecord(final FTableDescriptor td, final Record record) {
    if (td == null || td.getNumOfColumns() == 0 || record == null
        || record.getValueMap().isEmpty()) {
      return null;
    }
    return (byte[]) td.getEncoding().serializeValue(td, record);
  }


  public static byte[] extractSelectedColumns(final byte[] data,
      final List<Integer> columnPositions) {
    if (columnPositions.size() == 0)
      return data;
    byte[] trimmedData = new byte[data.length];
    int dataWritePos = 0;
    int dataReadPosition = 0;

    int newLength = 0;
    int columnsTraversed = 0;

    while (dataReadPosition < data.length) {
      final int colLength = Bytes.toInt(data, dataReadPosition);
      if (columnPositions.contains(columnsTraversed)) {
        // copy length
        Bytes.putBytes(trimmedData, dataWritePos, data, dataReadPosition, Bytes.SIZEOF_INT);
        dataWritePos += Bytes.SIZEOF_INT;
        dataReadPosition += Bytes.SIZEOF_INT;

        // copy value
        Bytes.putBytes(trimmedData, dataWritePos, data, dataReadPosition, colLength);
        dataWritePos += colLength;
        dataReadPosition += colLength;
        newLength += Bytes.SIZEOF_INT + colLength;
      } else {
        dataReadPosition += Bytes.SIZEOF_INT + colLength;
      }
      columnsTraversed++;
    }
    byte[] retArr = new byte[newLength];
    Bytes.putBytes(retArr, 0, trimmedData, 0, newLength);
    return retArr;
  }
}
