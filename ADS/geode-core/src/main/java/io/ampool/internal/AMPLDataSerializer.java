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
package io.ampool.internal;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class AMPLDataSerializer extends DataSerializer {

  private static final Logger logger = LogService.getLogger();

  /**
   * Writes a <code>LinkedHashMap</code> to a <code>DataOutput</code>. Note that even though
   * <code>map</code> may be an instance of a subclass of <code>LinkedHashMap</code>,
   * <code>readLinkedHashMap</code> will always return an instance of <code>LinkedHashMap</code>,
   * <B>not</B> an instance of the subclass. To preserve the class type of <code>map</code>,
   * {@link #writeObject(Object, DataOutput)} should be used for data serialization. This method
   * will serialize a <code>null</code> map and not throw a <code>NullPointerException</code>.
   *
   * @throws IOException A problem occurs while writing to <code>out</code>
   * @see #readLinkedHashMap
   */
  public static void writeLinkedHashMap(Map<?, ?> map, DataOutput out) throws IOException {

    InternalDataSerializer.checkOut(out);

    int size;
    if (map == null) {
      size = -1;
    } else {
      size = map.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing LinkedHashMap with {} elements: {}", size, map);
    }
    if (size > 0) {
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        writeObject(entry.getKey(), out);
        writeObject(entry.getValue(), out);
      }
    }
  }

  /**
   * Reads a <code>LinkedHashMap</code> from a <code>DataInput</code>.
   *
   * @throws IOException A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException The class of one of the <Code>HashMap</code>'s elements cannot
   *         be found.
   * @see #writeLinkedHashMap
   */
  public static <K, V> LinkedHashMap<K, V> readLinkedHashMap(DataInput in)
      throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      LinkedHashMap<K, V> map = new LinkedHashMap<>(size);
      for (int i = 0; i < size; i++) {
        K key = DataSerializer.<K>readObject(in);
        V value = DataSerializer.<V>readObject(in);
        map.put(key, value);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read LinkedHashMap with {} elements: {}", size, map);
      }

      return map;
    }
  }
}
