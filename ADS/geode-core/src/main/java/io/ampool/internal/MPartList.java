/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.ampool.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import io.ampool.monarch.table.DeSerializedRow;
import io.ampool.monarch.table.internal.InternalRow;
import io.ampool.monarch.table.region.ScanContext;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.sockets.CacheServerHelper;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.Releasable;
import org.apache.logging.log4j.Logger;

public class MPartList implements DataSerializableFixedID, Externalizable, Releasable {
  public MPartList() {
    //////
  }

  private static final Logger logger = LogService.getLogger();

  public static final byte BYTES = 0;
  public static final byte OBJECT = 1;
  public static final byte EXCEPTION = 2;
  public static final byte ROW = 3;
  public static final byte DES_ROW = 4;

  private byte[] typeArray;
  private Object[] objectArray;
  private Object[] keyArray;
  private boolean hasKeys;
  private int index = 0;
  private ScanContext sc;

  public MPartList(int maxSize, boolean hasKeys) {
    init(maxSize, hasKeys, null);
  }

  public MPartList(int maxSize, boolean hasKeys, final ScanContext sc) {
    init(maxSize, hasKeys, sc);
  }

  /**
   * Initialize the part-list.
   *
   * @param maxSize the maximum size of the part-list
   * @param hasKeys whether keys should be sent/retrieved via toData/fromData
   * @param sc the scan context
   */
  private void init(int maxSize, boolean hasKeys, final ScanContext sc) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException("Invalid size " + maxSize + " to MPartList constructor");
    }
    this.typeArray = new byte[maxSize];
    this.hasKeys = hasKeys;
    if (hasKeys) {
      this.keyArray = new Object[maxSize];
    }
    this.objectArray = new Object[maxSize];
    this.sc = sc;
  }

  public int size() {
    return index;
  }

  public void addExceptionPart(Object key, Exception ex) {
    addPart(key, ex, EXCEPTION, null);
  }

  public void addObjectPart(Object key, Object value, boolean isObject, VersionTag versionTag) {
    addPart(key, value, isObject ? OBJECT : BYTES, versionTag);
  }

  public void addPart(Object key, Object value, byte objectType, VersionTag versionTag) {
    int maxSize = this.typeArray.length;
    if (index >= maxSize) {
      throw new IndexOutOfBoundsException("Cannot add object part beyond " + maxSize + " elements");
    }
    if (this.hasKeys) {
      if (key == null) {
        throw new IllegalArgumentException("Cannot add null key");
      }
      this.keyArray[index] = key;
    }
    this.typeArray[index] = objectType;
    this.objectArray[index] = value;
    index++;
  }

  public Object getKey(int i) {
    return this.keyArray[i];
  }

  public Object getValue(int i) {
    return this.objectArray[i];
  }

  /**
   * Drain the latest value from the list as tuple: type, key, value. It removes the respective
   * value from the list making space available.
   *
   * @return an array with: type, key, and value
   */
  public Object[] drainValue() {
    if (this.index > 0) {
      int last = this.index - 1;
      final Object[] ret = new Object[] {typeArray[last], keyArray[last], objectArray[last]};
      this.objectArray[last] = null;
      this.typeArray[last] = 0;
      this.keyArray[last] = null;
      this.index--;
      return ret;
    }
    return null;
  }

  public void clear() {
    for (int i = 0; i < index; i++) {
      OffHeapHelper.release(this.objectArray[i]);
      this.objectArray[i] = null;
      if (hasKeys) {
        this.keyArray[i] = null;
      }
    }
    index = 0;
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  @Override
  public int getDSFID() {
    return AMPL_M_PART_LIST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeBoolean(this.hasKeys);
    if (this.typeArray != null) {
      byte[] bytes;
      out.writeInt(index);
      for (int i = 0; i < this.index; ++i) {
        Object value = this.objectArray[i];
        byte objectType = this.typeArray[i];
        if (this.hasKeys) {
          if (keyArray[i] == null) {
            MPartList.writeLength(-1, out);
          } else {
            bytes = (byte[]) keyArray[i];
            MPartList.writeLength(bytes.length, out);
            out.write(bytes);
          }
        }
        if (objectType == ROW) {
          out.writeByte(BYTES);
          ((InternalRow) value).writeSelectedColumns(out, sc.getScan().getColumns());
        } else if (objectType == DES_ROW) {
          out.writeByte(BYTES);
          sc.getTableDescriptor().getEncoding().writeDesRow(out, sc.getTableDescriptor(),
                  (DeSerializedRow) value, sc.getScan().getColumns());
        } else if (objectType == BYTES && value instanceof byte[]) {
          out.writeByte(BYTES);
          writeBytes(out, value);
        } else if (objectType == EXCEPTION) {
          out.writeByte(EXCEPTION);
          // write exception as byte array so native clients can skip it
          DataSerializer.writeByteArray(CacheServerHelper.serialize(value), out);
          // write the exception string for native clients
          DataSerializer.writeString(value.toString(), out);
        } else {
          out.writeByte(OBJECT);
          DataSerializer.writeObject(value, out);
        }
      }
    } else {
      out.writeInt(0);
    }
  }

  /**
   * Write an integer row length to the provided output.
   *
   * @param length row length
   * @param out the data-output
   * @throws IOException if failed to write to the output
   */
  public static void writeLength(final long length, final DataOutput out) throws IOException {
    InternalDataSerializer.writeSignedVL(length, out);
  }

  /**
   * Read an integer row length from the provided input.
   *
   * @param in the data input
   * @return row length
   * @throws IOException if failed to read from the input
   */
  public static long readLength(final DataInput in) throws IOException {
    return InternalDataSerializer.readSignedVL(in);
  }

  private static void writeBytes(final DataOutput out, final Object value) throws IOException {
    if (value == null) {
      MPartList.writeLength(-1, out);
    } else {
      final byte[] bytes = (byte[]) value;
      MPartList.writeLength(bytes.length, out);
      out.write(bytes);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    long l;
    if (in == null) {
      throw new NullPointerException("Null DataInput");
    }
    this.hasKeys = in.readBoolean();
    this.index = in.readInt();
    this.typeArray = new byte[index];
    this.keyArray = new Object[index];
    this.objectArray = new Object[index];
    byte objectType;
    Object value;
    int length;
    byte[] bytes;
    for (int i = 0; i < this.index; i++) {
      if (this.hasKeys) {
        length = (int) MPartList.readLength(in);
        bytes = new byte[length];
        in.readFully(bytes, 0, length);
        this.keyArray[i] = bytes;
      }
      objectType = in.readByte();
      if (objectType == BYTES) {
        length = (int) MPartList.readLength(in);
        if (length >= 0) {
          bytes = new byte[length];
          in.readFully(bytes, 0, length);
        } else {
          bytes = null;
        }
        value = bytes;
      } else if (objectType == OBJECT) {
        value = DataSerializer.readObject(in);
      } else if (objectType == EXCEPTION) {
        byte[] exBytes = DataSerializer.readByteArray(in);
        value = CacheServerHelper.deserialize(exBytes);
        // ignore the exception string meant for native clients
        DataSerializer.readString(in);
      } else {
        value = null;
      }
      this.objectArray[i] = value;
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {}

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {}

  /**
   * Release any off-heap data owned by this instance.
   */
  @Override
  public void release() {
    //// stub.. actual release happens in clear
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
