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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataSerializable;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class ByteArrayKey implements VersionedDataSerializable {

  private static final long serialVersionUID = -43885869425873512L;
  private byte[] data;

  public ByteArrayKey(byte[] key) {
    this.data = key;
  }

  public ByteArrayKey() {}

  public byte[] getByteArray() {
    return this.data;
  }

  public int hashCode() {
    return Arrays.hashCode(data);
  }

  public boolean equals(Object other) {
    return other instanceof ByteArrayKey && Arrays.equals(data, ((ByteArrayKey) other).data);
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(this.data, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.data = DataSerializer.readByteArray(in);
  }

  @Override
  public String toString() {
    return "ByteArrayKey{" + "data=" + Arrays.toString(data) + '}';
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
