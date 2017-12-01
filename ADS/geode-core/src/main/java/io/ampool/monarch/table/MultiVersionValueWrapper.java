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

import io.ampool.classification.InterfaceAudience;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * This class used to send multi dimensional byte array in case of multi version table
 *
 */
@InterfaceAudience.Private
public class MultiVersionValueWrapper implements DataSerializable {
  private byte[][] val;

  public MultiVersionValueWrapper() {}

  public MultiVersionValueWrapper(byte[][] val) {
    this.val = val;
  }

  public byte[][] getVal() {
    return val;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeArrayOfByteArrays(val, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.val = DataSerializer.readArrayOfByteArrays(in);
  }
}
