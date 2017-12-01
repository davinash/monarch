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

import java.io.Serializable;

/**
 * The supported operations for Ampool.
 *
 */
public enum MOperation implements Serializable {
  NONE, GET, PUT, DELETE, CHECK_AND_PUT, CHECK_AND_DELETE, CREATE, UPDATE, APPEND;

  /**
   * Get the byte value, instead of integer, corresponding to the operation.
   *
   * @return the byte value representing the operation
   */
  public byte ordinalByte() {
    return (byte) ordinal();
  }

  /**
   * Get the respective operation for the specified byte value of ordinal. In case of unsupported
   * value, it throws an ArrayIndexOutOfBoundsException.
   *
   * @param ordinal the ordinal value as byte
   * @return the respective operation
   */
  public static MOperation valueOf(byte ordinal) {
    for (final MOperation op : MOperation.values()) {
      if ((byte) op.ordinal() == ordinal) {
        return op;
      }
    }
    throw new ArrayIndexOutOfBoundsException("Value should be in range: min= " + NONE.ordinal()
        + "; max= " + CHECK_AND_DELETE.ordinal());
  }
}
