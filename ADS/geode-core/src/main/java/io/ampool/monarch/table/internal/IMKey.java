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

/**
 * An interface for key for the data to be stored in table. For any object to be stored as key
 * should implement this.
 *
 */
public interface IMKey extends Comparable<IMKey> {
  byte[] getBytes();

  /**
   * Get bytes from a key.. added as a central place to convert key into bytes.
   *
   * @param key the key
   * @return an array of bytes for the key
   */
  static byte[] getBytes(final Object key) {
    if (key instanceof IMKey) {
      return ((IMKey) key).getBytes();
    } else if (key instanceof byte[]) {
      return (byte[]) key;
    }
    return null;
  }
}
