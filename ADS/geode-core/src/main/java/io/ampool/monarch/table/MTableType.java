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

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * This class describes the type of the table. Type defines the order in which data will be stored.
 * If most of the operations are Put/Get then one should use UNORDERED type to get performance If
 * the opeations are SCAN oriented then one should use ORDERED_VERSIONED type. default type for
 * MTable is always ORDERED_VERSIONED.
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public enum MTableType implements Serializable {
  /**
   * Table Type ORDERED_VERSIONED make sure that data is stored sorted. Key for the table is of
   * byte[] and will be stored in lexicographic order of the same. This type of table also provides
   * selective get. Table can be queried using GET for selected list of columns.
   */
  ORDERED_VERSIONED("Ordered_Versioned"),
  /**
   * Table Type UNORDERED make sure that date is stored in hash-map. This type is suitable for
   * faster GETs, which is nearly equal to O(1). Type of the key is always byte[].This type of table
   * also provides selective get. Table can be queried using GET for selected list of columns. This
   * type of table does not support versioned information.
   */
  UNORDERED("UnOrdered");

  private final String name;

  MTableType(final String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return "MTableType{" + "name='" + name + '\'' + '}';
  }
}
