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
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.types.BasicTypes;

/**
 * The unit of storage in MTable consisting of the following fields: <br>
 * 
 * <pre>
 * 1) column name
 * 2) column value
 * 3) column type ({@link BasicTypes#BINARY} by default)
 * </pre>
 * 
 * @since 0.2.0.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Cell {

  /**
   * Return the column Name
   * 
   * @return column Name
   */
  byte[] getColumnName();

  /**
   *
   * @return the type of the column see {@link DataType}
   */
  DataType getColumnType();

  /**
   * Return the column value
   * 
   * @return column value
   */
  Object getColumnValue();

  /**
   * @return The array containing the value bytes.
   */
  byte[] getValueArray();

  /**
   * @return Array index of first value byte
   */
  int getValueOffset();

  /**
   * @return Number of value bytes.
   */
  int getValueLength();
}
