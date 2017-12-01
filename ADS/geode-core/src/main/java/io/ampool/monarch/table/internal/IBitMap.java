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

public interface IBitMap {

  void set(int index);

  void set(int index, boolean value);

  void unSet(int index);

  boolean get(int index);

  void or(IBitMap bitMap);

  void and(IBitMap bitMap);

  void andNot(IBitMap bitMap);

  int sizeOfByteArray();

  byte[] toByteArray();

  void clear();

  int capacity();

  // for now adding it
  int cardinality();
}
