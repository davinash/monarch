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

import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.TableDescriptor;

import java.util.List;
import java.util.Map;

/**
 * This class will have operation performed according to encoding scheme and based on how byte
 * buffer is stored.
 * <p>
 */
public interface StorageFormatter {

  Object performPutOperation(final TableDescriptor tableDescriptor, final MValue mValue,
      final MOpInfo mOpInfo, final Object oldValue);

  Object performCheckAndPutOperation(final TableDescriptor tableDescriptor, final MValue mValue,
      final MOpInfo opInfo, final Object oldValueBytes);

  Object performCheckAndDeleteOperation(TableDescriptor gTableDescriptor, MOpInfo opInfo,
      Object oldValueBytes);

  Object performDeleteOperation(TableDescriptor tableDescriptor, MOpInfo opInfo,
      Object oldValueBytes);

  Object performGetOperation(final MTableKey mTableKey, final TableDescriptor tableDescriptor,
      final Object rawByteValue, final Object aCallbackArgument);

  Map<Integer, Pair<Integer, Integer>> getOffsetsForEachColumn(TableDescriptor tableDescriptor,
      final byte[] oldValueBytes, List<Integer> selectiveCols);
}
