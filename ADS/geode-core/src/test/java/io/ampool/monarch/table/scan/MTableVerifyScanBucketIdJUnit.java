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
package io.ampool.monarch.table.scan;

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.internal.MTableUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Category(MonarchTest.class)
public class MTableVerifyScanBucketIdJUnit {

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceContinuous() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("011"), Bytes.toBytes("020")));
    map.put(1, new Pair<>(Bytes.toBytes("021"), Bytes.toBytes("030")));
    map.put(2, new Pair<>(Bytes.toBytes("031"), Bytes.toBytes("040")));
    map.put(3, new Pair<>(Bytes.toBytes("041"), Bytes.toBytes("050")));
    return map;
  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceNonContinuous() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("010"), Bytes.toBytes("020")));
    map.put(1, new Pair<>(Bytes.toBytes("030"), Bytes.toBytes("040")));
    map.put(2, new Pair<>(Bytes.toBytes("046"), Bytes.toBytes("050")));
    return map;
  }

  public static List<Pair<byte[], byte[]>> startEndKeys = new ArrayList<>();

  static {
    // Middle to greater
    startEndKeys.add(new Pair<>(Bytes.toBytes("012"), Bytes.toBytes("060")));
    // Less to Middle
    startEndKeys.add(new Pair<>(Bytes.toBytes("005"), Bytes.toBytes("045")));
    // Both Lesser
    startEndKeys.add(new Pair<>(Bytes.toBytes("005"), Bytes.toBytes("009")));
    // Both within
    startEndKeys.add(new Pair<>(Bytes.toBytes("025"), Bytes.toBytes("045")));
    // All inclusive
    startEndKeys.add(new Pair<>(Bytes.toBytes("005"), Bytes.toBytes("060")));
    // Both greater
    startEndKeys.add(new Pair<>(Bytes.toBytes("055"), Bytes.toBytes("060")));
  }

  @Test
  public void testBuckedIdContiguousRange() {
    List<Pair<Integer, Integer>> expectedBucketIds = new ArrayList<>();
    expectedBucketIds.add(new Pair<>(0, 3));
    expectedBucketIds.add(new Pair<>(0, 3));
    expectedBucketIds.add(new Pair<>(0, -1));
    expectedBucketIds.add(new Pair<>(1, 3));
    expectedBucketIds.add(new Pair<>(0, 3));
    expectedBucketIds.add(new Pair<>(-1, 3));

    for (int i = 0; i < startEndKeys.size(); i++) {
      System.out.println("Testing for contiguous i = " + i);
      Pair<byte[], byte[]> startEndKey = startEndKeys.get(i);
      Integer startBucketId =
          MTableUtils.getStartBucketId(startEndKey.getFirst(), getKeySpaceContinuous());
      Integer endBucketId =
          MTableUtils.getEndBucketId(startEndKey.getSecond(), getKeySpaceContinuous());
      Pair<Integer, Integer> integerIntegerPair = expectedBucketIds.get(i);
      Assert.assertEquals(integerIntegerPair.getFirst(), startBucketId);
      Assert.assertEquals(integerIntegerPair.getSecond(), endBucketId);
    }
  }

  @Test
  public void testBuckedIdNonContiguousRange() {
    List<Pair<Integer, Integer>> expectedBucketIds = new ArrayList<>();
    expectedBucketIds.add(new Pair<>(0, 2));
    expectedBucketIds.add(new Pair<>(0, 1));
    expectedBucketIds.add(new Pair<>(0, -1));
    expectedBucketIds.add(new Pair<>(1, 1));
    expectedBucketIds.add(new Pair<>(0, 2));
    expectedBucketIds.add(new Pair<>(-1, 2));

    for (int i = 0; i < startEndKeys.size(); i++) {
      System.out.println("Testing for non-contiguous i = " + i);
      Pair<byte[], byte[]> startEndKey = startEndKeys.get(i);
      Integer startBucketId =
          MTableUtils.getStartBucketId(startEndKey.getFirst(), getKeySpaceNonContinuous());
      Integer endBucketId =
          MTableUtils.getEndBucketId(startEndKey.getSecond(), getKeySpaceNonContinuous());
      Pair<Integer, Integer> integerIntegerPair = expectedBucketIds.get(i);
      Assert.assertEquals(integerIntegerPair.getFirst(), startBucketId);
      Assert.assertEquals(integerIntegerPair.getSecond(), endBucketId);
    }
  }

}
