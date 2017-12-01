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

import org.apache.geode.test.junit.categories.MonarchTest;
import io.ampool.monarch.table.exceptions.TableInvalidConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

@Category(MonarchTest.class)
public class MTableVerifyKeySpaceAPI {

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceContinuousGood() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("000"), Bytes.toBytes("010")));
    map.put(1, new Pair<>(Bytes.toBytes("011"), Bytes.toBytes("020")));
    map.put(2, new Pair<>(Bytes.toBytes("021"), Bytes.toBytes("030")));
    map.put(3, new Pair<>(Bytes.toBytes("031"), Bytes.toBytes("040")));
    map.put(3, new Pair<>(Bytes.toBytes("041"), Bytes.toBytes("041")));
    return map;
  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceNonContinuousGood() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("010"), Bytes.toBytes("020")));
    map.put(1, new Pair<>(Bytes.toBytes("030"), Bytes.toBytes("040")));
    map.put(2, new Pair<>(Bytes.toBytes("046"), Bytes.toBytes("050")));
    return map;
  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceMissingBuckets() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("000"), Bytes.toBytes("010")));
    map.put(1, new Pair<>(Bytes.toBytes("011"), Bytes.toBytes("020")));
    map.put(3, new Pair<>(Bytes.toBytes("030"), Bytes.toBytes("040")));
    return map;
  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceInvalidStartStop() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("010"), Bytes.toBytes("009")));
    map.put(1, new Pair<>(Bytes.toBytes("010"), Bytes.toBytes("020")));
    map.put(2, new Pair<>(Bytes.toBytes("030"), Bytes.toBytes("021")));
    map.put(3, new Pair<>(Bytes.toBytes("030"), Bytes.toBytes("040")));
    return map;
  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceNullStartStop1() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(null, Bytes.toBytes("010")));
    map.put(1, new Pair<>(Bytes.toBytes("011"), Bytes.toBytes("020")));
    map.put(2, new Pair<>(Bytes.toBytes("021"), Bytes.toBytes("030")));
    map.put(3, new Pair<>(Bytes.toBytes("031"), Bytes.toBytes("040")));
    return map;
  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceNullStartStop2() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("011"), null));
    map.put(1, new Pair<>(Bytes.toBytes("021"), Bytes.toBytes("030")));
    map.put(2, new Pair<>(Bytes.toBytes("031"), Bytes.toBytes("040")));
    return map;
  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceNullStartStop3() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("021"), Bytes.toBytes("030")));
    map.put(1, new Pair<>(null, null));
    map.put(2, new Pair<>(Bytes.toBytes("031"), Bytes.toBytes("040")));
    return map;
  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceOverlappingRanges1() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("000"), Bytes.toBytes("010")));
    map.put(1, new Pair<>(Bytes.toBytes("010"), Bytes.toBytes("020")));
    map.put(2, new Pair<>(Bytes.toBytes("020"), Bytes.toBytes("030")));
    map.put(3, new Pair<>(Bytes.toBytes("030"), Bytes.toBytes("040")));
    return map;
  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceOverlappingRanges2() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("000"), Bytes.toBytes("010")));
    map.put(1, new Pair<>(Bytes.toBytes("009"), Bytes.toBytes("020")));
    map.put(2, new Pair<>(Bytes.toBytes("019"), Bytes.toBytes("030")));
    map.put(3, new Pair<>(Bytes.toBytes("029"), Bytes.toBytes("040")));
    return map;
  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceDuplicate() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("000"), Bytes.toBytes("010")));
    map.put(1, new Pair<>(Bytes.toBytes("000"), Bytes.toBytes("010")));
    return map;
  }

  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceSubset() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("000"), Bytes.toBytes("020")));
    map.put(1, new Pair<>(Bytes.toBytes("011"), Bytes.toBytes("015")));
    return map;
  }


  private static Map<Integer, Pair<byte[], byte[]>> getKeySpaceIncorrectOrder() {
    Map<Integer, Pair<byte[], byte[]>> map = new java.util.HashMap<>();
    map.put(0, new Pair<>(Bytes.toBytes("011"), Bytes.toBytes("020")));
    map.put(1, new Pair<>(Bytes.toBytes("000"), Bytes.toBytes("010")));
    map.put(2, new Pair<>(Bytes.toBytes("021"), Bytes.toBytes("030")));
    map.put(3, new Pair<>(Bytes.toBytes("031"), Bytes.toBytes("040")));
    return map;
  }

  @Test
  public void testKeySpaceValidityChecks() {
    validateKeySpace(getKeySpaceContinuousGood());
    validateKeySpace(getKeySpaceNonContinuousGood());
    validateKeySpace(getKeySpaceDuplicate(),
        "Buckets(0,1): key ranges for buckets must not overlap");
    validateKeySpace(getKeySpaceIncorrectOrder(),
        "Buckets(0,1): key ranges for buckets must not overlap");
    validateKeySpace(getKeySpaceInvalidStartStop(),
        "Bucket 0:start of range must be <= end of range");
    validateKeySpace(getKeySpaceMissingBuckets(), "key range for bucket 2 not found in keyspace");
    validateKeySpace(getKeySpaceNullStartStop1(), "Bucket(0) start/stop keys must not be null");
    validateKeySpace(getKeySpaceNullStartStop2(), "Bucket(0) start/stop keys must not be null");
    validateKeySpace(getKeySpaceNullStartStop3(), "Bucket(1) start/stop keys must not be null");
    validateKeySpace(getKeySpaceOverlappingRanges1(),
        "Buckets(0,1): key ranges for buckets must not overlap");
    validateKeySpace(getKeySpaceOverlappingRanges2(),
        "Buckets(0,1): key ranges for buckets must not overlap");
    validateKeySpace(getKeySpaceSubset(), "Buckets(0,1): key ranges for buckets must not overlap");
  }

  private void validateKeySpace(Map<Integer, Pair<byte[], byte[]>> keySpace) {
    validateKeySpace(keySpace, null);
  }

  private void validateKeySpace(Map<Integer, Pair<byte[], byte[]>> keySpace, String errorMessage) {
    MTableDescriptor tableDescriptor = new MTableDescriptor();
    Exception e = null;
    try {
      tableDescriptor.setKeySpace(keySpace);
    } catch (TableInvalidConfiguration tableInvalidConfiguration) {
      e = tableInvalidConfiguration;
    }
    if (errorMessage != null) {
      Assert.assertNotNull(e);
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }
}
