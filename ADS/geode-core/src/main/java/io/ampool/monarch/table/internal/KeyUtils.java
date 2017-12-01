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

import io.ampool.classification.InterfaceAudience;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Pair;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * For internal use only. Generates keys for uniform distribution in ORDERED VERSIONED Table.
 */
@InterfaceAudience.Private
public class KeyUtils {
  public static Map<Integer, List<byte[]>> getKeysForAllBuckets(int numOfPartitions,
      int numOfKeysEachPartition) {
    Map<Integer, Pair<byte[], byte[]>> splitsMap =
        MTableUtils.getUniformKeySpaceSplit(numOfPartitions);
    Map<Integer, List<byte[]>> bucket2KeysMap = new HashMap<Integer, List<byte[]>>();

    splitsMap.forEach((K, V) -> {
      List<byte[]> keysInRange =
          getKeysInRange(V.getFirst(), V.getSecond(), numOfKeysEachPartition);
      bucket2KeysMap.put(K, keysInRange);
    });

    return bucket2KeysMap;

  }

  public static List<byte[]> getKeysInRange(byte[] start, byte[] stop, int numOfKeys) {
    byte[] aPadded;
    byte[] bPadded;
    Set<byte[]> result = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    if (start.length < stop.length) {
      aPadded = Bytes.padTail(start, stop.length - start.length);
      bPadded = stop;
    } else if (stop.length < start.length) {
      aPadded = start;
      bPadded = Bytes.padTail(stop, start.length - stop.length);
    } else {
      aPadded = start;
      bPadded = stop;
    }
    if (Bytes.compareTo(aPadded, bPadded) >= 0) {
      throw new IllegalArgumentException("b <= a");
    }
    if (numOfKeys <= 0) {
      throw new IllegalArgumentException("numOfKeys cannot be <= 0");
    }

    byte[] prependHeader = {1, 0};
    final BigInteger startBI = new BigInteger(Bytes.add(prependHeader, aPadded));
    final BigInteger stopBI = new BigInteger(Bytes.add(prependHeader, bPadded));

    BigInteger diffBI = stopBI.subtract(startBI);
    long difference = diffBI.longValue();
    if (diffBI.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0) {
      difference = Long.MAX_VALUE;
    }
    byte[] padded = null;
    for (int i = 0; i < numOfKeys; i++) {
      do {
        BigInteger keyBI = startBI.add(BigInteger.valueOf(nextLong(0, difference)));
        padded = keyBI.toByteArray();
        if (padded[1] == 0) {
          padded = Bytes.tail(padded, padded.length - 2);
        } else {
          padded = Bytes.tail(padded, padded.length - 1);
        }
      } while (!result.add(padded));
    }
    return new ArrayList<byte[]>(result);
  }

  public static long nextLong(final long lower, final long upper) throws IllegalArgumentException {
    Random rand = new Random();
    if (lower >= upper) {
      throw new IllegalArgumentException("Wrong Input");
    }
    final long max = (upper - lower) + 1;
    if (max <= 0) {
      while (true) {
        final long r = rand.nextLong();
        if (r >= lower && r <= upper) {
          return r;
        }
      }
    } else if (max < Integer.MAX_VALUE) {
      return lower + rand.nextInt((int) max);
    } else {
      return lower + nextLong(max);
    }
  }

  private static long nextLong(final long n) throws IllegalArgumentException {
    Random rand = new Random();
    if (n > 0) {
      final byte[] byteArray = new byte[8];
      long bits;
      long val;
      do {
        rand.nextBytes(byteArray);
        bits = 0;
        for (final byte b : byteArray) {
          bits = (bits << 8) | (((long) b) & 0xffL);
        }
        bits &= 0x7fffffffffffffffL;
        val = bits % n;
      } while (bits - val + (n - 1) < 0);
      return val;
    }
    throw new IllegalArgumentException("something wrong ");
  }
}
