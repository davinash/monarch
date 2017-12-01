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
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.MServerLocation;
import io.ampool.monarch.table.Pair;
import org.apache.geode.internal.cache.MTableGetStartEndKeysFunction;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.ServerLocation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An implementation of {@link MTableLocationInfo}. Used to view location information of single
 * Monarch table.
 * <p>
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class MTableLocationInfoImpl implements MTableLocationInfo {

  private final String tableName;
  private final ProxyMTableRegion table;

  public MTableLocationInfoImpl(String tableName, ProxyMTableRegion mTable) {
    this.tableName = tableName;
    this.table = mTable;
  }

  @Override
  public byte[][] getStartKeys() {
    return getStartEndKeys().getFirst();
  }

  @Override
  public byte[][] getEndKeys() {
    return getStartEndKeys().getSecond();

  }

  @Override
  public Pair<byte[][], byte[][]> getStartEndKeys() {

    Function startEndKeysFunction = new MTableGetStartEndKeysFunction();
    FunctionService.registerFunction(startEndKeysFunction);

    java.util.List<Object> inputList = new java.util.ArrayList<Object>();
    inputList.add(tableName);
    Execution members = FunctionService.onRegion(table.getTableRegion()).withArgs(inputList);

    List<List<Pair<byte[], byte[]>>> allServerResp = (List<List<Pair<byte[], byte[]>>>) members
        .execute(startEndKeysFunction.getId()).getResult();

    List<byte[]> startKeyList = new ArrayList<>();
    List<byte[]> endKeyList = new ArrayList<>();

    allServerResp.forEach((P) -> {
      P.forEach((S) -> {
        startKeyList.add(S.getFirst());
        endKeyList.add(S.getSecond());
      });
    });

    return new Pair<byte[][], byte[][]>(startKeyList.toArray(new byte[startKeyList.size()][]),
        endKeyList.toArray(new byte[endKeyList.size()][]));
  }

  @Override
  public List<MServerLocation> getAllMTableLocations() {
    List<MServerLocation> allMTableLocations = new ArrayList<>();
    int numBuckets = table.getTableDescriptor().getTotalNumOfSplits();
    for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
      allMTableLocations.add(getMTableLocation(bucketId));
    }
    return allMTableLocations;
  }


  public MServerLocation getMTableLocation(int bucketId) {

    Set<ServerLocation> serverLocationForBucketId =
        MTableUtils.getServerLocationForBucketId(table, bucketId);

    ServerLocation serverLocation = null;
    if (serverLocationForBucketId != null && serverLocationForBucketId.size() > 0) {
      // Returning first location by default
      serverLocation = serverLocationForBucketId.iterator().next();
    }

    if (serverLocation == null) {
      return null;
      // throw new IllegalStateException("Table Split # " + bucketId + " Not found, Table is
      // empty");
    }

    Pair<byte[], byte[]> range =
        MTableUtils.getStartEndKeysOverKeySpace(this.table.getTableDescriptor(), bucketId);

    byte[] startKey = range.getFirst();
    byte[] stopKey = range.getSecond();
    return new MServerLocation(serverLocation.getHostName(), serverLocation.getPort(), bucketId,
        startKey, stopKey);
  }

  public MServerLocation getMTablePrimaryBucketLocation(int bucketId) {

    ServerLocation serverLocation =
        MTableUtils.getPrimaryServerLocationForBucketId(table, bucketId);

    if (serverLocation == null) {
      return null;
      // throw new IllegalStateException("Table Split # " + bucketId + " Not found, Table is
      // empty");
    }

    Pair<byte[], byte[]> range =
        MTableUtils.getStartEndKeysOverKeySpace(this.table.getTableDescriptor(), bucketId);

    byte[] startKey = range.getFirst();
    byte[] stopKey = range.getSecond();
    return new MServerLocation(serverLocation.getHostName(), serverLocation.getPort(), bucketId,
        startKey, stopKey);
  }

  @Override
  public MServerLocation getMTableLocation(byte[] row) {
    int bucketId = MTableUtils.getBucketId(row, table.getTableDescriptor());
    return getMTableLocation(bucketId);
  }


}
