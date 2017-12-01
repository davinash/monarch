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

package org.apache.geode.internal.cache;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Pair;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MKeyBase;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.region.RowTuplePartitionResolver;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * MTableRangePartitionResolver is class which describes the range partition scheme of the table. It
 * has borrowed the split mechanism from HBase. which divides the byte[] range keyspace into almost
 * equal number of buckets.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MTableRangePartitionResolver
    implements RowTuplePartitionResolver, Declarable, DataSerializable {
  /* using DataSerializableFixedID causes test failures in SessionReplicationIntegrationJUnitTest */
  private static final Logger logger = LogService.getLogger();

  /**
   * Total number of buckes into which byte[] range keyspace should be divided .
   */
  private int totalNumberOfBuckets;

  /**
   * Optional key ranges for region split.
   */
  private byte[] startRangeKey;
  private byte[] stopRangeKey;

  /**
   * Internal map which holds the mapping Interger vs Pair of <START_KEY> <END_KEY> Integer here
   * defines the actual bucketId. Apache Geode BucketIds are 0 based.
   */
  private Map<Integer, Pair<byte[], byte[]>> splitsMap = null;
  /* TODO: remove usage of splitsMap.. only lookup map should be used */
  /* a faster way (log n) to retrieve the key to bucket-id mapping */
  private TreeMap<IMKey, Object[]> lookupMap = new TreeMap<>();

  /**
   * Default constructor called by Apache Geode client from
   * ClientPartitionAdvisor::ClientPartitionAdvisor
   */
  public MTableRangePartitionResolver() {}

  /**
   * Constructured during Table(Region) Creation.
   * 
   * @param numOfPartitions
   */
  public MTableRangePartitionResolver(final int numOfPartitions) {
    this.totalNumberOfBuckets = numOfPartitions;
    this.startRangeKey = null;
    this.stopRangeKey = null;
    this.splitsMap = MTableUtils.getUniformKeySpaceSplit(this.totalNumberOfBuckets);
    initLookupMap(this.splitsMap);
  }

  private void initLookupMap(final Map<Integer, Pair<byte[], byte[]>> map) {
    byte[] endKey;
    byte[] begKey;
    for (Map.Entry<Integer, Pair<byte[], byte[]>> entry : map.entrySet()) {
      if (entry.getValue() == null) {
        begKey = EMPTY_KEY;
        endKey = MAX_KEY;
      } else {
        begKey = entry.getValue().getFirst();
        endKey = entry.getValue().getSecond();
      }
      lookupMap.put(new MKeyBase(begKey), new Object[] {entry.getKey(), endKey});
    }
  }

  private static final byte[] EMPTY_KEY = new byte[0];
  private static final byte F = (byte) 0xFF;
  private static final byte[] MAX_KEY = new byte[] {F, F, F, F, F, F, F, F};

  public MTableRangePartitionResolver(final int numOfPartitions, final byte[] startRangeKey,
      final byte[] stopRangeKey, final Map<Integer, Pair<byte[], byte[]>> keySpace) {
    this.totalNumberOfBuckets = numOfPartitions;
    this.startRangeKey = startRangeKey;
    this.stopRangeKey = stopRangeKey;
    if ((this.startRangeKey != null) && (this.stopRangeKey != null)) {
      if (Bytes.compareTo(this.startRangeKey, this.stopRangeKey) >= 0) {
        throw new IllegalArgumentException("start of range must be <= end of range");
      }
    }
    if (keySpace == null) {
      this.splitsMap = MTableUtils.getUniformKeySpaceSplit(this.totalNumberOfBuckets,
          this.startRangeKey, this.stopRangeKey);
    } else {
      this.splitsMap = keySpace;
    }
    initLookupMap(this.splitsMap);
  }

  /**
   * Return the bucketId based on the range we have defined. If this function return null, then we
   * have goofed up big time and we need to re-think the strategy of partitioning the byte[]
   * keyspace.
   * 
   * @param opDetails the detail of the entry operation e.g.
   * @return returns the bucketId based on the range defined.
   */
  @Override
  public Object getRoutingObject(EntryOperation opDetails) {
    Object key = opDetails.getKey();
    return key instanceof IMKey ? getInternalRO(((IMKey) key).getBytes()) : null;
  }

  public Object getInternalRO(byte[] keyByteArray) {
    if (this.lookupMap == null || this.lookupMap.isEmpty()) {
      Map<Integer, Pair<byte[], byte[]>> ks;
      ks = MTableUtils.getUniformKeySpaceSplit(this.totalNumberOfBuckets, this.startRangeKey,
          this.stopRangeKey);
      initLookupMap(ks);
    }

    Map.Entry<IMKey, Object[]> floorEntry = this.lookupMap.floorEntry(new MKeyBase(keyByteArray));
    return floorEntry == null
        || Bytes.compareToPrefix(keyByteArray, (byte[]) floorEntry.getValue()[1]) > 0 ? null
            : floorEntry.getValue()[0];
  }
  // public Object getRoutingObject(byte[] keyByteArray) {
  // if (this.splitsMap == null || this.splitsMap.isEmpty()) {
  // this.splitsMap = MTableUtils.getUniformKeySpaceSplit(this.totalNumberOfBuckets,
  // this.startRangeKey, this.stopRangeKey);
  // }
  //
  // for (Map.Entry<Integer, Pair<byte[], byte[]>> entry : this.splitsMap.entrySet()) {
  //
  // Pair<byte[], byte[]> range = entry.getValue();
  //
  // byte[] startKey = range.getFirst();
  // byte[] stopKey = range.getSecond();
  //
  // /*
  // * Only compare up to the range's length; doing otherwise will generate strange results due to
  // * the lexicographical ordering. (i.e., treat the range key as a prefix).
  // */
  // if ((Bytes.compareToPrefix(keyByteArray, startKey) >= 0)
  // && (Bytes.compareToPrefix(keyByteArray, stopKey) <= 0)) {
  // return entry.getKey();
  // }
  // }
  // return null;
  // }

  public Map<Integer, Pair<byte[], byte[]>> getSplitsMap() {
    return this.splitsMap;
  }

  public int getNumberOfBuckets() {
    return this.totalNumberOfBuckets;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  public Map<IMKey, Object[]> getLookupMap() {
    return this.lookupMap;
  }

  @Override
  public void close() {

  }

  @Override
  public void setTotalNumberOfBuckets(int numberOfBuckets) {
    this.totalNumberOfBuckets = numberOfBuckets;
    // Setting splitsMap to null so that it is recalculated when getRoutingObject is called
    this.splitsMap = null;
  }

  @Override
  public void setStartStopRangeKey(byte[] startRangeKey, byte[] stopRangeKey) {
    this.startRangeKey = startRangeKey;
    this.stopRangeKey = stopRangeKey;
    // Setting splitsMap to null so that it is recalculated when getRoutingObject is called
    this.splitsMap = null;
  }

  @Override
  public void init(final Properties props) {}

  // @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  // @Override
  public int getDSFID() {
    return DataSerializableFixedID.AMPL_M_RANGE_PART_RESOLVER;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveInt(this.totalNumberOfBuckets, out);
    DataSerializer.writeByteArray(this.startRangeKey, out);
    DataSerializer.writeByteArray(this.stopRangeKey, out);
    DataSerializer.writePrimitiveInt(this.lookupMap.size(), out);
    for (Map.Entry<IMKey, Object[]> entry : this.lookupMap.entrySet()) {
      DataSerializer.writeByteArray(entry.getKey().getBytes(), out);
      DataSerializer.writePrimitiveInt((Integer) entry.getValue()[0], out);
      DataSerializer.writeByteArray((byte[]) entry.getValue()[1], out);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.totalNumberOfBuckets = DataSerializer.readPrimitiveInt(in);
    this.startRangeKey = DataSerializer.readByteArray(in);
    this.stopRangeKey = DataSerializer.readByteArray(in);
    final int size = DataSerializer.readPrimitiveInt(in);
    this.lookupMap = new TreeMap<>();
    byte[] arr1, arr2;
    int bid;
    for (int i = 0; i < size; i++) {
      arr1 = DataSerializer.readByteArray(in);
      bid = DataSerializer.readPrimitiveInt(in);
      arr2 = DataSerializer.readByteArray(in);
      this.lookupMap.put(new MKeyBase(arr1), new Object[] {bid, arr2});
    }
  }
}
