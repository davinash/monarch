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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.internal.CDCInformation;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

/**
 * Class which describe the input arguments for MTableCreationFunction . When you add new arguments
 * Make sure that We are adding default value and serialization logic for the same. Also is possible
 * add all the Getter and Setters.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MCreateFunctionArguments implements DataSerializableFixedID {

  private static final long serialVersionUID = 4064466035841983066L;
  private ArrayList<CDCInformation> cdcInformations = null;
  /**
   * Underlying region Name
   */
  private String regionName = null;
  /**
   * User table flag which decided if this this table creation for User table or Meta Table.
   */
  private boolean isUserTable = true;
  private String tableMapType = null;
  private int redundancyLevel = 0;
  private int numOfSplits = 113;
  private byte[] startRangeKey = null;
  private byte[] stopRangeKey = null;
  private String cacheStoreName = null;
  private boolean isDiskPersistance = false;
  private String diskStoreName;
  private List<String> coprocessorList;
  private Map<Integer, Pair<byte[], byte[]>> keySpace;

  private MDiskWritePolicy diskWritePolicy = MDiskWritePolicy.ASYNCHRONOUS;
  private MEvictionPolicy evictionPolicy = MEvictionPolicy.OVERFLOW_TO_DISK;
  private MExpirationAttributes expirationAttributes = null;

  /**
   * Default constructor for Serialization Mechanism.
   */
  public MCreateFunctionArguments() {

  }

  public MCreateFunctionArguments(String regionName, boolean isUserTable, String tableMapType,
      int redundancyLevel, int numOfSplits, byte[] startRangeKey, byte[] stopRangeKey,
      boolean isPeristentTable, String storeName, List<String> coproList,
      Map<Integer, Pair<byte[], byte[]>> keySpace, MDiskWritePolicy diskWritePolicy,
      MEvictionPolicy evictionPolicy, MExpirationAttributes expirationAttributes,
      ArrayList<CDCInformation> cdcInformations) {
    this.regionName = regionName;
    this.isUserTable = isUserTable;
    this.tableMapType = tableMapType;
    this.redundancyLevel = redundancyLevel;
    this.numOfSplits = numOfSplits;
    this.startRangeKey = startRangeKey;
    this.stopRangeKey = stopRangeKey;
    this.cacheStoreName = cacheStoreName;
    this.isDiskPersistance = isPeristentTable;
    this.diskStoreName = storeName;
    this.coprocessorList = coproList;
    this.keySpace = keySpace;

    this.diskWritePolicy = diskWritePolicy;
    this.evictionPolicy = evictionPolicy;
    this.expirationAttributes = expirationAttributes;
    this.cdcInformations = cdcInformations;
  }

  @Override
  public int getDSFID() {
    return AMPL_MTABLE_RGN_CREATE_ARGS;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.regionName, out);
    DataSerializer.writeBoolean(this.isUserTable, out);
    DataSerializer.writeString(this.tableMapType, out);
    DataSerializer.writeInteger(this.redundancyLevel, out);
    DataSerializer.writeInteger(this.numOfSplits, out);
    DataSerializer.writeByteArray(this.startRangeKey, out);
    DataSerializer.writeByteArray(this.stopRangeKey, out);
    DataSerializer.writeString(this.cacheStoreName, out);
    DataSerializer.writeObject(this.isDiskPersistance, out);
    DataSerializer.writeObject(this.diskStoreName, out);
    DataSerializer.writeArrayList((ArrayList) this.coprocessorList, out);
    DataSerializer.writeHashMap(this.keySpace, out);

    DataSerializer.writeEnum(this.diskWritePolicy, out);
    DataSerializer.writeEnum(this.evictionPolicy, out);
    DataSerializer.writeObject(this.expirationAttributes, out);
    DataSerializer.writeArrayList(this.cdcInformations, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.regionName = DataSerializer.readString(in);
    this.isUserTable = DataSerializer.readBoolean(in);
    this.tableMapType = DataSerializer.readString(in);
    this.redundancyLevel = DataSerializer.readInteger(in);
    this.numOfSplits = DataSerializer.readInteger(in);
    this.startRangeKey = DataSerializer.readByteArray(in);
    this.stopRangeKey = DataSerializer.readByteArray(in);
    this.cacheStoreName = DataSerializer.readString(in);
    this.isDiskPersistance = DataSerializer.readObject(in);
    this.diskStoreName = DataSerializer.readObject(in);
    this.coprocessorList = DataSerializer.readArrayList(in);
    this.keySpace = DataSerializer.readHashMap(in);

    this.diskWritePolicy = DataSerializer.readEnum(MDiskWritePolicy.class, in);
    this.evictionPolicy = DataSerializer.readEnum(MEvictionPolicy.class, in);
    this.expirationAttributes = DataSerializer.readObject(in);

    this.cdcInformations = DataSerializer.readArrayList(in);
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public boolean isUserTable() {
    return isUserTable;
  }

  public void setUserTable(boolean userTable) {
    isUserTable = userTable;
  }

  public String getTableMapType() {
    return tableMapType;
  }

  public void setTableMapType(String tableMapType) {
    this.tableMapType = tableMapType;
  }

  public int getRedundancyLevel() {
    return redundancyLevel;
  }

  public void setRedundancyLevel(int redundancyLevel) {
    this.redundancyLevel = redundancyLevel;
  }

  public int getNumOfSplits() {
    return numOfSplits;
  }

  public void setNumOfSplits(int numOfSplits) {
    this.numOfSplits = numOfSplits;
  }

  public byte[] getStartRangeKey() {
    return this.startRangeKey;
  }

  public void setStartRangeKey(byte[] startRangeKey) {
    this.startRangeKey = startRangeKey;
  }

  public byte[] getStopRangeKey() {
    return this.stopRangeKey;
  }

  public void setStopRangeKey(byte[] stopRangeKey) {
    this.stopRangeKey = stopRangeKey;
  }

  public String getCacheStore() {
    return cacheStoreName;
  }

  public boolean isDiskPersistance() {
    return isDiskPersistance;
  }

  public void setDiskPersistance(boolean isDiskPersistance) {
    this.isDiskPersistance = isDiskPersistance;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public void setDiskStoreName(String diskStoreName) {
    this.diskStoreName = diskStoreName;
  }

  public List<String> getCoprocessorList() {
    return this.coprocessorList;
  }

  public Map<Integer, Pair<byte[], byte[]>> getKeySpace() {
    return this.keySpace;
  }

  public void setKeySpace(Map<Integer, Pair<byte[], byte[]>> keySpace) {
    this.keySpace = keySpace;
  }

  public MDiskWritePolicy getDiskWritePolicy() {
    return diskWritePolicy;
  }

  public MEvictionPolicy getEvictionPolicy() {
    return evictionPolicy;
  }

  public MExpirationAttributes getExpirationAttributes() {
    return expirationAttributes;
  }

  public void setExpirationAttributes(MExpirationAttributes expirationAttributes) {
    this.expirationAttributes = expirationAttributes;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  public ArrayList<CDCInformation> getCdcInformations() {
    return cdcInformations;
  }
}
