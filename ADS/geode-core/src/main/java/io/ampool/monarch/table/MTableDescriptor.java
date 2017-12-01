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
import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.exceptions.TableColumnAlreadyExists;
import io.ampool.monarch.table.exceptions.TableInvalidConfiguration;
import io.ampool.monarch.table.internal.AbstractTableDescriptor;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.CDCConfigImpl;
import io.ampool.monarch.table.internal.CDCInformation;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.RowHeader;
import io.ampool.monarch.table.internal.TableType;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataSerializable;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class represents the schema of the table and other attributes like the maximum number of
 * version for each row, number of regions splits (data buckets), split range, etc.(see details
 * below). Call the functions in an MTableDescriptor to set the schema and attributes before calling
 * {@link Admin#createTable(String, MTableDescriptor) createTable}.
 *
 * Note this class only defines the template for table creation; changing its attributes will not
 * affect any tables already created.
 *
 * <P>
 * </P>
 *
 * DEFAULTS:
 * <P>
 * </P>
 * - ordered table,<br>
 * - 1 row versions maintained,<br>
 * - no disk persistence,<br>
 * - no cache store persistence configured,<br>
 * - no redundancy (redundant copies = 0),<br>
 * - number of splits = 113,<br>
 * - start range key = {},<br>
 * - stop range key = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF}.<br>
 * <br>
 * Usage Examples:<br>
 * <br>
 * - Simple table maintaining 2 row versions:<br>
 * <br>
 * List<String> columnNames = Arrays.asList("NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ");<br>
 * MTableDescriptor tableDescriptor = new MTableDescriptor();<br>
 * tableDescriptor.addColumn(Bytes.toBytes("NAME"))<br>
 * &nbsp;&nbsp;&nbsp;&nbsp; .addColumn(Bytes.toBytes("ID"))<br>
 * &nbsp;&nbsp;&nbsp;&nbsp; .addColumn("AGE")<br>
 * &nbsp;&nbsp;&nbsp;&nbsp; .addColumn("SALARY", new MTableColumnType("BINARY"))<br>
 * &nbsp;&nbsp;&nbsp;&nbsp; .addColumn(Bytes.toBytes("DEPT"))<br>
 * &nbsp;&nbsp;&nbsp;&nbsp; .addColumn(Bytes.toBytes("DOJ"))<br>
 * &nbsp;&nbsp;&nbsp;&nbsp; .setMaxVersions(2); <br>
 * MAdmin admin = clientCache.getAdmin();<br>
 * String tableName = "EmployeeTable";<br>
 * MTable table = admin.createTable(tableName, tableDescriptor);<br>
 * <br>
 *
 * @since 0.2.0.0 MTableDescriptor also has a function to set the type of the table. See
 *        {@link MTableType}
 *
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MTableDescriptor extends AbstractTableDescriptor implements VersionedDataSerializable {

  private static final long serialVersionUID = -3723739744868265284L;
  private static final Logger logger = LogService.getLogger();

  private final static int DEFAULT_MAX_VERSIONS = 1;

  private boolean isUserTable = true;

  /**
   * Holds the information about CDC stream for this table
   */
  private ArrayList<CDCInformation> cdcInformations = new ArrayList<>();

  /**
   * Maximum number of version to be stored for a row Once the maximum limit is reach oldest row
   * version will be deleted.
   */
  private int maxVersions = DEFAULT_MAX_VERSIONS;

  /**
   * First key in range for split
   */
  private byte[] startRangeKey = null;

  /**
   * First key in range for split
   */
  private byte[] stopRangeKey = null;

  /**
   * Defines the type of the table Default type of the table is MTableType.ORDERED_VERSIONED See
   * {@link TableType}
   */
  protected MTableType tableType = null;

  /**
   * container that maintains coprocessors added for this table
   */
  private ArrayList<String> coprocessorList;
  private Map<Integer, Pair<byte[], byte[]>> keySpace = null;
  private Map<Integer, Pair<byte[], byte[]>> keySpaceInternal = null;
  private String cacheLoaderClassName = null;

  /**
   * Get type of the table.
   * 
   * @return Returns the type {@link MTableType} of the table
   */
  public MTableType getTableType() {
    return tableType;
  }

  /**
   * Creates new table descriptor with table of type {@link MTableType#ORDERED_VERSIONED}
   */
  public MTableDescriptor() {
    this(MTableType.ORDERED_VERSIONED);
  }

  /**
   * Creates new table descriptor with specified table type
   */
  public MTableDescriptor(MTableType tableType) {
    super(tableType == MTableType.ORDERED_VERSIONED ? TableType.ORDERED_VERSIONED
        : TableType.UNORDERED);
    this.columnsByPositions = new LinkedHashMap<>();
    this.columnPosition = 0;
    this.maxVersions = DEFAULT_MAX_VERSIONS;
    this.columnsByName = new LinkedHashMap<>();
    this.isDiskPersistenceEnabled = false;
    this.coprocessorList = new ArrayList<>();
    this.tableType = tableType;
    this.keySpaceInternal = MTableUtils.getUniformKeySpaceSplit(this.totalNumOfSplits,
        this.startRangeKey, this.stopRangeKey);
  }

  /**
   * Add a CDC stream to the table schema.
   *
   * @param id name of the CDC stream to be added.
   * @param listenerName full classpath of the CDC stream listener.
   * @param cdcConfig configuration apart from default CDC stream.
   * @throws IllegalArgumentException if id or listenerName is null or empty.
   * @return Object of this class, used for chained calling.
   */
  public MTableDescriptor addCDCStream(String id, String listenerName, CDCConfig cdcConfig) {
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("CDC stream id is null or empty.");
    }
    if (listenerName == null || listenerName.isEmpty()) {
      throw new IllegalArgumentException(
          "CDC stream full listener class path name is null or empty.");
    }
    CDCInformation cdcInformation = new CDCInformation(id, listenerName, cdcConfig);
    cdcInformations.add(cdcInformation);
    return this;
  }

  /**
   * Creates {@link CDCConfig} for creating a CDCStream
   *
   * @return CDCConfig
   */
  public CDCConfig createCDCConfig() {
    return new CDCConfigImpl();
  }

  /**
   * Add a column to the table schema.
   *
   * @param colName name of the column to be added.
   * @return Object of this class, used for chained calling.
   */
  public MTableDescriptor addColumn(final byte[] colName) throws TableColumnAlreadyExists {
    return (MTableDescriptor) super.addColumn(colName);
  }

  /**
   * Add a column to the table schema.
   *
   * @param colName name of the column to be added.
   * @return Object of this class, used for chained calling.
   */
  public MTableDescriptor addColumn(final String colName) throws TableColumnAlreadyExists {
    return (MTableDescriptor) super.addColumn(colName);
  }

  /**
   * Add a column of a specific type to the table schema. Although columns are stored as byte[]
   * type, if the underlying type that the bytes correspond to is known to the MTable certain
   * efficiencies may result and for some persistent stores this type may be required.
   *
   * @param colName name of the column to be added.
   * @param type The {@link BasicTypes} for this column; for example MBasicObjectType.STRING.
   * @return Object of this class, used for chained calling.
   */
  public MTableDescriptor addColumn(final String colName, BasicTypes type)
      throws TableColumnAlreadyExists {
    return (MTableDescriptor) super.addColumn(colName, type);
  }

  /**
   * Add a column of a specific type to the table schema. Although columns are stored as byte[]
   * type, if the underlying type that the bytes correspond to is known to the MTable certain
   * efficiencies may result and for some persistent stores this type may be required.
   *
   * @param colName name of the column to be added.
   * @param type The {@link BasicTypes} for this column; for example MBasicObjectType.STRING.
   * @return Object of this class, used for chained calling.
   */
  public MTableDescriptor addColumn(final byte[] colName, BasicTypes type)
      throws TableColumnAlreadyExists {
    return (MTableDescriptor) super.addColumn(colName, type);
  }

  /**
   * Add a column of a specific type to the table schema. Although columns are stored as byte[]
   * type, if the underlying type that the bytes correspond to is known to the MTable certain
   * efficiencies may result and for some persistent stores this type may be required. This method
   * allows a a complex type to be defined with a String (see {@link MTableColumnType}. <b>Note on
   * column types: The use of strings to define complex types for {@link MTableColumnType} is for
   * advanced development and in general unless a complex or new type is needed always use one of
   * the types defined in {@link BasicTypes} with the
   * {@link MTableDescriptor#addColumn(String, BasicTypes)}.</b>
   *
   * @param colName name of the column to be added.
   * @param columnType The {@link MTableColumnType} for this column; for example new
   *        MTableColumnType(MBasicObjectType.STRING). Complex types may be defined using a properly
   *        formatted String (see {@link MTableColumnType}).
   * @return Object of this class, used for chained calling.
   */
  public MTableDescriptor addColumn(final String colName, MTableColumnType columnType)
      throws TableColumnAlreadyExists {
    return (MTableDescriptor) super.addColumn(colName, columnType);
  }

  /**
   * Add a column of a specific type to the table schema. Although columns are stored as byte[]
   * type, if the underlying type that the bytes correspond to is known to the MTable certain
   * efficiencies may result and for some persistent stores this type may be required. This method
   * allows a a complex type to be defined with a String (see {@link MTableColumnType}. <b>Note on
   * column types: The use of strings to define complex types for {@link MTableColumnType} is for
   * advanced development and in general unless a complex or new type is needed always use one of
   * the types defined in {@link BasicTypes} with the
   * {@link MTableDescriptor#addColumn(String, BasicTypes)}.</b>
   *
   * @param colName name of the column to be added.
   * @param columnType The {@link MTableColumnType} for this column; for example new
   *        MTableColumnType(MBasicObjectType.STRING). Complex types may be defined using a properly
   *        formatted String (see {@link MTableColumnType}).
   * @return Object of this class, used for chained calling.
   */
  public MTableDescriptor addColumn(final byte[] colName, MTableColumnType columnType)
      throws TableColumnAlreadyExists {
    return (MTableDescriptor) super.addColumn(colName, columnType);
  }

  /**
   * @deprecated Add a column of a specific type (as String) to the table schema. Use
   *             {@link MTableDescriptor#addColumn(String, MTableColumnType)} instead.
   *
   * @param colName name of the column to be added.
   * @param columnTypeId the object type String for this column; for example "STRING" or "INT".
   *        Complex types may be defined using a String (see {@link MTableColumnType}).
   * @return Object of this class, used for chained calling.
   * @deprecated Add a column of a specific type (as String) to the table schema. Use
   *             {@link MTableDescriptor#addColumn(String, MTableColumnType)} instead.
   */
  public MTableDescriptor addColumn(final String colName, String columnTypeId)
      throws TableColumnAlreadyExists {
    return (MTableDescriptor) super.addColumn(colName, columnTypeId);
  }

  // /**
  // * Returns the Map of columnDescriptor where the map key is the MColumnDescriptor and the value
  // is
  // * it's respective position in a row as defined by the schema.
  // *
  // * @return Column Descriptor Map.
  // */
  // @Override
  // public Map<MColumnDescriptor, Integer> getColumnDescriptorsMap() {
  // return this.columnsByPositions.size() > 0 ? this.columnsByPositions :
  // super.getColumnDescriptorsMap();
  // }
  // public int getNumOfColumns() {
  // return this.columnsByPositions.size() > 0 ? this.columnsByPositions.size() :
  // super.getNumOfColumns();
  // }
  /**
   * Set the maximum number of versions to be kept in memory for row versioning. Once the maximum
   * limit is reached the oldest version of that row will be discarded when a new version is created
   * by an update. The default maximum version is 1 (only the current version is maintained).
   *
   * @param maxVersions maximum number of row versions.
   */
  public MTableDescriptor setMaxVersions(int maxVersions) {
    if (maxVersions < 1) {
      throw new TableInvalidConfiguration("maximum number of row versions must be > 0");
    }
    this.maxVersions = maxVersions;
    return this;
  }

  /**
   * Returns the maximum number of versions for a row.
   *
   * @return Returns the configured maximum number of versions for rows in this table.
   */
  public int getMaxVersions() {
    return this.maxVersions;
  }


  /**
   * Sets the number of redundant copies of the data for a table. These copies are used to insure
   * data is available even in the case one or more cache servers become unavailable.
   *
   * @param copies the number of redundant copies
   */
  @Override
  public MTableDescriptor setRedundantCopies(int copies) {
    return (MTableDescriptor) super.setRedundantCopies(copies);
  }

  /**
   * Gets the first key for the split range. The range is inclusive so this is the first key that is
   * in the range. Only relevant for ORDERED tables.
   *
   * @return first split range key
   */
  public byte[] getStartRangeKey() {
    return this.startRangeKey;
  }

  /**
   * Gets the last key for the split range. The range is inclusive so this is the last key that is
   * in the range. Only relevant for ORDERED tables.
   *
   * @return last key in split range
   */
  public byte[] getStopRangeKey() {
    return this.stopRangeKey;
  }

  /**
   * Sets the number of configured hash or range splits for a table. Note that the hash (unordered
   * table) or range (ordered table) split creates exactly totalNumOfSplits buckets (there is no
   * default bucket for keys that do not map in the case of a range split). Note that the total
   * number of data buckets for a table will be number of configured splits times the number of
   * redundant data copies.
   *
   * @param totalNumOfSplits the total number of splits
   * @throws IllegalArgumentException
   */
  public MTableDescriptor setTotalNumOfSplits(int totalNumOfSplits)
      throws TableInvalidConfiguration {
    if (this.keySpace != null) {
      throw new IllegalArgumentException(
          "Either you can set keyspace or total number of splits not both");
    }
    if (totalNumOfSplits <= 0) {
      throw new IllegalArgumentException("TotalNumOfSplits cannot be negative or zero");
    }
    if (totalNumOfSplits >= 512) {
      throw new IllegalArgumentException("TotalNumOfSplits cannot be greater than or equal to 512");
    }
    this.totalNumOfSplits = totalNumOfSplits;
    this.keySpaceInternal = MTableUtils.getUniformKeySpaceSplit(this.totalNumOfSplits,
        this.startRangeKey, this.stopRangeKey);
    return this;
  }

  /**
   * Sets the start range key for a table split. The range is inclusive of the start and stop keys.
   * The default start key is the empty byte array and the default end key is
   * {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF}. Note that if your range is partial in the possible
   * key space some keys may not map, causing an exception if they are used (there is no default
   * bucket for key placement). For example the single byte range {0x00} to {0xFF} is complete, but
   * the range {0x03} to {0xFF} is partial and keys that start with 0x00, 0x01, and 0x02 will not
   * map to a bucket. And Sets the stop range key for a table split. The range is inclusive of the
   * start and stop keys. The default start key is the empty byte array and the default stop key is
   * {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF}. Note that if you range is partial in the possible
   * key space some keys may not map, causing an exception if they are used (there is no default
   * bucket for key placement). For example the single byte range {0x00} to {0xFF} is complete, but
   * the range {0x03} to {0xFF} is partial and keys that start with 0x00, 0x01, and 0x02 will not
   * map to a bucket.
   *
   * @param startRangeKey the start key for the table split
   * @param stopRangeKey the end key for the table split
   * @return this MTableDescriptor object
   */
  public MTableDescriptor setStartStopRangeKey(byte[] startRangeKey, byte[] stopRangeKey)
      throws TableInvalidConfiguration {
    if (this.keySpace != null) {
      throw new IllegalArgumentException(
          "Either you can set keyspace or start-stop range but not both");
    }
    if (startRangeKey == null) {
      throw new TableInvalidConfiguration("Start Range Key cannot be null");
    }
    if (stopRangeKey == null) {
      throw new TableInvalidConfiguration("Stop Range Key cannot be null");
    }

    if (Bytes.compareTo(startRangeKey, stopRangeKey) >= 0) {
      throw new TableInvalidConfiguration("start of range must be <= end of range");
    }

    this.startRangeKey = startRangeKey;
    this.stopRangeKey = stopRangeKey;
    this.keySpaceInternal = MTableUtils.getUniformKeySpaceSplit(this.totalNumOfSplits,
        this.startRangeKey, this.stopRangeKey);
    return this;
  }

  /**
   * Helper routine that properly sets the start and stop range for an integer value. This is useful
   * for the case when you key range is defined by an integer range.
   *
   * @param startRangeKey the start key for the table split
   * @param stopRangeKey the end key for the table split
   * @return this MTableDescriptor object
   */
  public MTableDescriptor setStartStopRangeKey(int startRangeKey, int stopRangeKey) {
    return setStartStopRangeKey(Bytes.intToBytesFlipped(startRangeKey),
        Bytes.intToBytesFlipped(stopRangeKey));
  }

  /**
   * Helper routine that properly sets the start and stop range for a long value. This is useful for
   * the case when you key range is defined by a long integer range.
   *
   * @param startRangeKey the start key for the table split
   * @param stopRangeKey the stop key for the table split
   * @return this MTableDescriptor object
   */
  public MTableDescriptor setStartStopRangeKey(long startRangeKey, long stopRangeKey) {
    return setStartStopRangeKey(Bytes.longToBytesFlipped(startRangeKey),
        Bytes.longToBytesFlipped(stopRangeKey));
  }

  /**
   * Sets disk store name, to be used if disk persistence is enabled.
   *
   * @param diskStoreName the name of the disk-store
   */
  public MTableDescriptor setDiskStore(String diskStoreName) {
    return (MTableDescriptor) super.setDiskStore(diskStoreName);
  }


  /**
   * Add a observer coprocessor {@link io.ampool.monarch.table.coprocessor.MTableObserver} to this
   * table descriptor.
   * <p>
   * For endpoint coprocessor See {@link MCoprocessor}, do not call this API.
   * </p>
   * 
   * @param className Fully qualified classname
   */
  public MTableDescriptor addCoprocessor(String className) {
    this.coprocessorList.add(className);
    return this;
  }

  /**
   * Get the list of coprocessors and observers associated with this table descriptor.
   *
   * @return List of co-processors (as fully qualified class names) associated with this table
   *         descriptor
   */
  public ArrayList<String> getCoprocessorList() {
    return coprocessorList;
  }

  // /**
  // * Get a map of column descriptors keyed by teh column name as a byte[] array.
  // *
  // * @return Map of byte[], MColumnDescriptor containing the columns defined in this table
  // * descriptor.
  // */
  // public Map<ByteArrayKey, MColumnDescriptor> getColumnsByName() {
  // return this.columnsByName.size() > 0 ? this.columnsByName : super.getColumnsByName();
  // }

  @Override
  public String toString() {
    return "MTableDescriptor{" + "version=" + version + ", isUserTable=" + isUserTable
        + ", columnsByPositions=" + columnsByPositions + ", columnPosition=" + columnPosition
        + ", maxVersions=" + maxVersions + ", tableType=" + getType() + ", columnsByName="
        + columnsByName + ", isDiskPersistenceEnabled=" + isDiskPersistenceEnabled
        + ", diskStoreName='" + diskStoreName + '\'' + ", redundantCopies=" + redundantCopies
        + ", totalNumOfSplits=" + totalNumOfSplits + ", startRangeKey="
        + Arrays.toString(startRangeKey) + ", stopRangeKey=" + Arrays.toString(stopRangeKey)
        + ", coprocessorList=" + coprocessorList + ", keySpace=" + keySpace + ", numOfColumns="
        + numOfColumns + ", diskWritePolicy=" + diskWritePolicy + ", evictionPolicy="
        + evictionPolicy + ", cdcInformations=" + cdcInformations + ", recoveryDelay="
        + recoveryDelay + ", startupRecoveryDelay=" + startupRecoveryDelay + '}';
  }

  /**
   * <em>INTERNAL</em> Used by Ampool connector interface.
   * 
   * @return the isUserTable
   */
  @InterfaceAudience.Private
  public boolean getUserTable() {
    return isUserTable;
  }

  /**
   * <em>INTERNAL</em> Used by Ampool connector interface.
   * 
   * @param isUserTable the isUserTable to set
   */
  @InterfaceAudience.Private
  public MTableDescriptor setUserTable(boolean isUserTable) {
    this.isUserTable = isUserTable;
    return this;
  }

  /**
   * Use specified key ranges for the buckets. The argument is a map of bucket-id with the
   * respective start-key and stop-key. Once the key range is specified, subsequent put operations
   * will store/route the keys falling in that range to the respective buckets.
   *
   * @param keySpace the bucket-id to start-key and stop-key mapping
   * @return the table descriptor
   */
  public MTableDescriptor setKeySpace(Map<Integer, Pair<byte[], byte[]>> keySpace) {
    if (this.startRangeKey != null || this.stopRangeKey != null) {
      throw new IllegalArgumentException(
          "Either you can set keyspace or start-stop range but not both");
    }
    validateKeySpace(keySpace);
    this.keySpace = keySpace;
    this.keySpaceInternal = null;
    this.totalNumOfSplits = keySpace.size();
    return this;
  }

  public static void validateKeySpace(Map<Integer, Pair<byte[], byte[]>> keySpace) {
    byte[] prevStart = null;
    byte[] prevStop = null;

    for (int i = 0; i < keySpace.size(); i++) {
      Pair<byte[], byte[]> entry = keySpace.get(i);

      if (entry == null) {
        throw new TableInvalidConfiguration("key range for bucket " + i + " not found in keyspace");
      }

      byte[] currStart = entry.getFirst();
      byte[] currStop = entry.getSecond();
      if (currStart == null || currStop == null) {
        throw new TableInvalidConfiguration("Bucket(" + i + ") start/stop keys must not be null");
      }

      if (Bytes.compareTo(currStart, currStop) > 0) {
        // throw exception
        throw new TableInvalidConfiguration(
            "Bucket " + i + ":start of range must be <= end of range");
      }
      if (prevStart != null && prevStop != null) {
        if (Bytes.compareTo(prevStop, currStart) >= 0) {
          // throw exception
          throw new TableInvalidConfiguration(
              "Buckets(" + (i - 1) + "," + i + "): key ranges for buckets must not overlap");
        }
      }
      prevStart = currStart;
      prevStop = currStop;
    }
  }

  /**
   * Provide the current key-range to bucket mapping of the table.
   *
   * @return the bucket-id to start-key and stop-key mapping
   */
  public Map<Integer, Pair<byte[], byte[]>> getKeySpace() {
    return this.keySpace == null ? this.keySpaceInternal : this.keySpace;
  }

  /**
   * sets local max memory that can be used by this table on one server.
   *
   * @param memory memory in mega bytes.
   * @return the table descriptor
   */
  @Override
  public MTableDescriptor setLocalMaxMemory(int memory) {
    return (MTableDescriptor) super.setLocalMaxMemory(memory);
  }


  /**
   * The state is written to disk and recovered from disk when the table is created. The writing to
   * the disk can be either synchronous or asynchronous and can be specified by the policy.
   *
   * @param policy the disk-write policy to be set for this table
   * @return the table descriptor
   *
   * @see MDiskWritePolicy
   */
  @Override
  public MTableDescriptor enableDiskPersistence(MDiskWritePolicy policy) {
    return (MTableDescriptor) super.enableDiskPersistence(policy);
  }

  /**
   * Set the policy on how to evict data when the eviction heap percentage (a server level
   * configuration) is reached. See {@link MEvictionPolicy} for allowed policies. For Table, By
   * default {@link MEvictionPolicy#OVERFLOW_TO_TIER} is set. This enables data to evict to tiered
   * storage If user choose to overwrite the default with {@link MEvictionPolicy#NO_ACTION}, data
   * will not be evicted to tiered storage.
   *
   * @param policy the eviction policy.
   * @return this MTableDescriptor object.
   * @throws IllegalArgumentException if policy is {@link MEvictionPolicy#OVERFLOW_TO_TIER}.
   * @see MEvictionPolicy
   */
  public MTableDescriptor setEvictionPolicy(MEvictionPolicy policy) {
    if (MEvictionPolicy.OVERFLOW_TO_TIER.equals(policy)) {
      throw new IllegalArgumentException("Specified EvictionPolicy not applicable for MTable!");
    }
    this.evictionPolicy = policy;
    return this;
  }

  /**
   * Setting expiration attributes for expiring entries from MTable based on time See
   * {@link MExpirationAttributes} for expiration attributes
   *
   * @param expirationAttributes
   * @return update MTable Descriptor
   */
  public MTableDescriptor setExpirationAttributes(MExpirationAttributes expirationAttributes) {
    return (MTableDescriptor) super.setExpirationAttributes(expirationAttributes);
  }

  // TODO : FTable Add storeage configuration handler

  /**
   * <em>INTERNAL</em>
   *
   * Serializes contents of MTableDescriptor to output stream.
   *
   * @param out
   * @throws IOException
   */
  @InterfaceAudience.Private
  @Override
  public void toData(final DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeBoolean(this.isUserTable, out);
    DataSerializer.writeInteger(this.maxVersions, out);
    DataSerializer.writeEnum(this.tableType, out);
    DataSerializer.writeByteArray(this.startRangeKey, out);
    DataSerializer.writeByteArray(this.stopRangeKey, out);
    DataSerializer.writeArrayList(this.coprocessorList, out);
    DataSerializer.writeHashMap(this.keySpace, out);
    DataSerializer.writeHashMap(this.keySpaceInternal, out);
    DataSerializer.writeArrayList(this.cdcInformations, out);
    DataSerializer.writeString(this.cacheLoaderClassName, out);
  }

  /**
   * <em>INTERNAL</em>
   *
   * Serializes contents from input stream into {@link MTableDescriptor}.
   *
   * @param in
   * @throws IOException
   */
  @InterfaceAudience.Private
  @Override
  public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.isUserTable = DataSerializer.readBoolean(in);
    this.maxVersions = DataSerializer.readInteger(in);
    this.tableType = DataSerializer.readEnum(MTableType.class, in);
    this.startRangeKey = DataSerializer.readByteArray(in);
    this.stopRangeKey = DataSerializer.readByteArray(in);
    this.coprocessorList = DataSerializer.readArrayList(in);
    this.keySpace = DataSerializer.readHashMap(in);
    this.keySpaceInternal = DataSerializer.readHashMap(in);
    this.cdcInformations = DataSerializer.readArrayList(in);
    if (version >= 2) {
      this.cacheLoaderClassName = DataSerializer.readString(in);
    }
  }

  /**
   * Returns list of {@link CDCInformation} attached to this table.
   * 
   * @return List of {@link CDCInformation}
   */
  public ArrayList<CDCInformation> getCdcInformations() {
    return this.cdcInformations;
  }

  /**
   * <em>INTERNAL</em>
   *
   * @return return the possible serialization version of the descriptor
   */
  @InterfaceAudience.Private
  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  /**
   * Returns row header according to table type and schema version
   *
   * @return Header bytes associated with current version of MTableDescriptor and byte array
   *         encoding scheme Order of bytes is MagicNumber, Encoding, Schema version, Reserved byte
   */
  public byte[] getRowHeaderBytes() {
    byte[] ret = new byte[4];
    switch (this.tableType) {
      case ORDERED_VERSIONED:
        ret[0] = 1;
        ret[1] = 1;
        ret[2] = schemaVersion;
        ret[3] = 0;
        break;
      case UNORDERED:
        ret[0] = 2;
        ret[1] = 1;
        ret[2] = schemaVersion;
        ret[3] = 0;
        break;
    }
    return ret;
  }

  /**
   * Returns row header according to table type and schema version
   *
   * @return RowHeader associated with current version of MTableDescriptor and byte array encoding
   *         scheme
   */
  public RowHeader getRowHeader() {
    byte[] ret = new byte[4];
    switch (this.tableType) {
      case ORDERED_VERSIONED:
        ret[0] = 1;
        ret[1] = 1;
        ret[2] = schemaVersion;
        ret[3] = 0;
        break;
      case UNORDERED:
        ret[0] = 2;
        ret[1] = 1;
        ret[2] = schemaVersion;
        ret[3] = 0;
        break;
    }
    return new RowHeader(ret);
  }

  public MTableDescriptor setCacheLoaderClassName(final String cacheLoaderClassName) {
    this.cacheLoaderClassName = cacheLoaderClassName;
    return this;
  }

  public String getCacheLoaderClassName() {
    return this.cacheLoaderClassName;
  }
}
