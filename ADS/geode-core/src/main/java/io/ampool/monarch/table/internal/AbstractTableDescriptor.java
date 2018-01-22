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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.internal.AMPLDataSerializer;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MExpirationAttributes;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.Schema;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.TableColumnAlreadyExists;
import io.ampool.monarch.table.exceptions.TableInvalidConfiguration;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.internal.cache.DiskStoreAttributes;

public abstract class AbstractTableDescriptor implements TableDescriptor {

  public final static int DEFAULT_TOTAL_NUM_SPLITS = 113;
  public final static int DEFAULT_LOCAL_MAX_MEMORY = Integer.MIN_VALUE;
  public final static int DEFAULT_LOCAL_MAX_MEMORY_PCT = 0;

  private static final long serialVersionUID = -7861536969139544805L;

  /**
   * Table descriptor version
   */
  protected int version = 2;

  /**
   * Name of table
   */
  protected String tableName;

  /**
   * Maintains the position of the column in the table schema
   */
  protected LinkedHashMap<MColumnDescriptor, Integer> columnsByPositions = null;

  protected Map<ByteArrayKey, MColumnDescriptor> columnsByName = null;
  protected Schema tableSchema = null;

  protected ArrayList<Integer> fixedLengthColumnIndices = new ArrayList<>();
  protected ArrayList<Integer> varibleLengthColumnIndices = new ArrayList<>();

  protected byte schemaVersion = 1;

  /**
   * Defines the type of the table Default type of the table is MTableType.ORDERED_VERSIONED See
   * {@link TableType}
   */
  protected TableType tableType = TableType.ORDERED_VERSIONED;

  /**
   * Variable defines how many copies of the data should be kept in memory. Default redundancy will
   * be none.
   */
  protected int redundantCopies = 0;

  /**
   * Total number of buckets for a region
   */
  protected int totalNumOfSplits = DEFAULT_TOTAL_NUM_SPLITS;

  protected int numOfColumns = 0;

  /**
   * counter used to assign the position to the new column while you do addColumn.
   */
  protected int columnPosition = 0;

  /**
   * Indicates whether or not Table is configured to use disk based persistence
   */
  protected boolean isDiskPersistenceEnabled;

  /**
   * Set the name of the existing diskstore for MTable disk persistence
   */
  protected String diskStoreName = MTableUtils.DEFAULT_MTABLE_DISK_STORE_NAME;

  protected DiskStoreAttributes diskStoreAttributes = null;

  protected MDiskWritePolicy diskWritePolicy = MDiskWritePolicy.ASYNCHRONOUS;
  protected MEvictionPolicy evictionPolicy = MEvictionPolicy.OVERFLOW_TO_DISK;
  protected MExpirationAttributes expirationAttributes = null;

  /**
   * Max local memory for this table
   */
  protected int localMaxMemory = DEFAULT_LOCAL_MAX_MEMORY;

  /**
   * Max local memory as percentage of max heap
   */
  protected int localMaxMemoryPct = DEFAULT_LOCAL_MAX_MEMORY_PCT;

  /**
   * Number of milliseconds to wait after a member failure before recovering redundancy.
   */
  protected long recoveryDelay = PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT;

  /**
   * Number of milliseconds to wait before triggering a redundancy recovery when a new server joins
   * the cluster.
   */
  protected long startupRecoveryDelay = PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT;

  protected AbstractTableDescriptor() {
    this.columnsByPositions = new LinkedHashMap<>();
    this.columnPosition = 0;
    this.columnsByName = new LinkedHashMap<>();
    this.tableType = TableType.ORDERED_VERSIONED;
    this.localMaxMemory = DEFAULT_LOCAL_MAX_MEMORY;
    this.recoveryDelay = PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT;
    this.startupRecoveryDelay = PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT;
  }

  protected AbstractTableDescriptor(final TableType tableType) {
    this.columnsByPositions = new LinkedHashMap<>();
    this.columnPosition = 0;
    this.columnsByName = new LinkedHashMap<>();
    this.tableType = tableType;
    this.localMaxMemory = DEFAULT_LOCAL_MAX_MEMORY;
    this.recoveryDelay = PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT;
    this.startupRecoveryDelay = PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT;
  }

  protected AbstractTableDescriptor(final Schema schema) {
    this(schema, TableType.ORDERED_VERSIONED);
  }

  protected AbstractTableDescriptor(final Schema schema, final TableType type) {
    this.tableSchema = schema;
    this.tableType = type;
    this.localMaxMemory = DEFAULT_LOCAL_MAX_MEMORY;
    this.recoveryDelay = PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT;
    this.startupRecoveryDelay = PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT;
  }

  @Override
  public Schema getSchema() {
    return this.tableSchema;
  }

  @Override
  public TableDescriptor setSchema(final Schema schema) {
    this.tableSchema = schema;
    return this;
  }

  /**
   * Temporary method to detect whether the columns were added in old way (addColumn) or new way
   * (Schema).
   *
   * @return true if the columns were added via addColumn; false if via Schema
   */
  private boolean isOldWay() {
    return this.columnsByPositions.size() > 0 || this.getSchema() == null;
  }

  /**
   * Returns the Map of columnDescriptor where the map key is the MColumnDescriptor and the value is
   * it's respective position in a row as defined by the schema.
   *
   * @return Column Descriptor Map.
   */
  public Map<MColumnDescriptor, Integer> getColumnDescriptorsMap() {
    if (isOldWay()) {
      return this.columnsByPositions;
    }
    final Map<ByteArrayKey, MColumnDescriptor> cm = this.getSchema().getColumnMap();
    final Map<MColumnDescriptor, Integer> ret = new LinkedHashMap<>(cm.size());
    for (Map.Entry<ByteArrayKey, MColumnDescriptor> entry : cm.entrySet()) {
      ret.put(entry.getValue(), entry.getValue().getIndex());
    }
    return ret;
  }

  /**
   * Get a map of column descriptors keyed by the column name as a byte[] array.
   *
   * @return Map of byte[], MColumnDescriptor containing the columns defined in this table
   *         descriptor.
   */
  public Map<ByteArrayKey, MColumnDescriptor> getColumnsByName() {
    return isOldWay() ? this.columnsByName : this.getSchema().getColumnMap();
  }

  /**
   * Add a column to the table schema.
   *
   * @param colName name of the column to be added.
   * @return Object of this class, used for chained calling.
   */
  public TableDescriptor addColumn(final String colName) throws TableColumnAlreadyExists {
    return this.addColumn(Bytes.toBytes(colName));
  }

  /**
   * Add a column to the table schema.
   *
   * @param colName name of the column to be added.
   * @return Object of this class, used for chained calling.
   */
  public TableDescriptor addColumn(final byte[] colName) throws TableColumnAlreadyExists {
    MColumnDescriptor columnDescriptor = new MColumnDescriptor(colName);
    return this.addColumn(colName, columnDescriptor);
  }

  /**
   * Add a column of a specific type to the table schema.
   *
   * @param colName name of the column to be added.
   * @param type The {@link BasicTypes} for this column; for example MBasicObjectType.STRING.
   * @return Object of this class, used for chained calling.
   */
  public TableDescriptor addColumn(final String colName, BasicTypes type)
      throws TableColumnAlreadyExists {
    MColumnDescriptor columnDescriptor = new MColumnDescriptor(colName, new MTableColumnType(type));
    return this.addColumn(Bytes.toBytes(colName), columnDescriptor);
  }

  /**
   * Add a column of a specific type to the table schema.
   *
   * @param colName name of the column to be added.
   * @param type The {@link BasicTypes} for this column; for example MBasicObjectType.STRING.
   * @return Object of this class, used for chained calling.
   */
  public TableDescriptor addColumn(final byte[] colName, BasicTypes type)
      throws TableColumnAlreadyExists {
    MColumnDescriptor columnDescriptor = new MColumnDescriptor(colName, new MTableColumnType(type));
    return this.addColumn(colName, columnDescriptor);
  }

  /**
   * Add a column of a specific type to the table schema. <br>
   * This method allows a complex type to be defined with a String (see {@link MTableColumnType}.
   * <b>Note on column types: The use of strings to define complex types for
   * {@link MTableColumnType} is for advanced development and in general unless a complex or new
   * type is needed always use one of the types defined in {@link BasicTypes} with the
   * {@link TableDescriptor#addColumn(String, BasicTypes)}.</b>
   *
   * @param colName name of the column to be added.
   * @param columnType The {@link MTableColumnType} for this column; for example new
   *        MTableColumnType(MBasicObjectType.STRING). Complex types may be defined using a properly
   *        formatted String (see {@link MTableColumnType}).
   * @return Object of this class, used for chained calling.
   */
  public TableDescriptor addColumn(final String colName, MTableColumnType columnType)
      throws TableColumnAlreadyExists {
    MColumnDescriptor columnDescriptor = new MColumnDescriptor(colName, columnType);
    return this.addColumn(Bytes.toBytes(colName), columnDescriptor);
  }

  /**
   * Add a column of a specific type to the table schema.
   * <p>
   * This method allows a complex type to be defined with a String (see {@link MTableColumnType}.
   * <b>Note on column types: The use of strings to define complex types for
   * {@link MTableColumnType} is for advanced development and in general unless a complex or new
   * type is needed always use one of the types defined in {@link BasicTypes} with the
   * {@link TableDescriptor#addColumn(String, BasicTypes)}.</b>
   *
   * @param colName name of the column to be added.
   * @param columnType The {@link MTableColumnType} for this column; for example new
   *        MTableColumnType(MBasicObjectType.STRING). Complex types may be defined using a properly
   *        formatted String (see {@link MTableColumnType}).
   * @return Object of this class, used for chained calling.
   */
  public TableDescriptor addColumn(final byte[] colName, MTableColumnType columnType)
      throws TableColumnAlreadyExists {
    MColumnDescriptor columnDescriptor = new MColumnDescriptor(colName, columnType);
    return this.addColumn(colName, columnDescriptor);
  }

  /**
   * @param colName name of the column to be added.
   * @param columnTypeId the object type String for this column; for example "STRING" or "INT".
   *        Complex types may be defined using a String (see {@link MTableColumnType}).
   * @return Object of this class, used for chained calling.
   * @deprecated Add a column of a specific type (as String) to the table schema. Use
   *             {@link TableDescriptor#addColumn(String, MTableColumnType)} instead.
   */
  public TableDescriptor addColumn(final String colName, String columnTypeId)
      throws TableColumnAlreadyExists {
    MColumnDescriptor columnDescriptor = new MColumnDescriptor(colName, columnTypeId);
    return this.addColumn(Bytes.toBytes(colName), columnDescriptor);
  }

  protected TableDescriptor addColumn(final byte[] colName, MColumnDescriptor columnDescriptor) {
    ByteArrayKey byteArrayKey = new ByteArrayKey(colName);
    if (this.columnsByName.get(byteArrayKey) == null) {
      columnDescriptor.setIndex(this.columnPosition);
      this.columnsByPositions.put(columnDescriptor, this.columnPosition);
      this.columnsByName.put(byteArrayKey, columnDescriptor);
      this.numOfColumns++;
      if (columnDescriptor.getColumnType().isFixedLength()) {
        fixedLengthColumnIndices.add(this.columnPosition);
      } else {
        varibleLengthColumnIndices.add(this.columnPosition);
      }
      this.columnPosition++;
    } else {
      throw new TableColumnAlreadyExists(
          "Column " + Arrays.toString(colName) + " Already Exists in Schema. ");
    }
    return this;
  }

  /*
   * @Override public MTableType getTableType() { return this.tableType; }
   */

  /**
   * Returns the number of redundant (extra) copies configured for this table.
   *
   * @return number of redundant copies set for this table.
   */
  public int getRedundantCopies() {
    return this.redundantCopies;
  }

  /**
   * Sets the number of redundant copies of the data for a Table. These copies are used to insure
   * data is available when one or more cache servers are unavailable.
   *
   * @param copies the number of redundant copies
   */
  @Override
  public TableDescriptor setRedundantCopies(int copies) {
    if (copies < 0) {
      throw new IllegalArgumentException("RedundantCopies cannot be negative");
    }
    this.redundantCopies = copies;
    return this;
  }

  /**
   * sets local max memory that can be used by this table on one server. It will reset any non
   * default value of local max memory set as percentage of max heap using
   * {@link #setLocalMaxMemoryPct(int)}.
   *
   * @param memory memory in mega bytes.
   * @return the table descriptor
   */
  @Override
  public TableDescriptor setLocalMaxMemory(int memory) {
    if (memory <= 0) {
      throw new IllegalArgumentException("Value of total-max=memory should be positive");
    }
    localMaxMemoryPct = DEFAULT_LOCAL_MAX_MEMORY_PCT;
    localMaxMemory = memory;
    return this;
  }

  @Override
  public TableDescriptor setLocalMaxMemoryPct(int memoryPct) {
    if (memoryPct <= 0 || memoryPct > 90) {
      throw new IllegalArgumentException(
          "Value of total-max=memory should be greater than 0 and smaller than or equal to 90");
    }
    localMaxMemory = DEFAULT_LOCAL_MAX_MEMORY;
    localMaxMemoryPct = memoryPct;
    return this;
  }

  /**
   * Sets the number of configured hash splits/buckets for a table. Note: Total number of data
   * buckets for a table will be number of configured splits times the number of redundant data
   * copies.
   *
   * @param totalNumOfSplits the total number of splits
   * @throws TableInvalidConfiguration if the specified number of splits are < 0 or >= 512
   */
  public TableDescriptor setTotalNumOfSplits(int totalNumOfSplits)
      throws TableInvalidConfiguration {

    if (totalNumOfSplits <= 0) {
      throw new IllegalArgumentException("TotalNumOfSplits cannot be negative or zero");
    }
    if (totalNumOfSplits >= 512) {
      throw new IllegalArgumentException("TotalNumOfSplits cannot be greater than or equal to 512");
    }
    this.totalNumOfSplits = totalNumOfSplits;

    return this;
  }

  /**
   * Gets the total number of configured hash (unordered table) or range (ordered table) region
   * splits for a table. Note that the total number of data buckets for a table will be number of
   * configured splits times the number of redundant data copies(primary + secondaries).
   *
   * @return total number of region splits for this table.
   */
  public int getTotalNumOfSplits() {
    return this.totalNumOfSplits;
  }

  /**
   * Table name with which this table descriptor is associated with
   *
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Set name of the table this descriptor associated with
   */
  public void setTableName(final String tableName) {
    this.tableName = tableName;
  }


  /**
   * Return the a List of all the columns (as MColumnDescriptor) configured in this table
   * descirptor.
   *
   * @return Return the the List of all the columns (list of MColumnDescriptor).
   */
  public List<MColumnDescriptor> getAllColumnDescriptors() {
    if (isOldWay()) {
      List<MColumnDescriptor> columnList = new ArrayList<>();
      this.columnsByPositions.forEach((MCD, POS) -> columnList.add(MCD));
      return columnList;
    } else {
      return this.tableSchema.getColumnDescriptors();
    }
  }

  /**
   * Return a collection of all columns (as MColumnDescriptor) configured in this table descriptor.
   * The difference from getAllColumnDescriptors is that it does not create an extra copy.
   *
   * @return the collection of all columns
   */
  @Override
  public Collection<MColumnDescriptor> getColumnDescriptors() {
    return isOldWay() ? this.columnsByPositions.keySet() : this.tableSchema.getColumnDescriptors();
  }

  /**
   * Setting by default to false
   *
   * @param policy the disk-write policy to be set for this table
   * @return the table descriptor (this)
   */
  @Override
  public TableDescriptor enableDiskPersistence(MDiskWritePolicy policy) {
    this.isDiskPersistenceEnabled = true;
    this.diskWritePolicy = policy;
    return this;
  }


  /**
   * Sets disk store name, to be used if disk persistence is enabled.
   *
   * @param diskStoreName the name of the disk-store
   */
  public TableDescriptor setDiskStore(String diskStoreName) {
    this.diskStoreName = diskStoreName;
    return this;
  }

  /**
   * Sets disk store attributes, to be used if disk persistence is enabled. Internal Only. Added for
   * internal testing
   *
   * @param diskStoreAttributes the name of the disk-store
   */
  @InterfaceAudience.Private
  public TableDescriptor setDiskStoreAttributes(DiskStoreAttributes diskStoreAttributes) {
    this.diskStoreAttributes = diskStoreAttributes;
    return this;
  }

  /**
   * Get the current disk write policy for Table persistence.
   *
   * @return the disk write policy
   */
  public MDiskWritePolicy getDiskWritePolicy() {
    return diskWritePolicy;
  }

  /**
   * Get the current data eviction policy for the Table.
   *
   * @return the data eviction policy
   */
  public MEvictionPolicy getEvictionPolicy() {
    return evictionPolicy;
  }

  /**
   * Setting expiration attributes for expiring entries from Table based on time See
   * {@link MExpirationAttributes} for expiration attributes
   *
   * @return updated Table Descriptor
   */
  public TableDescriptor setExpirationAttributes(MExpirationAttributes expirationAttributes) {
    this.expirationAttributes = expirationAttributes;
    return this;
  }

  /**
   * Get the current expiration attributes for the Table
   *
   * @return Expiration Attributes
   */
  public MExpirationAttributes getExpirationAttributes() {
    return expirationAttributes;
  }

  /**
   * Gets the flag indicating whether or not disk persistence is enabled for this table.
   *
   * @return true if disk persistence enabled else false.
   */
  public boolean isDiskPersistenceEnabled() {
    return isDiskPersistenceEnabled;
  }

  public int getNumOfColumns() {
    return isOldWay() ? this.columnsByPositions.size() : this.getSchema().getNumberOfColumns();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveInt(this.version, out);
    DataSerializer.writeObject(this.tableName, out);
    DataSerializer.writeEnum(this.tableType, out);
    DataSerializer.writeInteger(this.redundantCopies, out);
    DataSerializer.writeInteger(this.totalNumOfSplits, out);
    DataSerializer.writeInteger(this.columnPosition, out);
    DataSerializer.writeInteger(this.numOfColumns, out);
    DataSerializer.writeObject(this.evictionPolicy, out);
    DataSerializer.writeObject(this.expirationAttributes, out);
    DataSerializer.writeBoolean(this.isDiskPersistenceEnabled, out);
    DataSerializer.writeString(this.diskStoreName, out);
    DataSerializer.writeObject(this.diskWritePolicy, out);
    DataSerializer.writePrimitiveInt(this.localMaxMemory, out);
    if (version >= 1) {
      DataSerializer.writePrimitiveInt(this.localMaxMemoryPct, out);
    }
    DataSerializer.writeObject(this.tableSchema, out);
    DataSerializer.writePrimitiveLong(this.recoveryDelay, out);
    DataSerializer.writePrimitiveLong(this.startupRecoveryDelay, out);
    AMPLDataSerializer.writeLinkedHashMap(this.columnsByPositions, out);
    AMPLDataSerializer.writeLinkedHashMap(this.columnsByName, out);
    DataSerializer.writePrimitiveLong(this.recoveryDelay, out);
    DataSerializer.writePrimitiveLong(this.startupRecoveryDelay, out);
    DataSerializer.writeInteger(this.localMaxMemory, out);
    DataSerializer.writeArrayList(this.fixedLengthColumnIndices, out);
    DataSerializer.writeArrayList(this.varibleLengthColumnIndices, out);
    DataSerializer.writeObject(this.diskStoreAttributes, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.version = DataSerializer.readPrimitiveInt(in);
    this.tableName = DataSerializer.readObject(in);
    this.tableType = DataSerializer.readEnum(TableType.class, in);
    this.redundantCopies = DataSerializer.readInteger(in);
    this.totalNumOfSplits = DataSerializer.readInteger(in);
    this.columnPosition = DataSerializer.readInteger(in);
    this.numOfColumns = DataSerializer.readInteger(in);
    this.evictionPolicy = DataSerializer.readObject(in);
    this.expirationAttributes = DataSerializer.readObject(in);
    this.isDiskPersistenceEnabled = DataSerializer.readBoolean(in);
    this.diskStoreName = DataSerializer.readString(in);
    this.diskWritePolicy = DataSerializer.readObject(in);
    this.localMaxMemory = DataSerializer.readPrimitiveInt(in);
    if (version >= 1) {
      this.localMaxMemoryPct = DataSerializer.readPrimitiveInt(in);
    }
    this.tableSchema = DataSerializer.readObject(in);
    this.recoveryDelay = DataSerializer.readPrimitiveLong(in);
    this.startupRecoveryDelay = DataSerializer.readPrimitiveLong(in);
    this.columnsByPositions = AMPLDataSerializer.readLinkedHashMap(in);
    this.columnsByName = AMPLDataSerializer.readLinkedHashMap(in);
    this.recoveryDelay = DataSerializer.readPrimitiveLong(in);
    this.startupRecoveryDelay = DataSerializer.readPrimitiveLong(in);
    this.localMaxMemory = DataSerializer.readInteger(in);
    if (this.localMaxMemory == -1) {
      /* End of stream: DataInputStream.readInt return -1 in case of EOF */
      throw new IOException("End of stream encountered while expecting data for local-max-memory");
    }
    this.fixedLengthColumnIndices = DataSerializer.readArrayList(in);
    this.varibleLengthColumnIndices = DataSerializer.readArrayList(in);
    this.diskStoreAttributes = DataSerializer.readObject(in);
  }

  /**
   * gets local max memory that can be used by this table on one server.
   *
   * @return local max memory for this table.
   */
  @Override
  public int getLocalMaxMemory() {
    return localMaxMemory;
  }

  @Override
  public int getLocalMaxMemoryPct() {
    return localMaxMemoryPct;
  }

  /**
   * Get the disk store name associated with this table.
   *
   * @return disk store name.
   */
  public String getDiskStore() {
    return diskStoreName;
  }

  /**
   * Get the disk store attributes associated with the disk store attached to this table. Internal
   * Only. Added for internal testing
   *
   * @return disk store name.
   */
  @InterfaceAudience.Private
  public DiskStoreAttributes getDiskStoreAttributes() {
    return diskStoreAttributes;
  }


  /**
   * Gets the recovery delay for the table.
   *
   * recovery delay is the number of milliseconds to wait after a member failure before recovering
   * redundancy. A value of -1 will disable redundancy recovery. default Value: -1
   *
   * @return recovery delay
   */
  public long getRecoveryDelay() {
    return recoveryDelay;
  }

  /**
   * Sets the recovery delay for the table.
   *
   * recovery delay is the number of milliseconds to wait after a member failure before recovering
   * redundancy. A value of -1 will disable redundancy recovery. default Value: -1
   */
  public void setRecoveryDelay(long recoveryDelay) {
    this.recoveryDelay = recoveryDelay;
  }

  /**
   * Sets the startup recovery delay for the table.
   *
   * startup recovery delay is the number of milliseconds to wait after a member joins before
   * recovering redundancy. A value of -1 will disable redundancy recovery. default Value: 0
   */
  public void setStartupRecoveryDelay(long startupRecoveryDelay) {
    this.startupRecoveryDelay = startupRecoveryDelay;
  }

  /**
   * Gets the startup recovery delay for the table.
   *
   * startup recovery delay is the number of milliseconds to wait after a member joins before
   * recovering redundancy. A value of -1 will disable redundancy recovery. default Value: 0
   *
   * @return startup recovery delay
   */
  public long getStartupRecoveryDelay() {
    return startupRecoveryDelay;
  }

  /**
   * Helper method to get the version of the schema.
   *
   * @return returns current version of the schema
   */
  public int getSchemaVersion() {
    return this.schemaVersion;
  }


  @Override
  public TableType getType() {
    return this.tableType;
  }

  public List<Integer> getFixedLengthColumns() {
    return isOldWay() ? fixedLengthColumnIndices : this.tableSchema.getFixedLengthColumnIndices();

  }

  public List<Integer> getVaribleLengthColumns() {
    return isOldWay() ? varibleLengthColumnIndices
        : this.tableSchema.getVaribleLengthColumnIndices();
  }

  public int getBitMapLength() {
    return (int) Math.ceil(((double) this.getNumOfColumns()) / Byte.SIZE);

  }

  public MColumnDescriptor getColumnDescriptorByIndex(int index) {
    return this.getSchema().getColumnDescriptorByIndex(index);
  }

  public boolean isFixedLengthColumn(int columnPosition) {
    return getFixedLengthColumns().contains(columnPosition);
  }

  /**
   * For internal purpose only This method will add columns in schema if not provided by schema way
   */
  @InterfaceAudience.Private
  public void finalizeDescriptor() {
    if (this.getSchema() == null) {
      Schema.Builder sb = new Schema.Builder();
      for (final MColumnDescriptor cd : this.getColumnDescriptors()) {
        sb.column(cd.getColumnNameAsString(), cd.getColumnType());
      }

      if (this instanceof FTableDescriptor) {
        /* add insertion-time as last column in the schema.. */
        sb.column(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            FTableDescriptor.INSERTION_TIMESTAMP_COL_TYPE);
        // also add old way
        // TODO improve this way
        this.addColumn(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            FTableDescriptor.INSERTION_TIMESTAMP_COL_TYPE);
      }
      this.setSchema(sb.build());
    } else {
      if (this instanceof FTableDescriptor) {
        Schema.Builder sb = new Schema.Builder();
        for (final MColumnDescriptor cd : this.getColumnDescriptors()) {
          sb.column(cd.getColumnNameAsString(), cd.getColumnType());
        }
        sb.column(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME,
            FTableDescriptor.INSERTION_TIMESTAMP_COL_TYPE);
        this.setSchema(sb.build());
      }
    }
  }

  /**
   * Returns array in following sequence magic no, encoding, reserved bits. They are used to
   * identify storage formatter associated with this table descriptor.
   *
   * @return Bytes used to instantiated appropriate storage formatter
   */
  public byte[] getStorageFormatterIdentifiers() {
    return this.tableType.getFormat();
  }

  /**
   * Get the column descriptor of the specified column.
   *
   * @param columnName the column name
   * @return the column descriptor
   */
  @Override
  public MColumnDescriptor getColumnByName(final String columnName) {
    return this.tableSchema == null
        ? this.columnsByName.get(new ByteArrayKey(columnName.getBytes()))
        : this.tableSchema.getColumnDescriptorByName(columnName);
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.getEncoding(getType().getFormat()[RowHeader.OFFSET_ENCODING]);
  }
}
