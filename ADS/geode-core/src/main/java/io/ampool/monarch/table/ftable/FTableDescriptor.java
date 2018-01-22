/*
 * ========================================================================= Copyright (c) 2015
 * Ampool, Inc. All Rights Reserved. This product is protected by U.S. and international copyright
 * and intellectual property laws.
 * =========================================================================
 */

package io.ampool.monarch.table.ftable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceAudience.Private;
import io.ampool.classification.InterfaceStability;
import io.ampool.internal.AMPLDataSerializer;
import io.ampool.monarch.table.Admin;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.MDiskWritePolicy;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.MExpirationAttributes;
import io.ampool.monarch.table.MTableColumnType;
import io.ampool.monarch.table.exceptions.TableColumnAlreadyExists;
import io.ampool.monarch.table.exceptions.TableInvalidConfiguration;
import io.ampool.monarch.table.internal.AbstractTableDescriptor;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.MTableUtils;
import io.ampool.monarch.table.internal.TableType;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.orc.OrcUtils;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Version;

/**
 * Table descriptor implementation for Fact table.
 * 
 * @since 1.0.2
 */

/**
 * This class allows user to specify the table schema and attributes during creation of the table.
 * This object is passed to {@link Admin#createFTable(String, FTableDescriptor)}. <br>
 * <br>
 * Note: At this point, some classes associated w/ MTable are also used in common with FTable e.g.
 * MTableColumnType, exceptions like, TableInvalidConfiguration, TableColumnAlreadyExists etc.
 * <p>
 * <P>
 * </P>
 * <p>
 * DEFAULTS:
 * <P>
 * </P>
 * - disk persistence enabled,<br>
 * - Asynchronous disk write policy for persistene,<br>
 * - EvictionPolicy: OVERFLOW_TO_TIER, i.e evict data to local disk(ORC) store when heap usage
 * crosses eviction threshold<br>
 * - No redundancy (redundant copies = 0),<br>
 * - Number of splits = 113,<br>
 * <br>
 * Usage Examples:<br>
 * <br>
 * - Simple table with six columns:<br>
 * <br>
 * List<String> columnNames = Arrays.asList("NAME", "ID", "AGE", "SALARY", "DEPT", "DOJ");<br>
 * FTableDescriptor tableDescriptor = new FTableDescriptor();<br>
 * tableDescriptor.addColumn(Bytes.toBytes("NAME"))<br>
 * &nbsp;&nbsp;&nbsp;&nbsp; .addColumn(Bytes.toBytes("ID"))<br>
 * &nbsp;&nbsp;&nbsp;&nbsp; .addColumn(Bytes.toBytes("AGE"))<br>
 * &nbsp;&nbsp;&nbsp;&nbsp; .addColumn(Bytes.toBytes("SALARY"))<br>
 * &nbsp;&nbsp;&nbsp;&nbsp; .addColumn(Bytes.toBytes("DEPT"))<br>
 * &nbsp;&nbsp;&nbsp;&nbsp; .addColumn(Bytes.toBytes("DOJ"))<br>
 * <br>
 * MAdmin admin = clientCache.getAdmin();<br>
 * String tableName = "EmployeeTable";<br>
 * FTable table = admin.createFTable(tableName, tableDescriptor);<br>
 * <br>
 *
 * @since 1.1.1.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class FTableDescriptor extends AbstractTableDescriptor implements DataSerializable {

  private static final long serialVersionUID = 5645197680770907232L;

  public static final int DEFAULT_BLOCK_SIZE = 1000;

  private ByteArrayKey partitioningColumn;

  private int blockSize = DEFAULT_BLOCK_SIZE;

  private PartitionResolver partitionResolver;

  public static final String INSERTION_TIMESTAMP_COL_NAME = "__INSERTION_TIMESTAMP__";

  public static final BasicTypes INSERTION_TIMESTAMP_COL_TYPE = BasicTypes.LONG;

  private LinkedHashMap<String, TierStoreConfiguration> tierStoreHierarchy = null;
  private boolean isColumnStatisticsEnabled = true;

  public enum BlockFormat {
    AMP_BYTES, AMP_SNAPPY, ORC_BYTES;
  }

  public static final BlockFormat DEFAULT_BLOCK_FORMAT = BlockFormat.AMP_BYTES;
  private BlockFormat blockFormat = DEFAULT_BLOCK_FORMAT;
  private Properties blockProperties = null;
  private String orcSchema = null;

  /**
   * Creates default FTable descriptor. <br>
   * In addition to various columns added by user there will be a column
   * {@link #INSERTION_TIMESTAMP_COL_NAME} for every FTable. This column will hold insertion
   * timestamp for each record as Long value.
   */
  public FTableDescriptor() {
    super(TableType.IMMUTABLE);
    // Ref GEN-2139. By default it should be disabled.
    // Ref GEN-2174. Enabled persistence, by default, again..
    isDiskPersistenceEnabled = true;
    diskStoreName = MTableUtils.DEFAULT_FTABLE_DISK_STORE_NAME;
    diskWritePolicy = MDiskWritePolicy.ASYNCHRONOUS;
    evictionPolicy = MEvictionPolicy.OVERFLOW_TO_TIER;
    expirationAttributes = new MExpirationAttributes();
    tierStoreHierarchy = new LinkedHashMap<>();
  }

  /**
   * Get the name of recovery store associated with FTable.
   *
   * @return disk store name associated with table's recovery store.
   */
  @Private
  public String getRecoveryDiskStore() {
    return super.getDiskStore();
  }

  /**
   * Sets the number of redundant copies of the data for a FTable. These copies are used to insure
   * data is available when one or more cache servers are unavailable.
   *
   * @param copies the number of redundant copies
   * @throws IllegalArgumentException
   */
  @Override
  public FTableDescriptor setRedundantCopies(int copies) {
    return (FTableDescriptor) super.setRedundantCopies(copies);
  }

  /**
   * sets local max memory that can be used by this table on one server.
   *
   * @param memory memory in mega bytes.
   * @return the table descriptor
   */
  @Override
  public FTableDescriptor setLocalMaxMemory(int memory) {
    return (FTableDescriptor) super.setLocalMaxMemory(memory);
  }

  /**
   * Sets the number of configured hash splits/buckets for a table. Note: Total number of data
   * buckets for a table will be number of configured splits times the number of redundant data
   * copies.
   *
   * @param totalNumOfSplits the total number of splits
   * @throws TableInvalidConfiguration if invalid number of splits were specified
   */
  public FTableDescriptor setTotalNumOfSplits(int totalNumOfSplits)
      throws TableInvalidConfiguration {
    return (FTableDescriptor) super.setTotalNumOfSplits(totalNumOfSplits);
  }

  /**
   * Sets recovery disk store name for the table.
   *
   * @param recoveryDiskStoreName the name of the recovery disk-store
   */
  @Private
  public FTableDescriptor setRecoveryDiskStore(String recoveryDiskStoreName) {
    return (FTableDescriptor) super.setDiskStore(recoveryDiskStoreName);
  }

  /**
   * Set column name as byte array from schema that is used for partitioning the data.
   *
   * @param partitioningColumn columnName used for partitioning the data across the ampool
   *        cache-servers.
   * @return reference to the FTableDescriptor.
   */
  public FTableDescriptor setPartitioningColumn(byte[] partitioningColumn) {
    if (Bytes.compareTo(partitioningColumn,
        Bytes.toBytes(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) == 0) {
      throw new IllegalArgumentException("Partitioning column cannot be insertion timestamp");
    }
    this.partitioningColumn = new ByteArrayKey(partitioningColumn);
    return this;
  }

  /**
   * Set column name as String from schema that is used for partitioning the data.
   *
   * @param partitioningColumn columnName used for partitioning the data across the ampool
   *        cache-servers.
   * @return reference to the FTableDescriptor.
   */
  public FTableDescriptor setPartitioningColumn(final String partitioningColumn) {
    if (partitioningColumn.equals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
      throw new IllegalArgumentException("Partitioning column cannot be insertion timestamp");
    }
    this.partitioningColumn = new ByteArrayKey(Bytes.toBytes(partitioningColumn));
    return this;
  }

  /**
   * Get a columnName used for partitioning the data.
   *
   * @return columnName used for partitioning the data.
   */
  public ByteArrayKey getPartitioningColumn() {
    return this.partitioningColumn;
  }

  /**
   * Set a custom partition-resolver that overrides the default (hash-code based) partitioning
   * scheme. User can implement the <code>PartitionResolver</code> interface to have different
   * partitioning scheme.
   *
   * @param resolver the partition resolver
   * @return reference to the FTableDescriptor.
   */
  @InterfaceAudience.Public
  public FTableDescriptor setPartitionResolver(PartitionResolver resolver) {
    this.partitionResolver = resolver;
    return this;
  }

  /**
   * Gets blocksize used for storing the number of ftable records in one block.
   *
   * @return blocksize.
   */
  public int getBlockSize() {
    return blockSize;
  }

  /**
   * Sets block size for the ftable records. Blocksize is used for storing number of records in one
   * block
   *
   * @param blockSize the block size to be used for in-memory tier
   * @throws IllegalArgumentException if policy is set to less than or equal to 0
   */
  public void setBlockSize(final int blockSize) {
    if (blockSize <= 0) {
      throw new IllegalArgumentException("BlockSize cannot be negative or Zero");
    }
    this.blockSize = blockSize;
  }

  /**
   * Get a custom partition resolver if any used while creating a table. If user has not set the
   * partition resolver default used is hash partitioning.
   *
   * @return reference to the PartitionResolver or null if not set explicitly using
   *         {@link FTableDescriptor#setPartitionResolver(PartitionResolver)}.
   */
  public PartitionResolver getPartitionResolver() {
    return this.partitionResolver;
  }

  /**
   * Add a column to the table schema.
   *
   * @param colName name of the column to be added.
   * @return Object of this class, used for chained calling.
   */
  public FTableDescriptor addColumn(final byte[] colName) throws TableColumnAlreadyExists {
    return (FTableDescriptor) super.addColumn(colName);
  }

  /**
   * Add a column to the table schema.
   *
   * @param colName name of the column to be added.
   * @return Object of this class, used for chained calling.
   */
  public FTableDescriptor addColumn(final String colName) throws TableColumnAlreadyExists {
    return (FTableDescriptor) super.addColumn(colName);
  }

  /**
   * Add a column of a specific type to the table schema.
   *
   * @param colName name of the column to be added.
   * @param type The {@link BasicTypes} for this column; for example MBasicObjectType.STRING.
   * @return Object of this class, used for chained calling.
   */
  public FTableDescriptor addColumn(final String colName, BasicTypes type)
      throws TableColumnAlreadyExists {
    return (FTableDescriptor) super.addColumn(colName, type);
  }

  /**
   * Add a column of a specific type to the table schema.
   *
   * @param colName name of the column to be added.
   * @param type The {@link BasicTypes} for this column; for example MBasicObjectType.STRING.
   * @return Object of this class, used for chained calling.
   */
  public FTableDescriptor addColumn(final byte[] colName, BasicTypes type)
      throws TableColumnAlreadyExists {
    return (FTableDescriptor) super.addColumn(colName, type);
  }

  /**
   * Add a column of a specific type to the table schema. <br>
   * This method allows a complex type to be defined with a String (see {@link MTableColumnType}.
   * <b>Note on column types: The use of strings to define complex types for
   * {@link MTableColumnType} is for advanced development and in general unless a complex or new
   * type is needed always use one of the types defined in {@link BasicTypes} with the
   * {@link FTableDescriptor#addColumn(String, BasicTypes)}.</b>
   *
   * @param colName name of the column to be added.
   * @param columnType The {@link MTableColumnType} for this column; for example new
   *        MTableColumnType(MBasicObjectType.STRING). Complex types may be defined using a properly
   *        formatted String (see {@link MTableColumnType}).
   * @return Object of this class, used for chained calling.
   */
  public FTableDescriptor addColumn(final String colName, MTableColumnType columnType)
      throws TableColumnAlreadyExists {
    return (FTableDescriptor) super.addColumn(colName, columnType);
  }

  /**
   * Add a column of a specific type to the table schema.
   * <p>
   * This method allows a complex type to be defined with a String (see {@link MTableColumnType}.
   * <b>Note on column types: The use of strings to define complex types for
   * {@link MTableColumnType} is for advanced development and in general unless a complex or new
   * type is needed always use one of the types defined in {@link BasicTypes} with the
   * {@link FTableDescriptor#addColumn(String, BasicTypes)}.</b>
   *
   * @param colName name of the column to be added.
   * @param columnType The {@link MTableColumnType} for this column; for example new
   *        MTableColumnType(MBasicObjectType.STRING). Complex types may be defined using a properly
   *        formatted String (see {@link MTableColumnType}).
   * @return Object of this class, used for chained calling.
   */
  public FTableDescriptor addColumn(final byte[] colName, MTableColumnType columnType)
      throws TableColumnAlreadyExists {
    return (FTableDescriptor) super.addColumn(colName, columnType);
  }

  /**
   * @param colName name of the column to be added.
   * @param columnTypeId the object type String for this column; for example "STRING" or "INT".
   *        Complex types may be defined using a String (see {@link MTableColumnType}).
   * @return Object of this class, used for chained calling.
   * @deprecated Add a column of a specific type (as String) to the table schema. Use
   *             {@link FTableDescriptor#addColumn(String, MTableColumnType)} instead.
   */
  public FTableDescriptor addColumn(final String colName, String columnTypeId)
      throws TableColumnAlreadyExists {
    return (FTableDescriptor) super.addColumn(colName, columnTypeId);
  }

  /**
   * Set the policy on how to evict data when the eviction heap percentage (a server level
   * configuration) is reached. See {@link MEvictionPolicy} for allowed policies. For Table, By
   * default {@link MEvictionPolicy#OVERFLOW_TO_TIER} is set. This enables data to evict to tiered
   * storage If user choose to overwrite the default with {@link MEvictionPolicy#NO_ACTION}, data
   * will not be evicted to tiered storage.
   *
   * @param policy the eviction policy.
   * @return this FTableDescriptor object.
   * @throws IllegalArgumentException if policy is {@link MEvictionPolicy#OVERFLOW_TO_DISK}.
   * @see MEvictionPolicy
   */
  public FTableDescriptor setEvictionPolicy(MEvictionPolicy policy) {
    if (MEvictionPolicy.OVERFLOW_TO_DISK.equals(policy)) {
      throw new IllegalArgumentException("Specified EvictionPolicy not applicable for FTable!");
    }
    this.evictionPolicy = policy;
    return this;
  }

  /**
   * Add tier store hierarchy. This is applicable only if {@link MEvictionPolicy#OVERFLOW_TO_TIER}
   * is set.
   *
   * @param stores - Map of store names already created vs tier store configuration.
   * @return Object of this class, used for chained calling.
   * @throws IllegalArgumentException : If any tier looping is observed
   */
  public FTableDescriptor addTierStores(LinkedHashMap<String, TierStoreConfiguration> stores)
      throws IllegalArgumentException {
    this.tierStoreHierarchy = stores;
    return this;
  }

  /**
   * Returns tier store hierarchy if set. Otherwise returns null;
   *
   * @return tier store hierarchy if set
   */
  public LinkedHashMap<String, TierStoreConfiguration> getTierStores() {
    return this.tierStoreHierarchy;
  }

  private boolean isSameTierRepeated(final String[] stores) {
    if (stores.length != Arrays.stream(stores).distinct().count()) {
      return true;
    }
    return false;
  }

  // /**
  // * Get type of the table.
  // *
  // * @return Returns the type {@link FTable} of the table
  // */
  // public TableType getTableType() {
  // return tableType;
  // }

  /**
   * Serialize the this FTableDescriptor Class
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.partitioningColumn, out);
    DataSerializer.writeObject(this.partitionResolver, out);
    DataSerializer.writeInteger(this.blockSize, out);
    AMPLDataSerializer.writeLinkedHashMap(tierStoreHierarchy, out);
    DataSerializer.writeEnum(this.blockFormat, out);
    DataSerializer.writeProperties(this.blockProperties, out);
    DataSerializer.writeBoolean(this.isColumnStatisticsEnabled, out);
  }

  /**
   * Deserialize this FTableDescriptor Class
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.partitioningColumn = DataSerializer.readObject(in);
    this.partitionResolver = DataSerializer.readObject(in);
    this.blockSize = DataSerializer.readInteger(in);
    this.tierStoreHierarchy = AMPLDataSerializer.readLinkedHashMap(in);
    this.blockFormat = DataSerializer.readEnum(BlockFormat.class, in);
    this.blockProperties = DataSerializer.readProperties(in);
    this.isColumnStatisticsEnabled = DataSerializer.readBoolean(in);
  }

  /**
   * For Internal usage
   */
  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  private int insertionTimeOffset = -1;

  public int getOffsetToStoreInsertionTS() {
    return insertionTimeOffset < 0 ? calcInsertionTimeOffset() : insertionTimeOffset;
  }

  private int calcInsertionTimeOffset() {
    List<Integer> fixedLengthColumnIndices = this.tableSchema.getFixedLengthColumnIndices();
    int maxLength = 0;
    for (int i = 0; i < fixedLengthColumnIndices.size(); i++) {
      MColumnDescriptor columnDescriptorByIndex =
          this.getColumnDescriptorByIndex(fixedLengthColumnIndices.get(i));
      if (columnDescriptorByIndex.getColumnNameAsString()
          .equals(FTableDescriptor.INSERTION_TIMESTAMP_COL_NAME)) {
        return maxLength;
      } else if (columnDescriptorByIndex.getColumnType().isFixedLength()) {
        maxLength += columnDescriptorByIndex.getColumnType().lengthOfByteArray();
      }
    }
    insertionTimeOffset = maxLength;
    return maxLength;
  }

  public int getMaxLengthForFixedSizeColumns() {
    List<Integer> fixedLengthColumnIndices = this.tableSchema.getFixedLengthColumnIndices();
    int maxLength = 0;
    for (int i = 0; i < fixedLengthColumnIndices.size(); i++) {
      MColumnDescriptor columnDescriptorByIndex =
          this.getColumnDescriptorByIndex(fixedLengthColumnIndices.get(i));
      maxLength += columnDescriptorByIndex.getColumnType().lengthOfByteArray();
    }
    return maxLength;

  }

  /**
   * Set the format to be used for closed blocks. The block is considered closed once it reaches the
   * specified block-size.
   *
   * @param fmt the block format for closed blocks
   * @return this
   */
  public FTableDescriptor setBlockFormat(final BlockFormat fmt) {
    this.blockFormat = fmt == null ? DEFAULT_BLOCK_FORMAT : fmt;
    return this;
  }

  /**
   * Get the block-format configured for this table.
   *
   * @return the block format for closed blocks
   */
  public BlockFormat getBlockFormat() {
    return this.blockFormat;
  }

  /**
   * Set the additional block properties that could be leveraged at the time of closing the blocks.
   *
   * @param props the properties
   * @return this
   */
  public FTableDescriptor setBlockProperties(final Properties props) {
    this.blockProperties = props;
    return this;
  }

  /**
   * Return whether or not the column statistics are enabled per column per block.
   *
   * @return true if column statistics per block are enabled; false otherwise
   */
  public boolean isColumnStatisticsEnabled() {
    return isColumnStatisticsEnabled;
  }

  /**
   * Enable or disable the column statistics per column per block. The min and max values are
   * maintained per column per block. In case of filtered scan these help eliminate the whole block
   * without interpreting.
   *
   * @param isColumnStatisticsEnabled whether or not to enable the column statistics
   */
  public void setColumnStatisticsEnabled(final boolean isColumnStatisticsEnabled) {
    this.isColumnStatisticsEnabled = isColumnStatisticsEnabled;
  }

  /**
   * Get the additional block properties.
   *
   * @return the block properties
   */
  public Properties getBlockProperties() {
    return this.blockProperties;
  }

  public String getOrcSchema() {
    if (this.orcSchema == null) {
      this.orcSchema = OrcUtils.getOrcSchema(getSchema());
    }
    return this.orcSchema;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("FTableDescriptor{");
    sb.append("version=").append(version);
    sb.append(", tableName='").append(tableName).append('\'');
    sb.append(", columnsByPositions=").append(columnsByPositions);
    sb.append(", columnsByName=").append(columnsByName);
    sb.append(", tableSchema=").append(tableSchema);
    sb.append(", fixedLengthColumnIndices=").append(fixedLengthColumnIndices);
    sb.append(", varibleLengthColumnIndices=").append(varibleLengthColumnIndices);
    sb.append(", schemaVersion=").append(schemaVersion);
    sb.append(", tableType=").append(tableType);
    sb.append(", redundantCopies=").append(redundantCopies);
    sb.append(", totalNumOfSplits=").append(totalNumOfSplits);
    sb.append(", numOfColumns=").append(numOfColumns);
    sb.append(", columnPosition=").append(columnPosition);
    sb.append(", isDiskPersistenceEnabled=").append(isDiskPersistenceEnabled);
    sb.append(", diskStoreName='").append(diskStoreName).append('\'');
    sb.append(", diskWritePolicy=").append(diskWritePolicy);
    sb.append(", evictionPolicy=").append(evictionPolicy);
    sb.append(", expirationAttributes=").append(expirationAttributes);
    sb.append(", localMaxMemory=").append(localMaxMemory);
    sb.append(", localMaxMemoryPct=").append(localMaxMemoryPct);
    sb.append(", partitioningColumn=").append(partitioningColumn);
    sb.append(", blockSize=").append(blockSize);
    sb.append(", partitionResolver=").append(partitionResolver);
    sb.append(", recoveryDelay=").append(recoveryDelay);
    sb.append(", startupRecoveryDelay=").append(startupRecoveryDelay);
    sb.append(", tierStoreHierarchy=").append(tierStoreHierarchy);
    sb.append(", blockFormat=").append(blockFormat);
    sb.append(", isColumnStatisticsEnabled=").append(isColumnStatisticsEnabled);
    sb.append('}');
    return sb.toString();
  }
}
