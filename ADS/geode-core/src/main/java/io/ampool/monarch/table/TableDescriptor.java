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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.exceptions.TableColumnAlreadyExists;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.TableType;
import io.ampool.monarch.types.BasicTypes;
import org.apache.geode.internal.VersionedDataSerializable;

/**
 * Reference to a single Table Descriptor tableDescriptor.
 *
 * @since 1.0.2
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface TableDescriptor extends VersionedDataSerializable {

  /**
   * Add a column to the table schema.
   *
   * @param colName name of the column to be added.
   * @return Object of this class, used for chained calling.
   */
  public TableDescriptor addColumn(final byte[] colName) throws TableColumnAlreadyExists;

  /**
   * Add a column to the table schema.
   *
   * @param colName name of the column to be added.
   * @return Object of this class, used for chained calling.
   */
  public TableDescriptor addColumn(final String colName) throws TableColumnAlreadyExists;

  /**
   * Add a column of a specific type to the table schema. Although columns are stored as byte[]
   * type, if the underlying type that the bytes correspond to is known to the MTable certain
   * efficiencies may result and for some persistent stores this type may be required.
   *
   * @param colName name of the column to be added.
   * @param type The {@link BasicTypes} for this column; for example MBasicObjectType.STRING.
   * @return Object of this class, used for chained calling.
   */
  public TableDescriptor addColumn(final String colName, BasicTypes type)
      throws TableColumnAlreadyExists;

  /**
   * Add a column of a specific type to the table schema. Although columns are stored as byte[]
   * type, if the underlying type that the bytes correspond to is known to the MTable certain
   * efficiencies may result and for some persistent stores this type may be required.
   *
   * @param colName name of the column to be added.
   * @param type The {@link BasicTypes} for this column; for example MBasicObjectType.STRING.
   * @return Object of this class, used for chained calling.
   */
  public TableDescriptor addColumn(final byte[] colName, BasicTypes type)
      throws TableColumnAlreadyExists;

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
  public TableDescriptor addColumn(final String colName, MTableColumnType columnType)
      throws TableColumnAlreadyExists;

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
  public TableDescriptor addColumn(final byte[] colName, MTableColumnType columnType)
      throws TableColumnAlreadyExists;

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
  public TableDescriptor addColumn(final String colName, String columnTypeId)
      throws TableColumnAlreadyExists;


  Schema getSchema();

  TableDescriptor setSchema(final Schema schema);

  /**
   * Sets the number of redundant copies of the data for a table. These copies are used to insure
   * data is available even in the case one or more cache servers become unavailable.
   *
   * @param copies the number of redundant copies
   */
  TableDescriptor setRedundantCopies(int copies);

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
  TableDescriptor setTotalNumOfSplits(int totalNumOfSplits);

  /**
   * The state is written to disk and recovered from disk when the table is created. The writing to
   * the disk can be either synchronous or asynchronous and can be specified by the policy.
   *
   * @param policy the disk-write policy to be set for this table
   * @return the table descriptor
   *
   * @see MDiskWritePolicy
   */
  TableDescriptor enableDiskPersistence(MDiskWritePolicy policy);

  /**
   * Sets disk store name, to be used if disk persistence is enabled.
   *
   * @param diskStoreName the name of the disk-store
   */
  TableDescriptor setDiskStore(String diskStoreName);

  /**
   * Set the policy on how to evict data when the eviction heap percentage (a server level
   * configuration) is reached. See {@link MEvictionPolicy} for allowed policies.
   *
   * Set {@link MEvictionPolicy#OVERFLOW_TO_DISK}, to evict data to configured disk storage. Set
   * {@link MEvictionPolicy#NO_ACTION}, For no eviction action. Eviction of type
   * {@link MEvictionPolicy#OVERFLOW_TO_TIER} not applicable for MTable.
   *
   * @param policy the eviction policy
   * @return this MTableDescriptor object.
   * @throws IllegalArgumentException if policy is {@link MEvictionPolicy#OVERFLOW_TO_TIER}
   * @see MEvictionPolicy
   */
  TableDescriptor setEvictionPolicy(MEvictionPolicy policy);

  /**
   * Returns the number of redundant (extra) copies configured for this table.
   *
   * @return number of redundant copies set for this table.
   */
  public int getRedundantCopies();

  /**
   * Gets the total number of splits for a table. Note that the total number of data buckets for a
   * table will be number of configured splits times the number of redundant data copies.
   *
   * @return total number of region splits for this table.
   */
  public int getTotalNumOfSplits();

  /**
   * Returns the Map of columnDescriptor where the map key is the MColumnDescriptor and the value is
   * it's respective position in a row as defined by the schema.
   *
   * @return Column Descriptor Map.
   */
  public Map<MColumnDescriptor, Integer> getColumnDescriptorsMap();

  /**
   * <em>INTERNAL</em> Set name of the table this descriptor associated with
   * 
   * @param tableName
   */
  @InterfaceAudience.Private
  void setTableName(final String tableName);

  /**
   * Table name with which this table descriptor is associated with
   * 
   * @return the table name
   */
  String getTableName();

  /**
   * Get the total number of columns in the table-descriptor.
   *
   * @return the total number of columns
   */
  int getNumOfColumns();

  /**
   * Return the a List of all the columns (as MColumnDescriptor) configured in this table
   * descirptor.
   *
   * @return Return the the List of all the columns (list of MColumnDescriptor).
   */
  List<MColumnDescriptor> getAllColumnDescriptors();

  /**
   * Return a collection of all columns (as MColumnDescriptor) configured in this table descriptor.
   * The difference from getAllColumnDescriptors is that it does not create an extra copy.
   *
   * @return the collection of all columns
   */
  Collection<MColumnDescriptor> getColumnDescriptors();

  /**
   * Get a map of column descriptors keyed by teh column name as a byte[] array.
   *
   * @return Map of byte[], MColumnDescriptor containing the columns defined in this table
   *         descriptor.
   */
  Map<ByteArrayKey, MColumnDescriptor> getColumnsByName();

  /**
   * Sets local max memory for the table. Maximum megabytes of memory set aside for this table in
   * one member. This is all memory used by all splits for the table - for primary and any redundant
   * copies on that server. The value must be smaller than the initial heap provided for the server.
   * This will reset any non default value of local max memory set as percentage of max heap using
   * {@link #setLocalMaxMemoryPct(int)}.
   *
   * @param memory
   * @return table descriptor
   */
  TableDescriptor setLocalMaxMemory(int memory);

  /**
   * Sets local max memory for the table as percentage of max heap. This is all memory used by all
   * splits for the table - for primary and any redundant copies on that server. The value must be
   * smaller than 100. This will reset any non default value of local max memory set using
   * {@link #setLocalMaxMemory(int)}.
   *
   * @param memoryPct
   * @return table descriptor
   */
  TableDescriptor setLocalMaxMemoryPct(int memoryPct);

  /**
   * Get the local max memory assigned to this table.
   * 
   * @return value of max local memory for this table
   */
  int getLocalMaxMemory();

  /**
   * Get local max memory specified as percentage of max heap.
   * 
   * @return value of local max memory for this table in terms of percentage of max heap.
   */
  int getLocalMaxMemoryPct();

  TableType getType();

  /**
   * Gets the recovery delay for the table.
   *
   * recovery delay is the number of milliseconds to wait after a member failure before recovering
   * redundancy. A value of -1 will disable redundancy recovery. default Value: -1
   * 
   * @return recovery delay
   */
  long getRecoveryDelay();

  /**
   * Sets the recovery delay for the table.
   *
   * recovery delay is the number of milliseconds to wait after a member failure before recovering
   * redundancy. A value of -1 will disable redundancy recovery. default Value: -1
   * 
   * @param recoveryDelay
   */
  void setRecoveryDelay(long recoveryDelay);

  /**
   * Gets the startup recovery delay for the table.
   *
   * startup recovery delay is the number of milliseconds to wait after a member joins before
   * recovering redundancy. A value of -1 will disable redundancy recovery. default Value: 0
   * 
   * @return startup recovery delay
   */
  long getStartupRecoveryDelay();

  /**
   * Sets the startup recovery delay for the table.
   *
   * startup recovery delay is the number of milliseconds to wait after a member joins before
   * recovering redundancy. A value of -1 will disable redundancy recovery. default Value: 0
   * 
   * @param startupRecoveryDelay
   */
  void setStartupRecoveryDelay(long startupRecoveryDelay);

  /**
   * Get the column descriptor of the specified column.
   *
   * @param columnName the column name
   * @return the column descriptor
   */
  MColumnDescriptor getColumnByName(final String columnName);

  Encoding getEncoding();
}
