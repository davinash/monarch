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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.DataTypeFactory;
import io.ampool.monarch.types.StructType;
import io.ampool.monarch.types.interfaces.DataType;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataSerializable;


public class Schema implements VersionedDataSerializable {
  /**
   * The list of column descriptors and the map that has column-to-index map mapping.
   */
  private List<MColumnDescriptor> columnList = Collections.emptyList();
  private Object2IntLinkedOpenHashMap<String> columnToIndexMap =
      new Object2IntLinkedOpenHashMap<>();
  /* Duplicate map with ByteArrayKey just to make older lookups work.. remove this later */
  private Map<ByteArrayKey, MColumnDescriptor> columnMapDuplicate = Collections.emptyMap();

  protected List<Integer> fixedLengthColumnIndices = new ArrayList<>();
  protected List<Integer> varibleLengthColumnIndices = new ArrayList<>();

  public Schema() {
    /// for DataSerializable..
  }

  /**
   * Populate schema from the specified column names with column type {@link BasicTypes#BINARY}. The
   * columns will retain their order in the schema.
   *
   * @param columns the list of column names
   */
  public Schema(final String[] columns) {
    this(columns, Collections.nCopies(columns.length, BasicTypes.BINARY).toArray(new DataType[0]));
  }

  /**
   * Populate the schema from the specified column names and the respective column types. The order
   * of columns will be retained in the final schema.
   *
   * @param columns the list of column names
   * @param types the respective column types
   */
  public Schema(final String[] columns, final DataType[] types) {
    if (columns.length == 0) {
      throw new IllegalArgumentException("Schema: Must have at least one column.");
    }
    if (columns.length != types.length) {
      throw new IllegalArgumentException("Schema: Column names and types must be of same length.");
    }
    this.columnList = new ArrayList<>(columns.length);
    this.columnMapDuplicate = new LinkedHashMap<>(columns.length);
    for (int i = 0; i < columns.length; i++) {
      final MColumnDescriptor cd = new MColumnDescriptor(columns[i], types[i], i);
      this.columnList.add(cd);
      this.columnToIndexMap.put(columns[i], i);
      this.columnMapDuplicate.put(new ByteArrayKey(Bytes.toBytes(columns[i])), cd);
      if (cd.getColumnType().isFixedLength()) {
        this.fixedLengthColumnIndices.add(i);
      } else {
        this.varibleLengthColumnIndices.add(i);
      }

    }
    this.columnList = Collections.unmodifiableList(this.columnList);
    this.columnMapDuplicate = Collections.unmodifiableMap(this.columnMapDuplicate);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append(StructType.NAME).append(DataType.COMPLEX_TYPE_BEGIN_CHAR);
    for (MColumnDescriptor cd : getColumnDescriptors()) {
      sb.append(cd.getColumnNameAsString()).append(DataType.COMPLEX_TYPE_NAME_TYPE_SEPARATOR)
          .append(cd.getColumnType()).append(DataType.COMPLEX_TYPE_SEPARATOR);
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append(DataType.COMPLEX_TYPE_END_CHAR);
    return sb.toString();
  }

  /**
   * Create the schema from a specified string. The provided string is a representation like a
   * struct-type. For example, struct<column_1:INT,column_2:LONG>
   *
   * @param schemaStr the schema to be used for creating the table
   */
  public static Schema fromString(final String schemaStr) {
    StructType struct = (StructType) DataTypeFactory.getTypeFromString(schemaStr);
    return new Schema(struct.getColumnNames(), struct.getColumnTypes());
  }

  /**
   * Returns the versions where this classes serialized form was modified. Versions returned by this
   * method are expected to be in increasing ordinal order from 0 .. N. For instance,<br>
   * Version.GFE_7_0, Version.GFE_7_0_1, Version.GFE_8_0, Version.GFXD_1_1<br>
   * <p>
   * You are expected to implement toDataPre_GFE_7_0_0_0(), fromDataPre_GFE_7_0_0_0(), ...,
   * toDataPre_GFXD_1_1_0_0, fromDataPre_GFXD_1_1_0_0.
   * <p>
   * The method name is formed with the version's product name and its major, minor, release and
   * patch numbers.
   */
  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveInt(this.columnList.size(), out);
    for (final MColumnDescriptor cd : this.columnList) {
      DataSerializer.writeObject(cd, out);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    final int size = DataSerializer.readPrimitiveInt(in);
    if (size == 0) {
      this.columnList = Collections.emptyList();
      this.columnMapDuplicate = Collections.emptyMap();
    } else {
      this.columnList = new ArrayList<>(size);
      this.columnMapDuplicate = new LinkedHashMap<>(size);
      MColumnDescriptor cd;
      for (int i = 0; i < size; i++) {
        cd = DataSerializer.readObject(in);
        this.columnList.add(cd);
        this.columnToIndexMap.put(cd.getColumnNameAsString(), i);
        this.columnMapDuplicate.put(new ByteArrayKey(cd.getColumnName()), cd);
        if (cd.getColumnType().isFixedLength()) {
          this.fixedLengthColumnIndices.add(i);
        } else {
          this.varibleLengthColumnIndices.add(i);
        }
      }
      this.columnList = Collections.unmodifiableList(this.columnList);
      this.columnMapDuplicate = Collections.unmodifiableMap(this.columnMapDuplicate);
    }
  }

  /**
   * Get the column descriptor for the specified column name.
   *
   * @param columnName the column name
   * @return the column descriptor
   */
  public MColumnDescriptor getColumnDescriptorByName(final String columnName) {
    return this.columnList.get(this.columnToIndexMap.get(columnName));
  }

  /**
   * Get the column descriptor at the specified index from the schema. It will be slower as compared
   * to the {@link Schema#getColumnDescriptorByName(String)} as the
   *
   * @param index the index of the column in schema
   * @return the column descriptor
   */
  public MColumnDescriptor getColumnDescriptorByIndex(final int index) {
    return this.columnList.get(index);
  }

  /**
   * Get the list of column descriptors from the schema.
   *
   * @return the list of column descriptors
   */
  public List<MColumnDescriptor> getColumnDescriptors() {
    return this.columnList;
  }

  /**
   * Whether or not the specified column is part of the schema.
   *
   * @param columnName the column name
   * @return true if the column is part of the schema; false otherwise
   */
  public boolean containsColumn(final String columnName) {
    return this.columnToIndexMap.containsKey(columnName);
  }

  /* for column-names with byte-array.. to be removed when we don't need it. */
  private static final ThreadLocal<ByteArrayKey> tlKey = ThreadLocal.withInitial(ByteArrayKey::new);

  /**
   * Whether or not the specified column is part of the schema. For the older way, where column
   * names were byte-array.
   *
   * @param columnName the byte-array corresponding to the column name
   * @return true if the column is part of the schema; false otherwise
   */
  public boolean containsColumn(final byte[] columnName) {
    final ByteArrayKey bak = tlKey.get();
    bak.setData(columnName);
    return this.columnMapDuplicate.containsKey(bak);
  }

  /**
   * Get the column-descriptor map defined using column name using byte-array as key. Added this to
   * make it work with APIs referring to column names as byte-array.
   *
   * @return the map of column descriptors
   */
  public Map<ByteArrayKey, MColumnDescriptor> getColumnMap() {
    return this.columnMapDuplicate;
  }

  /**
   * Return the total number of columns in the schema.
   *
   * @return the total number of column in the schema
   */
  public int getNumberOfColumns() {
    return this.columnList.size();
  }

  /**
   * Return Fixed length columns indices
   * 
   * @return Return Fixed length columns indices
   */
  public List<Integer> getFixedLengthColumnIndices() {
    return fixedLengthColumnIndices;
  }

  /**
   * Return variable length columns indices
   * 
   * @return Return variable length columns indices
   */
  public List<Integer> getVaribleLengthColumnIndices() {
    return varibleLengthColumnIndices;
  }

  /**
   * The schema builder. It can be used to add columns one by one and then create the schema in the
   * end.
   */
  public static final class Builder {
    private List<String> cNameList = new ArrayList<>();
    private List<DataType> cTypeList = new ArrayList<>();

    /**
     * Add a column of {@link BasicTypes#BINARY} type to the table schema.
     * <p>
     *
     * @param columnName the column name
     * @return self object
     */
    public Builder column(final String columnName) {
      return column(columnName, BasicTypes.BINARY);
    }

    /**
     * Add a column of a specific type, represented as string, to the table schema.
     * <p>
     *
     * @param columnName the column name.
     * @param columnTypeStr the string representation of the column type
     * @return self object
     */
    public Builder column(final String columnName, final String columnTypeStr) {
      return column(columnName, DataTypeFactory.getTypeFromString(columnTypeStr));
    }

    /**
     * Add a column of a specific type to the table schema.
     * <p>
     *
     * @param columnName the column name.
     * @param columnType the column type
     * @return self object
     */
    public Builder column(final String columnName, final DataType columnType) {
      this.cNameList.add(columnName);
      this.cTypeList.add(columnType);
      return this;
    }

    /**
     * Create the table schema using the provided columns.
     *
     * @return the table schema
     */
    public Schema build() {
      final String[] cNames = this.cNameList.toArray(new String[this.cNameList.size()]);
      final DataType[] cTypes = this.cTypeList.toArray(new DataType[this.cTypeList.size()]);
      return new Schema(cNames, cTypes);
    }
  }
}
