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

package io.ampool.orc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.monarch.types.interfaces.DataType;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.ColumnStatisticsImpl;


public class AColumnStatistics implements DataSerializable {
  private static final Map<DataType, TypeDescription> ORC_TYPE_MAP =
      new HashMap<DataType, TypeDescription>(15) {
        {
          put(BasicTypes.BOOLEAN, TypeDescription.createBoolean());

          put(BasicTypes.BYTE, TypeDescription.createByte());
          put(BasicTypes.SHORT, TypeDescription.createShort());
          put(BasicTypes.INT, TypeDescription.createInt());
          put(BasicTypes.LONG, TypeDescription.createLong());

          put(BasicTypes.CHAR, TypeDescription.createString());
          put(BasicTypes.CHARS, TypeDescription.createString());
          put(BasicTypes.VARCHAR, TypeDescription.createString());
          put(BasicTypes.STRING, TypeDescription.createString());

          put(BasicTypes.FLOAT, TypeDescription.createFloat());
          put(BasicTypes.DOUBLE, TypeDescription.createDouble());

          put(BasicTypes.BIG_DECIMAL, TypeDescription.createDecimal());

          put(BasicTypes.DATE, TypeDescription.createDate());
          put(BasicTypes.TIMESTAMP, TypeDescription.createTimestamp());
        }
      };

  private ColumnStatisticsImpl[] stats;

  @SuppressWarnings("unused")
  public AColumnStatistics() {
    // for DataSerializable
  }

  /**
   * Create the wrapper that holds column statistics for the specified table descriptor.
   *
   * @param td the table descriptor
   */
  public AColumnStatistics(final TableDescriptor td) {
    int j = 0;
    stats = new ColumnStatisticsImpl[td.getNumOfColumns()];
    for (final MColumnDescriptor cd : td.getColumnDescriptors()) {
      final TypeDescription d = ORC_TYPE_MAP.get(cd.getColumnType());
      if (d != null) {
        stats[j] = ColumnStatisticsImpl.create(d);
      }
      j++;
    }
  }

  /**
   * Update the column statistics from the specified row.
   *
   * @param row the row
   */
  public void updateRowStatistics(final List<MColumnDescriptor> cds, final Row row) {
    for (int i = 0; i < row.size(); i++) {
      final DataType type = cds.get(i).getColumnType();
      if (!ORC_TYPE_MAP.containsKey(type)) {
        continue;
      }
      updateColumnStatistics((BasicTypes) type, row.getValue(i), i);
    }
  }

  /**
   * Update the column statistics for the respective column index using specified value.
   *
   * @param type the column type
   * @param value the de-serialized column value
   * @param index the column index
   */
  public void updateColumnStatistics(final BasicTypes type, final Object value, final int index) {
    if (value == null) {
      return;
    }
    switch (type) {
      case BOOLEAN:
        stats[index].updateBoolean((boolean) value, 1);
        stats[index].increment();
        break;
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        stats[index].updateInteger(((Number) value).longValue(), 1);
        stats[index].increment();
        break;
      case FLOAT:
      case DOUBLE:
        stats[index].updateDouble(((Number) value).doubleValue());
        stats[index].increment();
        break;
      case DATE:
        stats[index].updateDate((DateWritable.dateToDays((Date) value)));
        stats[index].increment();
        break;
      case TIMESTAMP:
        stats[index].updateTimestamp((Timestamp) value);
        stats[index].increment();
        break;
      case CHARS:
      case VARCHAR:
      case STRING:
        final byte[] bytes = ((String) value).getBytes();
        stats[index].updateString(bytes, 0, bytes.length, 1);
        stats[index].increment();
        break;
      default:
        break;
    }
  }

  /**
   * Reset all column statistics.
   */
  public void reset() {
    if (this.stats != null) {
      for (final ColumnStatisticsImpl stat : this.stats) {
        if (stat != null) {
          stat.reset();
        }
      }
    }
  }

  /**
   * Get statistics for the column specified by the index.
   *
   * @param index the column index
   * @return the respective column statistics
   */
  public ColumnStatistics getColumnStatistics(final int index) {
    return stats[index];
  }

  @Override
  public String toString() {
    return "AColumnStatistics{" + "stats=" + Arrays.toString(stats) + '}';
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  // @Override
  // public int getDSFID() {
  // return DataSerializableFixedID.AMPL_TABLE_COLUMN_STATS;
  // }

  @Override
  public void toData(DataOutput out) throws IOException {
    if (this.stats == null || this.stats.length == 0) {
      DataSerializer.writePrimitiveInt(0, out);
    } else {
      DataSerializer.writePrimitiveInt(this.stats.length, out);
      for (final ColumnStatisticsImpl stat : this.stats) {
        byte[] bytes = stat == null ? null : stat.serialize().build().toByteArray();
        DataSerializer.writeByteArray(bytes, out);
      }
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    final int size = DataSerializer.readPrimitiveInt(in);
    if (size <= 0) {
      this.stats = null;
    } else {
      this.stats = new ColumnStatisticsImpl[size];
      for (int i = 0; i < size; i++) {
        byte[] bytes = DataSerializer.readByteArray(in);
        this.stats[i] = bytes == null ? null
            : ColumnStatisticsImpl.deserialize(OrcProto.ColumnStatistics.parseFrom(bytes));
      }
    }
  }

  /**
   * Merge the column statistics from other set of column statistics.
   *
   * @param otherStats the other column statistics
   */
  public void merge(final AColumnStatistics otherStats) {
    if (this.stats.length != otherStats.stats.length) {
      return;
    }
    for (int i = 0; i < this.stats.length; i++) {
      if (this.stats[i] != null) {
        this.stats[i].merge(otherStats.stats[i]);
      }
    }
  }

  // @Override
  // public Version[] getSerializationVersions() {
  // return null;
  // }

  /**
   * Call the named method on statistics and get the value as string. Like, call getMinimum or
   * getMaximum on the respective column statistics object. Only used in tests.
   *
   * @param stats the column statistics
   * @param methodName the method-name
   * @return the string representation of the value or null in case of error/null-stats
   */
  public static String get(final ColumnStatistics stats, final String methodName) {
    if (stats != null) {
      try {
        final Method method = stats.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return String.valueOf(method.invoke(stats));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return null;
  }
}
