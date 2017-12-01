package io.ampool.monarch.table.internal;

import java.io.Serializable;
import java.util.Arrays;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.types.TypeHelper;
import io.ampool.monarch.types.interfaces.DataType;

/**
 * Class for reusing the MCell objects..
 *
 */

@InterfaceAudience.Private
@InterfaceStability.Stable
public class CellRef implements Cell, Serializable {

  private static final long serialVersionUID = -2106597746282170701L;
  private byte[] valueBytes = null;
  private int offset = 0;
  private int length = 0;
  private byte[] columnName;
  private byte[] columnValue = null;
  private DataType objectType;

  public CellRef(final byte[] columnName, final DataType objectType) {
    this.columnName = columnName;
    this.objectType = objectType;
  }

  public CellRef(final byte[] columnName, final DataType objectType, byte[] columnValue) {
    this.columnName = columnName;
    this.objectType = objectType;
    this.columnValue = columnValue;
  }

  public CellRef(final byte[] columnName, final DataType objectType, final byte[] valueBytes,
      final int offset, final int length) {
    this.columnName = columnName;
    this.objectType = objectType;
    this.init(valueBytes, offset, length);
  }

  public void init(final byte[] valueBytes, final int offset, final int length) {
    this.valueBytes = valueBytes;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Required in case of Filter where user can set column value directly
   * 
   * @param columnValue
   */
  public void setColumnValue(byte[] columnValue) {
    this.columnValue = columnValue;
    this.valueBytes = null;
    this.offset = -1;
    this.length = -1;
  }

  /**
   * Return the column Name
   *
   * @return column Name
   */
  @Override
  public byte[] getColumnName() {
    return this.columnName;
  }

  /**
   * Return the column value
   *
   * @return column value
   */
  @Override
  public Object getColumnValue() {

    if (columnValue != null) {
      return this.objectType.deserialize(columnValue);
    }
    return this.objectType.deserialize(valueBytes, offset, length);
  }

  @Override
  public DataType getColumnType() {
    return objectType;
  }

  @Override
  public byte[] getValueArray() {
    if (columnValue != null) {
      return this.columnValue;
    }
    return valueBytes;
  }

  public byte[] getValueArrayCopy() {
    if (columnValue != null) {
      return this.columnValue;
    }
    return Arrays.copyOfRange(valueBytes, offset, offset + length);
  }

  @Override
  public int getValueOffset() {
    if (columnValue != null) {
      return 0;
    }
    return this.offset;
  }

  @Override
  public int getValueLength() {
    if (columnValue != null) {
      return this.columnValue.length;
    }
    return this.length;
  }

  /**
   * Readable cell information with column type, name, and value.
   *
   * @return the string representation of the cell
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append('{').append("type=").append(objectType.toString()).append(", name=")
        .append(TypeHelper.BytesToStringFunction.apply(columnName, 0, columnName.length))
        .append(", offset=").append(getValueOffset()).append(", length=").append(getValueLength())
        .append(", value=");
    TypeHelper.deepToString(getColumnValue(), sb);
    sb.append('}');
    return sb.toString();
  }
}
