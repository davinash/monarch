package io.ampool.monarch.table.internal;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.store.StoreRecord;

public class EncodingB implements Encoding {
  public static final int ENC_IDX = 2;
  private int offset = 0;

  @Override
  public int initFullRow(final Object k, final Object v, final ThinRowShared trs, final int offset,
      final int length) {
    List<Cell> cells = trs.getCells();
    final byte[] buf = (byte[]) v;
    int len;
    int vlCount = 0, flOffset = offset, lenOffset = offset + length - Bytes.SIZEOF_INT,
        lastOff = -1;
    CellRef cell, vlLastCell = null;
    for (int i = 0; i < cells.size(); i++) {
      cell = (CellRef) cells.get(i);
      len = cell.getColumnType().lengthOfByteArray();
      if (len <= 0) {
        int off1 = offset + Bytes.toInt(buf, lenOffset);
        lenOffset -= Bytes.SIZEOF_INT;
        if (lastOff != -1) {
          vlLastCell.init(buf, lastOff, (off1 - lastOff));
        }
        ++vlCount;
        lastOff = off1;
        vlLastCell = cell;
      } else {
        cell.init(buf, flOffset, len);
        flOffset += len;
      }
    }
    if (vlLastCell != null) {
      vlLastCell.init(buf, lastOff, offset + (length - (Bytes.SIZEOF_INT * vlCount)) - lastOff);
    }
    return cells.size();
  }

  @Override
  public void writeSelectedColumns(DataOutput out, InternalRow row, List<Integer> columns)
      throws IOException {
    byte[] value = (byte[]) row.getRawValue();
    if (value == null) {
      out.writeShort(-1);
    } else if (row.getRowShared().isFullRow()) {
      out.writeShort(row.getLength());
      out.write(value, row.getOffset(), row.getLength());
    } else {
      int length = offset;
      int vlColumns = 0;
      final List<Cell> cells = row.getCells();
      Cell cell;
      for (int i = 0; i < columns.size(); i++) {
        cell = cells.get(columns.get(i));
        if (cell.getValueLength() >= 0) {
          length += cell.getValueLength();
          vlColumns += cell.getColumnType().isFixedLength() ? 0 : 1;
        }
      }
      length += (vlColumns * Bytes.SIZEOF_INT);
      out.writeShort(length);
      for (int i = 0; i < columns.size(); i++) {
        cell = cells.get(columns.get(i));
        if (!cell.getColumnType().isFixedLength()) {
          out.writeInt(cell.getValueLength());
        }
        out.write(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
      }
    }
  }

  @Override
  public int initPartialRow(final Object k, final Object v, final ThinRowShared trs) {
    List<Cell> cells = trs.getCells();
    final byte[] buf = (byte[]) v;
    int off = offset;
    int len;
    CellRef cell;
    for (int i = 0; i < cells.size(); i++) {
      cell = (CellRef) cells.get(i);
      len = cell.getColumnType().lengthOfByteArray();
      if (len <= 0) {
        len = Bytes.toInt(buf, off);
        off += Bytes.SIZEOF_INT;
      }
      cell.init(buf, off, len);
      off += len;
    }
    return cells.size();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object serializeValue(final TableDescriptor td, final Object inValue) {
    Object getterObj;
    BiFunction<Object, Object, Object> getterFun;
    if (inValue instanceof StoreRecord) {
      getterFun = (a1, a2) -> ((Iterator<Object>) a1).next();
      getterObj = Arrays.asList(((StoreRecord) inValue).getValues()).iterator();
    } else {
      getterFun = (a1, a2) -> ((Map<ByteArrayKey, Object>) a1).get((ByteArrayKey) a2);
      getterObj = ((Record) inValue).getValueMap();
    }

    int flIdx = 0, vlIdx = td.getNumOfColumns() - 1;
    ByteArrayKey bak = new ByteArrayKey();
    byte[] bytes;
    byte[][] data = new byte[td.getNumOfColumns()][];
    int length = 0;
    for (final MColumnDescriptor cd : td.getColumnDescriptors()) {
      bak.setData(cd.getColumnName());
      Object val = getterFun.apply(getterObj, bak);
      if (cd.getColumnType().isFixedLength() && val == null) {
        bytes = cd.getColumnType().serialize(cd.getColumnType().defaultValue());
      } else {
        if (val instanceof byte[]/* && !BasicTypes.BINARY.equals(type) */) {
          bytes = (byte[]) val;
        } else {
          bytes = cd.getColumnType().serialize(val);
        }
      }
      length += bytes.length;
      if (cd.getColumnType().isFixedLength()) {
        data[flIdx++] = bytes;
      } else {
        data[vlIdx--] = bytes;
        length += Bytes.SIZEOF_INT;
      }
    }

    byte[] newValue = new byte[length];
    int offset = 0;
    for (int i = 0; i < flIdx; i++) {
      System.arraycopy(data[i], 0, newValue, offset, data[i].length);
      offset += data[i].length;
    }
    int offset2 = length - Bytes.SIZEOF_INT;
    for (int j = data.length - 1; j > vlIdx; j--) {
      System.arraycopy(data[j], 0, newValue, offset, data[j].length);
      System.arraycopy(Bytes.toBytes(offset), 0, newValue, offset2, Bytes.SIZEOF_INT);
      offset += data[j].length;
      offset2 -= Bytes.SIZEOF_INT;
    }
    return newValue;
  }
}
