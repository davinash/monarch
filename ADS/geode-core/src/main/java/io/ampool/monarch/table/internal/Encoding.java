package io.ampool.monarch.table.internal;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.types.interfaces.DataType;
import io.ampool.monarch.table.DeSerializedRow;

public interface Encoding {
  int ENC_OFFSET = 1;
  Encoding[] ENCODINGS = {new EncodingA(), new EncodingB()};

  static Encoding getEncoding(byte index) {
    return ENCODINGS[index - ENC_OFFSET];
  }

  default int initFullRow(final Object k, final Object v, final ThinRowShared trs) {
    return initFullRow(k, v, trs, 0, ((byte[]) v).length);
  }

  int initFullRow(final Object k, final Object v, final ThinRowShared trs, final int offset,
      final int length);

  void writeSelectedColumns(final DataOutput out, final InternalRow row,
      final List<Integer> columns) throws IOException;

  /**
   * Write the de-serialized row to the provided output stream. If only selected columns were
   * requested, then write only the selected columns. The respective columns are serialized using
   * the specified table descriptor and then written to the output.
   *
   * @param out the output stream
   * @param td the table descriptor
   * @param row the de-serialized row
   * @param columns list of the required column indexes
   * @throws IOException if failed to write to the output stream
   */
  default void writeDesRow(final DataOutput out, final TableDescriptor td,
      final DeSerializedRow row, final List<Integer> columns) throws IOException {
    if (row == null) {
      out.writeShort(-1);
    } else if (columns.isEmpty() || columns.size() == td.getNumOfColumns()) {
      byte[] bytes;
      byte[][] data = new byte[td.getNumOfColumns()][];
      int flIdx = 0, vlIdx = td.getNumOfColumns() - 1;
      int length = 0;
      for (final MColumnDescriptor cd : td.getColumnDescriptors()) {
        Object val = row.getValue(cd.getIndex());
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

      out.writeShort(length);
      int offset = 0;
      for (int i = 0; i < flIdx; i++) {
        out.write(data[i]);
        offset += data[i].length;
      }
      for (int j = data.length - 1; j > vlIdx; j--) {
        out.write(data[j]);
        offset += data[j].length;
      }
      for (int j = flIdx; j < data.length; j++) {
        offset -= data[j].length;
        out.writeInt(offset);
      }
    } else {
      int length = 0;
      int vlColumns = 0;
      final List<MColumnDescriptor> cds = td.getAllColumnDescriptors();
      final byte[][] values = new byte[columns.size()][];
      DataType type;
      for (int i = 0; i < columns.size(); i++) {
        final MColumnDescriptor cd = cds.get(columns.get(i));
        type = cd.getColumnType();
        values[i] = type.serialize(row.getValue(cd.getIndex()));
        length += values[i].length;
        vlColumns += type.isFixedLength() ? 0 : 1;
      }
      length += (vlColumns * Bytes.SIZEOF_INT);
      out.writeShort(length);
      for (int i = 0; i < columns.size(); i++) {
        type = cds.get(columns.get(i)).getColumnType();
        if (!type.isFixedLength()) {
          out.writeInt(values[i].length);
        }
        out.write(values[i]);
      }
    }
  }

  int initPartialRow(final Object k, final Object v, final ThinRowShared trs);

  default int getDataOffset() {
    return 0;
  }

  Object serializeValue(final TableDescriptor td, final Object inValue);
}
