package io.ampool.monarch.table.internal;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import io.ampool.internal.MPartList;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.MTableDescriptor;
import io.ampool.monarch.table.TableDescriptor;

public class EncodingA implements Encoding {
  public static final int ENC_IDX = 1;
  private static final int DATA_OFFSET = Bytes.SIZEOF_INT;
  private static final int BM_OFFSET = DATA_OFFSET + Bytes.SIZEOF_LONG;

  @Override
  public int initFullRow(final Object k, final Object v, final ThinRowShared trs, final int offset,
      final int length) {
    final byte[] key = (byte[]) k;
    final byte[] buf = (byte[]) v;
    final List<Cell> cells = trs.getCells();

    final MTableDescriptor mtd = (MTableDescriptor) trs.getDescriptor();
    final BitMap bmLocal = trs.getColumnBitMap();
    bmLocal.reset(buf, BM_OFFSET, bmLocal.sizeOfByteArray());

    /* update cells of fixed-length columns */
    int off = BM_OFFSET + bmLocal.sizeOfByteArray();
    List<Integer> ids = mtd.getFixedLengthColumns();
    int ci;
    for (int i = 0; i < ids.size(); i++) {
      ci = ids.get(i);
      if (bmLocal.get(ci)) {
        int len = cells.get(ci).getColumnType().lengthOfByteArray();
        ((CellRef) cells.get(ci)).init(buf, off, len);
        off += len;
      } else {
        ((CellRef) cells.get(ci)).init(buf, -1, -1);
      }
    }
    /*
     * if lengths are stored, in byte-array, instead of offset, then this may not be required.
     */
    int lastOff = -1;
    int nonNullCount = 0;
    int lastColumnId = -1;
    int lenOffset = buf.length - Bytes.SIZEOF_INT;
    ids = mtd.getVaribleLengthColumns();
    for (int i = 0; i < ids.size(); i++) {
      ci = ids.get(i);
      if (bmLocal.get(ci)) {
        int off1 = Bytes.toInt(buf, lenOffset, Bytes.SIZEOF_INT);
        lenOffset -= Bytes.SIZEOF_INT;
        if (lastOff != -1) {
          ((CellRef) cells.get(lastColumnId)).init(buf, lastOff, (off1 - lastOff));
        }
        nonNullCount++;
        lastOff = off1;
        lastColumnId = ci;
      } else {
        ((CellRef) cells.get(ci)).init(buf, -1, -1);
      }
    }
    /* offset and length of the last present variable-length column */
    if (lastOff != -1) {
      int lastLen = (buf.length - (Bytes.SIZEOF_INT * nonNullCount)) - lastOff;
      ((CellRef) cells.get(lastColumnId)).init(buf, lastOff, lastLen);
    }
    /* row-key-column */
    if (Bytes.equals(MTableUtils.KEY_COLUMN_NAME_BYTES,
        cells.get(cells.size() - 1).getColumnName())) {
      ((CellRef) cells.get(cells.size() - 1)).init(key, 0, key == null ? -1 : key.length);
    }

    return 0;
  }

  @Override
  public void writeSelectedColumns(final DataOutput out, final InternalRow row,
      final List<Integer> columns) throws IOException {
    byte[] value = (byte[]) row.getRawValue();
    if (value == null) {
      MPartList.writeLength(-1, out);
    } else if (row.getRowShared().isFullRow()) {
      MPartList.writeLength(value.length, out);
      out.write(value);
    } else {
      ThinRowShared rowShared = row.getRowShared();
      BitMap bm1 = new BitMap(rowShared.getColumnBitMap().capacity());
      int length = Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG + bm1.sizeOfByteArray();
      int vlColumns = 0;
      final List<Cell> cells = row.getCells();
      int ci;
      for (int i = 0; i < columns.size(); i++) {
        ci = columns.get(i);
        if (cells.get(ci).getValueLength() > 0) {
          bm1.set(ci);
          length += cells.get(ci).getValueLength();
          vlColumns += cells.get(ci).getColumnType().isFixedLength() ? 0 : 1;
        }
      }
      length += (vlColumns * Bytes.SIZEOF_INT);
      MPartList.writeLength(length, out);
      out.write(value, 0, Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG);
      out.write(bm1.toByteArray());

      Cell cell;
      for (int i = 0; i < columns.size(); i++) {
        cell = cells.get(columns.get(i));
        if (cell.getValueLength() > 0) {
          if (!cell.getColumnType().isFixedLength()) {
            out.writeInt(cell.getValueLength());
          }
          out.write(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        }
      }
    }
  }

  @Override
  public int initPartialRow(final Object k, final Object v, final ThinRowShared trs) {
    byte[] key = (byte[]) k;
    byte[] buf = (byte[]) v;
    List<Cell> cells = trs.getCells();
    BitMap bmLocal = trs.getColumnBitMap();
    bmLocal.reset(buf, BM_OFFSET, bmLocal.sizeOfByteArray());

    int off = BM_OFFSET + bmLocal.sizeOfByteArray();
    final List<Integer> ids = trs.getColumnIds();
    CellRef cell;
    for (int i = 0; i < ids.size(); i++) {
      cell = (CellRef) cells.get(i);
      /* column-value present in data */
      if (bmLocal.get(ids.get(i))) {
        int len = cell.getColumnType().lengthOfByteArray();
        /* FL column */
        if (len <= 0) {
          len = Bytes.toInt(buf, off);
          off += Bytes.SIZEOF_INT;
        }
        cell.init(buf, off, len);
        off += len;
      } else {
        cell.init(buf, -1, -1);
      }
    }

    /* row-key-column */
    if (Bytes.equals(MTableUtils.KEY_COLUMN_NAME_BYTES,
        cells.get(cells.size() - 1).getColumnName())) {
      ((CellRef) cells.get(cells.size() - 1)).init(key, 0, key == null ? -1 : key.length);
    }

    return 0;
  }

  @Override
  public int getDataOffset() {
    return DATA_OFFSET;
  }

  @Override
  public Object serializeValue(TableDescriptor td, Object inValue) {
    // TODO: need to implement..
    return null;
  }
}
