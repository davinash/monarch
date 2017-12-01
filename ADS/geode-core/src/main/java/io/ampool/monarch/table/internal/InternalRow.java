package io.ampool.monarch.table.internal;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import io.ampool.monarch.table.Cell;

public interface InternalRow {
  void reset(final Object key, final Object value, final Encoding encoding,
      final List<byte[]> columnNameList);

  void reset(final Object key, final Object value, final Encoding encoding, final int offset,
      final int length);

  public Object getRawValue();

  ThinRowShared getRowShared();

  List<Cell> getCells();

  void writeSelectedColumns(DataOutput out, List<Integer> columns) throws IOException;

  int getOffset();

  int getLength();
}
