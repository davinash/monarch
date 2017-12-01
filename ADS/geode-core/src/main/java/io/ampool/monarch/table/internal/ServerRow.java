package io.ampool.monarch.table.internal;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.TableDescriptor;


public class ServerRow implements Row, InternalRow {
  private static final long serialVersionUID = -4578403656591436208L;
  private Encoding encoding;
  private Object key;
  private Object value;
  private ThinRowShared rowShared;
  private boolean isInitialized;
  private int offset = 0;
  private int length = 0;

  public ServerRow(final Encoding encoding, final Object key, final Object value,
      final ThinRowShared rowShared) {
    this.encoding = encoding;
    this.key = key;
    this.value = value;
    this.rowShared = rowShared;
    this.length = value instanceof byte[] ? ((byte[]) value).length : 0;
  }

  /**
   * Reset the key and value of the row so that it gets reused.
   *
   * @param key the key
   * @param value the value
   */
  @Override
  public void reset(final Object key, final Object value, final Encoding encoding,
      final List<byte[]> columnNameList) {
    this.key = key;
    this.value = value;
    this.isInitialized = false;
    this.encoding = encoding;
    this.offset = 0;
    this.length = value instanceof byte[] ? ((byte[]) value).length : 0;
  }

  @Override
  public String toString() {
    return "ServerRow{" + "encoding=" + encoding + ", key=" + key + ", value=" + value
        + ", rowShared=" + rowShared + ", isInitialized=" + isInitialized + ", offset=" + offset
        + ", length=" + length + '}';
  }

  @Override
  public void reset(Object key, Object value, Encoding encoding, int offset, int length) {
    this.key = key;
    this.value = value;
    this.isInitialized = false;
    this.encoding = encoding;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Create a list of the Cell's in this result.
   *
   * @return List of Cells; Cell value can be null if its value is deleted.
   */
  @Override
  public List<Cell> getCells() {
    return isInitialized ? this.rowShared.getCells() : initCells();
  }

  public ThinRowShared getRowShared() {
    return this.rowShared;
  }

  private List<Cell> initCells() {
    this.isInitialized = true;
    if (this.value != null) {
      this.encoding.initFullRow(key, value, rowShared, offset, length);
    }
    return this.rowShared.getCells();
  }

  /**
   * Get the size of the underlying MCell []
   *
   * @return size of MCell
   */
  @Override
  public int size() {
    return getCells().size();
  }

  /**
   * Check if the underlying MCell [] is empty or not
   *
   * @return true if empty
   */
  @Override
  public boolean isEmpty() {
    return getCells().isEmpty();
  }

  /**
   * Gets the timestamp associated with the row value.
   *
   * @return timestamp for the row
   */
  @Override
  public Long getRowTimeStamp() {
    return Bytes.toLong((byte[]) this.value, this.encoding.getDataOffset(), Bytes.SIZEOF_LONG);
  }

  /**
   * Gets the row Id for this instance of result
   *
   * @return rowid of this result
   */
  @Override
  public byte[] getRowId() {
    return (byte[]) key;
  }

  @Override
  public int compareTo(Row item) {
    return 0;
  }

  /**
   * Get all versions.
   *
   * @return Map of timestamp (version) to {@link SingleVersionRow}
   */
  @Override
  public Map<Long, SingleVersionRow> getAllVersions() {
    Map<Long, SingleVersionRow> map = new HashMap<>(1);
    map.put(this.getRowTimeStamp(),
        new SingleVersionRowImpl((byte[]) key, this.getRowTimeStamp(), this.getCells()));
    return map;
  }

  /**
   * Get particular version. If not found returns null.
   *
   * @param timestamp - Version identifier
   * @return {@link SingleVersionRow}
   */
  @Override
  public SingleVersionRow getVersion(Long timestamp) {
    return this.getRowTimeStamp().equals(timestamp)
        ? new SingleVersionRowImpl((byte[]) key, this.getRowTimeStamp(), this.getCells()) : null;
  }

  /**
   * Gives latest version.
   *
   * @return Latest version
   */
  @Override
  public SingleVersionRow getLatestRow() {
    return new SingleVersionRowImpl((byte[]) key, this.getRowTimeStamp(), this.getCells());
  }

  /**
   * Get the raw value respective to this row.
   *
   * @return the raw value
   */
  @Override
  public Object getRawValue() {
    return this.value;
  }

  @Override
  public void writeSelectedColumns(final DataOutput out, final List<Integer> columns)
      throws IOException {
    this.encoding.writeSelectedColumns(out, this, columns);
  }

  @Override
  public int getOffset() {
    return this.offset;
  }

  @Override
  public int getLength() {
    return this.length;
  }

  public static ServerRow create(final TableDescriptor td) {
    final List<Cell> cells = td.getColumnDescriptors().stream()
        .map(cd -> new CellRef(cd.getColumnName(), cd.getColumnType()))
        .collect(Collectors.toList());
    return new ServerRow(td.getEncoding(), null, null,
        new ThinRowShared(cells, td, Collections.emptyList()));
  }
}
