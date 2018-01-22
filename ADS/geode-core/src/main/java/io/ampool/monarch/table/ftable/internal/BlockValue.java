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

package io.ampool.monarch.table.ftable.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.ampool.monarch.table.MColumnDescriptor;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.PersistenceDelta;
import io.ampool.monarch.table.ftable.Record;
import io.ampool.monarch.table.internal.ByteArrayKey;
import io.ampool.monarch.table.internal.Encoding;
import io.ampool.monarch.table.internal.RowHeader;
import io.ampool.monarch.table.internal.ServerRow;
import io.ampool.monarch.table.internal.ThinRow;
import io.ampool.monarch.table.region.ScanUtils;
import io.ampool.monarch.types.BasicTypes;
import io.ampool.orc.AColumnStatistics;
import io.ampool.orc.OrcUtils;
import io.ampool.store.StoreRecord;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.lru.Sizeable;

public class BlockValue implements DataSerializableFixedID, Delta, PersistenceDelta, Sizeable {

  AtomicInteger currentIndex;
  private int valueSize;
  private RowHeader rowHeader;
  private ArrayList<byte[]> records;
  private volatile boolean isEvicted = false;
  /* new block is not considered for delta.. it starts from second record onwards */
  private int lastToDelta = 1;

  // delta counter used for persisting only delta information
  private int lastPersistenceDelta = 0;


  /* Indicates type of the stored data: Ampool bytes or ORC bytes */
  private Object value = null;
  private FTableDescriptor.BlockFormat fmt = FTableDescriptor.DEFAULT_BLOCK_FORMAT;

  /* Row is only used to update column statistics during ingestion */
  private ServerRow row = null;

  /* the column statistics */
  private AColumnStatistics stats = null;

  public BlockValue() {
    currentIndex = new AtomicInteger();
    rowHeader = new RowHeader();
    records = new ArrayList<>();
  }

  public BlockValue(final int blockSize) {
    currentIndex = new AtomicInteger();
    rowHeader = new RowHeader();
    value = records = new ArrayList<>(blockSize);
  }

  public synchronized boolean checkAndAddRecord(final Object record) {
    if (currentIndex.get() > records.size()) {
      return false;
    }
    if (record instanceof BlockValue) {
      BlockValue bv = (BlockValue) record;
      this.records.addAll(bv.getRecords());
      this.valueSize += bv.getValueSize();
      this.currentIndex.addAndGet(bv.size());
    } else {
      records.add((byte[]) record);
      valueSize += ((byte[]) record).length;
      this.currentIndex.getAndAdd(1);
    }
    return true;
  }

  /**
   * Update the column statistics from the de-serialized record/row and then serialize and add the
   * record to the block.
   *
   * @param record the record
   * @param td the table descriptor
   * @return true if the row was added to the block; false otherwise
   */
  public boolean addAndUpdateStats(final Record record, final FTableDescriptor td) {
    final Map<ByteArrayKey, MColumnDescriptor> columnsByName = td.getColumnsByName();
    if (this.stats == null) {
      this.stats = new AColumnStatistics(td);
    }
    for (Map.Entry<ByteArrayKey, Object> entry : record.getValueMap().entrySet()) {
      final MColumnDescriptor cd = columnsByName.get(entry.getKey());
      if (cd.getColumnType() instanceof BasicTypes) {
        this.stats.updateColumnStatistics((BasicTypes) cd.getColumnType(),
                record.getValueMap().get(entry.getKey()), cd.getIndex());
      }
    }
    return checkAndAddRecord(td.getEncoding().serializeValue(td, record));
  }

  /**
   * Add more row to the block, if not full, and also update the column statistics. Each row is
   * de-serialized (the respective column values) during the process.
   *
   * @param row the row or block value to be added/merged
   * @param td the table descriptor
   * @return true if the row was added to the block; false otherwise
   */
  public synchronized boolean checkAndAddRecord(Object row, FTableDescriptor td) {
    if (currentIndex.get() > records.size()) {
      return false;
    }
    if (td.isColumnStatisticsEnabled()) {
      if (row instanceof BlockValue) {
        BlockValue bv = (BlockValue) row;
        if (bv.stats == null) {
          for (byte[] bytes : bv.records) {
            this.records.add(updateStatistics(bytes, td));
          }
        } else {
          this.records.addAll(bv.records);
          if (this.stats == null) {
            this.stats = bv.stats;
          } else {
            this.stats.merge(bv.stats);
          }
        }
        this.valueSize += bv.getValueSize();
        this.currentIndex.addAndGet(bv.size());
      } else {
        records.add(updateStatistics((byte[]) row, td));
        valueSize += ((byte[]) row).length;
        this.currentIndex.getAndAdd(1);
      }
    } else {
      if (row instanceof BlockValue) {
        BlockValue bv = (BlockValue) row;
        this.records.addAll(bv.getRecords());
        this.valueSize += bv.getValueSize();
        this.currentIndex.addAndGet(bv.size());
      } else {
        records.add((byte[]) row);
        valueSize += ((byte[]) row).length;
        this.currentIndex.getAndAdd(1);
      }
    }
    return true;
  }

  /**
   * Update the column statistics (min/max) for each block.
   *
   * @param rowBytes the serialized row
   * @param td the table descriptor
   * @return the serialized row
   */
  public byte[] updateStatistics(final byte[] rowBytes, final FTableDescriptor td) {
    if (!td.isColumnStatisticsEnabled() || this.fmt == FTableDescriptor.BlockFormat.ORC_BYTES) {
      return rowBytes;
    }
    if (row == null) {
      row = ServerRow.create(td);
    }
    row.reset(null, rowBytes, td.getEncoding(), null);
    if (stats == null) {
      stats = new AColumnStatistics(td);
    }
    stats.updateRowStatistics(td.getAllColumnDescriptors(), row);
    return rowBytes;
  }

  /**
   * Get the column statistics for this block.
   *
   * @return the column statistics
   */
  public AColumnStatistics getColumnStatistics() {
    return this.stats;
  }

  public int getCurrentIndex() {
    return currentIndex.get();
  }

  public int getValueSize() {
    return valueSize;
  }

  public RowHeader getRowHeader() {
    return rowHeader;
  }

  public List<byte[]> getRecords() {
    return records;
  }

  /**
   * Get the record/row at the specified index.
   *
   * @param index the index of record/row within block
   * @return the respective record/row
   */
  public byte[] getRecord(final int index) {
    return this.records.get(index);
  }

  /**
   * Return the data format of this block.
   *
   * @return the block format
   */
  public FTableDescriptor.BlockFormat getFormat() {
    return this.fmt;
  }

  /**
   * Return the total number of records/rows in the block.
   *
   * @return the number of records
   */
  public int size() {
    return this.currentIndex.get();
  }

  public Object getValue() {
    return this.fmt == FTableDescriptor.BlockFormat.AMP_BYTES ? this.records : this.value;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return AMPL_FTABLE_BLOCKVALUE;
  }

  @Override
  public synchronized void toData(final DataOutput out) throws IOException {
    DataSerializer.writeInteger(currentIndex.get(), out);
    DataSerializer.writeInteger(valueSize, out);
    DataSerializer.writeByteArray(rowHeader.getHeaderBytes(), out);
    DataSerializer.writeEnum(this.fmt, out);
    if (this.fmt == FTableDescriptor.BlockFormat.AMP_BYTES) {
      DataSerializer.writeArrayList(records, out);
    } else {
      DataSerializer.writeByteArray((byte[]) this.value, out);
    }
    DataSerializer.writeObject(this.stats, out);
  }

  @Override
  public synchronized void fromData(final DataInput in) throws IOException, ClassNotFoundException {
    currentIndex.set(DataSerializer.readInteger(in));
    valueSize = DataSerializer.readInteger(in);
    rowHeader = new RowHeader(DataSerializer.readByteArray(in));
    fmt = DataSerializer.readEnum(FTableDescriptor.BlockFormat.class, in);
    if (this.fmt == FTableDescriptor.BlockFormat.AMP_BYTES) {
      records = DataSerializer.readArrayList(in);
    } else {
      this.value = DataSerializer.readByteArray(in);
    }
    this.stats = DataSerializer.readObject(in);
  }

  public byte[] getBytes() {
    return EntryEventImpl.serialize(this);
  }

  public static BlockValue fromBytes(final byte[] bytes) {
    return (BlockValue) EntryEventImpl.deserialize(bytes);
  }


  public Iterator<Object> iterator() {
    return iterator(null);
  }

  public Iterator<Object> iterator(final ReaderOptions orcOptions) {
    Iterator<Object> itr;
    switch (this.fmt) {
      case AMP_SNAPPY:
        itr = new OrcUtils.RowIterator(OrcUtils.decompressSnappy((byte[]) this.value));
        break;
      case ORC_BYTES:
        itr = new OrcUtils.OrcIterator((byte[]) this.value, (OrcUtils.OrcOptions) orcOptions);
        break;
      default:
        itr = new BlockValueIterator();
    }
    return itr;
  }


  public boolean isEvicted() {
    return isEvicted;
  }

  public void setEvicted(boolean evicted) {
    isEvicted = evicted;
  }

  /**
   * Truncate the block by removing the records that match the provided filters. In case the block
   * is closed, after the removal of records the block is closed again.
   *
   * @param lastKey the key respective to this block-value
   * @param filter the filters to be evaluated
   * @param td the table descriptor
   */
  public void truncateBlock(BlockKey lastKey, Filter filter, TableDescriptor td) {
    final ThinRow row = ThinRow.create(td, ThinRow.RowFormat.F_FULL_ROW);
    ArrayList<byte[]> newList = new ArrayList<>(size());
    Iterator<Object> iterator = iterator();
    Encoding enc = Encoding.getEncoding(this.rowHeader.getEncoding());
    Row r;
    if (this.stats != null) {
      this.stats.reset();
    }
    while (iterator.hasNext()) {
      final Object record = iterator.next();
      /* eventually StoreRecord should be used for filtering as well */
      if (record instanceof StoreRecord) {
        r = (StoreRecord) record;
      } else {
        row.reset(lastKey.getBytes(), (byte[]) record);
        r = row;
      }
      /* skip/remove the matching records */
      if (!ScanUtils.executeSimpleFilter(r, filter, td)) {
        final byte[] bytes = record instanceof StoreRecord ? (byte[]) enc.serializeValue(td, record)
                : (byte[]) record;
        newList.add(updateStatistics(bytes, (FTableDescriptor) td));
      }
    }
    reset(lastKey, (FTableDescriptor) td, newList);
  }

  /**
   * Reset the status of block-value after truncating the block. It resets the number of records in
   * block and also closes the block if required. It also updates the end-time of the block from
   * respective key.
   *
   * @param lastKey the key respective to this block-value
   * @param td the table descriptor
   * @param newList the new list of serialized records
   */
  private void reset(final BlockKey lastKey, final FTableDescriptor td,
                     final ArrayList<byte[]> newList) {
    this.currentIndex.set(newList.size());
    if (newList.size() == 0) {
      this.value = this.records = null;
      this.fmt = FTableDescriptor.BlockFormat.AMP_BYTES;
    } else {
      this.records = newList;
      if (this.fmt != FTableDescriptor.BlockFormat.AMP_BYTES) {
        close(td);
      }
    }
  }

  /**
   * Update the column values of records from this block matching the provided filters, if any. In
   * case no filters are provided all records will updated.
   *
   * @param lastKey the key respective to this block-value
   * @param filter the filters to be evaluated
   * @param colValues map of column-index to the value to be updated
   * @param td the table descriptor
   * @return true if the block has been updated; false otherwise
   */
  public boolean updateBlock(BlockKey lastKey, Filter filter, Map<Integer, Object> colValues,
                             TableDescriptor td) {
    final ThinRow row = ThinRow.create(td, ThinRow.RowFormat.F_FULL_ROW);
    ArrayList<byte[]> newList = new ArrayList<>(size());
    final Encoding enc = Encoding.getEncoding(this.rowHeader.getEncoding());
    final StoreRecord newRec = new StoreRecord(td.getNumOfColumns());
    Iterator<Object> iterator = iterator();
    if (this.stats != null) {
      this.stats.reset();
    }
    boolean isUpdated = false;
    while (iterator.hasNext()) {
      final Object record = iterator.next();
      byte[] bytes;
      /* eventually StoreRecord should be used for filtering as well */
      if (record instanceof StoreRecord) {
        bytes = (byte[]) enc.serializeValue(td, record);
      } else {
        bytes = (byte[]) record;
      }
      row.reset(lastKey.getBytes(), bytes);
      /* update the matching records */
      if (ScanUtils.executeSimpleFilter(row, filter, td)) {
        if (record instanceof StoreRecord) {
          System.arraycopy(((StoreRecord) record).getValues(), 0, newRec.getValues(), 0,
                  td.getNumOfColumns());
        } else {
          for (int i = 0; i < td.getNumOfColumns(); i++) {
            newRec.updateValue(i, row.getCells().get(i).getColumnValue());
          }
        }
        for (Map.Entry<Integer, Object> entry : colValues.entrySet()) {
          newRec.updateValue(entry.getKey(), entry.getValue());
        }
        bytes = (byte[]) enc.serializeValue(td, newRec);
        isUpdated = true;
      }
      newList.add(updateStatistics(bytes, (FTableDescriptor) td));
    }
    this.records = newList;
    if (this.fmt != FTableDescriptor.BlockFormat.AMP_BYTES) {
      close((FTableDescriptor) td);
    }
    this.lastPersistenceDelta = 0;
    return isUpdated;
  }

  public void resetDelta() {
    this.lastToDelta = size();
  }

  private boolean hasDeltaInternal(final int deltaIndex) {
    if (this.records == null || records.size() <= deltaIndex) {
      return false;
    }
    return true;
  }

  private synchronized void toDeltaInternal(DataOutput out, final int deltaIndex)
          throws IOException {
    int deltaCount = records.size() - deltaIndex;
    DataSerializer.writeInteger(deltaCount, out);
    for (int i = 0; i < deltaCount; i++) {
      DataSerializer.writeByteArray(records.get(deltaIndex + i), out);
    }
    DataSerializer.writeObject(this.stats, out);
  }

  private synchronized void fromDeltaInternal(DataInput in)
          throws IOException, InvalidDeltaException {
    int deltaCount = DataSerializer.readInteger(in);
    for (int i = 0; i < deltaCount; i++) {
      final byte[] bytes = DataSerializer.readByteArray(in);
      this.records.add(bytes);
      valueSize += bytes == null ? 0 : bytes.length;
    }
    try {
      this.stats = DataSerializer.readObject(in);
    } catch (ClassNotFoundException e) {
      ////
    }
    this.currentIndex.addAndGet(deltaCount);
  }

  @Override
  public boolean hasDelta() {
    return hasDeltaInternal(this.lastToDelta);
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    toDeltaInternal(out, this.lastToDelta);
    this.lastToDelta = size();
  }

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    fromDeltaInternal(in);
  }

  @Override
  public synchronized boolean hasPersistenceDelta() {
    if (this.lastPersistenceDelta == 0) {
      return false;
    }
    if (this.records == null || records.size() < lastPersistenceDelta) {
      return false;
    }
    return true;
  }

  public int getLastPersistenceDelta() {
    return lastPersistenceDelta;
  }

  @Override
  public synchronized void toPersistenceDelta(DataOutput out) {
    try {
      toDeltaInternal(out, this.lastPersistenceDelta);
      this.lastPersistenceDelta = size();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public synchronized void fromPersistenceDelta(DataInput in) {
    try {
      fromDeltaInternal(in);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public synchronized void resetPersistenceDelta() {
    this.lastPersistenceDelta = size();
  }

  @Override
  public String toString() {
    return "BlockValue{" + "currentIndex=" + currentIndex + ", valueSize=" + valueSize
            + ", lastPersistenceDelta=" + lastPersistenceDelta + '}';
  }

  /**
   * Returns the size (in bytes) of this object including the {@link #PER_OBJECT_OVERHEAD}.
   */
  @Override
  public int getSizeInBytes() {
    return this.valueSize;
  }



  private class BlockValueIterator implements Iterator<Object> {
    int itrIndex;

    public BlockValueIterator() {
      itrIndex = 0;
    }

    @Override
    public boolean hasNext() {
      if (currentIndex.get() > itrIndex) {
        return true;
      }
      return false;
    }

    @Override
    public byte[] next() {
      if (hasNext()) {
        return records.get(itrIndex++);
      } else {
        return null;
      }
    }
  }

  /**
   * Close the active block by converting the available rows to a binary blob (like ORC format).
   *
   * @param td the table descriptor
   */
  public void close(final FTableDescriptor td) {
    byte[] bytes;
    switch (td.getBlockFormat()) {
      case ORC_BYTES:
        bytes = OrcUtils.convertToOrcBytes(this, td);
        fmt = FTableDescriptor.BlockFormat.ORC_BYTES;
        this.stats = null;
        break;
      case AMP_SNAPPY:
        bytes = OrcUtils.compressSnappy(this, td);
        fmt = FTableDescriptor.BlockFormat.AMP_SNAPPY;
        break;
      default:
        bytes = null;
    }
    this.row = null;
    if (bytes != null) {
      /* TODO: need to synchronize the access to value or records */
      this.value = bytes;
      this.records = null;
      this.valueSize = bytes.length;
    }
  }
}
