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

import java.io.Serializable;
import java.util.*;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.filter.Filter;
import io.ampool.monarch.table.internal.MQuery;
import org.apache.geode.distributed.internal.ServerLocation;

/**
 * Used to perform Scan operations.
 * <p>
 * All operations are identical to {@link Get} with the exception of instantiation. Rather than
 * specifying a single row, an optional startRow and stopRow may be defined. If rows are not
 * specified, the Scanner will iterate over all rows.
 * </p>
 * <p>
 * The scan operation will by default always return keys (rowId's) in the scan, but the caller may
 * change this by calling {@link #setReturnKeysFlag(boolean)} to explicitly indicate the desired
 * behavior.
 * </p>
 * <p>
 * To modify scanner caching for just this scan, use {@link #setClientQueueSize(int)}. Set the
 * number of rows for caching that will be passed to scanners. Higher caching values will enable
 * faster scanners but will use more memory. In addition to row caching, it is possible to specify a
 * maximum result size, using {@link #setMaxResultLimit(int)} (long)}. When both are used, single
 * server requests are limited by either number of rows or maximum result size, whichever limit
 * comes first.
 * </p>
 * <p>
 * To limit the maximum number of values returned for each call to next(), execute
 * {@link #setBatchSize(int) in batching mode}.
 * </p>
 * <p>
 * To add a filter, execute setFilter {@link Scan#setFilter(Filter)}.
 * </p>
 * <p>
 * Scan can be set to retrieve multiple versions if present, use {@link Scan#setMaxVersions()} or
 * other overloaded methods accordingly. By default filters are executed only on latest version. If
 * filter condition is successful then complete row gets qualified. To filter out individual
 * versions set filter execution on each version using
 * {@link Scan#setFilterOnLatestVersionOnly(boolean)} api with value false.
 * </p>
 * <p>
 * Caution: Default scan (Streaming) is CPU intensive. If you are experiencing higher latencies in
 * scan, try running scan in batch mode {@link #enableBatchMode()}.
 * </p>
 * 
 * @since 0.3.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Scan extends MQuery implements Serializable {
  // defaults
  public final static int CLIENT_QUEUE_SIZE_DEFAULT = 5_000;
  public final static boolean RANGE_SCAN_INCLUDE_START_ROW = true;
  private static final long serialVersionUID = 7277157352951747288L;

  private byte[] startRow = null;
  private byte[] stopRow = null;
  private int maxVersions = 1;
  private List<byte[]> columnNameList = new ArrayList<>();
  /**
   * Boolean marker indicating whether or not should we include start row in scan result. Default it
   * is true. Internal marker: Used by client scanner
   */
  private boolean includeStartRow = RANGE_SCAN_INCLUDE_START_ROW;
  private boolean reversed = false;
  private Filter filter;

  // TODO Revert this.
  private boolean filterOnLatestVersionOnly = true;

  /**
   * Use list of columns as it is required for getting the specific columns
   **/
  private final List<Integer> EMPTY_LIST = new ArrayList<>(0);
  private List<Integer> columns = EMPTY_LIST;
  /**
   * use the list of specified buckets for scan; for un-ordered table
   **/
  private Set<Integer> bucketIds = Collections.emptySet();

  private int maxResultLimit = -1;

  /*
   * Server records batching, size of blocking queue for results
   */
  private int clientQueueSize = CLIENT_QUEUE_SIZE_DEFAULT;

  private int chunkSize = 0; // values <= 0 indicate to use server side default.

  private int bucketId = -1;

  private int timeout = 20; // default time to wait for client to consume data if queue full, in
  // seconds

  private boolean batchEnabled = false;

  private int batchSize = clientQueueSize;

  private boolean returnKeys = true; // default is to return keys

  private Map<Integer, Set<ServerLocation>> bucketToServerMap = null;

  // order of multiple versions
  private boolean isOldestFirst = false;

  /* whether or not to interrupt the scan on timeout */
  private boolean isInterruptOnTimeout = false;

  /**
   * Create a MScan operation accessing all rows.
   */
  public Scan() {}

  /**
   * Creates a new instance of this class while copying all values.
   * 
   * @param scan The MScan instance to copy from.
   */
  public Scan(Scan scan) {
    // This constructor MUST be kept up to date
    startRow = scan.getStartRow();
    stopRow = scan.getStopRow();
    maxVersions = scan.getMaxVersions();
    isOldestFirst = scan.isOldestFirst();
    columnNameList = scan.getColumnNameList();
    includeStartRow = scan.getIncludeStartRow();
    reversed = scan.isReversed();
    filter = scan.getFilter();
    columns = scan.getColumns();
    bucketIds = scan.getBucketIds();
    bucketId = scan.getBucketId();
    maxResultLimit = scan.getMaxResultLimit();
    clientQueueSize = scan.getClientQueueSize();
    chunkSize = scan.getMessageChunkSize();
    // bucketId used internally while scan is being processed, do not copy
    timeout = scan.getScanTimeout();
    returnKeys = scan.getReturnKeysFlag();
    batchEnabled = scan.batchModeEnabled();
    batchSize = scan.getBatchSize();
    returnKeys = scan.getReturnKeysFlag();
    bucketToServerMap = scan.getBucketToServerMap();
    filterOnLatestVersionOnly = scan.isFilterOnLatestVersionOnly();
    isInterruptOnTimeout = scan.isInterruptOnTimeout();
  }

  /**
   * Create a MScan operation starting at the specified row and with spcified filter.
   * <p>
   * If the specified row does not exist, the Scanner will start from the next closest row after the
   * specified row.
   *
   * <p>
   * <em>Not applicable for Immutable table</em>
   */
  public Scan(byte[] startRow, Filter filter) {
    this(startRow);
    this.filter = filter;
  }

  /**
   * Create a MScan operation starting at the specified row.
   * <p>
   * If the specified row does not exist, the Scanner will start from the next closest row after the
   * specified row.
   * <p>
   * <em>Not applicable for Immutable table</em>
   * 
   * @param startRow row to start scanner at or after
   */
  public Scan(byte[] startRow) {
    this.startRow = startRow;
  }

  /**
   * Create a MScan operation for the range of rows specified.
   *
   * <p>
   * <em>Not applicable for Immutable table</em>
   * 
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  public Scan(byte[] startRow, byte[] stopRow) {
    this.startRow = startRow;
    this.stopRow = stopRow;
  }

  /**
   * Add the column with the specified columnName to the scan. This will limit the scan to return
   * only the columns specified via {@link Scan#addColumn(byte[])}
   *
   * User should specify selective columns using either this API or {@link #addColumns(List)} or
   * {@link #setColumns(List)} API
   * 
   * @param columnName name of the columns in the table to be returned in scan
   * @return this
   */
  public Scan addColumn(byte[] columnName) {
    if (columnName == null) {
      throw new IllegalArgumentException("Column Name cannot be null");
    }
    this.columnNameList.add(columnName);
    return this;
  }

  /**
   * Add the column with the specified columnName to the scan. This will limit the scan to return
   * only the columns specified via {@link Scan#addColumn(byte[])}
   *
   * User should specify selective columns using either this API or {@link #addColumn(byte[])} or
   * {@link #setColumns(List)} API
   * 
   * @param columnNames list of name of the columns to be returned in scan
   * @return this
   */
  public Scan addColumns(List<byte[]> columnNames) {
    if (columnNames == null) {
      throw new IllegalArgumentException("Column Name cannot be null");
    }
    this.columnNameList.addAll(columnNames);
    return this;
  }

  /**
   * Get the column name list for this scan
   * 
   * @return list of column names
   */
  public List<byte[]> getColumnNameList() {
    return columnNameList;
  }

  /**
   * Get the currently set row filter of this scan. See {@link #setFilter(Filter)}.
   * 
   * @return RowFilter
   */
  public Filter getFilter() {
    return filter;
  }

  /**
   * Apply the specified server-side filter when performing the Query. Only
   * {@link Filter#filterCell(Cell, byte[])} } is called AFTER all tests for ttl, column match,
   * deletes and max versions have been run.
   * 
   * @param filter filter to run on the server
   * @return this for invocation chaining
   */
  public Scan setFilter(Filter filter) {
    this.filter = filter;
    return this;
  }

  /**
   * Returns whether scan has filter associated with it.
   * 
   * @return true is a filter has been specified, false if not
   */
  public boolean hasFilter() {
    return filter != null;
  }

  /**
   * Get whether this scan is a reversed order scan.
   *
   * <p>
   * <em>Not applicable for Immutable table</em>
   * 
   * @return true if backward scan, false if forward(default) scan
   */
  public boolean isReversed() {
    return reversed;
  }

  /**
   * Set whether this scan is a reversed order scan. Only applicable in ORDERED_VERSION table.
   * Throws exception if used for any other table.
   * <p>
   * This is false by default which means forward(normal) scan.
   * 
   * @param reversed if true, scan will be backward order
   */
  public Scan setReversed(boolean reversed) {
    this.reversed = reversed;
    return this;
  }

  /**
   * Returns the start row of the scan. If not set explicitly then {@code null} is returned.
   *
   * Valid only for ORDERED tables.
   * 
   * @return Start row as byte[]. Returns {@code null} if not set.
   */
  public byte[] getStartRow() {
    return this.startRow;
  }

  /**
   * Returns the stop row of the scan. If not set explicitly then {@code null} is returned.
   *
   * Valid only for ORDERED tables.
   * 
   * @return Stop row as byte[]. Returns {@code null} if not set.
   */
  public byte[] getStopRow() {
    return this.stopRow;
  }

  /**
   * Set the start row of the scan. If bucketId's are also set for this scan the intersection of the
   * set of bucket ID's defined by the start and stop row key and the provided set of bucket ID's is
   * used.
   *
   * Valid only for ORDERED tables.
   * <p>
   * <em>Not applicable for Immutable table</em>
   * 
   * @param startRow row to start scan on (inclusive) Note: In order to make startRow exclusive use
   *        {@link #setIncludeStartRow(boolean)}.
   * @return this
   */
  public Scan setStartRow(byte[] startRow) {
    this.startRow = startRow;
    return this;
  }

  /**
   * Set the stop row of the scan. If bucketId's are also set for this scan the intersection of the
   * set of bucket ID's defined by the start and stop key and the provided set of bucket ID's is
   * used.
   *
   * Valid only for ORDERED tables.
   * 
   * @param stopRow row to end at (exclusive)
   *        <p>
   *        <b>Note:</b> In order to make stopRow inclusive add a trailing 0 byte
   *        </p>
   *        <p>
   *        <b>Note:</b> When doing a filter for a rowKey <u>Prefix</u> The 'trailing 0' will not
   *        yield the desired result.
   *        </p>
   *        <p>
   *        <em>Not applicable for Immutable table</em>
   * @return this
   */
  public Scan setStopRow(byte[] stopRow) {
    this.stopRow = stopRow;
    return this;
  }

  /**
   * <P>
   * </P>
   * 
   * @return the maximum number of versions to fetch per row.
   */
  public int getMaxVersions() {
    return maxVersions;
  }

  /**
   * Order of versions in each result.
   * 
   * @return True if order is from older -> newer False if order is from newer -> older
   */
  public boolean isOldestFirst() {
    return isOldestFirst;
  }

  /**
   * @return true If filters are executed on latest version of row only. false If filters are
   *         executed on every version on row.
   */
  public boolean isFilterOnLatestVersionOnly() {
    return filterOnLatestVersionOnly;
  }

  /**
   * Whether to execute filters on every version of row and only to latest version of row. Default
   * is false, filters will be executed on each version of row.
   * 
   * @param filterOnLatestVersionOnly true - Filters will be executed on latest version of row only.
   *        false - Filters will be executed on every version of row.
   */
  public Scan setFilterOnLatestVersionOnly(boolean filterOnLatestVersionOnly) {
    this.filterOnLatestVersionOnly = filterOnLatestVersionOnly;
    return this;
  }

  /**
   * <P>
   * </P>
   * Set the number of versions to fetch per row. Note that if the row does not have this many
   * versions, less than this value will be returned.
   * 
   * @param maxVersions maximum versions to fetch for each row.
   * @param isOldestFirst - true defines order Older version (first) to newer version. false defines
   *        newer version (first) to older version
   */
  public Scan setMaxVersions(int maxVersions, boolean isOldestFirst)
      throws IllegalArgumentException {
    if (maxVersions <= 0) {
      throw new IllegalArgumentException("Zero or negative number is provided as maxVersions");
    }
    this.isOldestFirst = isOldestFirst;
    this.maxVersions = maxVersions;
    return this;
  }

  /**
   * <P>
   * </P>
   * Set the scan to get all available versions for each row.
   * 
   * @param isOldestFirst - true defines order Older version (first) to newer version. false defines
   *        newer version (first) to older version
   */
  public Scan setMaxVersions(boolean isOldestFirst) {
    this.isOldestFirst = isOldestFirst;
    this.maxVersions = Integer.MAX_VALUE;
    return this;
  }

  /**
   * <P>
   * </P>
   * Set the scan to get all available versions for each row. Default order is newer version (first)
   * to older version.
   */
  public Scan setMaxVersions() {
    this.setMaxVersions(false);
    return this;
  }

  /**
   * Get the size of the client size queue used to hold results ready for delivery.
   * 
   * @return The size of the queue used to hold returned results for the client.
   */
  public int getClientQueueSize() {
    return clientQueueSize;
  }

  /**
   * Set the size of the blocking queue used to hold results streaming in for the client. A larger
   * queue decreases the chance the scanner will need to block until space opens up in the queue but
   * takes more client memory to store results. The minimum Client Queue size is two (2). In case of
   * batch mode, by default the clientQueueSize is equal to batchsize. The default value is
   * {@value #CLIENT_QUEUE_SIZE_DEFAULT}.
   * 
   * @param clientQueueSize the size of the blocking queue holding entries.
   */
  public void setClientQueueSize(int clientQueueSize) {
    if (clientQueueSize < 1) {
      throw new IllegalArgumentException("cleint queue size must be > 0");
    }
    if (batchModeEnabled()) {
      this.clientQueueSize = getBatchSize();
    } else {
      this.clientQueueSize = clientQueueSize;
    }
  }

  /**
   * Set operation for batched data. This will cause the scan to send requests of batchSize (see
   * {@link #setBatchSize(int)}) size for each bucket rather than requesting the entire bucket. The
   * next bucket will not be requested until the result queue has been emptied, so by leaving at
   * least one item in the queue an inter-active session causes the scan to wait. This allows one
   * batch at a time to be displayed to users without worrying about network or scan timeouts. The
   * scan timeout will not affect the wait between batches as there are no outstanding server
   * requests. The default batch size is {@value #CLIENT_QUEUE_SIZE_DEFAULT}, use
   * {@link #setBatchSize(int)}) to change this value.
   *
   * <p>
   * <em>Not applicable for Immutable table</em>
   */
  public void enableBatchMode() {
    this.batchEnabled = true;
    setClientQueueSize(getBatchSize());
  }

  /**
   * Determine if batch mode is enabled; see {@link #enableBatchMode()}.
   * <p>
   * <em>Not applicable for Immutable table</em>
   * 
   * @return true is batch mode is enabled, else false
   */
  public boolean batchModeEnabled() {
    return this.batchEnabled;
  }

  /**
   * Determine the batch size set in case of batch mode
   *
   * <p>
   * <em>Not applicable for Immutable table</em>
   * 
   * @return batch size
   */
  public int getBatchSize() {
    return batchSize;
  }

  /**
   * Set the number of records that should be fetched in single batch fetch. This should be set only
   * in batch mode, since it sets client queue size to this value. The default value is
   * {@value #CLIENT_QUEUE_SIZE_DEFAULT}.
   *
   * <p>
   * <em>Not applicable for Immutable table</em>
   * 
   * @param batchSize The number of records to fetch per call to the server.
   */
  public void setBatchSize(int batchSize) {
    if (batchSize < 1) {
      throw new IllegalArgumentException("batch size must be > 0");
    }
    this.batchSize = batchSize;
    setClientQueueSize(batchSize);
  }

  /**
   * Get the timeout for the scan in seconds. This represents the amount of time the scan will wait
   * for the client to consume data when the result queue is full. Note that this timeout does not
   * effect the time a client waits for server data.
   * 
   * @return timeout in seconds.
   */
  public int getScanTimeout() {
    return this.timeout;
  }

  /**
   * Set the timeout for the scan. This controls how long the scan function will wait for the client
   * to make space when the result queue is full. If the client does not make space by removing
   * elements before this time interval the scan will raise an
   * {@link java.util.concurrent.TimeoutException}. Note that if the network timeout is less that
   * this timeout and the client stalls the network timeout will be thrown first.
   *
   * Note: if what you really want is long times between batches of results, see
   * {@link #enableBatchMode()}.
   * 
   * @param timeout in seconds.
   */
  public void setScanTimeout(int timeout) {
    if (timeout > 0) {
      this.timeout = timeout;
    }
  }

  /**
   * Get the message chunk size. When the server has fetched this many items (rows) it will write a
   * message chunk to the client-server connection. Values <= 0 will cause the server to use the
   * server side default value.
   * 
   * @return the size of a message chunk.
   */
  public int getMessageChunkSize() {
    return this.chunkSize;
  }

  /**
   * Set the chunk size for messages returned in a scan. This is an advanced parameter that can be
   * used to over-ride the default chunk size. The chunk size controls the number of items (rows)
   * that are added to a message before a message chunk is written to the network. For example a
   * batch size of 1000 and a chunk size of 100 yields 10 writes per batch. A value <= 0 will cause
   * the server to use the server side default value. If batch mode is being used and this value is
   * greater than batch size then each chunk will be of size batch size.
   *
   * <b>Be careful when setting this parameter!</b>
   * 
   * @param chunkSize the size for a message chunk.
   */
  public void setMessageChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  /**
   * Get the setting for whether or not this scan returns keys along with values in the results. The
   * default behavior for tables is to return keys in a scan.
   * 
   * @return true if keys will be returned, false otherwise.
   */
  public boolean getReturnKeysFlag() {
    return this.returnKeys;
  }

  /**
   * Set to return keys in the scan results or not. The default behavior tables is to return keys in
   * a scan. If the user calls this function with true or false the default or previously set
   * behavior will be over-ridden accordingly.
   * 
   * @param returnKeys if true return keys; if false do not return keys.
   */
  public void setReturnKeysFlag(boolean returnKeys) {
    this.returnKeys = returnKeys;
  }

  /**
   * @return the maximum result count
   */
  public int getMaxResultLimit() {
    return maxResultLimit;
  }

  /**
   * Set the maximum result MResult count. The default is -1; this means that no specific maximum
   * result count be set for this scan, and the global configured value will be used instead.
   * (Defaults to unlimited).
   * 
   * @param maxResultLimit The maximum result counts to fetch.
   */
  public void setMaxResultLimit(int maxResultLimit) {
    this.maxResultLimit = maxResultLimit;
  }

  /**
   * <em>INTERNAL</em>
   * <P>
   * </P>
   * 
   * @return Bucket id set for this scan
   */
  @InterfaceAudience.Private
  public int getBucketId() {
    return this.bucketId;
  }

  /**
   * <em>INTERNAL</em>
   * <P>
   * </P>
   * Restrict scanner to scan to this bucket only.
   */
  @InterfaceAudience.Private
  public void setBucketId(int bucketId) {
    this.bucketId = bucketId;
  }

  /**
   * <em>INTERNAL</em>
   * <P>
   * </P>
   * Return whether or not this scan will include the start row.
   * 
   * @return true if start row is include, else false.
   */
  @InterfaceAudience.Private
  public boolean getIncludeStartRow() {
    return includeStartRow;
  }

  /**
   * <em>INTERNAL</em>
   * <P>
   * </P>
   * Indicate whether or not to include the start row in a range scan (when start/stop) keys are
   * provided. The default is {@value #RANGE_SCAN_INCLUDE_START_ROW}.
   * 
   * @param includeStartRow true to include the start row in results, else false.
   */
  @InterfaceAudience.Private
  public void setIncludeStartRow(boolean includeStartRow) {
    this.includeStartRow = includeStartRow;
  }

  /**
   * Use the provided bucket-ids for scan. This is used for un-ordered table scan where mutually
   * exclusive splits/partitions are created to scan the provided buckets. If set for an ordered
   * table and one or both of start and stop key are provided the intersection of the set of buckets
   * provided and those covered by the start and stop keys are used for the scan.
   * 
   * @param bucketIds the set of bucket ids
   */
  public void setBucketIds(final Set<Integer> bucketIds) {
    this.bucketIds = bucketIds;
  }

  /**
   * Get the bucket ids used for scan.
   * 
   * @return the set of bucket ids
   */
  public Set<Integer> getBucketIds() {
    return this.bucketIds;
  }

  /**
   * Set the columns to be retrieved during the scan. The column-index depends on the order of
   * columns provided during the table creation. Alternatively, the column-index can also be
   * retrieved from the table-descriptor mapping for the respective table.
   *
   * User should specify selective columns using either this API or {@link #addColumn(byte[])} or
   * {@link #addColumns(List)} API.
   * 
   * @param columns the list of columns to be retrieved
   */
  public void setColumns(final List<Integer> columns) {
    /**
     * this list of columns is required to be ArrayList in MTableKey, but user may provide a generic
     * List..
     */
    this.columns = new ArrayList<>(columns);
  }

  /**
   * Get the list of columns that will be retrieved during scan.
   * 
   * @return the list of columns to be retrieved
   */
  public List<Integer> getColumns() {
    return this.columns;
  }

  /**
   * <em>INTERNAL</em>
   * <P>
   * </P>
   * Sets the bucket id to server location information which will be used by scan to get rows.
   * 
   * @param map Map of bucket id to ServerLocation (location of bucket)
   */
  @InterfaceAudience.Private
  public void setBucketToServerMap(final Map<Integer, Set<ServerLocation>> map) {
    this.bucketToServerMap = map;
  }

  /**
   * Set to true if scan is to be interrupted in case of timeout.
   *
   * @param interruptOnTimeout true to interrupt the scan in case of timeout; false otherwise
   */
  @InterfaceAudience.Public
  public Scan setInterruptOnTimeout(final boolean interruptOnTimeout) {
    this.isInterruptOnTimeout = interruptOnTimeout;
    return this;
  }

  /**
   * Whether or not to interrupt the scan in case of timeout.
   *
   * @return true when scan will be interrupted in case of timeout; false otherwise
   */
  public boolean isInterruptOnTimeout() {
    return this.isInterruptOnTimeout;
  }

  /**
   * <em>INTERNAL</em>
   * <P>
   * </P>
   * Returns the bucket id to server location mapping. This map is used to get the rows from server.
   * 
   * @return Map of bucket id to ServerLocation (location of bucket)
   */
  @InterfaceAudience.Private
  public Map<Integer, Set<ServerLocation>> getBucketToServerMap() {
    return this.bucketToServerMap;
  }

  @Override
  public String toString() {
    return "MScan{" + "bucketIds=" + bucketIds + ", startRow=" + Arrays.toString(startRow)
        + ", stopRow=" + Arrays.toString(stopRow) + ", maxVersions=" + maxVersions
        + ", columnNameList=" + columnNameList + ", includeStartRow=" + includeStartRow
        + ", reversed=" + reversed + ", filter=" + filter + ", EMPTY_LIST=" + EMPTY_LIST
        + ", columns=" + columns + ", maxResultLimit=" + maxResultLimit + ", clientQueueSize="
        + clientQueueSize + ", bucketId=" + bucketId + ", batchEnabled=" + batchEnabled
        + ", batchSize = " + batchSize + ", returnKeys=" + returnKeys + ", timeout=" + timeout
        + ", chunkSize" + chunkSize + ", isInterruptOnTimeout= " + isInterruptOnTimeout + '}';
  }
}
