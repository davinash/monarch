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

package io.ampool.tierstore.wal;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.StoreInternalException;
import io.ampool.monarch.table.ftable.internal.BlockValue;
import io.ampool.monarch.table.internal.IMKey;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import org.apache.commons.io.filefilter.*;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class WriteAheadLog {
  private static WriteAheadLog instance = null;
  public static final String WAL_DONE_SUFFIX = ".done";
  public static final String WAL_INPROGRESS_SUFFIX = ".inprogress";
  public static final int WAL_FILE_RECORD_LIMIT = 100;

  private static byte[] nodeId = null;
  private static final Logger logger = LogService.getLogger();

  private Path walDirectory = null;
  private int numFiles = 0;
  private Map<String, WALWriter> writersMap = new ConcurrentHashMap<>();
  private int recordLimit = WAL_FILE_RECORD_LIMIT;
  private static boolean initialized = false;
  private long fileExpirationTimeMins = Long.getLong("ampool.wal.expiration.time", 10L);

  private Map<String, AtomicLong> bucketSeqNoMap = null;

  /**
   * Get instance of WriteAheadLog
   *
   * @return Singleton instance of WriteAheadLog
   */
  public static WriteAheadLog getInstance() {
    if (instance == null) {
      synchronized (WriteAheadLog.class) {
        if (instance == null) {
          instance = new WriteAheadLog();
          initialized = false;
        }
      }
    }
    return instance;
  }


  /**
   * Initialize the WAL instance with default file size(number of records)
   *
   * @param directory
   * @throws IOException
   */
  public void init(Path directory) throws IOException {
    this.init(directory, WAL_FILE_RECORD_LIMIT);
  }

  /**
   * Initialize the WriteAheadLog instance
   *
   * @param directory WAL directory
   * @param recordLimit Max number of records a WAL file can contain.
   * @throws IOException
   */
  synchronized public void init(Path directory, int recordLimit) throws IOException {
    init(directory, recordLimit, fileExpirationTimeMins);
  }

  synchronized public void init(Path directory, int recordLimit, final long fileExpirationTimeMins)
      throws IOException {
    if (initialized) {
      throw new RuntimeException("WriteAheadLog can not be initialized multiple times");
    }
    walDirectory = directory;
    this.recordLimit = recordLimit;
    numFiles = 0;
    /* Create the directory */
    if (!directory.toFile().exists()) {
      if (!directory.toFile().mkdir()) { /* No other thread should create this file */
        throw new RuntimeException("Failed to create directory");
      }
    }
    /* populate the bucketId -> file seq Id map */
    bucketSeqNoMap = new HashMap<>();
    this.fileExpirationTimeMins = fileExpirationTimeMins;
    String[] walFiles = directory.toFile().list();
    if (walFiles != null && walFiles.length != 0) {
      for (String walFile : walFiles) {
        String tableName = getTableName(walFile);
        int partitionId = getPartitionId(walFile);
        long seqNumber = getSeqNumber(walFile);
        if (tableName != null && partitionId != -1 && seqNumber != -1) {
          AtomicLong mapValue = bucketSeqNoMap.get(tableName + "_" + partitionId);
          if (mapValue != null) {
            long savedSeqNumber = mapValue.get();
            if (savedSeqNumber < seqNumber + 1) {
              mapValue.set(seqNumber + 1);
            }
          } else {
            bucketSeqNoMap.put(tableName + "_" + partitionId, new AtomicLong(seqNumber + 1));
          }
        }
        if (walFile.endsWith(WAL_INPROGRESS_SUFFIX)) {
          markFileDone(walDirectory + "/" + walFile);
        }
      }
    }

    byte[] dsName = null;
    try {
      dsName = Bytes.toBytes(MCacheFactory.getAnyInstance().getDistributedSystem().getName());
    } catch (CacheClosedException cce) {
      dsName = Bytes.toBytes("==DsNameNotSet==");
      // logger.error("Distributed system name not set", cce);
    }

    nodeId = new byte[dsName.length + Integer.SIZE / 8];
    System.arraycopy(Bytes.toBytes(dsName.length), 0, nodeId, 0, Integer.SIZE / 8);
    System.arraycopy(dsName, 0, nodeId, Integer.SIZE / 8, dsName.length);
    initialized = true;
  }

  /* For tests */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  synchronized public void deinit() throws IOException {
    if (!initialized) {
      return;
    }

    for (Map.Entry<String, WALWriter> entry : writersMap.entrySet()) {
      entry.getValue().close();
    }
    writersMap.clear();
    initialized = false;
  }

  private WriteAheadLog() {

  }

  public static byte[] getNodeId() {
    return nodeId;
  }

  public int getWalFileRecordLimit() {
    return recordLimit;
  }

  /**
   * Move a file from processing state(being written by writer) to done state. It will also close
   * the writer associated with the WAL file.
   *
   * @param fileName Name of the WAL file.
   * @return New file name
   * @throws IOException
   */
  public String markFileDone(String fileName) throws IOException {
    return markFileDone(fileName, true);
  }

  /**
   * Move a file from processing state(being written by writer) to done state. It will also
   * optionally close the writer associated with the WAL file.
   *
   * @param fileName Name of the WAL file.
   * @param closeWriter Close the associated WAL writer.
   * @return New file name
   * @throws IOException
   */
  public String markFileDone(String fileName, boolean closeWriter) throws IOException {
    if (fileName.endsWith(WAL_DONE_SUFFIX)) {
      return fileName;
    }
    synchronized (this) {
      File oldFile = new File(fileName);
      if (oldFile.exists()) {
        /* TODO: check locking with WAL monitor thread */
        if (closeWriter) {
          final String name = oldFile.getName();
          final String key = getTableName(name) + "_" + getPartitionId(name);
          WALWriter writer = writersMap.get(key);
          if (writer != null) {
            writersMap.remove(key);
            writer.close();
          }
        }

        String baseName = oldFile.getName();
        String dirName = oldFile.getParent();
        String[] tmp = baseName.split("\\.");
        String newBaseName = tmp[0] + WAL_DONE_SUFFIX;
        File newFile = new File(dirName + "/" + newBaseName);
        if (!oldFile.renameTo(newFile)) {
          throw new IOException("Error while renaming file " + oldFile.getAbsolutePath() + " to "
              + newFile.getAbsolutePath());
        }
        return newFile.getAbsolutePath();
      } else {
        String baseName = oldFile.getName();
        String dirName = oldFile.getParent();
        String[] tmp = baseName.split("\\.");
        String newBaseName = tmp[0] + WAL_DONE_SUFFIX;
        return (dirName + "/" + newBaseName);
      }
    }
  }

  /**
   * Return a WAL writer for table name and partition id pair. If a WAL file is already open then a
   * reference to the existing writer will be returned else, a new writer will be created.
   *
   * @param tableName Name of the FTable.
   * @param partitionId Partition Id.
   * @return WALWriter for the table and partition pair.
   * @throws IOException
   */
  /* TODO: handle race */
  private WALWriter getWalWriter(String tableName, int partitionId) throws IOException {
    final String key = tableName + "_" + partitionId;
    WALWriter writer = writersMap.get(key);
    String fileName = null;
    if (writer == null) {
      synchronized (this) {
        writer = writersMap.get(key);
        if (writer == null) {
          fileName = key + "_" + getNextSeqNo(tableName, partitionId) + WAL_INPROGRESS_SUFFIX;
          writer = getWriter(tableName, partitionId, fileName, recordLimit);
          writersMap.put(key, writer);
        }
      }
    }
    return writer;
  }

  public int append(final String tableName, final int partitionId, final IMKey blockKey,
      final BlockValue blockValue) throws IOException {
    if (!initialized) {
      return 0;
    }
    WALWriter writer = getWalWriter(tableName, partitionId);
    /* TODO: prevent other threads to use the same writer */
    writer.write(blockKey, blockValue);
    return 1;
  }

  /**
   * Return the path of WAL directory
   *
   * @return path of the WAL directory
   */
  public Path getWalDirectory() {
    return walDirectory;
  }

  /**
   * Get absolute path of the specified file-name. If the specified file-name is not absolute then
   * prefix it with wal-directory path.
   *
   * @param fileName the file-name
   * @return the absolute path for the file
   */
  public Path getAbsolutePath(final String fileName) {
    return fileName.charAt(0) == '/' ? Paths.get(fileName)
        : Paths.get(walDirectory + "/" + fileName);
  }

  /**
   * Get the reader for a WAL file
   *
   * @param fileName
   * @return WALReader
   */
  public WALReader getReader(String fileName) throws IOException {
    return new WALReader(getAbsolutePath(fileName));
  }

  /**
   * Get the writer for a WAL file
   *
   * @param fileName
   * @return WALWriter
   */
  public WALWriter getWriter(String tableName, int partitionId, String fileName, int recordLimit)
      throws IOException {
    return new WALWriter(tableName, partitionId, getAbsolutePath(fileName), recordLimit);
  }

  /**
   * Delete a WAL file
   *
   * @param fileName
   */
  /* TODO: delete only if the file is not being currently written */
  public void deleteWALFile(String fileName) {
    File file;
    if (fileName.charAt(0) == '/') {
      file = new File(fileName);
    } else {
      file = new File(walDirectory + "/" + Paths.get(fileName).getFileName().toString());
    }
    file.delete();
  }

  /**
   * Get a list of completed files which can be pickedup for next stage
   *
   * @return list of files which are full and can be moved to next stage
   */
  public String[] getCompletedFiles() {
    if (!initialized) {
      return null;
    }
    String[] files = walDirectory.toFile().list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(WAL_DONE_SUFFIX);
      }
    });
    if (null != files && files.length > 0) {
      Arrays.sort(files, new WALFileComparator());
    }
    return files;
  }

  /**
   * Get a list for all WAL files for given tablename and partition Id
   */
  public String[] getAllfilesForBucket(String tableName, int partitionId) {
    if (!initialized) {
      return null;
    }
    String[] files = walDirectory.toFile().list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.startsWith(tableName + "_" + partitionId + "_"))
          return true;
        return false;
      }
    });
    if (null != files && files.length > 0) {
      Arrays.sort(files, new WALFileComparator());
    }
    return files;
  }

  /**
   * Delete all WAL files belonging to a bucket.
   *
   * @param tableName
   * @param partitionId
   */
  public void deleteBucket(String tableName, int partitionId) {
    String[] bucketFiles = getAllfilesForBucket(tableName, partitionId);
    if (bucketFiles != null && bucketFiles.length > 0) {
      for (String bucketFile : bucketFiles) {
        deleteWALFile(bucketFile);
      }
    }

    WALWriter walWriter = writersMap.remove(tableName + "_" + partitionId);
    if (walWriter != null) {
      try {
        walWriter.close();
      } catch (IOException e) {
        logger.warn("Error while closing wal file for: " + tableName + "_" + partitionId);
      }
    }

    /** remove the sequence-no for the bucket that is getting deleted **/
    bucketSeqNoMap.remove(tableName + "_" + partitionId);
  }

  /**
   * Get a list of files which are under process
   *
   * @return list of file which are currently being used by WAL writers
   */

  public String[] getInprogressFiles() {
    if (!initialized) {
      return null;
    }
    String[] files = walDirectory.toFile().list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.endsWith(WAL_INPROGRESS_SUFFIX))
          return true;
        return false;
      }
    });
    if (null != files && files.length > 0) {
      Arrays.sort(files, new WALFileComparator());
    }
    return files;
  }

  private class WALFileComparator implements Comparator<String> {
    public int compare(String f1, String f2) {
      Long seqId1 = WriteAheadLog.getInstance().getSeqNumber(f1);
      Long seqId2 = WriteAheadLog.getInstance().getSeqNumber(f2);
      return seqId1.compareTo(seqId2);
    }
  }

  private static final String[] EMPTY_ARRAY = new String[0];

  /**
   * Get files which are still in progress but are too old and need to be closed and moved to next
   * stage.
   *
   * @return list of expired files.
   */
  public String[] getExpiredFiles() {
    long epochDifference = System.currentTimeMillis() - (fileExpirationTimeMins * 60 * 1000);
    List<IOFileFilter> fileFilterList = new ArrayList<>();
    fileFilterList.add(new AgeFileFilter(new Date(epochDifference)));
    fileFilterList.add(new RegexFileFilter(".*" + WAL_INPROGRESS_SUFFIX));
    final String[] fileArr =
        walDirectory.toFile().getAbsoluteFile().list(new AndFileFilter(fileFilterList));
    if (fileArr == null) {
      return EMPTY_ARRAY;
    }
    Stream<String> files = Stream.of(fileArr);
    return files.map(this::getAbsolutePath).map(Path::toString).toArray(String[]::new);
    // return Stream.of(walDirectory.toFile().list(new AndFileFilter(fileFilterList)))
    // .map(this::getAbsolutePath).map(Path::toString).toArray(String[]::new);
  }

  /**
   * Get next sequence number
   *
   * @return next sequence number.
   */
  public long getNextSeqNo(String tableName, int partitionId) {
    synchronized (bucketSeqNoMap) {
      AtomicLong mapValue = bucketSeqNoMap.get(tableName + "_" + partitionId);
      if (mapValue != null) {
        return mapValue.getAndIncrement();
      }
      bucketSeqNoMap.put(tableName + "_" + partitionId, new AtomicLong(1));
      return 0;
    }
  }

  /**
   * Given a WAL file name, return the associated tablename
   *
   * @param fileName
   * @return Associated table name
   */
  public String getTableName(String fileName) {
    /* ["tablename_partitionId_seqNo"] */

    String[] nameComponents = fileName.split("_");
    if (nameComponents.length < 3) {
      return null;
    }
    if (nameComponents.length >= 3) {
      String tableName =
          String.join("_", Arrays.copyOfRange(nameComponents, 0, nameComponents.length - 2));
      return tableName;
    }
    return null;
  }

  /**
   * Given a WAL file name, return the associated partition Id.
   *
   * @param fileName
   * @return Associated partition Id
   */
  public Integer getPartitionId(String fileName) {
    String[] nameComponents = fileName.split("_");
    if (nameComponents.length < 3) {
      return -1;
    }
    try {
      return Integer.parseInt(nameComponents[nameComponents.length - 2]);
    } catch (NumberFormatException nfe) {
      return -1;
    }
  }

  /**
   * Given a WAL file name, return the associated sequence Number
   *
   * @param fileName
   * @return Associated sequence number.
   */
  public Long getSeqNumber(String fileName) {
    String[] nameComponents = fileName.split("_");

    if (nameComponents.length < 3) {
      return -1l;
    }

    try {
      return Long.parseLong(nameComponents[nameComponents.length - 1].split("\\.")[0]);
    } catch (NumberFormatException nfe) {
      return -1l;
    }
  }

  public WALResultScanner getScanner(String tableName, int partitionId) {
    return new WALResultScanner(tableName, partitionId);
  }

  public String[] getAllFilesForTable(String tableName) {
    if (!initialized) {
      return null;
    }
    final Pattern doneFileRegEx =
        Pattern.compile("^" + tableName + "_" + "\\d+" + "_" + "\\d+" + WAL_DONE_SUFFIX + "\\z");
    final Pattern inProgressFileRegex = Pattern
        .compile("^" + tableName + "_" + "\\d+" + "_" + "\\d+" + WAL_INPROGRESS_SUFFIX + "\\z$");
    return walDirectory.toFile().list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        Matcher doneMatcher = doneFileRegEx.matcher(name);
        Matcher inProgressMatcher = inProgressFileRegex.matcher(name);
        if (doneMatcher.matches() || inProgressMatcher.matches()) {
          return true;
        }
        return false;
      }
    });
  }

  private void closeAllWALFilesForTable(String tableName) {
    Matcher matcher = Pattern.compile("^" + tableName + "_\\d+$").matcher("_dummy_");
    Iterator<Map.Entry<String, WALWriter>> iterator = writersMap.entrySet().iterator();
    Map.Entry<String, WALWriter> entry;
    while (iterator.hasNext()) {
      entry = iterator.next();
      if (matcher.reset(entry.getKey()).matches()) {
        try {
          entry.getValue().close();
        } catch (IOException e) {
          logger.warn("Failed to close WAL writer: {}", entry.getKey(), e);
        } finally {
          iterator.remove();
        }
      }
    }
  }

  /**
   * Get a list of files which are done
   *
   * @return list of file which are done by WAL writers
   */

  public String[] getDoneFiles() {
    if (!initialized) {
      return null;
    }
    String[] files = walDirectory.toFile().list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.endsWith(WAL_DONE_SUFFIX))
          return true;
        return false;
      }
    });
    if (null != files && files.length > 0) {
      Arrays.sort(files, new WALFileComparator());
    }
    return files;
  }

  private static final class AllFilesForBucketFilter implements FilenameFilter {
    final Pattern doneFileRegEx;
    final Pattern inProgressFileRegex;

    public AllFilesForBucketFilter(final String tableName, final int bucketId) {
      doneFileRegEx = Pattern
          .compile("^" + tableName + "_" + bucketId + "_" + "\\d+" + WAL_DONE_SUFFIX + "\\z");
      inProgressFileRegex = Pattern.compile(
          "^" + tableName + "_" + bucketId + "_" + "\\d+" + WAL_INPROGRESS_SUFFIX + "\\z$");
    }

    @Override
    public boolean accept(File dir, String name) {

      Matcher doneMatcher = doneFileRegEx.matcher(name);
      Matcher inProgressMatcher = inProgressFileRegex.matcher(name);
      if (doneMatcher.matches() || inProgressMatcher.matches()) {
        return true;
      }
      return false;
    }
  }

  public String[] getAllFilesForTableWithBucketId(String tableName, int bucketID) {
    if (!initialized) {
      return null;
    }
    final String[] list =
        walDirectory.toFile().list(new AllFilesForBucketFilter(tableName, bucketID));
    if (list == null) {
      return null;
    }
    Stream<String> files = Stream.of(list);
    return files.map(this::getAbsolutePath).map(Path::toString).toArray(String[]::new);
  }

  /**
   * Flush all WAL files for a table to tier1
   *
   * @param tableName
   */
  synchronized public void flush(String tableName, int partitionId) {
    if (!initialized) {
      return;
    }
    synchronized (instance) {
      String[] filesToFlush = getAllFilesForTableWithBucketId(tableName, partitionId);
      if (filesToFlush != null && filesToFlush.length > 0) {
        final TableDescriptor tableDescriptor = MTableUtils
            .getTableDescriptor((MonarchCacheImpl) MCacheFactory.getAnyInstance(), tableName);
        for (String walFileName : filesToFlush) {
          final WALWriter writer = getCurrentWriter(tableName, partitionId);
          /* if this file is also being appended to, then sync on the writer */
          if (writer != null && writer.getFile().toString().equals(walFileName)) {
            synchronized (writer) {
              writeToStore(tableName, partitionId, tableDescriptor, walFileName);
            }
          } else {
            writeToStore(tableName, partitionId, tableDescriptor, walFileName);
          }
        }
      }
    }
  }

  /**
   * Write the WAL records to subsequent tier-store.
   *
   * @param tableName the table-name
   * @param partitionId the partition/bucket id
   * @param td the table descriptor
   * @param file the WAL file to read records
   */
  private void writeToStore(String tableName, int partitionId, TableDescriptor td, String file) {
    String readerWalFileName = Paths.get(file).getFileName().toString();
    WALRecord[] walRecords = null;
    try (WALReader reader = this.getReader(readerWalFileName)) {
      do {
        walRecords = reader.readNext(WAL_FILE_RECORD_LIMIT);
        WALUtils.writeToStore(tableName, partitionId, td, walRecords);
      } while (walRecords != null && walRecords.length == WAL_FILE_RECORD_LIMIT);
    } catch (IOException e) {
      logger.error("Error while writing to Tier1. Exception: " + e.getMessage());
      throw new StoreInternalException(
          "Error while writing to Tier1. Exception: " + e.getMessage());
    }
    try {
      file = this.markFileDone(file);
    } catch (IOException e) {
      logger.error("WAL Flush, Error while marking WAL file done  Exception: " + e.getMessage());
      throw new StoreInternalException(
          "Error while marking WAL file done. Exception: " + e.getMessage());
    }
    this.deleteWALFile(file);
  }

  /**
   * Get the currently active WAL writer.
   *
   * @param tableName the table-name
   * @param partitionId the partition/bucket id
   * @return currently active WAL writer
   */
  WALWriter getCurrentWriter(final String tableName, final int partitionId) {
    return writersMap.get(tableName + "_" + partitionId);
  }

  /**
   * Delete all file for a table
   *
   * @param tableName
   */
  public void deleteTable(String tableName) {
    if (!initialized) {
      return;
    }
    String[] filesTodelete = getAllFilesForTable(tableName);
    for (String file : filesTodelete) {
      deleteWALFile(file);
    }
    closeAllWALFilesForTable(tableName);
    /** remove the sequence-no for the table that is getting deleted **/
    Matcher matcher = Pattern.compile("^" + tableName + "_[0-9]+$").matcher("_dummy_");
    Iterator<Map.Entry<String, AtomicLong>> iterator = bucketSeqNoMap.entrySet().iterator();
    Map.Entry<String, AtomicLong> entry;
    while (iterator.hasNext()) {
      entry = iterator.next();
      if (matcher.reset(entry.getKey()).matches()) {
        iterator.remove();
      }
    }
  }
}
