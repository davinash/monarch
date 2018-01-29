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

import io.ampool.monarch.table.MCacheFactory;
import io.ampool.monarch.table.TableDescriptor;
import io.ampool.monarch.table.exceptions.StoreInternalException;
import io.ampool.monarch.table.internal.MTableUtils;
import org.apache.geode.internal.cache.MonarchCacheImpl;
import io.ampool.store.StoreHandler;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;

import static io.ampool.tierstore.wal.WriteAheadLog.WAL_FILE_RECORD_LIMIT;

/**
 * Write Ahead Log (WAL) monitoring thread monitors the WAL for the completed and expired files and
 * calls storehandler to expire records to store.
 */
public class WALMonitoringThread extends Thread {

  public static final int WAL_READ_BATCH = WAL_FILE_RECORD_LIMIT;
  private final long sleepIntervalSecs;
  private final WriteAheadLog writeAheadLog;
  private final StoreHandler storeHandler;
  private static final Logger logger = LogService.getLogger();
  private Set<String> pausedBuckets = null;
  private static final int MAX_FILES_IN_ITERATION =
      Integer.getInteger("ampool.wal.process.limit", -1);


  public WALMonitoringThread(StoreHandler storeHandler, WriteAheadLog writeAheadLog,
      Properties properties) {
    pausedBuckets = new ConcurrentHashSet<>();
    this.storeHandler = storeHandler;
    this.writeAheadLog = writeAheadLog;
    // Use properties to configure parameters
    this.sleepIntervalSecs = 5;
    setName("WALMonitoringThread");
    setDaemon(true);
    setPriority(Thread.MIN_PRIORITY);
  }

  @Override
  public void run() {
    boolean filesProcessedInCurrentIteration;
    boolean expiredFilesProcessed;
    boolean doContinue = true;

    while (doContinue && !Thread.currentThread().isInterrupted()) {
      try {
        synchronized (this.writeAheadLog) {
          // Process Completed Files
          filesProcessedInCurrentIteration = processCompletedFiles(this.writeAheadLog);

          // Get all expired files (files which have not been modifed since configured time)
          expiredFilesProcessed = processExpiredFiles(this.writeAheadLog);
        }

        filesProcessedInCurrentIteration =
            expiredFilesProcessed || filesProcessedInCurrentIteration;

        // If no files processed sleep for 5 (configurable) secs
        if (!filesProcessedInCurrentIteration) {
          try {
            Thread.sleep(sleepIntervalSecs * 1000);
          } catch (InterruptedException e) {
            logger.debug("WAL monitoring thread interrupted");
            doContinue = false;
          }
        }
      } catch (Throwable throwable) {
        logger.error("Error in WAL Monitoring thread ", throwable);
      }
    }
    logger.warn("Exiting WALMonitoringThread..");
  }

  /**
   * Pause the WAL monitoring thread
   *
   * @param tableName
   * @param partitionID
   */
  public void pauseWALMonitor(String tableName, int partitionID) {
    pausedBuckets.add(tableName + "_" + partitionID);
  }

  /**
   * Resume the WAL monitoring thread
   *
   * @param tableName
   * @param partitionID
   */
  public void resumeWALMonitor(String tableName, int partitionID) {
    pausedBuckets.remove(tableName + "_" + partitionID);
  }

  /**
   * Process Completed files and expire them to store
   *
   * @param writeAheadLog
   * @return boolean indicating whether files were processed or not
   */
  private boolean processCompletedFiles(WriteAheadLog writeAheadLog) {
    // 1. Monitor WAL dir - list files
    String[] completedWALFiles = writeAheadLog.getCompletedFiles();
    if (completedWALFiles != null && completedWALFiles.length > 0) {
      return expireWALFilesToStore(writeAheadLog, completedWALFiles);
    }
    return false;
  }

  /**
   * Process Expired files and expire them to store
   *
   * @param writeAheadLog
   * @return boolean indicating whether files were processed or not
   */
  private boolean processExpiredFiles(WriteAheadLog writeAheadLog) {
    String[] expiredWALFiles = writeAheadLog.getExpiredFiles();
    String[] doneWALFiles = new String[expiredWALFiles.length];

    if (expiredWALFiles.length > 0) {
      // Rename file and append .done to file name
      int i = 0;
      for (String expiredWALFile : expiredWALFiles) {
        final String fileName = Paths.get(expiredWALFile).getFileName().toString();
        String tableName = writeAheadLog.getTableName(fileName);
        int partitionId = writeAheadLog.getPartitionId(fileName);

        /* TODO: watch for races with writer */
        if (pausedBuckets.contains(tableName + "_" + partitionId)) {
          continue;
        }
        try {
          doneWALFiles[i++] = writeAheadLog.markFileDone(expiredWALFile);
        } catch (IOException ie) {
          /* TODO: Handle me */
          throw new StoreInternalException(
              "Error while writing to WAL. Exception: " + ie.getMessage());
        }
      }
      // Expire WAL files
      return expireWALFilesToStore(writeAheadLog, doneWALFiles);
    }
    return false;
  }

  /**
   * Expires WAL files to store
   *
   * @param writeAheadLog
   * @param walFilesToProcess
   * @return
   */
  private boolean expireWALFilesToStore(WriteAheadLog writeAheadLog, String[] walFilesToProcess) {
    boolean processed = false;
    if (walFilesToProcess != null && walFilesToProcess.length > 0) {
      int counter = 0;
      // For each file
      for (String walFileName : walFilesToProcess) {
        if (counter >= MAX_FILES_IN_ITERATION) {
          try {
            Thread.yield();
            Thread.sleep(sleepIntervalSecs);
            counter = 0;
          } catch (InterruptedException e) {
            logger.warn("Error during sleep..", e);
          }
        }
        counter++;
        // Get the StoreHierrarchy from MCache and
        // get store from StoreHandler using tablename
        /**
         * sometimes the server may be shutdown while in this loop.. so just need to make sure that
         * Geode cache (meta-region with descriptors) is available before we process any file.
         */
        if (MonarchCacheImpl.getInstance() == null) {
          logger.info("Seems GemFireCache is not available.. skipping.");
          break;
        }

        /**
         * logged and skipped the processing for invalid file names for now.. need to find out why
         * this might be happening.
         */
        if (walFileName == null) {
          logger.warn("Got <null> WAL file to process.. skipping.");
          continue;
        }
        String fileName = Paths.get(walFileName).getFileName().toString();
        if (fileName == null) {
          logger.warn("Got <null> path for the fileName.. skipping.");
          continue;
        }
        String tableName = writeAheadLog.getTableName(fileName);
        TableDescriptor td = MTableUtils
            .getTableDescriptor((MonarchCacheImpl) MCacheFactory.getAnyInstance(), tableName);
        int partitionId = writeAheadLog.getPartitionId(fileName);

        if (pausedBuckets.contains(tableName + "_" + partitionId)) {
          continue;
        }

        /* if this file is also being appended to, then sync on the writer */
        final WALWriter writer = writeAheadLog.getCurrentWriter(tableName, partitionId);
        if (writer != null && writer.getFile().getFileName().toString().equals(fileName)) {
          synchronized (writer) {
            processed |= writeToStore(writeAheadLog, fileName, tableName, partitionId, td);
          }
        } else {
          processed |= writeToStore(writeAheadLog, fileName, tableName, partitionId, td);
        }
      }
    }
    return processed;
  }

  /**
   * Write the WAL records to subsequent tier-store.
   *
   * @param writeAheadLog the WAL instance
   * @param fileName the WAL file to read records
   * @param tableName the table-name
   * @param partitionId the partition/bucket id
   * @param td the table descriptor
   * @return true if the records were successfully moved to next tier; false otherwise
   */
  private boolean writeToStore(WriteAheadLog writeAheadLog, String fileName, String tableName,
      int partitionId, TableDescriptor td) {
    boolean filesProcessedInCurrentIteration = false;
    // For each set of 1000 (configurable) records call store1.append
    // WALReader reader = null;
    WALRecord[] walRecords = null;
    boolean writeSuccess = true;

    try (final WALReader reader = writeAheadLog.getReader(fileName)) {
      do {
        walRecords = reader.readNext(WAL_READ_BATCH);
        WALUtils.writeToStore(tableName, partitionId, td, walRecords);
      } while (walRecords != null && walRecords.length == WAL_READ_BATCH);
    } catch (IOException e) {
      logger.error("Error moving data to tierStore for WAL file= {}", fileName, e);
      writeSuccess = false;
      // throw new StoreInternalException("Error while reading from WAL. Exception: " +
      // e.getMessage());
    }

    if (writeSuccess) {
      writeAheadLog.deleteWALFile(fileName);
      filesProcessedInCurrentIteration = true;
    }
    return filesProcessedInCurrentIteration;
  }


}
