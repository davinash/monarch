package io.ampool.monarch.table.internal;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.MTableType;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

/**
 * Created by rgeiger on 4/28/16.
 *
 * Using parallel scan has been shown to improve performance close to 3x on a 4 way split scan of 3M
 * rows for an unordered table, but has not been extensively tested. Ordered table scans are made
 * faster but ordering of results is not guaranteed; a parallel scan of a range partitioned ordered
 * table will not be in order.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MClientParallelScanner extends Scanner {
  Scan scan;
  MTable table;
  MTableType tableType;
  int clientQueueSize;
  boolean started;
  boolean scanCompleted;
  boolean isRandomPartitionedOrdered;

  Row lastItem = null;

  ArrayList<MClientScannerUsingGetAll> scanners;
  ArrayList<MClientScannerUsingGetAll> removeList;
  Queue<Row> results;

  static Logger logger = LogService.getLogger();

  public MClientParallelScanner(Scan scan, ProxyMTableRegion table, int groupSize) {
    if (scan.batchModeEnabled()) {
      throw new IllegalArgumentException("Parallel scanner does not support batch mode");
    }

    if (groupSize <= 0) {
      throw new IllegalArgumentException("Group size must be > 0");
    }

    this.scan = scan;
    this.table = table;
    this.tableType = table.getTableDescriptor().getTableType();
    this.clientQueueSize = scan.getClientQueueSize();
    this.started = false;
    this.scanCompleted = false;
    if (this.clientQueueSize < 2) {
      throw new IllegalArgumentException("batch size must be > 1");
    }

    this.isRandomPartitionedOrdered = false; // MTable does not support ordered random partitioned
                                             // yet
    if (isRandomPartitionedOrdered && !scan.getReturnKeysFlag()) {
      isRandomPartitionedOrdered = false;
    }

    Set<Integer> bucketIdSet = getApplicableBucketIds(scan, table.getTableDescriptor());

    scanners = new ArrayList<>(bucketIdSet.size());
    removeList = new ArrayList<>(scanners.size());

    // create scanner set for each bucket group (pass in bucket IDs)

    Iterator<Integer> iter = bucketIdSet.iterator();
    Set<Integer> scanSet = new TreeSet<>();
    int count = 0;

    while (iter.hasNext()) {
      scanSet.add(iter.next());
      count += 1;

      if ((count == groupSize) || !iter.hasNext()) {
        scan.setBucketIds(scanSet);
        MClientScannerUsingGetAll scanner = new MClientScannerUsingGetAll(scan, table);
        logger.debug("create scanner for " + scanSet.toString());
        scanners.add(scanner);

        if (iter.hasNext()) {
          scanSet = new TreeSet<>();
          count = 0;
        }
      }
    }
    // allocate a queue to hold the results fetched
    if (isRandomPartitionedOrdered) {
      results = new PriorityBlockingQueue<>((clientQueueSize * 2) + scanners.size());
    } else {
      results = new ArrayBlockingQueue<>((clientQueueSize * 2) + scanners.size());
    }
  }

  @Override
  public void close() {
    for (MClientScannerUsingGetAll scanner : scanners) {
      scanner.close();
    }
    scanCompleted = true;
    scanners.clear();
    results.clear();
  }

  @Override
  public Row next() {
    if (scanCompleted && results.isEmpty()) {
      return null;
    }

    /*
     * This is fast but does not preserve order for ordered tables; fetch in bunches and the return
     * results to the caller.
     */

    // pull from scanners to put into main queue, return next
    boolean resultsAdded = false;

    while (!scanCompleted && (results.size() < (clientQueueSize * 2))) {
      resultsAdded = false;

      for (MClientScannerUsingGetAll scanner : scanners) {
        Row item = scanner.next();
        if (item != null) {
          results.add(item);
          resultsAdded = true;
        } else {
          removeList.add(scanner);
        }
      }

      if (removeList.size() > 0) {
        for (int i = 0; i < removeList.size(); i++) {
          scanners.remove(removeList.get(i));
        }
        removeList.clear();
      }

      if (resultsAdded == false) {
        scanCompleted = true;
      }
    }

    if (results.isEmpty()) {
      return null;
    }

    return results.poll();
  }
}
