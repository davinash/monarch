package io.ampool.monarch.table.ftable;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Scanner;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.internal.Table;
import io.ampool.monarch.table.exceptions.MException;

/**
 * Defines interface to access FTable. <br>
 * Obtain an instance from a {@link io.ampool.monarch.table.MCache#getFTable(String)}.
 * <p>
 * Table reference can be used to append or scan data from a table.
 *
 * @since 1.1.1
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface FTable extends Table {

  /**
   * Returns FTableDescriptor associated with this table.
   *
   * @return {@link FTableDescriptor}
   */
  FTableDescriptor getTableDescriptor();

  /**
   * Get Name of the table
   *
   * @return Name of the table
   */
  String getName();

  /**
   * Returns number of columns in this table. This does NOT include special insertion time column
   * available by default with every FTable.
   *
   * @return Number of Columns
   */
  int getTotalNumOfColumns();

  /**
   * Adds one or more records to FTable. The records are partitioned on
   * {@link FTableDescriptor#getPartitioningColumn()} and are sorted on insertion timestamp within
   * each partition/bucket. Insertion timestamp is auto generated on the server side when record is
   * inserted into table.
   *
   * @param records Row(s) to append in to the table
   **/
  void append(Record... records);


  /**
   * Get a scanner instance for a FTable.
   *
   * @param scan see {@link Scan}
   * @return instance of a scanner
   * @throws MException
   */
  Scanner getScanner(final Scan scan) throws MException;
}
