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
package io.ampool.monarch.table.facttable;

import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.MEvictionPolicy;
import io.ampool.monarch.table.ftable.FTableDescriptor;
import io.ampool.monarch.table.ftable.PartitionResolver;


public class FTableDescriptorHelper {
  private static int DEFAULT_NUM_COLUMNS = 10;
  private static String DEFAULT_COL_PREFIX = "COL";

  public static FTableDescriptor getFTableDescriptor() {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      // sets a partitioningColumn
      if (i == 0) {
        tableDescriptor.setPartitioningColumn(Bytes.toBytes(DEFAULT_COL_PREFIX + i));
      }
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(PartitionResolver partitionResolver) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(PartitionResolver partitionResolver,
      MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }


  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      PartitionResolver partitionResolver) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      PartitionResolver partitionResolver) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      PartitionResolver partitionResolver) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      PartitionResolver partitionResolver) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, PartitionResolver partitionResolver) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, PartitionResolver partitionResolver,
      MEvictionPolicy mevictionPolicy) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(MEvictionPolicy mevictionPolicy,
      String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(PartitionResolver partitionResolver,
      String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(PartitionResolver partitionResolver,
      MEvictionPolicy mevictionPolicy, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }


  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      MEvictionPolicy mevictionPolicy, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      PartitionResolver partitionResolver, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy,
      String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(String recoveryDiskStore, int numColumns) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      MEvictionPolicy mevictionPolicy, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      PartitionResolver partitionResolver, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy,
      String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      MEvictionPolicy mevictionPolicy, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      PartitionResolver partitionResolver, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy,
      String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      MEvictionPolicy mevictionPolicy, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      PartitionResolver partitionResolver, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy,
      String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, MEvictionPolicy mevictionPolicy, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, PartitionResolver partitionResolver, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, PartitionResolver partitionResolver,
      MEvictionPolicy mevictionPolicy, String recoveryDiskStore) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRecoveryDiskStore(recoveryDiskStore);
    return tableDescriptor;
  }


  public static FTableDescriptor getFTableDescriptor(MEvictionPolicy mevictionPolicy,
      int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(PartitionResolver partitionResolver,
      int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(PartitionResolver partitionResolver,
      MEvictionPolicy mevictionPolicy, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }


  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      MEvictionPolicy mevictionPolicy, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      PartitionResolver partitionResolver, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      MEvictionPolicy mevictionPolicy, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      PartitionResolver partitionResolver, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      MEvictionPolicy mevictionPolicy, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      PartitionResolver partitionResolver, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      MEvictionPolicy mevictionPolicy, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      PartitionResolver partitionResolver, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, MEvictionPolicy mevictionPolicy, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, PartitionResolver partitionResolver, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, PartitionResolver partitionResolver,
      MEvictionPolicy mevictionPolicy, int numCopies) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }


  public static FTableDescriptor getFTableDescriptor(MEvictionPolicy mevictionPolicy, int numCopies,
      int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(PartitionResolver partitionResolver,
      int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(PartitionResolver partitionResolver,
      MEvictionPolicy mevictionPolicy, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }


  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn, int numCopies,
      int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      MEvictionPolicy mevictionPolicy, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      PartitionResolver partitionResolver, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(byte[] partitioningColumn,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy, int numCopies,
      int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < DEFAULT_NUM_COLUMNS; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      MEvictionPolicy mevictionPolicy, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      PartitionResolver partitionResolver, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy, int numCopies,
      int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      MEvictionPolicy mevictionPolicy, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      PartitionResolver partitionResolver, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, byte[] partitioningColumn,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy, int numCopies,
      int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(DEFAULT_COL_PREFIX + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      MEvictionPolicy mevictionPolicy, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      PartitionResolver partitionResolver, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      PartitionResolver partitionResolver, MEvictionPolicy mevictionPolicy, int numCopies,
      int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, MEvictionPolicy mevictionPolicy, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, PartitionResolver partitionResolver, int numCopies,
      int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

  public static FTableDescriptor getFTableDescriptor(int numColumns, String colPrefix,
      byte[] partitioningColumn, PartitionResolver partitionResolver,
      MEvictionPolicy mevictionPolicy, int numCopies, int numSplits) {
    FTableDescriptor tableDescriptor = new FTableDescriptor();
    for (int i = 0; i < numColumns; i++) {
      tableDescriptor.addColumn(colPrefix + i);
    }
    tableDescriptor.setPartitioningColumn(partitioningColumn);
    tableDescriptor.setPartitionResolver(partitionResolver);
    tableDescriptor.setEvictionPolicy(mevictionPolicy);
    tableDescriptor.setRedundantCopies(numCopies);
    tableDescriptor.setTotalNumOfSplits(numSplits);
    return tableDescriptor;
  }

}
