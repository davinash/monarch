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

package io.ampool.monarch.table.coprocessor;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Delete;
import io.ampool.monarch.table.Get;
import io.ampool.monarch.table.Put;

/**
 * An observer interface which observes client actions on MTable.
 *
 * To act on these actions, write observer either using interface {@link MTableObserver} or using
 * default implementation {@link MBaseRegionObserver}. {@link MBaseRegionObserver} has empty
 * implementation for each action event.
 *
 * @since 0.2.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MTableObserver {

  /**
   * Called after the table is created.
   *
   * This event called once for every node as parts of MTable are created on every node.
   *
   * WARNING: This method should not do any operation on Table.
   */
  void postOpen();

  /**
   * Called before a table is deleted. The <code>MTableObserver</code> will not additionally be
   * called for each entry that is deleted in the table as a result of deleting table.
   *
   * WARNING: This method should not destroy or create any regions itself or a deadlock will occur.
   *
   * @param mObserverContext
   */
  void preClose(MObserverContext mObserverContext);

  /**
   * Called after a table is deleted. The <code>MTableObserver</code> will not additionally be
   * called for each entry that is deleted in the table as a result of deleting table. This event
   * will be called once for every node as parts of MTable are created on every node.
   */
  void postClose();

  /**
   * Called before client GET operation is performed. MObserverContext.bypass can be used to skip
   * action.
   *
   * @param mObserverContext
   * @param get - User provided MGet object
   */
  void preGet(MObserverContext mObserverContext, Get get);

  /**
   * Called after client GET operation is performed.
   *
   * @param mObserverContext
   * @param get - User provided MGet object
   */
  void postGet(MObserverContext mObserverContext, Get get);

  /**
   * Called before client PUT operation is performed.
   *
   * @param mObserverContext
   * @param put - User provided MPut object
   */
  void prePut(MObserverContext mObserverContext, Put put);

  /**
   * Called after client PUT operation is performed.
   *
   * @param mObserverContext
   * @param put - User provided MPut object
   */
  void postPut(MObserverContext mObserverContext, Put put);

  /**
   * Called before client DELETE operation is performed.
   *
   * @param mObserverContext
   * @param delete - User provided MDelete object
   */
  void preDelete(MObserverContext mObserverContext, Delete delete);

  /**
   * Called after client DELETE operation is performed.
   *
   * @param mObserverContext
   * @param delete - User provided MDelete object
   */
  void postDelete(MObserverContext mObserverContext, Delete delete);

  /**
   * Called before PUT but after check.
   *
   * @param mObserverContext
   * @param rowKey - RowKey provided by client to check.
   * @param columnName - Column Name to check
   * @param columnValue - ColumnValue to verify.
   * @param put - MPut object to PUT if check is successful
   * @param isCheckSuccessful - Indicates if check is successful
   */
  void preCheckAndPut(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Put put, boolean isCheckSuccessful);

  /**
   * Called after PUT of CheckAndPut operation
   *
   * @param mObserverContext
   * @param rowKey - RowKey provided by client to check.
   * @param columnName - Column Name to check
   * @param columnValue - ColumnValue to verify.
   * @param put - MPut object to PUT if check is successful
   * @param isCheckSuccessful - Indicates if check is successful
   */
  void postCheckAndPut(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Put put, boolean isCheckSuccessful);

  /**
   * Called before DELETE but after check.
   *
   * @param mObserverContext
   * @param rowKey - RowKey provided by client to check.
   * @param columnName - Column Name to check
   * @param columnValue - ColumnValue to verify.
   * @param delete - MDelete object to DELETE if check is successful
   * @param isCheckSuccessful - Indicates if check is successful
   */
  void preCheckAndDelete(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Delete delete, boolean isCheckSuccessful);

  /**
   * Called before DELETE but after check.
   *
   * NOTE : This event is not invoked if check is unsuccessful
   * 
   * @param mObserverContext
   * @param rowKey - RowKey provided by client to check.
   * @param columnName - Column Name to check
   * @param columnValue - ColumnValue to verify.
   * @param delete - MDelete object to DELETE if check is successful
   * @param isCheckSuccessful - Indicates if check is successful
   */
  void postCheckAndDelete(MObserverContext mObserverContext, byte[] rowKey, byte[] columnName,
      byte[] columnValue, Delete delete, boolean isCheckSuccessful);
}
