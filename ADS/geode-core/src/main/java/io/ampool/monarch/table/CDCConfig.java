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

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;

/**
 * Config to create the <code>CDCStream</code>. Below example illustrates how to create the instance
 * of CDCConfig and add the <code>CDCStream</code>.
 * 
 * <PRE>
 MTableDescriptor tableDescriptor = new MTableDescriptor();

 // create CDCConfig from tableDescriptor
 CDCConfig cdcConfig = tableDescriptor.createCDCConfig();

 // set the attributes on cdcConfig
 cdcConfig.setBatchSize(batchSize);
 cdcConfig.setBatchConflationEnabled(isConflation);
 cdcConfig.setMaximumQueueMemory(maxMemory);
 .
 .
 // add CDCStream to MTable by providing the id and full class path name of
 MAsyncEventListener implementation and cdcConfig instance.

 tableDescriptor.addCDCStream("MTABLE_ASYNC_EVENT_QUEUE_ORDERED",
 "io.ampool.monarch.table.MTableEventListener2", cdcConfig);
 * </PRE>
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface CDCConfig {
  /**
   * Sets the disk store name for overflow or persistence. This Disk store needs to be already
   * created by admin using mash.
   * 
   * @param name Name of the disk-store
   */
  public CDCConfig setDiskStoreName(String name);

  /**
   * Sets the maximum amount of memory (in MB) for CDCStream Default is 100 MB.
   *
   * @param memory The maximum amount of memory (in MB) for CDCStream
   */
  public CDCConfig setMaximumQueueMemory(int memory);

  /**
   * Sets whether or not the writing to the disk is synchronous. Default is true.
   *
   * @param isSynchronous boolean if true indicates synchronous writes
   *
   */
  public CDCConfig setDiskSynchronous(boolean isSynchronous);

  /**
   * Sets the batch size for CDCStream Default is 100.
   *
   * @param size The size of batches sent to its <code>MAsyncEventListener</code>
   */
  public CDCConfig setBatchSize(int size);

  /**
   * Sets the batch time interval (in milliseconds) for a CDCStream. Default is 5 ms.
   *
   * @param interval The maximum time interval that can elapse before a partial batch is sent from a
   *        CDCStream.
   */
  public CDCConfig setBatchTimeInterval(int interval);

  /**
   * Sets whether the CDCStream is persistent or not. Default is true.
   *
   * @param isPersistent Whether to enable persistence for CDCStream.
   */
  public CDCConfig setPersistent(boolean isPersistent);

  /**
   * Sets whether to enable batch conflation for CDCStream. Default is false.
   *
   * @param isConflation Whether or not to enable batch conflation for batches sent from a CDCStream
   */
  public CDCConfig setBatchConflationEnabled(boolean isConflation);

  /**
   * Sets the number of dispatcher thread. Default is 5.
   *
   * @param numThreads number of dispatcher thread.
   */
  public CDCConfig setDispatcherThreads(int numThreads);

}
