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

import io.ampool.monarch.table.coprocessor.internal.MCoprocessorContextImpl;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TrxRegionEndpoint extends MCoprocessor {
  private static final Logger logger = LogService.getLogger();

  protected ConcurrentHashMap<String, TrxTransactionState> transactionsById =
      new ConcurrentHashMap<String, TrxTransactionState>();

  private Map<Long, String> stateMap = new HashMap<>();

  public Object methodA(MCoprocessorContext context) {
    // System.out.println("TrxRegionEndpoint.rowCount "+"Read from context:
    // "+context.getTable().getName());
    // System.out.println("TrxRegionEndpoint.run rowCount Printing State");
    // System.out.println("TrxRegionEndpoint.run " + stateMap);
    // stateMap.put(System.currentTimeMillis(), "rowCount");
    String txId = (String) context.getRequest().getTransactionId();
    RegionFunctionContextImpl rfci = ((MCoprocessorContextImpl) context).getNativeContext();
    int targetBucketId = rfci.getTargetBucketId();

    TrxTransactionState trxState = null;
    if (!transactionsById.containsKey(txId)) {
      trxState = new TrxTransactionState();
    } else {
      trxState = transactionsById.get(txId);
    }
    trxState.updateTrxState("methodA_" + targetBucketId);
    transactionsById.put(txId, trxState);


    Object[] args = (Object[]) context.getRequest().getArguments();
    if ("TrxCommit".equals(args[0])) {
      return transactionsById;
    } else {
      // return dummy value.
      return 10;
    }
  }

  public Object methodB(MCoprocessorContext context) {
    // System.out.println("TrxRegionEndpoint.rowCount "+"Read from context:
    // "+context.getTable().getName());
    // System.out.println("TrxRegionEndpoint.run rowCount Printing State");
    // System.out.println("TrxRegionEndpoint.run " + stateMap);
    // stateMap.put(System.currentTimeMillis(), "rowCount");
    String txId = (String) context.getRequest().getTransactionId();
    RegionFunctionContextImpl rfci = ((MCoprocessorContextImpl) context).getNativeContext();
    int targetBucketId = rfci.getTargetBucketId();

    TrxTransactionState trxState = null;
    if (!transactionsById.containsKey(txId)) {
      trxState = new TrxTransactionState();
    } else {
      trxState = transactionsById.get(txId);
    }
    trxState.updateTrxState("methodB_" + targetBucketId);
    transactionsById.put(txId, trxState);


    Object[] args = (Object[]) context.getRequest().getArguments();
    if ("TrxCommit".equals(args[0])) {
      return transactionsById;
    } else {
      // return dummy value.
      return 20;
    }
  }

  public Object methodC(MCoprocessorContext context) {
    // System.out.println("TrxRegionEndpoint.rowCount "+"Read from context:
    // "+context.getTable().getName());
    // System.out.println("TrxRegionEndpoint.run rowCount Printing State");
    // System.out.println("TrxRegionEndpoint.run " + stateMap);
    // stateMap.put(System.currentTimeMillis(), "rowCount");
    String txId = (String) context.getRequest().getTransactionId();
    RegionFunctionContextImpl rfci = ((MCoprocessorContextImpl) context).getNativeContext();
    int targetBucketId = rfci.getTargetBucketId();

    TrxTransactionState trxState = null;
    if (!transactionsById.containsKey(txId)) {
      trxState = new TrxTransactionState();
    } else {
      trxState = transactionsById.get(txId);
    }
    trxState.updateTrxState("methodC_" + targetBucketId);
    transactionsById.put(txId, trxState);


    Object[] args = (Object[]) context.getRequest().getArguments();
    if ("TrxCommit".equals(args[0])) {
      return transactionsById;
    } else {
      // return dummy value.
      return 30;
    }
  }

  public Object methodD(MCoprocessorContext context) {
    // System.out.println("TrxRegionEndpoint.rowCount "+"Read from context:
    // "+context.getTable().getName());
    // System.out.println("TrxRegionEndpoint.run rowCount Printing State");
    // System.out.println("TrxRegionEndpoint.run " + stateMap);
    // stateMap.put(System.currentTimeMillis(), "rowCount");
    String txId = (String) context.getRequest().getTransactionId();
    RegionFunctionContextImpl rfci = ((MCoprocessorContextImpl) context).getNativeContext();
    int targetBucketId = rfci.getTargetBucketId();

    TrxTransactionState trxState = null;
    if (!transactionsById.containsKey(txId)) {
      trxState = new TrxTransactionState();
    } else {
      trxState = transactionsById.get(txId);
    }
    trxState.updateTrxState("methodD_" + targetBucketId);
    transactionsById.put(txId, trxState);

    Object[] args = (Object[]) context.getRequest().getArguments();
    if ("TrxCommit".equals(args[0])) {
      return transactionsById;
    } else {
      // return dummy value.
      return 40;
    }
  }

  public Object commitTransaction(MCoprocessorContext context) {
    String txId = (String) context.getRequest().getTransactionId();
    return transactionsById.get(txId);
  }

  /*
   * @Override public String getId() { //return this.getClass().getName(); return this.id; }
   */
}
