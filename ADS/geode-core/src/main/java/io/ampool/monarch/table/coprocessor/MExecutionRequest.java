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

import java.io.Serializable;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.MTable;
import io.ampool.monarch.table.coprocessor.internal.MResultCollector;

/**
 * MExecutionRequest used to pass request params to server in coprocessor execution. For usage,
 * {@link MTable#coprocessorService(String, String, byte[], MExecutionRequest)} or
 * {@link MTable#coprocessorService(String, String, byte[], byte[], MExecutionRequest)}.
 *
 * @see Scan
 * @see MTable
 * 
 * @since 0.2.0
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MExecutionRequest implements Serializable {

  private static final long serialVersionUID = 1090819074270293371L;
  private Object arguments = null;
  private MResultCollector resultCollector;
  private Object filter;
  private Scan scanner;
  private String methodName;

  private String transactionId;

  /**
   * Create MExecutionRequest. This contains data to be passed in coprocessor execution
   * 
   * @param arguments arguments to be passed
   * @param resultCollector pass null for default, in future user will have a capabilities to set
   *        own.
   * @param filter filter to be passed to the server in coprocessor execution
   */
  public MExecutionRequest(Object arguments, MResultCollector resultCollector, Object filter) {
    this.arguments = arguments;
    this.resultCollector = resultCollector;
    this.filter = filter;
  }

  /**
   * Creates a new instance of MExecutionRequest
   */
  public MExecutionRequest() {
    this.arguments = null;
  }

  /**
   * Sets user arguments to be used in coprocessor execution
   * 
   * @param arguments user arguments.
   */
  public void setArguments(Object arguments) {
    this.arguments = arguments;
  }

  /**
   * Sets user's custom resultCollector. This is intended for the future release. For now default
   * resultCollector is used. User does not need to set this.
   * 
   * @param resultCollector
   */
  public void setResultCollector(MResultCollector resultCollector) {
    this.resultCollector = resultCollector;
  }

  /**
   * Sets user specified filter
   * 
   * @param filter filter to be used in coprocessor execution
   */
  public void setFilter(Object filter) {
    this.filter = filter;
  }

  /**
   * Fetch the user arguments
   * 
   * @return arguments
   */
  public Object getArguments() {
    return arguments;
  }

  /**
   * Fetch the user filter
   * 
   * @return filter
   */
  public Object getFilter() {
    return filter;
  }

  /**
   * Fetch the user specified resultCollector
   * 
   * @return resultCollector used to send result
   */
  public MResultCollector getResultCollector() {
    return resultCollector;
  }

  /**
   * Get the scanner
   * 
   * @return scanner
   */
  public Scan getScanner() {
    return scanner;
  }

  /**
   * Sets scanner to be used in coprocessor execution
   * 
   * @param scanner
   */
  public void setScanner(Scan scanner) {
    this.scanner = scanner;
  }

  /**
   * Gets a methodName to be executed in coprocessor execution
   * 
   * @return methodName Method to be executed
   */
  public String getMethodName() {
    return methodName;
  }

  /**
   * Sets a methodName to be executed in coprocessor execution
   * 
   * @param methodName Method to be executed
   */
  public void setMethodName(String methodName) {
    this.methodName = methodName;
  }

  /**
   * Gets a transactionId associated with a current coprocessor
   * 
   * @return transactionId associated trxId
   */
  public String getTransactionId() {
    return transactionId;
  }

  /**
   * Sets a transactionId associated with coprocessor execution
   * 
   * @param transactionId TrxId associated with coprocessor
   */
  public void setTransactionId(String transactionId) {
    this.transactionId = transactionId;
  }

}
