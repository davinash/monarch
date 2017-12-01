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
package io.ampool.management.internal.cli.domain;

import java.io.Serializable;

import io.ampool.monarch.cli.MashCliStrings;

public class MTableDataCommandRequest implements Serializable {
  private String command;
  private byte[] startKey;
  private byte[] endKey;
  private String mTableName;
  private String key;
  private String value;
  /** value and valueJson coexist for now.. **/
  private String valueJson;
  private int maxLimit;
  private long timeStamp;
  private Object principal;

  public boolean isScan() {
    return MashCliStrings.MSCAN.equals(command);
  }

  public void setStartKey(final byte[] startKey) {
    this.startKey = startKey;
  }

  public void setEndKey(final byte[] endKey) {
    this.endKey = endKey;
  }

  public void setMTableName(String mtableName) {
    this.mTableName = mtableName;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  public String getCommand() {
    return this.command;
  }

  public byte[] getStartKey() {
    return this.startKey;
  }

  public byte[] getEndKey() {
    return this.endKey;
  }

  public String getMTableName() {
    return this.mTableName;
  }

  public void setKey(final String key) {
    this.key = key;
  }

  public String getKey() {
    return this.key;
  }

  public void setValue(final String value) {
    this.value = value;
  }

  public String getValue() {
    return this.value;
  }

  public int getMaxLimit() {
    return maxLimit;
  }

  public void setMaxLimit(final int maxLimit) {
    this.maxLimit = maxLimit;
  }

  public void setTimeStamp(long timeStamp) {
    if (timeStamp < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative");
    }
    this.timeStamp = timeStamp;
  }

  public long getTimeStamp() {
    return (timeStamp);
  }

  /**
   * Get JSON value.
   *
   * @return the row value as JSON
   */
  public String getValueJson() {
    return valueJson;
  }

  /**
   * Set the JSON value
   * 
   * @param valueJson the row value as JSON
   */
  public void setValueJson(String valueJson) {
    this.valueJson = valueJson;
  }

  public Object getPrincipal() {
    return principal;
  }

  public void setPrincipal(Object principal) {
    this.principal = principal;
  }
}
