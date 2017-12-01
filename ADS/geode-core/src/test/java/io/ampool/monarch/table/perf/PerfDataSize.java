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
package io.ampool.monarch.table.perf;

public enum PerfDataSize {
  SMALL("/tmp/small.dat", 25, 128, "SMALL"), MEDIUM("/tmp/medium.dat", 25, 1280,
      "MEDIUM"), LARGE("/tmp/large.dat", 25, 12800, "LARGE");

  private String fileName;
  private int numOfColumns;
  private int bytesPerRow;
  private String name;

  PerfDataSize(String fileName, int numOfColumns, int bytesPerRow, String name) {

    this.fileName = fileName;
    this.numOfColumns = numOfColumns;
    this.bytesPerRow = bytesPerRow;
    this.name = name;
  }

  public String getFileName() {
    return fileName;
  }

  public int getNumOfColumns() {
    return numOfColumns;
  }

  public int getBytesPerRow() {
    return bytesPerRow;
  }

  public String getName() {
    return name;
  }
}
