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


import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.ampool.monarch.cli.MashCliStrings;
import io.ampool.monarch.table.Bytes;
import io.ampool.monarch.table.Cell;
import io.ampool.monarch.table.Row;
import io.ampool.monarch.types.TypeHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.multistep.CLIMultiStepHelper;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.logging.log4j.Logger;

public class MTableDataCommandResult implements Serializable {

  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = 1L;
  private String command;
  private List<Row> resultValues;

  public static final String SCAN_PAGE_START = "startCount";
  public static final String SCAN_PAGE_END = "endCount";

  public static final String RESULT_FLAG = "Result";
  public static final String NUM_ROWS = "Rows";

  private Object mTableName;
  private Object startKey;
  private Object endKey;
  private Throwable error;
  private String errorString;
  private boolean operationCompletedSuccessfully;
  private Throwable erorr;
  private boolean isFTable;

  public boolean isScan() {
    if (MashCliStrings.MSCAN.equals(command)) {
      return true;
    } else {
      return false;
    }
  }

  public static MTableDataCommandResult createMTableDataCommandResult(String command,
      String mTableName, byte[] startKey, byte[] endKey, List<Row> value, Throwable error,
      String errorString, boolean flag, boolean fTable) {
    MTableDataCommandResult result = new MTableDataCommandResult();
    result.command = command;
    result.mTableName = mTableName;
    result.startKey = startKey;
    result.endKey = endKey;
    result.resultValues = value;
    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    result.isFTable = fTable;
    return result;
  }

  public static MTableDataCommandResult createMTableDataCommandResult(String command,
      String mTableName, byte[] startKey, byte[] endKey, List<Row> value, Throwable error,
      String errorString, boolean flag) {
    MTableDataCommandResult result = new MTableDataCommandResult();
    result.command = command;
    result.mTableName = mTableName;
    result.startKey = startKey;
    result.endKey = endKey;
    result.resultValues = value;
    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    result.isFTable = false;
    return result;
  }

  public static MTableDataCommandResult createInfoMTableDataCommandResult(String command,
      String mTableName, byte[] startKey, byte[] endKey, List<Row> value, Throwable error,
      String errorString, boolean flag) {
    MTableDataCommandResult result = new MTableDataCommandResult();
    result.command = command;
    result.mTableName = mTableName;
    result.startKey = startKey;
    result.endKey = endKey;
    result.resultValues = value;
    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }

  public static MTableDataCommandResult createScanResult(String tableName, String startKey,
      String endKey, Throwable error, String errorString, boolean flag) {
    MTableDataCommandResult result = new MTableDataCommandResult();
    result.command = MashCliStrings.MSCAN;
    result.mTableName = tableName;
    result.startKey = startKey;
    result.endKey = endKey;
    result.resultValues = null;
    result.error = error;
    result.errorString = errorString;
    result.operationCompletedSuccessfully = flag;
    return result;
  }


  public void setErorr(final Throwable erorr) {
    this.erorr = erorr;
  }

  public void setErrorString(final String errorString) {
    this.errorString = errorString;
  }

  public String getErrorString() {
    return this.errorString;
  }

  /**
   * This method returns a "Page" as dictated by arguments startCount and endCount. Returned result
   * is not standard CommandResult and its consumed by Display Step
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public Result pageResult(int startCount, int endCount, String step) {
    List<String> fields = new ArrayList<String>();
    List values = new ArrayList<String>();
    fields.add(RESULT_FLAG);
    values.add(operationCompletedSuccessfully);
    fields.add(SCAN_PAGE_START);
    values.add(startCount);
    fields.add(SCAN_PAGE_END);
    values.add(endCount);
    if (errorString != null) {
      fields.add("Message");
      values.add(errorString + " " + error.getMessage());
      return CLIMultiStepHelper.createBannerResult(fields, values, step);
    } else {

      if (resultValues != null) {
        try {
          TabularResultData table = ResultBuilder.createTabularResultData();
          String[] headers = null;
          Object[][] rows = null;
          int rowCount = buildTable(table, startCount, endCount);
          GfJsonArray array = table.getHeaders();
          headers = new String[array.size()];
          rows = new Object[rowCount][array.size()];
          for (int i = 0; i < array.size(); i++) {
            headers[i] = (String) array.get(i);
            List<String> list = table.retrieveAllValues(headers[i]);
            for (int j = 0; j < list.size(); j++) {
              rows[j][i] = list.get(j);
            }
          }
          fields.add(NUM_ROWS);
          values.add((resultValues == null) ? 0 : resultValues.size());
          return CLIMultiStepHelper.createPageResult(fields, values, step, headers, rows);
        } catch (GfJsonException e) {
          String[] headers = new String[] {"Error"};
          Object[][] rows = {{e.getMessage()}};
          String fieldsArray[] = {SCAN_PAGE_START, SCAN_PAGE_END};
          Object valuesArray[] = {startCount, endCount};
          return CLIMultiStepHelper.createPageResult(fieldsArray, valuesArray, step, headers, rows);
        }
      } else {
        return CLIMultiStepHelper.createBannerResult(fields, values, step);
      }
    }
  }

  public void writeToFile(int startCount, int endCount, String fileName) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("MTableDataCommandResult.writeToFile startCount = " + startCount + " endCount = "
          + endCount + " fileName = " + fileName);
    }
    boolean isFTable = this.isFTable;

    FileWriter writer = new FileWriter(fileName, true);
    for (int i = startCount; i <= endCount; i++) {
      if (i >= this.resultValues.size()) {
        break;
      }
      Row row = resultValues.get(i);
      if (i == 0) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("#"); // This is for header (comment in csv).
        if (!isFTable) {
          stringBuilder.append("RowID");
        }
        List<Cell> cells = row.getCells();
        for (int j = 0; j < cells.size(); j++) {
          if (isFTable) {
            if (j > 0) {
              stringBuilder.append(",");
            }
          } else {
            stringBuilder.append(",");
          }
          stringBuilder.append(Bytes.toString(cells.get(j).getColumnName()));
          stringBuilder.append(":");
          stringBuilder.append(cells.get(j).getColumnType().toString());
        }
        writer.append(stringBuilder.toString());
        writer.append('\n');
        writer.flush();
      }
      toMResultCSV(row, writer, isFTable);
    }
    writer.close();
  }

  private int buildTable(TabularResultData table, int startCount, int endCount) {
    int rowCount = 0;
    // Introspect first using tabular data
    for (int i = startCount; i <= endCount; i++) {
      if (i >= resultValues.size()) {
        break;
      } else {
        rowCount++;
      }
      Row row = resultValues.get(i);
      table.accumulate(RESULT_FLAG, toMResultString(row));
    }
    return rowCount;
  }

  public String toMResultString(Row row) {
    StringBuilder stringBuilder = new StringBuilder();
    if (!this.isFTable) {
      stringBuilder.append("RowID = ");
      TypeHelper.deepToString(row.getRowId(), stringBuilder);
      stringBuilder.append(' ');
    }
    for (Cell cell : row.getCells()) {
      stringBuilder.append(Bytes.toString(cell.getColumnName())).append('=');
      TypeHelper.deepToString(cell.getColumnValue(), stringBuilder);
      stringBuilder.append(", ");
    }
    if (row.getCells().size() > 0) {
      stringBuilder.deleteCharAt(stringBuilder.length() - 2);
    }
    return stringBuilder.toString();
  }

  public void toMResultCSV(Row row, FileWriter writer, boolean isFTable) throws IOException {
    if (!isFTable) {
      writer.append(TypeHelper.deepToString(row.getRowId()));
    }
    int i = 0;
    for (Cell cell : row.getCells()) {
      if (isFTable) {
        if (i > 0) {
          writer.append(",");
        }
      } else {
        writer.append(",");
      }
      writer.append(TypeHelper.deepToString(cell.getColumnValue()));
      i++;
    }
    writer.append('\n');
    writer.flush();
  }

  public List<Row> getValues() {
    return this.resultValues;
  }

  public void addValues(List<Row> list) {
    if (this.resultValues == null)
      this.resultValues = list;
    else
      this.resultValues.addAll(list);
  }



  public CompositeResultData toScanCommandResult(String filePath) {
    boolean isFile = filePath != null && !filePath.isEmpty();
    if (errorString != null) {
      // return ResultBuilder.createGemFireErrorResult(errorString);
      CompositeResultData data = ResultBuilder.createCompositeResultData();
      CompositeResultData.SectionResultData section = data.addSection();
      section.addData("Message", errorString);
      section.addData(RESULT_FLAG, operationCompletedSuccessfully);
      return data;
    } else {
      CompositeResultData data = ResultBuilder.createCompositeResultData();
      CompositeResultData.SectionResultData section = data.addSection();
      TabularResultData table = section.addTable();
      section.addData(RESULT_FLAG, operationCompletedSuccessfully);
      if (this.resultValues != null) {
        if (isFile) {
          String message = this.resultValues.size() + " rows inserted to " + filePath;
          section.addData("Message", message);
        } else {
          section.addData(NUM_ROWS, this.resultValues.size());
          buildTable(table, 0, resultValues.size());
        }
      }
      return data;
    }
  }

  public Object getmTableName() {
    return mTableName;
  }

  public void setmTableName(final Object mTableName) {
    this.mTableName = mTableName;
  }

  public Object getStartKey() {
    return startKey;
  }

  public void setStartKey(final Object startKey) {
    this.startKey = startKey;
  }

  public Object getEndKey() {
    return endKey;
  }

  public void setEndKey(final Object endKey) {
    this.endKey = endKey;
  }

  public Throwable getError() {
    return error;
  }

  public void setError(final Throwable error) {
    this.error = error;
  }

  public boolean isOperationCompletedSuccessfully() {
    return this.operationCompletedSuccessfully;
  }

  public void aggregate(final MTableDataCommandResult result) {
    if (result == null)
      return;

    if (result.errorString != null && !result.errorString.equals(errorString)) {
      String newString = result.errorString + " " + errorString;
      errorString = newString;
    }

    if (result.getValues() != null) {
      this.operationCompletedSuccessfully = true;
      if (resultValues == null) {
        resultValues = result.getValues();
      } else {
        resultValues.addAll(result.getValues());
      }
      Collections.sort(resultValues);
    }
  }

}
